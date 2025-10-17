import logging
import os
import re
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import text

from df_to_azure.exceptions import DriverError, UpsertError


class SqlUpsert:
    def __init__(self, table_name, schema, id_cols, columns, preserve_identity=False):
        self.table_name = table_name
        self.schema = schema
        self.id_cols = id_cols
        self.columns = [col.strip() for col in columns]
        self.preserve_identity = preserve_identity
        self.identity_columns = []

    def get_identity_columns(self):
        """
        Query SQL Server to detect IDENTITY (auto-increment) columns in the target table.

        Returns
        -------
        list
            List of column names that have IDENTITY property in the target table.
        """
        query = text(f"""
        SELECT c.name
        FROM sys.identity_columns ic
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        INNER JOIN sys.tables t ON ic.object_id = t.object_id
        INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE t.name = '{self.table_name}' AND s.name = '{self.schema}'
        """)

        with auth_azure() as con:
            result = con.execute(query)
            return [row[0] for row in result]

    def validate_identity_usage(self):
        """
        Validate that user isn't trying to upsert on IDENTITY columns without explicit permission.

        This method checks if any of the id_cols (columns used for matching in upsert) are IDENTITY
        columns. If so, and preserve_identity=False, it raises an informative error with alternatives.

        If preserve_identity=True and IDENTITY columns are in id_cols, logs a warning about the risks.

        Raises
        ------
        UpsertError
            If id_cols contain IDENTITY columns and preserve_identity=False.
        """
        self.identity_columns = self.get_identity_columns()

        # Check if any id_cols are IDENTITY columns
        identity_in_id_cols = [col for col in self.id_cols if col in self.identity_columns]

        if identity_in_id_cols and not self.preserve_identity:
            # Build helpful error message based on the scenario
            if len(self.id_cols) == 1 and len(identity_in_id_cols) == 1:
                # Scenario A: IDENTITY is the only id_field
                raise UpsertError(
                    f"Column '{identity_in_id_cols[0]}' is an auto-increment (IDENTITY) column "
                    f"and cannot be used for upsert matching.\n\n"
                    f"Suggested alternatives:\n"
                    f"1. Use method='append' instead if you want to insert new records with auto-generated IDs\n"
                    f"2. Add a business key column (e.g., 'user_email', 'external_id') and use that for id_field\n"
                    f"3. If you must preserve existing ID values (e.g., data migration), set preserve_identity=True\n"
                    f"   WARNING: Using preserve_identity=True is not recommended as it can break ID sequence generation"
                )
            else:
                # Scenario B: IDENTITY is part of composite key
                other_cols = [col for col in self.id_cols if col not in identity_in_id_cols]
                raise UpsertError(
                    f"Column(s) {identity_in_id_cols} are auto-increment (IDENTITY) columns "
                    f"and are part of your id_field {self.id_cols}.\n\n"
                    f"Suggested alternatives:\n"
                    f"1. Remove IDENTITY column(s) from id_field and use only: {other_cols}\n"
                    f"2. If you must preserve existing ID values (e.g., data migration), set preserve_identity=True\n"
                    f"   WARNING: Using preserve_identity=True is not recommended as it can break ID sequence generation"
                )

        # If preserve_identity=True and IDENTITY columns are in id_cols, log warning
        if self.preserve_identity and identity_in_id_cols:
            logging.warning(
                f"preserve_identity=True: IDENTITY_INSERT will be enabled for {self.schema}.{self.table_name}. "
                f"This is not recommended and may cause ID sequence issues. "
                f"Consider using non-IDENTITY columns for id_field instead."
            )

    def create_on_statement(self):
        on = " AND ".join([f"s.[{id_col}] = t.[{id_col}]" for id_col in self.id_cols])
        return on

    def create_update_statement(self):
        update = ", ".join([f"t.[{col}] = s.[{col}]" for col in self.columns if col not in self.id_cols])
        return update

    def create_insert_statement(self):
        insert = f"([{'], ['.join(self.columns)}])"

        values = ", ".join([f"s.[{col}]" for col in self.columns])
        values = f"({values})"

        return insert, values

    def create_merge_query(self):
        """
        Generate MERGE statement with optional IDENTITY_INSERT handling.

        If preserve_identity=True, wraps the MERGE statement with
        SET IDENTITY_INSERT ON/OFF to allow explicit insertion of IDENTITY values.

        Returns
        -------
        text
            SQLAlchemy text object containing the CREATE PROCEDURE statement.
        """
        insert = self.create_insert_statement()

        merge_stmt = f"""MERGE {self.schema}.{self.table_name} t
            USING staging.{self.table_name} s
        ON {self.create_on_statement()}
        WHEN MATCHED
            THEN UPDATE SET
                {self.create_update_statement()}
        WHEN NOT MATCHED BY TARGET
            THEN INSERT {insert[0]}
                 VALUES {insert[1]};"""

        if self.preserve_identity:
            # Wrap with IDENTITY_INSERT ON/OFF
            query = f"""
        CREATE PROCEDURE [UPSERT_{self.table_name}]
        AS
        SET IDENTITY_INSERT {self.schema}.{self.table_name} ON;
        {merge_stmt}
        SET IDENTITY_INSERT {self.schema}.{self.table_name} OFF;
        """
        else:
            query = f"""
        CREATE PROCEDURE [UPSERT_{self.table_name}]
        AS
        {merge_stmt}
        """

        logging.debug(query)
        return text(query)

    def drop_procedure(self):
        query = f"DROP PROCEDURE IF EXISTS [UPSERT_{self.table_name}];"
        return text(query)

    def create_stored_procedure(self):
        """
        Create the stored procedure for upsert operation.

        This method first validates that IDENTITY columns are being used correctly,
        then creates the stored procedure with the MERGE statement.

        Raises
        ------
        UpsertError
            If IDENTITY columns are used incorrectly or if procedure creation fails.
        """
        # Validate IDENTITY usage BEFORE creating procedure
        self.validate_identity_usage()

        with auth_azure() as con:
            t = con.begin()
            query_drop_procedure = self.drop_procedure()
            con.execute(query_drop_procedure)
            query_create_merge = self.create_merge_query()
            try:
                con.execute(query_create_merge)
                t.commit()
            except ProgrammingError:
                raise UpsertError(
                    "During upsert there has been an issue. One of the sources could be that the table in"
                    " staging has columns that do not match the table you want to upsert. Remove the "
                    f"staging table {self.table_name} manually in that case"
                )


def get_sql_driver() -> str:
    import pyodbc

    sql_drivers = [driver for driver in pyodbc.drivers() if re.match(r"ODBC Driver \d+ for SQL Server", driver)]
    try:
        sql_driver = sql_drivers[-1]
    except IndexError:
        raise DriverError("ODBC driver not found")

    return sql_driver


def auth_azure(driver: str = None):
    if driver is None:
        driver = get_sql_driver()

    connection_string = "mssql+pyodbc://{}:{}@{}:1433/{}?driver={}".format(
        os.environ.get("SQL_USER"),
        quote_plus(os.environ.get("SQL_PW")),
        os.environ.get("SQL_SERVER"),
        os.environ.get("SQL_DB"),
        driver,
    )
    con = create_engine(connection_string).connect()

    return con


def execute_stmt(stmt: str):
    """
    Execute SQL query

    Parameters
    ----------
    stmt: str
        SQL query statement.
    Returns
    -------

    """

    with auth_azure() as con:
        with con.begin():
            con.execute(text(stmt))
