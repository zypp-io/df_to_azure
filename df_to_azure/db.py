import logging
import os
import re
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import text

from df_to_azure.auth import AUTH_ACTIVE_DIRECTORY_DEFAULT, AUTH_MANAGED_IDENTITY, AUTH_SQL_PASSWORD, get_auth_mode
from df_to_azure.exceptions import DriverError, UpsertError


class SqlUpsert:
    def __init__(self, table_name, schema, id_cols, columns):
        self.table_name = table_name
        self.schema = schema
        self.id_cols = id_cols
        self.columns = [col.strip() for col in columns]

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
        insert = self.create_insert_statement()
        query = f"""
        CREATE PROCEDURE [UPSERT_{self.table_name}]
        AS
        MERGE {self.schema}.{self.table_name} t
            USING staging.{self.table_name} s
        ON {self.create_on_statement()}
        WHEN MATCHED
            THEN UPDATE SET
                {self.create_update_statement()}
        WHEN NOT MATCHED BY TARGET
            THEN INSERT {insert[0]}
                 VALUES {insert[1]};
        """
        logging.debug(query)

        return text(query)

    def drop_procedure(self):
        query = f"DROP PROCEDURE IF EXISTS [UPSERT_{self.table_name}];"
        return text(query)

    def create_stored_procedure(self):
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

    sql_auth = get_auth_mode("DF_TO_AZURE_SQL_AUTH", AUTH_ACTIVE_DIRECTORY_DEFAULT)
    if sql_auth == AUTH_SQL_PASSWORD:
        connection_url = "mssql+pyodbc://{}:{}@{}:1433/{}?driver={}".format(
            os.environ.get("SQL_USER"),
            quote_plus(os.environ.get("SQL_PW")),
            os.environ.get("SQL_SERVER"),
            os.environ.get("SQL_DB"),
            driver,
        )
    else:
        authentication = {
            AUTH_ACTIVE_DIRECTORY_DEFAULT: "ActiveDirectoryDefault",
            AUTH_MANAGED_IDENTITY: "ActiveDirectoryMsi",
        }.get(sql_auth)
        if authentication is None:
            raise ValueError(
                "DF_TO_AZURE_SQL_AUTH must be 'active_directory_default', 'managed_identity', or 'sql_password'."
            )

        username = None
        if sql_auth == AUTH_MANAGED_IDENTITY:
            username = os.environ.get("DF_TO_AZURE_SQL_MANAGED_IDENTITY_CLIENT_ID")

        connection_url = URL.create(
            "mssql+pyodbc",
            username=username,
            host=os.environ.get("SQL_SERVER"),
            port=1433,
            database=os.environ.get("SQL_DB"),
            query={
                "driver": driver,
                "Encrypt": "yes",
                "TrustServerCertificate": "no",
                "Connection Timeout": "600",
                "Authentication": authentication,
            },
        )

    con = create_engine(connection_url).connect()

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
