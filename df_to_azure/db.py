import logging
import re
import struct
from urllib.parse import quote_plus

from azure.identity import DefaultAzureCredential
from sqlalchemy import create_engine, event
from sqlalchemy.engine import URL
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql import text

from df_to_azure.env import get_env
from df_to_azure.exceptions import DriverError, UpsertError

SQL_COPT_SS_ACCESS_TOKEN = 1256
SQL_DATABASE_SCOPE = "https://database.windows.net/.default"
TRUSTED_CONNECTION_OPTIONS = (";Trusted_Connection=Yes", ";Trusted_Connection=yes")


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

    # Explicit SQL credentials win; without them the connection is passwordless.
    # Same priority as the ADF SQL linked service.
    if get_env("SQL_USER") and get_env("SQL_PW"):
        connection_url = create_sql_password_url(driver)
        return create_engine(connection_url).connect()

    connection_url = create_passwordless_sql_url(driver)
    engine = create_engine(connection_url)
    add_access_token_listener(engine)
    return engine.connect()


def create_passwordless_sql_url(driver: str):
    return URL.create(
        "mssql+pyodbc",
        host=get_env("SQL_SERVER"),
        port=1433,
        database=get_env("SQL_DB"),
        query={
            "driver": driver,
            "Encrypt": "yes",
            "TrustServerCertificate": "no",
            "Connection Timeout": "600",
        },
    )


def add_access_token_listener(engine, credential=None):
    credential = credential or DefaultAzureCredential()

    @event.listens_for(engine, "do_connect")
    def provide_access_token(dialect, conn_rec, cargs, cparams):
        add_access_token_to_connection(cargs, cparams, credential)


def add_access_token_to_connection(cargs, cparams, credential):
    cargs[0] = remove_trusted_connection(cargs[0])
    cparams.setdefault("attrs_before", {})[SQL_COPT_SS_ACCESS_TOKEN] = create_access_token_struct(credential)


def remove_trusted_connection(connection_string: str):
    for option in TRUSTED_CONNECTION_OPTIONS:
        connection_string = connection_string.replace(option, "")

    return connection_string


def create_access_token_struct(credential=None):
    token = (credential or DefaultAzureCredential()).get_token(SQL_DATABASE_SCOPE).token.encode("utf-16-le")
    return struct.pack(f"<I{len(token)}s", len(token), token)


def create_sql_password_url(driver: str):
    return "mssql+pyodbc://{}:{}@{}:1433/{}?driver={}".format(
        get_env("SQL_USER"),
        quote_plus(get_env("SQL_PW")),
        get_env("SQL_SERVER"),
        get_env("SQL_DB"),
        driver,
    )


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
