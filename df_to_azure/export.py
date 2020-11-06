from sqlalchemy import create_engine
import pandas as pd
import os
import logging
from df_to_azure.exceptions import CreateSchemaError
from df_to_azure.functions import create_dir
from df_to_azure.adf import create_blob_service_client
import df_to_azure.adf as adf
from df_to_azure.parse_settings import TableParameters


def table_list(df_dict: dict, schema: str, method: str, id_field: str, yaml_path: str) -> list:
    tables = []
    for name, df in df_dict.items():

        table = TableParameters(
            df=df,
            name=name,
            schema=schema,
            method=method,
            id_field=id_field,
            yaml_path=yaml_path,
        )

        tables.append(table)

    return tables


def run_multiple(df_dict, schema, method="create", id_field=None, yaml_path=None):

    tables = table_list(df_dict, schema, method, id_field, yaml_path)

    if tables[0].azure["create"]:
        create_schema(tables[0])

        # azure components
        adf.create_resourcegroup(tables[0].azure)
        adf.create_datafactory(tables[0].azure)
        adf.create_blob_container(tables[0].azure)

        # linked services
        adf.create_linked_service_sql(tables[0].azure)
        adf.create_linked_service_blob(tables[0].azure)

    for table in tables:

        upload_dataset(table)
        adf.create_input_blob(table)
        adf.create_output_sql(table)

    # pipelines
    adf.create_multiple_activity_pipeline(tables)


def run(df, tablename, schema, method="create", id_field=None, yaml_path=None):
    table = TableParameters(
        df=df, name=tablename, schema=schema, method=method, id_field=id_field, yaml_path=yaml_path
    )

    if table.azure["create"]:
        create_schema(table)

        # azure components
        adf.create_resourcegroup(table.azure)
        adf.create_datafactory(table.azure)
        adf.create_blob_container(table.azure)

        # linked services
        adf.create_linked_service_sql(table.azure)
        adf.create_linked_service_blob(table.azure)

    upload_dataset(table)
    adf.create_input_blob(table)
    adf.create_output_sql(table)

    # pipelines
    adf.create_pipeline(table)


def upload_dataset(table):

    if len(table.df) == 0:
        return logging.info("no new records to upload.")

    if table.method == "create":
        push_to_azure(table)
    if table.method == "upsert":
        delete_current_records(table)

    upload_to_blob(table)
    logging.info("Finished.number of transactions:{}".format(len(table.df)))


def push_to_azure(table):
    connection_string = auth_azure(table.azure)
    engine = create_engine(connection_string, pool_size=10, max_overflow=20)
    table.df.head(n=0).to_sql(
        table.name,
        engine,
        chunksize=100000,
        if_exists="replace",
        index=False,
        schema=table.schema,
    )
    number_of_columns = str(len(table.df.columns))

    result = (
        "push successful ({}):".format(table.name),
        len(table.df),
        "records pushed to Microsoft Azure ({} columns)".format(number_of_columns),
    )
    logging.info(result)


def auth_azure(settings):

    connectionstring = "mssql+pyodbc://{}:{}@{}:1433/{}?driver={}".format(
        settings["ls_sql_database_user"],
        settings["ls_sql_database_password"],
        settings["ls_sql_server_name"],
        settings["ls_sql_database_name"],
        "ODBC Driver 17 for SQL Server",
    )

    return connectionstring


def upload_to_blob(table):

    current_dir = os.path.dirname(__file__)
    stagingdir = create_dir(os.path.join(current_dir, "..", "data", "staging"))

    full_path_to_file = os.path.join(stagingdir, table.name + ".csv")
    table.df.to_csv(
        full_path_to_file, index=False, sep="^", line_terminator="\n"
    )  # export file to staging

    blob_service_client = create_blob_service_client(table.azure)

    blob_client = blob_service_client.get_blob_client(
        container=table.azure["ls_blob_container_name"],
        blob=f"{table.name}/{table.name}",
    )

    logging.info(f"start uploading blob {table.name}...")
    with open(full_path_to_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info(f"finished uploading blob {table.name}!")


def create_schema(table):
    try:
        execute_stmt(stmt=f"create schema {table.schema}", settings=table.azure)
    except CreateSchemaError:
        logging.info(f"CreateSchemaError: did not create schema {table.schema}")


def execute_stmt(stmt, settings):
    """
    :param stmt: SQL statement to be executed
    :return: executes the statment
    """
    conn = auth_azure(settings)
    engn = create_engine(conn)

    with engn.connect() as con:
        rs = con.execute(stmt)

    return rs


def delete_current_records(table):
    """
    :param df: the dataframe containing new records
    :param tablename: the name of the table
    :param schema: schema in the database
    :param id_field: field to check if the values are already in the database
    :return: executes a delete statement in Azure SQL for the new records.
    """

    del_list = get_overlapping_records(table)
    stmt = create_sql_delete_stmt(del_list, table)

    if len(del_list):
        execute_stmt(stmt, settings=table.azure)
    else:
        logging.info("skip deleting. no records in delete statement")


def get_overlapping_records(table):
    """
    :param df: the dataframe containing new records
    :param tablename: the name of the table
    :param schema: schema in the database
    :param id_field: field to check if the values are already in the database
    :return:  a list of records that are overlapping
    """
    conn = auth_azure(table.azure)
    engn = create_engine(conn)

    current_db = pd.read_sql_table(table.name, engn, schema=table.schema)
    overlapping_records = current_db[current_db[table.id_field].isin(table.df[table.id_field])]
    del_list = overlapping_records.astype(str)[table.id_field].to_list()

    new_records = table.df[~table.df[table.id_field].isin(current_db[table.id_field])]
    logging.info(f"{len(overlapping_records)} updated records and {len(new_records)} new records")

    return del_list


def create_sql_delete_stmt(del_list, table):
    """
    :param del_list: list of records that need to be formatted in SQL delete statement.
    :param tablename: the name of the table
    :param schema: schema in the database
    :param id_field: field to check if the values are already in the database
    :return: SQL statement for deleting the specific records
    """

    del_list = ["'" + x + "'" for x in del_list]

    sql_list = ", ".join(del_list)
    sql_stmt = f"DELETE FROM {table.schema}.{table.name} WHERE {table.id_field} IN ({sql_list})"
    logging.info(f"{len(del_list)} run_id's in delete statement")

    return sql_stmt
