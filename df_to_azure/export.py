from sqlalchemy import create_engine
import pandas as pd
import os
import logging
from df_to_azure.exceptions import CreateSchemaError
from df_to_azure.adf import create_blob_service_client
import df_to_azure.adf as adf
from df_to_azure.parse_settings import TableParameters


def table_list(df_dict: dict, schema: str, method: str, id_field: str, cwd: str) -> list:
    tables = []
    for name, df in df_dict.items():

        table = TableParameters(
            df=df, name=name, schema=schema, method=method, id_field=id_field, cwd=cwd
        )

        tables.append(table)

    return tables


def run_multiple(df_dict, schema, method="create", id_field=None, cwd=None):

    tables = table_list(df_dict, schema, method, id_field, cwd)

    create = True if os.environ.get("create") == "True" else False
    if create:
        create_schema(tables[0])

        # azure components
        adf.create_resourcegroup()
        adf.create_datafactory()
        adf.create_blob_container()

        # linked services
        adf.create_linked_service_sql()
        adf.create_linked_service_blob()

    for table in tables:

        upload_dataset(table)
        adf.create_input_blob(table)
        adf.create_output_sql(table)

    # pipelines
    adf.create_multiple_activity_pipeline(tables)


def run(df, tablename, schema, method="create", id_field=None, cwd=None):
    table = TableParameters(
        df=df, name=tablename, schema=schema, method=method, id_field=id_field, cwd=cwd
    )
    create = True if os.environ.get("create") == "True" else False
    if create:
        create_schema(table)

        # azure components
        adf.create_resourcegroup()
        adf.create_datafactory()
        adf.create_blob_container()

        # linked services
        adf.create_linked_service_sql()
        adf.create_linked_service_blob()

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
    connection_string = auth_azure()
    engine = create_engine(connection_string, pool_size=10, max_overflow=20)
    table.df.head(n=0).to_sql(
        table.name,
        engine,
        if_exists="replace",
        index=False,
        schema=table.schema,
    )
    result = (
        f"push successful ({table.name}):",
        len(table.df),
        f"records pushed to Microsoft Azure ({table.df.shape[1]} columns)",
    )
    logging.info(result)


def auth_azure():

    connection_string = "mssql+pyodbc://{}:{}@{}:1433/{}?driver={}".format(
        os.environ.get("ls_sql_database_user"),
        os.environ.get("ls_sql_database_password"),
        os.environ.get("ls_sql_server_name"),
        os.environ.get("ls_sql_database_name"),
        "ODBC Driver 17 for SQL Server",
    )

    return connection_string


def upload_to_blob(table):
    blob_client = create_blob_service_client()
    blob_client = blob_client.get_blob_client(
        container=os.environ.get("ls_blob_container_name"),
        blob=f"{table.name}/{table.name}",
    )
    full_path_to_file = os.path.join("tmp", table.name + ".csv")

    table.df.to_csv(
        full_path_to_file, index=False, sep="^", line_terminator="\n"
    )  # export file to staging

    logging.info(f"start uploading blob {table.name}...")
    with open(full_path_to_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info(f"finished uploading blob {table.name}!")


# TODO: create download_blob method
# def download_blob(table):
#     blob_client = create_blob_service_client()
#     blob_list = blob_client.list_blobs()
#     blob_client = blob_client.get_blob_client(
#         container=table.azure["ls_blob_container_name"],
#         blob=f"{table.name}/{table.name}",
#     )
#     download_file_path = os.path.join(local_path, filename.split('/')[-1:][0])
#
#     with open(download_file_path, "wb") as download_file:
#         download_file.write(blob_client.download_blob().readall())


def create_schema(table):
    try:
        execute_stmt(stmt=f"create schema {table.schema}")
    except CreateSchemaError:
        logging.info(f"CreateSchemaError: did not create schema {table.schema}")


def execute_stmt(stmt):
    """
    :param stmt: SQL statement to be executed
    :return: executes the statment
    """
    conn = auth_azure()
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
        execute_stmt(stmt)
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
    conn = auth_azure()
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
