from .adf import create_blob_service_client
from sqlalchemy import create_engine
import pandas as pd
import os
import logging
from .functions import create_dir
from . import adf
from .parse_settings import get_settings


settings = get_settings(os.environ.get("AZURE_TO_DF_SETTINGS"))


def run_multiple(df_dict, schema, incremental=False, id_field=None):
    if settings["create"]:
        create_schema(schema)

        # azure components
        adf.create_resourcegroup()
        adf.create_datafactory()
        adf.create_blob_container()

        # linked services
        adf.create_linked_service_sql()
        adf.create_linked_service_blob()

    for tablename, df in df_dict.items():
        upload_dataset(tablename, df, schema, incremental, id_field)
        adf.create_input_blob(tablename)
        adf.create_output_sql(tablename, schema)

    # pipelines
    adf.create_multiple_activity_pipeline(df_dict)


def run(df, tablename, schema, incremental=False, id_field=None):
    if settings["create"]:
        create_schema(schema)

        # azure components
        adf.create_resourcegroup()
        adf.create_datafactory()
        adf.create_blob_container()

        # linked services
        adf.create_linked_service_sql()
        adf.create_linked_service_blob()

    upload_dataset(tablename, df, schema, incremental, id_field)
    adf.create_input_blob(tablename)
    adf.create_output_sql(tablename, schema)

    # pipelines
    adf.create_pipeline(tablename)


def upload_dataset(tablename, df, schema, incremental, id_field):

    if len(df) == 0:
        return logging.info("no new records to upload.")

    if not incremental:
        push_to_azure(
            df=df.head(n=0),
            tablename=tablename,
            schema_name=schema,
        )
    elif incremental:
        delete_current_records(df, tablename, schema, id_field)

    upload_to_blob(df, tablename)
    logging.info("Finished.number of transactions:{}".format(len(df)))


def push_to_azure(df, tablename, schema_name):
    connectionstring = auth_azure()
    engn = create_engine(connectionstring, pool_size=10, max_overflow=20)
    df.to_sql(
        tablename,
        engn,
        chunksize=100000,
        if_exists="replace",
        index=False,
        schema=schema_name,
    )
    numberofcolumns = str(len(df.columns))

    result = (
        "push successful ({}):".format(tablename),
        len(df),
        "records pushed to Microsoft Azure ({} columns)".format(numberofcolumns),
    )
    logging.info(result)


def auth_azure():

    connectionstring = "mssql+pyodbc://{}:{}@{}:1433/{}?driver={}".format(
        settings["ls_sql_database_user"],
        settings["ls_sql_database_password"],
        settings["ls_sql_server_name"],
        settings["ls_sql_database_name"],
        "ODBC Driver 17 for SQL Server",
    )

    return connectionstring


def upload_to_blob(df, tablename):

    current_dir = os.path.dirname(__file__)
    stagingdir = create_dir(os.path.join(current_dir, "../data", "staging"))

    full_path_to_file = os.path.join(stagingdir, tablename + ".csv")
    df.to_csv(
        full_path_to_file, index=False, sep="^", line_terminator="\n"
    )  # export file to staging

    blob_service_client = create_blob_service_client()

    blob_client = blob_service_client.get_blob_client(
        container=settings["ls_blob_container_name"],
        blob=f"{tablename}/{tablename}",
    )

    logging.info(f"start uploading blob {tablename}...")
    with open(full_path_to_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info(f"finished uploading blob {tablename}!")


def create_schema(schema):
    try:
        execute_stmt(stmt=f"create schema {schema}")
        logging.info(f"succesfully created schema {schema}")
    except:
        logging.info(f"did not create schema {schema}")


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


def delete_current_records(df, tablename, schema, id_field):
    """
    :param df: new records
    :param name: name of the table
    :return: executes a delete statement in Azure SQL for the new records.
    """

    del_list = get_overlapping_records(df, tablename, schema, id_field)
    stmt = create_sql_delete_stmt(del_list, tablename, schema, id_field)

    if len(del_list):
        execute_stmt(stmt)
    else:
        logging.info("skip deleting. no records in delete statement")


def get_overlapping_records(df, tablename, schema, id_field):
    """
    :param df: the dataframe containing new records
    :param name: the name of the table
    :return:  a list of records that are overlapping
    """
    conn = auth_azure()
    engn = create_engine(conn)

    current_db = pd.read_sql_table(tablename, engn, schema=schema)
    overlapping_records = current_db[current_db[id_field].isin(df[id_field])]
    del_list = overlapping_records.astype(str)[id_field].to_list()

    new_records = df[~df[id_field].isin(current_db[id_field])]
    logging.info(
        f"{len(overlapping_records)} updated records and {len(new_records)} new records"
    )

    return del_list


def create_sql_delete_stmt(del_list, tablename, schema, id_field):
    """
    :param del_list: list of records that need to be formatted in SQL delete statement.
    :param tablename: the name of the table
    :return: SQL statement for deleting the specific records
    """

    del_list = ["'" + x + "'" for x in del_list]

    sql_list = ", ".join(del_list)
    sql_stmt = f"DELETE FROM {schema}.{tablename} WHERE {id_field} IN ({sql_list})"
    logging.info(f"{len(del_list)} run_id's in delete statement")

    return sql_stmt
