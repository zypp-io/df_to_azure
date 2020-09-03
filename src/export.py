from src.adf import create_blob_service_client
from sqlalchemy import create_engine
import pandas as pd
import os
from src.parse_settings import get_settings
import logging

adf_settings = get_settings("settings/yml/adf_settings.yml")
azure_settings = get_settings("settings/yml/azure_settings.yml")


def upload_sample_dataset(tablename):

    pickle_file = os.path.join(os.getcwd(), "data", "pickles", tablename + ".pkl")
    df = pd.read_pickle(pickle_file)

    push_to_azure(df=df.head(n=0), tablename=tablename, schema_name=adf_settings["ls_sql_schema_name"])
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
        adf_settings["ls_sql_database_user"],
        adf_settings["ls_sql_database_password"],
        adf_settings["ls_sql_server_name"],
        adf_settings["ls_sql_database_name"],
        "ODBC Driver 17 for SQL Server",
    )

    return connectionstring


def upload_to_blob(df, tablename):

    full_path_to_file = os.path.join(os.getcwd(), "data", "staging", tablename + ".csv")
    df.to_csv(full_path_to_file, index=False)  # export file to staging

    blob_service_client = create_blob_service_client()

    blob_client = blob_service_client.get_blob_client(
        container=adf_settings["ls_blob_container_name"], blob=f"{tablename}/{tablename}"
    )

    logging.info(f"start uploading blob {tablename}...")
    with open(full_path_to_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info(f"finished uploading blob {tablename}!")
