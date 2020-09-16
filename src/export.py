from .adf import create_blob_service_client
from sqlalchemy import create_engine
import pandas as pd
import os
import logging
from .functions import cat_modules, create_dir
from . import adf
from .parse_settings import adf_settings


def run(tablename, df):
    if adf_settings["create"]:
        # azure components
        adf.create_resourcegroup()
        adf.create_datafactory()
        adf.create_blob_container()

        # linked services
        adf.create_linked_service_sql()
        adf.create_linked_service_blob()

    upload_dataset(tablename, df)
    adf.create_input_blob(tablename)
    adf.create_output_sql(tablename)

    # pipelines
    adf.create_pipeline(tablename)


def upload_dataset(tablename, df):

    push_to_azure(
        df=df.head(n=0),
        tablename=tablename,
        schema_name=adf_settings["ls_sql_schema_name"],
    )
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

    current_dir = os.path.dirname(__file__)
    stagingdir = create_dir(os.path.join(current_dir, "../data", "staging"))

    full_path_to_file = os.path.join(stagingdir, tablename + ".csv")
    df.to_csv(full_path_to_file, index=False, sep="^")  # export file to staging

    blob_service_client = create_blob_service_client()

    blob_client = blob_service_client.get_blob_client(
        container=adf_settings["ls_blob_container_name"],
        blob=f"{tablename}/{tablename}",
    )

    logging.info(f"start uploading blob {tablename}...")
    with open(full_path_to_file, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    logging.info(f"finished uploading blob {tablename}!")
