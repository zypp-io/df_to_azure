import os
from io import BytesIO
from time import sleep

from azure.storage.blob import BlobServiceClient
from pandas import DataFrame, read_parquet
from pandas.testing import assert_frame_equal

from df_to_azure import df_to_azure
from df_to_azure.tests import data


def test_create_parquet():
    df = data["sample_1"]
    df_to_azure(df=df, tablename="my_test_tablename_create", schema="my_test_schema", parquet=True)


def test_append_parquet():
    df = data["sample_1"]

    df_to_azure(df=df, tablename="my_test_tablename_append", schema="my_test_schema", parquet=True, method="append")
    sleep(1)
    df_to_azure(df=df, tablename="my_test_tablename_append", schema="my_test_schema", parquet=True, method="append")


def test_upsert_parquet_same_shape():
    df = DataFrame({"id": range(1000, 1050, 10), "value1": range(10, 60, 10), "value2": list("abcde")})
    # upload original df to storage
    df_to_azure(df=df, tablename="test_upsert", schema="test_parquet", parquet=True)

    # make updates to table
    df = df.copy()
    df.loc[0, "value1"] = 99
    df.loc[3, "value2"] = "ZZ"

    # perform upsert
    df_to_azure(df=df, tablename="test_upsert", schema="test_parquet", parquet=True)

    # download the parquet back
    blob_service_client = BlobServiceClient.from_connection_string(os.environ.get("AZURE_STORAGE_CONNECTION_STRING"))
    container_client = blob_service_client.get_container_client(container="parquet")
    downloaded_blob = container_client.download_blob("test_parquet/test_upsert.parquet")
    bytes_io = BytesIO(downloaded_blob.readall())
    result = read_parquet(bytes_io)

    # check if upsert was successful
    assert_frame_equal(df, result)


if __name__ == "__main__":
    test_upsert_parquet_same_shape()
