import os
from io import BytesIO
from time import sleep

import pytest
from azure.storage.blob import BlobServiceClient
from pandas import DataFrame, concat, read_parquet
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
    df_to_azure(df=df, tablename="upsert_same_shape", schema="test_parquet", parquet=True)

    # make updates to table
    df.loc[0, "value1"] = 99
    df.loc[3, "value2"] = "ZZ"

    # perform upsert
    df_to_azure(
        df=df, tablename="upsert_same_shape", schema="test_parquet", method="upsert", parquet=True, id_field=["id"]
    )

    # download the parquet back
    blob_service_client = BlobServiceClient.from_connection_string(os.environ.get("AZURE_STORAGE_CONNECTION_STRING"))
    container_client = blob_service_client.get_container_client(container="parquet")
    downloaded_blob = container_client.download_blob("test_parquet/upsert_same_shape.parquet")
    bytes_io = BytesIO(downloaded_blob.readall())
    result = read_parquet(bytes_io)

    # check if upsert was successful
    assert_frame_equal(df, result)


def test_upsert_new_rows():
    df1 = DataFrame({"id": [1, 2, 3], "value1": ["A", "B", "C"], "value2": ["D", "E", "F"]})

    # create new rows
    df2 = DataFrame({"id": [4], "value1": ["Z"], "value2": ["ZZ"]})
    df2 = concat([df1, df2], ignore_index=True)

    # upload original df to storage
    df_to_azure(df=df1, tablename="upsert_new_rows", schema="test_parquet", parquet=True)

    # perform upsert
    df_to_azure(
        df=df2, tablename="upsert_new_rows", schema="test_parquet", method="upsert", parquet=True, id_field=["id"]
    )

    # download the parquet back
    blob_service_client = BlobServiceClient.from_connection_string(os.environ.get("AZURE_STORAGE_CONNECTION_STRING"))
    container_client = blob_service_client.get_container_client(container="parquet")
    downloaded_blob = container_client.download_blob("test_parquet/upsert_new_rows.parquet")
    bytes_io = BytesIO(downloaded_blob.readall())
    result = read_parquet(bytes_io)

    # check if upsert was successful
    assert_frame_equal(df2, result)


def test_uniqueness_id_cols():
    df = DataFrame({"id": range(1000, 1050, 10), "value1": range(10, 60, 10), "value2": list("abcde")})
    df.loc[1, "id"] = df.loc[0, "id"]

    # this should raise since columns are not unique
    with pytest.raises(AssertionError):
        df_to_azure(df=df, tablename="test_upsert", schema="test_parquet", method="upsert", id_field=["id"])
