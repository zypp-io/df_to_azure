from time import sleep

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
