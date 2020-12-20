import os
import logging
import time
import pandas as pd
from pandas._testing import assert_frame_equal
from dotenv import load_dotenv

from df_to_azure import df_to_azure, dfs_to_azure
from df_to_azure.db import auth_azure
from df_to_azure.exceptions import PipelineRunError

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
load_dotenv(verbose=True, override=True)

"""
This is the testing suite for df_to_azure. In general the following steps will be done per test:

1. Read a sample csv file from repo or the web
2. Write file to DB with df_to_azure, this can be create or upsert.
3. Use `wait_till_pipeline_is_done` function to wait for ADF pipeline to be Succeeded before continuing.
4. Read data back from DB and test if we got expected output with pandas._testing.assert_frame_equal.
"""


def wait_till_pipeline_is_done(adf_client, run_response):
    """
    Function to check if pipeline is done, else wait.
    We need this because tests will compare dataframes, else
      the dataframe will be read before it's uploaded
      through ADF.
    """
    status = ""
    while status != "Succeeded":
        pipeline_run = adf_client.pipeline_runs.get(
            os.environ.get("rg_name"), os.environ.get("df_name"), run_response.run_id
        )
        time.sleep(2)
        status = pipeline_run.status

        if status.lower() == "failed":
            raise PipelineRunError("Pipeline failed")


# ---- CREATE TESTS ----
def test_create_sample(file_dir="data"):
    file_dir = file_dir + "/sample_1.csv"
    expected = pd.read_csv(file_dir)
    adf_client, run_response = df_to_azure(
        df=expected,
        tablename="sample",
        schema="test",
        method="create",
        id_field="col_a",
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    with auth_azure() as con:
        result = pd.read_sql_table(table_name="sample", con=con, schema="test")

    assert_frame_equal(expected, result)


def test_create_category(file_dir="data"):
    file_dir = file_dir + "/category_1.csv"
    expected = pd.read_csv(file_dir)
    adf_client, run_response = df_to_azure(
        df=expected,
        tablename="category",
        schema="test",
        method="create",
        id_field="category_id",
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    with auth_azure() as con:
        result = pd.read_sql_table(table_name="category", con=con, schema="test")

    assert_frame_equal(expected, result)


# ---- UPSERT TESTS ----
def test_upsert_sample(file_dir="data"):
    file_dir = file_dir + "/sample_2.csv"
    adf_client, run_response = df_to_azure(
        df=pd.read_csv(file_dir),
        tablename="sample",
        schema="test",
        method="upsert",
        id_field="col_a",
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    expected = pd.DataFrame({
        "col_a": [1, 3, 4, 5, 6],
        "col_b": ["updated value", "test", "test", "new value", "also new"],
        "col_c": ["E", "Z", "A", "F", "H"]
    })

    with auth_azure() as con:
        result = pd.read_sql_table(table_name="sample", con=con, schema="test")

    assert_frame_equal(expected, result)


def test_upsert_category(file_dir="data"):
    file_dir = file_dir + "/category_2.csv"
    adf_client, run_response = df_to_azure(
        df=pd.read_csv(file_dir),
        tablename="category",
        schema="test",
        method="upsert",
        id_field="category_id",
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    expected = pd.DataFrame({
        "category_id": [1, 2, 3, 4, 5, 6],
        "category_name": ["Children Bicycles", "Comfort Bicycles", "Cruisers Bicycles", "Cyclocross Bicycles", "Electric Bikes", "Mountain Bikes"],
        "amount": [15000, 25000, 13000, 20000, 10000, 10000]
    })

    with auth_azure() as con:
        result = pd.read_sql_table(table_name="category", con=con, schema="test")

    assert_frame_equal(expected, result)


def test_upsert_id_field_multiple_columns():
    # create table in database first
    df = pd.read_csv("https://data.rivm.nl/covid-19/COVID-19_aantallen_gemeente_cumulatief.csv", sep=";")
    df_to_azure(
        df=df,
        tablename="covid_19",
        schema="test",
        method="create",
        id_field=["Date_of_report", "Municipality_code"]
    )

    # change data to test upsert
    df = df.groupby("Municipality_code").sample(100).reset_index(drop=True)
    df[["Total_reported", "Hospital_admission", "Deceased"]] = 999999
    adf_client, run_response = df_to_azure(
        df=df,
        tablename="covid_19",
        schema="test",
        method="upsert",
        id_field=["Date_of_report", "Municipality_code"]
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    # read data back from upserted table in SQL
    with auth_azure() as con:
        result = pd.read_sql_table(table_name="covid_19", con=con, schema="test")

    result = result[result["Total_reported"] == 999999].reset_index(drop=True)

    assert_frame_equal(df, result)


def test_run_multiple(file_dir="data"):

    df_dict = dict()
    for file in os.listdir(file_dir):
        if file.endswith(".csv"):
            df_dict[file.split(".csv")[0]] = pd.read_csv(os.path.join("data", file))

    dfs_to_azure(df_dict, schema="test", method="create")


if __name__ == "__main__":
    file_dir_run = "../data"
    # test_create_sample(file_dir_run)
    # test_upsert_sample(file_dir_run)
    # test_create_category(file_dir_run)
    # test_upsert_category(file_dir_run)
    # test_upsert_id_field_multiple_columns()
    # test_run_multiple()
