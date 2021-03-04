import os
import logging
import time
import pytest
from pandas import Series, DataFrame, read_csv, read_sql_table, date_range, read_sql_query, concat
from numpy import array, nan
from pandas._testing import assert_frame_equal
from dotenv import load_dotenv

from df_to_azure import df_to_azure, dfs_to_azure
from df_to_azure.db import auth_azure
from df_to_azure.exceptions import PipelineRunError

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
load_dotenv(verbose=True, override=True)
print(os.environ.get("client_id"))
"""
This is the testing suite for df_to_azure. In general the following steps will be done per test:

1. Read a sample csv file from repo or the web
2. Write file to DB with df_to_azure, this can be create or upsert.
3. Use `wait_till_pipeline_is_done` function to wait for ADF pipeline
     to be Succeeded before continuing.
4. Read data back from DB and test if we got expected output
     with pandas._testing.assert_frame_equal.

NOTE: To keep the testing lightweight, we don't import whole modules but just the methods we need.
        like DataFrame from pandas.
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


# #############################
# #### CREATE METHOD TESTS ####
# #############################
def test_create_sample(file_dir="data"):
    file_dir = os.path.join(file_dir, "sample_1.csv")
    expected = read_csv(file_dir)
    adf_client, run_response = df_to_azure(
        df=expected,
        tablename="sample",
        schema="test",
        method="create",
        id_field="col_a",
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    with auth_azure() as con:
        result = read_sql_table(table_name="sample", con=con, schema="test")

    assert_frame_equal(expected, result)


def test_create_category(file_dir="data"):
    file_dir = os.path.join(file_dir, "category_1.csv")
    expected = read_csv(file_dir)
    adf_client, run_response = df_to_azure(
        df=expected,
        tablename="category",
        schema="test",
        method="create",
        id_field="category_id",
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    with auth_azure() as con:
        result = read_sql_table(table_name="category", con=con, schema="test")

    assert_frame_equal(expected, result)


# #############################
# #### UPSERT METHOD TESTS ####
# #############################
def test_upsert_sample(file_dir="data"):
    file_dir = os.path.join(file_dir, "sample_2.csv")
    adf_client, run_response = df_to_azure(
        df=read_csv(file_dir),
        tablename="sample",
        schema="test",
        method="upsert",
        id_field="col_a",
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    expected = DataFrame(
        {
            "col_a": [1, 3, 4, 5, 6],
            "col_b": ["updated value", "test", "test", "new value", "also new"],
            "col_c": ["E", "Z", "A", "F", "H"],
        }
    )

    with auth_azure() as con:
        result = read_sql_table(table_name="sample", con=con, schema="test")

    assert_frame_equal(expected, result)


def test_upsert_category(file_dir="data"):
    file_dir = os.path.join(file_dir, "category_2.csv")
    adf_client, run_response = df_to_azure(
        df=read_csv(file_dir),
        tablename="category",
        schema="test",
        method="upsert",
        id_field="category_id",
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    expected = DataFrame(
        {
            "category_id": [1, 2, 3, 4, 5, 6],
            "category_name": [
                "Children Bicycles",
                "Comfort Bicycles",
                "Cruisers Bicycles",
                "Cyclocross Bicycles",
                "Electric Bikes",
                "Mountain Bikes",
            ],
            "amount": [15000.00, 25000.00, 13000.00, 20000.00, 10000.00, 10000.00],
        }
    )

    with auth_azure() as con:
        result = read_sql_table(table_name="category", con=con, schema="test")

    assert_frame_equal(expected, result)


def test_upsert_id_field_multiple_columns(file_dir="data"):
    # create table in database first
    file_dir_1 = os.path.join(file_dir, "employee_1.csv")
    df = read_csv(file_dir_1)
    df_to_azure(
        df=df,
        tablename="employee_1",
        schema="test",
        method="create",
        id_field=["employee_id", "week_nr"],
    )

    # upsert data
    file_dir_2 = os.path.join(file_dir, "employee_2.csv")
    df = read_csv(file_dir_2)
    adf_client, run_response = df_to_azure(
        df=df,
        tablename="employee_1",
        schema="test",
        method="upsert",
        id_field=["employee_id", "week_nr"],
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    # read data back from upserted table in SQL
    with auth_azure() as con:
        result = read_sql_table(table_name="employee_1", con=con, schema="test")

    assert_frame_equal(df, result)


def test_duplicate_keys_upsert(file_dir="data"):
    file_dir_1 = os.path.join(file_dir, "employee_duplicate_keys_1.csv")
    df = read_csv(file_dir_1)
    df_to_azure(
        df=df,
        tablename="employee_duplicate_keys",
        schema="test",
        method="create",
        id_field=["employee_id", "week_nr"],
    )

    # upsert data
    file_dir_2 = os.path.join(file_dir, "employee_duplicate_keys_2.csv")
    df = read_csv(file_dir_2)
    with pytest.raises(Exception):
        df_to_azure(
            df=df,
            tablename="employee_duplicate_keys",
            schema="test",
            method="upsert",
            id_field=["employee_id", "week_nr"],
        )


# #############################
# #### APPEND METHOD TESTS ####
# #############################
def test_append():

    df = DataFrame({"A": [1, 2, 3], "B": list("abc"), "C": [4.0, 5.0, nan]})

    # 1. we create a new dataframe
    adf_client, run_response = df_to_azure(
        df=df, tablename="append", schema="test", method="create"
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    # 2. we append the same data
    adf_client, run_response = df_to_azure(
        df=df, tablename="append", schema="test", method="append"
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    # 3. we test if the data is what we expect
    with auth_azure() as con:
        result = read_sql_table(table_name="append", con=con, schema="test")

    expected = concat([df, df], ignore_index=True)

    assert_frame_equal(result, expected)


# #######################
# #### GENERAL TESTS ####
# #######################
def test_run_multiple(file_dir="data"):

    df_dict = dict()
    for file in os.listdir(file_dir):
        if file.endswith(".csv"):
            df_dict[file.split(".csv")[0]] = read_csv(os.path.join(file_dir, file))

    dfs_to_azure(df_dict, schema="test", method="create")


def test_mapping_column_types():
    """
    Test if the mapping of the pandas column types to SQL column types goes correctly.
    """

    dr1 = date_range("2020-01-01", periods=3, freq="D")
    dr2 = date_range("2019-06-23", periods=3, freq="D")
    df = DataFrame(
        {
            "String": list("abc"),
            "pd_String": Series(list("abc"), dtype="string"),
            "Int": [1, 2, 3],
            "Int16": array([1, 2, 3], dtype="int16"),
            "pd_Int64": Series([1, 2, 3], dtype="Int64"),
            "Float": [4.0, 5.0, 6.0],
            "Float32": array([4, 4, 6], dtype="float32"),
            "Date": dr1,
            "Timedelta": dr1 - dr2,
            "Bool": [True, False, True],
        }
    )
    adf_client, run_response = df_to_azure(
        df, tablename="test_df_to_azure", schema="test", method="create"
    )
    wait_till_pipeline_is_done(adf_client, run_response)

    expected = DataFrame(
        {
            "COLUMN_NAME": [
                "String",
                "pd_String",
                "Int",
                "Int16",
                "pd_Int64",
                "Float",
                "Float32",
                "Date",
                "Timedelta",
                "Bool",
            ],
            "DATA_TYPE": [
                "varchar",
                "varchar",
                "int",
                "int",
                "int",
                "real",
                "real",
                "datetime",
                "real",
                "bit",
            ],
            "CHARACTER_MAXIMUM_LENGTH": [255, 255, nan, nan, nan, nan, nan, nan, nan, nan],
            "NUMERIC_PRECISION": [nan, nan, 10, 10, 10, 24, 24, nan, 24, nan],
        }
    )

    query = """
    SELECT
        COLUMN_NAME,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION
    FROM
        INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_NAME = 'test_df_to_azure';
    """

    with auth_azure() as con:
        result = read_sql_query(query, con=con)

    assert_frame_equal(expected, result)


def test_wrong_method():
    """
    Not existing method
    """
    df = DataFrame({"A": [1, 2, 3], "B": list("abc"), "C": [4.0, 5.0, nan]})
    with pytest.raises(ValueError):
        df_to_azure(df=df, tablename="wrong_method", schema="test", method="insert")


def test_upsert_no_id_field():
    """
    When upsert method is used, id_field has to be given
    """
    df = DataFrame({"A": [1, 2, 3], "B": list("abc"), "C": [4.0, 5.0, nan]})
    with pytest.raises(ValueError):
        df_to_azure(df=df, tablename="wrong_method", schema="test", method="insert")


def test_long_string():
    """
    When upsert method is used, id_field has to be given
    """
    df = DataFrame({"A": ["1" * 10000, "2", "3"]})
    df_to_azure(df=df, tablename="long_string", schema="test", method="create")


# --- CLEAN UP ----
def test_clean_up_db():
    tables_dict = {
        "covid": ["covid_19"],
        "staging": ["category", "employee_1", "employee_2", "sample"],
        "test": [
            "category",
            "category_1",
            "category_2",
            "employee_1",
            "employee_2",
            "employee_duplicate_keys",
            "employee_duplicate_keys_1",
            "employee_duplicate_keys_2",
            "sample",
            "sample_1",
            "sample_2",
            "test_df_to_azure",
            "wrong_method",
            "long_string",
        ],
    }

    with auth_azure() as con:
        with con.begin():
            for schema, tables in tables_dict.items():
                for table in tables:
                    query = f"DROP TABLE IF EXISTS {schema}.{table};"
                    con.execute(query)


if __name__ == "__main__":
    file_dir_run = "../data"
    test_create_sample(file_dir_run)
    # test_upsert_sample(file_dir_run)
    # test_create_category(file_dir_run)
    # test_upsert_category(file_dir_run)
    # test_upsert_id_field_multiple_columns(file_dir_run)
    # test_run_multiple(file_dir_run)
    # test_clean_up_db()
    # test_append()
    # test_wrong_method()
    # test_upsert_no_id_field()
    # test_clean_up_db()
    # test_long_string()
