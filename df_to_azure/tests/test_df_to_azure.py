import os
import logging
import pytest
from pandas import Series, DataFrame, read_csv, read_sql_table, date_range, read_sql_query, concat
from numpy import array, nan
from pandas._testing import assert_frame_equal
from keyvault import secrets_to_environment

from df_to_azure import df_to_azure, dfs_to_azure
from df_to_azure.db import auth_azure


logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
secrets_to_environment(keyvault_name="df-to-azure")

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

# #############################
# #### CREATE METHOD TESTS ####
# #############################


def test_create_sample(file_dir="data"):
    file_dir = os.path.join(file_dir, "sample_1.csv")
    expected = read_csv(file_dir)
    df_to_azure(
        df=expected,
        tablename="sample",
        schema="test",
        method="create",
        id_field="col_a",
        wait_till_finished=True,
    )

    with auth_azure() as con:
        result = read_sql_table(table_name="sample", con=con, schema="test")

    assert_frame_equal(expected, result)


def test_create_category(file_dir="data"):
    file_dir = os.path.join(file_dir, "category_1.csv")
    expected = read_csv(file_dir)
    df_to_azure(
        df=expected,
        tablename="category",
        schema="test",
        method="create",
        id_field="category_id",
        wait_till_finished=True,
    )

    with auth_azure() as con:
        result = read_sql_table(table_name="category", con=con, schema="test")

    assert_frame_equal(expected, result)


# #############################
# #### UPSERT METHOD TESTS ####
# #############################
def test_upsert_sample(file_dir="data"):
    file_dir = os.path.join(file_dir, "sample_2.csv")
    df_to_azure(
        df=read_csv(file_dir),
        tablename="sample",
        schema="test",
        method="upsert",
        id_field="col_a",
        wait_till_finished=True,
    )

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
    df_to_azure(
        df=read_csv(file_dir),
        tablename="category",
        schema="test",
        method="upsert",
        id_field="category_id",
        wait_till_finished=True,
    )

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
        wait_till_finished=True,
    )

    # upsert data
    file_dir_2 = os.path.join(file_dir, "employee_2.csv")
    df = read_csv(file_dir_2)
    df_to_azure(
        df=df,
        tablename="employee_1",
        schema="test",
        method="upsert",
        id_field=["employee_id", "week_nr"],
        wait_till_finished=True,
    )

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
        wait_till_finished=True,
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
            wait_till_finished=True,
        )


# #############################
# #### APPEND METHOD TESTS ####
# #############################
def test_append():

    df = DataFrame({"A": [1, 2, 3], "B": list("abc"), "C": [4.0, 5.0, nan]})

    # 1. we create a new dataframe
    df_to_azure(
        df=df,
        tablename="append",
        schema="test",
        method="create",
        wait_till_finished=True,
    )

    # 2. we append the same data
    df_to_azure(
        df=df,
        tablename="append",
        schema="test",
        method="append",
        wait_till_finished=True,
    )

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

    dfs_to_azure(
        df_dict,
        schema="test",
        method="create",
        wait_till_finished=True,
    )


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
            "Float": [4.52, 5.28, 6.71],
            "Float32": array([4.52, 5.28, 6.71], dtype="float32"),
            "Date": dr1,
            "Timedelta": dr1 - dr2,
            "Bool": [True, False, True],
        }
    )
    df_to_azure(
        df,
        tablename="test_df_to_azure",
        schema="test",
        method="create",
        wait_till_finished=True,
        pipeline_name="test_column_types",
    )

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
                "numeric",
                "numeric",
                "datetime",
                "numeric",
                "bit",
            ],
            "CHARACTER_MAXIMUM_LENGTH": [255, 255, nan, nan, nan, nan, nan, nan, nan, nan],
            "NUMERIC_PRECISION": [nan, nan, 10, 10, 10, 18, 18, nan, 18, nan],
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
        df_to_azure(
            df=df,
            tablename="wrong_method",
            schema="test",
            method="insert",
            wait_till_finished=True,
        )


def test_upsert_no_id_field():
    """
    When upsert method is used, id_field has to be given
    """
    df = DataFrame({"A": [1, 2, 3], "B": list("abc"), "C": [4.0, 5.0, nan]})
    with pytest.raises(ValueError):
        df_to_azure(
            df=df,
            tablename="wrong_method",
            schema="test",
            method="insert",
            wait_till_finished=True,
        )


def test_long_string():
    """
    Test if long string is set correctly
    """
    df = DataFrame({"A": ["1" * 10000, "2", "3"]})
    df_to_azure(
        df=df,
        tablename="long_string",
        schema="test",
        method="create",
        wait_till_finished=True,
    )


def test_quote_char():
    """
    Check if quote char is used correctly when line seperator is in text column
    """

    df = DataFrame({"A": ["text1", "text2", "text3 \n with line 'seperator' \n test"]})

    df_to_azure(
        df=df,
        tablename="quote_char",
        schema="test",
        method="create",
        wait_till_finished=True,
    )

    with auth_azure() as con:
        result = read_sql_table(table_name="quote_char", con=con, schema="test")

    # check if amount of rows is equal
    assert df.shape[0] == result.shape[0]


def test_pipeline_name():
    """
    Test the argument pipeline_name
    """
    df = DataFrame({"A": [1, 2, 3], "B": list("abc"), "C": [4.0, 5.0, nan]})
    df_to_azure(
        df=df,
        tablename="pipeline_name",
        schema="test",
        method="create",
        wait_till_finished=True,
        pipeline_name="test_pipeline_name",
    )


def test_empty_dataframe():
    df = DataFrame()

    with pytest.raises(SystemExit):
        df_to_azure(
            df=df,
            tablename="empty_dataframe",
            schema="test",
            method="create",
            wait_till_finished=True,
        )


def test_convert_bigint():
    df = DataFrame({"A": [1, 2, -2147483649], "B": [10, 20, 30]})

    df_to_azure(df=df, tablename="bigint", schema="test", wait_till_finished=True)

    query = """
    SELECT
        COLUMN_NAME,
        DATA_TYPE
    FROM
        INFORMATION_SCHEMA.COLUMNS
    WHERE
        TABLE_NAME = 'bigint';
    """

    with auth_azure() as con:
        result = read_sql_query(query, con=con)

    expected = DataFrame({"COLUMN_NAME": ["A", "B"], "DATA_TYPE": ["bigint", "int"]})
    assert_frame_equal(result, expected)


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
            "quote_char",
            "append",
            "pipeline_name",
            "bigint",
            "bigint_convert",
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
    # test_create_sample(file_dir_run)
    # test_upsert_sample(file_dir_run)
    # test_create_category(file_dir_run)
    # test_upsert_category(file_dir_run)
    # test_upsert_id_field_multiple_columns(file_dir_run)
    # test_mapping_column_types()
    # test_run_multiple(file_dir_run)
    # test_append()
    # test_wrong_method()
    # test_upsert_no_id_field()
    # test_clean_up_db()
    # test_long_string()
    # test_quote_char()
    # test_pipeline_name()
    test_empty_dataframe()
    # test_convert_bigint()

    # RUN AS LAST FUNCTION
    test_clean_up_db()
