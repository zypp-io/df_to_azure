import logging

import pytest
from keyvault import secrets_to_environment
from numpy import array, nan
from pandas import DataFrame, Series, date_range, read_sql_query, read_sql_table
from pandas._testing import assert_frame_equal

from df_to_azure import df_to_azure
from df_to_azure.db import auth_azure
from df_to_azure.exceptions import DoubleColumnNamesError

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
secrets_to_environment(keyvault_name="df-to-azure")


# #######################
# #### GENERAL TESTS ####
# #######################


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
            "pd_Int32": Series([1, 2, 3], dtype="Int32"),
            "pd_Int16": Series([1, 2, 3], dtype="Int16"),
            "pd_Int8": Series([1, 2, 3], dtype="Int8"),
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
                "pd_Int32",
                "pd_Int16",
                "pd_Int8",
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
                "int",
                "int",
                "int",
                "numeric",
                "numeric",
                "datetime",
                "numeric",
                "bit",
            ],
            "CHARACTER_MAXIMUM_LENGTH": [255, 255, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan, nan],
            "NUMERIC_PRECISION": [nan, nan, 10, 10, 10, 10, 10, 10, 18, 18, nan, 18, nan],
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


def test_double_column_names():
    df_double_names = DataFrame({"A": [1, 2, 3], "B": [10, 20, 30], "C": ["X", "Y", "Z"]})
    df_double_names = df_double_names.rename(columns={"C": "A"})
    with pytest.raises(DoubleColumnNamesError):
        df_to_azure(
            df=df_double_names,
            tablename="double_column_names",
            schema="test",
            wait_till_finished=True,
        )
