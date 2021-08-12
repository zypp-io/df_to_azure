from pandas import DataFrame, Timedelta, date_range, read_sql_query, read_sql_table
from pandas._testing import assert_frame_equal
from sqlalchemy.types import Date

from df_to_azure import df_to_azure
from df_to_azure.db import auth_azure
from df_to_azure.tests import data

# #############################
# #### CREATE METHOD TESTS ####
# #############################


def test_create_sample():
    expected = data["sample_1"]
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


def test_create_category():
    expected = data["category_1"]
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


def test_dtype_given():
    dates = date_range(start="2021-01-01", periods=5, freq="D")
    df = DataFrame({"datetime_col": dates, "date_col": dates.date + Timedelta(value=7, unit="D")})

    df_to_azure(
        df=df,
        tablename="given_dtype",
        schema="test",
        wait_till_finished=True,
        pipeline_name="given_dtype",
        dtypes={"date_col": Date()},
    )

    expected = DataFrame(
        {
            "COLUMN_NAME": ["datetime_col", "date_col"],
            "DATA_TYPE": ["datetime", "date"],
            "CHARACTER_MAXIMUM_LENGTH": [None, None],
            "NUMERIC_PRECISION": [None, None],
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
        TABLE_NAME = 'given_dtype';
    """

    with auth_azure() as con:
        result = read_sql_query(query, con=con)

    assert_frame_equal(expected, result)
