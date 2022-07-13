import os

import pytest
from pandas import DataFrame, read_csv, read_sql_table
from pandas._testing import assert_frame_equal

from df_to_azure import df_to_azure
from df_to_azure.db import auth_azure
from df_to_azure.exceptions import UpsertError

from ..tests import data


# #############################
# #### UPSERT METHOD TESTS ####
# #############################
def test_upsert_sample(file_dir="data"):
    df1 = data["sample_1"]
    df_to_azure(
        df=df1,
        tablename="sample",
        schema="test",
        method="create",
        wait_till_finished=True,
    )

    df2 = data["sample_2"]
    df_to_azure(
        df=df2,
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
    df = data["category_2"]
    df_to_azure(
        df=df,
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
    df1 = data["employee_1"]
    df_to_azure(
        df=df1,
        tablename="employee_1",
        schema="test",
        method="create",
        id_field=["employee_id", "week_nr"],
        wait_till_finished=True,
    )

    # upsert data
    df2 = data["employee_2"]
    df_to_azure(
        df=df2,
        tablename="employee_1",
        schema="test",
        method="upsert",
        id_field=["employee_id", "week_nr"],
        wait_till_finished=True,
    )

    # read data back from upserted table in SQL
    with auth_azure() as con:
        result = read_sql_table(table_name="employee_1", con=con, schema="test")

    assert_frame_equal(df2, result)


def test_duplicate_keys_upsert(file_dir="data"):
    df1 = data["employee_duplicate_keys_1"]
    df_to_azure(
        df=df1,
        tablename="employee_duplicate_keys",
        schema="test",
        method="create",
        id_field=["employee_id", "week_nr"],
        wait_till_finished=True,
    )

    # upsert data
    df2 = data["employee_duplicate_keys_2"]
    with pytest.raises(Exception):
        df_to_azure(
            df=df2,
            tablename="employee_duplicate_keys",
            schema="test",
            method="upsert",
            id_field=["employee_id", "week_nr"],
            wait_till_finished=True,
        )


def test_upsert_spaces_column_name(file_dir="data"):
    df = data["sample_1"]
    df = df.rename(columns={"col_a": "col a", "col_b": "col b"})
    df_to_azure(
        df=df,
        tablename="sample_spaces_column_name",
        schema="test",
        method="create",
        wait_till_finished=True,
    )

    df = read_csv(os.path.join(file_dir, "sample_3.csv"))
    df_to_azure(
        df=df,
        tablename="sample_spaces_column_name",
        schema="test",
        method="upsert",
        id_field="col a",
        wait_till_finished=True,
    )

    expected = DataFrame(
        {
            "col a": [1, 3, 4, 5, 6],
            "col b": ["updated value", "test", "test", "new value", "also new"],
            "col_c": ["E", "Z", "A", "F", "H"],
        }
    )

    with auth_azure() as con:
        result = read_sql_table(table_name="sample_spaces_column_name", con=con, schema="test")

    assert_frame_equal(expected, result)


@pytest.mark.parametrize("clean_staging", [True, False])
def test_upsert_same_tablename(clean_staging):
    """
    Test upsert with the same table name but different columns. Work with clean_staging=True and fails when staging
    is not cleaned.
    """
    df1 = data["sample_1"]
    df_to_azure(
        df=df1,
        tablename="sample",
        schema="test",
        method="create",
        wait_till_finished=True,
    )

    df2 = data["sample_2"]
    df_to_azure(
        df=df2,
        tablename="sample",
        schema="test",
        method="upsert",
        id_field="col_a",
        wait_till_finished=True,
        clean_staging=clean_staging,
    )

    new_col_names = {"col_a": "test_a", "col_b": "test_b", "col_c": "test_c"}
    df1 = df1.rename(columns=new_col_names)
    df2 = df2.rename(columns=new_col_names)
    df_to_azure(
        df=df1,
        tablename="sample",
        schema="test",
        method="create",
        wait_till_finished=True,
    )
    if clean_staging:
        df_to_azure(
            df=df2,
            tablename="sample",
            schema="test",
            method="upsert",
            id_field="test_a",
            wait_till_finished=True,
        )
        expected = DataFrame(
            {
                "test_a": [1, 3, 4, 5, 6],
                "test_b": ["updated value", "test", "test", "new value", "also new"],
                "test_c": ["E", "Z", "A", "F", "H"],
            }
        )

        with auth_azure() as con:
            result = read_sql_table(table_name="sample", con=con, schema="test")

        assert_frame_equal(expected, result)
    else:
        with pytest.raises(UpsertError):
            df_to_azure(
                df=df2,
                tablename="sample",
                schema="test",
                method="upsert",
                id_field="test_a",
                wait_till_finished=True,
            )
