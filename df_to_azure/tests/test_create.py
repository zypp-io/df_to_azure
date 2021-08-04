from pandas import read_sql_table
from pandas._testing import assert_frame_equal

from df_to_azure import df_to_azure
from df_to_azure.db import auth_azure
from df_to_azure.tests import data

# #############################
# #### CREATE METHOD TESTS ####
# #############################


def test_create_sample(file_dir="data"):
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


def test_create_category(file_dir="data"):
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
