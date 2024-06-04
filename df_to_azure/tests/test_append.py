from numpy import nan
from pandas import DataFrame, concat, read_sql_table
from pandas._testing import assert_frame_equal

from df_to_azure import df_to_azure
from df_to_azure.db import auth_azure


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
