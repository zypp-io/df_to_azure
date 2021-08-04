import pytest
from numpy import nan
from pandas import DataFrame

from df_to_azure import df_to_azure


def test_wrong_method():
    """
    Not existing method
    """
    df = DataFrame()
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
            method="upsert",
            id_field=None,
            wait_till_finished=True,
        )
