import pytest
from pandas import DataFrame, read_sql_table
from pandas._testing import assert_frame_equal

from df_to_azure import df_to_azure
from df_to_azure.db import auth_azure, execute_stmt
from df_to_azure.exceptions import UpsertError

SCHEMA = "test"


def reset_identity_table(table_name: str) -> None:
    execute_stmt(
        f"""
IF OBJECT_ID('{SCHEMA}.{table_name}', 'U') IS NOT NULL
    DROP TABLE [{SCHEMA}].[{table_name}];
CREATE TABLE [{SCHEMA}].[{table_name}](
    [id] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [value] NVARCHAR(255) NOT NULL
);
"""
    )


def insert_values(table_name: str, values: list[str]) -> None:
    for value in values:
        escaped = value.replace("'", "''")
        execute_stmt(f"INSERT INTO [{SCHEMA}].[{table_name}] ([value]) VALUES ('{escaped}')")


def test_upsert_identity_column_requires_preserve():
    table_name = "identity_no_preserve"
    reset_identity_table(table_name)
    insert_values(table_name, ["original value"])

    df = DataFrame({"id": [1], "value": ["updated value"]})

    with pytest.raises(UpsertError) as excinfo:
        df_to_azure(
            df=df,
            tablename=table_name,
            schema=SCHEMA,
            method="upsert",
            id_field="id",
            wait_till_finished=True,
        )

    assert "Column 'id' is an auto-increment (IDENTITY) column" in str(excinfo.value)


def test_upsert_identity_column_with_preserve_identity():
    table_name = "identity_with_preserve"
    reset_identity_table(table_name)
    insert_values(table_name, ["original value"])

    df = DataFrame({"id": [1, 10], "value": ["updated value", "migrated value"]})

    df_to_azure(
        df=df,
        tablename=table_name,
        schema=SCHEMA,
        method="upsert",
        id_field="id",
        wait_till_finished=True,
        preserve_identity=True,
    )

    with auth_azure() as con:
        result = read_sql_table(table_name=table_name, con=con, schema=SCHEMA).sort_values("id")

    expected = DataFrame({"id": [1, 10], "value": ["updated value", "migrated value"]})
    assert_frame_equal(expected, result.reset_index(drop=True))
