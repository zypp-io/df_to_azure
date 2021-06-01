import pandas as pd
from numpy import dtype
from sqlalchemy.types import Boolean, DateTime, Float, Integer, String
import os
import logging
import tempfile
from df_to_azure.adf import create_blob_service_client
import df_to_azure.adf as adf
from df_to_azure.parse_settings import TableParameters
from df_to_azure.db import SqlUpsert, auth_azure, test_uniqueness_columns
from df_to_azure.functions import wait_until_pipeline_is_done


def table_list(df_dict: dict, schema: str, method: str, id_field: str, cwd: str) -> list:
    tables = []
    for name, df in df_dict.items():
        table = TableParameters(
            df=df, name=name, schema=schema, method=method, id_field=id_field, cwd=cwd
        )

        tables.append(table)

    return tables


def run_multiple(
    df_dict,
    schema,
    method="create",
    id_field=None,
    cwd=None,
    wait_till_finished=False,
    pipeline_name=False,
):

    tables = table_list(df_dict, schema, method, id_field, cwd)

    create = True if os.environ.get("create") == "True" else False
    if create:
        create_schema(tables[0])

        # azure components
        adf.create_resourcegroup()
        adf.create_datafactory()
        adf.create_blob_container()

        # linked services
        adf.create_linked_service_sql()
        adf.create_linked_service_blob()

    for table in tables:

        upload_dataset(table)
        adf.create_input_blob(table)
        adf.create_output_sql(table)

    # pipelines
    adf_client, run_response = adf.create_multiple_activity_pipeline(tables, p_name=pipeline_name)
    if wait_till_finished:
        wait_until_pipeline_is_done(adf_client, run_response)

    return adf_client, run_response


def run(
    df,
    tablename,
    schema,
    method="create",
    id_field=None,
    cwd=None,
    wait_till_finished=False,
    pipeline_name=None,
):
    table = TableParameters(
        df=df, name=tablename, schema=schema, method=method, id_field=id_field, cwd=cwd
    )
    create = True if os.environ.get("create") == "True" else False
    if create:
        create_schema(table)

        # azure components
        adf.create_resourcegroup()
        adf.create_datafactory()
        adf.create_blob_container()

        # linked services
        adf.create_linked_service_sql()
        adf.create_linked_service_blob()

    upload_dataset(table)
    adf.create_input_blob(table)
    adf.create_output_sql(table)

    # pipelines
    adf_client, run_response = adf.create_pipeline(table, pipeline_name)
    if wait_till_finished:
        wait_until_pipeline_is_done(adf_client, run_response)

    return adf_client, run_response


def upload_dataset(table):

    if len(table.df) == 0:
        return logging.info("no new records to upload.")

    if table.method == "create":
        create_schema(table)
        push_to_azure(table)
    if table.method == "upsert":
        # key columns have to be unique for upsert.
        test_uniqueness_columns(table.df, table.id_field)
        upsert = SqlUpsert(
            table_name=table.name,
            schema=table.schema,
            id_cols=table.id_field,
            columns=table.df.columns,
        )
        upsert.create_stored_procedure()
        table.schema = "staging"
        create_schema(table)
        push_to_azure(table)

    upload_to_blob(table)
    logging.info(f"Finished! exported {table.df.shape[0]} records.")


def push_to_azure(table):
    table.df = convert_timedelta_to_seconds(table.df)
    max_str = get_max_str_len(table.df)
    col_types = column_types(table.df)
    col_types.update(max_str)

    with auth_azure() as con:
        table.df.head(n=0).to_sql(
            name=table.name,
            con=con,
            if_exists="replace",
            index=False,
            schema=table.schema,
            dtype=col_types,
        )

    logging.info(f"created {table.df.shape[1]} columns in {table.schema}.{table.name}.")


def upload_to_blob(table):
    blob_client = create_blob_service_client()
    blob_client = blob_client.get_blob_client(
        container=os.environ.get("ls_blob_container_name"),
        blob=f"{table.name}/{table.name}",
    )
    with tempfile.TemporaryDirectory(suffix="_df_to_azure") as temp_dir:
        full_path_to_file = os.path.join(temp_dir, table.name + ".csv")
        table.df.to_csv(
            full_path_to_file, index=False, sep="^", quotechar='"', line_terminator="\n"
        )  # export file to staging

        logging.debug(f"start uploading blob {table.name}...")
        with open(full_path_to_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logging.debug(f"finished uploading blob {table.name}!")


def create_schema(table):
    query = f"""
    IF NOT EXISTS ( SELECT  *
                FROM    sys.schemas
                WHERE   name = N'{table.schema}' )
    EXEC('CREATE SCHEMA [{table.schema}]');
    """
    execute_stmt(query)


def execute_stmt(stmt):
    """
    :param stmt: SQL statement to be executed
    :return: executes the statment
    """
    with auth_azure() as con:
        t = con.begin()
        con.execute(stmt)
        t.commit()


def convert_timedelta_to_seconds(df: pd.DataFrame) -> pd.DataFrame:
    """
    We convert timedelta columns to seconds since this will error in SQL DB.
    If there are no timedelta columns, nothing will happen.

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame to convert timedelta columns to seconds

    Returns
    -------
    df: pd.DataFrame
        DataFrame where timedelta columns are converted to seconds.
    """
    td_cols = df.iloc[:1].select_dtypes("timedelta").columns

    if len(td_cols):
        df[td_cols] = df[td_cols].apply(lambda x: x.dt.total_seconds())

    return df


def column_types(df: pd.DataFrame, text_length: int = 255, decimal_precision: int = 2) -> dict:
    """
    Convert pandas / numpy dtypes to SQLAlchemy dtypes when writing data to database.

    Parameters
    ----------
    df: pd.DataFrame
        dataframe to write to SQL database
    text_length: int
        maximum length of text columns
    decimal_precision: int
        Decimal precision of float columns

    Returns
    -------
    col_types: dict
        Dictionary with mapping of pandas dtypes to SQLAlchemy types.
    """
    type_conversion = {
        dtype("O"): String(length=text_length),
        pd.StringDtype(): String(length=text_length),
        dtype("int64"): Integer(),
        dtype("int32"): Integer(),
        dtype("int16"): Integer(),
        dtype("int8"): Integer(),
        pd.Int64Dtype(): Integer(),
        dtype("float64"): Float(precision=decimal_precision),
        dtype("float32"): Float(precision=decimal_precision),
        dtype("float16"): Float(precision=decimal_precision),
        dtype("<M8[ns]"): DateTime(),
        dtype("bool"): Boolean(),
    }

    col_types = {
        col_name: type_conversion[col_type] for col_name, col_type in df.dtypes.to_dict().items()
    }

    return col_types


def get_max_str_len(df):
    df = df.select_dtypes("object")
    default_len = 255

    update_dict_len = {}
    if not df.empty:
        for col in df.columns:
            len_col = df[col].astype(str).str.len().max()
            if len_col > default_len:
                update_dict_len[col] = String(length=int(len_col))
            if len_col > 8000:
                update_dict_len[col] = String(length=None)

    return update_dict_len
