import logging
import sys
from typing import Union

from numpy import dtype
from pandas import BooleanDtype, DataFrame, Int64Dtype, StringDtype
from sqlalchemy.types import BigInteger, Boolean, DateTime, Integer, Numeric, String

from df_to_azure.adf import ADF
from df_to_azure.db import SqlUpsert, auth_azure, execute_stmt, test_uniqueness_columns
from df_to_azure.utils import wait_until_pipeline_is_done


def df_to_azure(
    df,
    tablename,
    schema,
    method="create",
    id_field=None,
    wait_till_finished=False,
    pipeline_name=None,
    text_length=255,
    decimal_precision=2,
    create=False,
):
    adf_client, run_response = DfToAzure(
        df=df,
        tablename=tablename,
        schema=schema,
        method=method,
        id_field=id_field,
        wait_till_finished=wait_till_finished,
        pipeline_name=pipeline_name,
        text_length=text_length,
        decimal_precision=decimal_precision,
        create=create,
    ).run()

    return adf_client, run_response


class DfToAzure(ADF):
    def __init__(
        self,
        df: DataFrame,
        tablename: str,
        schema: str,
        method: str = "create",
        id_field: Union[str, list] = None,
        pipeline_name: str = None,
        wait_till_finished: bool = False,
        text_length: int = 255,
        decimal_precision: int = 2,
        create: bool = False,
    ):
        super().__init__(
            df=df,
            tablename=tablename,
            schema=schema,
            method=method,
            id_field=id_field,
            pipeline_name=pipeline_name,
            create=create,
        )
        self.wait_till_finished = wait_till_finished
        self.text_length = text_length
        self.decimal_precision = decimal_precision

    def run(self):
        if self.create:

            # azure components
            self.create_resourcegroup()
            self.create_datafactory()
            self.create_blob_container()

            # linked services
            self.create_linked_service_sql()
            self.create_linked_service_blob()

        self.upload_dataset()
        self.create_input_blob()
        self.create_output_sql()

        # pipelines
        run_response = self.create_pipeline(pipeline_name=self.pipeline_name)
        if self.wait_till_finished:
            wait_until_pipeline_is_done(self.adf_client, run_response)

        return self.adf_client, run_response

    def upload_dataset(self):

        if self.df.empty:
            logging.info("Data empty, no new records to upload.")
            sys.exit(1)

        if self.method == "create":
            self.create_schema()
            self.push_to_azure()
        if self.method == "upsert":
            # key columns have to be unique for upsert.
            test_uniqueness_columns(self.df, self.id_field)
            upsert = SqlUpsert(
                table_name=self.table_name,
                schema=self.schema,
                id_cols=self.id_field,
                columns=self.df.columns,
            )
            upsert.create_stored_procedure()
            self.schema = "staging"
            self.create_schema()
            self.push_to_azure()

        self.upload_to_blob()
        logging.info(f"Finished exporting {self.df.shape[0]} records to Azure Blob Storage.")

    def push_to_azure(self):
        self.convert_timedelta_to_seconds()
        max_str = self.get_max_str_len()
        bigint = self.check_for_bigint()
        col_types = self.column_types()
        for u in [max_str, bigint]:
            col_types.update(u)

        with auth_azure() as con:
            self.df.head(n=0).to_sql(
                name=self.table_name,
                con=con,
                if_exists="replace",
                index=False,
                schema=self.schema,
                dtype=col_types,
            )

        logging.info(f"Created {self.df.shape[1]} columns in {self.schema}.{self.table_name}.")

    def upload_to_blob(self):
        blob_client = self.blob_service_client()
        blob_client = blob_client.get_blob_client(
            container="dftoazure",
            blob=f"{self.table_name}/{self.table_name}.csv",
        )

        data = self.df.to_csv(index=False, sep="^", quotechar='"', line_terminator="\n")
        blob_client.upload_blob(data, overwrite=True)

    def create_schema(self):
        query = f"""
        IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = N'{self.schema}')
        EXEC('CREATE SCHEMA [{self.schema}]');
        """
        execute_stmt(query)

    def convert_timedelta_to_seconds(self):
        """
        We convert timedelta columns to seconds since this will error in SQL DB.
        If there are no timedelta columns, nothing will happen.

        Parameters
        ----------
        self.df: DataFrame
            DataFrame to convert timedelta columns to seconds

        """
        td_cols = self.df.iloc[:1].select_dtypes("timedelta").columns

        if len(td_cols):
            self.df[td_cols] = self.df[td_cols].apply(lambda x: x.dt.total_seconds())

    def column_types(self) -> dict:
        """
        Convert pandas / numpy dtypes to SQLAlchemy dtypes when writing data to database.

        Returns
        -------
        col_types: dict
            Dictionary with mapping of pandas dtypes to SQLAlchemy types.
        """
        string = String(length=self.text_length)
        numeric = Numeric(precision=18, scale=self.decimal_precision)
        type_conversion = {
            dtype("O"): string,
            StringDtype(): string,
            dtype("int64"): Integer(),
            dtype("int32"): Integer(),
            dtype("int16"): Integer(),
            dtype("int8"): Integer(),
            Int64Dtype(): Integer(),
            dtype("float64"): numeric,
            dtype("float32"): numeric,
            dtype("float16"): numeric,
            dtype("<M8[ns]"): DateTime(),
            dtype("bool"): Boolean(),
            BooleanDtype(): Boolean(),
        }

        col_types = {col_name: type_conversion[col_type] for col_name, col_type in self.df.dtypes.to_dict().items()}

        return col_types

    def get_max_str_len(self):
        df = self.df.select_dtypes("object")
        default_len = self.text_length

        update_dict_len = {}
        if not df.empty:
            for col in df.columns:
                len_col = df[col].astype(str).str.len().max()
                if default_len < len_col < 8000:
                    update_dict_len[col] = String(length=int(len_col))
                elif len_col > 8000:
                    update_dict_len[col] = String(length=None)
                else:
                    update_dict_len[col] = String(length=default_len)

        return update_dict_len

    def check_for_bigint(self):
        ints = self.df.select_dtypes(include=["int8", "int16", "int32", "int64"])
        if ints.empty:
            return {}

        # These are the highest and lowest number
        # which can be stored in an integer column in SQL.
        # For numbers out of these bounds, we convert to bigint
        check = (ints.lt(-2147483648) | ints.gt(2147483647)).any()
        cols_bigint = check[check].index.tolist()

        update_dict_bigint = {col: BigInteger() for col in cols_bigint}

        return update_dict_bigint
