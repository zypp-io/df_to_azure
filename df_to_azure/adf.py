import logging
import os
from re import sub
from typing import Union

from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import (
    ActivityDependency,
    AzureBlobDataset,
    AzureBlobStorageLinkedService,
    AzureSqlDatabaseAuthenticationType,
    AzureSqlDatabaseLinkedService,
    AzureSqlTableDataset,
    AzureStorageAuthenticationType,
    BlobSource,
    CopyActivity,
    CredentialReference,
    DatasetReference,
    DatasetResource,
    DependencyCondition,
    Factory,
    LinkedServiceReference,
    LinkedServiceResource,
    ParquetFormat,
    PipelineResource,
    SqlServerStoredProcedureActivity,
    SqlSink,
)
from azure.mgmt.resource import ResourceManagementClient
from pandas import DataFrame

from df_to_azure.auth import (
    create_blob_service_client,
    create_default_credential,
)
from df_to_azure.exceptions import EnvVariableNotSetError
from df_to_azure.settings import TableParameters
from df_to_azure.utils import print_item


class ADF(TableParameters):
    def __init__(
        self,
        df: DataFrame,
        tablename: str,
        schema: str,
        method: str = "create",
        id_field: Union[str, list] = None,
        pipeline_name: str = None,
        create: bool = False,
    ):
        super().__init__(df, tablename, schema, method, id_field)
        self.credentials = self.create_credentials()
        self.adf_client = self.adf_client()
        self.pipeline_name = pipeline_name
        self.ls_blob_account_name = os.environ.get("ls_blob_account_name")
        self.rg_name = os.environ.get("rg_name")
        self.df_name = os.environ.get("df_name")
        self.ls_sql_name = "server={} database={}".format(
            sub("[<>*#.%&:\\\\+?/]", "-", os.environ.get("SQL_SERVER")),
            sub("[<>*#.%&:\\\\+?/]", "-", os.environ.get("SQL_DB")),
        )
        self.ls_blob_name = f"accountname={os.environ.get('ls_blob_account_name')}"
        self.create = create

    @staticmethod
    def check_env_variables():
        """
        Check if the required environment variables are set.

        Returns
        -------

        """
        required_env_vars = {
            "SQL_DB",
            "SQL_SERVER",
            "df_name",
            "ls_blob_account_name",
            "rg_location",
            "rg_name",
            "subscription_id",
        }

        not_set = [env for env in required_env_vars if os.environ.get(env) is None]

        if not_set:
            raise EnvVariableNotSetError(f"The following required variable(s) are not set: {', '.join(not_set)}")

    @staticmethod
    def create_credentials():
        return create_default_credential()

    def adf_client(self):
        adf_client = DataFactoryManagementClient(self.credentials, os.environ.get("subscription_id"))

        return adf_client

    def resource_client(self):
        resource_client = ResourceManagementClient(self.credentials, os.environ.get("subscription_id"))

        return resource_client

    def create_resourcegroup(self):
        rg_params = {"location": os.environ.get("rg_location")}
        rg = self.resource_client().resource_groups.create_or_update(self.rg_name, rg_params)
        print_item(rg)

    def create_datafactory(self):
        df_resource = Factory(location=os.environ.get("rg_location"))
        df = self.adf_client.factories.create_or_update(self.rg_name, self.df_name, df_resource)
        print_item(df)

        while df.provisioning_state != "Succeeded":
            df = self.adf_client.factories.get(self.rg_name, self.df_name)
            logging.info(f"Datafactory {os.environ.get('df_name')} created!")

    def blob_service_client(self):
        return create_blob_service_client(self.ls_blob_account_name, credential=self.credentials, timeout=600)

    def create_blob_container(self):
        try:
            self.blob_service_client().create_container("dftoazure")
        except Exception as e:
            logging.info(e)

    def create_linked_service_sql(self):
        credential_name = os.environ.get("DF_TO_AZURE_ADF_CREDENTIAL_NAME")
        if credential_name:
            linked_service = AzureSqlDatabaseLinkedService(
                server=os.environ.get("SQL_SERVER"),
                database=os.environ.get("SQL_DB"),
                encrypt="mandatory",
                trust_server_certificate=False,
                authentication_type=AzureSqlDatabaseAuthenticationType.USER_ASSIGNED_MANAGED_IDENTITY,
                credential=CredentialReference(
                    type="CredentialReference",
                    reference_name=credential_name,
                ),
            )
        else:
            linked_service = AzureSqlDatabaseLinkedService(
                server=os.environ.get("SQL_SERVER"),
                database=os.environ.get("SQL_DB"),
                encrypt="mandatory",
                trust_server_certificate=False,
                authentication_type=AzureSqlDatabaseAuthenticationType.SYSTEM_ASSIGNED_MANAGED_IDENTITY,
            )

        ls_azure_sql = LinkedServiceResource(properties=linked_service)

        self.adf_client.linked_services.create_or_update(
            self.rg_name,
            self.df_name,
            self.ls_sql_name,
            ls_azure_sql,
        )

    def create_linked_service_blob(self):
        linked_service = AzureBlobStorageLinkedService(
            service_endpoint=f"https://{os.environ.get('ls_blob_account_name')}.blob.core.windows.net/",
            account_kind=os.environ.get("DF_TO_AZURE_STORAGE_ACCOUNT_KIND", "StorageV2"),
            authentication_type=AzureStorageAuthenticationType.MSI,
        )
        ls_azure_blob = LinkedServiceResource(properties=linked_service)
        self.adf_client.linked_services.create_or_update(
            self.rg_name,
            self.df_name,
            self.ls_blob_name,
            ls_azure_blob,
        )

    def create_input_blob(self):
        ds_name = f"BLOB_dftoazure_{self.table_name}"

        ds_ls = LinkedServiceReference(type="LinkedServiceReference", reference_name=self.ls_blob_name)
        ds_azure_blob = AzureBlobDataset(
            linked_service_name=ds_ls,
            folder_path=f"dftoazure/{self.table_name}",
            file_name=f"{self.table_name}.parquet",
            format=ParquetFormat(),
        )
        ds_azure_blob = DatasetResource(properties=ds_azure_blob)
        self.adf_client.datasets.create_or_update(self.rg_name, self.df_name, ds_name, ds_azure_blob)

    def create_output_sql(self):
        ds_name = f"SQL_dftoazure_{self.table_name}"

        ds_ls = LinkedServiceReference(type="LinkedServiceReference", reference_name=self.ls_sql_name)
        data_azure_sql = AzureSqlTableDataset(
            linked_service_name=ds_ls,
            table_name=f"{self.schema}.{self.table_name}",
        )
        data_azure_sql = DatasetResource(properties=data_azure_sql)
        self.adf_client.datasets.create_or_update(self.rg_name, self.df_name, ds_name, data_azure_sql)

    def create_pipeline(self, pipeline_name):
        activities = [self.create_copy_activity()]
        # If user wants to upsert, we append stored procedure activity to pipeline.
        if self.method == "upsert":
            activities.append(self.stored_procedure_activity())
        # Create a pipeline with the copy activity
        if not pipeline_name:
            pipeline_name = f"{self.schema} {self.table_name} to SQL"
        params_for_pipeline = {}
        p_obj = PipelineResource(activities=activities, parameters=params_for_pipeline)
        self.adf_client.pipelines.create_or_update(self.rg_name, self.df_name, pipeline_name, p_obj)

        logging.info(f"Triggering pipeline run for {self.table_name}!")
        run_response = self.adf_client.pipelines.create_run(self.rg_name, self.df_name, pipeline_name, parameters={})

        return run_response

    def create_copy_activity(self):
        act_name = f"Copy {self.table_name} to SQL"
        blob_source = BlobSource()
        sql_sink = SqlSink()

        ds_in_ref = DatasetReference(type="DatasetReference", reference_name=f"BLOB_dftoazure_{self.table_name}")
        ds_out_ref = DatasetReference(type="DatasetReference", reference_name=f"SQL_dftoazure_{self.table_name}")
        copy_activity = CopyActivity(
            name=act_name,
            inputs=[ds_in_ref],
            outputs=[ds_out_ref],
            source=blob_source,
            sink=sql_sink,
        )

        return copy_activity

    def stored_procedure_activity(self):
        dependency_condition = DependencyCondition("Succeeded")
        dependency = ActivityDependency(
            activity=f"Copy {self.table_name} to SQL", dependency_conditions=[dependency_condition]
        )
        linked_service_reference = LinkedServiceReference(
            type="LinkedServiceReference", reference_name=self.ls_sql_name
        )
        activity = SqlServerStoredProcedureActivity(
            stored_procedure_name=f"UPSERT_{self.table_name}",
            name="UPSERT procedure",
            description="Trigger UPSERT procedure in SQL",
            depends_on=[dependency],
            linked_service_name=linked_service_reference,
        )

        return activity
