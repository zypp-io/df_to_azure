import logging
import os
from df_to_azure.utils import print_item
from azure.mgmt.datafactory.models import (
    ActivityDependency,
    Factory,
    SecureString,
    AzureSqlDatabaseLinkedService,
    AzureStorageLinkedService,
    LinkedServiceReference,
    AzureBlobDataset,
    PipelineResource,
    BlobSource,
    DatasetReference,
    AzureSqlTableDataset,
    CopyActivity,
    SqlSink,
    SqlServerStoredProcedureActivity,
    DependencyCondition,
    DatasetResource,
)
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.storage.blob import BlobServiceClient


class ADF:
    def __init__(self, table_name, table_schema, table_method):
        self.credentials = self.create_credentials()
        self.ls_blob_account_name = os.environ.get("ls_blob_account_name")
        self.rg_name = os.environ.get("rg_name")
        self.df_name = os.environ.get("df_name")
        self.ls_sql_name = os.environ.get("ls_sql_name")
        self.ls_blob_name = os.environ.get("ls_blob_name")
        self.table_name = table_name
        self.table_schema = table_schema
        self.table_method = table_method

    @staticmethod
    def create_credentials():
        credentials = ClientSecretCredential(
            client_id=os.environ.get("AZURE_CLIENT_ID"),
            client_secret=os.environ.get("AZURE_CLIENT_SECRET"),
            tenant_id=os.environ.get("AZURE_TENANT_ID"),
        )

        return credentials

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
        adf_client = self.adf_client()
        df = adf_client.factories.create_or_update(
            self.rg_name, self.df_name, df_resource
        )
        print_item(df)

        while df.provisioning_state != "Succeeded":
            df = adf_client.factories.get(self.rg_name, self.df_name)
            logging.info(f"Datafactory {os.environ.get('df_name')} created!")

    def blob_service_client(self):
        connect_str = (
            f"DefaultEndpointsProtocol=https;AccountName={self.ls_blob_account_name}"
            f";AccountKey={os.environ.get('ls_blob_account_key')}"
        )
        blob_service_client = BlobServiceClient.from_connection_string(connect_str, timeout=600)

        return blob_service_client

    def create_blob_container(self):
        try:
            self.blob_service_client().create_container("dftoazure")
        except Exception as e:
            logging.info(e)

    def create_linked_service_sql(self):
        conn_string = SecureString(
            value=f"integrated security=False;encrypt=True;connection timeout=600;data "
            f"source={os.environ.get('ls_sql_server_name')}"
            f";initial catalog={os.environ.get('ls_sql_database_name')}"
            f";user id={os.environ.get('ls_sql_database_user')}"
            f";password={os.environ.get('ls_sql_database_password')}"
        )

        ls_azure_sql = AzureSqlDatabaseLinkedService(connection_string=conn_string)

        self.adf_client().linked_services.create_or_update(
            self.rg_name,
            self.df_name,
            self.ls_sql_name,
            ls_azure_sql,
        )

    def create_linked_service_blob(self):
        storage_string = SecureString(
            value=f"DefaultEndpointsProtocol=https;AccountName={os.environ.get('ls_blob_account_name')}"
            f";AccountKey={os.environ.get('ls_blob_account_key')}"
        )
    
        ls_azure_blob = AzureStorageLinkedService(connection_string=storage_string)
        self.adf_client().linked_services.create_or_update(
            self.rg_name,
            self.df_name,
            self.ls_blob_name,
            ls_azure_blob,
        )

    # TODO: fix table
    def create_input_blob(self):
        ds_name = f"BLOB_dftoazure_{self.table_name}"

        ds_ls = LinkedServiceReference(reference_name=self.ls_blob_name)
        ds_azure_blob = AzureBlobDataset(
            linked_service_name=ds_ls,
            folder_path=f"dftoazure/{self.table_name}",
            file_name=f"{self.table_name}.csv",
            format={
                "type": "TextFormat",
                "columnDelimiter": "^",
                "rowDelimiter": "\n",
                "treatEmptyAsNull": "true",
                "skipLineCount": 0,
                "firstRowAsHeader": "true",
                "quoteChar": '"',
            },
        )
        ds_azure_blob = DatasetResource(properties=ds_azure_blob)
        self.adf_client().datasets.create_or_update(
            self.rg_name, self.df_name, ds_name, ds_azure_blob
        )

    def create_output_sql(self):

        ds_name = f"SQL_dftoazure_{self.table_name}"

        ds_ls = LinkedServiceReference(reference_name=self.ls_sql_name)
        data_azure_sql = AzureSqlTableDataset(
            linked_service_name=ds_ls,
            table_name=f"{self.table_schema}.{self.table_name}",
        )
        data_azure_sql = DatasetResource(properties=data_azure_sql)
        self.adf_client().datasets.create_or_update(
            self.rg_name, self.df_name, ds_name, data_azure_sql
        )

    def create_pipeline(self, pipeline_name):

        activities = [self.create_copy_activity()]
        # If user wants to upsert, we append stored procedure activity to pipeline.
        if self.table_method == "upsert":
            activities.append(self.stored_procedure_activity())
        # Create a pipeline with the copy activity
        if not pipeline_name:
            pipeline_name = f"{self.table_schema} {self.table_name} to SQL"
        params_for_pipeline = {}
        p_obj = PipelineResource(activities=activities, parameters=params_for_pipeline)
        self.adf_client().pipelines.create_or_update(
            self.rg_name, self.df_name, pipeline_name, p_obj
        )

        logging.info(f"Triggering pipeline run for {self.table_name}!")
        run_response = self.adf_client().pipelines.create_run(
            self.rg_name, self.df_name, pipeline_name, parameters={}
        )

        return adf_client, run_response

    def create_copy_activity(self):
        act_name = f"Copy {self.table_name} to SQL"
        blob_source = BlobSource()
        sql_sink = SqlSink()

        ds_in_ref = DatasetReference(reference_name=f"BLOB_dftoazure_{self.table_name}")
        ds_out_ref = DatasetReference(reference_name=f"SQL_dftoazure_{self.table_name}")
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
        linked_service_reference = LinkedServiceReference(reference_name=self.ls_sql_name)
        activity = SqlServerStoredProcedureActivity(
            stored_procedure_name=f"UPSERT_{self.table_name}",
            name="UPSERT procedure",
            description="Trigger UPSERT procedure in SQL",
            depends_on=[dependency],
            linked_service_name=linked_service_reference,
        )

        return activity
