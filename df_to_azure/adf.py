import logging
import os
from df_to_azure.functions import print_item
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


def create_credentials():
    credentials = ClientSecretCredential(
        client_id=os.environ.get("AZURE_CLIENT_ID"),
        client_secret=os.environ.get("AZURE_CLIENT_SECRET"),
        tenant_id=os.environ.get("AZURE_TENANT_ID"),
    )

    return credentials


def create_adf_client():
    credentials = create_credentials()
    adf_client = DataFactoryManagementClient(credentials, os.environ.get("subscription_id"))

    return adf_client


def create_resource_client():
    credentials = create_credentials()
    resource_client = ResourceManagementClient(credentials, os.environ.get("subscription_id"))

    return resource_client


def create_resourcegroup():
    resource_client = create_resource_client()
    rg_params = {"location": os.environ.get("rg_location")}
    rg = resource_client.resource_groups.create_or_update(os.environ.get("rg_name"), rg_params)
    print_item(rg)


def create_datafactory():
    df_resource = Factory(location=os.environ.get("rg_location"))
    adf_client = create_adf_client()
    df = adf_client.factories.create_or_update(
        os.environ.get("rg_name"), os.environ.get("df_name"), df_resource
    )
    print_item(df)

    while df.provisioning_state != "Succeeded":
        df = adf_client.factories.get(os.environ.get("rg_name"), os.environ.get("df_name"))
        logging.info(f"datafactory {os.environ.get('df_name')} created!")


def create_blob_service_client():
    connect_str = (
        f"DefaultEndpointsProtocol=https;AccountName={os.environ.get('ls_blob_account_name')}"
        f";AccountKey={os.environ.get('ls_blob_account_key')}"
    )
    blob_service_client = BlobServiceClient.from_connection_string(connect_str, timeout=600)

    return blob_service_client


def create_blob_container():
    blob_service_client = create_blob_service_client()
    try:
        blob_service_client.create_container("dftoazure")
    except:
        logging.info("CreateContainerError: Container already exists.")


def create_linked_service_sql():
    conn_string = SecureString(
        value=f"integrated security=False;encrypt=True;connection timeout=600;data "
        f"source={os.environ.get('ls_sql_server_name')}"
        f";initial catalog={os.environ.get('ls_sql_database_name')}"
        f";user id={os.environ.get('ls_sql_database_user')}"
        f";password={os.environ.get('ls_sql_database_password')}"
    )

    ls_azure_sql = AzureSqlDatabaseLinkedService(connection_string=conn_string)
    adf_client = create_adf_client()

    adf_client.linked_services.create_or_update(
        os.environ.get("rg_name"),
        os.environ.get("df_name"),
        os.environ.get("ls_sql_name"),
        ls_azure_sql,
    )


def create_linked_service_blob():
    storage_string = SecureString(
        value=f"DefaultEndpointsProtocol=https;AccountName={os.environ.get('ls_blob_account_name')}"
        f";AccountKey={os.environ.get('ls_blob_account_key')}"
    )

    ls_azure_blob = AzureStorageLinkedService(connection_string=storage_string)
    adf_client = create_adf_client()
    adf_client.linked_services.create_or_update(
        os.environ.get("rg_name"),
        os.environ.get("df_name"),
        os.environ.get("ls_blob_name"),
        ls_azure_blob,
    )


def create_input_blob(table):
    ds_name = f"BLOB_dftoazure_{table.name}"

    ds_ls = LinkedServiceReference(reference_name=os.environ.get("ls_blob_name"))
    ds_azure_blob = AzureBlobDataset(
        linked_service_name=ds_ls,
        folder_path=f"dftoazure/{table.name}",
        file_name=f"{table.name}.csv",
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
    adf_client = create_adf_client()
    adf_client.datasets.create_or_update(
        os.environ.get("rg_name"), os.environ.get("df_name"), ds_name, ds_azure_blob
    )


def create_output_sql(table):

    ds_name = f"SQL_dftoazure_{table.name}"

    ds_ls = LinkedServiceReference(reference_name=os.environ.get("ls_sql_name"))
    data_azure_sql = AzureSqlTableDataset(
        linked_service_name=ds_ls,
        table_name=f"{table.schema}.{table.name}",
    )
    data_azure_sql = DatasetResource(properties=data_azure_sql)
    adf_client = create_adf_client()
    adf_client.datasets.create_or_update(
        os.environ.get("rg_name"), os.environ.get("df_name"), ds_name, data_azure_sql
    )


def create_pipeline(table, p_name):

    activities = [create_copy_activity(table)]
    # If user wants to upsert, we append stored procedure activity to pipeline.
    if table.method == "upsert":
        activities.append(stored_procedure_activity(table.name))
    # Create a pipeline with the copy activity
    if not p_name:
        p_name = f"{table.schema} {table.name} to SQL"
    params_for_pipeline = {}
    p_obj = PipelineResource(activities=activities, parameters=params_for_pipeline)
    adf_client = create_adf_client()
    adf_client.pipelines.create_or_update(
        os.environ.get("rg_name"), os.environ.get("df_name"), p_name, p_obj
    )

    logging.info(f"triggering pipeline run for {table.name}!")
    run_response = adf_client.pipelines.create_run(
        os.environ.get("rg_name"), os.environ.get("df_name"), p_name, parameters={}
    )

    return adf_client, run_response


def create_copy_activity(table):
    act_name = f"Copy {table.name} to SQL"
    blob_source = BlobSource()
    sql_sink = SqlSink()

    ds_in_ref = DatasetReference(reference_name=f"BLOB_dftoazure_{table.name}")
    ds_out_ref = DatasetReference(reference_name=f"SQL_dftoazure_{table.name}")
    copy_activity = CopyActivity(
        name=act_name,
        inputs=[ds_in_ref],
        outputs=[ds_out_ref],
        source=blob_source,
        sink=sql_sink,
    )

    return copy_activity


def create_multiple_activity_pipeline(tables, p_name):

    copy_activities = list()
    for table in tables:
        copy_activities.append(create_copy_activity(table))

    # Create a pipeline with the copy activity
    if not p_name:
        p_name = "DF_TO_AZURE to SQL"
    params_for_pipeline = {}
    p_obj = PipelineResource(activities=copy_activities, parameters=params_for_pipeline)
    adf_client = create_adf_client()
    adf_client.pipelines.create_or_update(
        os.environ.get("rg_name"), os.environ.get("df_name"), p_name, p_obj
    )

    logging.info("triggering pipeline run for df_to_azure!")
    run_response = adf_client.pipelines.create_run(
        os.environ.get("rg_name"), os.environ.get("df_name"), p_name, parameters={}
    )

    return adf_client, run_response


def stored_procedure_activity(table_name):
    dependency_condition = DependencyCondition("Succeeded")
    dependency = ActivityDependency(
        activity=f"Copy {table_name} to SQL", dependency_conditions=[dependency_condition]
    )
    linked_service_reference = LinkedServiceReference(reference_name=os.environ.get("ls_sql_name"))
    activity = SqlServerStoredProcedureActivity(
        stored_procedure_name=f"UPSERT_{table_name}",
        name="UPSERT procedure",
        description="Trigger UPSERT procedure in SQL",
        depends_on=[dependency],
        linked_service_name=linked_service_reference,
    )

    return activity
