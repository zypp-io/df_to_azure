import logging
from df_to_azure.functions import print_item
from df_to_azure.exceptions import CreateContainerError
from azure.mgmt.datafactory.models import (
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
)
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.storage.blob import BlobServiceClient


logging.getLogger(__name__).setLevel(logging.INFO)

# global variables


def create_adf_client(settings):
    credentials = ServicePrincipalCredentials(
        client_id=settings["client_id"],
        secret=settings["secret"],
        tenant=settings["tenant"],
    )

    adf_client = DataFactoryManagementClient(credentials, settings["subscription_id"])

    return adf_client


def create_resource_client(settings):
    credentials = ServicePrincipalCredentials(
        client_id=settings["client_id"],
        secret=settings["secret"],
        tenant=settings["tenant"],
    )
    resource_client = ResourceManagementClient(credentials, settings["subscription_id"])

    return resource_client


def create_resourcegroup(settings):

    resource_client = create_resource_client(settings)
    rg_params = {"location": settings["rg_location"]}
    rg = resource_client.resource_groups.create_or_update(settings["rg_name"], rg_params)
    print_item(rg)


def create_datafactory(settings):

    df_resource = Factory(location=settings["rg_location"])
    adf_client = create_adf_client(settings)
    df = adf_client.factories.create_or_update(
        settings["rg_name"], settings["df_name"], df_resource
    )
    print_item(df)

    while df.provisioning_state != "Succeeded":
        df = adf_client.factories.get(settings["rg_name"], settings["df_name"])
        logging.info(f"datafactory {settings['df_name']} created!")


def create_blob_service_client(settings):
    connect_str = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={}".format(
        settings["ls_blob_account_name"], settings["ls_blob_account_key"]
    )

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    return blob_service_client


def create_blob_container(settings):
    blob_service_client = create_blob_service_client(settings)
    try:
        blob_service_client.create_container(settings["ls_blob_container_name"])
    except CreateContainerError:
        logging.info("CreateContainerError: Container already exists.")


def create_linked_service_sql(settings):
    conn_string = SecureString(
        value=f"integrated security=False;encrypt=True;connection timeout=30;data "
        f"source={settings['ls_sql_server_name']}"
        f";initial catalog={settings['ls_sql_database_name']}"
        f";user id={settings['ls_sql_database_user']}"
        f";password={settings['ls_sql_database_password']}"
    )

    ls_azure_sql = AzureSqlDatabaseLinkedService(connection_string=conn_string)
    adf_client = create_adf_client(settings)

    adf_client.linked_services.create_or_update(
        settings["rg_name"],
        settings["df_name"],
        settings["ls_sql_name"],
        ls_azure_sql,
    )


def create_linked_service_blob(settings):
    storage_string = SecureString(
        value=f"DefaultEndpointsProtocol=https;AccountName={settings['ls_blob_account_name']}"
        f";AccountKey={settings['ls_blob_account_key']}"
    )

    ls_azure_blob = AzureStorageLinkedService(connection_string=storage_string)
    adf_client = create_adf_client(settings)
    adf_client.linked_services.create_or_update(
        settings["rg_name"],
        settings["df_name"],
        settings["ls_blob_name"],
        ls_azure_blob,
    )


def create_input_blob(table):
    ds_name = f"BLOB_{table.azure['ls_blob_container_name']}_{table.name}"

    ds_ls = LinkedServiceReference(reference_name=table.azure["ls_blob_name"])
    ds_azure_blob = AzureBlobDataset(
        linked_service_name=ds_ls,
        folder_path=f"{table.azure['ls_blob_container_name']}/{table.name}",
        file_name=table.name,
        format={
            "type": "TextFormat",
            "columnDelimiter": "^",
            "rowDelimiter": "",
            "treatEmptyAsNull": "true",
            "skipLineCount": 0,
            "firstRowAsHeader": "true",
        },
    )
    adf_client = create_adf_client(table.azure)
    adf_client.datasets.create_or_update(
        table.azure["rg_name"], table.azure["df_name"], ds_name, ds_azure_blob
    )


def create_output_sql(table):

    ds_name = f"SQL_{table.azure['ls_blob_container_name']}_{table.name}"

    ds_ls = LinkedServiceReference(reference_name=table.azure["ls_sql_name"])
    data_azureSql = AzureSqlTableDataset(
        linked_service_name=ds_ls,
        table_name=f"{table.schema}.{table.name}",
    )
    adf_client = create_adf_client(table.azure)
    adf_client.datasets.create_or_update(
        table.azure["rg_name"], table.azure["df_name"], ds_name, data_azureSql
    )


def create_pipeline(table):

    copy_activity = create_copy_activity(table)

    # Create a pipeline with the copy activity
    p_name = f"{table.azure['ls_blob_container_name'].capitalize()} {table.name} to SQL"
    params_for_pipeline = {}
    p_obj = PipelineResource(activities=[copy_activity], parameters=params_for_pipeline)
    adf_client = create_adf_client(table.azure)
    adf_client.pipelines.create_or_update(
        table.azure["rg_name"], table.azure["df_name"], p_name, p_obj
    )

    if table.azure["trigger"]:
        logging.info(f"triggering pipeline run for {table.name}!")
        adf_client.pipelines.create_run(
            table.azure["rg_name"], table.azure["df_name"], p_name, parameters={}
        )


def create_copy_activity(table):
    act_name = f"Copy {table.name} to SQL"
    blob_source = BlobSource()
    sql_sink = SqlSink()

    dsin_ref = DatasetReference(
        reference_name=f"BLOB_{table.azure['ls_blob_container_name']}_{table.name}"
    )
    dsOut_ref = DatasetReference(
        reference_name=f"SQL_{table.azure['ls_blob_container_name']}_{table.name}"
    )
    copy_activity = CopyActivity(
        name=act_name,
        inputs=[dsin_ref],
        outputs=[dsOut_ref],
        source=blob_source,
        sink=sql_sink,
    )

    return copy_activity


def create_multiple_activity_pipeline(tables):

    copy_activities = list()
    for table in tables:
        copy_activities.append(create_copy_activity(table))

    # Create a pipeline with the copy activity
    p_name = f"{tables[0].azure['ls_blob_container_name'].capitalize()} to SQL"
    params_for_pipeline = {}
    p_obj = PipelineResource(activities=copy_activities, parameters=params_for_pipeline)
    adf_client = create_adf_client(tables[0].azure)
    adf_client.pipelines.create_or_update(
        tables[0].azure["rg_name"], tables[0].azure["df_name"], p_name, p_obj
    )

    if tables[0].azure["trigger"]:
        logging.info(
            f"triggering pipeline run for {tables[0].azure['ls_blob_container_name'].capitalize()}!"
        )
        adf_client.pipelines.create_run(
            tables[0].azure["rg_name"], tables[0].azure["df_name"], p_name, parameters={}
        )
