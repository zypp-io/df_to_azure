from unittest.mock import Mock, patch

import pytest
from azure.mgmt.datafactory.models import (
    AzureBlobDataset,
    AzureBlobStorageLinkedService,
    AzureSqlDatabaseAuthenticationType,
    AzureSqlDatabaseLinkedService,
    AzureSqlTableDataset,
    AzureStorageLinkedService,
    AzureStorageAuthenticationType,
    CopyActivity,
    PipelineResource,
    SqlServerStoredProcedureActivity,
)

from df_to_azure.adf import ADF
from df_to_azure.auth import create_blob_service_client
from df_to_azure.db import (
    SQL_COPT_SS_ACCESS_TOKEN,
    SQL_DATABASE_SCOPE,
    add_access_token_to_connection,
    auth_azure,
    create_access_token_struct,
    remove_trusted_connection,
)
from df_to_azure.env import get_env


AUTH_ENV_VARS = [
    "AZURE_STORAGE_CONNECTION_STRING",
    "DF_TO_AZURE_ADF_CREDENTIAL_NAME",
    "DF_TO_AZURE_STORAGE_ACCOUNT_KIND",
    "SQL_DB",
    "SQL_PW",
    "SQL_SERVER",
    "SQL_USER",
    "df_name",
    "ls_blob_account_key",
    "ls_blob_account_name",
    "rg_location",
    "rg_name",
    "subscription_id",
]


class FakeLinkedServices:
    def create_or_update(self, rg_name, df_name, linked_service_name, linked_service):
        self.rg_name = rg_name
        self.df_name = df_name
        self.linked_service_name = linked_service_name
        self.linked_service = linked_service


class FakeDatasets:
    def create_or_update(self, rg_name, df_name, dataset_name, dataset):
        self.rg_name = rg_name
        self.df_name = df_name
        self.dataset_name = dataset_name
        self.dataset = dataset


class FakeAdfClient:
    def __init__(self):
        self.linked_services = FakeLinkedServices()
        self.datasets = FakeDatasets()


def clear_auth_env(monkeypatch):
    for env_var in AUTH_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)
        monkeypatch.delenv(env_var.upper(), raising=False)
        monkeypatch.delenv(env_var.lower(), raising=False)


def set_passwordless_env(monkeypatch):
    monkeypatch.setenv("SQL_DB", "test-db")
    monkeypatch.setenv("SQL_SERVER", "test-server.database.windows.net")
    monkeypatch.setenv("df_name", "test-adf")
    monkeypatch.setenv("ls_blob_account_name", "teststorage")
    monkeypatch.setenv("rg_location", "westeurope")
    monkeypatch.setenv("rg_name", "test-rg")
    monkeypatch.setenv("subscription_id", "00000000-0000-0000-0000-000000000000")


def set_uppercase_passwordless_env(monkeypatch):
    monkeypatch.setenv("SQL_DB", "test-db")
    monkeypatch.setenv("SQL_SERVER", "test-server.database.windows.net")
    monkeypatch.setenv("DF_NAME", "test-adf")
    monkeypatch.setenv("LS_BLOB_ACCOUNT_NAME", "teststorage")
    monkeypatch.setenv("RG_LOCATION", "westeurope")
    monkeypatch.setenv("RG_NAME", "test-rg")
    monkeypatch.setenv("SUBSCRIPTION_ID", "00000000-0000-0000-0000-000000000000")


def create_adf_stub():
    adf = ADF.__new__(ADF)
    adf.rg_name = "test-rg"
    adf.df_name = "test-adf"
    adf.ls_sql_name = "sql-linked-service"
    adf.ls_blob_name = "blob-linked-service"
    adf.table_name = "test_table"
    adf.schema = "dbo"
    adf.method = "create"
    adf.adf_client = FakeAdfClient()
    return adf


def test_check_env_variables_accepts_passwordless_defaults(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)

    ADF.check_env_variables()


def test_check_env_variables_accepts_uppercase_aliases(monkeypatch):
    clear_auth_env(monkeypatch)
    set_uppercase_passwordless_env(monkeypatch)

    ADF.check_env_variables()


def test_get_env_prefers_existing_lowercase_names(monkeypatch):
    clear_auth_env(monkeypatch)
    monkeypatch.setenv("rg_name", "lowercase-rg")
    monkeypatch.setenv("RG_NAME", "uppercase-rg")

    assert get_env("rg_name") == "lowercase-rg"


def test_check_env_variables_accepts_storage_connection_string_without_account_name(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    monkeypatch.delenv("ls_blob_account_name")
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "legacy-storage-connection-string")

    ADF.check_env_variables()


@patch("df_to_azure.db.add_access_token_listener")
@patch("df_to_azure.db.create_engine")
def test_auth_azure_uses_access_token_without_sql_credentials(create_engine, add_access_token_listener, monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    create_engine.return_value.connect.return_value = Mock()

    auth_azure(driver="ODBC Driver 18 for SQL Server")

    url = create_engine.call_args.args[0]
    assert create_engine.call_count == 1
    assert create_engine.call_args.kwargs == {}
    assert "Authentication" not in url.query
    assert url.username is None
    assert url.host == "test-server.database.windows.net"
    add_access_token_listener.assert_called_once_with(create_engine.return_value)


def test_create_access_token_struct_encodes_token_for_odbc():
    credential = Mock()
    credential.get_token.return_value.token = "abc"

    token_struct = create_access_token_struct(credential=credential)

    assert token_struct == b"\x06\x00\x00\x00a\x00b\x00c\x00"
    credential.get_token.assert_called_once_with(SQL_DATABASE_SCOPE)


def test_remove_trusted_connection_from_sqlalchemy_pyodbc_connection_string():
    connection_string = "DRIVER={ODBC Driver 18 for SQL Server};Server=tcp:test;Trusted_Connection=Yes"

    assert remove_trusted_connection(connection_string) == "DRIVER={ODBC Driver 18 for SQL Server};Server=tcp:test"


def test_add_access_token_to_connection_removes_trusted_connection_and_adds_token():
    credential = Mock()
    credential.get_token.return_value.token = "abc"
    cargs = ["DRIVER={ODBC Driver 18 for SQL Server};Server=tcp:test;Trusted_Connection=Yes"]
    cparams = {}

    add_access_token_to_connection(cargs, cparams, credential)

    assert cargs == ["DRIVER={ODBC Driver 18 for SQL Server};Server=tcp:test"]
    assert cparams["attrs_before"][SQL_COPT_SS_ACCESS_TOKEN] == b"\x06\x00\x00\x00a\x00b\x00c\x00"


@patch("df_to_azure.db.create_engine")
def test_auth_azure_uses_sql_password_when_sql_credentials_exist(create_engine, monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    monkeypatch.setenv("SQL_USER", "legacy-user")
    monkeypatch.setenv("SQL_PW", "legacy-password")
    create_engine.return_value.connect.return_value = Mock()

    auth_azure(driver="ODBC Driver 18 for SQL Server")

    url = create_engine.call_args.args[0]
    assert create_engine.call_count == 1
    assert str(url).startswith("mssql+pyodbc://legacy-user:")


@patch("df_to_azure.auth.BlobServiceClient")
def test_create_blob_service_client_prefers_connection_string(blob_service_client, monkeypatch):
    clear_auth_env(monkeypatch)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "legacy-storage-connection-string")
    monkeypatch.setenv("ls_blob_account_name", "teststorage")

    create_blob_service_client()

    blob_service_client.from_connection_string.assert_called_once_with("legacy-storage-connection-string", timeout=None)


@patch("df_to_azure.auth.BlobServiceClient")
def test_create_blob_service_client_uses_account_key_when_account_key_exists(blob_service_client, monkeypatch):
    clear_auth_env(monkeypatch)
    monkeypatch.setenv("ls_blob_account_name", "teststorage")
    monkeypatch.setenv("ls_blob_account_key", "legacy-account-key")

    create_blob_service_client()

    blob_service_client.from_connection_string.assert_called_once_with(
        "DefaultEndpointsProtocol=https;AccountName=teststorage;AccountKey=legacy-account-key", timeout=None
    )


@patch("df_to_azure.auth.BlobServiceClient")
def test_create_blob_service_client_accepts_uppercase_storage_aliases(blob_service_client, monkeypatch):
    clear_auth_env(monkeypatch)
    monkeypatch.setenv("LS_BLOB_ACCOUNT_NAME", "teststorage")
    monkeypatch.setenv("LS_BLOB_ACCOUNT_KEY", "legacy-account-key")

    create_blob_service_client()

    blob_service_client.from_connection_string.assert_called_once_with(
        "DefaultEndpointsProtocol=https;AccountName=teststorage;AccountKey=legacy-account-key", timeout=None
    )


@patch("df_to_azure.auth.BlobServiceClient")
@patch("df_to_azure.auth.DefaultAzureCredential")
def test_create_blob_service_client_uses_default_credential_when_only_account_name_exists(
    default_azure_credential, blob_service_client, monkeypatch
):
    clear_auth_env(monkeypatch)
    monkeypatch.setenv("ls_blob_account_name", "teststorage")

    create_blob_service_client()

    blob_service_client.assert_called_once_with(
        account_url="https://teststorage.blob.core.windows.net",
        credential=default_azure_credential.return_value,
        timeout=None,
    )
    blob_service_client.from_connection_string.assert_not_called()


def test_create_blob_service_client_raises_without_storage_settings(monkeypatch):
    clear_auth_env(monkeypatch)

    with pytest.raises(ValueError, match="ls_blob_account_name"):
        create_blob_service_client()


def test_adf_sql_linked_service_uses_sql_auth_when_sql_credentials_exist(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    monkeypatch.setenv("SQL_USER", "legacy-user")
    monkeypatch.setenv("SQL_PW", "legacy-password")
    adf = create_adf_stub()

    adf.create_linked_service_sql()

    linked_service = adf.adf_client.linked_services.linked_service
    assert isinstance(linked_service.properties, AzureSqlDatabaseLinkedService)
    assert linked_service.properties.type == "AzureSqlDatabase"
    assert linked_service.as_dict()["properties"]["type"] == "AzureSqlDatabase"
    assert "user id=legacy-user" in linked_service.properties.connection_string.value
    assert "password=legacy-password" in linked_service.properties.connection_string.value


def test_adf_sql_linked_service_accepts_uppercase_aliases(monkeypatch):
    clear_auth_env(monkeypatch)
    set_uppercase_passwordless_env(monkeypatch)
    adf = create_adf_stub()

    adf.create_linked_service_sql()

    linked_service = adf.adf_client.linked_services.linked_service
    assert linked_service.properties.server == "test-server.database.windows.net"
    assert linked_service.properties.database == "test-db"
    assert linked_service.properties.type == "AzureSqlDatabase"
    assert (
        linked_service.properties.authentication_type
        == AzureSqlDatabaseAuthenticationType.SYSTEM_ASSIGNED_MANAGED_IDENTITY
    )


def test_adf_sql_linked_service_uses_system_assigned_managed_identity_by_default(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    adf = create_adf_stub()

    adf.create_linked_service_sql()

    linked_service = adf.adf_client.linked_services.linked_service
    assert isinstance(linked_service.properties, AzureSqlDatabaseLinkedService)
    assert linked_service.properties.type == "AzureSqlDatabase"
    assert linked_service.as_dict()["properties"]["type"] == "AzureSqlDatabase"
    assert (
        linked_service.properties.authentication_type
        == AzureSqlDatabaseAuthenticationType.SYSTEM_ASSIGNED_MANAGED_IDENTITY
    )


def test_adf_sql_linked_service_uses_user_assigned_managed_identity_when_credential_name_exists(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    monkeypatch.setenv("DF_TO_AZURE_ADF_CREDENTIAL_NAME", "user-assigned-mi-credential")
    adf = create_adf_stub()

    adf.create_linked_service_sql()

    linked_service = adf.adf_client.linked_services.linked_service
    assert (
        linked_service.properties.authentication_type
        == AzureSqlDatabaseAuthenticationType.USER_ASSIGNED_MANAGED_IDENTITY
    )
    assert linked_service.properties.type == "AzureSqlDatabase"
    assert linked_service.properties.credential.reference_name == "user-assigned-mi-credential"


def test_adf_blob_linked_service_uses_account_key_when_account_key_exists(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    monkeypatch.setenv("ls_blob_account_key", "legacy-account-key")
    adf = create_adf_stub()

    adf.create_linked_service_blob()

    linked_service = adf.adf_client.linked_services.linked_service
    assert isinstance(linked_service.properties, AzureStorageLinkedService)
    assert linked_service.properties.type == "AzureStorage"
    assert linked_service.as_dict()["properties"]["type"] == "AzureStorage"
    assert "AccountKey=legacy-account-key" in linked_service.properties.connection_string.value


def test_adf_blob_linked_service_accepts_uppercase_storage_aliases(monkeypatch):
    clear_auth_env(monkeypatch)
    set_uppercase_passwordless_env(monkeypatch)
    adf = create_adf_stub()

    adf.create_linked_service_blob()

    linked_service = adf.adf_client.linked_services.linked_service
    assert isinstance(linked_service.properties, AzureBlobStorageLinkedService)
    assert linked_service.properties.type == "AzureBlobStorage"
    assert linked_service.properties.service_endpoint == "https://teststorage.blob.core.windows.net/"


def test_adf_blob_linked_service_uses_connection_string_when_connection_string_exists(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "legacy-storage-connection-string")
    adf = create_adf_stub()

    adf.create_linked_service_blob()

    linked_service = adf.adf_client.linked_services.linked_service
    assert isinstance(linked_service.properties, AzureStorageLinkedService)
    assert linked_service.properties.type == "AzureStorage"
    assert linked_service.properties.connection_string.value == "legacy-storage-connection-string"


def test_adf_blob_linked_service_uses_managed_identity_by_default(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    adf = create_adf_stub()

    adf.create_linked_service_blob()

    linked_service = adf.adf_client.linked_services.linked_service
    assert isinstance(linked_service.properties, AzureBlobStorageLinkedService)
    assert linked_service.properties.type == "AzureBlobStorage"
    assert linked_service.as_dict()["properties"]["type"] == "AzureBlobStorage"
    assert linked_service.properties.authentication_type == AzureStorageAuthenticationType.MSI


def test_adf_input_blob_dataset_serializes_dataset_type():
    adf = create_adf_stub()

    adf.create_input_blob()

    dataset = adf.adf_client.datasets.dataset
    assert isinstance(dataset.properties, AzureBlobDataset)
    assert dataset.properties.type == "AzureBlob"
    assert dataset.as_dict()["properties"]["type"] == "AzureBlob"


def test_adf_output_sql_dataset_serializes_dataset_type():
    adf = create_adf_stub()

    adf.create_output_sql()

    dataset = adf.adf_client.datasets.dataset
    assert isinstance(dataset.properties, AzureSqlTableDataset)
    assert dataset.properties.type == "AzureSqlTable"
    assert dataset.as_dict()["properties"]["type"] == "AzureSqlTable"


def test_adf_copy_activity_serializes_copy_type():
    adf = create_adf_stub()

    activity = adf.create_copy_activity()

    assert isinstance(activity, CopyActivity)
    assert activity.type == "Copy"
    assert activity.as_dict()["type"] == "Copy"
    assert PipelineResource(activities=[activity]).as_dict()["properties"]["activities"][0]["type"] == "Copy"


def test_adf_stored_procedure_activity_serializes_stored_procedure_type():
    adf = create_adf_stub()

    activity = adf.stored_procedure_activity()

    assert isinstance(activity, SqlServerStoredProcedureActivity)
    assert activity.type == "SqlServerStoredProcedure"
    assert activity.as_dict()["type"] == "SqlServerStoredProcedure"
