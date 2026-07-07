from unittest.mock import Mock, patch

from azure.mgmt.datafactory.models import (
    AzureBlobStorageLinkedService,
    AzureSqlDatabaseAuthenticationType,
    AzureSqlDatabaseLinkedService,
    AzureStorageAuthenticationType,
)

from df_to_azure.adf import ADF
from df_to_azure.auth import create_blob_service_client
from df_to_azure.db import auth_azure


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


class FakeAdfClient:
    def __init__(self):
        self.linked_services = FakeLinkedServices()


def clear_auth_env(monkeypatch):
    for env_var in AUTH_ENV_VARS:
        monkeypatch.delenv(env_var, raising=False)


def set_passwordless_env(monkeypatch):
    monkeypatch.setenv("SQL_DB", "test-db")
    monkeypatch.setenv("SQL_SERVER", "test-server.database.windows.net")
    monkeypatch.setenv("df_name", "test-adf")
    monkeypatch.setenv("ls_blob_account_name", "teststorage")
    monkeypatch.setenv("rg_location", "westeurope")
    monkeypatch.setenv("rg_name", "test-rg")
    monkeypatch.setenv("subscription_id", "00000000-0000-0000-0000-000000000000")


def create_adf_stub():
    adf = ADF.__new__(ADF)
    adf.rg_name = "test-rg"
    adf.df_name = "test-adf"
    adf.ls_sql_name = "sql-linked-service"
    adf.ls_blob_name = "blob-linked-service"
    adf.adf_client = FakeAdfClient()
    return adf


def test_check_env_variables_accepts_passwordless_defaults(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)

    ADF.check_env_variables()


@patch("df_to_azure.db.create_engine")
def test_auth_azure_prefers_active_directory_default(create_engine, monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    monkeypatch.setenv("SQL_USER", "legacy-user")
    monkeypatch.setenv("SQL_PW", "legacy-password")
    create_engine.return_value.connect.return_value = Mock()

    auth_azure(driver="ODBC Driver 18 for SQL Server")

    url = create_engine.call_args.args[0]
    assert create_engine.call_count == 1
    assert url.query["Authentication"] == "ActiveDirectoryDefault"
    assert url.username is None
    assert url.host == "test-server.database.windows.net"


@patch("df_to_azure.db.create_engine")
def test_auth_azure_falls_back_to_sql_password_when_default_auth_fails(create_engine, monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    monkeypatch.setenv("SQL_USER", "legacy-user")
    monkeypatch.setenv("SQL_PW", "legacy-password")

    default_engine = Mock()
    default_engine.connect.side_effect = RuntimeError("default auth failed")
    sql_password_engine = Mock()
    sql_password_engine.connect.return_value = Mock()
    create_engine.side_effect = [default_engine, sql_password_engine]

    auth_azure(driver="ODBC Driver 18 for SQL Server")

    default_url = create_engine.call_args_list[0].args[0]
    fallback_url = create_engine.call_args_list[1].args[0]
    assert default_url.query["Authentication"] == "ActiveDirectoryDefault"
    assert str(fallback_url).startswith("mssql+pyodbc://legacy-user:")


@patch("df_to_azure.auth.BlobServiceClient")
@patch("df_to_azure.auth.create_default_credential")
def test_create_blob_service_client_prefers_default_credential_when_account_name_exists(
    create_default_credential, blob_service_client, monkeypatch
):
    clear_auth_env(monkeypatch)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "legacy-storage-connection-string")
    create_default_credential.return_value = Mock()

    create_blob_service_client("teststorage")

    blob_service_client.assert_called_once_with(
        account_url="https://teststorage.blob.core.windows.net",
        credential=create_default_credential.return_value,
        timeout=None,
    )
    blob_service_client.from_connection_string.assert_not_called()


@patch("df_to_azure.auth.BlobServiceClient")
def test_create_blob_service_client_falls_back_to_connection_string(blob_service_client, monkeypatch):
    clear_auth_env(monkeypatch)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", "legacy-storage-connection-string")

    create_blob_service_client(None)

    blob_service_client.from_connection_string.assert_called_once_with("legacy-storage-connection-string", timeout=None)


def test_adf_sql_linked_service_uses_system_assigned_managed_identity_by_default(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    adf = create_adf_stub()

    adf.create_linked_service_sql()

    linked_service = adf.adf_client.linked_services.linked_service
    assert isinstance(linked_service.properties, AzureSqlDatabaseLinkedService)
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
    assert linked_service.properties.credential.reference_name == "user-assigned-mi-credential"


def test_adf_blob_linked_service_uses_managed_identity(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    adf = create_adf_stub()

    adf.create_linked_service_blob()

    linked_service = adf.adf_client.linked_services.linked_service
    assert isinstance(linked_service.properties, AzureBlobStorageLinkedService)
    assert linked_service.properties.authentication_type == AzureStorageAuthenticationType.MSI
