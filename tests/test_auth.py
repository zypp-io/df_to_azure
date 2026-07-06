from unittest.mock import Mock, patch

import pytest
from azure.mgmt.datafactory.models import (
    AzureBlobStorageLinkedService,
    AzureSqlDatabaseAuthenticationType,
    AzureSqlDatabaseLinkedService,
    AzureStorageAuthenticationType,
)

from df_to_azure.adf import ADF
from df_to_azure.auth import create_blob_service_client
from df_to_azure.db import auth_azure
from df_to_azure.exceptions import EnvVariableNotSetError


AUTH_ENV_VARS = [
    "DF_TO_AZURE_ADF_CREDENTIAL_NAME",
    "DF_TO_AZURE_ADF_SQL_AUTH",
    "DF_TO_AZURE_SQL_AUTH",
    "DF_TO_AZURE_SQL_MANAGED_IDENTITY_CLIENT_ID",
    "DF_TO_AZURE_STORAGE_AUTH",
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


def test_check_env_variables_requires_legacy_secrets_only_when_legacy_auth_is_selected(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    monkeypatch.setenv("DF_TO_AZURE_STORAGE_AUTH", "key")
    monkeypatch.setenv("DF_TO_AZURE_SQL_AUTH", "sql_password")

    with pytest.raises(EnvVariableNotSetError) as exc:
        ADF.check_env_variables()

    message = str(exc.value)
    assert "SQL_PW" in message
    assert "SQL_USER" in message
    assert "ls_blob_account_key" in message


@patch("df_to_azure.db.create_engine")
def test_auth_azure_uses_active_directory_default_by_default(create_engine, monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    create_engine.return_value.connect.return_value = Mock()

    auth_azure(driver="ODBC Driver 18 for SQL Server")

    url = create_engine.call_args.args[0]
    assert url.query["Authentication"] == "ActiveDirectoryDefault"
    assert url.username is None
    assert url.host == "test-server.database.windows.net"


@patch("df_to_azure.db.create_engine")
def test_auth_azure_can_use_managed_identity_client_id(create_engine, monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    monkeypatch.setenv("DF_TO_AZURE_SQL_AUTH", "managed_identity")
    monkeypatch.setenv("DF_TO_AZURE_SQL_MANAGED_IDENTITY_CLIENT_ID", "managed-identity-client-id")
    create_engine.return_value.connect.return_value = Mock()

    auth_azure(driver="ODBC Driver 18 for SQL Server")

    url = create_engine.call_args.args[0]
    assert url.query["Authentication"] == "ActiveDirectoryMsi"
    assert url.username == "managed-identity-client-id"


@patch("df_to_azure.auth.BlobServiceClient")
@patch("df_to_azure.auth.create_default_credential")
def test_create_blob_service_client_uses_default_credential(
    create_default_credential, blob_service_client, monkeypatch
):
    clear_auth_env(monkeypatch)
    create_default_credential.return_value = Mock()

    create_blob_service_client("teststorage")

    blob_service_client.assert_called_once_with(
        account_url="https://teststorage.blob.core.windows.net",
        credential=create_default_credential.return_value,
        timeout=None,
    )


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


def test_adf_blob_linked_service_uses_managed_identity_by_default(monkeypatch):
    clear_auth_env(monkeypatch)
    set_passwordless_env(monkeypatch)
    adf = create_adf_stub()

    adf.create_linked_service_blob()

    linked_service = adf.adf_client.linked_services.linked_service
    assert isinstance(linked_service.properties, AzureBlobStorageLinkedService)
    assert linked_service.properties.authentication_type == AzureStorageAuthenticationType.MSI
