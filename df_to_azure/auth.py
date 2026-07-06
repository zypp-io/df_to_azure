import os

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


AUTH_ACTIVE_DIRECTORY_DEFAULT = "active_directory_default"
AUTH_DEFAULT = "default"
AUTH_KEY = "key"
AUTH_MANAGED_IDENTITY = "managed_identity"
AUTH_SQL_PASSWORD = "sql_password"
AUTH_SYSTEM_ASSIGNED_MANAGED_IDENTITY = "system_assigned_managed_identity"
AUTH_USER_ASSIGNED_MANAGED_IDENTITY = "user_assigned_managed_identity"


def get_auth_mode(env_var: str, default: str) -> str:
    return os.environ.get(env_var, default).strip().lower()


def create_default_credential():
    return DefaultAzureCredential()


def create_blob_service_client(account_name: str, credential=None, timeout: int = None):
    storage_auth = get_auth_mode("DF_TO_AZURE_STORAGE_AUTH", AUTH_DEFAULT)
    if storage_auth == AUTH_KEY:
        connect_str = (
            f"DefaultEndpointsProtocol=https;AccountName={account_name}"
            f";AccountKey={os.environ.get('ls_blob_account_key')}"
        )
        return BlobServiceClient.from_connection_string(connect_str, timeout=timeout)

    if storage_auth != AUTH_DEFAULT:
        raise ValueError("DF_TO_AZURE_STORAGE_AUTH must be 'default' or 'key'.")

    return BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=credential or create_default_credential(),
        timeout=timeout,
    )
