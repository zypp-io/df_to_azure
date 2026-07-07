import os

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


def create_default_credential():
    return DefaultAzureCredential()


def create_blob_service_client(account_name: str, credential=None, timeout: int = None):
    if account_name:
        return BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=credential or create_default_credential(),
            timeout=timeout,
        )

    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if connection_string:
        return BlobServiceClient.from_connection_string(connection_string, timeout=timeout)

    account_key = os.environ.get("ls_blob_account_key")
    account_name = os.environ.get("ls_blob_account_name")
    if account_name and account_key:
        connect_str = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key}"
        return BlobServiceClient.from_connection_string(connect_str, timeout=timeout)

    raise ValueError(
        "Set ls_blob_account_name for passwordless storage auth, or provide AZURE_STORAGE_CONNECTION_STRING."
    )
