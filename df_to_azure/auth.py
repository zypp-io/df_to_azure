from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from df_to_azure.env import get_env


def create_blob_service_client(credential=None, timeout: int = None):
    """
    Create a BlobServiceClient with the same priority as the ADF blob linked service:
    explicit secrets win, passwordless (DefaultAzureCredential) is used when only
    ls_blob_account_name is set.
    """
    connection_string = get_env("AZURE_STORAGE_CONNECTION_STRING")
    if connection_string:
        return BlobServiceClient.from_connection_string(connection_string, timeout=timeout)

    account_name = get_env("ls_blob_account_name")
    account_key = get_env("ls_blob_account_key")
    if account_name and account_key:
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key}"
        return BlobServiceClient.from_connection_string(connection_string, timeout=timeout)

    if account_name:
        return BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=credential or DefaultAzureCredential(),
            timeout=timeout,
        )

    raise ValueError(
        "Set ls_blob_account_name for passwordless storage auth, or provide AZURE_STORAGE_CONNECTION_STRING."
    )
