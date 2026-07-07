DF to Azure
===

[![Downloads](https://pepy.tech/badge/df_to_azure)](https://pepy.tech/project/keyvault)
[![Open Source](https://badges.frapsoft.com/os/v1/open-source.svg?v=103)](https://opensource.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![PyPI](https://img.shields.io/pypi/v/df_to_azure)](https://pypi.org/project/df-to-azure/)
[![Latest release](https://badgen.net/github/release/zypp-io/df_to_azure)](https://github.com/zypp-io/df_to_azure/releases)

> Python module for fast upload of pandas DataFrame to Azure SQL Database using automatic created pipelines in Azure Data Factory.

Supported Python versions: 3.11, 3.12, and 3.13.

## Introduction

The purpose of this project is to upload large datasets using Azure Data Factory combined with an Azure SQL Server.
In steps the following process kicks off:<p>
    1. The data will be uploaded as a .csv file to Azure Blob storage.<br>
    2. A SQL table is prepared based on [pandas DataFrame types](https://pandas.pydata.org/pandas-docs/stable/user_guide/basics.html#basics-dtypes),
which will be converted to the corresponding [SQLAlchemy types](https://docs.sqlalchemy.org/en/14/core/type_basics.html). <br>
    3. A pipeline is created in datafactory for uploading the .csv from the Blob storage into the SQL table.<br>
    4. The pipeline is triggered, so that the .csv file is bulk inserted into the SQL table.<br>

## How it works

Based on the following attributes, it is possible to bulk insert your dataframe into the SQL Database:

```python
from df_to_azure import df_to_azure

df_to_azure(df=df, tablename="table_name", schema="schema", method="create")
```

1. `df`: dataframe you wish to export
2. `tablename`: desired name of the table
3. `schema`: desired sql schema
4. `method`: option for "create" "append" or "upsert"
5. `id_field`: id field of the table. Necessary if `method` is set to "upsert"

**Important**: the csv's are uploaded to a container called `dftoazure`, so create this in your storage account before using this module.

##### Upsert / create or append
It is possible to upsert the SQL table with (new) records, if present in the dataframe you want to upload.
Based on the id_field, the SQL table is being checked on overlapping values.
If there are new records, the "old" records will be updated in the SQL table.
The new records will be uploaded and appended to the current SQL table.

# Settings
The default authentication path is passwordless:

- Python Azure SDK clients use `DefaultAzureCredential`.
- Local development can use `az login`.
- Deployed Python workloads can use a managed identity.
- Data Factory linked services use the Data Factory managed identity by default.

You should not need SQL passwords, storage account keys, or storage connection strings for the default setup. When
legacy values are present, the package uses a fixed fallback order instead of auth-mode environment variables.

## Parquet
Since version 0.6.0, functionality for uploading dataframe to parquet is supported. simply add argument `parquet=True` to upload the dataframe to the Azure storage container parquet.
The arguments tablename and schema will be used to create a folder structure. if parquet is set to True, the dataset will not be uploaded to a SQL database.

```text
# Azure subscription and Data Factory settings
subscription_id=""
rg_name=""
rg_location="westeurope"
df_name=""

# Storage account used for temporary SQL upload parquet files and parquet=True uploads
ls_blob_account_name=""

# Azure SQL Database
SQL_SERVER="<server-name>.database.windows.net"
SQL_DB=""
```

The authentication order is fixed:

1. Azure management clients use `DefaultAzureCredential`.
2. Python Blob clients use `ls_blob_account_name` with `DefaultAzureCredential`.
3. If no storage account name is available, Blob clients fall back to `AZURE_STORAGE_CONNECTION_STRING`.
4. If no storage connection string is available, Blob clients fall back to `ls_blob_account_name` +
   `ls_blob_account_key`.
5. Direct Python SQL connections use ODBC `ActiveDirectoryDefault`.
6. If direct Python SQL authentication fails and both `SQL_USER` and `SQL_PW` are set, SQL falls back to SQL password
   authentication.
7. Azure Data Factory Blob linked services always use managed identity.
8. Azure Data Factory SQL linked services use the system-assigned managed identity by default.

For a user-assigned managed identity on Data Factory, first create an ADF credential and set its name:

```text
DF_TO_AZURE_ADF_CREDENTIAL_NAME=""
```

## Azure permissions for passwordless auth

There are two identities involved in a normal SQL upload:

1. The identity running Python.
2. The managed identity of the Azure Data Factory that runs the copy pipeline.

For local development, sign in first:

```commandline
az login
```

Grant the local developer identity:

- Azure permissions to create or update Data Factory pipelines, datasets, and linked services. `Data Factory Contributor` on the Data Factory is usually enough when `create=False`; resource group `Contributor` is needed when using `create=True`.
- `Storage Blob Data Contributor` on the storage account or the relevant containers.
- A contained Azure SQL user and database permissions for creating schemas, tables, and upsert procedures.

Example SQL permissions for the local developer or deployed Python host identity:

```sql
CREATE USER [user-or-managed-identity-name] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [user-or-managed-identity-name];
ALTER ROLE db_datawriter ADD MEMBER [user-or-managed-identity-name];
ALTER ROLE db_ddladmin ADD MEMBER [user-or-managed-identity-name];
```

Grant the Data Factory managed identity:

- `Storage Blob Data Reader` on the storage account or the `dftoazure` container so ADF can read the staged parquet file.
- An Azure SQL contained user with permission to insert into the target and staging tables.
- `EXECUTE` permission when using `method="upsert"` because ADF runs the generated upsert stored procedure.

Example SQL permissions for the Data Factory system-assigned managed identity:

```sql
CREATE USER [your-data-factory-name] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [your-data-factory-name];
ALTER ROLE db_datawriter ADD MEMBER [your-data-factory-name];
GRANT EXECUTE TO [your-data-factory-name];
```

Azure SQL must have a Microsoft Entra admin configured before `CREATE USER ... FROM EXTERNAL PROVIDER` works.

For user-assigned managed identity on Data Factory, assign the identity to the factory, create a Data Factory credential for it, and set `DF_TO_AZURE_ADF_CREDENTIAL_NAME` to that credential name.

Passwordless SQL connections also require a Microsoft ODBC Driver for SQL Server version that supports Microsoft Entra authentication modes such as `ActiveDirectoryDefault`. Use the newest available ODBC Driver 18 where possible.

## Maintained by [Zypp](https://github.com/zypp-io):
- [Melvin Folkers](https://github.com/melvinfolkers)
- [Erfan Nariman](https://github.com/erfannariman)

## Support:
For support on using this module, you can reach us at [hello@zypp.io](mailto:hello@zypp.io)

---

## Testing

To run the test suite, use:

```commandline
pytest df_to_azure
```

To run pytest for a single test:
```commandline
pytest df_to_azure/tests/test_df_to_azure.py::test_duplicate_keys_upsert
```
