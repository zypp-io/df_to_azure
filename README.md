<div style="text-align:center"><img alt="logo" src="https://www.zypp.io/static/assets/img/logos/zypp/white/500px.png" width="200"></div>

# DF to Azure

> Python module for fast upload of pandas DataFrame to azure using automatic created pipelines in Azure Data Factory.

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

df_to_azure(df=df, tablename="table_name", schema="schema", method="create", id_field="col_a")
```

1. `df`: dataframe you wish to export
2. `tablename`: desired name of the table 
3. `schema`: desired sql schema
4. `method`: option for "create" "append" or "upsert"
5. `id_field`: id field of the table. Necessary if 4 is set to "upsert"

**Important**: the csv's are uploaded to a container called `dftoazure`, so create this in your storage account before using this module.

##### Upsert / create or append
It is possible to upsert the SQL table with (new) records, if present in the dataframe you want to upload.
Based on the id_field, the SQL table is being checked on overlapping values.
If there are new records, the "old" records will be updated in the SQL table.
The new records will be uploaded and appended to the current SQL table.

# Settings
To use this module, you need to add the `azure subscriptions settings` and `azure data factory settings` to your environment variables.
We recommend to work with `.env` files (or even better, automatically load them with [Azure Keyvault](https://pypi.org/project/keyvault/)) and load them in during runtime. But this is optional and they can be set as system variables as well.
Use the following template when using `.env`

```text
# --- ADF SETTINGS ---
# general run settings
create="False"

# data factory settings
rg_name : ""
rg_location: "westeurope"
df_name : ""

# blob settings
ls_blob_name : "Python Blob Linked Service"
ls_blob_account_name : ""
ls_blob_container_name : ""
ls_blob_account_key : ""

# SQL settings
ls_sql_name : "Python SQL Linked Service"
ls_sql_server_name: ""
ls_sql_database_name: ""
ls_sql_database_user: ""
ls_sql_database_password: ""

# --- AZURE SETTINGS ---
# azure credentials for connecting to azure subscription.
client_id : ""
secret : ""
tenant : ""
subscription_id : ""
```

## Maintained by Zypp:
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
pytest df_to_azure/tests/test_df_to_azure::test_duplicate_keys_upsert
```