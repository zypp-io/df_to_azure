<img src="https://static.wixstatic.com/media/a9ca5e_825bd4d39e7d468faf735b801fa3dea4~mv2.png/v1/fill/w_1458,h_246,al_c,usm_0.66_1.00_0.01/a9ca5e_825bd4d39e7d468faf735b801fa3dea4~mv2.png" width="200">

# Azure ADF

> Repository for Automatically creating pipelines with copy Activity from blob to SQL

## introduction

The purpose of this project is to upload large datasets using Azure Data Factory combined with a Azure SQL Server. 
In steps the following process kicks off:<p>
    1. The data will be uploaded as a .csv file to Azure Blob storage.<br>
    2. A SQL Database table is prepared based on the parameters in the `settings/yml/adf_settings.yml`<br>
    3. A pipeline is created for uploading the .csv from the Blob storage into the SQL table.<br>
    4. The pipeline is triggered, so that the .csv file is bulk inserted into the SQL table.<br>

## How it works

based on the following attributes, it is possible to bulk insert your dataframe into the SQL Database:

`run(df, tablename, schema, incremental=True, id_field="col_a")`

        1. df: dataframe you wish to export
        2. tablename: desired name of the table 
        3. schema: desired sql schema
        4. incremental:option for only inserting new data
        5. id_field: id field of the table. necessary if 4 is set to True

##### Incremental or full update
it is possible to only update the SQL table with new records, if present in the dataframe you want to upload.<br>
Based on the id_field, the SQL table is being checked on overlapping values.<br>
If there are new records, the "old" records will be deleted in the SQL table. <br>
The new records will be uploaded and appended to the current SQL table.

# Settings
To use this module, you need to add the `azure subscriptions settings` and `azure data factory settings` to your YAML file.
Use the following template:
```yaml
# --- ADF SETTINGS ---
# general run settings
create : False

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

# pipeline settings
trigger : True

# --- AZURE SETTINGS ---
# azure credentials for connecting to azure subscription.
client_id : ""
secret : ""
tenant : ""
subscription_id : ""
```

Place this YAML file somewhere on your laptop. Then add the path to your environment in the variable `DF_TO_AZURE_SETTINGS`. 
For example `DF_TO_AZURE_SETTINGS='/Users/myname/settings.yml'`
The script will use `os.environ.get('DF_TO_AZURE_SETTINGS')` to import the settings in the YAML file.

## Maintainers:
- [Melvin Folkers](https://github.com/melvinfolkers)
