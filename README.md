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



## Azure parameters
because a connection to Azure services is required, we have to set some parameters in 2 seperate yml files.
##### Azure subscription settings
The details of the Azure subscription should be placed in the file  `settings/yml/adf_settings.yml`.<br>
There is a template for this file in the location `settings/templates/adf_settings.yml`
##### Azure data factory settings
The details of the Azure subscription should be placed in the file  `settings/yml/azure_settings.yml`.<br>
There is a template for this file in the location `settings/templates/azure_settings.yml`





## Maintainers:
- [Melvin Folkers](https://github.com/melvinfolkers)
