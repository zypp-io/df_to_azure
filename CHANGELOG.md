Changelog
===

# 0.6.0
- Added support for uploading parquet files (create and append)
- Added dependency pyarrow for handling parquet export

# 0.5.4
- Added requirements to setup.cfg

# 0.5.3

- Added `dtype` argument, so users can specify their own SqlAlchemy dtype for certain columns.

# 0.5.2

- Removed the required environment variables `ls_sql_name` and `ls_blob_name` since it's standard.

# 0.5.3

- Added `dtype` argument, so users can specify their own SqlAlchemy dtype for certain columns.

# 0.5.4

- Set requirements to specific version

# 0.6.0

- Always create linked services for blob and sql. This way the user can switch source blob storages and sink databases easier.
- Use environment variable AZURE_STORAGE_CONNECTION_STRING for parquet upload
- If parquet upload is a single file, place it in the root of the folder

# 0.7.0

- Add upsert for parquet files
- Logging level to debug for query
- Fix bugs and add requirements checks
- Fix bug for staging schema with upsert
- Add support all pandas int dtypes
- Add customisable container name

# 0.8.0

- Upgrade dependency packages
- Fix failing pipeline because of removed staging schema

# 0.9.0

- Add more dtypes
- Upgrade package version
- Fix bug when dataframe is empty

# 0.9.1

- Fix bug categorical dtype
- Make pyodbc driver dynamic

# 1.0.0

- Upgrade packages and set minimal versions
- Fix code to work with upgraded packages
- Export to parquet on storage instead of csv
