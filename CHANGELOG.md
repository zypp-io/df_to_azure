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
