# AGENTS.md

This file provides guidance to coding agents when working with code in this repository.

## Project Overview

`df_to_azure` is a Python library for fast upload of pandas DataFrames to Azure SQL Database using automatically created pipelines in Azure Data Factory. The library supports three methods: `create`, `append`, and `upsert`, and can also upload DataFrames as parquet files to Azure Blob Storage.

## Development Commands

### Running Tests
```bash
# Unit tests, no Azure needed
pytest df_to_azure/tests/test_auth.py

# Full suite: integration tests that need real Azure resources,
# see the Testing section in README.md for the required environment variables
pytest df_to_azure
```

### Linting and Code Quality
```bash
# Run pre-commit hooks on all files
pre-commit run --all-files

# Verify setup.cfg and requirements.txt are in sync
python scripts/check_setupcfg_and_requirements_equal.py
```

### Code Style
- Line length: 120 characters
- Formatter: ruff (configured via ruff.toml and .pre-commit-config.yaml)
- The project uses ruff for both linting and formatting

## Architecture Overview

### Core Components

1. **export.py** - Main entry point
   - `df_to_azure()`: Primary function users interact with
   - `DfToAzure` class: Orchestrates the SQL upload workflow
   - `DfToParquet` class: Handles parquet file uploads to blob storage

2. **adf.py** - Azure Data Factory Management
   - `ADF` class: Creates and manages Data Factory pipelines
   - Handles linked services (Blob, SQL), datasets, and pipeline creation
   - Creates copy activities and stored procedure activities for upsert

3. **db.py** - Database Operations
   - `SqlUpsert` class: Generates MERGE queries for upsert operations
   - `auth_azure()`: Creates SQLAlchemy engine connections
   - `get_sql_driver()`: Automatically detects ODBC drivers (filters for SQL Server drivers only)
   - `execute_stmt()`: Executes SQL statements via SQLAlchemy

4. **settings.py** - Configuration & Validation
   - `TableParameters` class: Validates input parameters
   - Ensures method is valid (`create`, `append`, or `upsert`)
   - Validates id_field is provided when method is `upsert`

5. **utils.py** - Helper Functions
   - `wait_until_pipeline_is_done()`: Monitors ADF pipeline execution (3-hour timeout)
   - `test_uniqueness_columns()`: Validates uniqueness of id columns for upsert
   - `test_unique_column_names()`: Ensures DataFrame columns have unique names

### Data Flow

#### SQL Upload (method='create', 'append', or 'upsert'):
1. DataFrame is converted to parquet and uploaded to blob storage container `dftoazure`
2. SQL table is created based on pandas dtypes → SQLAlchemy types conversion
3. For upsert: staging schema and stored procedure are created
4. ADF pipeline with copy activity is created and triggered
5. Data is bulk inserted from blob to SQL via ADF pipeline
6. For upsert: stored procedure executes MERGE query, then staging table is cleaned up

#### Parquet Upload (parquet=True):
1. DataFrame is converted to parquet format
2. Uploaded directly to blob storage container (default: `parquet`)
3. Folder structure: `{schema}/{tablename}.parquet`
4. For method='append': timestamp suffix is added to filename
5. For method='upsert': existing parquet is downloaded, merged, and re-uploaded

### Type Conversion

The library converts pandas dtypes to SQLAlchemy types (in `column_types()` method):
- String → String(length=255 or auto-detected)
- Boolean → Boolean()
- Integer → Integer() or BigInteger() (auto-detected based on value range)
- Float → Numeric(precision=18, scale=2)
- Datetime → DateTime()
- Timedelta → Converted to seconds, then stored as numeric
- Categorical → String(length=255)

String length auto-detection:
- Default: 255
- If max length > 255 and < 8000: uses actual max length
- If max length > 8000: uses String(length=None) for unlimited

### Environment Variables

Required environment variables (checked in `ADF.check_env_variables()`):
- `subscription_id` - Azure subscription ID
- `rg_name`, `rg_location`, `df_name` - Resource group and Data Factory settings
- `SQL_SERVER`, `SQL_DB` - Azure SQL database
- `ls_blob_account_name` - Blob storage (not required when `AZURE_STORAGE_CONNECTION_STRING` is set)

Authentication is passwordless by default (`DefaultAzureCredential` / managed identity). Explicit credentials win
when set:
- `SQL_USER`, `SQL_PW` - SQL password authentication
- `AZURE_STORAGE_CONNECTION_STRING` or `ls_blob_account_key` - storage secrets
- `DF_TO_AZURE_ADF_CREDENTIAL_NAME` - user-assigned managed identity for the ADF SQL linked service

### Upsert Logic

SQL upsert:
- Creates a `staging` schema with temporary table
- Generates a stored procedure named `UPSERT_{tablename}`
- Uses SQL MERGE statement to update matching rows and insert new rows
- Based on id_field(s) for matching
- After pipeline completes, staging table is dropped (unless `clean_staging=False`)

Parquet upsert:
- Downloads existing parquet file from blob storage
- If new data has NaN values: uses `pd.concat()` + `drop_duplicates()`
- If new data has no NaN values: uses `combine_first()` for better performance
- Uploads merged DataFrame back to blob storage

## Important Conventions

- The blob container `dftoazure` must exist before using SQL upload methods
- Pipeline names follow pattern: `{schema} {tablename} to SQL`
- Dataset names: `BLOB_dftoazure_{tablename}` and `SQL_dftoazure_{tablename}`
- Linked service names are generated from server/database names with special characters replaced by `-`
- Datetime columns are converted to strings before parquet upload to avoid ADF conversion issues
- Maximum pipeline wait time: 3 hours
- All tests live in `df_to_azure/tests/`; `test_auth.py` is unit-level, the rest are integration tests that need real Azure resources
