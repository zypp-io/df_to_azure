import os
import yaml

# global variables
CREATE = True if os.environ.get("create") == "True" else False
CLIENT_ID = os.environ.get("client_id")
SECRET = os.environ.get("secret")
TENANT = os.environ.get("tenant")
SUBSCRIPTION_ID = os.environ.get("subscription_id")
RG_LOCATION = os.environ.get("rg_location")
RG_NAME = os.environ.get("rg_name")
DF_NAME = os.environ.get("df_name")
LS_BLOB_ACCOUNT_NAME = os.environ.get("ls_blob_account_name")
LS_BLOB_ACCOUNT_KEY = os.environ.get("ls_blob_account_key")
LS_BLOB_CONTAINER_NAME = os.environ.get("ls_blob_container_name")
LS_BLOB_NAME = os.environ.get("ls_blob_name")
LS_SQL_SERVER_NAME = os.environ.get("ls_sql_server_name")
LS_SQL_DATABASE_NAME = os.environ.get("ls_sql_database_name")
LS_SQL_DATABASE_USER = os.environ.get("ls_sql_database_user")
LS_SQL_DATABASE_PASSWORD = os.environ.get("ls_sql_database_password")
LS_SQL_NAME = os.environ.get("ls_sql_name")
TRIGGER = True if os.environ.get("trigger") == "True" else False


class TableParameters:
    def __init__(self, df, name, schema, method, id_field, cwd):

        self.df = df
        self.name = name
        self.schema = schema
        self.method = method
        self.id_field = id_field
        self.cwd = cwd

    @staticmethod
    def get_settings(file_name):
        with open(file_name, "r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
