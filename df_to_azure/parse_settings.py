import yaml
import os
import logging


class TableParameters:
    def __init__(self, df, tablename, schema, method, id_field, local):

        self.df = df
        self.name = tablename
        self.schema = schema
        self.method = method
        self.id_field = id_field
        self.local = local
        self.azure = self.select_settings()

    def select_settings(self):
        if self.local:
            logging.debug("using local settings in file 'azure.yml'")
            settings = get_settings("azure.yml")
        else:
            settings = get_settings(os.environ.get("DF_TO_AZURE_SETTINGS"))

        return settings


def get_settings(file_name):
    with open(file_name, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
