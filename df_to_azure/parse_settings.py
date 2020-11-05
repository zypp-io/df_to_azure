import yaml
import os
import logging


class TableParameters:
    def __init__(self, df, name, schema, method, id_field, yaml_path):

        self.df = df
        self.name = name
        self.schema = schema
        self.method = method
        self.id_field = id_field
        self.yaml_path = yaml_path
        self.azure = self.select_settings()

    def select_settings(self):
        if self.yaml_path:
            logging.debug("using local settings in file 'azure.yml'")
            settings = self.get_settings(self.yaml_path)
        else:
            settings = self.get_settings(os.environ.get("DF_TO_AZURE_SETTINGS"))

        return settings

    @staticmethod
    def get_settings(file_name):
        with open(file_name, "r") as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
