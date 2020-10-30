import yaml
import os


def get_settings(file_name):
    with open(file_name, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


current_dir = os.path.dirname(__file__)
yml_dir = os.path.join(current_dir, "../settings/yml")

adf_settings = get_settings(os.path.join(yml_dir, "adf_settings.yml"))
azure_settings = get_settings(os.path.join(yml_dir, "azure_settings.yml"))
