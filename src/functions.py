import logging, os
from src.parse_settings import get_settings
import pandas as pd


def print_item(group):
    """Print an Azure object instance."""
    logging.info("\tName: {}".format(group.name))
    logging.info("\tId: {}".format(group.id))
    if hasattr(group, "location"):
        logging.info("\tLocation: {}".format(group.location))
    if hasattr(group, "tags"):
        logging.info("\tTags: {}".format(group.tags))
    if hasattr(group, "properties"):
        print_properties(group.properties)


def print_properties(props):
    """Print a ResourceGroup properties instance."""
    if props and hasattr(props, "provisioning_state") and props.provisioning_state:
        logging.info("\tProperties:")
        logging.info("\t\tProvisioning State: {}".format(props.provisioning_state))
    logging.info("")


def print_activity_run_details(activity_run):
    """Print activity run details."""
    logging.info("\n\tActivity run details\n")
    logging.info("\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == "Succeeded":
        logging.info("\tNumber of bytes read: {}".format(activity_run.output["dataRead"]))
        logging.info("\tNumber of bytes written: {}".format(activity_run.output["dataWritten"]))
        logging.info("\tCopy duration: {}".format(activity_run.output["copyDuration"]))
    else:
        logging.info("\tErrors: {}".format(activity_run.error["message"]))


def print_settings():
    adf_settings = get_settings("settings/yml/adf_settings.yml")
    azure_settings = get_settings("settings/yml/azure_settings.yml")

    logging.info(10 * "*" + "\nAZURE SETTINGS\n" + 10 * "*")
    for k, v in azure_settings.items():
        logging.info(k + " = " + v)
    logging.info(34 * "*" + "\n")

    logging.info(10 * "*" + "\nAZURE DATA FACTORY SETTINGS\n" + 10 * "*")
    for k, v in adf_settings.items():
        logging.info(k + " = " + v)
    logging.info(47 * "*" + "\n")


def cat_modules(directory, tablename):

    all = list()
    for file in os.listdir(directory):

        if file.find(tablename) == -1:
            continue

        file_path = os.path.join(directory, file)
        df = pd.read_pickle(file_path)
        all.append(df)

    data = pd.concat(all, axis=0, sort=False)

    logging.info(f"imported {len(all)} file(s) for table '{tablename}'")

    return data
