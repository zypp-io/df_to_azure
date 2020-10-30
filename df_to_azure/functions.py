import logging
import os
from parse_settings import get_settings
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
        logging.info(
            "\tNumber of bytes read: {}".format(activity_run.output["dataRead"])
        )
        logging.info(
            "\tNumber of bytes written: {}".format(activity_run.output["dataWritten"])
        )
        logging.info("\tCopy duration: {}".format(activity_run.output["copyDuration"]))
    else:
        logging.info("\tErrors: {}".format(activity_run.error["message"]))


def print_settings():
    settings = get_settings(os.environ.get("AZURE_TO_DF_SETTINGS"))

    logging.info(10 * "*" + "\nAZURE & ADF SETTINGS\n" + 10 * "*")
    for k, v in settings.items():
        logging.info(k + " = " + v)
    logging.info(34 * "*" + "\n")


def cat_modules(directory, tablename):

    all_files = list()
    for file in os.listdir(directory):

        if file.find(tablename) == -1:
            continue

        file_path = os.path.join(directory, file)
        print(file_path)
        df = pd.read_pickle(file_path)
        all_files.append(df)

    data = pd.concat(all_files, axis=0, sort=False)

    logging.info(f"imported {len(all_files)} file(s) for table '{tablename}'")

    return data


def create_dir(destination):
    try:
        if not os.path.exists(destination):
            os.makedirs(destination)
    except OSError:
        logging.warning("Error Creating directory. " + destination)
    return destination
