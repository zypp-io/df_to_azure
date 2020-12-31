import logging
import os
import pandas as pd


def print_item(group):
    """
    Print an Azure object instance.
    :param group: Azure group
    """
    logging.info("\tName: {}".format(group.name))
    logging.info("\tId: {}".format(group.id))
    if hasattr(group, "location"):
        logging.info("\tLocation: {}".format(group.location))
    if hasattr(group, "tags"):
        logging.info("\tTags: {}".format(group.tags))
    if hasattr(group, "properties"):
        print_properties(group.properties)


def print_properties(props):
    """
    Print a ResourceGroup properties instance.
    :param props: Azure ResourceGroup property
    """
    if props and hasattr(props, "provisioning_state") and props.provisioning_state:
        logging.info("\tProperties:")
        logging.info(f"\t\tProvisioning State: {props.provisioning_state}")
    logging.info("")


def print_activity_run_details(activity_run):
    """
    Print activity run details.
    :param activity_run: Azure activity
    """
    logging.info("\n\tActivity run details\n")
    logging.info(f"\tActivity run status: {activity_run.status}")
    if activity_run.status == "Succeeded":
        logging.info(f"\tNumber of bytes read: {activity_run.output['dataRead']}")
        logging.info(f"\tNumber of bytes written: {activity_run.output['dataWritten']}")
        logging.info(f"\tCopy duration: {activity_run.output['copyDuration']}")
    else:
        logging.info(f"\tErrors: {activity_run.error['message']}")


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
