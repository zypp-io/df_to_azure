from src.log import set_logging
import logging
from datetime import datetime
from src import adf
from src.export import upload_dataset
from src.parse_settings import get_settings
import os
import pandas as pd

# azure settings
adf_settings = get_settings("settings/yml/adf_settings.yml")


def run(tablename, df):

    if adf_settings["create"]:
        # azure components
        adf.create_resourcegroup()
        adf.create_datafactory()
        adf.create_blob_container()

        # linked services
        adf.create_linked_service_sql()
        adf.create_linked_service_blob()

    upload_dataset(tablename, df)
    adf.create_input_blob(tablename)
    adf.create_output_sql(tablename)

    # pipelines
    adf.create_pipeline(tablename)


if __name__ == "__main__":
    set_logging()
    logging.info(f"started script  at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # datasets
    tablename = "sample"
    df = pd.read_csv(f"/Users/melvinfolkers/Documents/github/azure_adf/data/sample/{tablename}.csv")

    # file_path = '/Users/melvinfolkers/Documents/github/workgenda/staging/carriere'
    # tablenames = os.listdir(file_path)
    # tablenam = ''
    run(tablename, df)
