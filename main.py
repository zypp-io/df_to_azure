from src.log import set_logging
import logging
from datetime import datetime
from src import adf
from src.functions import print_settings
from src.export import upload_sample_dataset
from src.parse_settings import get_settings

adf_settings = get_settings("settings/yml/adf_settings.yml")


def run():

    logging.info("insert scripts here...")
    # print_settings()

    if adf_settings["create"]:
        # azure components
        adf.create_resourcegroup()
        adf.create_datafactory()
        adf.create_blob_container()

        # linked services
        adf.create_linked_service_sql()
        adf.create_linked_service_blob()

    # datasets
    tablename = "sample"
    upload_sample_dataset(tablename)
    adf.create_input_blob(tablename)
    adf.create_output_sql(tablename)

    # pipelines
    adf.create_pipeline(tablename)


if __name__ == "__main__":
    set_logging()
    logging.info(f"started script  at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    run()
