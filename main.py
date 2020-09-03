from src.log import set_logging
import logging
from datetime import datetime
from src import adf
from src.functions import print_settings
from src.export import upload_sample_dataset


def run():

    logging.info("insert scripts here...")
    print_settings()

    adf.create_resourcegroup()
    adf.create_datafactory()
    adf.create_blob_container()

    adf.create_linked_service_sql()
    adf.create_linked_service_blob()

    upload_sample_dataset()


if __name__ == "__main__":
    set_logging()
    logging.info(f"started script  at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    run()
