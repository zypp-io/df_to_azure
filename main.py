from src.log import set_logging
from src.parse_settings import get_settings
import logging
from datetime import datetime

settings = get_settings("settings/yml/run_settings.yml")


def run():

    logging.info("insert scripts here...")


if __name__ == "__main__":
    set_logging()
    logging.info(f"started script  at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    run()
