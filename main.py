import logging
from df_to_azure.log import set_logging
from datetime import datetime
import time
from df_to_azure import tests


if __name__ == "__main__":
    set_logging()
    logging.info(f"started script  at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # testing.
    # tests.test_create()

    # time.sleep(3)

    # tests.test_upsert()

    tests.test_run_multiple()
