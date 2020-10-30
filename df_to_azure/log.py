import logging
import os
import sys
from datetime import datetime
from df_to_azure.functions import create_dir


def set_logging():

    logfilename = "runlog_" + datetime.now().strftime("%Y%m%d")
    full_path = os.path.join(os.getcwd(), "log")
    create_dir(full_path)

    log_formatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
    root_logger = logging.getLogger()

    file_handler = logging.FileHandler("{0}/{1}.log".format(full_path, logfilename))
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

    logging.getLogger().setLevel(logging.INFO)
