import logging
import os, sys
from datetime import datetime


def set_logging():

    logfilename = "runlog_" + datetime.now().strftime("%Y%m%d")
    full_path = os.path.join(os.getcwd(), "log")

    logFormatter = logging.Formatter("%(asctime)s [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger()

    fileHandler = logging.FileHandler("{0}/{1}.log".format(full_path, logfilename))
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)

    logging.getLogger().setLevel(logging.INFO)
