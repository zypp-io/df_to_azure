import logging
import os
from datetime import datetime

logging.getLogger().setLevel(logging.INFO)


class RunParameters:
    def __init__(self):

        self.projectdir = os.getcwd()
        self.inputdir = "/Users/melvinfolkers/ShareFile/Shared Folders/Orinco/Steekproef/proefbestand/"

        self.datadir = self.create_dir(destination=os.path.join(self.projectdir, "data"))
        self.logdir = self.create_dir(destination=os.path.join(self.datadir, "log"))
        self.pickledir = self.create_dir(destination=os.path.join(self.datadir, "pickles"))
        self.stagingdir = self.create_dir(destination=os.path.join(self.datadir, "staging"))
        self.starttijd = self.set_logging()

    def create_dir(self, destination):

        try:
            if not os.path.exists(destination):
                os.makedirs(destination)
        except OSError:
            logging.warning("Error Creating directory. " + destination)
        return destination

    def set_logging(self):
        start = datetime.now()

        logfilename = "runlog_" + start.strftime(format="%Y%m%d_%H%M") + ".log"
        full_path = os.path.join(self.logdir, logfilename)

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            filename=full_path, level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
        )

        # define a Handler which writes INFO messages or higher to the sys.stderr
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        # add the handler to the root logger
        logging.getLogger("").addHandler(console)

        return start


def mainscript():
    logging.info("hier een beetje magie.")

    run_params = RunParameters()


if __name__ == "__main__":
    mainscript()
