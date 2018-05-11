import logging
import os

dir_path = os.path.dirname(os.path.realpath(__file__))

logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("ThreadPool").setLevel(logging.CRITICAL)


def configure_logging(handler_type):
    logger = logging.getLogger()
    if "file" in str(handler_type):
        filename = dir_path + os.sep + "scraper.log"
        os.remove(filename) if os.path.exists(filename) else None
        handler = logging.FileHandler(filename=filename)
    else:
        handler = logging.StreamHandler()

    logFormatter = logging.Formatter("%(filename)s:%(lineno)s %(asctime)s [%(levelname)-5.5s]  %(message)s")
    handler.setFormatter(logFormatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
