import logging_setup
import logging
import traceback
from bookingsync import run_bookingsync
logging_setup.configure_logging('file')


def main():
    try:
        run_bookingsync()
        logging.info("Completed bookingsync to mysql itegration!!!")
    except:
        logging.critical(traceback.format_exc())


if __name__ == "__main__":
    main()
