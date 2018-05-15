import logging_setup
import logging
import traceback
from bookingsync import get_bookingsync_data

logging_setup.configure_logging('file')


def main():
    try:
        get_bookingsync_data()
    except:
        logging.critical(traceback.format_exc())


if __name__ == "__main__":
    main()
