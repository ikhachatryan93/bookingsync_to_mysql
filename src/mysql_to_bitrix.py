import logging_setup
import logging
import traceback
from bookingsync import run_bookingsync
import bitrix

logging_setup.configure_logging('stream')


def main():
    try:
        #run_bookingsync()
        bitrix.run_bitrix()
    except:
        logging.critical(traceback.format_exc())


if __name__ == "__main__":
    main()
