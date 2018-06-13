import logging_setup
import logging
import traceback
import bitrix

logging_setup.configure_logging('file')


def main():
    try:
        bitrix.run_bitrix()
    except:
        logging.critical(traceback.format_exc())


if __name__ == "__main__":
    main()
