from datetime import timedelta
from utilities import Cfg
import logging
from utilities import to_float

fee_names = Cfg.get('fee_mapping')
a_day = timedelta(days=1)


def is_same_month(end, start): return start.month == end.month and start.year == end.year


def get_fee_name(fee):
    for loc in fee['name'].keys():
        if fee['name'][loc]:
            try:
                return fee_names[fee['name'][loc].lower()]
            except (KeyError, ValueError):
                continue

    logging.warning('No tax name in: '.format(fee))
    return ''


def get_fees_for_splitted_booking(booking_split, fees, portion=1):
    # initialize fee fields
    for fee_name in set(fee_names.values()):
        booking_split[fee_name] = ''

    # get fee info
    for fee in fees:
        booking_split[fee['name']] = to_float(fee['price'] * portion)


payment_rule = Cfg.get('bks_payment_rule')


def is_payment_splittable(key): return payment_rule[key]
