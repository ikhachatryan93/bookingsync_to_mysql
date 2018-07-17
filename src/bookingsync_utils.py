from datetime import timedelta
from utilities import Cfg
import logging
import re
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


def get_fees_for_splitted_booking(booking_split, fees, bkg, portion=1):
    # initialize fee fields
    for fee_name in set(fee_names.values()):
        booking_split[fee_name] = ''

    # try to get fee info from booking comments, as some booking_fee data is not available via api
    # remove this if you can get all bookings_fee info in future
    if bkg['comments']:
        for fee_name in fee_names.keys():
            fee_info = re.search('{}(.*)'.format(fee_name.replace('_', ' ').lower()), bkg['comments'].lower())
            if fee_info:
                fee_initial = re.search('[0-9]+\.[0-9]*', fee_info[1])
                if fee_initial:
                    booking_split[fee_names[fee_name]] = to_float(fee_initial[0])

    # get fee info
    if fees:
        for fee in fees:
            booking_split[fee['name']] = to_float(fee['price'] * portion)


payment_rule = Cfg.get('bks_payment_rule')


def is_payment_splittable(key): return payment_rule[key]
