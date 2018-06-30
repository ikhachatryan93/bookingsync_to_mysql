import json
import os
import sys
import time
from datetime import datetime

import requests

from bookingsync_utils import *
from mysql_wrapper import MySQL
from utilities import Cfg
from utilities import find_all_dicts_in_list
from utilities import find_dict_in_list
from utilities import is_number
from utilities import to_float
from utilities import to_int
from utilities import write_data_to_db

dir_path = os.path.dirname(os.path.realpath(__file__))

sys.path.append(os.path.join(dir_path, "drivers"))

_json_file = dir_path + os.sep + '..' + os.sep + Cfg.get('bks_token_file')
_client_id = Cfg.get('bks_client_id')
_client_secret = Cfg.get('bks_client_secret')
_redirect_uri = Cfg.get('bks_redirect_uri')
_native_date_format = '%Y-%m-%dT%H:%M:%SZ'

with open(_json_file, 'r') as f:
    _token = json.load(f)


def update_token():
    global _token
    req = requests.post('https://www.bookingsync.com/oauth/token', params={'client_id': _client_id,
                                                                           'client_secret': _client_secret,
                                                                           'refresh_token': _token['refresh_token'],
                                                                           'grant_type': 'refresh_token',
                                                                           'scope': _token['scope'],
                                                                           'redirect_uri': _redirect_uri})
    if req.status_code == 200:
        _token = json.loads(req.content.decode('utf-8'))
        with open(_json_file, 'w') as f:
            json.dump(_token, f, ensure_ascii=False)


def request_data(url, params=None, rec=True):
    headers = {'Authorization': str('Bearer ' + _token['access_token']), 'content-type': 'application/json',
               'charset': 'UTF-8'}

    req = requests.get(url, headers=headers, params=params)
    if req.status_code == 401:
        if rec:
            update_token()
            time.sleep(10)
            return request_data(url, params, rec=False)
        else:
            raise Exception('401 error while requesting {}'.format(url))
    elif req.status_code == 429:
        # wait until request counts will be updated
        if rec:
            reset_time = datetime.fromtimestamp(int(req.headers['X-RateLimit-Reset']))
            time.sleep((reset_time - datetime.now()).seconds)
            a = request_data(url, params, rec=False)
            return a
        else:
            raise Exception('429 error, something went wrong with rate limit handler'.format(url))

    elif req.status_code != 200:
        raise Exception('{} error while requesting a {}'.format(req.status_code, url))

    return json.loads(req.content.decode('utf-8'))


def advanced_request(target, ids=None, fields=None, params={}):
    # e.g. target is account and url will be accounts_base_url
    url_base = Cfg.get('bks_' + target + 's_base_url')

    params['per_page'] = Cfg.get('bks_x_per_page')
    if ids:
        params['id'] = ids
    if fields:
        params['fields'] = fields

    req = request_data(url_base, params)
    data = req[target + 's']
    total = int(req['meta']['X-Total-Pages'])
    if total > 1:
        params['per_page'] = req['meta']['X-Per-Page']
        for i in range(2, total + 1):
            params['page'] = i
            data += request_data(url_base, params)[target + 's']

    return data


def to_datetime(st):
    if st:
        try:
            return datetime.strptime(st, _native_date_format)
        except ValueError:
            logging.error('Could not convert date from {} to {}'.format('st', _native_date_format))
    return ''


def get_bookings(bookings):
    my_bookings = []

    t_ren = time.time()
    sources = advanced_request('source', params={'from': '20111101', 'fields': 'id,name'})
    logging.info('Obtained Sources data in {} sec'.format(time.time() - t_ren))

    t_ren = time.time()
    comments = advanced_request('booking_comment', params={'from': '20111101', 'fields': 'id,content'})
    logging.info('Obtained Comments data in {} sec'.format(time.time() - t_ren))

    t_ren = time.time()
    accounts = request_data(Cfg.get('bks_accounts_base_url'), params={'fields': 'id,business_name'})
    logging.info('Obtained Accounts data in {} sec'.format(time.time() - t_ren))

    for b in bookings:
        if b['status'] == 'Canceled':
            if (to_datetime(b['start_at']).date() - to_datetime(b['canceled_at']).date()).days > Cfg.get('btx_payed_status_interval'):
                continue

        my_booking = {'id': int(b['id']),
                      'client_id': to_int(b['links']['client']),
                      'rental_id': to_int(b['links']['rental']),
                      'start_at': to_datetime(b['start_at']),
                      'end_at': to_datetime(b['end_at']),
                      'created_at': to_datetime(b['created_at']),
                      'canceled_at': to_datetime(b['canceled_at']),
                      'balance_due_at': to_datetime(b['balance_due_at']),
                      'tentative_expires_at': to_datetime(b['tentative_expires_at']),
                      'updated_at': to_datetime(b['updated_at']),
                      'contract_updated_at': to_datetime(b['contract_updated_at']),
                      'status': b['status'],
                      'reference': b['reference'],
                      'booked': b['booked'],
                      'unavailable': b['unavailable'],
                      'initial_price': to_float(b['initial_price']),
                      'initial_rental_price': to_float(b['initial_rental_price']),
                      'channel_price': to_float(b['channel_price']),
                      'discount': b['discount'],
                      'final_rental_price': to_float(b['final_rental_price']),
                      'final_price': to_float(b['final_price']),
                      'paid_amount': to_float(b['paid_amount']),
                      'currency': b['currency'],
                      'notes': b['notes'],
                      'damage_deposit': to_float(b['damage_deposit']),
                      'charge_damage_deposit_on_arrival': to_float(b['charge_damage_deposit_on_arrival']),
                      'adults': to_int(b['adults']),
                      'children': to_int(b['children']),
                      'bookings_payments_count': to_int(b['bookings_payments_count']),
                      'review_requests_count': to_int(b['review_requests_count']),
                      'locked': b['locked'],
                      'cancelation_reason': b['cancelation_reason'],
                      'expected_checkin_time': b['expected_checkin_time'],
                      'expected_checkout_time': b['expected_checkout_time'],
                      'payment_url': b['payment_url'],
                      'rental_payback_to_owner': to_float(b['rental_payback_to_owner']),
                      'final_payback_to_owner': to_float(b['final_payback_to_owner']),
                      'commission': to_float(b['commission']),
                      'door_key_code': b['door_key_code'],
                      'payment_left_to_collect': to_float(b['payment_left_to_collect']),
                      'owned_by_app': b['owned_by_app'],
                      'account_id': to_int(b['links']['account']),
                      'probability_win': None, 'source': b['links']['source'] if b['links']['source'] else 'Praguestars.com'}

        if my_booking['source']:
            source = find_dict_in_list(sources, 'id', my_booking['source'])
            if source:
                my_booking['source'] = source['name']

        if my_booking['start_at']:
            days_interval = (my_booking['start_at'] - datetime.now()).days
            my_booking['probability_win'] = to_int(Cfg.get_interval_prob(days_interval))

        # get comments
        comment_str = ''
        for comment_id in b['links']['booking_comments']:
            comment = find_dict_in_list(comments, 'id', comment_id)
            if comment:
                comment_str += comment['content'] + '\n'
        my_booking['comments'] = comment_str

        my_bookings.append(my_booking)

    # get account name
    for b in my_bookings:
        for a in accounts['accounts']:
            if len(a) > 0 and a['id'] == b['account_id']:
                b['account'] = a['business_name']

    return my_bookings


def get_rentals(rentals):
    my_rentals = []
    for r in rentals:
        my_rentals.append({'id': r['id'],
                           'rental_type': r['rental_type'],
                           'created_at': to_datetime(r['created_at']),
                           'updated_at': to_datetime(r['updated_at']),
                           'published_at': to_datetime(r['published_at']),
                           'name': r['name'],
                           'address1': r['address1'],
                           'address2': r['address2'],
                           'currency': r['currency'],
                           'bedrooms_count': to_int(r['bedrooms_count']),
                           'bathrooms_count': to_int(r['bathrooms_count']),
                           'sleeps': to_int(r['sleeps']),
                           'sleeps_max': to_int(r['sleeps_max']),
                           'city': r['city'],
                           'country_code': r['country_code'],
                           'contact_name': r['contact_name'],
                           'zip': r['zip'],
                           'notes': r['notes'],
                           'base_rate': to_float(r['base_rate']),
                           'base_rate_kind': r['base_rate_kind'],
                           'downpayment': to_float(r['downpayment']),
                           'initial_price': to_float(r['initial_price']),
                           'final_price': to_float(r['final_price']),
                           'damage_deposit': to_float(r['damage_deposit']),
                           'min_price': to_float(r['min_price']),
                           'max_price': to_float(r['max_price']),
                           'absolute_min_price': to_float(r['absolute_min_price'])})

    return my_rentals


def get_clients(clients):
    my_clients = []
    for c in clients:
        my_client = {'id': c['id'], 'email': ''}

        # emails
        for e in c['emails']:
            my_client['email'] += e['email'] + ', '

        my_client['email'] = my_client['email'].strip(', ')

        # phones
        my_client['phone'] = ''
        my_client['mobile'] = ''
        for phone in c['phones']:
            label = phone['label']
            if label == 'phone' or label == 'mobile':
                my_client[label] = phone['number']

        # address details
        my_labels = ['address1', 'address2', 'city', 'zip', 'state', 'country_code']
        for info in c['addresses']:
            label = info['label']
            if label in my_labels:
                my_client[label] = info[label]
        for label in my_labels:
            if label not in my_client:
                my_client[label] = ''

        my_client['updated_at'] = to_datetime(c['updated_at'])
        my_client['fullname'] = c['fullname']
        my_client['firstname'] = c['firstname']
        my_client['lastname'] = c['lastname']
        my_client['preferred_locale'] = c['preferred_locale']
        my_client['notes'] = c['notes']
        my_client['company'] = c['company']
        my_client['passport'] = c['passport']
        my_client['vat_number'] = c['vat_number']

        my_clients.append(my_client)

    return my_clients


def get_bookings_fee(bookings_fee):
    my_bfees = []
    for fee in bookings_fee:
        my_bfee = {'id': fee['id'], 'updated_at': to_datetime(fee['updated_at']),
                   'created_at': to_datetime(fee['created_at']), 'booking_id': to_int(fee['links']['booking']),
                   'included_in_price': fee['included_in_price'], 'price': to_float(fee['price']),
                   'required': fee['required'], 'times_booked': int(fee['times_booked']), 'name': get_fee_name(fee)}

        my_bfees.append(my_bfee)
    return my_bfees


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def add_rental_data(booking_split, rental, proportion=1):
    payments = ['downpayment', 'min_price', 'max_price']
    for payment in payments:
        if is_payment_splittable(payment) and is_number(payment):
            booking_split[payment] = rental[payment] * proportion
        else:
            booking_split[payment] = rental[payment]


def add_booking_data(booking_split, booking, proportion=1):
    exceptions = ['start_at', 'end_at', 'id']
    prices = [
        'initial_price', 'initial_rental_price', 'charge_damage_deposit_on_arrival', 'channel_price', 'vat',
        'city_tax', 'cleaning_fee', 'discount', 'final_rental_price', 'rental_min_price', 'rental_max_price',
        'final_price', 'payment_left_to_collect', 'damage_deposit', 'paid_amount']

    for key, val in booking.items():
        if key not in exceptions:
            if key in prices and is_payment_splittable(key) and is_number(val):
                booking_split[key] = to_float(val * proportion)
            else:
                booking_split[key] = val


def get_bookings_splitted(bookings_fee, bookings, rentals):
    bookings_splitted = []

    for bkg in bookings:

        # if booking id is 111 then booking_split_0 id will be 11100 and booking_split_1 id will be 11101
        id_ = int(bkg['id']) * 100

        # find rental related to this booking
        rental = find_dict_in_list(rentals, 'id', bkg['rental_id'])

        # find all fees related to this booking
        fees = find_all_dicts_in_list(bookings_fee, 'booking_id', bkg['id'])

        start_at = bkg['start_at'].date()
        end_at = bkg['end_at'].date()
        total_days = (end_at - start_at).days

        if is_same_month(start_at, end_at):
            booking_split = {'id': id_,
                             'booking_id': bkg['id'],
                             'booking_fee_ids': ','.join([str(fee['id']) for fee in fees]),
                             'start_at': start_at,
                             'end_at': end_at,
                             'number_of_nights': total_days}

            get_fees_for_splitted_booking(booking_split, fees)
            add_booking_data(booking_split, bkg)
            add_rental_data(booking_split, rental)

            bookings_splitted.append(booking_split)
        else:
            current_date = start_at
            current_start_date = start_at

            while current_date <= end_at:
                if current_date.month != current_start_date.month:
                    number_of_nights = (current_date - current_start_date).days
                    booking_split = {'id': id_,
                                     'booking_id': bkg['id'],
                                     'booking_fee_ids': ','.join([str(fee['id']) for fee in fees]),
                                     'start_at': current_start_date,
                                     'end_at': current_date - a_day,
                                     'number_of_nights': number_of_nights}

                    proportion = number_of_nights / total_days

                    get_fees_for_splitted_booking(booking_split, fees, proportion)

                    add_booking_data(booking_split, bkg, proportion)
                    add_rental_data(booking_split, rental, proportion)

                    bookings_splitted.append(booking_split)

                    current_start_date = current_date
                    id_ += 1

                # last day
                elif current_date == end_at:
                    number_of_nights = (current_date - current_start_date).days
                    booking_split = {'id': id_,
                                     'booking_id': bkg['id'],
                                     'booking_fee_ids': ','.join([str(fee['id']) for fee in fees]),
                                     'start_at': current_start_date,
                                     'end_at': current_date,
                                     'number_of_nights': number_of_nights}

                    proportion = number_of_nights / total_days
                    get_fees_for_splitted_booking(booking_split, fees, number_of_nights / total_days)

                    add_booking_data(booking_split, bkg, proportion)
                    add_rental_data(booking_split, rental, proportion)

                    bookings_splitted.append(booking_split)
                    break

                current_date += a_day

    return bookings_splitted


def run_bookingsync():
    t_total = time.time()
    logging.info('Obtaining data from bookingsync...')

    t_bfe = time.time()
    bookings_fee = advanced_request('bookings_fee', params={'from': '20111101', 'status': 'booked,unavailable,tentative'})
    logging.info('Obtained Bookings fee data in {} sec'.format(time.time() - t_bfe))

    t_bkg = time.time()
    bookings = advanced_request('booking', params={'from': '20111101', 'status': 'booked,unavailable,tentative', 'include_canceled': True})
    logging.info('Obtained Bookings data in {} sec'.format(time.time() - t_bkg))

    t_cl = time.time()
    clients = advanced_request('client', params={'from': '20111101'})
    logging.info('Obtained Clients data in {} sec'.format(time.time() - t_cl))

    t_ren = time.time()
    rentals = advanced_request('rental', params={'from': '20111101'})
    logging.info('Obtained Rentals data in {} sec'.format(time.time() - t_ren))

    t_prc = time.time()
    data = {'clients': get_clients(clients),
            'rentals': get_rentals(rentals),
            'bookings': get_bookings(bookings),
            'bookings_fee': get_bookings_fee(bookings_fee)}

    data['bookings_split'] = get_bookings_splitted(data['bookings_fee'], data['bookings'], data['rentals'])

    for b in data['bookings']:
        if b['client_id'] and not find_dict_in_list(data['clients'], 'id', b['client_id']):
            # logging.info('Invalid foreign key client_id {}'.format(b['client_id']))
            b['client_id'] = None

        if b['rental_id'] and not find_dict_in_list(data['rentals'], 'id', b['rental_id']):
            # logging.info('Invalid foreign key renal_id {}'.format(b['rental_id']))
            b['rental_id'] = None

    for bf in data['bookings_fee']:
        if bf['booking_id'] and not find_dict_in_list(data['bookings'], 'id', bf['booking_id']):
            # logging.info('Invalid foreign key booking_id {}'.format(bf['booking_id']))
            bf['booking_id'] = None

    logging.info('Processed obtained data in {} sec'.format(time.time() - t_prc))
    logging.info('Completed in {} second.'.format(time.time() - t_total))

    db = MySQL(host=Cfg.get('db_host'),
               port=int(Cfg.get('db_port')),
               user=Cfg.get('db_user'),
               password=Cfg.get('db_password'),
               db=Cfg.get('db_name'))

    write_data_to_db(db, data, Cfg.get('db_tables'))
