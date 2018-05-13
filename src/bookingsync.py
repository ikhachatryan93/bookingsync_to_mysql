import json
import os
import sys
import time
import requests
import logging
from datetime import datetime
from utilities import Configs
from mysql_wrapper import MySQL
from utilities import write_data_to_db
from utilities import find

dir_path = os.path.dirname(os.path.realpath(__file__))

sys.path.append(os.path.join(dir_path, "drivers"))

_json_file = dir_path + os.sep + '..' + os.sep + Configs.get('token_file')
_client_id = Configs.get('client_id')
_client_secret = Configs.get('client_secret')
_redirect_uri = Configs.get('redirect_uri')
_native_date_format = '%Y-%m-%dT%H:%M:%SZ'
_mysql_date_format = '%Y-%m-%d %H:%M:%S'

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
    headers = {'Authorization': str('Bearer ' + _token['access_token']), 'content-type': 'application/json'}

    req = requests.get(url, headers=headers, params=params)
    if req.status_code == 401:
        if rec:
            update_token()
            return request_data(url, params, rec=False)
        else:
            raise Exception('401 error while requesting {}'.format(url))
    elif req.status_code == 429:
        # wait until request counts will be updated
        if rec:
            reset_time = datetime.fromtimestamp(int(req.headers['X-RateLimit-Reset']))
            time.sleep((reset_time - datetime.now()).seconds)
            print('waiting')
            a = request_data(url, params, rec=False)
            print('waited')
            return a
        else:
            raise Exception('429 error, something went wrong with rate limit handler'.format(url))

    elif req.status_code != 200:
        raise Exception('{} error while requesting a {}'.format(req.status_code, url))

    return json.loads(req.content.decode('utf-8'))


def advanced_request(target, ids=None, fields=None, params={}):
    # e.g. target is account and url will be accounts_base_url
    url_base = Configs.get(target + 's_base_url')

    params['per_page'] = Configs.get('x_per_page')
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
            return str(datetime.strptime(st, _native_date_format))
        except ValueError:
            logging.error('Could not convert date from {} to {}'.format('st', _native_date_format))
    return ''


def get_bookings(bookings):
    my_bookings = []
    for b in bookings:
        my_booking = {'id': b['id'], 'client_id': b['links']['client'], 'rental_id': b['links']['rental'],
                      'start_at': to_datetime(b['start_at']), 'end_at': to_datetime(b['end_at']),
                      'created_at': to_datetime(b['created_at']), 'canceled_at': to_datetime(b['canceled_at']),
                      'balance_due_at': to_datetime(b['balance_due_at']),
                      'tentative_expires_at': to_datetime(b['tentative_expires_at']),
                      'updated_at': to_datetime(b['updated_at']),
                      'contract_updated_at': to_datetime(b['contract_updated_at']), 'status': b['status'],
                      'reference': b['reference'], 'booked': b['booked'], 'unavailable': b['unavailable'],
                      'initial_price': b['initial_price'], 'initial_rental_price': b['initial_rental_price'],
                      'channel_price': b['channel_price'], 'discount': b['discount'],
                      'final_rental_price': b['final_rental_price'], 'final_price': b['final_price'],
                      'paid_amount': b['paid_amount'], 'currency': b['currency'], 'notes': b['notes'],
                      'damage_deposit': b['damage_deposit'],
                      'charge_damage_deposit_on_arrival': b['charge_damage_deposit_on_arrival'],
                      'adults': b['adults'], 'children': b['children'],
                      'bookings_payments_count': b['bookings_payments_count'],
                      'review_requests_count': b['review_requests_count'], 'locked': b['locked'],
                      'cancelation_reason': b['cancelation_reason'],
                      'expected_checkin_time': b['expected_checkin_time'],
                      'expected_checkout_time': b['expected_checkout_time'], 'payment_url': b['payment_url'],
                      'rental_payback_to_owner': b['rental_payback_to_owner'],
                      'final_payback_to_owner': b['final_payback_to_owner'], 'commission': b['commission'],
                      'door_key_code': b['door_key_code'],
                      'payment_left_to_collect': b['payment_left_to_collect'],
                      'owned_by_app': b['owned_by_app'], 'account_id': b['links']['account'],
                      'probability_win': None}

        if my_booking['start_at']:
            days_interval = (datetime.strptime(my_booking['start_at'], _mysql_date_format) - datetime.now()).seconds
            my_booking['probability_win'] = Configs.get_interval_prob(days_interval)

        my_bookings.append(my_booking)

    # get account name
    accounts = request_data(Configs.get('accounts_base_url'), params={'fields': 'id,business_name'})
    for b in my_bookings:
        for a in accounts['accounts']:
            if len(a) > 0:
                if a['id'] == b['account_id']:
                    b['account'] = a['business_name']

    return my_bookings


def get_rentals(rentals):
    my_rentals = []
    for r in rentals:
        my_rentals.append({'id': r['id'], 'created_at': to_datetime(r['created_at']),
                           'updated_at': to_datetime(r['updated_at']), 'published_at': to_datetime(r['published_at']),
                           'name': r['name'], 'address1': r['address1'], 'address2': r['address2'],
                           'currency': r['currency'],
                           'min_price': r['min_price'], 'max_price': r['max_price'], 'downpayment': r['downpayment'],
                           'bedrooms_count': r['bedrooms_count'], 'bathrooms_count': r['bathrooms_count'],
                           'sleeps': r['sleeps'], 'sleeps_max': r['sleeps_max'], 'city': r['city'],
                           'country_code': r['country_code'], 'contact_name': r['contact_name'], 'zip': r['zip'],
                           'notes': r['notes'], 'base_rate': r['base_rate'], 'base_rate_kind': r['base_rate_kind'],
                           'damage_deposit': r['damage_deposit'], 'rental_type': r['rental_type'],
                           'absolute_min_price': r['absolute_min_price']})

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
                   'created_at': to_datetime(fee['created_at']), 'booking_id': fee['links']['booking'],
                   'included_in_price': fee['included_in_price'], 'price': fee['price'], 'required': fee['required'],
                   'times_booked': fee['times_booked'], 'name': None if 'en' not in fee['name'] else fee['name']['en']}

        my_bfees.append(my_bfee)
    return my_bfees


def get_bookingsync_data():
    t1 = time.time()
    logging.info('Obtaining data from bookingsync...')
    print('Obtaining data from bookingsync...')

    bookings_fee = advanced_request('bookings_fee')
    bookings = advanced_request('booking', params={'from': '20171101'})
    print(len(bookings))
    clients = advanced_request('client')
    rentals = advanced_request('rental')

    data = {'clients': get_clients(clients),
            'rentals': get_rentals(rentals),
            'bookings': get_bookings(bookings),
            'bookings_fee': get_bookings_fee(bookings_fee)}

    for b in data['bookings']:
        if b['client_id'] and not find(data['clients'], 'id', b['client_id']):
            #logging.warning('Invalid foreign key client_id {}'.format(b['client_id']))
            b['client_id'] = None

        if b['rental_id'] and not find(data['rentals'], 'id', b['rental_id']):
            #logging.warning('Invalid foreign key renal_id {}'.format(b['rental_id']))
            b['client_id'] = None

    for bf in data['bookings_fee']:
        if bf['booking_id'] and not find(data['bookings'], 'id', bf['booking_id']):
            #logging.warning('Invalid foreign key booking_id {}'.format(bf['booking_id']))
            bf['booking_id'] = None

    logging.info('Completed in {} second.'.format(time.time() - t1))
    print('Completed in {} second.'.format(time.time() - t1))


    db = MySQL(host="67.222.38.91", port=3306, user="myreser4_db", password="so8oep", db="myreser4_db")
    write_data_to_db(db, data, ['clients', 'rentals', 'bookings', 'bookings_fee'])
