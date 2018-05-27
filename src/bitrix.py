import json
import logging
import os
import re
import time
from copy import deepcopy
from datetime import datetime
from multiprocessing.pool import ThreadPool
from utilities import print_std_and_log

import requests
import tqdm
from multidimensional_urlencode import urlencode

from mysql_wrapper import MySQL
from utilities import Cfg
from utilities import find_dict_in_list
from utilities import get_db_data

dir_path = os.path.dirname(os.path.realpath(__file__))

_json_file = dir_path + os.sep + '..' + os.sep + Cfg.get('btx_token_file')
_client_id = Cfg.get('btx_client_id')
_client_secret = Cfg.get('btx_client_secret')
_redirect_uri = Cfg.get('btx_redirect_uri')
_main_url = 'https://praguestars.bitrix24.com/rest/METHOD'
_fields_method = 'crm.deal.fields'
_deals_list = 'crm.deal.list'
_deals_remove = 'crm.deal.delete'
_deals_update = 'crm.deal.update'
_deals_add = 'crm.deal.add'

with open(_json_file, 'r') as f:
    _token = json.load(f)


class STAGE:
    PAYED = 'PREPARATION'
    BOOKED = 'NEW'
    STAY = 'EXECUTING'
    ARRIVED = 'PREPAYMENT_INVOICE'
    DEPARTED = 'WON'
    CANCELED = 'LOSE'
    ANALYZE_FAILURE = 'APOLOGY'


def update_token():
    global _token
    req = requests.post('https://praguestars.bitrix24.com/oauth/token',
                        params={'client_id': _client_id,
                                'client_secret': _client_secret,
                                'refresh_token': _token[
                                    'refresh_token'],
                                'grant_type': 'refresh_token',
                                'scope': _token['scope'],
                                'redirect_uri': _redirect_uri})
    if req.status_code == 200:
        _token = json.loads(req.content.decode('utf-8'))
        with open(_json_file, 'w') as fd:
            json.dump(_token, fd, ensure_ascii=False)


def bitrix_request(method, params={}, rec=True):
    url = _main_url.replace('METHOD', method)
    # url = url.replace('AUTH', _token['access_token'])
    params['auth'] = _token['access_token']

    req = requests.get(url, params=urlencode(params))
    if req.status_code == 401:
        if rec:
            update_token()
            time.sleep(10)
            return bitrix_request(method, params, rec=False)
        else:
            raise Exception('401 error while requesting {}'.format(url))
    elif req.status_code == 429:
        # wait until request counts will be updated
        if rec:
            reset_time = datetime.fromtimestamp(int(req.headers['X-RateLimit-Reset']))
            time.sleep((reset_time - datetime.now()).seconds)
            a = bitrix_request(url, params, rec=False)
            return a
        else:
            raise Exception('429 error, something went wrong with rate limit handler'.format(url))

    elif req.status_code == 400:
        logging.error('Wrong data, {}'.format(req.content))
    elif req.status_code == 503:
        print_std_and_log('503 error')
    elif req.status_code != 200:
        raise Exception('{} error while requesting a {}'.format(req.status_code, url))

    return json.loads(req.content.decode('utf-8'))


def get_fields_key_mapping():
    fields = bitrix_request(_fields_method)
    _map = {}
    for key_, val_ in fields['result'].items():
        if str(key_).startswith('UF_CRM_') and 'formLabel' in val_:
            _map[val_['formLabel'].lower()] = key_

    return _map


def contains(deal, deals):
    for deal2 in deals:
        if deal['ID'] == deal2['ID']:
            return True
    return False


def get_bitrix_data():
    old_size = 0
    start = 0
    data = bitrix_request(_deals_list, params={'select': ['UF_*', '*']})['result']
    while len(data) != old_size:
        old_size = len(data)
        start += 50
        for v in bitrix_request(_deals_list, params={'select': ['UF_*', '*'], 'start': start})['result']:
            if not contains(v, data):
                data.append(v)
    return data


def are_differ(m1, m2):
    for _key in m1.keys():
        if _key not in m2:
            logging.error(
                'Trying to add a field \'{}\' to bitrix which does not exist, please check the code'.format(_key))

    for _key, val1 in m1.items():
        if _key == 'ID': continue
        # e.g empty list and none is equal
        val2 = m2[_key]
        if not val1 and not val2:
            continue

        if type(val1) == datetime:
            pass
            tm = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}([\+-]\d{2}:\d{2})', val2)
            val2 = datetime.strptime(val2, '%Y-%m-%dT%H:%M:%S{}'.format(tm.group(1)))

        if str(val1) != str(val2):
            return True

    return False


def prepare_bitrix_data(new_deals):
    bitrix_deals = get_bitrix_data()
    to_add = []
    to_remove = []
    to_update = []
    id_key = get_fields_key_mapping()['id booking (source)']
    for deal in new_deals:
        btx_deal = find_dict_in_list(bitrix_deals, id_key, str(deal[id_key]))

        if btx_deal:
            # a workaround
            btx_deal['OPPORTUNITY'] = None if not btx_deal['OPPORTUNITY'] else float(btx_deal['OPPORTUNITY'])
            if are_differ(deal, btx_deal):
                deal['ID'] = btx_deal['ID']
                to_update.append(deal)

            bitrix_deals.remove(btx_deal)
        else:
            to_add.append(deal)

    for old_deal in bitrix_deals:
        to_remove.append(old_deal['ID'])

    return to_add, to_update, to_remove


def get_returning_host(client_id, bookings):
    n = 0
    for b in bookings:
        if client_id == b['client_id']:
            n += 1

    if n > 1:
        return '1'
    return '0'


def get_stage(start_at, end_at, status):
    # Deal stage: is something like status.
    # By default it is "booked".
    # If the reservation is 32days ahead or less we change it to :"payed".
    # If start_at is today, then we change it to "Arrival".
    # If start_at<today<end_at then value is "staying", and if end_at=today then "departing"
    stage_id = STAGE.BOOKED if status == 'Booked' else None
    reserv_days_ahead = (start_at - datetime.now()).days
    try:
        if reserv_days_ahead == 0:
            stage_id = STAGE.ARRIVED
        elif start_at < datetime.now() <= end_at:
            stage_id = STAGE.STAY
        elif datetime.now() > end_at:
            stage_id = STAGE.DEPARTED
        elif 0 < reserv_days_ahead <= Cfg.get('btx_payed_status_interval'):
            stage_id = STAGE.PAYED
    except:
        pass

    return stage_id


def process_deals_from_db():
    db = MySQL(host=Cfg.get('db_host'),
               port=int(Cfg.get('db_port')),
               user=Cfg.get('db_user'),
               password=Cfg.get('db_password'),
               db=Cfg.get('db_name'))
    db_data = get_db_data(db, Cfg.get('db_tables'), charset='utf8')
    fields = get_fields_key_mapping()

    deals = []
    for booking in db_data['bookings']:
        deal = {'OPPORTUNITY': booking['final_price']}

        deal['CURRENCY_ID'] = booking['currency']

        deal[fields['available to everyone']] = '1'

        # deal[fields['EVENT DATE']] = str(booking['start_at'])
        # deal['BEGINDATE'] = booking['start_at']
        # deal['CLOSEDATE'] = booking['end_at']
        deal[fields['assumed close date']] = booking['end_at']
        deal[fields['start date']] = booking['start_at']

        # deal['RESPONSIBLE'] = 'unassigned'
        deal[fields['adults arrived']] = booking['adults']
        deal[fields['children arrived']] = booking['children']
        deal[fields['comments booking']] = booking['notes']
        deal[fields['id booking (source)']] = booking['id']

        ci = booking['expected_checkin_time']
        co = booking['expected_checkout_time']
        deal[fields['check in time']] = ci if ci else None
        deal[fields['check out time']] = co if co else None

        start_at = booking['start_at']
        end_at = booking['end_at']

        try:
            deal[fields['number of nights']] = (end_at - start_at).days
        except (ValueError, TypeError):
            deal[fields['number of nights']] = None

        client = find_dict_in_list(db_data['clients'], 'id', booking['client_id'])
        client_name = 'unknown' if not client else client['fullname']
        deal[fields['returning host']] = get_returning_host(booking['client_id'], db_data['bookings'])

        deal['TITLE'] = client_name if client_name else 'Unknown'
        rental = find_dict_in_list(db_data['rentals'], 'id', booking['rental_id'])
        rental_name = '' if not rental or 'name' not in rental else rental['name']

        # event description client_name, rental_name, start_at, number of nights
        deal[fields['event description']] = client_name + ', ' + rental_name + ', from {}, {} night(s)'.format(
            booking['start_at'], deal[fields['number of nights']])

        deal['STAGE_ID'] = get_stage(start_at, end_at, booking['status'])

        deal[fields['comments booking']] = booking['comments']
        deal[fields['source']] = booking['source']
        deal['PROBABILITY'] = booking['probability_win']
        deal[fields['quantity']] = '1'
        deal[fields['bookingsync link']] = 'https://www.bookingsync.com/en/bookings/{}'.format(booking['id'])

        deals.append(deal)

    return deals


def delete_bitrix_deals(deal_ids):
    for _id in deal_ids:
        try:
            bitrix_request(_deals_remove, params={'id': _id})
        except:
            logging.error('Could not delete deal with id: {}'.format(_id))


def update_bitrix_deals(deals):
    for deal in deals:
        for k, v in list(deal.items()):
            if not v:
                del deal[k]
        assert 'ID' in deal
        deepcopy(deal).pop('ID')
        bitrix_request(_deals_update, params={'id': deal['ID'], 'fields': deal})


def add_deal(deal):
    for k, v in list(deal.items()):
        if not v:
            del deal[k]
    bitrix_request(_deals_add, params={'fields': deal})


def add_bitrix_deals(deals):
    pool = ThreadPool(1)
    list(tqdm.tqdm(pool.imap(add_deal, deals), total=len(deals)))
    pool.close()
    pool.join()


def run_bitrix():
    print_std_and_log('Uploading bitrix data...')
    deals = process_deals_from_db()
    to_add, to_update, to_delete = prepare_bitrix_data(deals)
    if Cfg.get('btx_remove_old_rows'):
        delete_bitrix_deals(to_delete)
    update_bitrix_deals(to_update)
    add_bitrix_deals(to_add)
    print_std_and_log('Updated: {}'.format(len(to_update)))
    print_std_and_log('Added: {}'.format(len(to_add)))
    print_std_and_log('Deleted: {}'.format(len(to_delete)))
    print_std_and_log('Note: The items above may not been delete if remove_old_rows flag is flase')
