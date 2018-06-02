import json
import logging
import os
import re
import time
from datetime import datetime
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

_contact_fields = 'crm.contact.fields'
_contact_list = 'crm.contact.list'
_contact_remove = 'crm.contact.delete'
_contact_update = 'crm.contact.update'
_contact_add = 'crm.contact.add'

_deal_add_contact = 'crm.deal.contact.add'
_deal_add_product = 'crm.deal.productrows.set'
_deal_fields = 'crm.deal.fields'
_deal_list = 'crm.deal.list'
_deal_remove = 'crm.deal.delete'
_deal_update = 'crm.deal.update'
_deal_add = 'crm.deal.add'

_product_fields = 'crm.product.fields'
_product_list = 'crm.product.list'
_product_remove = 'crm.product.delete'
_product_update = 'crm.product.update'
_product_add = 'crm.product.add'

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
    elif req.status_code == 400:
        logging.error('Wrong data, {}'.format(req.content))
    elif req.status_code != 200:
        time.sleep(10)
        if rec:
            return bitrix_request(method, params, rec=False)
        else:
            print_std_and_log('{} error while requesting a {}'.format(req.status_code, url))
            print_std_and_log('Second try was failed, possible data loss')

    return json.loads(req.content.decode('utf-8'))


deal_fields_mapping = {}

fields = bitrix_request(_deal_fields)
for key_, val_ in fields['result'].items():
    if str(key_).startswith('UF_CRM_') and 'formLabel' in val_:
        deal_fields_mapping[val_['formLabel'].lower()] = key_

contact_fields_mapping = {}

fields = bitrix_request(_contact_fields)
for key_, val_ in fields['result'].items():
    if str(key_).startswith('UF_CRM_') and 'formLabel' in val_:
        contact_fields_mapping[val_['formLabel'].lower()] = key_

client_id_key = contact_fields_mapping['client id']

product_fields_mapping = {}
fields = bitrix_request(_product_fields)
for key_, val_ in fields['result'].items():
    if str(key_).startswith('UF_CRM_') and 'formLabel' in val_:
        product_fields_mapping[val_['formLabel'].lower()] = key_


def contains(deal, deals):
    for deal2 in deals:
        if deal['ID'] == deal2['ID']:
            return True
    return False


def get_bitrix_data(content_type, params):
    old_size = 0

    req = bitrix_request(content_type, params=params)
    data = req['result']
    if 'next' not in req:
        return data

    while len(data) != old_size:
        old_size = len(data)
        params['start'] = req['next']
        req = bitrix_request(content_type, params=params)
        for v in req['result']:
            if not contains(v, data):
                data.append(v)
        if 'next' not in req:
            break

    return data


def are_differ(m1, m2):
    for _key in m1.keys():
        if _key not in m2:
            logging.error(
                'Trying to add a field \'{}\' to bitrix which does not exist, please check the code'.format(_key))

    for _key, val1 in m1.items():
        if _key == 'ID': continue
        val2 = m2[_key]

        val1 = val1 if val1 != '0' else None
        val2 = val2 if val2 != '0' else None
        if not val1 and not val2:
            continue

        old_val2 = val2
        if type(val1) == datetime:
            tm = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}([\+-]\d{2}:\d{2})', val2)
            val2 = datetime.strptime(val2, '%Y-%m-%dT%H:%M:%S{}'.format(tm.group(1)))

        if str(val1) != str(val2):
            print('from {} differ {} and {}, {}'.format(_key, val1, val2, old_val2))
            return True

    return False


def prepare_contacts(new_clients):
    bitrix_contacts = get_bitrix_data(_contact_list, params={'select': ['UF_*', '*']})
    to_add = []
    to_remove = []
    to_update = []
    for client in new_clients:
        btx_contact = find_dict_in_list(bitrix_contacts, client_id_key, str(client['ID']))

        if btx_contact:
            if are_differ(client, btx_contact):
                client['ID'] = btx_contact['ID']
                to_update.append(client)

            bitrix_contacts.remove(btx_contact)
        else:
            to_add.append(client)

    for old_deal in bitrix_contacts:
        to_remove.append(old_deal['ID'])

    return to_add, to_update, to_remove


def prepare_products(new_products):
    bitrix_products = get_bitrix_data(_product_list, params={'select': ['UF_*', '*']})
    to_add = []
    to_remove = []
    to_update = []
    id_key = product_fields_mapping.get('')


def prepare_deals(new_deals):
    bitrix_deals = get_bitrix_data(_deal_list, params={'select': ['UF_*', '*']})
    to_add = []
    to_remove = []
    to_update = []
    id_key = deal_fields_mapping.get('id booking (source)')
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
        elif datetime.now() > end_at:
            stage_id = STAGE.DEPARTED
        elif 0 < reserv_days_ahead <= Cfg.get('btx_payed_status_interval'):
            stage_id = STAGE.PAYED
    except:
        pass

    return stage_id


def get_clients_from_db(db_data):
    contacts = []
    for client in db_data['clients']:
        contact = {}
        contact['ID'] = client['id']

        contact['NAME'] = client['firstname']
        contact['LAST_NAME'] = client['lastname']
        contact['SECOND_NAME'] = client['fullname']
        contact[contact_fields_mapping['street']] = client['address1']
        # contact[contact_fields_mapping['ADDRESS_2']] = client['address2']
        contact[contact_fields_mapping['city']] = client['city']
        contact[contact_fields_mapping['country']] = client['country_code']
        contact[contact_fields_mapping['state']] = client['state']
        contact[contact_fields_mapping['prefered language']] = client['preferred_locale']
        contact[contact_fields_mapping['mobile']] = client['mobile'] if client['mobile'] else client['phone']
        contact[contact_fields_mapping['email']] = client['email']
        contact[contact_fields_mapping['client id']] = client['id']
        contact['COMMENTS'] = client['notes']
        contacts.append(contact)
    return contacts


def get_products_from_db(db_data):
    products = []
    for rental in db_data['rentals']:
        products.append(
            {'ID': rental['id'], 'NAME': rental['name'], 'STREET': rental['address1'], 'CITY': rental['city'],
             'CONTACT NAME': '{}, {} {}'.format(rental['contact_name'], rental['address1'], rental['city'])})


def get_deals_from_db(db_data):
    global deal_fields_mapping
    fields = deal_fields_mapping

    deals = []
    ignored = 0
    for booking in db_data['bookings']:
        if int(booking['unavailable']):
            ignored += 1
            continue

        deal = dict(OPPORTUNITY=booking['final_price'])

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
            deal[fields['number of nights']] = (end_at.date() - start_at.date()).days
        except (ValueError, TypeError):
            deal[fields['number of nights']] = None

        client = find_dict_in_list(db_data['clients'], 'id', booking['client_id'])
        if client:
            deal[fields['client id']] = booking['client_id']
            deal[fields['returning host']] = get_returning_host(booking['client_id'], db_data['bookings'])
            client_name = client['fullname']
        else:
            deal[fields['client id']] = None
            deal[fields['returning host']] = '0'
            client_name = 'unknown'

        rental = find_dict_in_list(db_data['rentals'], 'id', booking['rental_id'])
        rental_name = '' if not rental or 'name' not in rental else rental['name']

        # event description client_name, rental_name, start_at, number of nights
        deal[fields['event description']] = client_name + ', ' + rental_name + ', from {}, {} night(s)'.format(
            booking['start_at'].date(), deal[fields['number of nights']])
        deal['TITLE'] = deal[fields['event description']]

        deal['STAGE_ID'] = get_stage(start_at, end_at, booking['status'])

        deal[fields['comments booking']] = booking['comments']
        deal[fields['source']] = booking['source']
        deal['PROBABILITY'] = booking['probability_win']
        deal[fields['quantity']] = '1'
        deal[fields['bookingsync link']] = 'https://www.bookingsync.com/en/bookings/{}'.format(booking['id'])

        deals.append(deal)

    print_std_and_log('Ignored {} bookings as they have unavailable status'.format(ignored))
    return deals


def delete_bitrix_fields(remove_method, field_ids, name):
    tq = tqdm.tqdm(total=len(field_ids))
    tq.set_description('Removing old {}'.format(name))
    for _id in field_ids:
        tq.update(1)
        try:
            bitrix_request(remove_method, params={'id': _id})
        except:
            logging.error('Could not delete deal with id: {}'.format(_id))


def update_bitrix_fields(update_method, fields, name):
    tq = tqdm.tqdm(total=len(fields))
    tq.set_description('Updating modified {}'.format(name))
    for field in fields:
        tq.update(1)
        # for k, v in list(field.items()):
        # if not v:
        #     del field[k]
        assert 'ID' in field
        bitrix_request(update_method, params={'id': field['ID'], 'fields': field})


client_contact_ids = {}


def get_contact_ids():
    contacts = get_bitrix_data(_contact_list, params={'select': ['ID', 'UF_*']})
    for c in contacts:
        client_contact_ids[c[client_id_key]] = c['ID']


def add_contacts_to_deals(field, res):
    global client_contact_ids
    client_id_k = deal_fields_mapping['client id']
    if 'result' in res:
        client_id = field[client_id_k]
        if client_id:
            bitrix_request(_deal_add_contact, params={'id': res['result'],
                                                      'fields': {'CONTACT_ID': client_contact_ids[str(field[client_id_k])],
                                                                 'IS_PRIMARY': 'Y'}})
        else:
            print_std_and_log('Booking {} has not client id'.find(str(res['result'])))


def add_bitrix_fields(add_method, fields, name, callback=None):
    tq = tqdm.tqdm(total=len(fields))
    tq.set_description('Adding new {}'.format(name))
    for field in fields:
        tq.update(1)
        res = bitrix_request(add_method, params={'fields': field})
        if callback:
            callback(field, res)


def upload_deals(to_add, to_update, to_delete):
    get_contact_ids()
    if Cfg.get('btx_remove_old_rows'):
        delete_bitrix_fields(_deal_remove, to_delete, 'deals')
    update_bitrix_fields(_deal_update, to_update, 'deals')
    add_bitrix_fields(_deal_add, to_add, 'deals', add_contacts_to_deals)
    print_std_and_log('Updated: {}'.format(len(to_update)))
    print_std_and_log('Added: {}'.format(len(to_add)))
    print_std_and_log('Deleted: {}'.format(len(to_delete)))
    print_std_and_log('Note: The items above may not been delete if remove_old_rows flag is false')


def upload_contacts(to_add, to_update, to_delete):
    print_std_and_log('Uploading contacts...')
    if Cfg.get('btx_remove_old_rows'):
        delete_bitrix_fields(_contact_remove, to_delete, 'contacts')

    update_bitrix_fields(_contact_update, to_update, 'contacts')
    update_bitrix_fields(_contact_update, to_update, 'contacts')
    update_bitrix_fields(_contact_update, to_update, 'contacts')
    add_bitrix_fields(_contact_add, to_add, 'contacts')
    print_std_and_log('Updated: {}'.format(len(to_update)))
    print_std_and_log('Added: {}'.format(len(to_add)))
    print_std_and_log('Deleted: {}'.format(len(to_delete)))
    print_std_and_log('Note: The items above may not been delete if remove_old_rows flag is false')


# def bind_contact_to_deal(deal):
#     contact_id = deal[fields_mapping.get('client id')]
#     btx_contact = bitrix_request('crm.contact.get', params={'id': contact_id})
#     if 'result' in btx_contact:
#         bitrix_request()


def run_bitrix():
    print_std_and_log('Uploading bitrix data...')
    db = MySQL(host=Cfg.get('db_host'),
               port=int(Cfg.get('db_port')),
               user=Cfg.get('db_user'),
               password=Cfg.get('db_password'),
               db=Cfg.get('db_name'))
    db_data = get_db_data(db, Cfg.get('db_tables'), charset='utf8')

    clients = get_clients_from_db(db_data)
    to_add, to_update, to_delete = prepare_contacts(clients)
    upload_contacts(to_add, to_update, to_delete)

    deals = get_deals_from_db(db_data)
    to_add, to_update, to_delete = prepare_deals(deals)
    upload_deals(to_add, to_update, to_delete)
