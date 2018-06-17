import json
import logging
import os
import re
import time
from datetime import datetime
from multidimensional_urlencode import urlencode

import requests
import tqdm

from mysql_wrapper import MySQL
from utilities import Cfg
from utilities import find_dict_in_list
from utilities import get_db_data

dir_path = os.path.dirname(os.path.realpath(__file__))

_json_file = dir_path + os.sep + '..' + os.sep + Cfg.get('btx_token_file')
_client_id = Cfg.get('btx_client_id')
_client_secret = Cfg.get('btx_client_secret')
_redirect_uri = 'blabla'
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


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, datetime):
        return str(obj)
    raise TypeError("Type %s not serializable" % type(obj))


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


headers = {'content-type': 'application/json'}


def bitrix_request(method, params, rec=True, post=True):
    url = _main_url.replace('METHOD', method)
    # url = url.replace('AUTH', _token['access_token'])
    params['auth'] = _token['access_token']

    if post:
        req = requests.post(url, data=json.dumps(params, default=json_serial), headers=headers)
    else:
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
            logging.info('{} error while requesting a {}'.format(req.status_code, url))
            logging.info('Second try was failed, possible data loss')

    # return json.loads(req.content.decode('utf-8'))
    return req.json()


# deal fields and mappings
deal_fields_mapping = {}

fields = bitrix_request(_deal_fields, {'select': ['UF_*']})
for key_, val_ in fields['result'].items():
    if str(key_).startswith('UF_CRM_') and 'formLabel' in val_:
        deal_fields_mapping[val_['formLabel'].lower()] = key_

deal_client_id_key = deal_fields_mapping['client id']
deal_rental_id_key = deal_fields_mapping['rental id']

# contact fields and mappings
contact_fields_mapping = {}

fields = bitrix_request(_contact_fields, params={})
for key_, val_ in fields['result'].items():
    if str(key_).startswith('UF_CRM_') and 'formLabel' in val_:
        contact_fields_mapping[val_['formLabel'].lower()] = key_

contact_client_id_key = contact_fields_mapping['client id']

# product fields and mappings
product_fields_mapping = {}
fields = bitrix_request(_product_fields, params={})
for key_, val_ in fields['result'].items():
    if str(key_).startswith('PROPERTY_') and 'title' in val_:
        product_fields_mapping[val_['title'].lower()] = key_

product_rental_id_key = product_fields_mapping['rental_id']


def contains(deal, deals):
    for deal2 in deals:
        if deal['ID'] == deal2['ID']:
            return True
    return False


def get_bitrix_data(content_type, params):
    old_size = 0

    req = bitrix_request(content_type, params=params, post=False)
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


def are_differ(bookingsync_record, bitrix_record, exceptions: list):
    for _key, bks_field in bookingsync_record.items():
        if _key != exceptions:
            continue

        btx_field = bitrix_record[_key]

        # for multi-field values
        if type(bks_field) == list:
            assert type(btx_field) == list
            for i, val in enumerate(bks_field):
                if are_differ(val, btx_field[i], exceptions):
                    return True
            continue

        # for multi-field values
        if type(bks_field) == dict:
            assert type(btx_field) == dict
            if are_differ(bks_field, btx_field, exceptions):
                return True
            continue

        if not bks_field and not btx_field:
            continue

        # change btx_field data format to bks_field
        if type(bks_field) == datetime:
            tm = re.search(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}([\+-]\d{2}:\d{2})', btx_field)
            btx_field = datetime.strptime(btx_field, '%Y-%m-%dT%H:%M:%S{}'.format(tm.group(1)))

        # compare in string format
        if str(bks_field).strip() != str(btx_field).strip():
            try:
                logging.info('for {} key, differ {} and {}'.format(_key, bks_field, btx_field))
            except UnicodeEncodeError:
                logging.info('for {} key, something is differ')
            return True

    return False


def are_differ_products(bookingsync_record, bitrix_record):
    for _key, val1 in bookingsync_record.items():
        if _key not in Cfg.get('btx_product_mutable_fields'):
            continue

        val2 = bitrix_record[_key]

        if not val1 and not val2:
            continue

        if type(val2) == dict:
            if str(val1) != str(val2['value']):
                return True
        elif str(val1) != str(val2):
            try:
                logging.info('for {} key, differ {} and {}'.format(_key, val1, val2))
            except UnicodeEncodeError:
                logging.info('for {} key, something is differ')

            return True

    return False


def prepare_contacts(new_clients):
    bitrix_contacts = get_bitrix_data(_contact_list, params={'select': ['*', 'UF_*', 'PHONE', 'EMAIL']})
    to_add = []
    to_remove = []
    to_update = []
    for client in new_clients:
        btx_contact = find_dict_in_list(bitrix_contacts, contact_client_id_key, client['ID'])

        if btx_contact:
            if 'PHONE' not in btx_contact:
                btx_contact['PHONE'] = [{'VALUE': '', 'VALUE_TYPE': 'OTHER'}]
            if 'EMAIL' not in btx_contact:
                btx_contact['EMAIL'] = [{'VALUE': '', 'VALUE_TYPE': 'OTHER'}]

            assert type(btx_contact) == dict
            if are_differ(client, btx_contact, Cfg.get('btx_contact_mutable_fields')):
                for i, p in enumerate(client['PHONE']):
                    p['ID'] = btx_contact['PHONE'][i]['ID']

                for i, p in enumerate(client['EMAIL']):
                    p['ID'] = btx_contact['EMAIL'][i]['ID']

                client['ID'] = btx_contact['ID']
                to_update.append(client)

            bitrix_contacts.remove(btx_contact)
        else:
            to_add.append(client)

    for old_deal in bitrix_contacts:
        to_remove.append(old_deal['ID'])

    return to_add, to_update, to_remove


def find_product_in_list(lst, key, value):
    for dic in lst:
        if str(dic[key]['value']) == str(value):
            return dic
    return None


def prepare_products(new_products):
    bitrix_products = get_bitrix_data(_product_list, params={'select': ['*', 'PROPERTY_*']})
    to_add = []
    to_remove = []
    to_update = []
    id_key = product_rental_id_key
    for product in new_products:
        btx_prod = find_product_in_list(bitrix_products, id_key, product[id_key])
        if btx_prod:
            if are_differ_products(product, btx_prod):
                product['ID'] = btx_prod['ID']
                to_update.append(product)
            bitrix_products.remove(btx_prod)
        else:
            to_add.append(product)

    for old_product in bitrix_products:
        to_remove.append(old_product['ID'])

    return to_add, to_update, to_remove


def prepare_deals(new_deals):
    bitrix_deals = get_bitrix_data(_deal_list, params={'select': ['UF_*', '*']})
    to_add = []
    to_remove = []
    to_update = []
    id_key = deal_fields_mapping.get('id booking (source)')
    for deal in new_deals:
        btx_deal = find_dict_in_list(lst=bitrix_deals, key=id_key, value=deal[id_key])

        if btx_deal:
            # a workaround for difference of precisions
            btx_deal['OPPORTUNITY'] = None if not btx_deal['OPPORTUNITY'] else float(btx_deal['OPPORTUNITY'])

            if are_differ(deal, btx_deal, Cfg.get('btx_deal_mutable_fields')):
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

    return '1' if n > 1 else '0'


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
        elif start_at <= datetime.now() <= end_at:
            stage_id = STAGE.STAY
        elif datetime.now() > end_at:
            stage_id = STAGE.DEPARTED
        elif 0 < reserv_days_ahead <= Cfg.get('btx_payed_status_interval'):
            stage_id = STAGE.PAYED
    except Exception as e:
        logging.info('Problem when trying to count stage_id: {}'.format(str(e)))

    return stage_id


def get_clients_from_db(db_data):
    contacts = []
    for client in db_data['clients']:
        contact = dict()

        contact['ID'] = client['id']
        contact['NAME'] = client['firstname'] if client['firstname'] else ''
        contact['LAST_NAME'] = client['lastname'] if client['lastname'] else ''
        contact['SECOND_NAME'] = client['fullname'] if client['fullname'] else ''
        contact[contact_fields_mapping['street']] = client['address1']
        contact[contact_fields_mapping['city']] = client['city']
        contact[contact_fields_mapping['country']] = client['country_code']
        contact[contact_fields_mapping['state']] = client['state']
        contact[contact_fields_mapping['prefered language']] = client['preferred_locale']

        mobile = client['mobile'] if client['mobile'] else client['phone'] if client['phone'] else ''
        contact['PHONE'] = [{'VALUE': '{}'.format(m), 'VALUE_TYPE': "OTHER"} for m in mobile.split(',')]

        email = client['email'] if client['email'] else ''
        contact['EMAIL'] = [{'VALUE': '{}'.format(e), 'VALUE_TYPE': "OTHER"} for e in email.split(',')]

        contact[contact_fields_mapping['client id']] = client['id']
        contact['COMMENTS'] = client['notes']
        contacts.append(contact)

        for k in contact.keys():
            if not contact[k]:
                contact[k] = ''
    return contacts


def get_products_from_db(db_data):
    products = []
    pm = product_fields_mapping
    for rental in db_data['rentals']:
        products.append(
            {
                'NAME': rental['name'],
                'SECTION_ID': Cfg.get('btx_product_section_id'),
                pm['rental_id']: rental['id'],
                pm['street']: rental['address1'],
                pm['city']: rental['city'],
                'DESCRIPTION': '{}, {} {}'.format(rental['contact_name'],
                                                  rental['address1'],
                                                  rental['city']
                                                  )
            })
    return products


client_contact_ids = {}


def get_contact_ids():
    contacts = get_bitrix_data(_contact_list, params={'select': ['ID', 'UF_*']})
    for c in contacts:
        client_contact_ids[int(c[contact_client_id_key])] = c['ID']


rental_product_ids = {}


def get_product_ids():
    product = get_bitrix_data(_product_list, params={'select': ['ID', 'PROPERTY_*']})
    for c in product:
        rental_product_ids[int(c[product_rental_id_key]['value'])] = c['ID']


def get_deals_from_db(db_data):
    global deal_fields_mapping
    dm = deal_fields_mapping
    get_product_ids()

    deals = []
    ignored = 0
    for booking in db_data['bookings']:
        if int(booking['unavailable']):
            ignored += 1
            continue

        deal = dict(OPPORTUNITY=booking['final_price'])

        deal['CURRENCY_ID'] = booking['currency']

        deal[dm['available to everyone']] = '1'

        start_at = booking['start_at']
        end_at = booking['end_at']
        deal[dm['assumed close date']] = end_at
        deal[dm['start date']] = start_at
        deal['STAGE_ID'] = get_stage(start_at, end_at, booking['status'])

        # deal['RESPONSIBLE'] = 'unassigned'
        deal[dm['adults arrived']] = booking['adults']
        deal[dm['children arrived']] = booking['children']
        deal[dm['comments booking']] = booking['notes']
        deal[dm['id booking (source)']] = booking['id']

        ci = booking['expected_checkin_time']
        co = booking['expected_checkout_time']
        deal[dm['check in time']] = ci if ci else None
        deal[dm['check out time']] = co if co else None

        try:
            deal[dm['number of nights']] = (end_at.date() - start_at.date()).days
        except (ValueError, TypeError):
            deal[dm['number of nights']] = None

        client = find_dict_in_list(db_data['clients'], 'id', booking['client_id'])
        if client:
            deal[dm['client id']] = booking['client_id']
            deal[dm['returning host']] = get_returning_host(booking['client_id'], db_data['bookings'])
            client_name = client['fullname']
        else:
            deal[dm['client id']] = None
            deal[dm['returning host']] = '0'
            client_name = 'unknown'

        rental = find_dict_in_list(db_data['rentals'], 'id', booking['rental_id'])
        if rental:
            deal[dm['rental id']] = booking['rental_id']
        rental_name = '' if not rental or 'name' not in rental else rental['name']

        # event description client_name, rental_name, start_at, number of nights
        deal[dm['event description']] = client_name + ', ' + rental_name + ', from {}, {} night(s)'.format(
            booking['start_at'].date(), deal[dm['number of nights']])
        deal['TITLE'] = deal[dm['event description']]

        deal[dm['comments booking']] = booking['comments']
        deal[dm['source']] = booking['source']
        deal['PROBABILITY'] = booking['probability_win']
        deal[dm['quantity']] = '1'
        deal[dm['bookingsync link']] = 'https://www.bookingsync.com/en/bookings/{}'.format(booking['id'])
        deal[dm['product']] = rental_product_ids[booking['rental_id']]

        for k in deal.keys():
            if deal[k] is None:
                deal[k] = ''

        deals.append(deal)

    logging.info('Ignored {} bookings as they have unavailable status'.format(ignored))
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
    status_bar = tqdm.tqdm(total=len(fields))
    status_bar.set_description('Updating modified {}'.format(name))
    for field in fields:
        status_bar.update(1)
        # for k, v in list(field.items()):
        # if not v:
        #     del field[k]
        assert 'ID' in field
        bitrix_request(update_method, params={'id': field['ID'], 'fields': field})


def add_contacts_to_deals(deal, res):
    global rental_product_ids
    global client_contact_ids

    if 'result' in res:
        deal_id = res['result']

        # bind contact to deals
        try:
            client_id = int(deal[deal_client_id_key])
        except (ValueError, TypeError):
            client_id = None

        if client_id:
            bitrix_request(_deal_add_contact,
                           params={'id': deal_id,
                                   'fields': {
                                       'CONTACT_ID': client_contact_ids[client_id],
                                       'IS_PRIMARY': 'Y'
                                   }
                                   })
        else:
            logging.info('Booking {} has not client id'.find(str(res['result'])))

        # bind product to deals
        # try:
        #     product_id = int(deal[deal_rental_id_key])
        # except TypeError:
        #     product_id = None

        # if product_id:
        #     bitrix_request(_deal_add_product,
        #                    params={'id': deal_id,
        #                            'rows': [{
        #                                'PRODUCT_ID': rental_product_ids[int(product_id)],
        #                                # 'SECTION_ID': Cfg.get('btx_product_section_id'),
        #                                # 'PRICE': 123,
        #                                # 'QUANTITY': 1
        #                            }]
        #                            })
        # else:
        #     print_std_and_log('Booking {} has not rental id'.find(str(res['result'])))


def add_bitrix_fields(add_method, records, name, callback=None):
    progress_bar = tqdm.tqdm(total=len(records))
    progress_bar.set_description('Adding new {}'.format(name))
    for record in records:
        progress_bar.update(1)
        res = bitrix_request(add_method, params={'fields': record})
        if callback:
            callback(record, res)


def upload_products(to_add, to_update, to_delete):
    logging.info('Updating Bitrix products...')
    if Cfg.get('btx_remove_old_rows'):
        delete_bitrix_fields(_product_remove, to_delete, 'products')
    update_bitrix_fields(_product_update, to_update, 'products')
    add_bitrix_fields(_product_add, to_add, 'products')
    logging.info('Updated: {}'.format(len(to_update)))
    logging.info('Added: {}'.format(len(to_add)))
    logging.info('Deleted: {}'.format(len(to_delete)))
    logging.info('Note: The items above may not been delete if remove_old_rows flag is false')


def upload_deals(to_add, to_update, to_delete):
    get_contact_ids()
    get_product_ids()
    logging.info('Processing Bitrix deals...')
    if Cfg.get('btx_remove_old_rows'):
        delete_bitrix_fields(_deal_remove, to_delete, 'deals')

    update_bitrix_fields(_deal_update, to_update, 'deals')
    add_bitrix_fields(_deal_add, to_add, 'deals', add_contacts_to_deals)
    logging.info('Updated: {}'.format(len(to_update)))
    logging.info('Added: {}'.format(len(to_add)))
    logging.info('Deleted: {}'.format(len(to_delete)))
    logging.info('Note: The items above may not been delete if remove_old_rows flag is false')


def upload_contacts(to_add, to_update, to_delete):
    logging.info('Processing Bitrix contacts...')
    if Cfg.get('btx_remove_old_rows'):
        delete_bitrix_fields(_contact_remove, to_delete, 'contacts')

    update_bitrix_fields(_contact_update, to_update, 'contacts')
    add_bitrix_fields(_contact_add, to_add, 'contacts')
    logging.info('Updated: {}'.format(len(to_update)))
    logging.info('Added: {}'.format(len(to_add)))
    logging.info('Deleted: {}'.format(len(to_delete)))
    logging.info('Note: The items above may not been delete if remove_old_rows flag is false')


def run_bitrix():
    logging.info('Uploading bitrix data...')
    db = MySQL(host=Cfg.get('db_host'),
               port=int(Cfg.get('db_port')),
               user=Cfg.get('db_user'),
               password=Cfg.get('db_password'),
               db=Cfg.get('db_name'))
    db_data = get_db_data(db, Cfg.get('db_tables'), charset='utf8')

    rentals = get_products_from_db(db_data)
    to_add, to_update, to_delete = prepare_products(rentals)
    upload_products(to_add, to_update, to_delete)

    clients = get_clients_from_db(db_data)
    to_add, to_update, to_delete = prepare_contacts(clients)
    upload_contacts(to_add, to_update, to_delete)

    deals = get_deals_from_db(db_data)
    to_add, to_update, to_delete = prepare_deals(deals)
    upload_deals(to_add, to_update, to_delete)
