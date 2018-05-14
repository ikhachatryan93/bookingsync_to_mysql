import configparser
import copy
import logging
import os
import re
import sys
import time
import traceback
from datetime import datetime

from mysql_wrapper import MySQL

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(dir_path, "drivers"))

_mysql_date_format = '%Y-%m-%d %H:%M:%S'


def send_email(_from, _to, _subject, _message):
    import smtplib

    # Import the email modules we'll need
    from email.mime.text import MIMEText

    msg = MIMEText(_message)

    # me == the sender's email address
    # you == the recipient's email address
    msg['Subject'] = _subject
    msg['From'] = _from
    msg['To'] = _to

    # Send the message via our own SMTP server, but don't include the
    # envelope header.
    s = smtplib.SMTP('localhost')
    s.sendmail(_from, [_to], msg.as_string())
    s.quit()


class Configs:
    cfg_file = dir_path + os.sep + '..' + os.sep + 'configs.ini'
    # input_file = "input.txt"
    config = {}
    parsed = False

    @staticmethod
    def parse_config_file():
        config_parser = configparser.RawConfigParser()
        config_parser.optionxform = str
        config_parser.read(Configs.cfg_file)

        Configs.config['login'] = config_parser.get('config', 'login')
        Configs.config['password'] = config_parser.get('config', 'password')
        Configs.config['home_url'] = config_parser.get('config', 'home_url')

        Configs.config['token_file'] = config_parser.get('bookingsync', 'token_file')
        Configs.config['private_app_secret_code'] = config_parser.get('bookingsync', 'token_file')
        Configs.config['client_secret'] = config_parser.get('bookingsync', 'client_secret')
        Configs.config['client_id'] = config_parser.get('bookingsync', 'client_id')
        Configs.config['redirect_uri'] = config_parser.get('bookingsync', 'redirect_uri')
        Configs.config['x_per_page'] = config_parser.get('bookingsync', 'x_per_page')
        Configs.config['bookings_base_url'] = config_parser.get('bookingsync', 'bookings_base_url')
        Configs.config['clients_base_url'] = config_parser.get('bookingsync', 'clients_base_url')
        Configs.config['accounts_base_url'] = config_parser.get('bookingsync', 'accounts_base_url')
        Configs.config['rentals_base_url'] = config_parser.get('bookingsync', 'rentals_base_url')
        Configs.config['bookings_fees_base_url'] = config_parser.get('bookingsync', 'bookings_fees_base_url')
        Configs.config['clean_before_insert'] = config_parser.getboolean('bookingsync', 'clean_before_insert')

        Configs.config['interval_prob'] = []
        is_interval_cmpl = re.compile('(\d+)\s*-\s*(\d+|inf)\s*days\s*(\d+)\%?')
        for var_name, prob in dict(config_parser.items('probability_win_intervals')).items():
            match = is_interval_cmpl.match(prob)
            if match:
                Configs.config['interval_prob'].append((match.group(1), match.group(2), match.group(3)))
            else:
                print('Error while parsing {} interval'.format(var_name))
                exit(1)

        Configs.parsed = True

    @staticmethod
    def get_interval_prob(interval):
        for pr in Configs.get('interval_prob'):
            if float(pr[0]) <= interval <= float(pr[1]):
                return pr[2]

        logging.warning('Could not find probability for interval: {}'.format(interval))
        return None

    @staticmethod
    def get(key):
        if not Configs.parsed:
            Configs.parse_config_file()
        return Configs.config[key]

    @staticmethod
    def set(section, key, value):
        parser = configparser.ConfigParser()
        parser.read(Configs.cfg_file)
        parser.set(section, key, value)
        with open(Configs.cfg_file, 'w+') as f:
            parser.write(f)

    @staticmethod
    def get_mapping():
        if not Configs.parsed:
            Configs.parse_config_file()
        return Configs.config['fields_mapping']


def clean_db_records(db, table_list):
    db.connect()
    db.execute('SET SQL_SAFE_UPDATES = 0')
    db.execute('SET FOREIGN_KEY_CHECKS = 0')
    for table_name in reversed(table_list):
        db.execute('DELETE FROM {}'.format(table_name))
    db.execute('SET FOREIGN_KEY_CHECKS = 1')
    db.conn.commit()
    db.disconnect()


def get_col_names_by_table(db, table_list):
    db.connect()
    col_names = {}
    for table_name in table_list:
        col_names[table_name] = []
        query = 'DESCRIBE {}'.format(table_name)
        out = db.read_all_rows(query)
        for i in out:
            col_names[table_name].append(i['Field'])

    db.disconnect()
    return col_names


def generate_initial_queries(table_list, col_names):
    data_for_db = {}
    for table_name in table_list:
        columns = ','.join(col_names[table_name])
        data_for_db[table_name] = 'insert into {} ({}) values '.format(table_name, columns)

    return data_for_db


def find(lst, key, value):
    for dic in lst:
        if dic[key] == value:
            return dic
    return None


def prepare_data_for_db(db, response_dict):
    db.connect()

    data_to_insert = {}
    data_to_delete = {}
    data_to_update = {}

    # filter out the data which is not updated
    for key, my_table in response_dict.items():
        to_delete = []
        to_insert = []
        to_update = []

        db_table = list(db.read_all_rows('SELECT * FROM {}'.format(key)))
        for my_row in my_table:
            db_row = find(db_table, 'id', my_row['id'])
            if db_row:
                for col_name, col_value in db_row.items():
                    if col_value == '' or col_value is None:
                        col_value = ''
                    if my_row[col_name] == '' or my_row[col_name] is None:
                        my_row[col_name] = ''

                    if my_row[col_name] != col_value:
                        to_update.append(my_row)
                        break

                db_table.remove(db_row)
            else:
                to_insert.append(my_row)

        # exist in db but not in source
        # old data in db should be removed
        for db_row in db_table:
            to_delete.append(db_row)

        data_to_insert[key] = to_insert
        data_to_delete[key] = to_delete
        data_to_update[key] = to_update

    db.disconnect()
    return data_to_insert, data_to_update, data_to_delete


def delete_old_rows(db, to_delete):
    db.connect()
    db.execute('SET SQL_SAFE_UPDATES = 0')
    db.execute('SET FOREIGN_KEY_CHECKS = 0')
    for table_name, table_rows in to_delete.items():
        for row in table_rows:
            db.execute('DELETE FROM {} WHERE id={}'.format(table_name, row['id']))
    db.execute('SET SQL_SAFE_UPDATES = 1')
    db.execute('SET FOREIGN_KEY_CHECKS = 1')
    db.conn.commit()
    db.disconnect()


def process_column_value(col):
    if col == '' or col is None:
        return 'null,'
    elif type(col) == datetime:
        return '"{}",'.format(str(col))
    elif type(col) == int or type(col) == float:
        return '{},'.format(col)
    elif type(col) == bool:
        return '{},'.format('1' if col else '0')
    elif type(col) == str:
        return '"{}",'.format(col)
    else:
        assert 0


def update_modified_rows(db, to_update, column_names):
    db.connect()
    db.execute('SET SQL_SAFE_UPDATES = 0')
    db.execute('SET FOREIGN_KEY_CHECKS = 0')
    for table_name, table_rows in to_update.items():
        for row in table_rows:
            update_script = 'UPDATE {} SET '.format(table_name)
            for col in column_names[table_name]:
                if col != 'id':
                    update_script += '''{} = {} '''.format(col, process_column_value(row[col]))
            update_script = update_script.strip(', ') + ' WHERE {}.id = {}'.format(table_name, row['id'])
            db.execute(update_script)
    db.conn.commit()
    db.execute('SET FOREIGN_KEY_CHECKS = 1')
    db.disconnect()


def write_data_to_db(db: MySQL, dt: dict, table_list: list, package_size=500):
    start_time = time.time()

    logging.info('Writing db ...')
    print('Writing db ...')

    column_names = get_col_names_by_table(db, table_list=table_list)
    initial_queries = generate_initial_queries(table_list=table_list, col_names=column_names)

    if Configs.get('clean_before_insert'):
        clean_db_records(db, table_list)

    to_insert, to_update, to_delete = prepare_data_for_db(db, dt)
    delete_old_rows(db, to_delete)
    update_modified_rows(db, to_update, column_names)

    db.connect()

    for tbl_name in table_list:
        table = to_insert[tbl_name]
        data_for_db = copy.deepcopy(initial_queries[tbl_name])
        count = 0
        for record in table:
            values = ''
            for col_name in column_names[tbl_name]:
                values += process_column_value(record[col_name])

            write_query = '({})'.format(values.strip(','))
            data_for_db += (',{}'.format(write_query) if count != 0 else write_query)

            count += 1
            if count == package_size:
                try:
                    db.insert(data_for_db)
                except:
                    logging.error(traceback.format_exc())
                    print(traceback.format_exc())

                data_for_db = copy.deepcopy(initial_queries[tbl_name])
                count = 0

        if data_for_db != initial_queries[tbl_name]:
            try:
                db.insert(data_for_db)
            except:
                logging.error(traceback.format_exc())
                print(traceback.format_exc())

    db.disconnect()

    # if failed:
    #     print('Some part of insertion has been failed')
    # else:
    #     print('Successfully inserted all scraped data')

    elapsed_time = time.time() - start_time
    logging.info('DB write is finished in {} seconds'.format(elapsed_time))
    logging.info('Booking records added: {}'.format(len(to_insert['bookings'])))
    logging.info('Booking records updated: {}'.format(len(to_update['bookings'])))
    logging.info('Booking records deleted {}'.format(len(to_delete['bookings'])))

    logging.info('Rental records added: {}'.format(len(to_insert['rentals'])))
    logging.info('Rental records updated {}'.format(len(to_update['rentals'])))
    logging.info('Rental records deleted {}'.format(len(to_delete['rentals'])))

    logging.info('Client records added: {}'.format(len(to_insert['clients'])))
    logging.info('Client records updated {}'.format(len(to_update['clients'])))
    logging.info('Client records deleted {}'.format(len(to_delete['clients'])))
    logging.info('Bookings_fee records added: {}'.format(len(to_insert['bookings_fee'])))
    logging.info('Bookings_fee records deleted {}'.format(len(to_delete['bookings_fee'])))
    logging.info('Bookings_fee records updated {}'.format(len(to_update['bookings_fee'])))

    print('DB write is finished in {} seconds'.format(elapsed_time))
    print('Booking records added: {}'.format(len(to_insert['bookings'])))
    print('Booking records updated: {}'.format(len(to_update['bookings'])))
    print('Booking records deleted {}'.format(len(to_delete['bookings'])))

    print('Rental records added: {}'.format(len(to_insert['rentals'])))
    print('Rental records updated {}'.format(len(to_update['rentals'])))
    print('Rental records deleted {}'.format(len(to_delete['rentals'])))

    print('Client records added: {}'.format(len(to_insert['clients'])))
    print('Client records updated {}'.format(len(to_update['clients'])))
    print('Client records deleted {}'.format(len(to_delete['clients'])))

    print('Bookings_fee records added: {}'.format(len(to_insert['bookings_fee'])))
    print('Bookings_fee records deleted {}'.format(len(to_delete['bookings_fee'])))
    print('Bookings_fee records updated {}'.format(len(to_update['bookings_fee'])))

    return 0
