import configparser
import copy
import logging
import os
import re
import sys
import time
import traceback
import json
from datetime import datetime

from mysql_wrapper import MySQL

dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(os.path.join(dir_path, "drivers"))

_mysql_date_format = '%Y-%m-%d %H:%M:%S'


def print_std_and_log(msg):
    logging.info(msg)
    print(msg)


class Cfg:
    cfg_file = dir_path + os.sep + '..' + os.sep + 'configs.ini'
    # input_file = "input.txt"
    config = {}
    parsed = False

    @staticmethod
    def parse_config_file():
        config_parser = configparser.RawConfigParser()
        config_parser.optionxform = str
        config_parser.read(Cfg.cfg_file)

        Cfg.config['login'] = config_parser.get('config', 'login')
        Cfg.config['password'] = config_parser.get('config', 'password')
        Cfg.config['home_url'] = config_parser.get('config', 'home_url')

        # mysql
        Cfg.config['db_name'] = config_parser.get('mysql', 'name')
        Cfg.config['db_host'] = config_parser.get('mysql', 'host')
        Cfg.config['db_user'] = config_parser.get('mysql', 'user')
        Cfg.config['db_port'] = config_parser.get('mysql', 'port')
        Cfg.config['db_password'] = config_parser.get('mysql', 'password')
        Cfg.config['db_tables'] = config_parser.get('mysql', 'db_tables').split()

        # bookingsync
        Cfg.config['bks_token_file'] = config_parser.get('bookingsync', 'token_file')
        Cfg.config['bks_private_app_secret_code'] = config_parser.get('bookingsync', 'token_file')
        Cfg.config['bks_client_secret'] = config_parser.get('bookingsync', 'client_secret')
        Cfg.config['bks_client_id'] = config_parser.get('bookingsync', 'client_id')
        Cfg.config['bks_redirect_uri'] = config_parser.get('bookingsync', 'redirect_uri')
        Cfg.config['bks_x_per_page'] = config_parser.get('bookingsync', 'x_per_page')
        Cfg.config['bks_bookings_base_url'] = config_parser.get('bookingsync', 'bookings_base_url')
        Cfg.config['bks_clients_base_url'] = config_parser.get('bookingsync', 'clients_base_url')
        Cfg.config['bks_accounts_base_url'] = config_parser.get('bookingsync', 'accounts_base_url')
        Cfg.config['bks_rentals_base_url'] = config_parser.get('bookingsync', 'rentals_base_url')
        Cfg.config['bks_bookings_fees_base_url'] = config_parser.get('bookingsync', 'bookings_fees_base_url')
        Cfg.config['bks_sources_base_url'] = config_parser.get('bookingsync', 'sources_base_url')
        Cfg.config['bks_booking_comments_base_url'] = config_parser.get('bookingsync', 'comments_base_url')
        Cfg.config['bks_clean_before_insert'] = config_parser.getboolean('bookingsync', 'clean_before_insert')

        # bitrix24
        Cfg.config['btx_token_file'] = config_parser.get('bitrix24', 'token_file')
        Cfg.config['btx_private_app_secret_code'] = config_parser.get('bitrix24', 'token_file')
        Cfg.config['btx_client_secret'] = config_parser.get('bitrix24', 'client_secret')
        Cfg.config['btx_client_id'] = config_parser.get('bitrix24', 'client_id')
        Cfg.config['btx_redirect_uri'] = config_parser.get('bitrix24', 'redirect_uri')
        Cfg.config['btx_x_per_page'] = config_parser.get('bitrix24', 'x_per_page')
        Cfg.config['btx_payed_status_interval'] = config_parser.getint('bitrix24', 'payed_status_interval')
        Cfg.config['btx_remove_old_rows'] = config_parser.getboolean('bitrix24', 'remove_old_rows')

        Cfg.config['interval_prob'] = []
        is_interval_cmpl = re.compile('(\d+)\s*-\s*(\d+|inf)\s*days\s*(\d+)\%?')
        for var_name, prob in dict(config_parser.items('probability_win_intervals')).items():
            match = is_interval_cmpl.match(prob)
            if match:
                Cfg.config['interval_prob'].append((match.group(1), match.group(2), match.group(3)))
            else:
                print_std_and_log('Error while parsing {} interval'.format(var_name))
                exit(1)

        Cfg.parsed = True

    @staticmethod
    def get_interval_prob(interval):
        for pr in Cfg.get('interval_prob'):
            if float(pr[0]) <= interval <= float(pr[1]):
                return pr[2]

        print_std_and_log('Could not find probability for interval: {}'.format(interval))
        return None

    @staticmethod
    def get(key):
        if not Cfg.parsed:
            Cfg.parse_config_file()
        return Cfg.config[key]

    @staticmethod
    def set(section, key, value):
        parser = configparser.ConfigParser()
        parser.read(Cfg.cfg_file)
        parser.set(section, key, value)
        with open(Cfg.cfg_file, 'w+') as f:
            parser.write(f)

    @staticmethod
    def get_mapping():
        if not Cfg.parsed:
            Cfg.parse_config_file()
        return Cfg.config['fields_mapping']


def get_db_data(db, tables, charset='utf8'):
    db.connect(charset=charset)

    db_tables = {}

    for table in tables:
        db_tables[table] = list(db.read_all_rows('SELECT * FROM {}'.format(table)))

    db.disconnect()

    return db_tables


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


def find_dict_in_list(lst, key, value):
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
            db_row = find_dict_in_list(db_table, 'id', str(my_row['id']))
            if db_row:
                for col_name, col_value in db_row.items():
                    if col_value is None:
                        col_value = ''
                    if my_row[col_name] is None:
                        my_row[col_name] = ''

                    if my_row[col_name] != col_value:
                        print('from {} table db {}={} source {}={}'.format(key, col_name, col_value, col_name,
                                                                           my_row[col_name]))
                        logging.warning('from {} table db {}={} source {}={}'.format(key, col_name, col_value, col_name,
                                                                                     my_row[col_name]))
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
    db.execute('SET SQL_SAFE_UPDATES = 1')
    db.disconnect()


def write_data_to_db(db: MySQL, dt: dict, table_list: list, package_size=500):
    print_std_and_log('Writing db ...')
    start_time = time.time()
    clean_db_records(db, table_list)

    column_names = get_col_names_by_table(db, table_list=table_list)
    initial_queries = generate_initial_queries(table_list=table_list, col_names=column_names)

    if Cfg.get('bks_clean_before_insert'):
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
                    print_std_and_log(traceback.format_exc())

                data_for_db = copy.deepcopy(initial_queries[tbl_name])
                count = 0

        if data_for_db != initial_queries[tbl_name]:
            try:
                db.insert(data_for_db)
            except:
                print_std_and_log(traceback.format_exc())

    db.disconnect()
    elapsed_time = time.time() - start_time

    print_std_and_log('DB write is finished in {} seconds'.format(elapsed_time))
    for t in table_list:
        print_std_and_log('{} records added: {}'.format(t, len(to_insert[t])))
        print_std_and_log('{} records updated: {}'.format(t, len(to_update[t])))
        print_std_and_log('{} records deleted {}'.format(t, len(to_delete[t])))

    return 0
