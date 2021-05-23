import psycopg2
import os
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
import sys
from decimal import *

import constants as c

# Database macros
DB_USER = os.environ.get('STOCK_MARKET_DB_USER')
DB_PASS = os.environ.get('STOCK_MARKET_DB_PASS')
DB_NAME = 'StockMarket'
DB_PORT = 5433
DB_HOST =  'localhost'

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

class DBModel:

    def __init__(self):
        try:
            connection = psycopg2.connect(f"dbname='{DB_NAME}' user={DB_USER} host='{DB_HOST}' password={DB_PASS} port='{DB_PORT}'")
            logger.debug(f'Database \'{DB_NAME}\' connected successfully.')
        except:
            logger.error(f'Database \'{DB_NAME}\' connection failed.')
            sys.exit(c.DB_CONNECTION_ERR)

        self._connection = connection
        self._cursor = self._connection.cursor()

    def __del__(self):
        self._connection.close()
        self._cursor.close()
        # logger.debug('Database \'StockMarket\' connection closed.')

    def query(self, query, params=None):
        try:
            self._cursor.execute(query, params)
        except Exception as error:
            logger.error('Error executing query "{}", error: {}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            # logger.debug('Database \'StockMarket\' connection closed.')
            sys.exit(c.QUERY_ERR)

        return self._cursor.fetchall()

    def insert_update(self, query, params=None):
        try:
            self._cursor.execute(query, params)
            self._connection.commit()
        except Exception as error:
            logger.error('Error executing query "{}", error: {}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            sys.exit(c.QUERY_ERR)

    def get_all_classifications(self):
        result = self.query("""SELECT economic_sector, economic_subsector, economic_segment FROM company_classification""")
        return(result)

    def get_date_range(self, ticker):
        result = self.query(f"""SELECT last_update_hourly_candles, initial_date_hourly_candles, final_date_hourly_candles, last_update_daily_candles, initial_date_daily_candles, final_date_daily_candles FROM status WHERE ticker = \'{ticker}\';""")
        return result

    def get_holidays(self, start_date, end_date):
        result = self.query(f"""SELECT day FROM holidays WHERE day >= \'{start_date.strftime('%Y-%m-%d')}\' and day <= \'{end_date.strftime('%Y-%m-%d')}\';""")
        return result

    def insert_daily_candles(self, ticker, data):

        number_of_rows = len(data)
        query = 'INSERT INTO daily_candles (ticker, day, open_price, max_price, min_price, close_price, volume)\nVALUES\n'

        for n, (index, row) in enumerate(data.iterrows()):
            if n == number_of_rows - 1:
                query = query + f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, {row['Close']:.6f}, {row['Volume']:.0f})\n"""
            else:
                query = query + f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, {row['Close']:.6f}, {row['Volume']:.0f}),\n"""

        query = query + 'ON CONFLICT ON CONSTRAINT daily_data_pkey DO NOTHING;'

        self.insert_update(query)

    def insert_hourly_candles(self, ticker, data):

        number_of_rows = len(data)
        query = 'INSERT INTO hourly_candles (ticker, date_hour, open_price, max_price, min_price, close_price, volume)\nVALUES\n'

        for n, (index, row) in enumerate(data.iterrows()):
            if n == number_of_rows - 1:
                query = query + f"""('{ticker}', '{index.strftime('%Y-%m-%d %H:%M:%S')}', {row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, {row['Close']:.6f}, {row['Volume']:.0f})\n"""
            else:
                query = query + f"""('{ticker}', '{index.strftime('%Y-%m-%d %H:%M:%S')}', {row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, {row['Close']:.6f}, {row['Volume']:.0f}),\n"""

        query = query + 'ON CONFLICT ON CONSTRAINT hourly_data_pkey DO NOTHING;'

        self.insert_update(query)

    def upsert_splits(self, ticker, data):

        number_of_rows = len(data)
        query = 'INSERT INTO split (ticker, split_date, ratio)\nVALUES\n'

        for n, (index, row) in enumerate(data.iterrows()):
            if n == number_of_rows - 1:
                query = query + f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Stock Splits']})\n"""
            else:
                query = query + f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Stock Splits']}),\n"""

        query = query + 'ON CONFLICT ON CONSTRAINT split_pkey DO'
        query = query + '\nUPDATE SET ratio = EXCLUDED.ratio'

        self.insert_update(query)

    def upsert_dividends(self, ticker, data):

        number_of_rows = len(data)
        query = 'INSERT INTO dividends (ticker, payment_date, price_per_stock)\nVALUES\n'

        for n, (index, row) in enumerate(data.iterrows()):
            if n == number_of_rows - 1:
                query = query + f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Dividends']})\n"""
            else:
                query = query + f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Dividends']}),\n"""

        query = query + 'ON CONFLICT ON CONSTRAINT dividends_pkey DO'
        query = query + '\nUPDATE SET price_per_stock = EXCLUDED.price_per_stock'

        self.insert_update(query)

    def update_candles_with_split(self, ticker, start_date, end_date, split_ratio, interval='1d'):

        if not (interval in ['1d', '1h']):
            logger.error(f'Error argument \'interval\'=\'{interval}\' is not valid.')
            sys.exit(c.INVALID_ARGUMENT_ERR)

        table_name = 'daily_candles'
        data_column = 'day'

        if interval == '1h':
            table_name = 'hourly_candles'
            data_column = 'date_hour'

        query = f'UPDATE {table_name}\nSET\n'
        query = query + f"""  open_price = open_price/{split_ratio:.6f},\n"""
        query = query + f"""  max_price = max_price/{split_ratio:.6f},\n"""
        query = query + f"""  min_price = min_price/{split_ratio:.6f},\n"""
        query = query + f"""  close_price = close_price/{split_ratio:.6f}\n"""
        query = query + f"""WHERE\n  ticker = \'{ticker}\'\n"""
        query = query + f"""  AND {data_column} >= \'{start_date.strftime('%Y-%m-%d')}\'\n"""
        query = query + f"""  AND {data_column} < \'{end_date.strftime('%Y-%m-%d')}\'"""

        self.insert_update(query)
