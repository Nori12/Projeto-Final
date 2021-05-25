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

    def update_daily_candles_with_split(self, ticker, start_date, end_date, split_ratio):

        query = f'UPDATE daily_candles\nSET\n'
        query = query + f"""  open_price = open_price/{split_ratio:.6f},\n"""
        query = query + f"""  max_price = max_price/{split_ratio:.6f},\n"""
        query = query + f"""  min_price = min_price/{split_ratio:.6f},\n"""
        query = query + f"""  close_price = close_price/{split_ratio:.6f}\n"""
        query = query + f"""WHERE\n  ticker = \'{ticker}\'\n"""
        query = query + f"""  AND date_hour >= \'{start_date.strftime('%Y-%m-%d')}\'\n"""
        query = query + f"""  AND date_hour < \'{end_date.strftime('%Y-%m-%d')}\'"""

        self.insert_update(query)

    def update_hourly_candles_with_split(self, ticker):

        # Update by comparing to daily_candles prices
        query = f'UPDATE hourly_candles hc\nSET\n'
        query = query + f"""  open_price = ROUND(open_price / n.norm_ratio, 2),\n"""
        query = query + f"""  max_price = ROUND(max_price / n.norm_ratio, 2),\n"""
        query = query + f"""  min_price = ROUND(min_price / n.norm_ratio, 2),\n"""
        query = query + f"""  close_price = ROUND(close_price / n.norm_ratio, 2)\n"""
        query = query + f"""FROM\n"""
        query = query + f"""  (SELECT q.ticker, q.date_hour,\n"""
        query = query + f"""    CASE ROUND(q.max_price / dc.max_price, 1) >= 0.9\n"""
        query = query + f"""    WHEN TRUE THEN ROUND(q.max_price / dc.max_price, 0)\n"""
        query = query + f"""    ELSE ROUND(q.max_price / dc.max_price, 6) END AS norm_ratio\n"""
        query = query + f"""  FROM daily_candles dc\n"""
        query = query + f"""  INNER JOIN\n"""
        query = query + f"""    (SELECT ticker, MIN(date_hour) AS date_hour, MAX(max_price) AS max_price\n"""
        query = query + f"""    FROM hourly_candles\n"""
        query = query + f"""    GROUP BY ticker, EXTRACT(YEAR FROM date_hour), EXTRACT(MONTH FROM date_hour), EXTRACT(DAY FROM date_hour)) q\n"""
        query = query + f"""  ON q.ticker = dc.ticker\n"""
        query = query + f"""    AND DATE(dc.day) = DATE(q.date_hour)\n"""
        query = query + f"""  ) n\n"""
        query = query + f"""WHERE\n"""
        query = query + f"""  n.ticker = \'{ticker}\'\n"""
        query = query + f"""  AND n.ticker = hc.ticker\n"""
        query = query + f"""  AND DATE(n.date_hour) = DATE(hc.date_hour)\n"""
        query = query + f"""  AND n.norm_ratio <> 1;\n"""

        self.insert_update(query)

    def create_missing_daily_candles_from_hourly(self, ticker):

        query = f"""INSERT INTO daily_candles (ticker, day, open_price, max_price, min_price, close_price, volume)\n"""
        query = query + f"""SELECT agg2.ticker, agg2.date_hour AS day, agg2.open_price, agg2.max_price, agg2.min_price, hc3.close_price, agg2.volume\n"""
        query = query + f"""FROM\n"""
        query = query + f"""	(SELECT agg.ticker, agg.date_hour, hc2.open_price, agg.max_price, agg.min_price, agg.volume, agg.max_date_hour\n"""
        query = query + f"""	FROM\n"""
        query = query + f"""		(SELECT hc.ticker, DATE(hc.date_hour) AS date_hour, MIN(hc.date_hour) AS min_date_hour, MAX(hc.date_hour) AS max_date_hour, MAX(hc.max_price) AS max_price, MIN(hc.min_price) AS min_price, SUM(hc.volume) AS volume\n"""
        query = query + f"""		FROM hourly_candles hc\n"""
        query = query + f"""		LEFT JOIN daily_candles dc ON DATE(dc.day) = DATE(hc.date_hour)\n"""
        query = query + f"""		WHERE\n"""
        query = query + f"""		  dc.day IS NULL\n"""
        query = query + f"""		GROUP BY hc.ticker, DATE(hc.date_hour)) agg\n"""
        query = query + f"""	INNER JOIN hourly_candles hc2 ON hc2.ticker = agg.ticker and hc2.date_hour = agg.min_date_hour) agg2\n"""
        query = query + f"""INNER JOIN hourly_candles hc3 ON hc3.ticker = agg2.ticker and hc3.date_hour = agg2.max_date_hour\n"""
        query = query + f"""WHERE agg2.ticker = \'{ticker}\'\n"""
        query = query + f"""ON CONFLICT ON CONSTRAINT daily_data_pkey DO NOTHING;"""

        self.insert_update(query)

    def delete_weekly_candles(self, ticker):

        query = f"""DELETE FROM weekly_candles WHERE ticker = \'{ticker}\'"""

        self.insert_update(query)

    def create_weekly_candles_from_daily(self, ticker):

        query = f"""INSERT INTO weekly_candles (ticker, week, open_price, max_price, min_price, close_price, volume)\n"""
        query = query + f"""SELECT agg2.ticker, agg2.min_day, agg2.open_price, agg2.max_price, agg2.min_price, dc3.close_price, agg2.volume\n"""
        query = query + f"""FROM\n"""
        query = query + f"""	(SELECT agg.ticker, agg.min_day, dc2.open_price, agg.max_price, agg.min_price, agg.volume, agg.max_day\n"""
        query = query + f"""	FROM\n"""
        query = query + f"""		(SELECT dc.ticker, DATE_PART('week', dc.day) AS week, MIN(dc.day) AS min_day, MAX(dc.day) AS max_day, \n"""
        query = query + f"""			MAX(dc.max_price) AS max_price, MIN(dc.min_price) AS min_price, SUM(dc.volume) AS volume\n"""
        query = query + f"""		FROM daily_candles dc\n"""
        query = query + f"""		GROUP BY dc.ticker, DATE_PART('week', dc.day)) agg\n"""
        query = query + f"""	INNER JOIN daily_candles dc2 ON dc2.ticker = agg.ticker AND dc2.day = agg.min_day) agg2\n"""
        query = query + f"""INNER JOIN daily_candles dc3 ON dc3.ticker = agg2.ticker AND dc3.day = agg2.max_day\n"""
        query = query + f"""WHERE dc3.ticker = \'{ticker}\'"""
        query = query + f"""ON CONFLICT ON CONSTRAINT weekly_data_pkey DO NOTHING;"""

        self.insert_update(query)