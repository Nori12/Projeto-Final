import psycopg2
import os
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
import sys
from decimal import *
import pandas as pd
import numpy as np

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

class DBTickerModel:

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

    def get_date_range(self, ticker):
        result = self.query(f"""SELECT last_update_daily_candles, initial_date_daily_candles, final_date_daily_candles FROM status WHERE ticker = \'{ticker}\';""")

        return result

    def insert_daily_candles(self, ticker, data):

        number_of_rows = len(data)
        query = 'INSERT INTO daily_candles (ticker, day, open_price, max_price, min_price, close_price, volume)\nVALUES\n'

        for n, (index, row) in enumerate(data.iterrows()):
            if n == number_of_rows - 1:
                query += f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, {row['Close']:.6f}, {row['Volume']:.0f})\n"""
            else:
                query += f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, {row['Close']:.6f}, {row['Volume']:.0f}),\n"""

        query += 'ON CONFLICT ON CONSTRAINT daily_data_pkey DO NOTHING'

        self.insert_update(query)

    def upsert_splits(self, ticker, data):

        number_of_rows = len(data)
        query = 'INSERT INTO split (ticker, split_date, ratio)\nVALUES\n'

        for n, (index, row) in enumerate(data.iterrows()):
            if n == number_of_rows - 1:
                query += f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Stock Splits']})\n"""
            else:
                query += f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Stock Splits']}),\n"""

        query += 'ON CONFLICT ON CONSTRAINT split_pkey DO'
        query += '\nUPDATE SET ratio = EXCLUDED.ratio'

        self.insert_update(query)

    def upsert_dividends(self, ticker, dataframe):

        number_of_rows = len(dataframe)
        query = 'INSERT INTO dividends (ticker, payment_date, price_per_stock)\nVALUES\n'

        for n, (index, row) in enumerate(dataframe.iterrows()):
            if n == number_of_rows - 1:
                query += f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Dividends']})\n"""
            else:
                query += f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Dividends']}),\n"""

        query += 'ON CONFLICT ON CONSTRAINT dividends_pkey DO'
        query += '\nUPDATE SET price_per_stock = EXCLUDED.price_per_stock'

        self.insert_update(query)

    def update_daily_candles_with_split(self, ticker, start_date, end_date, split_ratio):

        query = f'UPDATE daily_candles\nSET\n'
        query += f"""  open_price = open_price/{split_ratio:.6f},\n"""
        query += f"""  max_price = max_price/{split_ratio:.6f},\n"""
        query += f"""  min_price = min_price/{split_ratio:.6f},\n"""
        query += f"""  close_price = close_price/{split_ratio:.6f}\n"""
        query += f"""WHERE\n  ticker = \'{ticker}\'\n"""
        query += f"""  AND date_hour >= \'{start_date.strftime('%Y-%m-%d')}\'\n"""
        query += f"""  AND date_hour < \'{end_date.strftime('%Y-%m-%d')}\'"""

        self.insert_update(query)

    def delete_weekly_candles(self, ticker):

        query = f"""DELETE FROM weekly_candles WHERE ticker = \'{ticker}\'"""

        self.insert_update(query)

    def create_weekly_candles_from_daily(self, ticker):

        query = f"""INSERT INTO weekly_candles (ticker, week, open_price, max_price, min_price, close_price, volume)\n"""
        query += f"""SELECT agg2.ticker, agg2.min_day, agg2.open_price, agg2.max_price, agg2.min_price, dc3.close_price, agg2.volume\n"""
        query += f"""FROM\n"""
        query += f"""	(SELECT agg.ticker, agg.min_day, dc2.open_price, agg.max_price, agg.min_price, agg.volume, agg.max_day\n"""
        query += f"""	FROM\n"""
        query += f"""		(SELECT dc.ticker, DATE_PART('year', dc.day) AS year, DATE_PART('week', dc.day) AS week, MIN(dc.day) AS min_day, MAX(dc.day) AS max_day,\n"""
        query += f"""			MAX(dc.max_price) AS max_price, MIN(dc.min_price) AS min_price, SUM(dc.volume) AS volume\n"""
        query += f"""		FROM daily_candles dc\n"""
        query += f"""		GROUP BY dc.ticker, DATE_PART('year', dc.day), DATE_PART('week', dc.day)) agg\n"""
        query += f"""	INNER JOIN daily_candles dc2 ON dc2.ticker = agg.ticker AND dc2.day = agg.min_day) agg2\n"""
        query += f"""INNER JOIN daily_candles dc3 ON dc3.ticker = agg2.ticker AND dc3.day = agg2.max_day\n"""
        query += f"""WHERE dc3.ticker = \'{ticker}\'\n"""
        query += f"""ON CONFLICT ON CONSTRAINT weekly_data_pkey DO NOTHING;"""

        self.insert_update(query)

    def get_candles_dataframe(self, ticker, initial_date, final_date, interval='1d'):

        table = 'daily_candles'
        time_column = 'day'

        if interval == '1wk':
            table = 'weekly_candles'
            time_column = 'week'

        query = f"""SELECT ticker, {time_column}, open_price, max_price, min_price, close_price, volume\n"""
        query += f"""FROM {table}\n"""
        query += f"""WHERE\n"""

        query += f"""  (ticker = \'{ticker}\' and {time_column} >= \'{initial_date.strftime('%Y-%m-%d')}\' and {time_column} < \'{final_date.strftime('%Y-%m-%d')}\')\n"""

        query += f""";"""

        return pd.read_sql_query(query, self._connection)

    def upsert_features(self, dataframe, interval='1d'):

        table = 'daily_features'
        time_column = 'day'
        pkey_constraint = 'daily_features_pkey'

        if interval == '1wk':
            table = 'weekly_features'
            time_column = 'week'
            pkey_constraint = 'weekly_features_pkey'

        number_of_rows = len(dataframe)

        query = f"""INSERT INTO {table} (ticker, {time_column}, peak, ema_17, ema_72, up_down_trend_coef, up_down_trend_status)\nVALUES\n"""

        for n, (_, row) in enumerate(dataframe.iterrows()):
            if n != number_of_rows - 1:
                query += f"""('{row['ticker']}', '{row[time_column].strftime('%Y-%m-%d')}', {row['peak']}, {row['ema_17']:.3f}, {row['ema_72']:.3f}, {row['up_down_trend_coef']:.4f}, {row['up_down_trend_status']}),\n"""
            else:
                query += f"""('{row['ticker']}', '{row[time_column].strftime('%Y-%m-%d')}', {row['peak']}, {row['ema_17']:.3f}, {row['ema_72']:.3f}, {row['up_down_trend_coef']:.4f}, {row['up_down_trend_status']})\n"""

        query = query.replace('nan', 'NULL')

        query += f"""ON CONFLICT ON CONSTRAINT {pkey_constraint} DO\n"""
        query += f"""UPDATE SET ticker = EXCLUDED.ticker, {time_column} = EXCLUDED.{time_column}, peak = EXCLUDED.peak, ema_17 = EXCLUDED.ema_17, ema_72 = EXCLUDED.ema_72, up_down_trend_coef = EXCLUDED.up_down_trend_coef, up_down_trend_status = EXCLUDED.up_down_trend_status;"""

        self.insert_update(query)

class DBGeneralModel:
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

    def get_holidays(self, start_date, end_date):
        holidays = self.query(f"""SELECT day FROM holidays WHERE day >= \'{start_date.strftime('%Y-%m-%d')}\' and day <= \'{end_date.strftime('%Y-%m-%d')}\';""")

        if len(holidays) == 0:
            holidays = ['2200-01-01']
        else:
            holidays = [holiday[0].strftime('%Y-%m-%d') for holiday in holidays]

        return holidays

    def get_candles_dataframe(self, tickers, initial_dates, final_dates, interval='1d'):

        candles_table = 'daily_candles'
        features_table = 'daily_features'
        time_column = 'day'

        if interval == '1wk':
            candles_table = 'weekly_candles'
            features_table = 'weekly_features'
            time_column = 'week'

        query = f"""SELECT \n"""
        query += f"""  cand.ticker, \n"""
        query += f"""  cand.{time_column}, \n"""
        query += f"""  cand.open_price, \n"""
        query += f"""  cand.max_price, \n"""
        query += f"""  cand.min_price, \n"""
        query += f"""  cand.close_price, \n"""
        query += f"""  cand.volume, \n"""
        query += f"""  feat.peak, \n"""
        query += f"""  feat.ema_17, \n"""
        query += f"""  feat.ema_72, \n"""
        query += f"""  feat.up_down_trend_coef, \n"""
        query += f"""  feat.up_down_trend_status\n"""
        query += f"""FROM {candles_table} cand\n"""
        query += f"""INNER JOIN {features_table} feat ON feat.ticker = cand.ticker AND feat.{time_column} = cand.{time_column}\n"""
        query += f"""WHERE\n"""

        for index, (ticker, initial_date, final_date) in enumerate(zip(tickers, initial_dates, final_dates)):
            if index == 0:
                query += f"""  (cand.ticker = \'{ticker}\' and cand.{time_column} >= \'{initial_date.strftime('%Y-%m-%d')}\' and cand.{time_column} < \'{final_date.strftime('%Y-%m-%d')}\')\n"""
            else:
                query += f"""  OR (cand.ticker = \'{ticker}\' and cand.{time_column} >= \'{initial_date.strftime('%Y-%m-%d')}\' and cand.{time_column} < \'{final_date.strftime('%Y-%m-%d')}\')\n"""

        query += f""";"""

        df = pd.read_sql_query(query, self._connection)
        df['ticker'] =  df['ticker'].apply(lambda x: x.rstrip())
        df.sort_values(['ticker', time_column], axis=0, ascending=True, ignore_index=True, inplace=True)
        # df.set_index(['ticker', time_column], inplace=True)

        return df

    def get_tickers(self, on_flag, pn_flag, units_flag, fractional_market=False, sectors=[], subsectors=[], segments=[]):

        if not any([on_flag, pn_flag, units_flag]):
            logger.error('Program aborted. At least one filter is required.')
            sys.exit(c.INVALID_ARGUMENT_ERR)

        filters = []

        if on_flag == True:
            filters.append('____3')
        if pn_flag == True:
            filters.append('____4')
        if units_flag == True:
            filters.append('____11')
        if fractional_market == True:
            filters = [filter + "F" for filter in filters]

        filters = [filter.ljust(7) for filter in filters]

        query = f"""SELECT ticker FROM symbol s\n"""
        query += f"""INNER JOIN entity e ON e.trading_name = s.trading_name\n"""
        query += f"""INNER JOIN company_classification cc ON cc.id = e.company_classification_id\n"""
        query += f"""WHERE\n"""

        for index, filter in enumerate(filters):
            if index == 0:
                query += f"""  (ticker LIKE \'{filter}\'\n"""
            else:
                query += f"""  OR ticker LIKE \'{filter}\'\n"""

            if index == len(filters)-1:
                query += f"""  )\n"""

        if any([sectors, subsectors, segments]):
            if len(filters) > 0:
                query += f"""  AND """

            for index, sector in enumerate(sectors):
                if index == 0:
                    query += f"""  (cc.economic_sector ILIKE \'%{sector}%\'\n"""
                else:
                    query += f"""  OR cc.economic_sector ILIKE \'%{sector}%\'\n"""

                if index == len(sectors)-1:
                    query += f"""  )\n"""

            if len(subsectors) > 0:
                query += f"""  AND """

            for index, subsector in enumerate(subsectors):
                if index == 0:
                    query += f"""  (cc.economic_subsector ILIKE \'%{subsector}%\'\n"""
                else:
                    query += f"""  OR cc.economic_subsector ILIKE \'%{subsector}%\'\n"""

                if index == len(subsectors)-1:
                    query += f"""  )\n"""


            if len(segments) > 0:
                query += f"""  AND """

            for index, segment in enumerate(segments):
                if index == 0:
                    query += f"""  (cc.economic_segment ILIKE \'%{segment}%\'\n"""
                else:
                    query += f"""  OR cc.economic_segment ILIKE \'%{segment}%\'\n"""

                if index == len(segments)-1:
                    query += f"""  )\n"""

        query += "ORDER BY ticker ASC;"

        return self.query(query)