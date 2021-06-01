import psycopg2
import os
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
import sys
from decimal import *
import pandas as pd

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
        result = self.query(f"""SELECT last_update_hourly_candles, initial_date_hourly_candles, final_date_hourly_candles, last_update_daily_candles, initial_date_daily_candles, final_date_daily_candles FROM status WHERE ticker = \'{ticker}\';""")
        return result

    def insert_daily_candles(self, ticker, data):

        number_of_rows = len(data)
        query = 'INSERT INTO daily_candles (ticker, day, open_price, max_price, min_price, close_price, volume)\nVALUES\n'

        for n, (index, row) in enumerate(data.iterrows()):
            if n == number_of_rows - 1:
                query += f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, {row['Close']:.6f}, {row['Volume']:.0f})\n"""
            else:
                query += f"""('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, {row['Close']:.6f}, {row['Volume']:.0f}),\n"""

        query += 'ON CONFLICT ON CONSTRAINT daily_data_pkey DO NOTHING;'

        self.insert_update(query)

    def insert_hourly_candles(self, ticker, data):

        number_of_rows = len(data)
        query = 'INSERT INTO hourly_candles (ticker, date_hour, open_price, max_price, min_price, close_price, volume)\nVALUES\n'

        for n, (index, row) in enumerate(data.iterrows()):
            if n == number_of_rows - 1:
                query += f"""('{ticker}', '{index.strftime('%Y-%m-%d %H:%M:%S')}', {row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, {row['Close']:.6f}, {row['Volume']:.0f})\n"""
            else:
                query += f"""('{ticker}', '{index.strftime('%Y-%m-%d %H:%M:%S')}', {row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, {row['Close']:.6f}, {row['Volume']:.0f}),\n"""

        query += 'ON CONFLICT ON CONSTRAINT hourly_data_pkey DO NOTHING;'

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

    def upsert_dividends(self, ticker, data):

        number_of_rows = len(data)
        query = 'INSERT INTO dividends (ticker, payment_date, price_per_stock)\nVALUES\n'

        for n, (index, row) in enumerate(data.iterrows()):
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

    def update_hourly_candles_with_split(self, ticker):

        # Update by comparing to daily_candles prices
        query = f'UPDATE hourly_candles hc\nSET\n'
        query += f"""  open_price = ROUND(open_price / n.norm_ratio, 2),\n"""
        query += f"""  max_price = ROUND(max_price / n.norm_ratio, 2),\n"""
        query += f"""  min_price = ROUND(min_price / n.norm_ratio, 2),\n"""
        query += f"""  close_price = ROUND(close_price / n.norm_ratio, 2)\n"""
        query += f"""FROM\n"""
        query += f"""  (SELECT q.ticker, q.date_hour,\n"""
        query += f"""    CASE\n"""
        query += f"""    WHEN ROUND(q.max_price / dc.max_price, 1) >= 0.9 THEN ROUND(q.max_price / dc.max_price, 0)\n"""
        query += f"""    WHEN ROUND(dc.max_price / q.max_price, 1) >= 0.9 THEN ROUND(1 / ROUND(dc.max_price / q.max_price, 0), 6)\n"""
        query += f"""    ELSE 1 END AS norm_ratio\n"""
        query += f"""  FROM daily_candles dc\n"""
        query += f"""  INNER JOIN\n"""
        query += f"""    (SELECT ticker, MIN(date_hour) AS date_hour, MAX(max_price) AS max_price\n"""
        query += f"""    FROM hourly_candles\n"""
        query += f"""    GROUP BY ticker, DATE(date_hour)) q\n"""
        query += f"""  ON q.ticker = dc.ticker\n"""
        query += f"""    AND DATE(dc.day) = DATE(q.date_hour)\n"""
        query += f"""  ) n\n"""
        query += f"""WHERE\n"""
        query += f"""  n.ticker = \'{ticker}\'\n"""
        query += f"""  AND n.ticker = hc.ticker\n"""
        query += f"""  AND DATE(n.date_hour) = DATE(hc.date_hour)\n"""
        query += f"""  AND n.norm_ratio <> 1;\n"""

        self.insert_update(query)

        # The last query does not correct a value in hourly_candles in which there is no corresponding day in daily_candles.
        # So a splitted value there will not be corrected and further generate an equally not splitted daily_candle.
        query2 = f"""UPDATE hourly_candles hc\n"""
        query2 += f"""SET\n"""
        query2 += f"""  open_price = ROUND(open_price / n.norm_ratio, 2),\n"""
        query2 += f"""  max_price = ROUND(max_price / n.norm_ratio, 2),\n"""
        query2 += f"""  min_price = ROUND(min_price / n.norm_ratio, 2),\n"""
        query2 += f"""  close_price = ROUND(close_price / n.norm_ratio, 2)\n"""
        query2 += f"""FROM\n"""
        query2 += f"""  (SELECT \n"""
        query2 += f"""    hc1.ticker, \n"""
        query2 += f"""    hc1.open_price AS price1,\n"""
        query2 += f"""    hc1.date_hour AS date_hour, \n"""
        query2 += f"""    hc2.open_price AS price2,\n"""
        query2 += f"""    hc2.date_hour AS hc2_day, \n"""
        query2 += f"""    hc3.open_price AS price3,\n"""
        query2 += f"""    hc3.date_hour AS hc3_day,\n"""
        query2 += f"""    hc4.open_price AS price4,\n"""
        query2 += f"""    hc4.date_hour AS hc4_day,\n"""
        query2 += f"""    hc5.open_price AS price5,\n"""
        query2 += f"""    hc5.date_hour AS hc5_day,\n"""
        query2 += f"""    CASE \n"""
        query2 += f"""      WHEN ROUND(hc1.close_price / hc2.open_price, 1) >= 0.9 THEN ROUND(hc1.close_price / hc2.open_price, 0)\n"""
        query2 += f"""      WHEN ROUND(hc1.close_price / hc3.open_price, 1) >= 0.9 THEN ROUND(hc1.close_price / hc3.open_price, 0)\n"""
        query2 += f"""      WHEN ROUND(hc1.close_price / hc4.open_price, 1) >= 0.9 THEN ROUND(hc1.close_price / hc4.open_price, 0)\n"""
        query2 += f"""      WHEN ROUND(hc1.close_price / hc5.open_price, 1) >= 0.9 THEN ROUND(hc1.close_price / hc5.open_price, 0)\n"""
        query2 += f"""      WHEN ROUND(hc2.open_price / hc1.close_price, 1) >= 0.9 THEN ROUND(1 / ROUND(hc2.open_price / hc1.close_price, 0), 6)\n"""
        query2 += f"""      WHEN ROUND(hc3.open_price / hc1.close_price, 1) >= 0.9 THEN ROUND(1 / ROUND(hc3.open_price / hc1.close_price, 0), 6)\n"""
        query2 += f"""      WHEN ROUND(hc4.open_price / hc1.close_price, 1) >= 0.9 THEN ROUND(1 / ROUND(hc4.open_price / hc1.close_price, 0), 6)\n"""
        query2 += f"""      WHEN ROUND(hc5.open_price / hc1.close_price, 1) >= 0.9 THEN ROUND(1 / ROUND(hc5.open_price / hc1.close_price, 0), 6)\n"""
        query2 += f"""      ELSE 1\n"""
        query2 += f"""    END AS norm_ratio\n"""
        query2 += f"""  FROM hourly_candles hc1\n"""
        query2 += f"""  LEFT JOIN hourly_candles hc2 ON hc2.date_hour = hc1.date_hour + interval '1' day AND hc2.ticker = hc1.ticker\n"""
        query2 += f"""  LEFT JOIN hourly_candles hc3 ON hc3.date_hour = hc1.date_hour + interval '2' day AND hc3.ticker = hc1.ticker\n"""
        query2 += f"""  LEFT JOIN hourly_candles hc4 ON hc4.date_hour = hc1.date_hour + interval '3' day AND hc4.ticker = hc1.ticker\n"""
        query2 += f"""  LEFT JOIN hourly_candles hc5 ON hc5.date_hour = hc1.date_hour + interval '4' day AND hc5.ticker = hc1.ticker\n"""
        query2 += f"""  LEFT JOIN daily_candles dc ON dc.ticker = hc1.ticker AND dc.day = DATE(hc1.date_hour)\n"""
        query2 += f"""  WHERE hc1.ticker = \'{ticker}\'\n"""
        query2 += f"""    AND dc.day IS NULL) n\n"""
        query2 += f"""WHERE\n"""
        query2 += f"""  hc.ticker = \'{ticker}\'\n"""
        query2 += f"""  AND hc.ticker = n.ticker\n"""
        query2 += f"""  AND hc.date_hour = n.date_hour\n"""
        query2 += f"""  AND n.norm_ratio <> 1;"""

        self.insert_update(query2)

    def create_missing_daily_candles_from_hourly(self, ticker):

        query = f"""INSERT INTO daily_candles (ticker, day, open_price, max_price, min_price, close_price, volume)\n"""
        query += f"""SELECT agg2.ticker, agg2.date_hour AS day, agg2.open_price, agg2.max_price, agg2.min_price, hc3.close_price, agg2.volume\n"""
        query += f"""FROM\n"""
        query += f"""	(SELECT agg.ticker, agg.date_hour, hc2.open_price, agg.max_price, agg.min_price, agg.volume, agg.max_date_hour\n"""
        query += f"""	FROM\n"""
        query += f"""		(SELECT hc.ticker, DATE(hc.date_hour) AS date_hour, MIN(hc.date_hour) AS min_date_hour, MAX(hc.date_hour) AS max_date_hour, MAX(hc.max_price) AS max_price, MIN(hc.min_price) AS min_price, SUM(hc.volume) AS volume\n"""
        query += f"""		FROM hourly_candles hc\n"""
        query += f"""		LEFT JOIN daily_candles dc ON DATE(dc.day) = DATE(hc.date_hour)\n"""
        query += f"""		WHERE\n"""
        query += f"""		  dc.day IS NULL\n"""
        query += f"""		GROUP BY hc.ticker, DATE(hc.date_hour)) agg\n"""
        query += f"""	INNER JOIN hourly_candles hc2 ON hc2.ticker = agg.ticker and hc2.date_hour = agg.min_date_hour) agg2\n"""
        query += f"""INNER JOIN hourly_candles hc3 ON hc3.ticker = agg2.ticker and hc3.date_hour = agg2.max_date_hour\n"""
        query += f"""WHERE agg2.ticker = \'{ticker}\'\n"""
        query += f"""ON CONFLICT ON CONSTRAINT daily_data_pkey DO NOTHING;"""

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
        query += f"""		(SELECT dc.ticker, DATE_PART('week', dc.day) AS week, MIN(dc.day) AS min_day, MAX(dc.day) AS max_day, \n"""
        query += f"""			MAX(dc.max_price) AS max_price, MIN(dc.min_price) AS min_price, SUM(dc.volume) AS volume\n"""
        query += f"""		FROM daily_candles dc\n"""
        query += f"""		GROUP BY dc.ticker, DATE_PART('week', dc.day)) agg\n"""
        query += f"""	INNER JOIN daily_candles dc2 ON dc2.ticker = agg.ticker AND dc2.day = agg.min_day) agg2\n"""
        query += f"""INNER JOIN daily_candles dc3 ON dc3.ticker = agg2.ticker AND dc3.day = agg2.max_day\n"""
        query += f"""WHERE dc3.ticker = \'{ticker}\'"""
        query += f"""ON CONFLICT ON CONSTRAINT weekly_data_pkey DO NOTHING;"""

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

        table = 'daily_candles'
        time_column = 'day'

        if interval == '1wk':
            table = 'weekly_candles'
            time_column = 'week'
        elif interval == '1h':
            table = 'hourly_candles'
            time_column = 'date_hour'

        query = f"""SELECT ticker, {time_column}, open_price, max_price, min_price, close_price, volume\n"""
        query += f"""FROM {table}\n"""
        query += f"""WHERE\n"""

        for index, (ticker, initial_date, final_date) in enumerate(zip(tickers, initial_dates, final_dates)):
            if index == 0:
                query += f"""  (ticker = \'{ticker}\' and {time_column} >= \'{initial_date.strftime('%Y-%m-%d')}\' and {time_column} < \'{final_date.strftime('%Y-%m-%d')}\')\n"""
            else:
                query += f"""  OR (ticker = \'{ticker}\' and {time_column} >= \'{initial_date.strftime('%Y-%m-%d')}\' and {time_column} < \'{final_date.strftime('%Y-%m-%d')}\')\n"""

        query += f""";"""

        return pd.read_sql_query(query, self._connection)

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