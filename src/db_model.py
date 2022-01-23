import psycopg2
import os
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
import sys
from decimal import *
import pandas as pd
import numpy as np
import math
from datetime import timedelta

import constants as c
from utils import State

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
    """Database connection class that handles single ticker operations."""
    def __init__(self):
        try:
            connection = psycopg2.connect(f"dbname='{DB_NAME}' user={DB_USER} "
                f"host='{DB_HOST}' password={DB_PASS} port='{DB_PORT}'")
            logger.debug(f'Database \'{DB_NAME}\' connected successfully.')
        except:
            logger.error(f'Database \'{DB_NAME}\' connection failed.')
            sys.exit(c.DB_CONNECTION_ERR)

        self._connection = connection
        self._cursor = self._connection.cursor()

    def __del__(self):
        self._connection.close()
        self._cursor.close()

    def _insert_update(self, query, params=None):
        """Insert/Update given query in database."""
        try:
            self._cursor.execute(query, params)
            self._connection.commit()
        except Exception as error:
            logger.error('Error executing query "{}", error:\n{}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            sys.exit(c.QUERY_ERR)

    def get_date_range(self, ticker):
        """
        Get date range in symbol_status table.

        Args
        ----------
        ticker : str
            Ticker name.
        """
        query = f"SELECT\n"
        query += f"  last_update_daily_candles,\n"
        query += f"  start_date_daily_candles,\n"
        query += f"  end_date_daily_candles,\n"
        query += f"  last_update_weekly_candles,\n"
        query += f"  start_date_weekly_candles,\n"
        query += f"  end_date_weekly_candles\n"
        query += f"FROM symbol_status\n"
        query += f"WHERE ticker = \'{ticker}\';"

        df = pd.read_sql_query(query, self._connection)
        return df

    def insert_daily_candles(self, ticker, candles_df, ordinary_ticker=True):
        """
        Insert candlestick data.

        No treatment if data is empty because it is not supposed to be.
        Be careful not to insert duplicate data.

        Args
        ----------
        ticker : str
            Ticker name.
        data : `pandas.DataFrame`
            DataFrame of candles with columns 'Open', 'High', 'Low', 'Close'
            and index of dates.
        ordinary_ticker : bool
            Indication whether is ordinary or not (e.g., index, curency).
            Non ordinary tickers allow duplicate pkey insertion.
        """
        number_of_rows = len(candles_df)
        query = "INSERT INTO daily_candles (ticker, day, open_price, max_price, "\
            "min_price, close_price, volume)\nVALUES\n"

        for n, (index, row) in enumerate(candles_df.iterrows()):
            if n == number_of_rows - 1:
                query += f"(\'{ticker}\', \'{index.strftime('%Y-%m-%d')}\', " \
                    f"{row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, " \
                    f"{row['Close']:.6f}, {row['Volume']:.0f})\n"
            else:
                query += f"(\'{ticker}\', \'{index.strftime('%Y-%m-%d')}\', " \
                    f"{row['Open']:.6f}, {row['High']:.6f}, {row['Low']:.6f}, " \
                    f"{row['Close']:.6f}, {row['Volume']:.0f}),\n"
        if ordinary_ticker == False:
            query += 'ON CONFLICT ON CONSTRAINT daily_data_pkey DO NOTHING'
        query += ';'
        self._insert_update(query)

    def upsert_splits(self, ticker, splits_df):
        """"
        Update insert split data.

        Args
        ----------
        ticker : str
            Ticker name.
        splits_df : `pandas.DataFrame`
            DataFrame of candles with column 'Stock Splits' and index of dates.
        """
        number_of_rows = len(splits_df)
        query = 'INSERT INTO split (ticker, split_date, ratio)\nVALUES\n'

        for n, (index, row) in enumerate(splits_df.iterrows()):
            if n == number_of_rows - 1:
                query += f"('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Stock Splits']})\n"
            else:
                query += f"('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Stock Splits']}),\n"

        query += 'ON CONFLICT ON CONSTRAINT split_pkey DO'
        query += '\nUPDATE SET ratio = EXCLUDED.ratio'
        self._insert_update(query)

    def upsert_dividends(self, ticker, dividends_df):
        """"
        Update insert dividends data.

        Args
        ----------
        ticker : str
            Ticker name.
        dividends_df : `pandas.DataFrame`
            DataFrame of candles with column 'Dividends' and index of dates.
        """
        number_of_rows = len(dividends_df)
        query = 'INSERT INTO dividends (ticker, payment_date, price_per_stock)\nVALUES\n'

        for n, (index, row) in enumerate(dividends_df.iterrows()):
            if n == number_of_rows - 1:
                query += f"('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Dividends']})\n"
            else:
                query += f"('{ticker}', '{index.strftime('%Y-%m-%d')}', {row['Dividends']}),\n"

        query += 'ON CONFLICT ON CONSTRAINT dividends_pkey DO'
        query += '\nUPDATE SET price_per_stock = EXCLUDED.price_per_stock'

        self._insert_update(query)

    def normalize_daily_candles(self, ticker, start_date, end_date, normalization_factor):
        """
        Update candlestick data by dividing by `normalization_factor`.

        Args
        ----------
        ticker : str
            Ticker name.
        start_date : `datetime.date`
            Start date.
        end_date : `datetime.date`
            End date.
        normalization_factor : float
            Normalization factor.
        """
        query = f'UPDATE daily_candles\nSET\n'
        query += f"  open_price = open_price/{normalization_factor:.6f},\n"
        query += f"  max_price = max_price/{normalization_factor:.6f},\n"
        query += f"  min_price = min_price/{normalization_factor:.6f},\n"
        query += f"  close_price = close_price/{normalization_factor:.6f}\n"
        query += f"WHERE\n  ticker = \'{ticker}\'\n"
        query += f"  AND day >= \'{start_date.strftime('%Y-%m-%d')}\'\n"
        query += f"  AND day <= \'{end_date.strftime('%Y-%m-%d')}\'"

        self._insert_update(query)

    def delete_weekly_candles(self, ticker):
        """
        Delete all weekly candlesticks of ticker.

        Args
        ----------
        ticker : str
            Ticker name.
        """
        query = f"DELETE FROM weekly_candles WHERE ticker = \'{ticker}\'"
        self._insert_update(query)

    def create_weekly_candles(self, ticker):
        """
        Create all weekly candlesticks of ticker from daily data.

        Args
        ----------
        ticker : str
            Ticker name.
        """

        query = f"INSERT INTO weekly_candles (ticker, week, open_price, max_price, min_price, close_price, volume)\n"
        query += f"SELECT agg2.ticker, agg2.min_day, agg2.open_price, agg2.max_price, agg2.min_price, dc3.close_price, agg2.volume\n"
        query += f"FROM\n"
        query += f"	(SELECT agg.ticker, agg.min_day, dc2.open_price, agg.max_price, agg.min_price, agg.volume, agg.max_day\n"
        query += f"	FROM\n"
        query += f"		(\n"
        query += f"        SELECT \n"
        query += f"            dc.ticker, \n"
        query += f"            MIN(dc.day) AS min_day, \n"
        query += f"			MAX(dc.day) AS max_day, \n"
        query += f"            MAX(dc.max_price) AS max_price, \n"
        query += f"            MIN(dc.min_price) AS min_price, \n"
        query += f"            SUM(dc.volume) AS volume \n"
        query += f"        FROM daily_candles dc\n"
        query += f"         WHERE dc.ticker = \'{ticker}\'\n"
        query += f"        GROUP BY dc.ticker,\n"
        query += f"            CASE \n"
        query += f"                WHEN DATE_PART('week', dc.day) = 1 AND DATE_PART('month', dc.day) = 12 THEN DATE_PART('year', dc.day) + 1\n"
        query += f"                ELSE DATE_PART('year', dc.day)\n"
        query += f"            END, \n"
        query += f"            DATE_PART('week', dc.day)\n"
        query += f"        ) agg\n"
        query += f"	INNER JOIN daily_candles dc2 ON dc2.ticker = agg.ticker AND dc2.day = agg.min_day) agg2\n"
        query += f"INNER JOIN daily_candles dc3 ON dc3.ticker = agg2.ticker AND dc3.day = agg2.max_day\n"
        query += f"ON CONFLICT ON CONSTRAINT weekly_data_pkey DO NOTHING;"

        self._insert_update(query)

    def get_candlesticks(self, ticker, start_date, end_date, days_before_initial_date=0, interval='1d'):
        """
        Get daily or weekly candlesticks.

        Args
        ----------
        ticker : str
            Ticker name.
        start_date : `datetime.date`
            Start date.
        end_date : `datetime.date`
            End date.
        days_before_initial_date :

        interval : str, default '1d'
            Selected interval: '1d', '1wk'.

        Returns
        ----------
        `pandas.DataFrame`
            DataFrame with candlestick data.
        """
        if interval not in ['1d', '1wk']:
            logger.error(f"Error argument \'interval\'=\'"
                f"{interval}\' must be \'1d\' or \'1wk\'.")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        table = 'daily_candles'
        time_column = 'day'

        if interval == '1wk':
            table = 'weekly_candles'
            time_column = 'week'

        query = f"SELECT ticker, {time_column}, open_price, max_price, min_price, " \
            f"close_price, volume\nFROM {table}\nWHERE\n"

        query += f"  (ticker = \'{ticker}\' AND {time_column} >= " \
            f"\'{(start_date-timedelta(days=days_before_initial_date)).strftime('%Y-%m-%d')}\'" \
            f" AND {time_column} <= \'{end_date.strftime('%Y-%m-%d')}\')"

        query += f"ORDER BY ticker, {time_column};"
        return pd.read_sql_query(query, self._connection)

    def delete_features(self, ticker, interval='1d'):
        """
        Delete all features from given ticker in the specified time interval.

        Args
        ----------
        ticker : str
            Ticker name.
        interval : str, default '1d'
            Selected interval: '1d', '1wk'.
        """
        if interval not in ['1d', '1wk']:
            logger.error(f"Error argument \'interval\'=\'"
                f"{interval}\' must be \'1d\' or \'1wk\'.")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        table = 'daily_features'

        if interval == '1wk':
            table = 'weekly_features'

        query = f"DELETE FROM {table}\nWHERE\n"
        query += f"  ticker = \'{ticker}\';"

        self._insert_update(query)

    def upsert_features(self, df, interval='1d'):
        """
        Update/Insert features.
        """
        table = 'daily_features'
        time_column = 'day'
        if interval == '1wk':
            table = 'weekly_features'
            time_column = 'week'

        columns_list = list(df.columns)

        query = f"INSERT INTO {table} (ticker, {time_column}"
        if 'ema_17' in columns_list:
            query += ", ema_17"
        if 'ema_72' in columns_list:
            query += ", ema_72"
        if 'target_buy_price' in columns_list:
            query += ", target_buy_price"
        if 'stop_loss' in columns_list:
            query += ", stop_loss"
        if 'up_down_trend_status' in columns_list:
            query += ", up_down_trend_status"
        if 'peak' in columns_list:
            query += ", peak"
        if 'up_down_trend_status_strict' in columns_list:
            query += ", up_down_trend_status_strict"
        query += ")\nVALUES\n"

        number_of_rows = len(df)

        for n, (_, row) in enumerate(df.iterrows()):
            query += f"(\'{row['ticker']}\', \'{row[time_column].strftime('%Y-%m-%d')}\'"

            if 'ema_17' in columns_list:
                query += f", {row['ema_17']:.2f}"
            if 'ema_72' in columns_list:
                query += f", {row['ema_72']:.2f}"
            if 'target_buy_price' in columns_list:
                query += f", {row['target_buy_price']:.2f}"
            if 'stop_loss' in columns_list:
                query += f", {row['stop_loss']:.2f}"
            if 'up_down_trend_status' in columns_list:
                query += f", {row['up_down_trend_status']}"
            if 'peak' in columns_list:
                query += f", {row['peak']:.2f}"
            if 'up_down_trend_status_strict' in columns_list:
                query += f", {row['up_down_trend_status_strict']}"
            query += ")"

            if n != number_of_rows - 1:
                query += ',\n'
            else:
                query += ';'

        query = query.replace('nan', 'NULL')

        # query += f"ON CONFLICT ON CONSTRAINT {pkey_constraint} DO\n"
        # query += f"UPDATE SET ticker = EXCLUDED.ticker, {time_column} = EXCLUDED.{time_column}, " \
        #     f"peak = EXCLUDED.peak, ema_17 = EXCLUDED.ema_17, ema_72 = EXCLUDED.ema_72, " \
        #     f"up_down_trend_status = EXCLUDED.up_down_trend_status;"

        self._insert_update(query)

class DBGenericModel:
    """Database connection class that handles generic queries."""
    def __init__(self):
        try:
            connection = psycopg2.connect(f"dbname='{DB_NAME}' user={DB_USER} " \
                f"host='{DB_HOST}' password={DB_PASS} port='{DB_PORT}'")
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

    def _query(self, query, params=None):
        try:
            self._cursor.execute(query, params)
        except Exception as error:
            logger.error('Error executing query "{}", error:\n{}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            # logger.debug('Database \'StockMarket\' connection closed.')
            sys.exit(c.QUERY_ERR)

        return self._cursor.fetchall()

    def _insert_update(self, query, params=None):
        try:
            self._cursor.execute(query, params)
            self._connection.commit()
        except Exception as error:
            logger.error('Error executing query "{}", error:\n{}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            sys.exit(c.QUERY_ERR)

    def get_holidays(self, start_date, end_date):
        """
        Get holidays.

        Args
        ----------
        start_date : `datetime.date`
            Start date.
        end_date : `datetime.date`
            End date.

        Returns
        ----------
        `pandas.DataFrame`
            DataFrame of holidays. Columns: 'day'
        """
        query = f"SELECT day FROM holidays\nWHERE\n"
        query += f"  day >= \'{start_date.strftime('%Y-%m-%d')}\' and day <= " \
            f"\'{end_date.strftime('%Y-%m-%d')}\';"

        df = pd.read_sql_query(query, self._connection).squeeze()

        if df.empty:
            logger.error(f"Holidays table is empty.")
            sys.exit(c.NO_HOLIDAYS_DATA_ERR)

        return df

    def get_holidays_interval(self):
        """
        Get oldest and most recent holidays dates.

        Returns
        ----------
        `datetime.date`
            Oldest date.
        `datetime.date`
            Most recent date.
        """
        query = f"SELECT MIN(day) as min_day, MAX(day) AS max_day FROM holidays;"

        df = pd.read_sql_query(query, self._connection)

        if df.empty:
            logger.error(f"cdi table is empty")
            sys.exit(c.NO_CDI_DATA_ERR)

        return df['min_day'][0].to_pydatetime().date(), df['max_day'][0].to_pydatetime().date()

    def get_cdi_interval(self):
        """
        Get oldest and most recent CDI dates.

        Returns
        ----------
        `datetime.date`
            Oldest date.
        `datetime.date`
            Most recent date.
        """
        query = f"SELECT MIN(day) as min_day, MAX(day) AS max_day FROM cdi;"

        df = pd.read_sql_query(query, self._connection)

        if df.empty:
            logger.error(f"CDI table is empty.")
            sys.exit(c.NO_CDI_DATA_ERR)

        return df['min_day'][0].to_pydatetime().date(), df['max_day'][0].to_pydatetime().date()

    def get_tickers(self, on_flag, pn_flag, units_flag, fractional_market=False,
        sectors=None, subsectors=None, segments=None):

        if not any([on_flag, pn_flag, units_flag]):
            logger.error('Program aborted. At least one filter is required.')
            sys.exit(c.INVALID_ARGUMENT_ERR)

        # Transform parameters without loosing its value
        if sectors == None:
            sectors = []
        else:
            sectors = [item.strip() for item in sectors.split('|') if item != '']

        if subsectors == None:
            subsectors = []
        else:
            subsectors = [item.strip() for item in subsectors.split('|') if item != '']

        if segments == None:
            segments = []
        else:
            segments = [item.strip() for item in segments.split('|') if item != '']

        filters = []
        if on_flag == True:
            filters.append('____3')
        if pn_flag == True:
            filters.append('____4')
        if units_flag == True:
            filters.append('____11')
        if fractional_market == True:
            filters = [filter + "F" for filter in filters]

        query = f"SELECT ticker FROM symbol s\n"
        query += f"INNER JOIN entity e ON e.trading_name = s.trading_name\n"
        query += f"INNER JOIN company_classification cc ON cc.id = e.company_classification_id\n"
        query += f"WHERE\n"

        for index, filter in enumerate(filters):
            if index == 0:
                query += f"  (ticker LIKE \'{filter}\'\n"
            else:
                query += f"  OR ticker LIKE \'{filter}\'\n"
            if index == len(filters)-1:
                query += f"  )\n"

        if any([sectors, subsectors, segments]):
            if len(sectors) > 0:
                query += f"  AND "
            for index, sector in enumerate(sectors):
                if index == 0:
                    query += f"  (cc.economic_sector ILIKE \'%{sector}%\'\n"
                else:
                    query += f"  OR cc.economic_sector ILIKE \'%{sector}%\'\n"
                if index == len(sectors)-1:
                    query += f"  )\n"

            if len(subsectors) > 0:
                query += f"  AND "
            for index, subsector in enumerate(subsectors):
                if index == 0:
                    query += f"  (cc.economic_subsector ILIKE \'%{subsector}%\'\n"
                else:
                    query += f"  OR cc.economic_subsector ILIKE \'%{subsector}%\'\n"
                if index == len(subsectors)-1:
                    query += f"  )\n"

            if len(segments) > 0:
                query += f"  AND "
            for index, segment in enumerate(segments):
                if index == 0:
                    query += f"  (cc.economic_segment ILIKE \'%{segment}%\'\n"
                else:
                    query += f"  OR cc.economic_segment ILIKE \'%{segment}%\'\n"
                if index == len(segments)-1:
                    query += f"  )\n"

        query += "ORDER BY ticker ASC;"

        df = pd.read_sql_query(query, self._connection)
        return df

class DBStrategyModel:
    def __init__(self, name, tickers, start_dates, end_dates, total_capital, alias=None, comment=None, risk_capital_product=None, min_volume_per_year=0):
        try:
            connection = psycopg2.connect(f"dbname='{DB_NAME}' user={DB_USER} " \
                f"host='{DB_HOST}' password={DB_PASS} port='{DB_PORT}'")
            logger.debug(f'Database \'{DB_NAME}\' connected successfully.')
        except:
            logger.error(f'Database \'{DB_NAME}\' connection failed.')
            sys.exit(c.DB_CONNECTION_ERR)

        self._connection = connection
        self._cursor = self._connection.cursor()

        self._name = name
        self._tickers = tickers
        self._start_dates = start_dates
        self._end_dates = end_dates
        self._total_capital = total_capital
        self._alias = alias
        self._comment = comment
        self._risk_capital_product = risk_capital_product
        self._min_volume_per_year = min_volume_per_year

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, alias):
        self._alias = alias

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, comment):
        self._comment = comment

    @property
    def risk_capital_product(self):
        return self._risk_capital_product

    @risk_capital_product.setter
    def risk_capital_product(self, risk_capital_product):
        self._risk_capital_product = risk_capital_product

    @property
    def min_volume_per_year(self):
        return self._min_volume_per_year

    @min_volume_per_year.setter
    def min_volume_per_year(self, min_volume_per_year):
        self._min_volume_per_year = min_volume_per_year

    @property
    def tickers(self):
        return self._tickers

    @property
    def start_dates(self):
        return self._start_dates

    @property
    def end_dates(self):
        return self._end_dates

    def __del__(self):
        self._connection.close()
        self._cursor.close()
        # logger.debug('Database \'StockMarket\' connection closed.')

    def _query(self, query, params=None):
        try:
            self._cursor.execute(query, params)
        except Exception as error:
            logger.error('Error executing query "{}", error:\n{}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            # logger.debug('Database \'StockMarket\' connection closed.')
            sys.exit(c.QUERY_ERR)

        return self._cursor.fetchall()

    def _insert_update(self, query, params=None):
        try:
            self._cursor.execute(query, params)
            self._connection.commit()
        except Exception as error:
            logger.error('Error executing query "{}", error:\n{}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            sys.exit(c.QUERY_ERR)

    def _insert_update_with_returning(self, query, params=None):
        try:
            self._cursor.execute(query, params)
            id_of_new_row = self._cursor.fetchone()[0]
            self._connection.commit()
        except Exception as error:
            logger.error('Error executing query "{}", error:\n{}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            sys.exit(c.QUERY_ERR)

        return id_of_new_row

    def get_tickers_above_min_volume(self):
        query = f"SELECT DISTINCT tic_y.ticker\n"
        query += f"FROM\n"
        query += f"(SELECT dc.ticker, EXTRACT(YEAR FROM dc.day), ROUND(AVG(dc.volume), 0) " \
            f"FROM daily_candles dc\n"
        query += f"GROUP BY dc.ticker, EXTRACT(YEAR FROM dc.day)\n"
        query += f"HAVING ROUND(AVG(dc.volume), 0) > {self._min_volume_per_year}) tic_y\n"
        query += f"ORDER BY tic_y.ticker;"

        return self._query(query)

    def get_data_chunk(self, tickers, start_date, end_date, interval='1d'):

        if interval not in ['1d', '1wk']:
            logger.error(f"Error argument \'interval\'=\'"
                f"{interval}\' must be \'1d\' or \'1wk\'.")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        candles_table = 'daily_candles'
        features_table = 'daily_features'
        time_column = 'day'

        if interval == '1wk':
            candles_table = 'weekly_candles'
            features_table = 'weekly_features'
            time_column = 'week'

        query = f"SELECT\n"
        query += f"  cand.ticker,\n"
        query += f"  cand.{time_column},\n"
        if interval == '1d':
            query += f"  cand.open_price,\n"
            query += f"  cand.max_price,\n"
            query += f"  cand.min_price,\n"
            query += f"  cand.close_price,\n"
            # query += f"  cand.volume, \n"
            query += f"  feat.ema_17,\n"
            query += f"  feat.ema_72,\n"
            query += f"  feat.target_buy_price,\n"
            query += f"  feat.stop_loss,\n"
            query += f"  feat.up_down_trend_status,\n"
            query += f"  feat.peak\n"
        else:
            query += f"  feat.ema_72\n"
        query += f"FROM {candles_table} cand\n"
        query += f"INNER JOIN {features_table} feat\n"
        query += f"  ON feat.ticker = cand.ticker AND feat.{time_column} = cand.{time_column}\n"
        query += f"WHERE\n"
        query += f"  cand.ticker in {str(list(tickers.keys())).replace('[', '(').replace(']', ')')}\n"

        if interval == '1wk':
            new_start_date = start_date - pd.Timedelta(days=7)
            # query += f"  AND cand.{time_column} >= TO_DATE(CONCAT(EXTRACT(YEAR FROM TIMESTAMP " \
            #     f"\'{new_start_date.strftime('%Y-%m-%d')}\'), EXTRACT(WEEK FROM TIMESTAMP " \
            #     f"\'{new_start_date.strftime('%Y-%m-%d')}\')), \'IYYYIW\')\n"
            query += f"  AND cand.week >= \n"
            query += f"    CASE\n"
            query += f"        WHEN EXTRACT(MONTH FROM TIMESTAMP " \
                f"\'{new_start_date.strftime('%Y-%m-%d')}\') = 12 AND EXTRACT(WEEK " \
                f"FROM TIMESTAMP \'{new_start_date.strftime('%Y-%m-%d')}\') = 1\n"
            query += f"        THEN TO_DATE(CONCAT(EXTRACT(YEAR FROM TIMESTAMP \'" \
                f"{new_start_date.strftime('%Y-%m-%d')}\')+1, EXTRACT(WEEK FROM TIMESTAMP " \
                f"\'{new_start_date.strftime('%Y-%m-%d')}\')), \'IYYYIW\')\n"
            query += f"        ELSE TO_DATE(CONCAT(EXTRACT(YEAR FROM TIMESTAMP " \
                f"\'{new_start_date.strftime('%Y-%m-%d')}\'), EXTRACT(WEEK FROM TIMESTAMP " \
                f"\'{new_start_date.strftime('%Y-%m-%d')}\')), \'IYYYIW\')\n"
            query += f"    END\n"

            # query += f"  AND cand.{time_column} < TO_DATE(CONCAT(EXTRACT(YEAR FROM TIMESTAMP " \
            #     f"\'{end_date.strftime('%Y-%m-%d')}\'), EXTRACT(WEEK FROM TIMESTAMP " \
            #     f"\'{end_date.strftime('%Y-%m-%d')}\')), \'IYYYIW\')\n"
            query += f"  AND cand.week < \n"
            query += f"  CASE \n"
            query += f"    WHEN EXTRACT(MONTH FROM TIMESTAMP \'" \
                f"{end_date.strftime('%Y-%m-%d')}\') = 12 AND EXTRACT(WEEK FROM TIMESTAMP" \
                f" \'{end_date.strftime('%Y-%m-%d')}\') = 1\n"
            query += f"      THEN TO_DATE(CONCAT(EXTRACT(YEAR FROM TIMESTAMP \'" \
                f"{end_date.strftime('%Y-%m-%d')}\')+1, EXTRACT(WEEK FROM TIMESTAMP " \
                f"\'{end_date.strftime('%Y-%m-%d')}\')), \'IYYYIW\')\n"
            query += f"    ELSE TO_DATE(CONCAT(EXTRACT(YEAR FROM TIMESTAMP " \
                f"\'{end_date.strftime('%Y-%m-%d')}\'), EXTRACT(WEEK FROM TIMESTAMP " \
                f"\'{end_date.strftime('%Y-%m-%d')}\')), \'IYYYIW\')\n"
            query += f"  END\n"
        elif interval == '1d':
            query += f"  AND cand.{time_column} >= \'{start_date.strftime('%Y-%m-%d')}\'\n"
            query += f"  AND cand.{time_column} <= \'{end_date.strftime('%Y-%m-%d')}\'\n"

        query += f"ORDER BY cand.{time_column}, cand.ticker;\n"

        df = pd.read_sql_query(query, self._connection)
        # df['ticker'] =  df['ticker'].apply(lambda x: x.rstrip())
        # df.sort_values(['ticker', time_column], axis=0, ascending=True, ignore_index=True, inplace=True)

        return df

    # Reconsider
    # def get_candles_dataframe(self, tickers_and_dates, interval='1d', days_before_initial_dates=0):

    #     tickers = []
    #     start_dates = []
    #     end_dates = []

    #     for ticker, date in tickers_and_dates.items():
    #         tickers.append(ticker)
    #         start_dates.append(date['start_date'])
    #         end_dates.append(date['end_date'])

    #     candles_table = 'daily_candles'
    #     features_table = 'daily_features'
    #     time_column = 'day'

    #     if interval == '1wk':
    #         candles_table = 'weekly_candles'
    #         features_table = 'weekly_features'
    #         time_column = 'week'

    #     query = f"SELECT \n"
    #     query += f"  cand.ticker, \n"
    #     query += f"  cand.{time_column}, \n"
    #     if interval == '1d':
    #         query += f"  cand.open_price, \n"
    #         query += f"  cand.max_price, \n"
    #         query += f"  cand.min_price, \n"
    #         query += f"  cand.close_price, \n"
    #     # query += f"  cand.volume, \n"
    #     query += f"  feat.ema_17, \n"
    #     query += f"  feat.ema_72, \n"
    #     if interval == '1d':
    #         query += f"  feat.target_buy_price, \n"
    #         query += f"  feat.stop_loss, \n"
    #     query += f"  feat.up_down_trend_status\n"
    #     query += f"FROM {candles_table} cand\n"
    #     query += f"INNER JOIN {features_table} feat "
    #     query += f"  ON feat.ticker = cand.ticker AND feat.{time_column} = cand.{time_column}\n"
    #     query += f"WHERE\n"

    #     for index, (ticker, start_date, end_date) in enumerate(zip(tickers, start_dates, end_dates)):
    #         if index != 0:
    #             query += f"  OR "
    #         query += f"  (cand.ticker = \'{ticker}\' and cand.{time_column} >= " \
    #             f"\'{(start_date - timedelta(days=days_before_initial_dates)).strftime('%Y-%m-%d')}\' " \
    #             f"and cand.{time_column} < \'{end_date.strftime('%Y-%m-%d')}\')\n"
    #     query += f";"

    #     df = pd.read_sql_query(query, self._connection)
    #     df['ticker'] =  df['ticker'].apply(lambda x: x.rstrip())
    #     # df.sort_values(['ticker', time_column], axis=0, ascending=True, ignore_index=True, inplace=True)

    #     return df

    def insert_strategy_results(self, result_parameters, operations, performance_dataframe):
        strategy_id = self._insert_strategy()
        self._insert_strategy_tickers(strategy_id)
        self._insert_operations(strategy_id, operations)
        self._insert_strategy_statistics(strategy_id, result_parameters)
        self._insert_strategy_performance(strategy_id, performance_dataframe)

    def _insert_strategy(self):
        query = f"INSERT INTO strategy (name, alias, comment, total_capital, " \
            f"risk_capital_product)\nVALUES\n"

        query += f"(\'{self._name}\', \'{self._alias if self._alias is not None else ''}\', " \
            f"\'{self._comment if self._comment is not None else ''}\', {self._total_capital}, " \
            f"{self._risk_capital_product})\n"
        query += f"RETURNING id;"

        strategy_id = self._insert_update_with_returning(query)

        return strategy_id

    def _insert_strategy_tickers(self, strategy_id):

        number_of_rows = len(self._tickers)

        query = f"INSERT INTO strategy_tickers (strategy_id, ticker, start_date, end_date)\nVALUES\n"

        for n, (ticker, initial_date, final_date) in \
            enumerate(zip(self._tickers, self._start_dates, self._end_dates)):
            query += f"({strategy_id}, '{ticker}', '{initial_date.strftime('%Y-%m-%d')}', " \
                f"'{final_date.strftime('%Y-%m-%d')}')\n"

            if n != number_of_rows - 1:
                query += ',\n'
            else:
                query += ';'

        self._insert_update(query)

    def _insert_operations(self, strategy_id, operations):

        for operation in operations:
            query = f"INSERT INTO operation (strategy_id, ticker, start_date, end_date, " \
                f"state, target_purchase_price, target_sale_price, stop_loss, profit, yield)\nVALUES\n"

            # if/elif statement only because can not handle operation.end_date being None
            if operation.state == State.CLOSE:
                query += f"({strategy_id}, \'{operation.ticker}\', " \
                    f"\'{operation.start_date.strftime('%Y-%m-%d')}\', " \
                    f"\'{operation.end_date.strftime('%Y-%m-%d')}\', " \
                    f"\'{operation.state.value}\', {operation.target_purchase_price:.2f}, " \
                    f"{operation.target_sale_price:.2f}, {operation.stop_loss:.2f}, " \
                    f"{round(operation.profit, 2) if (operation.profit is not None) and (not math.isnan(operation.profit)) else 'NULL'}, " \
                    f"{round(operation.result_yield, 6) if (operation.result_yield is not None) and (not math.isnan(operation.result_yield)) else 'NULL'})\n"
            elif operation.state == State.OPEN:
                query += f"({strategy_id}, \'{operation.ticker}\', " \
                    f"\'{operation.start_date.strftime('%Y-%m-%d')}\', NULL, \'{operation.state.value}\', " \
                    f"{operation.target_purchase_price:.2f}, {operation.target_sale_price:.2f}, " \
                    f"{operation.stop_loss:.2f}, {round(operation.profit, 2) if (operation.profit is not None) and (not math.isnan(operation.profit)) else 'NULL'}, " \
                    f"{round(operation.result_yield, 6) if (operation.result_yield is not None) and (not math.isnan(operation.result_yield)) else 'NULL'})\n"

            query += f"RETURNING id;"

            operation_id = self._insert_update_with_returning(query)

            self._insert_negotiations(operation_id, operation)

    def _insert_negotiations(self, operation_id, operation):

        number_of_negotiations = operation.number_of_orders
        current_negotiation = 0

        query = f"INSERT INTO negotiation (operation_id, day, buy_sell_flag, price, " \
            f"volume, stop_flag, partial_sale_flag)\nVALUES\n"

        for index in range(len(operation.purchase_price)):
            query += f"({operation_id}, \'{operation.purchase_datetime[index].strftime('%Y-%m-%d')}\', " \
                f"'B', {operation.purchase_price[index]:.2f}, {operation.purchase_volume[index]:.0f}, " \
                f"False, False)"

            current_negotiation += 1

            if current_negotiation != number_of_negotiations:
                query += ',\n'
            else:
                query += ';'

        for index in range(len(operation.sale_price)):
            query += f"({operation_id}, \'{operation._sale_datetime[index].strftime('%Y-%m-%d')}\', " \
                f"'S', {operation.sale_price[index]:.2f}, {operation.sale_volume[index]:.0f}, " \
                f"{operation.stop_loss_flag[index]}, {operation.partial_sale_flag[index]})"

            current_negotiation += 1

            if current_negotiation != number_of_negotiations:
                query += ',\n'
            else:
                query += ';'

        self._insert_update(query)

    def _insert_strategy_statistics(self, strategy_id, result_parameters):
        query = f"INSERT INTO strategy_statistics (strategy_id, volatility, sharpe_ratio, " \
            f"profit, max_used_capital, avg_used_capital, yield, annualized_yield, ibov_yield, " \
            f"annualized_ibov_yield, avr_tickers_yield, annualized_avr_tickers_yield)\nVALUES\n"

        query += f"  ({strategy_id}, {result_parameters['volatility']}, " \
            f"{result_parameters['sharpe_ratio']}, {result_parameters['profit']}, " \
            f"{result_parameters['max_used_capital']}, {result_parameters['avg_used_capital']}, " \
            f"{result_parameters['yield']}, {result_parameters['annualized_yield']}, " \
            f"{result_parameters['ibov_yield']}, {result_parameters['annualized_ibov_yield']}, " \
            f"{result_parameters['avr_tickers_yield']}, " \
            f"{result_parameters['annualized_avr_tickers_yield']});"

        self._insert_update(query)

    def _insert_strategy_performance(self, strategy_id, performance_dataframe):
        query = f"INSERT INTO strategy_performance (strategy_id, day, capital, " \
            f"capital_in_use, active_operations, tickers_average, ibov)\nVALUES\n"

        number_of_rows = len(performance_dataframe)

        for n, (_, row) in enumerate(performance_dataframe.iterrows()):
            query += f"  ({strategy_id}, \'{row['day'].to_pydatetime().strftime('%Y-%m-%d')}\', " \
                f"{row['capital']}, {row['capital_in_use']}, {row['active_operations']}, " \
                f"{row['tickers_average']}, {row['ibov']})"

            if n != number_of_rows - 1:
                query += ',\n'
            else:
                query += ';'

        self._insert_update(query)

    def get_ticker_price(self, ticker, start_date, end_date):

        query = f"SELECT dc.day, dc.close_price FROM daily_candles dc\n"
        query += f"WHERE\n"
        query += f"  dc.ticker = \'{ticker}\'\n"
        query += f"  AND dc.day >= \'{start_date.to_pydatetime().strftime('%Y-%m-%d')}\'\n"
        query += f"  AND dc.day <= \'{end_date.to_pydatetime().strftime('%Y-%m-%d')}\'\n"
        query += f"ORDER BY day;"

        df = pd.read_sql_query(query, self._connection)

        return df

    def get_cdi_index(self, start_date, end_date):
        query = f"SELECT day, value\n"
        query += f"FROM cdi\n"
        query += f"WHERE\n"
        query += f"  day >= \'{start_date}\'\n"
        query += f"  AND day < \'{end_date}\'\n"
        query += f"ORDER BY\n"
        query += f"  day ASC;"

        df = pd.read_sql_query(query, self._connection)

        df['cumulative'] = df['value'].cumprod()

        return df

class DBStrategyAnalyzerModel:
    def __init__(self):
        try:
            connection = psycopg2.connect(f"dbname='{DB_NAME}' user={DB_USER} " \
                f"host='{DB_HOST}' password={DB_PASS} port='{DB_PORT}'")
            logger.debug(f'Database \'{DB_NAME}\' connected successfully.')
        except:
            logger.error(f'Database \'{DB_NAME}\' connection failed.')
            sys.exit(c.DB_CONNECTION_ERR)

        self._connection = connection
        self._cursor = self._connection.cursor()

    def __del__(self):
        self._connection.close()
        self._cursor.close()

    def _query(self, query, params=None):
        try:
            self._cursor.execute(query, params)
        except Exception as error:
            logger.error('Error executing query "{}", error:\n{}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            sys.exit(c.QUERY_ERR)

        return self._cursor.fetchall()

    def get_strategy_ids(self, strategy_id=None):
        query = f"SELECT id, name, alias, comment, total_capital, risk_capital_product\n"
        query += f"FROM strategy\n"

        if strategy_id != None:
            query += f"WHERE id = {strategy_id}\n"

        query += f"ORDER BY id DESC;"

        df = pd.read_sql_query(query, self._connection)

        return df

    def get_strategy_performance(self, strategy_id):
        query = f"SELECT sp.day, sp.capital, sp.capital_in_use, sp.active_operations, " \
            f"sp.tickers_average, sp.ibov\n"
        query += f"FROM strategy_performance sp\n"
        query += f"INNER JOIN strategy s ON s.id = sp.strategy_id\n"
        query += f"WHERE s.id = {strategy_id}\n"
        query += f"ORDER BY sp.day ASC;"

        df = pd.read_sql_query(query, self._connection)

        return df

    def get_strategy_statistics(self, strategy_id):
        query = f"SELECT ss.volatility, ss.sharpe_ratio, ss.profit, ss.max_used_capital, " \
            f"ss.avg_used_capital, ss.yield, ss.annualized_yield, ss.ibov_yield, " \
            f"ss.annualized_ibov_yield, ss.avr_tickers_yield, ss.annualized_avr_tickers_yield\n"
        query += f"FROM strategy_statistics ss\n"
        query += f"INNER JOIN strategy s ON s.id = ss.strategy_id\n"
        query += f"WHERE s.id = {strategy_id};"

        df = pd.read_sql_query(query, self._connection)

        return df

    def get_strategy_tickers(self, strategy_id):
        query = f"SELECT ticker, start_date, end_date\n"
        query += f"FROM strategy_tickers st\n"
        query += f"INNER JOIN strategy s ON s.id = st.strategy_id\n"
        query += f"WHERE\n"
        query += f"  s.id = {strategy_id}\n"
        query += f"ORDER BY\n"
        query += f"  st.ticker ASC;"

        df = pd.read_sql_query(query, self._connection)

        return df

    def get_ticker_prices_and_features(self, ticker, start_date, end_date, interval='1d'):
        query = ""
        if interval == "1wk":
            query = f"SELECT wc.week, wc.close_price, wf.ema_17, wf.ema_72, " \
                f"wf.up_down_trend_status, wf.target_buy_price, wf.stop_loss, wf.peak\n"
            query += f"FROM weekly_candles wc\n"
            query += f"INNER JOIN weekly_features wf ON wf.ticker = wc.ticker AND wf.week = wc.week\n"
            query += f"WHERE \n"
            query += f"  wc.ticker = \'{ticker}\'\n"
            query += f"  AND wc.week >= \'{start_date.to_pydatetime().strftime('%Y-%m-%d')}\'\n"
            query += f"  AND wc.week < \'{end_date.to_pydatetime().strftime('%Y-%m-%d')}\'\n"
            query += f"ORDER BY wc.week ASC;"
        elif interval == '1d':
            query = f"SELECT dc.day, dc.open_price, dc.max_price, dc.min_price, " \
                f"dc.close_price, df.ema_17, df.ema_72, df.up_down_trend_status, " \
                f"df.target_buy_price, df.stop_loss, df.peak, " \
                f"df.up_down_trend_status_strict\n"
            query += f"FROM daily_candles dc\n"
            query += f"INNER JOIN daily_features df ON df.ticker = dc.ticker AND df.day = dc.day\n"
            query += f"WHERE \n"
            query += f"  dc.ticker = \'{ticker}\'\n"
            query += f"  AND dc.day >= \'{start_date.to_pydatetime().strftime('%Y-%m-%d')}\'\n"
            query += f"  AND dc.day < \'{end_date.to_pydatetime().strftime('%Y-%m-%d')}\'\n"
            query += f"ORDER BY dc.day ASC;"


        df = pd.read_sql_query(query, self._connection)

        return df

    def get_operations(self, strategy_id, ticker):
        query = f"SELECT o.ticker, o.id as operation_id, neg.day, neg.price, neg.volume, " \
            f"neg.order_type\n"
        query += f"FROM operation o\n"
        query += f"INNER JOIN strategy s on s.id = o.strategy_id\n"
        query += f"INNER JOIN\n"
        query += f"  (SELECT \n"
        query += f"    operation_id,\n"
        query += f"    day, \n"
        query += f"    price,\n"
        query += f"    volume,\n"
        query += f"    CASE \n"
        query += f"      WHEN buy_sell_flag = 'B' THEN 'PURCHASE'\n"
        query += f"      WHEN stop_flag = TRUE THEN 'STOP_LOSS'\n"
        query += f"	     WHEN partial_sale_flag = TRUE THEN 'PARTIAL_SALE'\n"
        query += f"  	   ELSE 'TARGET_SALE'\n"
        query += f"    END AS order_type\n"
        query += f"  FROM negotiation\n"
        query += f"  ORDER BY day) neg \n"
        query += f"  ON neg.operation_id = o.id\n"
        query += f"WHERE \n"
        query += f"  s.id = {strategy_id}\n"
        query += f"  AND o.ticker = \'{ticker}\'\n"
        query += f"ORDER BY o.strategy_id, o.ticker, neg.day;"

        df = pd.read_sql_query(query, self._connection)

        return df

    def get_operations_statistics(self, strategy_id):

        tolerance = 50

        query = f"SELECT q.status, COUNT(q.status) AS number\n"
        query += f"FROM\n"
        query += f"(SELECT\n"
        query += f"  CASE\n"
        query += f"    WHEN profit > {tolerance} THEN \'SUCCESS\'\n"
        query += f"	WHEN profit > -{tolerance} THEN \'NEUTRAL\'\n"
        query += f"	WHEN o.state = \'OPEN\' THEN \'OPEN\'\n"
        query += f"	ELSE \'FAILURE\'\n"
        query += f"  END AS status\n"
        query += f"FROM operation o\n"
        query += f"INNER JOIN \n"
        query += f"  (SELECT operation_id, MAX(volume) AS volume\n"
        query += f"  FROM negotiation\n"
        query += f"  GROUP BY operation_id) n ON n.operation_id = o.id\n"
        query += f"INNER JOIN strategy s ON s.id = o.strategy_id\n"
        query += f"WHERE\n"
        query += f"  s.id = {strategy_id}\n"
        query += f"  AND n.volume != 0) q\n"
        query += f"GROUP BY q.status\n"
        query += f"ORDER BY q.status;"

        df = pd.read_sql_query(query, self._connection)

        return df

    def get_cdi_index(self, start_date, end_date):
        query = f"SELECT day, value\n"
        query += f"FROM cdi\n"
        query += f"WHERE\n"
        query += f"  day >= \'{start_date}\'\n"
        query += f"  AND day < \'{end_date}\'\n"
        query += f"ORDER BY\n"
        query += f"  day ASC;"

        df = pd.read_sql_query(query, self._connection)

        df['cumulative'] = df['value'].cumprod()

        return df