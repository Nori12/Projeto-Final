import psycopg2
import os
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler
import sys
from decimal import *
import pandas as pd
import math
from datetime import timedelta

import constants as c
from utils import State

# Database macros
DB_USER = os.environ.get('STOCK_MARKET_DB_USER')
DB_PASS = os.environ.get('STOCK_MARKET_DB_PASS')
DB_PORT = os.environ.get('STOCK_MARKET_DB_PORT')
DB_HOST = os.environ.get('STOCK_MARKET_DB_HOST')
DB_NAME = os.environ.get('STOCK_MARKET_DB_NAME')

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
            connection = psycopg2.connect(host=DB_HOST, database=DB_NAME,
                user=DB_USER, password=DB_PASS, port=DB_PORT)

            # logger.debug(f'Database \'{DB_NAME}\' connected successfully.')
        except Exception as error:
            print(f'Database \'{DB_NAME}\' connection failed.\n{error}')
            logger.error(f'Database \'{DB_NAME}\' connection failed.\n{error}')
            # sys.exit(c.DB_CONNECTION_ERR)
            raise error

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
            # sys.exit(c.QUERY_ERR)
            raise error

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
        if ordinary_ticker is False:
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
            # sys.exit(c.INVALID_ARGUMENT_ERR)
            raise Exception

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
            # sys.exit(c.INVALID_ARGUMENT_ERR)
            raise Exception

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
            # logger.debug(f'Database \'{DB_NAME}\' connected successfully.')
        except Exception as error:
            logger.error(f'Database \'{DB_NAME}\' connection failed.')
            # sys.exit(c.DB_CONNECTION_ERR)
            raise error

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
            # sys.exit(c.QUERY_ERR)
            raise error

        return self._cursor.fetchall()

    def _insert_update(self, query, params=None):
        try:
            self._cursor.execute(query, params)
            self._connection.commit()
        except Exception as error:
            logger.error('Error executing query "{}", error:\n{}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            # sys.exit(c.QUERY_ERR)
            raise error

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
            # sys.exit(c.NO_HOLIDAYS_DATA_ERR)
            raise Exception

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
            # sys.exit(c.NO_CDI_DATA_ERR)
            raise Exception

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
            # sys.exit(c.NO_CDI_DATA_ERR)
            raise Exception

        return df['min_day'][0].to_pydatetime().date(), df['max_day'][0].to_pydatetime().date()

    def get_tickers(self, on_flag, pn_flag, units_flag, fractional_market=False,
        sectors=None, subsectors=None, segments=None):

        if not any([on_flag, pn_flag, units_flag]):
            logger.error('Program aborted. At least one filter is required.')
            # sys.exit(c.INVALID_ARGUMENT_ERR)
            raise Exception

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
        if on_flag is True:
            filters.append('____3')
        if pn_flag is True:
            filters.append('____4')
        if units_flag is True:
            filters.append('____11')
        if fractional_market is True:
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

    def __init__(self, name, tickers, start_dates, end_dates, total_capital, alias=None,
        comment=None, risk_capital_product=None, min_order_volume=1, min_risk= None, max_risk=None,
        max_days_per_operation=None, partial_sale=None, min_days_after_successful_operation=None,
        min_days_after_failure_operation=None, stop_type=None, purchase_margin=None,
        stop_margin=None, ema_tolerance=None, gain_loss_ratio=None,
        enable_frequency_normalization=None, enable_profit_compensation=None,
        enable_crisis_halt=None, enable_downtrend_halt=None, enable_dynamic_rcc=None,
        dynamic_rcc_reference=None, dynamic_rcc_k=None, operation_risk=None,
        profit_comp_start_std=None, profit_comp_end_std=None, profit_comp_gain_loss=None):
        try:
            connection = psycopg2.connect(f"dbname='{DB_NAME}' user={DB_USER} " \
                f"host='{DB_HOST}' password={DB_PASS} port='{DB_PORT}'")
            # logger.debug(f'Database \'{DB_NAME}\' connected successfully.')
        except Exception as error:
            logger.error(f'Database \'{DB_NAME}\' connection failed.')
            # sys.exit(c.DB_CONNECTION_ERR)
            raise Exception

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
        self._min_risk = min_risk
        self._max_risk = max_risk
        self._max_days_per_operation = max_days_per_operation
        self._partial_sale = partial_sale
        self._min_days_after_successful_operation = min_days_after_successful_operation
        self._min_days_after_failure_operation = min_days_after_failure_operation
        self._stop_type = stop_type
        self._purchase_margin = purchase_margin
        self._stop_margin = stop_margin
        self._ema_tolerance = ema_tolerance
        self._gain_loss_ratio = gain_loss_ratio
        self._min_order_volume = min_order_volume
        self._enable_frequency_normalization = enable_frequency_normalization
        self._enable_profit_compensation = enable_profit_compensation
        self._enable_crisis_halt = enable_crisis_halt
        self._enable_downtrend_halt = enable_downtrend_halt
        self._enable_dynamic_rcc = enable_dynamic_rcc
        self._dynamic_rcc_reference = dynamic_rcc_reference
        self._dynamic_rcc_k = dynamic_rcc_k
        self._operation_risk = operation_risk
        self._profit_comp_start_std = profit_comp_start_std
        self._profit_comp_end_std = profit_comp_end_std
        self._profit_comp_gain_loss = profit_comp_gain_loss

    @property
    def tickers(self):
        return self._tickers

    @property
    def start_dates(self):
        return self._start_dates

    @property
    def end_dates(self):
        return self._end_dates

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name= name

    @property
    def alias(self):
        return self._alias

    @property
    def comment(self):
        return self._comment

    @property
    def total_capital(self):
        return self._total_capital

    @property
    def risk_capital_product(self):
        return self._risk_capital_product

    @property
    def min_risk(self):
        return self._min_risk

    @property
    def max_risk(self):
        return self._max_risk

    @property
    def max_days_per_operation(self):
        return self._max_days_per_operation

    @property
    def partial_sale(self):
        return self._partial_sale

    @property
    def min_days_after_successful_operation(self):
        return self._min_days_after_successful_operation

    @property
    def min_days_after_failure_operation(self):
        return self._min_days_after_failure_operation

    @property
    def stop_type(self):
        return self._stop_type

    @property
    def purchase_margin(self):
        return self._purchase_margin

    @property
    def stop_margin(self):
        return self._stop_margin

    @property
    def ema_tolerance(self):
        return self._ema_tolerance

    @property
    def gain_loss_ratio(self):
        return self._gain_loss_ratio

    @property
    def gain_loss_ratio(self):
        return self._gain_loss_ratio

    @property
    def min_order_volume(self):
        return self._min_order_volume

    @property
    def enable_frequency_normalization(self):
        return self._enable_frequency_normalization

    @property
    def enable_profit_compensation(self):
        return self._enable_profit_compensation

    @property
    def enable_crisis_halt(self):
        return self._enable_crisis_halt

    @property
    def enable_downtrend_halt(self):
        return self._enable_downtrend_halt

    @property
    def enable_dynamic_rcc(self):
        return self._enable_dynamic_rcc

    @property
    def dynamic_rcc_reference(self):
        return self._dynamic_rcc_reference

    @property
    def dynamic_rcc_k(self):
        return self._dynamic_rcc_k

    @property
    def operation_risk(self):
        return self._operation_risk

    @property
    def profit_comp_start_std(self):
        return self._profit_comp_start_std

    @property
    def profit_comp_end_std(self):
        return self._profit_comp_end_std

    @property
    def profit_comp_gain_loss(self):
        return self._profit_comp_gain_loss


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
            # sys.exit(c.QUERY_ERR)
            raise error

        return self._cursor.fetchall()

    def _insert_update(self, query, params=None):
        try:
            self._cursor.execute(query, params)
            self._connection.commit()
        except Exception as error:
            logger.error('Error executing query "{}", error:\n{}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            # sys.exit(c.QUERY_ERR)
            raise error

    def _insert_update_with_returning(self, query, params=None):
        try:
            self._cursor.execute(query, params)
            id_of_new_row = self._cursor.fetchone()[0]
            self._connection.commit()
        except Exception as error:
            logger.error('Error executing query "{}", error:\n{}'.format(query, error))
            self._connection.close()
            self._cursor.close()
            # sys.exit(c.QUERY_ERR)
            raise error

        return id_of_new_row

    def get_data_chunk(self, tickers, start_date, end_date, interval='1d', volume=False):

        if interval not in ['1d', '1wk']:
            logger.error(f"Error argument \'interval\'=\'"
                f"{interval}\' must be \'1d\' or \'1wk\'.")
            # sys.exit(c.INVALID_ARGUMENT_ERR)
            raise Exception

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
            if volume:
                query += f"  cand.volume, \n"
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

    def insert_strategy_results(self, result_parameters, operations, performance_dataframe):
        strategy_id = self._insert_strategy()
        self._insert_strategy_tickers(strategy_id)
        self._insert_operations(strategy_id, operations)
        self._insert_strategy_statistics(strategy_id, result_parameters)
        self._insert_strategy_performance(strategy_id, performance_dataframe)

    def _insert_strategy(self):
        query = "INSERT INTO strategy (name, alias, comment, total_capital, " \
            "risk_capital_product, min_risk, max_risk, max_days_per_operation, " \
            "partial_sale, min_days_after_successful_operation, " \
            "min_days_after_failure_operation, stop_type, purchase_margin, stop_margin, " \
            "ema_tolerance, gain_loss_ratio, min_order_volume, enable_frequency_normalization, " \
            "enable_profit_compensation, enable_crisis_halt, enable_downtrend_halt, " \
            "enable_dynamic_rcc, dynamic_rcc_reference, dynamic_rcc_k, operation_risk, " \
            "profit_comp_start_std, profit_comp_end_std, profit_comp_gain_loss"

        query += ")\nVALUES\n"

        query += f"(\'{self.name}\', \'{self.alias if self.alias is not None else ''}\', " \
            f"\'{self.comment if self.comment is not None else ''}\', {self.total_capital}, " \
            f"{self.risk_capital_product}, {self.min_risk}, {self.max_risk}, " \
            f"{self.max_days_per_operation}, {self.partial_sale}, " \
            f"{self.min_days_after_successful_operation}, " \
            f"{self.min_days_after_failure_operation}, \'{self.stop_type}\', " \
            f"{self.purchase_margin}, {self.stop_margin}, {self.ema_tolerance}, " \
            f"{self.gain_loss_ratio}, {self.min_order_volume}, " \
            f"{self.enable_frequency_normalization}, {self.enable_profit_compensation}, " \
            f"{self.enable_crisis_halt}, {self.enable_downtrend_halt}, " \
            f"{self.enable_dynamic_rcc}, {self.dynamic_rcc_reference}, {self.dynamic_rcc_k}, " \
            f"{self.operation_risk}, {self.profit_comp_start_std}, {self.profit_comp_end_std}, " \
            f"{self.profit_comp_gain_loss}"

        query += f")\nRETURNING id;"

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
            f"volume, stop_flag, partial_sale_flag, timeout_flag)\nVALUES\n"

        for index in range(len(operation.purchase_price)):
            query += f"({operation_id}, \'{operation.purchase_datetime[index].strftime('%Y-%m-%d')}\', " \
                f"'B', {operation.purchase_price[index]:.2f}, {operation.purchase_volume[index]:.0f}, " \
                f"False, False, False)"

            current_negotiation += 1

            if current_negotiation != number_of_negotiations:
                query += ',\n'
            else:
                query += ';'

        for index in range(len(operation.sale_price)):
            query += f"({operation_id}, \'{operation._sale_datetime[index].strftime('%Y-%m-%d')}\', " \
                f"'S', {operation.sale_price[index]:.2f}, {operation.sale_volume[index]:.0f}, " \
                f"{operation.stop_loss_flag[index]}, {operation.partial_sale_flag[index]}, " \
                f"{operation.timeout_flag[index]})"

            current_negotiation += 1

            if current_negotiation != number_of_negotiations:
                query += ',\n'
            else:
                query += ';'

        self._insert_update(query)

    def _insert_strategy_statistics(self, strategy_id, result_parameters):
        query = f"INSERT INTO strategy_statistics (strategy_id, total_volatility, " \
            f"volatility_ann, baseline_total_volatility, baseline_volatility_ann, sharpe_ratio, " \
            f"baseline_sharpe_ratio, sortino_ratio, baseline_sortino_ratio, ibov_pearson_corr, " \
            f"ibov_spearman_corr, baseline_pearson_corr, " \
            f"baseline_spearman_corr, profit, max_used_capital, avg_used_capital, total_yield, " \
            f"total_yield_ann, total_ibov_yield, total_ibov_yield_ann, total_baseline_yield, " \
            f"total_baseline_yield_ann)\nVALUES\n"

        query += f"  ({strategy_id}, {result_parameters['total_volatility']}, " \
            f"{result_parameters['volatility_ann']}, {result_parameters['baseline_total_volatility']}, " \
            f"{result_parameters['baseline_volatility_ann']}, {result_parameters['sharpe_ratio']}, " \
            f"{result_parameters['baseline_sharpe_ratio']}, {result_parameters['sortino_ratio']}, " \
            f"{result_parameters['baseline_sortino_ratio']}, " \
            f"{result_parameters['ibov_pearson_corr']}, {result_parameters['ibov_spearman_corr']}, " \
            f"{result_parameters['baseline_pearson_corr']}, {result_parameters['baseline_spearman_corr']}, " \
            f"{result_parameters['profit']}, {result_parameters['max_used_capital']}, " \
            f"{result_parameters['avg_used_capital']}, {result_parameters['total_yield']}, " \
            f"{result_parameters['total_yield_ann']}, {result_parameters['total_ibov_yield']}, " \
            f"{result_parameters['total_ibov_yield_ann']}, {result_parameters['total_baseline_yield']}, " \
            f"{result_parameters['total_baseline_yield_ann']});"

        self._insert_update(query)

    def _insert_strategy_performance(self, strategy_id, performance_dataframe):
        query = f"INSERT INTO strategy_performance (strategy_id, day, capital, " \
            f"capital_in_use, active_operations, baseline, ibov)\nVALUES\n"

        number_of_rows = len(performance_dataframe)

        for n, (_, row) in enumerate(performance_dataframe.iterrows()):
            query += f"  ({strategy_id}, \'{row['day'].to_pydatetime().strftime('%Y-%m-%d')}\', " \
                f"{row['capital']}, {row['capital_in_use']}, {row['active_operations']}, " \
                f"{row['baseline']}, {row['ibov']})"

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
            # logger.debug(f'Database \'{DB_NAME}\' connected successfully.')
        except Exception as error:
            logger.error(f'Database \'{DB_NAME}\' connection failed.')
            # sys.exit(c.DB_CONNECTION_ERR)
            raise error

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
            # sys.exit(c.QUERY_ERR)
            raise error

        return self._cursor.fetchall()

    def get_strategy_ids(self, strategy_id=None):
        query = f"SELECT id, name, alias, comment, total_capital, risk_capital_product, " \
            f"min_order_volume, partial_sale, ema_tolerance, min_risk, max_risk, " \
            f"purchase_margin, stop_margin, stop_type, min_days_after_successful_operation, " \
            f"min_days_after_failure_operation, gain_loss_ratio, max_days_per_operation, " \
            f"enable_frequency_normalization, enable_profit_compensation, enable_crisis_halt, " \
            f"enable_downtrend_halt, enable_dynamic_rcc, dynamic_rcc_reference, dynamic_rcc_k, " \
            f"operation_risk\n"
        query += f"FROM strategy\n"

        if strategy_id != None:
            query += f"WHERE id = {strategy_id}\n"

        query += f"ORDER BY id DESC;"

        df = pd.read_sql_query(query, self._connection)

        return df

    def get_strategy_performance(self, strategy_id):
        query = f"SELECT sp.day, sp.capital, sp.capital_in_use, " \
            f"sp.baseline, sp.ibov\n"
        query += f"FROM strategy_performance sp\n"
        query += f"INNER JOIN strategy s ON s.id = sp.strategy_id\n"
        query += f"WHERE s.id = {strategy_id}\n"
        query += f"ORDER BY sp.day ASC;"

        df = pd.read_sql_query(query, self._connection)

        return df

    def get_benchmark_performance(self, strategy_id):
        query = f"SELECT sp.day, sp.capital\n"
        query += f"FROM strategy_performance sp\n"
        query += f"INNER JOIN strategy s ON s.id = sp.strategy_id\n"
        query += f"WHERE s.id = {strategy_id}\n"
        query += f"ORDER BY sp.day ASC;"

        df = pd.read_sql_query(query, self._connection)

        return df

    def get_strategy_active_operations(self, strategy_id):
        query = f"SELECT sp.active_operations\n"
        query += f"FROM strategy_performance sp\n"
        query += f"INNER JOIN strategy s ON s.id = sp.strategy_id\n"
        query += f"WHERE s.id = {strategy_id}\n"
        query += f"ORDER BY sp.day ASC;"

        df = pd.read_sql_query(query, self._connection)

        return df

    def get_strategy_statistics(self, strategy_id):
        query = f"SELECT ss.total_volatility, ss.volatility_ann, ss.baseline_total_volatility, " \
            f"ss.baseline_volatility_ann, ss.sharpe_ratio, ss.baseline_sharpe_ratio, ss.sortino_ratio, " \
            f"ss.baseline_sortino_ratio, ss.ibov_pearson_corr, " \
            f"ss.ibov_spearman_corr, ss.baseline_pearson_corr, ss.baseline_spearman_corr, " \
            f"ss.profit, ss.max_used_capital, ss.avg_used_capital, ss.total_yield, ss.total_yield_ann, " \
            f"ss.total_ibov_yield, ss.total_ibov_yield_ann, ss.total_baseline_yield, " \
            f"ss.total_baseline_yield_ann\n"
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
                f"dc.close_price, dc.volume, df.ema_17, df.ema_72, df.up_down_trend_status, " \
                f"df.target_buy_price, df.stop_loss, df.peak\n"
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
        query += f"	     WHEN timeout_flag = TRUE THEN 'TIMEOUT'\n"
        query += f"  	 ELSE 'TARGET_SALE'\n"
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

        query = f"SELECT q.status, COUNT(q.status) AS number\n"
        query += f"FROM\n"
        query += f"(SELECT\n"
        query += f"  CASE\n"
        query += f"    WHEN op.state = 'CLOSE' AND stop_flag = FALSE AND partial_sale_flag = FALSE AND timeout_flag = FALSE THEN 'SUCCESS'\n"
        query += f"	WHEN op.state = 'CLOSE' AND partial_sale_flag = TRUE THEN 'PARTIAL_SUCCESS'\n"
        query += f"	WHEN op.state = 'CLOSE' AND stop_flag = TRUE AND timeout_flag = FALSE THEN 'FAILURE'\n"
        query += f"	WHEN op.state = 'CLOSE' AND timeout_flag = TRUE THEN 'TIMEOUT'\n"
        query += f"	WHEN op.state = 'OPEN' THEN 'UNFINISHED'\n"
        query += f"	ELSE 'NEITHER'\n"
        query += f"  END AS status\n"
        query += f"FROM operation op\n"
        query += f"INNER JOIN \n"
        query += f"  (SELECT operation_id, MAX(volume) AS volume, \n"
        query += f"    CASE WHEN SUM(CASE WHEN stop_flag THEN 1 ELSE 0 END) > 0\n"
        query += f"      THEN TRUE ELSE FALSE\n"
        query += f"    END AS stop_flag,\n"
        query += f"    CASE WHEN SUM(CASE WHEN partial_sale_flag THEN 1 ELSE 0 END) > 0\n"
        query += f"      THEN TRUE ELSE FALSE\n"
        query += f"    END AS partial_sale_flag,\n"
        query += f"    CASE WHEN SUM(CASE WHEN timeout_flag THEN 1 ELSE 0 END) > 0\n"
        query += f"      THEN TRUE ELSE FALSE\n"
        query += f"    END AS timeout_flag\n"
        query += f"  FROM negotiation\n"
        query += f"  GROUP BY operation_id) neg ON neg.operation_id = op.id\n"
        query += f"INNER JOIN strategy s ON s.id = op.strategy_id\n"
        query += f"WHERE\n"
        query += f"  s.id = {strategy_id}\n"
        query += f"  AND neg.volume != 0) q\n"
        query += f"GROUP BY q.status\n"
        query += f"ORDER BY q.status;\n"


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