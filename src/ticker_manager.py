from pathlib import Path
# import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import sys
# import re
import yfinance as yf

import constants as c
from utils import compare_dates
from db_model import DBTickerModel

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

class TickerManager:
    """The class that implements all necessary pre-processing.

    Attributes:
        ticker (str): Ticker name.
        initial_date (datetime): Start date of the time interval.
        final_date (datetime): End date of the time interval.
        input_files_path (Path, optional): Path to input files folder.
        output_files_path (Path, optional): Path to output files folder.
    """

    tickers = []
    initial_dates = []
    final_dates = []
    number_of_tickers = 0
    db_ticker_model = DBTickerModel()

    def __init__(self, ticker, initial_date, final_date):
        self._ticker = ticker.upper()
        self._initial_date = initial_date
        self._final_date = final_date
        self._holidays = []

        # TickerManager.db_ticker_model = DBTickerModel()

        TickerManager.number_of_tickers += 1
        TickerManager.tickers.append(self._ticker)
        TickerManager.initial_dates.append(self._initial_date)
        TickerManager.final_dates.append(self._final_date)

    @property
    def holidays(self):
        return self._holidays

    @holidays.setter
    def holidays(self, holidays):
        self._holidays = holidays

    @property
    def ticker(self):
        """Return the ticker name."""
        return self._ticker

    @property
    def initial_date(self):
        """Return the start date of the time interval."""
        return self._initial_date

    @property
    def final_date(self):
        """Return the end date of the time interval."""
        return self._final_date

    def update_interval(self):

        # Important: update interval '1d' is the most reliable,
        # so update it first because '1h' depends on it for some adjustments
        self._update_candles(interval='1d')
        # self._update_candles(interval='1h')
        # self._create_missing_daily_candles_from_hourly()
        self._update_weekly_candles()

    def _update_candles(self, interval = '1d'):

        if not (interval in ['1d', '1h']):
            logger.error(f'Error argument \'interval\'=\'{interval}\' is not valid.')
            sys.exit(c.INVALID_ARGUMENT_ERR)

        last_update_candles = None
        initial_date_candles = None
        final_date_candles = None

        try:
            date_range = TickerManager.db_ticker_model.get_date_range(self._ticker)

            if len(date_range) != 0:
                if interval == '1h':
                    last_update_candles = date_range[0][0]
                    initial_date_candles = date_range[0][1]
                    final_date_candles = date_range[0][2]
                else:
                    last_update_candles = date_range[0][3]
                    initial_date_candles = date_range[0][4]
                    final_date_candles = date_range[0][5]

            if all([last_update_candles, initial_date_candles, final_date_candles]):

                if (compare_dates(self._initial_date, initial_date_candles, self._holidays) <= 0 and compare_dates(self._final_date, final_date_candles, self._holidays) >= 0):
                    logger.info(f"""Ticker \'{self._ticker}\' already updated.""")
                    return True

                # Database lacks most recent data
                if compare_dates(self._final_date, final_date_candles, self._holidays) < 0:
                    self._update_candles_splits_dividends(final_date_candles+timedelta(days=1), self._final_date, split_nomalization_date=initial_date_candles, last_update_date=last_update_candles, interval=interval)

                # Database lacks older data
                if compare_dates(self._initial_date, initial_date_candles, self._holidays) > 0:
                    self._update_candles_splits_dividends(self._initial_date, initial_date_candles, interval=interval)
            else:
                self._update_candles_splits_dividends(self._initial_date, self._final_date, interval=interval)

            logger.info(f"""Ticker \'{self._ticker}\' {'daily' if interval=='1d' else 'hourly'} candles update finished.""")

        except Exception as error:
            logger.error(f"""Error updating {'daily' if interval=='1d' else 'hourly'} candles, error: {error}""")
            sys.exit(c.UPDATING_DB_ERR)

        return True

    def _get_data(self, start, end, interval='1d'):

        ticker = self._ticker + '.SA'
        try:
            msft = yf.Ticker(ticker)
            if interval == '1d':
                hist = msft.history(start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d'), prepost=True, back_adjust=True, rounding=True)
            elif interval == '1h':
                hist = msft.history(start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d'), interval='1h', prepost=True, back_adjust=True, rounding=True)
            else:
                hist = None
        except Exception as error:
            logger.error('Error getting yfinance data, error: {}'.format(error))
            sys.exit(c.YFINANCE_ERR)

        return hist

    def _verify_splits(self, data, last_update_threshold=datetime(2200, 1, 1)):

        cumulative_splits = 1.0
        data2 = data[data['Stock Splits'] != 0].copy()

        for index, row in data2.iterrows():
            if index.date() >= last_update_threshold.date():
                cumulative_splits = cumulative_splits * row['Stock Splits']

        return data2, cumulative_splits

    def _verify_dividends(self, data):
        return data[data['Dividends'] != 0].copy()

    def _update_candles_splits_dividends(self, start_datetime, end_datetime, split_nomalization_date=datetime(2200, 1, 1), last_update_date=datetime(2200, 1, 1), interval='1d'):

        update_flag = False

        if split_nomalization_date != datetime(2200, 1, 1) and split_nomalization_date > start_datetime:
            logger.error(f'Error argument \'split_nomalization_date\'=\'{split_nomalization_date}\' is greater than \'start_datetime\'={start_datetime}.')
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if last_update_date != datetime(2200, 1, 1) and last_update_date < split_nomalization_date:
            logger.error(f'Error argument \'last_update_date\'=\'{last_update_date}\' is less than \'split_nomalization_date\'={split_nomalization_date}.')
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if bool(split_nomalization_date != datetime(2200, 1, 1)) != bool(last_update_date != datetime(2200, 1, 1)):
            logger.error(f'Error arguments \'split_nomalization_date\' and \'last_update_date\' must be provided together.')
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if split_nomalization_date != datetime(2200, 1, 1) and last_update_date != datetime(2200, 1, 1):
            update_flag = True

        new_candles = self._get_data(start_datetime, end_datetime, interval)

        if new_candles.empty:
            logger.warning(f"""yfinance has no data for ticker \'{self._ticker}\' (\'{start_datetime.strftime('%Y-%m-%d')}\', \'{end_datetime.strftime('%Y-%m-%d')}\').""")
            return False

        if update_flag == True:
            new_splits, cumulative_splits = self._verify_splits(new_candles, last_update_threshold=last_update_date)
        else:
            new_splits, cumulative_splits = self._verify_splits(new_candles)

        new_dividends = self._verify_dividends(new_candles)

        new_candles.drop(['Dividends', 'Stock Splits'], axis=1, inplace=True)
        new_candles.replace(0, np.nan, inplace=True)
        new_candles.dropna(axis=0, how='any', inplace=True)

        self._check_dataframe_constraints(new_candles)

        if new_candles.empty:
            logger.warning(f"""No valid data to update for ticker \'{self._ticker}\', (\'{start_datetime.strftime('%Y-%m-%d')}\', \'{end_datetime.strftime('%Y-%m-%d')}\').""")
            return False

        # Insert candles
        if interval == '1d':
            TickerManager.db_ticker_model.insert_daily_candles(self._ticker, new_candles)
        elif interval == '1h':
            TickerManager.db_ticker_model.insert_hourly_candles(self._ticker, new_candles)

        # Insert splits
        if interval == '1d' and not new_splits.empty:
            TickerManager.db_ticker_model.upsert_splits(self._ticker, new_splits)

        # Insert dividends
        if interval == '1d' and not new_dividends.empty:
            TickerManager.db_ticker_model.upsert_dividends(self._ticker, new_dividends)

        # Normalize candles
        if interval == '1d' and update_flag == True and cumulative_splits != 1.0:
            TickerManager.db_ticker_model.update_daily_candles_with_split(self._ticker, split_nomalization_date, start_datetime, cumulative_splits)
        elif interval == '1h':
            # yfinance does not always retrive hourly candles normalized (i.e. split) with respect to the most recent date.
            # In fact the normalization depends on the interval selected, which is a problem.
            TickerManager.db_ticker_model.update_hourly_candles_with_split(self._ticker)

        return True

    def _create_missing_daily_candles_from_hourly(self):

        # yfinance does not retrieve days in which the brazilian stock market opens after lunch (1pm),
        # although they had negotiations. The solution is mount then from hourly candles data.
        # The volume information from hourly candles are inaccurate, but better than nothing.
        TickerManager.db_ticker_model.create_missing_daily_candles_from_hourly(self._ticker)

    def _update_weekly_candles(self):
        TickerManager.db_ticker_model.delete_weekly_candles(self._ticker)
        TickerManager.db_ticker_model.create_weekly_candles_from_daily(self._ticker)

        logger.info(f"""Ticker \'{self._ticker}\' weekly candles update finished.""")

    def _check_dataframe_constraints(self, df):

        df['Low'] = df.apply(lambda x: x['Close'] if x['Low'] > x['Close'] else x['Low'], axis=1)
        df['Low'] = df.apply(lambda x: x['Open'] if x['Low'] > x['Open'] else x['Low'], axis=1)
        df['High'] = df.apply(lambda x: x['Close'] if x['High'] < x['Close'] else x['High'], axis=1)
        df['High'] = df.apply(lambda x: x['Open'] if x['High'] < x['Open'] else x['High'], axis=1)

