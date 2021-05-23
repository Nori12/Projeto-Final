import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import yfinance as yf
from datetime import date, timedelta, datetime
import pandas as pd
import sys
import numpy as np

import constants as c
from utils import compare_dates
from db_model import DBModel

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

class DBManager:

    def __init__(self, ticker, initial_date, final_date):
        self._ticker = ticker.upper()
        self._initial_date = initial_date
        self._final_date = final_date

        self._db_model = DBModel()

    def update_candles(self, interval = '1d'):

        if not (interval in ['1d', '1h']):
            logger.error(f'Error argument \'interval\'=\'{interval}\' is not valid.')
            sys.exit(c.INVALID_ARGUMENT_ERR)

        last_update_candles = None
        initial_date_candles = None
        final_date_candles = None

        try:
            date_range = self._db_model.get_date_range(self._ticker)

            "Get holidays and convert it to list(str)"
            holidays = self._db_model.get_holidays(self._initial_date, self._final_date)
            if len(holidays) == 0:
                holidays = ['2200-01-01']
            else:
                holidays = [holiday[0].strftime('%Y-%m-%d') for holiday in holidays]

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

                if (compare_dates(self._initial_date, initial_date_candles, holidays) <= 0 and compare_dates(self._final_date, final_date_candles, holidays) >= 0):
                    logger.info(f"""Ticker \'{self._ticker}\' already updated.""")
                    return True

                # Database lacks most recent data
                if compare_dates(self._final_date, final_date_candles, holidays) < 0:
                    self._update_candles_splits_dividends(self._ticker, final_date_candles+timedelta(days=1), self._final_date, split_nomalization_date=initial_date_candles, last_update_date=last_update_candles, interval=interval)

                # Database lacks older data
                if compare_dates(self._initial_date, initial_date_candles, holidays) > 0:
                    self._update_candles_splits_dividends(self._ticker, self._initial_date, initial_date_candles, interval=interval)
            else:
                self._update_candles_splits_dividends(self._ticker, self._initial_date, self._final_date, interval=interval)

            logger.info(f"""Ticker \'{self._ticker}\' {'daily'if interval=='1d' else 'hourly'} candles update finished.""")

        except Exception as error:
            logger.error(f"""Error updating {'daily 'if interval=='1d' else 'hourly'} candles, error: {error}""")
            sys.exit(c.UPDATING_DB_ERR)

        return True

    @staticmethod
    def _get_data(ticker, start, end, interval='1d'):

        ticker = ticker + '.SA'
        try:
            msft = yf.Ticker(ticker)
            if interval == '1d':
                hist = msft.history(start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d'))
            elif interval == '1h':
                hist = msft.history(start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d'), interval='1h')
            else:
                hist = None
        except Exception as error:
            logger.error('Error getting yfinance data, error: {}'.format(error))
            sys.exit(c.YFINANCE_ERR)

        return hist

    @staticmethod
    def _verify_splits(data, last_update_threshold=datetime(2200, 1, 1)):

        cumulative_splits = 1.0
        data2 = data[data['Stock Splits'] != 0].copy()

        for index, row in data2.iterrows():
            if index.date() >= last_update_threshold.date():
                cumulative_splits = cumulative_splits * row['Stock Splits']

        return data2, cumulative_splits

    @staticmethod
    def _verify_dividends(data):
        return data[data['Dividends'] != 0].copy()

    def _update_candles_splits_dividends(self, ticker, start_datetime, end_datetime, split_nomalization_date=datetime(2200, 1, 1), last_update_date=datetime(2200, 1, 1), interval='1d'):

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

        new_candles = DBManager._get_data(ticker, start_datetime, end_datetime, interval)

        if new_candles.empty:
            logger.warning(f"""yfinance has no data for ticker \'{ticker}\' (\'{start_datetime.strftime('%Y-%m-%d')}\', \'{end_datetime.strftime('%Y-%m-%d')}\').""")
            return False

        if update_flag == True:
            new_splits, cumulative_splits = DBManager._verify_splits(new_candles, last_update_threshold=last_update_date)
        else:
            new_splits, cumulative_splits = DBManager._verify_splits(new_candles)

        new_dividends = DBManager._verify_dividends(new_candles)

        new_candles.dropna(axis='index', how='any', inplace=True)
        if interval == '1d':
            self._db_model.insert_daily_candles(self._ticker, new_candles)
        elif interval == '1h':
            self._db_model.insert_hourly_candles(self._ticker, new_candles)

        if not new_splits.empty:
            self._db_model.upsert_splits(self._ticker, new_splits)

        if update_flag == True and cumulative_splits != 1.0:
            self._db_model.update_candles_with_split(self._ticker, split_nomalization_date, start_datetime, cumulative_splits, interval=interval)

        if not new_dividends.empty:
            self._db_model.upsert_dividends(self._ticker, new_dividends)

        return True
