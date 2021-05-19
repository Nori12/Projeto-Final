import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import yfinance as yf
from datetime import date, timedelta, datetime
import pandas as pd
import sys

import constants as c
from db_model import DBModel

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=10)
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

    def update_daily_candles(self):

        yf_ticker = self._ticker + '.SA'
        today = date.today()
        new_ticker = True

        date_range = self._db_model.get_date_range(self._ticker)
        initial_date_daily_candles = 0
        final_date_daily_candles = 0

        if (date_range is not None and len(date_range) != 0):
            initial_date_daily_candles = date_range[0][2]
            final_date_daily_candles = date_range[0][3]
            new_ticker = False

        # Importante!!!
        # Rever essa lógica porque vou tacar tudo e foda-se mesmo. Esse if fica mais pra frente caso atualize os dados não mais trazidos
        if new_ticker == False:
            new_candles = DBManager._get_data(yf_ticker, final_date_daily_candles+timedelta(days=1), today)

            if new_candles.empty:
                logger.error(f"""yfinance has no data to ticker \'{yf_ticker}\' (\'{(final_date_daily_candles+timedelta(days=1)).strftime('%Y-%m-%d')}\', \'{today.strftime('%Y-%m-%d')}\').""")
                sys.exit(c.YFINANCE_ERR)

            # self._db_model.insert_daily_candles(self._ticker, new_candles)
        elif new_ticker == True:
            new_candles = DBManager._get_data(yf_ticker, self._initial_date, today)

            if new_candles.empty:
                logger.error(f"""yfinance has no data to ticker \'{yf_ticker}\' (\'{self._initial_date.strftime('%Y-%m-%d')}\', \'{today.strftime('%Y-%m-%d')}\').""")
                sys.exit(c.YFINANCE_ERR)

        new_splits = DBManager._verify_splits(new_candles)
        if not new_candles.empty:
            self._db_model.upsert_splits(self._ticker, new_splits)
        else:
            pass # Log message here

        # if (new_candles is not None and len(new_candles) > 0):
        #     self._db_model.insert_daily_candles(self._ticker, new_candles)
        #     new_splits = DBManager._verify_splits(new_candles)
        #     DBManager._update_splits(new_splits)
        # else:
        #     logger.error(f"""yfinance has no data to ticker \'{self._ticker}\' (\'{self._initial_date.strftime('%Y-%m-%d')}\', \'{self._final_date.strftime('%Y-%m-%d')}\').""")
        #     sys.exit(c.YFINANCE_ERR)

        # logger.info(f"""Ticker \'{self._ticker}\' updated successfully.""")

    @staticmethod
    def _get_data(ticker, start, end):

        try:
            msft = yf.Ticker(ticker)
            hist = msft.history(start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d'))
        except Exception as error:
            logger.error('Error getting yfinance data, error: {}'.format(error))
            sys.exit(c.YFINANCE_ERR)

        return hist

    @staticmethod
    def _verify_splits(data):
        return data[data['Stock Splits'] != 0]

    @staticmethod
    def _update_splits(data):
        # data[data['Stock Splits'] != 0]
        print(data.loc[data['Stock Splits'] != 0, ['Stock Splits']])
        # self._dbmodel.upsert_splits(data.loc[data['Stock Splits'] != 0, ['Stock Splits']])
