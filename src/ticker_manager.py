from pathlib import Path
import pandas as pd
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import sys
import re

import constants as c
from utils import RunTime
from db_manager import DBManager

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

    def __init__(self, ticker, initial_date, final_date):
        self._ticker = ticker.upper()
        self._initial_date = initial_date
        self._final_date = final_date
        self._db_manager = DBManager(self._ticker, self._initial_date, self._final_date)

    @property
    def ticker(self):
        """Return the ticker name."""
        return self._ticker

    @ticker.setter
    def ticker(self, ticker):
        self._ticker = ticker

    @property
    def initial_date(self):
        """Return the start date of the time interval."""
        return self._initial_date

    @initial_date.setter
    def initial_date(self, initial_date):
        self._initial_date = initial_date

    @property
    def final_date(self):
        """Return the end date of the time interval."""
        return self._final_date

    @final_date.setter
    def final_date(self, final_date):
        self._final_date = final_date

    def update_interval(self):

        # Important: update interval '1d' is the most reliable,
        # so update it first because '1h' depends on it for some adjustments
        self._db_manager.update_candles(interval='1d')
        self._db_manager.update_candles(interval='1h')
        self._db_manager.create_missing_daily_candles_from_hourly()
        self._db_manager.update_weekly_candles()
