from pathlib import Path
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import random
from abc import ABC, abstractmethod
from enum import Enum
import sys

import constants as c
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

class State(Enum):
    NOT_STARTED = "NOT_STARTED"
    OPEN = "OPEN"
    CLOSE = "CLOSE"

class Operation:

    def __init__(self, ticker, strategy_name, strategy_id):
        self._ticker = ticker
        self._strategy_name = strategy_name
        self._strategy_id = strategy_id
        self._start = None
        self._end = None
        self._state = State.NOT_STARTED
        self._target_purchase_price = []
        self._purchase_price = []
        self._purchase_volume = []
        self._purchase_datetime = []
        self._target_sale_price = []
        self._sale_price = []
        self._sale_volume = []
        self._sale_datetime = []
        self._stop_flag = []

    # General properties
    @property
    def ticker(self):
        return self._ticker

    @property
    def strategy_name(self):
        return self._strategy_name

    @property
    def strategy_id(self):
        return self._strategy_id

    @property
    def state(self):
        return self._state

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end

    # Purchase properties
    @property
    def target_purchase_price(self):
        return self._target_purchase_price

    @property
    def purchase_price(self):
        return self._purchase_price

    @property
    def purchase_volume(self):
        return self._purchase_volume

    @property
    def purchase_datetime(self):
        return self._purchase_datetime

    @property
    def purchase_capital(self):
        capital = 0.0
        for (purchase, volume) in zip(self._purchase_price, self._purchase_volume):
            capital = capital + purchase * volume

        return capital

    # Sale properties
    @property
    def target_sale_price(self):
        return self._target_sale_price

    @property
    def sale_price(self):
        return self._sale_price

    @property
    def sale_volume(self):
        return self._sale_volume

    @property
    def sale_datetime(self):
        return self._sale_datetime

    @property
    def stop_flag(self):
        return self._stop_flag

    @property
    def sale_capital(self):
        capital = 0.0
        for (sale, volume) in zip(self._sale_price, self._sale_volume):
            capital = capital + sale * volume

        return capital

    def add_purchase(self, target_purchase_price, purchase_price, purchase_volume, purchase_datetime):

        if self.state != State.CLOSE:
            self._target_purchase_price.append(target_purchase_price)
            self._purchase_price.append(purchase_price)
            self._purchase_volume.append(purchase_volume)
            self._purchase_datetime.append(purchase_datetime)

            if self.state == State.NOT_STARTED:
                self._start = purchase_datetime
                self._state = State.OPEN

            return True

        return False

    def add_sale(self, target_sale_price, sale_price, sale_volume, sale_datetime, stop_flag=False):

        if self.state == State.OPEN and self.purchase_capital >= sale_price * sale_volume + self.sale_capital:
            self._target_sale_price.append(target_sale_price)
            self._sale_price.append(sale_price)
            self._sale_volume.append(sale_volume)
            self._sale_datetime.append(sale_datetime)
            self._stop_flag.append(stop_flag)

            if self.sale_capital == self.purchase_capital:
                self._state = State.CLOSE
                self._end = sale_datetime

            return True
        return False

class Strategy(ABC):

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def total_capital(self):
        pass

    @property
    @abstractmethod
    def max_capital_per_operation(self):
        pass

    @property
    @abstractmethod
    def operations(self):
        pass

    @abstractmethod
    def set_input_data(self, dataframe, interval):
        pass

    @abstractmethod
    def process_operations(self):
        pass

class AndreMoraesStrategy(Strategy):

    def __init__(self, name, tickers, initial_dates, final_dates, total_capital=100000, max_capital_per_operation = 0.10):

        if max_capital_per_operation < 0.0 or max_capital_per_operation > 1.0:
            logger.error(f"""Parameter \'max_capital_per_operation\' must be in the interval [0, 1].""")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        self._name = name
        self._tickers = [ticker.upper() for ticker in tickers]
        self._initial_dates = initial_dates
        self._final_dates = final_dates
        self._total_capital = total_capital
        self._max_capital_per_operation = max_capital_per_operation
        random.seed()
        self._id = random.randint(0, 9223372036854775807) # Max 8 bytes integer
        self._operations = []

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def total_capital(self):
        return self.total_capital

    @total_capital.setter
    def total_capital(self, total_capital):
        self._total_capital = total_capital

    @property
    def max_capital_per_operation(self):
        return self._max_capital_per_operation

    @max_capital_per_operation.setter
    def max_capital_per_operation(self, max_capital_per_operation):
        self._max_capital_per_operation = max_capital_per_operation

    @property
    def operations(self):
        return self._operations

    def set_input_data(self, dataframe, interval):

        if not (interval in ['1wk', '1d']):
            logger.error(f'Error argument \'interval\'=\'{interval}\' is not valid.')
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if interval == '1wk':
            self._week_df = dataframe
        elif interval == '1d':
            self._day_df = dataframe

    def process_operations(self):
        pass


