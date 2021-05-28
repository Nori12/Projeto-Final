from pathlib import Path
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from random import random

import constants as c
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

class Operation:

    def __init__(self):
        pass

class Strategy:

    def __init__(self, name, tickers, initial_dates, final_dates, allocated_amount=100000, max_capital_per_operation = 0.10):
        self.name = name
        self._tickers = [ticker.upper() for ticker in tickers]
        self._initial_dates = initial_dates
        self._final_dates = final_dates
        self.allocated_amount = allocated_amount
        self.max_capital_per_operation = max_capital_per_operation
        random.seed(datetime.now())
        self.id = random.randbytes(8)
        self.operations = []

    @property
    def name(self):
        return self.name

    @name.setter
    def name(self, name):
        self.name = name

    @property
    def allocated_amount(self):
        return self.allocated_amount

    @allocated_amount.setter
    def allocated_amount(self, allocated_amount):
        self.allocated_amount = allocated_amount

    @property
    def max_capital_per_operation(self):
        return self.max_capital_per_operation

    @max_capital_per_operation.setter
    def max_capital_per_operation(self, max_capital_per_operation):
        self.max_capital_per_operation = max_capital_per_operation

    @property
    def operations(self):
        return self.operations

    def set_input_date(self, dataframe, interval):
        pass

    def process_operations(self):
        pass


