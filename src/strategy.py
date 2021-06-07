from pathlib import Path
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import random
from abc import ABC, abstractmethod
from enum import Enum
import sys
import numpy as np

import constants as c
from utils import calculate_maximum_volume, calculate_annualized_yield
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

# TODO: Add holidays in self._result_yield_annualized calculation
class Operation:

    def __init__(self, ticker, strategy_name, strategy_id):
        self._ticker = ticker
        self._strategy_name = strategy_name
        self._strategy_id = strategy_id
        self._start = None
        self._end = None
        self._state = State.NOT_STARTED
        self._target_purchase_price = None
        self._purchase_price = []
        self._purchase_volume = []
        self._purchase_datetime = []
        self._target_sale_price = None
        self._sale_price = []
        self._sale_volume = []
        self._sale_datetime = []
        self._stop_flag = []
        self._partial_sale_flag = []
        self._stop_loss = None
        self._partial_sale_price = None
        self._result_profit = None
        self._result_yield = None
        self._result_yield_annualized = None

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

    @target_purchase_price.setter
    def target_purchase_price(self, target_purchase_price):
        self._target_purchase_price = target_purchase_price

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
    def total_purchase_capital(self):
        capital = 0.0
        for (purchase, volume) in zip(self._purchase_price, self._purchase_volume):
            capital = capital + purchase * volume

        return round(capital, 2)

    @property
    def total_purchase_volume(self):
        total_volume = 0.0
        for volume in self._purchase_volume:
            total_volume = total_volume + volume

        return total_volume

    # Sale properties
    @property
    def target_sale_price(self):
        return self._target_sale_price

    @target_sale_price.setter
    def target_sale_price(self, target_sale_price):
        self._target_sale_price = target_sale_price

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
    def stop_loss(self):
        return self._stop_loss

    @stop_loss.setter
    def stop_loss(self, stop_loss):
        self._stop_loss = stop_loss

    @property
    def partial_sale_price(self):
        return self._partial_sale_price

    @partial_sale_price.setter
    def partial_sale_price(self, partial_sale_price):
        self._partial_sale_price = partial_sale_price

    @property
    def total_sale_capital(self):
        capital = 0.0
        for (sale, volume) in zip(self._sale_price, self._sale_volume):
            capital = capital + sale * volume

        return round(capital, 2)

    @property
    def total_sale_volume(self):
        total_volume = 0.0
        for volume in self._sale_volume:
            total_volume = total_volume + volume

        return total_volume

    def add_purchase(self, purchase_price, purchase_volume, purchase_datetime):

        if self.state != State.CLOSE:
            self._purchase_price.append(purchase_price)
            self._purchase_volume.append(purchase_volume)
            self._purchase_datetime.append(purchase_datetime)

            if self.state == State.NOT_STARTED:
                self._start = purchase_datetime
                self._state = State.OPEN

            return True
        return False

    def set_purchase_target(self, target_purchase_price):

        if self.state != State.CLOSE:
            self._target_purchase_price= target_purchase_price

            return True
        return False

    def add_sale(self, sale_price, sale_volume, sale_datetime, stop_flag=False):

        if self.state == State.OPEN and self.total_purchase_volume >= self.total_sale_volume + sale_volume:
            self._sale_price.append(sale_price)
            self._sale_volume.append(sale_volume)
            self._sale_datetime.append(sale_datetime)
            self._stop_flag.append(stop_flag)

            if self.total_purchase_volume == self.total_sale_volume:
                self._partial_sale_flag.append(False)
                self._end = sale_datetime
                self._result_profit = self.total_sale_capital - self.total_purchase_capital
                self._result_yield = self._result_profit / self.total_purchase_capital
                self._result_yield_annualized = calculate_annualized_yield(self._result_yield, np.busday_count(self._start.date(), (self._end+timedelta(days=1)).date() ))
                self._state = State.CLOSE
            else:
                self._partial_sale_flag.append(True)

            return True
        return False

    def set_stop_loss(self, stop_loss):
        if self.state != State.CLOSE:
            self._stop_loss = stop_loss

            return True
        return False

    def set_sale_target(self, target_sale_price):
        if self.state != State.CLOSE:
            self._target_sale_price = target_sale_price

            return True
        return False

    def set_partial_sale(self, partial_sale_price):
        if self.state != State.CLOSE:
            self._partial_sale_price = partial_sale_price

            return True
        return False

class Strategy(ABC):

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def id(self):
        pass

    @property
    @abstractmethod
    def tickers(self):
        pass

    @property
    @abstractmethod
    def initial_dates(self):
        pass

    @property
    @abstractmethod
    def final_dates(self):
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
        random.seed()
        self._id = random.randint(0, 9223372036854775807) # Max 8 bytes integer
        self._tickers = [ticker.upper() for ticker in tickers]
        self._initial_dates = initial_dates
        self._final_dates = final_dates
        self._total_capital = total_capital
        self._max_capital_per_operation = max_capital_per_operation
        self._operations = []

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def id(self):
        return self._id

    @property
    def tickers(self):
        return self._tickers

    @property
    def initial_dates(self):
        return self._initial_dates

    @property
    def final_dates(self):
        return self._final_dates

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

    # TODO: correct potentially problem of stop loss and target sale price being hit on the same day
    def process_operations(self):

        ongoing_operation_flag = False
        operation = None
        partial_sale_flag = False

        for _, ticker in enumerate(self._tickers):
            for _, row in self._day_df.loc[self._day_df['ticker'] == ticker].iterrows():

                if (ongoing_operation_flag == False) or (operation is not None and operation.state == State.NOT_STARTED):
                    # Check price tendency in Main Graph Time
                    if row['up_down_trend_status'] == 1:

                        # Check if price is next to one EMA in Main Graph Time
                        if row['close_price'] < max(row['ema_17'], row['ema_72']) * 1.01 and row['close_price'] > min(row['ema_17'], row['ema_72']) * 0.99:

                            maj_graph_time_ema_72 = self._week_df.loc[(self._week_df['ticker'] == ticker) & (self._week_df['week'] <= row['day'])].tail(2)['ema_72'].head(1).values[0]

                            maj_graph_time_trend = self._week_df.loc[(self._week_df['ticker'] == ticker) & (self._week_df['week'] <= row['day'])].tail(2)['up_down_trend_status'].head(1).values[0]

                            # Check if price is greater than EMA_72 in Major Graph Time
                            if row['close_price'] > maj_graph_time_ema_72 and maj_graph_time_trend == 1:

                                # Identify last maximum peak
                                if not self._day_df.loc[(self._day_df['ticker'] == ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == 1) & (self._day_df['max_price'] > row['close_price'])].empty:
                                    purchase_target = self._day_df.loc[(self._day_df['ticker'] == ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == 1) & (self._day_df['max_price'] > row['close_price'])].tail(1)['max_price'].values[0]
                                    purchase_target = round(purchase_target, 2)
                                # If no peak has a maximum greater enough, choose the first past maximum
                                else:
                                    purchase_target = self._day_df.loc[(self._day_df['ticker'] == ticker) & (self._day_df['day'] < row['day']) & (self._day_df['max_price'] > row['close_price'])].tail(1)['max_price'].values[0]
                                    purchase_target = round(purchase_target, 2)

                                stop_loss = self._day_df.loc[(self._day_df['ticker'] == ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == -1) & (self._day_df['min_price'] < row['close_price'])].tail(1)['min_price'].values[0]

                                operation = Operation(ticker, self._name, self._id)
                                operation.set_purchase_target(purchase_target)
                                operation.set_stop_loss(stop_loss)
                                operation.set_sale_target(round(purchase_target + (purchase_target - stop_loss) * 3, 2))
                                operation.set_partial_sale(round(purchase_target + (purchase_target - stop_loss), 2))

                                ongoing_operation_flag = True

                if ongoing_operation_flag == True:

                    if operation.state == State.NOT_STARTED:

                        # Check if the target purchase price was hit
                        if operation.target_purchase_price >= row['min_price'] and operation.target_purchase_price <= row['max_price']:

                            operation.add_purchase(operation.target_purchase_price, calculate_maximum_volume(operation.target_purchase_price, self._max_capital_per_operation * self._total_capital), row['day'])

                            operation.target_purchase_price = None

                    elif operation.state == State.OPEN:

                        # Check if te partial sale price was hit
                        if partial_sale_flag == False and operation.partial_sale_price >= row['min_price']:
                            operation.add_sale(operation.partial_sale_price, operation.purchase_volume[0]/2, row['day'])
                            partial_sale_flag = True

                        # Check if the target sale price was hit
                        if operation.target_sale_price >= row['min_price']:
                            operation.add_sale(operation.target_sale_price, operation.purchase_volume[0]/2, row['day'])

                            operation.target_sale_price = None

                        # Check if the target stop loss was hit
                        if operation.stop_loss >= row['min_price'] and operation.target_sale_price <= row['max_price']:
                            operation.add_sale(operation.stop_loss, operation.purchase_volume[0], row['day'])

                            operation.stop_loss = None

                    if operation.state == State.CLOSE:
                        self.operations.append(operation)
                        operation = None
                        ongoing_operation_flag = False
                        partial_sale_flag = False

            ongoing_operation_flag = False
            partial_sale_flag = False
            if operation is not None and operation.state == State.OPEN:
                self.operations.append(operation)
        print(1)
