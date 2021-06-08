from pathlib import Path
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from datetime import timedelta
import random
from abc import ABC, abstractmethod
from enum import Enum
import sys
import numpy as np
import math

import constants as c
from utils import calculate_maximum_volume, State
from db_model import DBStrategyModel

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

    def __init__(self, ticker):
        self._ticker = ticker
        self._start_date = None
        self._end_date = None
        self._state = State.NOT_STARTED
        self._number_of_orders = 0
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

    # General properties
    @property
    def ticker(self):
        return self._ticker

    @property
    def state(self):
        return self._state

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date

    @property
    def number_of_orders(self):
        return self._number_of_orders

    @property
    def result_profit(self):
        return self._result_profit

    @property
    def result_yield(self):
        return self._result_yield

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
    def partial_sale_flag(self):
        return self._partial_sale_flag

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
            self._number_of_orders += 1

            if self.state == State.NOT_STARTED:
                self._start_date = purchase_datetime
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
            self._number_of_orders += 1

            if self.total_purchase_volume == self.total_sale_volume:
                self._partial_sale_flag.append(False)
                self._end_date = sale_datetime
                self._result_profit = self.total_sale_capital - self.total_purchase_capital
                self._result_yield = self._result_profit / self.total_purchase_capital
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
    def alias(self):
        pass

    @alias.setter
    @abstractmethod
    def alias(self, alias):
        pass

    @property
    @abstractmethod
    def comment(self):
        pass

    @comment.setter
    @abstractmethod
    def comment(self, comment):
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
    def operations(self):
        pass

    @abstractmethod
    def set_input_data(self, dataframe, interval):
        pass

    @abstractmethod
    def process_operations(self):
        pass

    @abstractmethod
    def save(self):
        pass

class AndreMoraesStrategy(Strategy):

    def __init__(self, tickers, initial_dates, final_dates, alias=None, comment=None, total_capital=100000, risk_capital_product=0.0036):

        if risk_capital_product < 0.0 or risk_capital_product > 1.0:
            logger.error(f"""Parameter \'risk_capital_product\' must be in the interval [0, 1].""")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        self._name = "Andre Moraes"
        self._alias = alias
        self._comment = comment
        self._tickers = [ticker.upper() for ticker in tickers]
        self._initial_dates = initial_dates
        self._final_dates = final_dates
        self._total_capital = total_capital
        self._current_capital = total_capital
        self._risk_capital_product = risk_capital_product
        self._operations = []

        self._db_strategy_model = DBStrategyModel(self._name, self._tickers, self._initial_dates, self._final_dates, self._total_capital, alias=self._alias, comment=self._comment, risk_capital_product=self._risk_capital_product)

    @property
    def name(self):
        return self._name

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, alias):
        self._alias = alias
        self._db_strategy_model.alias = alias

    @property
    def comment(self):
        return self._comment

    @comment.setter
    def comment(self, comment):
        self._comment = comment
        self._db_strategy_model.comment = comment

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

    @property
    def current_capital(self):

        allocated_capital = 0.0

        for operation in self._operations:
            allocated_capital = allocated_capital - operation.total_purchase_capital + operation.total_sale_capital

        return round(allocated_capital, 2)

    @property
    def risk_capital_product(self):
        return self._risk_capital_product

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

    def _get_capital_per_risk(self, risk):
        return round(self._risk_capital_product * self._total_capital / risk, 2)

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

                                operation = Operation(ticker)
                                operation.set_purchase_target(purchase_target)
                                operation.set_stop_loss(stop_loss)
                                operation.set_sale_target(round(purchase_target + (purchase_target - stop_loss) * 3, 2))
                                operation.set_partial_sale(round(purchase_target + (purchase_target - stop_loss), 2))

                                ongoing_operation_flag = True

                if ongoing_operation_flag == True:

                    if operation.state == State.NOT_STARTED:

                        # Check if the target purchase price was hit
                        if operation.target_purchase_price >= row['min_price'] and operation.target_purchase_price <= row['max_price']:

                            operation.add_purchase(operation.target_purchase_price, calculate_maximum_volume(operation.target_purchase_price, self._get_capital_per_risk((operation.target_purchase_price - operation.stop_loss)/(operation.target_purchase_price)), minimum_volume=1), row['day'])

                    elif operation.state == State.OPEN:

                        # Check if the target STOP LOSS is hit
                        if operation.stop_loss >= row['min_price'] and operation.stop_loss <= row['max_price']:
                            operation.add_sale(operation.stop_loss, operation.total_purchase_volume - operation.total_sale_volume, row['day'], stop_flag=True)

                        # Check if the target STOP LOSS is skipped
                        if operation.stop_loss > row['max_price']:
                            operation.add_sale(row['min_price'], operation.total_purchase_volume - operation.total_sale_volume, row['day'], stop_flag=True)

                        # After hitting the stol loss, the operation can be closed
                        if operation.state == State.OPEN:

                            # Check if the PARTIAL SALE price is hit
                            if partial_sale_flag == False and operation.partial_sale_price >= row['min_price'] and operation.partial_sale_price <= row['max_price']:
                                operation.add_sale(operation.partial_sale_price, math.ceil(operation.purchase_volume[0] / 2), row['day'])
                                partial_sale_flag = True

                            # Check if the PARTIAL SALE price is skipped but not TARGET SALE
                            if partial_sale_flag == False and operation.partial_sale_price < row['min_price'] and operation.target_sale_price > row['max_price']:
                                operation.add_sale(row['min_price'], math.ceil(operation.purchase_volume[0] / 2), row['day'])
                                partial_sale_flag = True

                            # Check if the TARGET SALE price is hit
                            if operation.target_sale_price >= row['min_price'] and operation.target_sale_price <= row['max_price']:
                                operation.add_sale(operation.target_sale_price, operation.total_purchase_volume - operation.total_sale_volume, row['day'])

                            # Check if the TARGET SALE price is skipped
                            if operation.target_sale_price < row['min_price']:
                                operation.add_sale(row['min_price'], operation.total_purchase_volume - operation.total_sale_volume, row['day'])

                    if operation.state == State.CLOSE:
                        self.operations.append(operation)
                        operation = None
                        ongoing_operation_flag = False
                        partial_sale_flag = False

            ongoing_operation_flag = False
            partial_sale_flag = False
            if operation is not None and operation.state == State.OPEN:
                self.operations.append(operation)

    def save(self):
        self._db_strategy_model.insert_strategy_results(self.operations)