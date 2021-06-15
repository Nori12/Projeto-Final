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

from yfinance import ticker

import constants as c
from utils import calculate_maximum_volume, State, calculate_yield_annualized
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
            capital += purchase * volume

        return round(capital, 2)

    @property
    def total_purchase_volume(self):
        total_volume = 0.0
        for volume in self._purchase_volume:
            total_volume += volume

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
            capital += + sale * volume

        return round(capital, 2)

    @property
    def total_sale_volume(self):
        total_volume = 0.0
        for volume in self._sale_volume:
            total_volume += + volume

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

    def __init__(self, tickers, initial_dates, final_dates, alias=None, comment=None, total_capital=100000, risk_reference=0.06, min_volume_per_year=1000000):

        if risk_reference < 0.0 or risk_reference > 1.0:
            logger.error(f"""Parameter \'risk_reference\' must be in the interval [0, 1].""")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        self._name = "Andre Moraes"
        self._alias = alias
        self._comment = comment
        self._tickers = [ticker.upper() for ticker in tickers]
        self._initial_dates = initial_dates
        self._final_dates = final_dates
        self._total_capital = total_capital
        self._risk_capital_product = risk_reference / len(tickers)
        self._min_volume_per_year = min_volume_per_year
        self._operations = []

        self._start_date = None
        self._end_date = None

        self._db_strategy_model = DBStrategyModel(self._name, self._tickers, self._initial_dates, self._final_dates, self._total_capital, alias=self._alias, comment=self._comment, risk_capital_product=self._risk_capital_product, min_volume_per_year=min_volume_per_year)

        self._statistics_graph = None
        self._statistics_parameters = {'profit': None, 'max_used_capital': None, 'yield': None, 'annualized_yield': None, 'ibov_yield': None, 'annualized_ibov_yield': None, 'avr_tickers_yield': None, 'annualized_avr_tickers_yield': None, 'volatility': None, 'sharpe_ratio': None}

        if self._min_volume_per_year != 0:
            self._filter_tickers_per_min_volume()

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
    def available_capital(self):

        allocated_capital = 0.0

        for operation in self._operations:
            allocated_capital += operation.total_sale_capital - operation.total_purchase_capital

        return round(self._total_capital - allocated_capital, 2)

    @property
    def risk_capital_product(self):
        return self._risk_capital_product

    @property
    def min_volume_per_year(self):
        return self._min_volume_per_year

    @property
    def operations(self):
        return self._operations

    @property
    def start_date(self):

        if self._operations:

            start_dates_list = [operation.start_date for operation in self._operations if operation.start_date is not None]

            if any(start_dates_list):
                return min(start_dates_list)

        return None

    @property
    def end_date(self):
        if self._operations:

            end_dates_list = [operation.end_date for operation in self._operations if operation.end_date is not None]

            if any(end_dates_list):
                return max(end_dates_list)

        return None

    def _filter_tickers_per_min_volume(self):

        allowed_tickers_raw = self._db_strategy_model.get_tickers_above_min_volume()

        if len(allowed_tickers_raw) != 0:

            allowed_tickers = [ticker[0] for ticker in allowed_tickers_raw]

            intersection_tickers = list(set(self._tickers).intersection(allowed_tickers))

            if len(intersection_tickers) < len(self._tickers):
                logger.info(f"""\'{self._name}\': Removing tickers which the average volume negotiation per year is less than {self._min_volume_per_year}.""")

                removed_tickers = [ticker for ticker in self._tickers if ticker not in intersection_tickers]

                for rem_ticker in removed_tickers:
                    logger.info(f"""\'{self._name}\': Removed ticker: \'{rem_ticker}\'""")

                new_tickers = [ticker for ticker in self._tickers if ticker in allowed_tickers]
                new_initial_dates = [self._initial_dates[self._tickers.index(ticker)] for ticker in new_tickers]
                new_final_dates = [self._final_dates[self._tickers.index(ticker)] for ticker in new_tickers]

                logger.info(f"""\'{self._name}\': Filtered tickers:""")

                for ticker, initial_date, final_date in zip(new_tickers, new_initial_dates, new_final_dates):
                    logger.info(f"""\'{self._name}\': Ticker: \'{ticker.ljust(6)}\'\tInital date: {initial_date.strftime('%d/%m/%Y')}\t\tFinal date: {final_date.strftime('%d/%m/%Y')}""")

                self._tickers = new_tickers
                self._initial_dates = new_initial_dates
                self._final_dates = new_final_dates

    def set_input_data(self, dataframe, interval):

        if not (interval in ['1wk', '1d']):
            logger.error(f'Error argument \'interval\'=\'{interval}\' is not valid.')
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if interval == '1wk':
            self._week_df = dataframe
            self._week_df.sort_values(['ticker', 'week'], axis=0, ascending=True, ignore_index=True, inplace=True)
        elif interval == '1d':
            self._day_df = dataframe
            self._day_df.sort_values(['ticker', 'day'], axis=0, ascending=True, ignore_index=True, inplace=True)

    def _get_capital_per_risk(self, risk):
        return round(self._risk_capital_product * self._total_capital / risk, 2)

    # Old
    # def process_single_ticker_operations(self, ticker):

    #     ongoing_operation_flag = False
    #     operation = None
    #     partial_sale_flag = False

    #     if ticker in self._tickers:
    #         for _, row in self._day_df.loc[self._day_df['ticker'] == ticker].iterrows():

    #             if (ongoing_operation_flag == False) or (operation is not None and operation.state == State.NOT_STARTED):
    #                 # Check price tendency in Main Graph Time
    #                 if row['up_down_trend_status'] == 1:

    #                     # Check if price is next to one EMA in Main Graph Time
    #                     if row['close_price'] < max(row['ema_17'], row['ema_72']) * 1.01 and row['close_price'] > min(row['ema_17'], row['ema_72']) * 0.99:

    #                         maj_graph_time_ema_72 = self._week_df.loc[(self._week_df['ticker'] == ticker) & (self._week_df['week'] <= row['day'])].tail(2)['ema_72'].head(1).values[0]

    #                         maj_graph_time_trend = self._week_df.loc[(self._week_df['ticker'] == ticker) & (self._week_df['week'] <= row['day'])].tail(2)['up_down_trend_status'].head(1).values[0]

    #                         # Check if price is greater than EMA_72 in Major Graph Time
    #                         if row['close_price'] > maj_graph_time_ema_72 and maj_graph_time_trend == 1:

    #                             # Identify last maximum peak
    #                             if not self._day_df.loc[(self._day_df['ticker'] == ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == 1) & (self._day_df['max_price'] > row['close_price'])].empty:
    #                                 purchase_target = self._day_df.loc[(self._day_df['ticker'] == ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == 1) & (self._day_df['max_price'] > row['close_price'])].tail(1)['max_price'].values[0]
    #                                 purchase_target = round(purchase_target, 2)
    #                             # If no peak has a maximum greater enough, choose the first past maximum
    #                             else:
    #                                 purchase_target = self._day_df.loc[(self._day_df['ticker'] == ticker) & (self._day_df['day'] < row['day']) & (self._day_df['max_price'] > row['close_price'])].tail(1)['max_price'].values[0]
    #                                 purchase_target = round(purchase_target, 2)

    #                             stop_loss = self._day_df.loc[(self._day_df['ticker'] == ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == -1) & (self._day_df['min_price'] < row['close_price'])].tail(1)['min_price'].values[0]

    #                             operation = Operation(ticker)
    #                             operation.set_purchase_target(purchase_target)
    #                             operation.set_stop_loss(stop_loss)
    #                             operation.set_sale_target(round(purchase_target + (purchase_target - stop_loss) * 3, 2))
    #                             operation.set_partial_sale(round(purchase_target + (purchase_target - stop_loss), 2))

    #                             ongoing_operation_flag = True

    #             if ongoing_operation_flag == True:

    #                 if operation.state == State.NOT_STARTED:

    #                     # Check if the target purchase price was hit
    #                     if operation.target_purchase_price >= row['min_price'] and operation.target_purchase_price <= row['max_price']:

    #                         operation.add_purchase(operation.target_purchase_price, calculate_maximum_volume(operation.target_purchase_price, self._get_capital_per_risk((operation.target_purchase_price - operation.stop_loss)/(operation.target_purchase_price)), minimum_volume=1), row['day'])

    #                 elif operation.state == State.OPEN:

    #                     # Check if the target STOP LOSS is hit
    #                     if operation.stop_loss >= row['min_price'] and operation.stop_loss <= row['max_price']:
    #                         operation.add_sale(operation.stop_loss, operation.total_purchase_volume - operation.total_sale_volume, row['day'], stop_flag=True)

    #                     # Check if the target STOP LOSS is skipped
    #                     elif operation.stop_loss > row['max_price']:
    #                         operation.add_sale(row['min_price'], operation.total_purchase_volume - operation.total_sale_volume, row['day'], stop_flag=True)

    #                     # After hitting the stol loss, the operation can be closed
    #                     if operation.state == State.OPEN:

    #                         # Check if the PARTIAL SALE price is hit
    #                         if partial_sale_flag == False and operation.partial_sale_price >= row['min_price'] and operation.partial_sale_price <= row['max_price']:
    #                             operation.add_sale(operation.partial_sale_price, math.ceil(operation.purchase_volume[0] / 2), row['day'])
    #                             partial_sale_flag = True

    #                         # Check if the PARTIAL SALE price is skipped but not TARGET SALE
    #                         elif partial_sale_flag == False and operation.partial_sale_price < row['min_price'] and operation.target_sale_price > row['max_price']:
    #                             operation.add_sale(row['min_price'], math.ceil(operation.purchase_volume[0] / 2), row['day'])
    #                             partial_sale_flag = True

    #                             # LOG skip cases

    #                         # Check if the TARGET SALE price is hit
    #                         if operation.target_sale_price >= row['min_price'] and operation.target_sale_price <= row['max_price']:
    #                             operation.add_sale(operation.target_sale_price, operation.total_purchase_volume - operation.total_sale_volume, row['day'])

    #                         # Check if the TARGET SALE price is skipped
    #                         if operation.target_sale_price < row['min_price']:
    #                             operation.add_sale(row['min_price'], operation.total_purchase_volume - operation.total_sale_volume, row['day'])

    #                 if operation.state == State.CLOSE:
    #                     self.operations.append(operation)
    #                     operation = None
    #                     ongoing_operation_flag = False
    #                     partial_sale_flag = False

    #         ongoing_operation_flag = False
    #         partial_sale_flag = False
    #         if operation is not None and operation.state == State.OPEN:
    #             self.operations.append(operation)

    def process_operations(self):

        minimum_volume_batch = 1

        ticker_priority_list = [self.TickerState(ticker, self._initial_dates[self._tickers.index(ticker)], self._final_dates[self._tickers.index(ticker)]) for ticker in self._tickers]

        # Iterate over each day chronologically
        for _, day in self._day_df.sort_values(by=['day'], axis=0, kind='mergesort', ascending=True, ignore_index=True)['day'].drop_duplicates().iteritems():

            # Some data will be modified during loop
            ticker_priority_list_copy = ticker_priority_list.copy()

            for index, ts in enumerate(ticker_priority_list_copy):

                # This day must be in the user selected bounds for the ticker
                if day >= ts.initial_date and day <= ts.final_date:

                    row =  self._day_df.loc[(self._day_df['day'] == day) & (self._day_df['ticker'] == ts.ticker)].squeeze()

                    if row.empty == False:
                        if (ts.ongoing_operation_flag == False) or (ts.operation is not None and ts.operation.state == State.NOT_STARTED):
                            # # Check price tendency in Main Graph Time
                            # if row['up_down_trend_status'] == 1:

                            # Check if price is next to one EMA in Main Graph Time
                            if row['close_price'] < max(row['ema_17'], row['ema_72']) * 1.01 and row['close_price'] > min(row['ema_17'], row['ema_72']) * 0.99:

                                maj_graph_time_ema_72 = self._week_df.loc[(self._week_df['ticker'] == ts.ticker) & (self._week_df['week'] <= row['day'])].tail(2)['ema_72'].head(1).values[0]

                                maj_graph_time_trend = self._week_df.loc[(self._week_df['ticker'] == ts.ticker) & (self._week_df['week'] <= row['day'])].tail(2)['up_down_trend_status'].head(1).values[0]

                                # Check if price is greater than EMA_72 in Major Graph Time
                                if row['close_price'] > maj_graph_time_ema_72 and maj_graph_time_trend == 1:

                                    # Purchase strategic filters satified, but it must exists a stop_loss reference
                                    if self._day_df.loc[(self._day_df['ticker'] == ts.ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == -1) & (self._day_df['min_price'] < row['close_price'])].empty == False:

                                        # Set purchase target price by identifying the last maximum peak
                                        if not self._day_df.loc[(self._day_df['ticker'] == ts.ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == 1) & (self._day_df['max_price'] > row['close_price'])].empty:
                                            purchase_target = self._day_df.loc[(self._day_df['ticker'] == ts.ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == 1) & (self._day_df['max_price'] > row['close_price'])].tail(1)['max_price'].values[0]
                                            purchase_target = round(purchase_target, 2)

                                        # If no peak has a maximum greater enough, choose the first past maximum
                                        else:
                                            purchase_target = self._day_df.loc[(self._day_df['ticker'] == ts.ticker) & (self._day_df['day'] < row['day']) & (self._day_df['max_price'] > row['close_price'])].tail(1)['max_price'].values[0]
                                            purchase_target = round(purchase_target, 2)

                                        stop_loss = self._day_df.loc[(self._day_df['ticker'] == ts.ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == -1) & (self._day_df['min_price'] < row['close_price'])].tail(1)['min_price'].values[0]

                                        purchase_target = round(row['close_price'] + (row['close_price']- stop_loss) * 3, 2)

                                        ticker_priority_list[index].operation = Operation(ts.ticker)
                                        ticker_priority_list[index].operation.set_purchase_target(purchase_target)
                                        ticker_priority_list[index].operation.set_stop_loss(stop_loss)
                                        ticker_priority_list[index].operation.set_sale_target(round(purchase_target + (purchase_target - stop_loss) * 3, 2))
                                        ticker_priority_list[index].operation.set_partial_sale(round(purchase_target + (purchase_target - stop_loss), 2))
                                        ticker_priority_list[index].ongoing_operation_flag = True

                        if ts.ongoing_operation_flag == True:

                            if ts.operation.state == State.NOT_STARTED:

                                # Check if the target purchase price was hit
                                if ts.operation.target_purchase_price >= row['min_price'] and ts.operation.target_purchase_price <= row['max_price']:

                                    available_money = self.available_capital - sum([ts.operation.total_purchase_capital - ts.operation.total_sale_capital for ts in ticker_priority_list if (ts.operation is not None and ts.operation.state == State.OPEN)])

                                    purchase_money = ts.operation.target_purchase_price * calculate_maximum_volume(ts.operation.target_purchase_price, self._get_capital_per_risk((ts.operation.target_purchase_price - ts.operation.stop_loss)/(ts.operation.target_purchase_price)), minimum_volume=minimum_volume_batch)

                                    # Check if there is enough money
                                    if available_money >= purchase_money:
                                        ticker_priority_list[index].operation.add_purchase(ts.operation.target_purchase_price, calculate_maximum_volume(ts.operation.target_purchase_price, self._get_capital_per_risk((ts.operation.target_purchase_price - ts.operation.stop_loss)/(ts.operation.target_purchase_price)), minimum_volume=1), row['day'])
                                    else:
                                        ticker_priority_list[index].operation.add_purchase(ts.operation.target_purchase_price, calculate_maximum_volume(ts.operation.target_purchase_price, available_money, minimum_volume=minimum_volume_batch), row['day'])

                            elif ts.operation.state == State.OPEN:

                                # Check if the target STOP LOSS is hit
                                if ts.operation.stop_loss >= row['min_price'] and ts.operation.stop_loss <= row['max_price']:
                                    ticker_priority_list[index].operation.add_sale(ts.operation.stop_loss, ts.operation.total_purchase_volume - ts.operation.total_sale_volume, row['day'], stop_flag=True)

                                # Check if the target STOP LOSS is skipped
                                elif ts.operation.stop_loss > row['max_price']:
                                    ticker_priority_list[index].operation.add_sale(row['min_price'], ts.operation.total_purchase_volume - ts.operation.total_sale_volume, row['day'], stop_flag=True)

                                # After hitting the stol loss, the operation can be closed
                                if ts.operation.state == State.OPEN:

                                    # Check if the PARTIAL SALE price is hit
                                    if ts.partial_sale_flag == False and ts.operation.partial_sale_price >= row['min_price'] and ts.operation.partial_sale_price <= row['max_price']:
                                        ticker_priority_list[index].operation.add_sale(ts.operation.partial_sale_price, math.ceil(ts.operation.purchase_volume[0] / 2), row['day'])
                                        ticker_priority_list[index].partial_sale_flag = True

                                    # Check if the PARTIAL SALE price is skipped but not TARGET SALE
                                    elif ts.partial_sale_flag == False and ts.operation.partial_sale_price < row['min_price'] and ts.operation.target_sale_price > row['max_price']:
                                        ticker_priority_list[index].operation.add_sale(row['min_price'], math.ceil(ts.operation.purchase_volume[0] / 2), row['day'])
                                        ticker_priority_list[index].partial_sale_flag = True

                                        # LOG skip cases

                                    # Check if the TARGET SALE price is hit
                                    if ts.operation.target_sale_price >= row['min_price'] and ts.operation.target_sale_price <= row['max_price']:
                                        ticker_priority_list[index].operation.add_sale(ts.operation.target_sale_price, ts.operation.total_purchase_volume - ts.operation.total_sale_volume, row['day'])

                                    # Check if the TARGET SALE price is skipped
                                    if ts.operation.target_sale_price < row['min_price']:
                                        ticker_priority_list[index].operation.add_sale(row['min_price'], ts.operation.total_purchase_volume - ts.operation.total_sale_volume, row['day'])

                            if ticker_priority_list[index].operation.state == State.CLOSE:
                                self.operations.append(ticker_priority_list[index].operation)
                                ticker_priority_list[index].operation = None
                                ticker_priority_list[index].ongoing_operation_flag = False
                                ticker_priority_list[index].partial_sale_flag = False

            ticker_priority_list = self._order_by_priority(ticker_priority_list)

        # Insert remaining open operations
        for ts in ticker_priority_list:
            if ts.operation is not None and ts.operation.state == State.OPEN:
                self.operations.append(ts.operation)

    def _order_by_priority(self, ticker_priority_list):
        # Priority:
        #   1) Open operation
        #   2) Not started operation
        #   3) Any
        # ticker_priority_list: list(TickerState)

        new_list = sorted(ticker_priority_list, key=lambda ticker_state: str(int(ticker_state.operation.state == State.OPEN if ticker_state.operation is not None else 0)) + str(int(ticker_state.operation.state == State.NOT_STARTED if ticker_state.operation is not None else 0)))

        return new_list

    def save(self):
        self._db_strategy_model.insert_strategy_results(self._statistics_parameters, self.operations, self._statistics_graph)

    def calculate_statistics(self):
        self._calculate_statistics_graph()
        self._calculate_statistics_parameters()

    def _calculate_statistics_graph(self):

        statistics = pd.DataFrame(columns=['day', 'capital', 'capital_in_use', 'tickers_average', 'ibov'])

        statistics['day'] = self._day_df[self._day_df['day'] >= min(self._initial_dates)].sort_values(by=['day'], axis=0, kind='mergesort', ascending=True, ignore_index=True)['day'].drop_duplicates()

        statistics.reset_index(inplace=True)

        ibov_data = self._db_strategy_model.get_ticker_price('^BVSP', pd.to_datetime(statistics['day'].head(1).values[0]), pd.to_datetime(statistics['day'].tail(1).values[0]))

        statistics['ibov'] = ibov_data.sort_values(by='day', axis=0, ascending=True, ignore_index=True)['close_price']

        statistics['tickers_average'] = self._calculate_average_tickers_yield(statistics)

        statistics['capital'], statistics['capital_in_use'] = self._calculate_capital_usage(statistics)

        statistics.fillna(method='ffill', inplace=True)

        self._statistics_graph = statistics

    def _calculate_statistics_parameters(self):

        # Profit
        last_capital_value = self._statistics_graph['capital'].tail(1).values[0]

        self._statistics_parameters['profit'] = round(last_capital_value - self._total_capital, 2)

        # Maximum Capital Used
        self._statistics_parameters['max_used_capital'] = round(max(self._statistics_graph['capital_in_use']), 2)

        # Yield
        self._statistics_parameters['yield'] = round(self._statistics_parameters['profit'] / self._total_capital, 4)

        # Annualized Yield
        bus_day_count = len(self._statistics_graph)

        self._statistics_parameters['annualized_yield'] = round(calculate_yield_annualized(self._statistics_parameters['yield'], bus_day_count), 4)

        # IBOV Yield
        first_ibov_value = self._statistics_graph['ibov'].head(1).values[0]
        last_ibov_value = self._statistics_graph['ibov'].tail(1).values[0]
        ibov_yield = (last_ibov_value / first_ibov_value) - 1

        self._statistics_parameters['ibov_yield'] = round(ibov_yield, 4)

        # Annualized IBOV Yield
        self._statistics_parameters['annualized_ibov_yield'] = round(calculate_yield_annualized(self._statistics_parameters['ibov_yield'], bus_day_count), 4)

        # Average Tickers Yield
        first_avr_tickers_value = self._statistics_graph['tickers_average'].head(1).values[0]
        last_avr_tickers_value = self._statistics_graph['tickers_average'].tail(1).values[0]
        avr_tickers_yield = (last_avr_tickers_value / first_avr_tickers_value) - 1

        self._statistics_parameters['avr_tickers_yield'] = round(avr_tickers_yield, 4)

        # Annualized Average Tickers Yield
        self._statistics_parameters['annualized_avr_tickers_yield'] = round(calculate_yield_annualized(self._statistics_parameters['avr_tickers_yield'], bus_day_count), 4)

        # Volatility
        returns = self._statistics_graph['capital'] / self._statistics_graph['capital'][0] - 1
        self._statistics_parameters['volatility'] = returns.describe().loc[['std']].squeeze()

        # Sharpe Ration
        # Risk-free yield by CDI index

        cdi_df = self._db_strategy_model.get_cdi_index(min(self._initial_dates), max(self._final_dates))

        self._statistics_parameters['sharpe_ratio'] = (self._statistics_parameters['yield'] - (cdi_df['cumulative'].tail(1).squeeze() - 1.0)) / self._statistics_parameters['volatility']


    def _calculate_average_tickers_yield(self, statistics):

        tickers_data = [None] * len(statistics)
        tickers_first_values = [None] * len(self._tickers)
        tickers_first_values_flag = [False] * len(self._tickers)

        # Iterate over each day chronologically
        for day_index, day in statistics['day'].iteritems():

            relative_yield = [None] * len(self._tickers)

            for ticker_index, ticker in enumerate(self._tickers):

                if tickers_first_values_flag[ticker_index] == False and day >= self._day_df.loc[self._day_df['ticker'] == ticker, ['day']].squeeze().values[0]:
                    tickers_first_values[ticker_index] = self._day_df.loc[(self._day_df['day'] == day) & (self._day_df['ticker'] == ticker)]['close_price'].head(1).values[0]
                    tickers_first_values_flag[ticker_index] = True

                # This day must be in the user selected bounds for the ticker
                if day >= self._day_df.loc[self._day_df['ticker'] == ticker, ['day']].squeeze().values[0] and day < self._final_dates[ticker_index]:
                    relative_yield[ticker_index] = self._day_df.loc[(self._day_df['day'] == day) & (self._day_df['ticker'] == ticker)]['close_price'].head(1).values[0] / tickers_first_values[ticker_index]

            tickers_data[day_index] = round(sum(list(filter(None, relative_yield))) / len(relative_yield), 4)

        return tickers_data

    def _calculate_capital_usage(self, statistics):

        capital = [None] * len(statistics)
        capital_in_use = [None] * len(statistics)

        current_capital = self._total_capital
        current_capital_in_use = 0.0

        # Iterate over each day chronologically
        for day_index, day in statistics['day'].iteritems():

            holding_papers_capital = 0.0

            for oper in self._operations:

                # Compute purchases debts
                for p_price, p_volume, p_day in zip(oper.purchase_price, oper.purchase_volume, oper.purchase_datetime):
                    if day == p_day:
                        amount = round(p_price * p_volume, 2)

                        current_capital -= amount
                        current_capital_in_use += amount

                # Compute sale credits
                for s_price, s_volume, s_day in zip(oper.sale_price, oper.sale_volume, oper.sale_datetime):
                    if day == s_day:
                        amount = round(s_price * s_volume, 2)

                        current_capital += amount
                        current_capital_in_use -= amount

                #Compute holding papers prices
                if (oper.state == State.OPEN and day >= oper.start_date) or (oper.state == State.CLOSE and day >= oper.start_date and day < oper.end_date):

                    bought_volume = sum([p_volume for p_date, p_volume in zip(oper.purchase_datetime, oper.purchase_volume) if p_date <= day])

                    sold_volume = sum([s_volume for s_date, s_volume in zip(oper.sale_datetime, oper.sale_volume) if s_date <= day])

                    papers_in_hands = bought_volume - sold_volume

                    price = self._day_df.loc[(self._day_df['day'] == day) & (self._day_df['ticker'] == oper.ticker)]['close_price'].head(1).values[0]

                    holding_papers_capital += round(price * papers_in_hands, 2)

            capital[day_index] = round(current_capital + holding_papers_capital, 2)
            capital_in_use[day_index] = round(current_capital_in_use, 2)

        return capital, capital_in_use

    class TickerState:
        def __init__(self, ticker, initial_date, final_date, ongoing_operation_flag=False, partial_sale_flag=False, operation=None):
            self._ticker = ticker
            self._initial_date = initial_date
            self._final_date = final_date
            self._ongoing_operation_flag = ongoing_operation_flag
            self._partial_sale_flag = partial_sale_flag
            self._operation = operation

        @property
        def ticker(self):
            return self._ticker

        @property
        def initial_date(self):
            return self._initial_date

        @property
        def final_date(self):
            return self._final_date

        @property
        def ongoing_operation_flag(self):
            return self._ongoing_operation_flag

        @ongoing_operation_flag.setter
        def ongoing_operation_flag(self, ongoing_operation_flag):
            self._ongoing_operation_flag = ongoing_operation_flag

        @property
        def partial_sale_flag(self):
            return self._partial_sale_flag

        @partial_sale_flag.setter
        def partial_sale_flag(self, partial_sale_flag):
            self._partial_sale_flag = partial_sale_flag

        @property
        def operation(self):
            return self._operation

        @operation.setter
        def operation(self, operation):
            self._operation = operation