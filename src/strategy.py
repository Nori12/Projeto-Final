from os import close
from pathlib import Path
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import random
from abc import ABC, abstractmethod
import sys
import numpy as np
import math
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# from yfinance import ticker
# import time
from operator import add

import constants as c
from utils import RunTime, calculate_maximum_volume, calculate_yield_annualized, State, Trend
from db_model import DBStrategyModel, DBGenericModel

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
    """
    Operation object for handling `Strategy` attempts of purchase and sale.

    An Operation starts with the purchase of stocks and ends with its complete sale.

    Support ONE purchase and multiple sales.
    Do not support short operations.

    Args
    ----------
    ticker : str
        Ticker name.

    Properties
    ----------
    ticker : str
        Ticker name.
    state : `utils.State`
        Operation state (open, close or not started).
    start_date : `datetime.date`
        Date of the first purchase.
    end_date : `datetime.date`
        Date of the last sale.
    number_of_orders : int
        Total number of purchase or sale orders.
    profit : float
        Profit after operation close.
    result_yield : float
        Yield after operation close.
    target_purchase_price : float
        Target purchase price.
    purchase_price : `list` of float
        Actual purchase prices.
    purchase_volume : `list` of int
        Purchases volumes.
    purchase_datetime : `list` of `datetime.datetime`
        Purchases datetimes.
    total_purchase_capital : float
        Total purchase capital.
    total_purchase_volume : int
        Total purchase volume.
    target_sale_price : float
        Target sale price.
    stop_loss : float
        Stop loss.
    partial_sale_price : float
        Partial sale price.
    sale_price : `list` of float
        Actual sale prices.
    sale_volume : `list` of int
        Sales volumes.
    sale_datetime : `list` of `datetime.datetime`
        Sales datetimes.
    stop_loss_flag : `list` of `bool`
        Indicator that partial sale price hit triggered sale.
    partial_sale_flag : `list` of `bool`
        Indicator that stop loss triggered sale.
    total_sale_capital : float
        Total sale capital.
    total_sale_volume : int
        Total sale volume.

    Methods
    ----------
    add_purchase(purchase_price, purchase_volume, purchase_datetime)
        Add purchase execution.
    add_sale(sale_price, sale_volume, sale_datetime, stop_loss_flag=False)
        Add sale execution.
    """
    def __init__(self, ticker):
        self._ticker = ticker
        self._state = State.NOT_STARTED
        self._start_date = None
        self._end_date = None
        self._number_of_orders = 0
        self._target_purchase_price = None
        self._purchase_price = []
        self._purchase_volume = []
        self._purchase_datetime = []
        self._target_sale_price = None
        self._sale_price = []
        self._sale_volume = []
        self._sale_datetime = []
        self._stop_loss_flag = []
        self._partial_sale_flag = []
        self._stop_loss = None
        self._partial_sale_price = None
        self._profit = None
        self._yield = None

    # General properties
    @property
    def ticker(self):
        """str : Ticker name."""
        return self._ticker

    @property
    def state(self):
        """`utils.State` : Operation state (open, close or not started)."""
        return self._state

    @property
    def start_date(self):
        """`datetime.date` : Date of the first purchase."""
        return self._start_date

    @property
    def end_date(self):
        """`datetime.date` : Date of the last sale."""
        return self._end_date

    @property
    def number_of_orders(self):
        """int : Total number of purchase or sale orders."""
        return self._number_of_orders

    @property
    def profit(self):
        """float : Profit after operation close."""
        return self._profit

    @property
    def result_yield(self):
        """float : Yield after operation close."""
        return self._yield

    # Purchase properties
    @property
    def target_purchase_price(self):
        """float : Target purchase price."""
        return self._target_purchase_price

    @target_purchase_price.setter
    def target_purchase_price(self, target_purchase_price):
        if self.state != State.CLOSE:
            self._target_purchase_price = target_purchase_price

    @property
    def purchase_price(self):
        """`list` of float : Actual purchase prices."""
        return self._purchase_price

    @property
    def purchase_volume(self):
        """`list` of int : Purchases volumes."""
        return self._purchase_volume

    @property
    def purchase_datetime(self):
        """`list` of `datetime.datetime` : Purchases datetimes."""
        return self._purchase_datetime

    @property
    def total_purchase_capital(self):
        """float : Total purchase capital."""
        capital = 0.0
        for (purchase, volume) in zip(self._purchase_price, self._purchase_volume):
            capital += purchase * volume
        return round(capital, 2)

    @property
    def total_purchase_volume(self):
        """int : Total purchase volume."""
        total_volume = 0.0
        for volume in self._purchase_volume:
            total_volume += volume
        return total_volume

    # Sale properties
    @property
    def target_sale_price(self):
        """float : Target sale price."""
        return self._target_sale_price

    @target_sale_price.setter
    def target_sale_price(self, target_sale_price):
        if self.state != State.CLOSE:
            self._target_sale_price = target_sale_price

    @property
    def stop_loss(self):
        """float : Stop loss."""
        return self._stop_loss

    @stop_loss.setter
    def stop_loss(self, stop_loss):
        if self.state != State.CLOSE:
            self._stop_loss = stop_loss

    @property
    def partial_sale_price(self):
        """float : Partial sale price."""
        return self._partial_sale_price

    @partial_sale_price.setter
    def partial_sale_price(self, partial_sale_price):
        if self.state != State.CLOSE:
            self._partial_sale_price = partial_sale_price

    @property
    def sale_price(self):
        """`list` of float : Actual sale prices."""
        return self._sale_price

    @property
    def sale_volume(self):
        """`list` of int : Sales volumes."""
        return self._sale_volume

    @property
    def sale_datetime(self):
        """`list` of `datetime.datetime` : Sales datetimes."""
        return self._sale_datetime

    @property
    def partial_sale_flag(self):
        """`list` of `bool` : Indicator that partial sale price hit triggered sale."""
        return self._partial_sale_flag

    @property
    def stop_loss_flag(self):
        """`list` of `bool` : Indicator that stop loss triggered sale."""
        return self._stop_loss_flag

    @property
    def total_sale_capital(self):
        """float : Total sale capital."""
        capital = 0.0
        for (sale, volume) in zip(self._sale_price, self._sale_volume):
            capital += + sale * volume
        return round(capital, 2)

    @property
    def total_sale_volume(self):
        """int : Total sale volume."""
        total_volume = 0.0
        for volume in self._sale_volume:
            total_volume += + volume
        return total_volume

    def add_purchase(self, purchase_price, purchase_volume, purchase_datetime):
        """
        Add purchase execution.

        Only add if `state` is not close.
        Once add, change `state` to open.

        Args
        ----------
        purchase_price : float
            Purchase price.
        purchase_volume : int
            Purchase volume.
        purchase_datetime : `datetime.datetime`
            Purchase datetime.
        """
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

    def add_sale(self, sale_price, sale_volume, sale_datetime, stop_loss_flag=False,
        partial_sale_flag=False):
        """
        Add sale execution.

        Only add if `state` is not close.
        Once add, change `state` to open.

        Args
        ----------
        sale_price : float
            Sale price.
        sale_volume : int
            Sale volume.
        sale_datetime : `datetime.datetime`
            Sale datetime.
        stop_loss_flag : bool
            Indicator that stop loss triggered sale.
        partial_sale_flag : bool
            Indicator that partial sale price hit triggered sale.
        """
        if stop_loss_flag == partial_sale_flag == True:
            logger.error(f"Error arguments \'stop_loss_flag\' and "
                f"\'partial_sale_flag\' can not be True simultaneously.")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if self.state == State.OPEN \
            and self.total_purchase_volume >= self.total_sale_volume + sale_volume:
            self._sale_price.append(sale_price)
            self._sale_volume.append(sale_volume)
            self._sale_datetime.append(sale_datetime)
            self._stop_loss_flag.append(stop_loss_flag)
            self._partial_sale_flag.append(partial_sale_flag)
            self._number_of_orders += 1

            if self.total_purchase_volume == self.total_sale_volume:
                # self._partial_sale_flag.append(False)
                self._end_date = sale_datetime
                self._profit = self.total_sale_capital - self.total_purchase_capital
                self._yield = self._profit / self.total_purchase_capital
                self._state = State.CLOSE
            # else:
            #     self._partial_sale_flag.append(True)

            return True
        return False

class Strategy(ABC):
    """
    Base class for all strategies.

    Properties
    ----------
    name : str
        Strategy name.
    alias : str
        Alias for strategy fast identification.
    comment : str
        Comment.
    tickers : `dict`
        All tickers. Value must be another `dict` with `start_date` and `end_date` keys.
    total_capital : float
        Total available capital.
    operations : `list` of `Operation`
        List containing all registered operations, no matter its state.

    Methods
    ----------
    process_operations()
        Process each ticker and day to create a `list` of `Operation`.
    calculate_statistics()
        Calculate all statistics results. Must be used after `process_operations()`.
    save()
        Save operations and statistics in database.
    """
    @property
    @abstractmethod
    def name(self):
        """str : Strategy name."""
        pass

    @property
    @abstractmethod
    def alias(self):
        """str : Alias for strategy fast identification."""
        pass

    @alias.setter
    @abstractmethod
    def alias(self, alias):
        pass

    @property
    @abstractmethod
    def comment(self):
        """str : Comment."""
        pass

    @comment.setter
    @abstractmethod
    def comment(self, comment):
        pass

    @property
    @abstractmethod
    def tickers(self):
        """`dict` : All tickers. Value is another `dict` with `start_date` and `end_date` keys."""
        pass

    @property
    @abstractmethod
    def total_capital(self):
        """float : Total available capital."""
        pass

    @property
    @abstractmethod
    def operations(self):
        """`list` of `Operation` : List containing all registered operations, no matter its state."""
        pass

    @abstractmethod
    def process_operations(self):
        """Process each ticker and day to create a `list` of `Operation`."""
        pass

    @abstractmethod
    def calculate_statistics(self):
        """Calculate all statistics results. Must be used after `process_operations()`."""
        pass

    @abstractmethod
    def save(self):
        """Save operations and statistics in database."""
        pass

class AndreMoraesStrategy(Strategy):

    def __init__(self, tickers, alias=None, comment=None, min_order_volume=1,
        total_capital=100000, risk_capital_product=0.10, min_volume_per_year=1000000):
        #add self.price_to_emas_tolerance
        if risk_capital_product < 0.0 or risk_capital_product > 1.0:
            logger.error(f"""Parameter \'risk_reference\' must be in the interval [0, 1].""")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        self._name = "Andre Moraes"
        self._alias = alias
        self._comment = comment
        self._tickers = []
        self._initial_dates = []
        self._final_dates = []
        self.min_order_volume = min_order_volume
        self._total_capital = total_capital
        self._risk_capital_product = risk_capital_product
        self._min_volume_per_year = min_volume_per_year
        self._operations = []

        self.price_to_emas_tolerance = 0.05

        self._start_date = None
        self._end_date = None

        # self._day_df = None
        # self._week_df = None

        self._tickers_and_dates = tickers
        for ticker, date in tickers.items():
            self._tickers.append(ticker)
            self._initial_dates.append(date['start_date'])
            self._final_dates.append(date['end_date'])

        self.first_date = min(tickers.values(), key=lambda x: x['start_date'])['start_date']
        self.last_date = max(tickers.values(), key=lambda x: x['end_date'])['end_date']

        self._db_strategy_model = DBStrategyModel(self._name, self._tickers, self._initial_dates,
            self._final_dates, self._total_capital, alias=self._alias, comment=self._comment,
            risk_capital_product=self._risk_capital_product, min_volume_per_year=min_volume_per_year)
        self._db_generic_model = DBGenericModel()

        self._statistics_graph = None
        self._statistics_parameters = {'profit': None, 'max_used_capital': None, 'yield': None,
            'annualized_yield': None, 'ibov_yield': None, 'annualized_ibov_yield': None,
            'avr_tickers_yield': None, 'annualized_avr_tickers_yield': None, 'volatility': None,
            'sharpe_ratio': None}

        # if self._min_volume_per_year != 0:
        #     self._filter_tickers_per_min_volume()

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
    def tickers_and_dates(self):
        return self._tickers_and_dates

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
        return self._total_capital

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

    class DataGen:
        def __init__(self, tickers, db_connection, days_batch=30):
            self.tickers = tickers
            self.first_date = min(self.tickers.values(), key=lambda x: x['start_date'])['start_date']
            self.last_date = max(self.tickers.values(), key=lambda x: x['end_date'])['end_date']
            self.db_connection = db_connection
            self.days_batch = days_batch

            self._db_generic_model = DBGenericModel()
            holidays = self._db_generic_model.get_holidays(self.first_date, self.last_date).to_list()
            self.dates = pd.date_range(start=self.first_date, end=self.last_date, freq='B').to_list()
            self.dates = [date for date in self.dates if date not in holidays]

            self.dates_length = len(self.dates)
            self.current_date_index = 0

            self.daily_data = pd.DataFrame()
            self.weekly_data = pd.DataFrame()

        def __next__(self):
            return self.run()

        def run(self):

            if self.current_date_index == self.dates_length:
                raise StopIteration()

            if self.daily_data.empty or \
                self.daily_data[self.daily_data['day'] >= self.dates[self.current_date_index]].empty:

                next_chunk_end_index = self.current_date_index + self.days_batch - 1 \
                    if self.current_date_index + self.days_batch - 1 < self.dates_length \
                    else self.dates_length - 1

                self.daily_data = self.db_connection.get_data_chunk(self.tickers,
                    self.dates[self.current_date_index],
                    self.dates[next_chunk_end_index], interval='1d')
                self.weekly_data = self.db_connection.get_data_chunk(self.tickers,
                    self.dates[self.current_date_index],
                    self.dates[next_chunk_end_index], interval='1wk')

            self.current_date_index += 1

            year, week, _ = (self.dates[self.current_date_index-1] - pd.Timedelta(days=7)).isocalendar()
            return self.daily_data[self.daily_data['day'] == self.dates[self.current_date_index-1]], \
                self.weekly_data[(self.weekly_data['week'].dt.isocalendar().year == year) \
                    & (self.weekly_data['week'].dt.isocalendar().week == week)]

    # @RunTime('AndreMoraesStrategy.load_data')
    # def load_data(self):
    #     try:
    #         self._day_df = self._db_strategy_model.get_candles_dataframe(self.tickers_and_dates, interval='1d')
    #         self._week_df = self._db_strategy_model.get_candles_dataframe(self.tickers_and_dates, interval='1wk')
    #         pass
    #     except Exception as error:
    #         logger.exception(f"Error loading data, error:\n{error}")
    #         sys.exit(c.UPDATING_DB_ERR)

    @RunTime('AndreMoraesStrategy.process_operations')
    def process_operations(self):
        try:
            ticker_priority_list = [self.TickerState(ticker, dates['start_date'], dates['end_date']) \
                for ticker, dates in self.tickers_and_dates.items()]

            data_gen = self.DataGen(self.tickers_and_dates, self._db_strategy_model, days_batch=30)

            while True:
                try:
                    day_info, week_info = next(data_gen)

                    # List will be modified during loop
                    ticker_priority_list_cp = ticker_priority_list.copy()

                    if not (day_info.empty or week_info.empty):

                        day = day_info.head(1)['day'].squeeze()

                        for index, ts in enumerate(ticker_priority_list_cp):

                            if day_info[(day_info['ticker'] == ts.ticker)].empty:
                                continue

                            if day >= ts.initial_date and day <= ts.final_date:

                                # DEBUG
                                # if day == pd.to_datetime('2018-04-24', format='%Y-%m-%d'):
                                #     print()

                                open_price_day = day_info[(day_info['ticker'] == ts.ticker)]['open_price'].squeeze()
                                max_price_day = day_info[(day_info['ticker'] == ts.ticker)]['max_price'].squeeze()
                                min_price_day = day_info[(day_info['ticker'] == ts.ticker)]['min_price'].squeeze()
                                close_price_day = day_info[(day_info['ticker'] == ts.ticker)]['close_price'].squeeze()
                                # volume_day = day_info[(day_info['ticker'] == ts.ticker)]['volume'].squeeze()
                                ema_17_day = day_info[(day_info['ticker'] == ts.ticker)]['ema_17'].squeeze()
                                ema_72_day = day_info[(day_info['ticker'] == ts.ticker)]['ema_72'].squeeze()
                                target_buy_price_day = day_info[(day_info['ticker'] == ts.ticker)]['target_buy_price'].squeeze()
                                stop_loss_day = day_info[(day_info['ticker'] == ts.ticker)]['stop_loss'].squeeze()
                                up_down_trend_status_day = day_info[(day_info['ticker'] == ts.ticker)]['up_down_trend_status'].squeeze()

                                # open_price_week = week_info[(week_info['ticker'] == ts.ticker)]['open_price'].squeeze()
                                # max_price_week = week_info[(week_info['ticker'] == ts.ticker)]['max_price'].squeeze()
                                # min_price_week = week_info[(week_info['ticker'] == ts.ticker)]['min_price'].squeeze()
                                # close_price_week = week_info[(week_info['ticker'] == ts.ticker)]['close_price'].squeeze()
                                # volume_week = week_info[(week_info['ticker'] == ts.ticker)]['volume'].squeeze()
                                # ema_17_week = week_info[(week_info['ticker'] == ts.ticker)]['ema_17'].squeeze()
                                ema_72_week = week_info[(week_info['ticker'] == ts.ticker)]['ema_72'].squeeze()
                                # target_buy_price_week = week_info[(week_info['ticker'] == ts.ticker)]['target_buy_price'].squeeze()
                                # stop_loss_week = week_info[(week_info['ticker'] == ts.ticker)]['stop_loss'].squeeze()
                                up_down_trend_status_week = week_info[(week_info['ticker'] == ts.ticker)]['up_down_trend_status'].squeeze()

                                if (ts.ongoing_operation_flag == False) or (ts.operation \
                                    is not None and ts.operation.state == State.NOT_STARTED):

                                    # Strategy core rules
                                    if (up_down_trend_status_day >= Trend.ALMOST_UPTREND.value \
                                        and up_down_trend_status_week >= Trend.ALMOST_UPTREND.value) \
                                        and (close_price_day < max(ema_17_day, ema_72_day)*(1+self.price_to_emas_tolerance) \
                                        and close_price_day > min(ema_17_day, ema_72_day)*(1-self.price_to_emas_tolerance)) \
                                        and (close_price_day > ema_72_week):

                                        if target_buy_price_day != 0 and stop_loss_day != 0:

                                            ticker_priority_list[index].operation = Operation(ts.ticker)
                                            ticker_priority_list[index].operation.target_purchase_price = \
                                                target_buy_price_day
                                            ticker_priority_list[index].operation.stop_loss = stop_loss_day
                                            ticker_priority_list[index].operation.target_sale_price = \
                                                round(target_buy_price_day + (target_buy_price_day - stop_loss_day) * 3, 2)
                                            ticker_priority_list[index].operation.partial_sale_price = \
                                                round(target_buy_price_day + (target_buy_price_day - stop_loss_day), 2)
                                            ticker_priority_list[index].ongoing_operation_flag = True
                                        else:
                                            logger.warning(f"Ticker satisfies all purchase conditions, " \
                                                f"but no target price or stop loss is set. (\'{ts.ticker}\'" \
                                                f", \'{day.strftime('%Y-%m-%d')}\')")

                                if ts.ongoing_operation_flag == True:

                                    if ts.operation.state == State.NOT_STARTED:

                                        # Check if the target purchase price was hit
                                        if ts.operation.target_purchase_price >= min_price_day and \
                                            ts.operation.target_purchase_price <= max_price_day:

                                            # TODO: Check if I'm really getting the available money here.
                                            available_money = self.available_capital - sum([ts.operation.total_purchase_capital \
                                                - ts.operation.total_sale_capital for ts in ticker_priority_list \
                                                if (ts.operation is not None and ts.operation.state == State.OPEN)])

                                            purchase_money = ts.operation.target_purchase_price * calculate_maximum_volume(
                                                ts.operation.target_purchase_price, self._get_capital_per_risk(
                                                (ts.operation.target_purchase_price - ts.operation.stop_loss)/ \
                                                (ts.operation.target_purchase_price)), minimum_volume=self.min_order_volume)

                                            # if day == pd.Timestamp('2021-02-22T00'):
                                            #     print()

                                            # Check if there is enough money
                                            if available_money >= purchase_money:
                                                ticker_priority_list[index].operation.add_purchase(
                                                    ts.operation.target_purchase_price, calculate_maximum_volume(
                                                    ts.operation.target_purchase_price, self._get_capital_per_risk(
                                                    (ts.operation.target_purchase_price - ts.operation.stop_loss)/ \
                                                    (ts.operation.target_purchase_price)), minimum_volume=1), day)
                                            else:
                                                ticker_priority_list[index].operation.add_purchase(
                                                    ts.operation.target_purchase_price, calculate_maximum_volume(
                                                    ts.operation.target_purchase_price, available_money,
                                                    minimum_volume=self.min_order_volume), day)

                                    elif ts.operation.state == State.OPEN:

                                        # Check if the target STOP LOSS is hit
                                        if ts.operation.stop_loss >= min_price_day and \
                                            ts.operation.stop_loss <= max_price_day:
                                            ticker_priority_list[index].operation.add_sale(
                                                ts.operation.stop_loss, ts.operation.total_purchase_volume \
                                                - ts.operation.total_sale_volume, day, stop_loss_flag=True)

                                        # Check if the target STOP LOSS is skipped
                                        elif ts.operation.stop_loss > max_price_day:
                                            ticker_priority_list[index].operation.add_sale(open_price_day,
                                                ts.operation.total_purchase_volume - ts.operation.total_sale_volume,
                                                day, stop_loss_flag=True)

                                        # After hitting the stol loss, the operation can be closed
                                        if ts.operation.state == State.OPEN:

                                            # Check if the PARTIAL SALE price is hit
                                            if ts.partial_sale_flag == False and \
                                                ts.operation.partial_sale_price >= min_price_day and \
                                                ts.operation.partial_sale_price <= max_price_day:
                                                ticker_priority_list[index].operation.add_sale(
                                                    ts.operation.partial_sale_price, math.ceil(
                                                    ts.operation.purchase_volume[0] / 2), day, partial_sale_flag=True)
                                                ticker_priority_list[index].partial_sale_flag = True

                                            # Check if the PARTIAL SALE price is skipped but not TARGET SALE
                                            elif ts.partial_sale_flag == False and \
                                                ts.operation.partial_sale_price < min_price_day and \
                                                ts.operation.target_sale_price > max_price_day:
                                                ticker_priority_list[index].operation.add_sale(
                                                    open_price_day, math.ceil(ts.operation.purchase_volume[0] / 2),
                                                    day, partial_sale_flag=True)
                                                ticker_priority_list[index].partial_sale_flag = True

                                                # LOG skip cases

                                            # Check if the TARGET SALE price is hit
                                            if ts.operation.target_sale_price >= min_price_day and \
                                                ts.operation.target_sale_price <= max_price_day:
                                                ticker_priority_list[index].operation.add_sale(
                                                    ts.operation.target_sale_price, ts.operation.total_purchase_volume \
                                                        - ts.operation.total_sale_volume, day)

                                            # Check if the TARGET SALE price is skipped
                                            if ts.operation.target_sale_price < min_price_day:
                                                ticker_priority_list[index].operation.add_sale(open_price_day,
                                                ts.operation.total_purchase_volume - ts.operation.total_sale_volume,
                                                day)

                                    if ticker_priority_list[index].operation.state == State.CLOSE:
                                        self.operations.append(ticker_priority_list[index].operation)
                                        ticker_priority_list[index].operation = None
                                        ticker_priority_list[index].ongoing_operation_flag = False
                                        ticker_priority_list[index].partial_sale_flag = False

                        ticker_priority_list = self._order_by_priority(ticker_priority_list)
                except StopIteration:
                    break

            # Insert remaining open operations
            for ts in ticker_priority_list:
                if ts.operation is not None and ts.operation.state == State.OPEN:
                    self.operations.append(ts.operation)

        except Exception as error:
            logger.exception(f"Error processing operations, error:\n{error}")
            sys.exit(c.PROCESSING_OPERATIONS_ERR)


    def _get_capital_per_risk(self, risk):
        return round(self._risk_capital_product * self._total_capital / risk, 2)

    def _order_by_priority(self, ticker_priority_list):
        """
        Order a `list` of `TickerState` by priority.

        Priority:
          1) Open operation
          2) Not started operation
          3) Any

        Args
        ----------
        ticker_priority_list : `list`
            `list` of `TickerState`.

        Returns
        ----------
        `list` of `TickerState`.
            Ordered list.
        """
        # TODO: Improve slightly performance by using list.sort()
        new_list = sorted(ticker_priority_list, key=lambda ticker_state: \
            str(int(ticker_state.operation.state == State.OPEN
            if ticker_state.operation is not None else 0)) \
            + str(int(ticker_state.operation.state == State.NOT_STARTED
            if ticker_state.operation is not None else 0)), reverse=True)

        return new_list

    def save(self):
        self._db_strategy_model.insert_strategy_results(self._statistics_parameters,
            self.operations, self._statistics_graph)

    @RunTime('AndreMoraesStrategy.calculate_statistics')
    def calculate_statistics(self):
        """
        Calculate statistics.

        Capital (day),
        Capital in use (day),
        Tickers average (day),
        Ticker average annualized (day),
        IBOV (day),

        Profit,
        Maximum used capital,
        Volatility,
        Sharpe Ratio
        Yield,
        Annualized Yield,
        IBOV Yield,
        Annualized IBOV Yield,
        Average Tickers Yield,
        Annualized Average Tickers Yield.
        """
        try:
            self._calc_performance(days_batch=30)
            self._calc_statistics_params()
        except Exception as error:
            logger.exception(f"Error calculating statistics, error:\n{error}")
            sys.exit(c.PROCESSING_OPERATIONS_ERR)

    def _calc_performance(self, days_batch=30):
        """
        Calculate time domain performance indicators.

        Capital, capital in use, tickers average, IBOV.

        Set _statistics_graph dataframe with columns 'day', 'capital', 'capital_in_use',
        'tickers_average', 'ibov'.

        Args
        ----------
        days_batch : int, default 30
            Data chunk size when requesting to database.
        """
        statistics = pd.DataFrame(columns=['day', 'capital', 'capital_in_use',
            'tickers_average', 'ibov'])

        data_gen = self.DataGen(self.tickers_and_dates, self._db_strategy_model,
            days_batch=days_batch)
        close_prices = {key: [] for key in self.tickers_and_dates}
        last_price = 0.0
        dates = []
        while True:
            try:
                day_info, _ = next(data_gen)

                if not day_info.empty:
                    day = day_info.head(1)['day'].squeeze()
                    dates.append(day)

                    for ticker, tck_dates in self.tickers_and_dates.items():
                        if day >= tck_dates['start_date'] and day <= tck_dates['end_date']:
                            close_price = day_info[(day_info['ticker'] == ticker)] \
                                ['close_price'].squeeze()

                            if (not isinstance(close_price, pd.Series)) and \
                                (close_price is not None):
                                close_prices[ticker].append(close_price)
                                last_price = close_price
                            else:
                                close_prices[ticker].append(last_price)
                                logger.debug(f"Ticker \'{ticker}\' has no close_price for "
                                    f"day \'{day.strftime('%d/%m/%Y')}\'.")
                        else:
                            close_prices[ticker].append(np.nan)

            except StopIteration:
                break

        statistics['day'] = dates

        ibov_data = self._db_strategy_model.get_ticker_price('^BVSP', \
            pd.to_datetime(self.first_date), pd.to_datetime(self.last_date))

        # statistics['ibov'] = ibov_data.sort_values(by='day', axis=0, ascending=True, ignore_index=True)['close_price']
        statistics['ibov'] = ibov_data['close_price']

        statistics['tickers_average'] = AndreMoraesStrategy.tickers_yield(
            close_prices, precision=4)

        statistics['capital'], statistics['capital_in_use'] = self._calc_capital_usage(
            dates, close_prices)

        statistics.fillna(method='ffill', inplace=True)

        self._statistics_graph = statistics

    def _calc_statistics_params(self):

        money_precision = 2
        real_precision = 4

        # Profit
        last_capital_value = self._statistics_graph['capital'].tail(1).values[0]

        self._statistics_parameters['profit'] = \
            round(last_capital_value - self._total_capital, money_precision)

        # Maximum Capital Used
        self._statistics_parameters['max_used_capital'] = \
            round(max(self._statistics_graph['capital_in_use']), money_precision)

        # Yield
        self._statistics_parameters['yield'] = \
            round(self._statistics_parameters['profit'] / self._total_capital,
                real_precision)

        # Annualized Yield
        bus_day_count = len(self._statistics_graph)

        self._statistics_parameters['annualized_yield'] = round(
            calculate_yield_annualized(self._statistics_parameters['yield'],
                bus_day_count), real_precision)

        # IBOV Yield
        first_ibov_value = self._statistics_graph['ibov'].head(1).values[0]
        last_ibov_value = self._statistics_graph['ibov'].tail(1).values[0]
        ibov_yield = (last_ibov_value / first_ibov_value) - 1

        self._statistics_parameters['ibov_yield'] = round(ibov_yield, real_precision)

        # Annualized IBOV Yield
        self._statistics_parameters['annualized_ibov_yield'] = round(
            calculate_yield_annualized(self._statistics_parameters['ibov_yield'],
                bus_day_count), real_precision)

        # Average Tickers Yield
        # first_avr_tickers_value = self._statistics_graph['tickers_average'].head(1).values[0]
        last_avr_tickers_value = self._statistics_graph['tickers_average'].tail(1).squeeze()
        avr_tickers_yield = last_avr_tickers_value
        self._statistics_parameters['avr_tickers_yield'] = round(avr_tickers_yield,
            real_precision)

        # Annualized Average Tickers Yield
        self._statistics_parameters['annualized_avr_tickers_yield'] = round(
            calculate_yield_annualized(self._statistics_parameters['avr_tickers_yield'],
                bus_day_count), real_precision)

        # Volatility
        temp = self._statistics_graph['capital'] / self._statistics_graph['capital'][0] - 1
        self._statistics_parameters['volatility'] = round(temp.describe().loc[['std']].
            squeeze(), real_precision)

        # Sharpe Ration
        # Risk-free yield by CDI index
        cdi_df = self._db_strategy_model.get_cdi_index(min(self._initial_dates),
            max(self._final_dates))

        if self._statistics_parameters['volatility'] != 0.0:
            self._statistics_parameters['sharpe_ratio'] = round(
                (self._statistics_parameters['yield'] - (cdi_df['cumulative'].tail(1).squeeze() \
                - 1.0)) / self._statistics_parameters['volatility'], real_precision)
        else:
            self._statistics_parameters['sharpe_ratio'] = 0.0

    # TODO: Solve cumulative numeric error.
    @staticmethod
    def tickers_yield(close_prices, precision=4):
        """
        Calculate average tickers yield.

        If more than one ticker is available during an interval, the result yield
        during that interval is calculated by the simple average of those particular
        yields.

        Handle late start tickers.

        Args
        ----------
        close_prices : `dict` of `list`
            Tickers prices. Prices `list` must have the same length.
        precision : int, default 4
            Output final precision.

        Returns
        ----------
        `list` of float
            Average yield. Same length as prices.
        """
        # Get yield relative to previous day
        norm_prices = {ticker: [np.nan]*len(prices) for ticker, prices in close_prices.items()}
        for ticker in close_prices:
            first_price = 0
            for index, value in enumerate(close_prices[ticker]):
                first_price = np.float64(value)
                first_index = index
                if first_price is not np.nan:
                    break

            for index, price in enumerate(close_prices[ticker]):
                if index > first_index:
                    norm_prices[ticker][index] = price/close_prices[ticker][index-1] - 1
                    # norm_prices[ticker][index] = round(
                        # price / close_prices[ticker][index-1] - 1.0, precision+2)
                    # norm_prices[ticker][index] = round(np.float64(price) / \
                        # np.float64(close_prices[ticker][index-1]) - np.float64(1.0), precision+2)

        # Get average yield by integrating daily yields
        result_yield = []
        cumulative = np.float64(1.0)
        for index in range(len( norm_prices[tuple(norm_prices.keys())[0]] )):
            partial_sum = 0
            partial_weight = 0
            for ticker in norm_prices:
                if norm_prices[ticker][index] is not np.nan:
                    partial_sum += norm_prices[ticker][index]
                    partial_weight += 1
            partial_weight = partial_weight if partial_weight > 0 else 1

            cumulative *= round(1.0 + partial_sum / partial_weight, precision+2)
            # cumulative *= 1 + partial_sum / partial_weight
            result_yield.append(round(cumulative - 1, precision))

        return result_yield

    # TODO: Iterate over generator to remove self._day_df references .
    def _calc_capital_usage(self, dates, close_prices):
        """
        Calculate capital usage per day.

        Capital: total non-using money plus money in stocks.
        Capital in use: total ongoing purchase money.

        Args
        ----------
        dates: `list` of `pd.Timestamp`
            Dates.
        close_prices : `dict` of `list`
            Tickers prices. Prices `list` and `dates` must have the same length.

        Returns
        ----------
        `list` of float
            Capital.
        `list` of float
            Capital in use.
        """

        capital = [None] * len(dates)
        capital_in_use = [None] * len(dates)

        current_capital = self.total_capital
        current_capital_in_use = 0.0

        # Iterate over each day chronologically
        # for day_index, day in statistics['day'].iteritems():
        for day_index, day in enumerate(dates):

            holding_papers_capital = 0.0

            for oper in self._operations:
                # Compute purchases debts
                for p_price, p_volume, p_day in zip(oper.purchase_price, oper.purchase_volume, \
                    oper.purchase_datetime):
                    if day == p_day:
                        amount = round(p_price * p_volume, 2)

                        current_capital -= amount
                        current_capital_in_use += amount

                # Compute sale credits
                for s_price, s_volume, s_day in zip(oper.sale_price, oper.sale_volume, \
                    oper.sale_datetime):
                    if day == s_day:
                        amount = round(s_price * s_volume, 2)

                        current_capital += amount
                        current_capital_in_use -= amount

                #Compute holding papers prices
                if (oper.state == State.OPEN and day >= oper.start_date) or \
                    (oper.state == State.CLOSE and day >= oper.start_date and day < oper.end_date):

                    bought_volume = sum([p_volume for p_date, p_volume in \
                        zip(oper.purchase_datetime, oper.purchase_volume) if p_date <= day])
                    sold_volume = sum([s_volume for s_date, s_volume in \
                        zip(oper.sale_datetime, oper.sale_volume) if s_date <= day])
                    papers_in_hands = bought_volume - sold_volume

                    # if not self._day_df.loc[(self._day_df['day'] == day) & \
                    #     (self._day_df['ticker'] == oper.ticker)].empty:
                    #     price = self._day_df.loc[(self._day_df['day'] == day) & \
                    #         (self._day_df['ticker'] == oper.ticker)]['close_price'].head(1).values[0]
                    # # Set last price
                    # else:
                    #     last_day = statistics.iloc[day_index-1]['day']
                    #     price = self._day_df.loc[(self._day_df['day'] == last_day) & \
                    #         (self._day_df['ticker'] == oper.ticker)]['close_price'].head(1).values[0]

                    price = close_prices[oper.ticker][day_index]
                    holding_papers_capital += round(price * papers_in_hands, 2)

            capital[day_index] = round(current_capital + holding_papers_capital, 2)
            capital_in_use[day_index] = round(current_capital_in_use, 2)

        return capital, capital_in_use

    class TickerState:
        def __init__(self, ticker, initial_date, final_date, ongoing_operation_flag=False,
            partial_sale_flag=False, operation=None):
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

    # @RunTime('AndreMoraesStrategy.process_operations')
    # def process_operations(self):
    #     try:
    #         minimum_volume_batch = 1

    #         ticker_priority_list = [self.TickerState(ticker, self._initial_dates[self._tickers.index(ticker)],
    #             self._final_dates[self._tickers.index(ticker)]) for ticker in self._tickers]

    #         # Progress logging
    #         prog_percent_step = 0.05
    #         prog_last_update = prog_percent_step
    #         prog_days_interval = (max(self._day_df['day']) - min(self._day_df['day'])).days
    #         prog_start_date = min(self._day_df['day'])

    #         # Iterate over each day chronologically
    #         for _, day in self._day_df.sort_values(by=['day'], axis=0, kind='mergesort', ascending=True, ignore_index=True)['day'].drop_duplicates().iteritems():

    #             completion_percentage = (day - prog_start_date).days / prog_days_interval
    #             if completion_percentage >= prog_last_update:
    #                 logger.info(f"Processing operations: {prog_last_update * 100:.0f}%.")
    #                 prog_last_update += prog_percent_step

    #             # Some data will be modified during loop
    #             ticker_priority_list_copy = ticker_priority_list.copy()

    #             for index, ts in enumerate(ticker_priority_list_copy):

    #                 # This day must be in the user selected bounds for the ticker
    #                 if day >= ts.initial_date and day <= ts.final_date:

    #                     row =  self._day_df.loc[(self._day_df['day'] == day) & (self._day_df['ticker'] == ts.ticker)].squeeze()

    #                     if row.empty == False:
    #                         if (ts.ongoing_operation_flag == False) or (ts.operation is not None and ts.operation.state == State.NOT_STARTED):
    #                             # # Check price tendency in Main Graph Time
    #                             # if row['up_down_trend_status'] == 1:

    #                             # Check if price is next to one EMA in Main Graph Time
    #                             if row['close_price'] < max(row['ema_17'], row['ema_72']) * (1+self.price_to_emas_tolerance) and row['close_price'] > min(row['ema_17'], row['ema_72']) * (1-self.price_to_emas_tolerance):

    #                                 maj_graph_time_ema_72 = self._week_df.loc[(self._week_df['ticker'] == ts.ticker) & (self._week_df['week'] <= row['day'])].tail(2)['ema_72'].head(1).values[0]

    #                                 maj_graph_time_trend = self._week_df.loc[(self._week_df['ticker'] == ts.ticker) & (self._week_df['week'] <= row['day'])].tail(2)['up_down_trend_status'].head(1).values[0]

    #                                 # Check if price is greater than EMA_72 in Major Graph Time
    #                                 if row['close_price'] > maj_graph_time_ema_72 and maj_graph_time_trend == 1:

    #                                     # Purchase strategic filters satified, but it must exists a stop_loss reference
    #                                     if self._day_df.loc[(self._day_df['ticker'] == ts.ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == -1) & (self._day_df['min_price'] < row['close_price'])].empty == False:

    #                                         # Set purchase target price by identifying the last maximum peak
    #                                         if not self._day_df.loc[(self._day_df['ticker'] == ts.ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == 1) & (self._day_df['max_price'] > row['close_price'])].empty:
    #                                             purchase_target = self._day_df.loc[(self._day_df['ticker'] == ts.ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == 1) & (self._day_df['max_price'] > row['close_price'])].tail(1)['max_price'].values[0]
    #                                             purchase_target = round(purchase_target, 2)

    #                                         # If no peak has a maximum greater enough, choose the first past maximum
    #                                         elif not self._day_df.loc[(self._day_df['ticker'] == ts.ticker) & (self._day_df['day'] < row['day']) & (self._day_df['max_price'] > row['close_price'])].empty:
    #                                             purchase_target = self._day_df.loc[(self._day_df['ticker'] == ts.ticker) & (self._day_df['day'] < row['day']) & (self._day_df['max_price'] > row['close_price'])].tail(1)['max_price'].values[0]
    #                                             purchase_target = round(purchase_target, 2)

    #                                         # If no maximum peak before, choose current day close_price
    #                                         else:
    #                                             purchase_target = row['close_price']

    #                                         stop_loss = self._day_df.loc[(self._day_df['ticker'] == ts.ticker) & (self._day_df['day'] < row['day']) & (self._day_df['peak'] == -1) & (self._day_df['min_price'] < row['close_price'])].tail(1)['min_price'].values[0]

    #                                         purchase_target = round(row['close_price'] + (row['close_price']- stop_loss) * 3, 2)

    #                                         ticker_priority_list[index].operation = Operation(ts.ticker)
    #                                         ticker_priority_list[index].operation.target_purchase_price = purchase_target
    #                                         ticker_priority_list[index].operation.stop_loss = stop_loss
    #                                         ticker_priority_list[index].operation.target_sale_price = round(purchase_target + (purchase_target - stop_loss) * 3, 2)
    #                                         ticker_priority_list[index].operation.partial_sale_price = round(purchase_target + (purchase_target - stop_loss), 2)
    #                                         ticker_priority_list[index].ongoing_operation_flag = True

    #                         if ts.ongoing_operation_flag == True:

    #                             if ts.operation.state == State.NOT_STARTED:

    #                                 # Check if the target purchase price was hit
    #                                 if ts.operation.target_purchase_price >= row['min_price'] and ts.operation.target_purchase_price <= row['max_price']:

    #                                     available_money = self.available_capital - sum([ts.operation.total_purchase_capital - ts.operation.total_sale_capital for ts in ticker_priority_list if (ts.operation is not None and ts.operation.state == State.OPEN)])

    #                                     purchase_money = ts.operation.target_purchase_price * calculate_maximum_volume(ts.operation.target_purchase_price, self._get_capital_per_risk((ts.operation.target_purchase_price - ts.operation.stop_loss)/(ts.operation.target_purchase_price)), minimum_volume=minimum_volume_batch)

    #                                     # Check if there is enough money
    #                                     if available_money >= purchase_money:
    #                                         ticker_priority_list[index].operation.add_purchase(ts.operation.target_purchase_price, calculate_maximum_volume(ts.operation.target_purchase_price, self._get_capital_per_risk((ts.operation.target_purchase_price - ts.operation.stop_loss)/(ts.operation.target_purchase_price)), minimum_volume=1), row['day'])
    #                                     else:
    #                                         ticker_priority_list[index].operation.add_purchase(ts.operation.target_purchase_price, calculate_maximum_volume(ts.operation.target_purchase_price, available_money, minimum_volume=minimum_volume_batch), row['day'])

    #                             elif ts.operation.state == State.OPEN:

    #                                 # Check if the target STOP LOSS is hit
    #                                 if ts.operation.stop_loss >= row['min_price'] and ts.operation.stop_loss <= row['max_price']:
    #                                     ticker_priority_list[index].operation.add_sale(ts.operation.stop_loss, ts.operation.total_purchase_volume - ts.operation.total_sale_volume, row['day'], stop_loss_flag=True)

    #                                 # Check if the target STOP LOSS is skipped
    #                                 elif ts.operation.stop_loss > row['max_price']:
    #                                     ticker_priority_list[index].operation.add_sale(row['open_price'], ts.operation.total_purchase_volume - ts.operation.total_sale_volume, row['day'], stop_loss_flag=True)

    #                                 # After hitting the stol loss, the operation can be closed
    #                                 if ts.operation.state == State.OPEN:

    #                                     # Check if the PARTIAL SALE price is hit
    #                                     if ts.partial_sale_flag == False and ts.operation.partial_sale_price >= row['min_price'] and ts.operation.partial_sale_price <= row['max_price']:
    #                                         ticker_priority_list[index].operation.add_sale(ts.operation.partial_sale_price, math.ceil(ts.operation.purchase_volume[0] / 2), row['day'], partial_sale_flag=True)
    #                                         ticker_priority_list[index].partial_sale_flag = True

    #                                     # Check if the PARTIAL SALE price is skipped but not TARGET SALE
    #                                     elif ts.partial_sale_flag == False and ts.operation.partial_sale_price < row['min_price'] and ts.operation.target_sale_price > row['max_price']:
    #                                         ticker_priority_list[index].operation.add_sale(row['open_price'], math.ceil(ts.operation.purchase_volume[0] / 2), row['day'], partial_sale_flag=True)
    #                                         ticker_priority_list[index].partial_sale_flag = True

    #                                         # LOG skip cases

    #                                     # Check if the TARGET SALE price is hit
    #                                     if ts.operation.target_sale_price >= row['min_price'] and ts.operation.target_sale_price <= row['max_price']:
    #                                         ticker_priority_list[index].operation.add_sale(ts.operation.target_sale_price, ts.operation.total_purchase_volume - ts.operation.total_sale_volume, row['day'])

    #                                     # Check if the TARGET SALE price is skipped
    #                                     if ts.operation.target_sale_price < row['min_price']:
    #                                         ticker_priority_list[index].operation.add_sale(row['open_price'], ts.operation.total_purchase_volume - ts.operation.total_sale_volume, row['day'])

    #                             if ticker_priority_list[index].operation.state == State.CLOSE:
    #                                 self.operations.append(ticker_priority_list[index].operation)
    #                                 ticker_priority_list[index].operation = None
    #                                 ticker_priority_list[index].ongoing_operation_flag = False
    #                                 ticker_priority_list[index].partial_sale_flag = False

    #             ticker_priority_list = self._order_by_priority(ticker_priority_list)

    #         # Insert remaining open operations
    #         for ts in ticker_priority_list:
    #             if ts.operation is not None and ts.operation.state == State.OPEN:
    #                 self.operations.append(ts.operation)

    #     except Exception as error:
    #         logger.exception(f"Error processing operations, error:\n{error}")
    #         sys.exit(c.UPDATING_DB_ERR)