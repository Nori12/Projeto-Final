from pathlib import Path
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from datetime import timedelta
from pandas.tseries.offsets import BDay
import random
from abc import ABC, abstractmethod
import sys
import numpy as np
import math
import joblib
from scipy.interpolate import interp1d
import matplotlib.pyplot as plt
from scipy import stats

import constants as c
from utils import RunTime, calculate_maximum_volume, calculate_yield_annualized, \
    get_capital_per_risk, State, Trend, get_avg_index_of_first_burst_of_ones
from db_model import DBStrategyModel, DBGenericModel
from operation import Operation

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)


class PseudoStrategy(ABC):
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
    tickers_and_dates : `dict`
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
    def tickers_and_dates(self):
        pass

    @property
    @abstractmethod
    def total_capital(self):
        """float : Total available capital."""
        pass

    @property
    @abstractmethod
    def risk_capital_product(self):
        pass

    @risk_capital_product.setter
    @abstractmethod
    def risk_capital_product(self, risk_capital_product):
        pass

    @property
    @abstractmethod
    def purchase_margin(self):
        """float : Percentage margin aplied on target purchase price."""
        pass

    @purchase_margin.setter
    @abstractmethod
    def purchase_margin(self, purchase_margin):
        pass

    @property
    @abstractmethod
    def stop_margin(self):
        """float : Percentage margin aplied on stop loss price."""
        pass

    @stop_margin.setter
    @abstractmethod
    def stop_margin(self, stop_margin):
        pass

    @property
    @abstractmethod
    def operations(self):
        """`list` of `Operation` : List containing all registered operations, no matter its state."""
        pass

    @property
    def start_date(self):
        pass

    @property
    def end_date(self):
        pass

    @property
    @abstractmethod
    def max_days_per_operation(self):
        pass

    @max_days_per_operation.setter
    @abstractmethod
    def max_days_per_operation(self, max_days_per_operation):
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

    # ********* Auxiliary methods for 'process_operations' modularity *********

    @abstractmethod
    def _parse_data(self, ticker_name, initial_date, final_date, day_info, week_info,
        business_data):
        pass

    @abstractmethod
    def _set_operation_purchase(self, ticker_name, available_capital,
        tcks_priority, tck_idx, business_data):
        pass

    @abstractmethod
    def _check_business_rules(self, business_data, last_business_data):
        pass

    @abstractmethod
    def _sell_on_stop_hit(self, tcks_priority, tck_idx, business_data):
        pass

    @abstractmethod
    def _sell_on_target_hit(self, tcks_priority, tck_idx, business_data):
        pass

    @abstractmethod
    def _sell_on_timeout_hit(self, tcks_priority, tck_idx, business_data):
        pass

    @abstractmethod
    def _save_and_reset_closed_operation(self, tcks_priority, tck_idx):
        pass


class AdaptedAndreMoraesStrategy(PseudoStrategy):
    """

    Properties
    ----------
    min_risk : float
        Minimum risk per operation.
    max_risk : float
        Maximum risk per operation.
    purchase_margin : float
        Percentage margin aplied on target purchase price.
    stop_margin : float
        Percentage margin aplied on stop loss price.

    total_strategies : int
        Total Strategy's instanced.
    strategy_number : int
        Number of this instance.
    """

    total_strategies = 0
    strategy_number = 0

    def __init__(self, tickers, alias=None, comment=None, risk_capital_product=0.10,
        total_capital=100000, min_order_volume=1, partial_sale=False, ema_tolerance=0.01,
        min_risk=0.01, max_risk=0.15, purchase_margin=0.0, stop_margin=0.0,
        stop_type='normal', min_days_after_successful_operation=0,
        min_days_after_failure_operation=0, gain_loss_ratio=3, max_days_per_operation=90,
        tickers_bag='listed_first', tickers_number=0, total_strategies=0):

        if risk_capital_product < 0.0 or risk_capital_product > 1.0:
            logger.error(f"Parameter \'risk_reference\' must be in the interval [0, 1].")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if stop_type not in ['normal', 'staircase']:
            logger.error(f"Parameter \'stop_type\' must be ['normal', 'staircase'].")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if tickers_bag not in ['listed_first', 'random']:
            logger.error(f"Parameter \'tickers_bag\' must be ['listed_first', 'random'].")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        self._name = "Adapted Andre Moraes"
        self._alias = alias
        self._comment = comment
        self._risk_capital_product = risk_capital_product
        self._total_capital = total_capital
        self._min_order_volume = min_order_volume
        self._partial_sale = partial_sale
        self._ema_tolerance = ema_tolerance
        self._min_risk = min_risk
        self._max_risk = max_risk
        self._purchase_margin = purchase_margin
        self._stop_margin = stop_margin
        self._stop_type = stop_type
        self._min_days_after_successful_operation = min_days_after_successful_operation
        self._min_days_after_failure_operation = min_days_after_failure_operation
        self._gain_loss_ratio = gain_loss_ratio
        self._max_days_per_operation = max_days_per_operation
        self._tickers_bag = tickers_bag
        self._tickers_number = tickers_number


        self._operations = []
        self._start_date = None
        self._end_date = None

        self._tickers_and_dates = AdaptedAndreMoraesStrategy._filter_tickers(tickers,
            tickers_bag, tickers_number)

        # For DBStrategyModel compatibility and cdi index calculation
        only_tickers = []
        self._initial_dates = []
        self._final_dates = []

        for ticker, dates in self._tickers_and_dates.items():
            only_tickers.append(ticker)
            self._initial_dates.append(dates['start_date'])
            self._final_dates.append(dates['end_date'])

        self._db_strategy_model = DBStrategyModel(self._name, only_tickers, self._initial_dates,
            self._final_dates, alias=self._alias, comment=self._comment,
            risk_capital_product=self._risk_capital_product, total_capital=self._total_capital,
            min_order_volume=self._min_order_volume, partial_sale=self._partial_sale,
            ema_tolerance=self._ema_tolerance, min_risk=self._min_risk, max_risk=self._max_risk,
            purchase_margin=self._purchase_margin, stop_margin=self._stop_margin,
            stop_type=self._stop_type,
            min_days_after_successful_operation=self._min_days_after_successful_operation,
            min_days_after_failure_operation=self._min_days_after_failure_operation,
            gain_loss_ratio=self._gain_loss_ratio,
            max_days_per_operation=self._max_days_per_operation)
        self._db_generic_model = DBGenericModel()

        # For statistics calculations
        self.first_date = min(tickers.values(), key=lambda x: x['start_date'])['start_date']
        self.last_date = max(tickers.values(), key=lambda x: x['end_date'])['end_date']

        self._statistics_graph = None
        self._statistics_parameters = {'profit': None, 'max_used_capital': None, 'yield': None,
            'annualized_yield': None, 'ibov_yield': None, 'annualized_ibov_yield': None,
            'avr_tickers_yield': None, 'annualized_avr_tickers_yield': None, 'volatility': None,
            'sharpe_ratio': None}

        # TODO: Rever RCC
        tickers_rcc_path = Path(__file__).parent.parent/c.TICKERS_OPER_OPT_PATH
        self._tickers_rcc_df = pd.read_csv(tickers_rcc_path, sep=',')

        AdaptedAndreMoraesStrategy.total_strategies = total_strategies
        AdaptedAndreMoraesStrategy.strategy_number += 1
        self.strategy_number = AdaptedAndreMoraesStrategy.strategy_number


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
    def total_capital(self):
        return self._total_capital

    @property
    def min_order_volume(self):
        return self._min_order_volume

    @property
    def risk_capital_product(self):
        return self._risk_capital_product

    @risk_capital_product.setter
    def risk_capital_product(self, risk_capital_product):
        self._risk_capital_product = risk_capital_product
        self._db_strategy_model.risk_capital_product = risk_capital_product

    @property
    def purchase_margin(self):
        """float : Percentage margin aplied on target purchase price."""
        return self._purchase_margin

    @purchase_margin.setter
    def purchase_margin(self, purchase_margin):
        self._purchase_margin = purchase_margin
        self._db_strategy_model.purchase_margin = purchase_margin

    @property
    def stop_margin(self):
        """float : Percentage margin aplied on stop loss price."""
        return self._stop_margin

    @stop_margin.setter
    def stop_margin(self, stop_margin):
        self._stop_margin = stop_margin
        self._db_strategy_model.stop_margin = stop_margin

    @property
    def operations(self):
        return self._operations

    @property
    def ema_tolerance(self):
        return self._ema_tolerance

    @ema_tolerance.setter
    def ema_tolerance(self, ema_tolerance):
        self._ema_tolerance = ema_tolerance
        self._db_strategy_model.ema_tolerance = ema_tolerance

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

    @property
    def min_risk(self):
        return self._min_risk

    @min_risk.setter
    def min_risk(self, min_risk):
        self._min_risk = min_risk
        self._db_strategy_model.min_risk = min_risk

    @property
    def max_risk(self):
        return self._max_risk

    @max_risk.setter
    def max_risk(self, max_risk):
        self._max_risk = max_risk
        self._db_strategy_model.max_risk = max_risk

    @property
    def stop_type(self):
        return self._stop_type

    @stop_type.setter
    def stop_type(self, stop_type):
        self._stop_type = stop_type
        self._db_strategy_model.stop_type = stop_type

    @property
    def min_days_after_successful_operation(self):
        return self._min_days_after_successful_operation

    @min_days_after_successful_operation.setter
    def min_days_after_successful_operation(self, min_days_after_successful_operation):
        self._min_days_after_successful_operation = min_days_after_successful_operation
        self._db_strategy_model.min_days_after_successful_operation = min_days_after_successful_operation

    @property
    def min_days_after_failure_operation(self):
        return self._min_days_after_failure_operation

    @min_days_after_failure_operation.setter
    def min_days_after_failure_operation(self, min_days_after_failure_operation):
        self._min_days_after_failure_operation = min_days_after_failure_operation
        self._db_strategy_model.min_days_after_failure_operation = min_days_after_failure_operation

    @property
    def partial_sale(self):
        return self._partial_sale

    @partial_sale.setter
    def partial_sale(self, partial_sale):
        self._partial_sale = partial_sale
        self._db_strategy_model.partial_sale = partial_sale

    @property
    def gain_loss_ratio(self):
        return self._gain_loss_ratio

    @gain_loss_ratio.setter
    def gain_loss_ratio(self, gain_loss_ratio):
        self._gain_loss_ratio = gain_loss_ratio
        self._db_strategy_model.gain_loss_ratio = gain_loss_ratio

    @property
    def max_days_per_operation(self):
        return self._max_days_per_operation

    @max_days_per_operation.setter
    def max_days_per_operation(self, max_days_per_operation):
        self._max_days_per_operation = max_days_per_operation
        self._db_strategy_model.max_days_per_operation = max_days_per_operation

    @property
    def tickers_bag(self):
        return self._tickers_bag

    @property
    def tickers_number(self):
        return self._tickers_number


    @staticmethod
    def _filter_tickers(tickers_and_dates, tickers_bag, tickers_number):

        filteres_tickers_and_dates = {}

        if len(tickers_and_dates) > tickers_number > 0:
            if tickers_bag == 'listed_first':
                choosen_tickers = {ticker: value for idx, (ticker, value) in enumerate(tickers_and_dates.items()) if idx < tickers_number}
            elif tickers_bag == 'random':
                choosen_tickers = random.sample(list(tickers_and_dates), k=tickers_number)

            for ticker in tickers_and_dates:
                if ticker in choosen_tickers:
                    filteres_tickers_and_dates[ticker] = tickers_and_dates[ticker]
        else:
            return tickers_and_dates

        return filteres_tickers_and_dates

    @RunTime('process_operations')
    def process_operations(self, days_before_start=120):
        try:
            tcks_priority = [self.TickerState(ticker, dates['start_date'], dates['end_date'],
                min_days_after_suc_oper=self.min_days_after_successful_operation,
                min_days_after_fail_oper=self.min_days_after_failure_operation) \
                for ticker, dates in self.tickers_and_dates.items()]

            self._initialize_tcks_priority(tcks_priority)

            data_gen = self.DataGen(self.tickers_and_dates, self._db_strategy_model,
                days_batch=30, days_before_start=days_before_start)
            available_capital = self.total_capital

            ref_data = self._get_empty_ref_data()

            self._start_progress_bar(update_step=0.10)

            while True:
                try:
                    day_info, week_info = next(data_gen)

                    for index in range(len(tcks_priority)):

                        ticker_name = tcks_priority[index].ticker

                        business_data = self._get_empty_business_data()
                        data_validation_flag = False

                        data_validation_flag = self._parse_data(ticker_name,
                            tcks_priority[index].initial_date,
                            tcks_priority[index].final_date, day_info,
                            week_info, business_data)
                        if data_validation_flag is False:
                            tcks_priority[index].last_business_data = {}
                            continue

                        self._update_progress_bar(business_data["day"])

                        data_validation_flag = self._process_auxiliary_data(ticker_name,
                            tcks_priority, index, business_data, ref_data)
                        if data_validation_flag is False:
                            tcks_priority[index].last_business_data = business_data.copy()
                            continue

                        if business_data["day"] < tcks_priority[index].initial_date \
                            or business_data["day"] > tcks_priority[index].final_date:
                            tcks_priority[index].last_business_data = business_data.copy()
                            continue

                        if not self._check_operation_freezetime(tcks_priority, index):
                            tcks_priority[index].last_business_data = business_data.copy()
                            continue

                        # DEBUG
                        # if business_data["day"] == pd.Timestamp('2019-05-31'):
                        #     print()

                        if (tcks_priority[index].ongoing_operation_flag is False):

                            purchase_price = self._get_purchase_price(business_data)

                            # Strategy business rules
                            if self._check_business_rules(business_data, tcks_priority,
                                index, purchase_price):

                                stop_price = self._get_stop_price(ticker_name, purchase_price,
                                    business_data)

                                purchase_amount = self._set_operation_purchase(ticker_name,
                                    purchase_price, stop_price, available_capital, tcks_priority,
                                    index, business_data)
                                available_capital = round(available_capital - purchase_amount, 2)

                                if purchase_amount >= 0.01 and self.stop_type == "staircase":
                                    self._set_staircase_stop(tcks_priority, index)
                        else:
                            if tcks_priority[index].operation.state == State.OPEN:

                                # If hits the stop loss, the operation is automatically closed
                                sale_amount = self._sell_on_stop_hit(tcks_priority,
                                    index, business_data)
                                available_capital = round(available_capital + sale_amount, 2)

                                if tcks_priority[index].operation.state == State.OPEN:

                                    if self.partial_sale is True:
                                        sale_amount = self._sell_on_partial_hit(tcks_priority,
                                            index, business_data)
                                        available_capital = round(available_capital + sale_amount, 2)

                                    sale_amount = self._sell_on_target_hit(tcks_priority,
                                        index, business_data)
                                    available_capital = round(available_capital + sale_amount, 2)

                                    sale_amount = self._sell_on_timeout_hit(tcks_priority,
                                        index, business_data)
                                    available_capital = round(available_capital + sale_amount, 2)

                            # Update stop loss threshold
                            if tcks_priority[index].operation.state == State.OPEN \
                                and self.stop_type == "staircase":
                                self._update_staircase_stop(tcks_priority, index, business_data)

                            if tcks_priority[index].operation.state == State.CLOSE:
                                self._save_and_reset_closed_operation(tcks_priority, index)

                        tcks_priority[index].last_business_data = business_data.copy()

                    tcks_priority = AdaptedAndreMoraesStrategy._order_by_priority(tcks_priority)
                except StopIteration:
                    break

            # Insert remaining open operations
            for ts in tcks_priority:
                if ts.operation is not None and ts.operation.state == State.OPEN:
                    self.operations.append(ts.operation)

        except Exception as error:
            logger.exception(f"Error processing operations, error:\n{error}")
            sys.exit(c.PROCESSING_OPERATIONS_ERR)

    @RunTime('calculate_statistics')
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

    def save(self):
        self._db_strategy_model.insert_strategy_results(self._statistics_parameters,
            self.operations, self._statistics_graph)

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
            days_batch=days_batch, days_before_start=0)
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
                        if day.date() >= tck_dates['start_date'] and day <= tck_dates['end_date']:
                            close_price = day_info[(day_info['ticker'] == ticker)] \
                                ['close_price'].squeeze()

                            if (not isinstance(close_price, pd.Series)) and \
                                (close_price is not None):
                                close_prices[ticker].append(close_price)
                                last_price = close_price
                            else:
                                close_prices[ticker].append(last_price)
                                # logger.debug(f"Ticker \'{ticker}\' has no close_price for "
                                #     f"day \'{day.strftime('%d/%m/%Y')}\'.")
                        else:
                            close_prices[ticker].append(np.nan)

            except StopIteration:
                break

        ibov_data = self._db_strategy_model.get_ticker_price('^BVSP', \
            pd.to_datetime(self.first_date), pd.to_datetime(self.last_date))

        statistics['day'] = dates
        statistics['ibov'] = ibov_data['close_price']
        statistics['tickers_average'] = AdaptedAndreMoraesStrategy.tickers_yield(
            close_prices, precision=4)
        statistics['capital'], statistics['capital_in_use'] = self._calc_capital_usage(
            dates, close_prices)
        statistics['active_operations'] = self._get_number_of_operation_per_day(dates)
        statistics.fillna(method='ffill', inplace=True)

        self._statistics_graph = statistics

    def _calc_statistics_params(self):

        money_precision = 2
        real_precision = 4

        # Profit
        last_capital_value = self._statistics_graph['capital'].tail(1).values[0]

        self._statistics_parameters['profit'] = \
            round(last_capital_value - self._total_capital, money_precision)

        # Maximum Used Capital
        self._statistics_parameters['max_used_capital'] = \
            round(max(self._statistics_graph['capital_in_use']), real_precision)

        # Average Used Capital
        self._statistics_parameters['avg_used_capital'] = \
            round(np.average(self._statistics_graph['capital_in_use']), real_precision)

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

    def _get_number_of_operation_per_day(self, dates):
        """
        Calculate number of operations per day.

        Args
        ----------
        dates: `list` of `pd.Timestamp`
            Dates.

        Returns
        ----------
        `list` of int
            Operations per day.
        """
        active_operations = [0] * len(dates)

        for oper in self._operations:

            if oper.state == State.CLOSE:
                for d_index in [index for index, date in enumerate(dates) \
                    if date >= oper.start_date and date <= oper.end_date]:
                    active_operations[d_index] += 1
            elif (oper.state == State.OPEN):
                for d_index in [index for index, date in enumerate(dates) \
                    if date >= oper.start_date]:
                    active_operations[d_index] += 1

        return active_operations

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
                # Compute purchase debts
                for p_price, p_volume, p_day in zip(oper.purchase_price, oper.purchase_volume, \
                    oper.purchase_datetime):
                    if day == p_day:
                        amount = round(p_price * p_volume, 2)

                        current_capital = round(current_capital - amount, 2)
                        current_capital_in_use = round(current_capital_in_use + amount, 2)

                # Compute sale credits
                for s_price, s_volume, s_day in zip(oper.sale_price, oper.sale_volume, \
                    oper.sale_datetime):
                    if day == s_day:
                        amount = round(s_price * s_volume, 2)

                        current_capital = round(current_capital + amount, 2)
                        current_capital_in_use = round(current_capital_in_use - \
                            oper.purchase_price[0] * s_volume, 2)

                # Compute holding papers prices
                if (oper.state == State.OPEN and day >= oper.start_date) or \
                    (oper.state == State.CLOSE and day >= oper.start_date and day < oper.end_date):

                    bought_volume = sum([p_volume for p_date, p_volume in \
                        zip(oper.purchase_datetime, oper.purchase_volume) if p_date <= day])
                    sold_volume = sum([s_volume for s_date, s_volume in \
                        zip(oper.sale_datetime, oper.sale_volume) if s_date <= day])
                    papers_in_hands = bought_volume - sold_volume

                    price = close_prices[oper.ticker][day_index]
                    holding_papers_capital += round(price * papers_in_hands, 2)

            capital[day_index] = round(current_capital + holding_papers_capital, 2)
            capital_in_use[day_index] = round(current_capital_in_use / capital[day_index], 4)

        return capital, capital_in_use

    @staticmethod
    def _order_by_priority(tcks_priority):
        """
        Order a `list` of `TickerState` by priority.

        Priority:
          1) Open operation
          2) Not started operation
          3) Any

        Args
        ----------
        tcks_priority : `list`
            `list` of `TickerState`.

        Returns
        ----------
        `list` of `TickerState`.
            Ordered list.
        """
        # TODO: Improve slightly performance by using list.sort()
        new_list = sorted(tcks_priority, key=lambda ticker_state: \
            str(int(ticker_state.operation.state == State.OPEN
            if ticker_state.operation is not None else 0)) \
            + str(int(ticker_state.operation.state == State.NOT_STARTED
            if ticker_state.operation is not None else 0)), reverse=True)

        return new_list

    # Assumption: all tickers have the same length of the first one.
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
        initial_capital = 100000
        current_capital = initial_capital
        number_of_days = len(close_prices[tuple(close_prices.keys())[0]])
        volumes = {ticker: [np.nan]*len(prices) for ticker, prices in close_prices.items()}
        capital = []

        last_num_tickers = 0
        for day_index in range(number_of_days):

            num_tickers = 0
            lesser_price = {"ticker": "", "price": initial_capital}
            for ticker in close_prices:
                num_tickers += 1 if close_prices[ticker][day_index] is not np.nan else 0
                if close_prices[ticker][day_index] is not np.nan and \
                    close_prices[ticker][day_index] < lesser_price["price"]:
                    lesser_price["price"] = close_prices[ticker][day_index]
                    lesser_price["ticker"] = ticker

            if day_index == 0:
                amount_per_stock = round(initial_capital / num_tickers, 2)

                for ticker in close_prices:
                    if close_prices[ticker][day_index] is not np.nan:
                        volumes[ticker][day_index] = int(amount_per_stock // close_prices[ticker][day_index])
                        current_capital = round(current_capital - close_prices[ticker][day_index] * volumes[ticker][day_index], 2)

                if current_capital >= lesser_price["price"]:
                    bonus_volume = int(current_capital // close_prices[lesser_price["ticker"]][day_index])
                    volumes[lesser_price["ticker"]][day_index] += bonus_volume
                    current_capital = round(current_capital - close_prices[lesser_price["ticker"]][day_index] * bonus_volume, 2)

            elif num_tickers != last_num_tickers:
                # Sell everything by current close day price
                for ticker in close_prices:
                    if close_prices[ticker][day_index] is not np.nan and \
                        volumes[ticker][day_index-1] is not np.nan:
                        current_capital = round(current_capital + close_prices[ticker][day_index] * volumes[ticker][day_index-1], 2)

                # Now re-buy
                amount_per_stock = round(current_capital / num_tickers, 2)
                for ticker in close_prices:
                    if close_prices[ticker][day_index] is not np.nan:
                        volumes[ticker][day_index] = int(amount_per_stock // close_prices[ticker][day_index])
                        current_capital = round(current_capital - close_prices[ticker][day_index] * volumes[ticker][day_index], 2)

                if current_capital >= lesser_price["price"]:
                    bonus_volume = int(current_capital // close_prices[lesser_price["ticker"]][day_index])
                    volumes[lesser_price["ticker"]][day_index] += bonus_volume
                    current_capital = round(current_capital - close_prices[lesser_price["ticker"]][day_index] * bonus_volume, 2)
            else:
                for ticker in close_prices:
                    volumes[ticker][day_index] = volumes[ticker][day_index-1]

            last_num_tickers = num_tickers

            total_money = 0.0
            for ticker in close_prices:
                if close_prices[ticker][day_index] is not np.nan:
                    total_money += close_prices[ticker][day_index] * volumes[ticker][day_index]
            total_money += current_capital
            if day_index == 0:
                capital.append(0.0)
            else:
                capital.append(round(total_money/initial_capital - 1, precision))

        return capital

    class DataGen:
        def __init__(self, tickers, db_connection, days_batch=30, days_before_start=120):
            self.tickers = tickers
            self.first_date = min(self.tickers.values(), key=lambda x: x['start_date'])['start_date']
            self.first_date = self.first_date - BDay(days_before_start)
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
            return \
                self.daily_data[self.daily_data['day'] == self.dates[self.current_date_index-1]], \
                self.weekly_data[(self.weekly_data['week'].dt.isocalendar().year == year) \
                    & (self.weekly_data['week'].dt.isocalendar().week == week)] \
                    if not self.weekly_data.empty \
                    else self.weekly_data

    class TickerState:
        def __init__(self, ticker, initial_date, final_date, ongoing_operation_flag=False,
            partial_sale_flag=False, operation=None, min_days_after_suc_oper=0,
            min_days_after_fail_oper=0):
            self._ticker = ticker
            self._initial_date = initial_date
            self._final_date = final_date
            self._ongoing_operation_flag = ongoing_operation_flag
            self._partial_sale_flag = partial_sale_flag
            self._operation = operation
            self._mark_1_stop_trigger = None
            self._mark_1_stop_loss = None
            self._mark_2_stop_trigger = None
            self._mark_2_stop_loss = None
            self._current_mark = 0
            self._days_on_operation = 0
            self._days_after_suc_oper = min_days_after_suc_oper + 1
            self._days_after_fail_oper = min_days_after_fail_oper + 1

            # Must be dict if used
            self._last_business_data = {}

            # Must be dict if used
            self._extra_vars = {}

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

        @property
        def mark_1_stop_trigger(self):
            return self._mark_1_stop_trigger

        @mark_1_stop_trigger.setter
        def mark_1_stop_trigger(self, mark_1_stop_trigger):
            self._mark_1_stop_trigger = mark_1_stop_trigger

        @property
        def mark_1_stop_loss(self):
            return self._mark_1_stop_loss

        @mark_1_stop_loss.setter
        def mark_1_stop_loss(self, mark_1_stop_loss):
            self._mark_1_stop_loss = mark_1_stop_loss

        @property
        def mark_2_stop_trigger(self):
            return self._mark_2_stop_trigger

        @mark_2_stop_trigger.setter
        def mark_2_stop_trigger(self, mark_2_stop_trigger):
            self._mark_2_stop_trigger = mark_2_stop_trigger

        @property
        def mark_2_stop_loss(self):
            return self._mark_2_stop_loss

        @mark_2_stop_loss.setter
        def mark_2_stop_loss(self, mark_2_stop_loss):
            self._mark_2_stop_loss = mark_2_stop_loss

        @property
        def current_mark(self):
            return self._current_mark

        @current_mark.setter
        def current_mark(self, current_mark):
            self._current_mark = current_mark

        @property
        def days_on_operation(self):
            return self._days_on_operation

        @days_on_operation.setter
        def days_on_operation(self, days_on_operation):
            self._days_on_operation = days_on_operation

        @property
        def days_after_suc_oper(self):
            return self._days_after_suc_oper

        @days_after_suc_oper.setter
        def days_after_suc_oper(self, days_after_suc_oper):
            self._days_after_suc_oper = days_after_suc_oper

        @property
        def days_after_fail_oper(self):
            return self._days_after_fail_oper

        @days_after_fail_oper.setter
        def days_after_fail_oper(self, days_after_fail_oper):
            self._days_after_fail_oper = days_after_fail_oper

        @property
        def last_business_data(self):
            return self._last_business_data

        @last_business_data.setter
        def last_business_data(self, last_business_data):
            self._last_business_data = last_business_data.copy()

        @property
        def extra_vars(self):
            return self._extra_vars

        @extra_vars.setter
        def extra_vars(self, extra_vars):
            self._extra_vars = extra_vars.copy()

    def _start_progress_bar(self, update_step=0.05):
        self._update_step = update_step
        self._next_update_percent = update_step
        self._total_days = (self.last_date - self.first_date).days
        print(f"\nStrategy {self.name} ({AdaptedAndreMoraesStrategy.strategy_number} ", end='')

        if AdaptedAndreMoraesStrategy.total_strategies != 0:
            print(f"of {AdaptedAndreMoraesStrategy.total_strategies}", end='')

        print(f") - ", end='')

    def _update_progress_bar(self, current_day):
        completion_percentage = ((current_day.to_pydatetime().date() -
            self.first_date).days + 1) / self._total_days

        if completion_percentage + 1e-3 >= self._next_update_percent:

            if completion_percentage >= 0.999:
                print(f"{self._next_update_percent * 100:.0f}%.")
            else:
                print(f"{self._next_update_percent * 100:.0f}% ", end='')

            self._next_update_percent += self._update_step

    def _check_operation_freezetime(self, tcks_priority, tck_idx):
        tcks_priority[tck_idx].days_after_suc_oper += 1
        tcks_priority[tck_idx].days_after_fail_oper += 1

        if tcks_priority[tck_idx].days_after_suc_oper > \
                self.min_days_after_successful_operation \
            and tcks_priority[tck_idx].days_after_fail_oper > \
                self.min_days_after_failure_operation:
                return True

        return False

    def _initialize_tcks_priority(self, tcks_priority):
        for index in range(len(tcks_priority)):
            tcks_priority[index].days_on_operation = 0

    def _get_empty_ref_data(self):
        return {}

    def _process_auxiliary_data(self, ticker_name, tcks_priority,
        tck_idx, business_data, ref_data):

        if tcks_priority[tck_idx].ongoing_operation_flag and \
            tcks_priority[tck_idx].operation.state == State.OPEN:

            tcks_priority[tck_idx].days_on_operation += 1

        return True

    def _get_empty_business_data(self):
        business_data = {"day": None, "open_price_day": None, "max_price_day": None,
            "min_price_day": None, "close_price_day": None, "ema_17_day": None,
            "ema_72_day": None, "target_buy_price_day": None, "stop_loss_day": None,
            "up_down_trend_status_day": None, "peak_day": None, "ema_72_week": None}

        return business_data

    def _parse_data(self, ticker_name, initial_date, final_date, day_info, week_info,
        business_data):

        if day_info.empty or week_info.empty:
            return False

        day = day_info.head(1)['day'].squeeze()

        if day_info[(day_info['ticker'] == ticker_name)].empty:
            logger.info(f"Could not get day (\'{day.strftime('%Y-%m-%d')}\') " \
                f"for ticker \'{ticker_name}\'.")
            return False

        if week_info[(week_info['ticker'] == ticker_name)].empty:
            logger.info(f"Could not get last week for ticker \'{ticker_name}\' " \
                f"(week before day \'{day.strftime('%Y-%m-%d')}\').")
            return False

        open_price_day = day_info[(day_info['ticker'] == ticker_name)]['open_price'].squeeze()
        max_price_day = day_info[(day_info['ticker'] == ticker_name)]['max_price'].squeeze()
        min_price_day = day_info[(day_info['ticker'] == ticker_name)]['min_price'].squeeze()
        close_price_day = day_info[(day_info['ticker'] == ticker_name)]['close_price'].squeeze()
        ema_17_day = day_info[(day_info['ticker'] == ticker_name)]['ema_17'].squeeze()
        ema_72_day = day_info[(day_info['ticker'] == ticker_name)]['ema_72'].squeeze()
        target_buy_price_day = day_info[(day_info['ticker'] == ticker_name)]['target_buy_price'].squeeze()
        stop_loss_day = day_info[(day_info['ticker'] == ticker_name)]['stop_loss'].squeeze()
        up_down_trend_status_day = day_info[(day_info['ticker'] == ticker_name)]['up_down_trend_status'].squeeze()
        peak_day = day_info[(day_info['ticker'] == ticker_name)]['peak'].squeeze()

        ema_72_week = week_info[(week_info['ticker'] == ticker_name)]['ema_72'].squeeze()

        if isinstance(open_price_day, pd.Series) or \
            isinstance(max_price_day, pd.Series) or \
            isinstance(min_price_day, pd.Series) or \
            isinstance(close_price_day, pd.Series) or \
            isinstance(ema_17_day, pd.Series) or \
            isinstance(ema_72_day, pd.Series) or \
            isinstance(target_buy_price_day, pd.Series) or \
            isinstance(stop_loss_day, pd.Series) or \
            isinstance(up_down_trend_status_day, pd.Series) or \
            isinstance(peak_day, pd.Series) or \
            isinstance(ema_72_week, pd.Series):
            logger.warning(f"Ticker \'{ticker_name}\' has missing " \
                f"data for day \'{day.strftime('%Y-%m-%d')}\'.")
            return False

        target_buy_price_day = self._parse_target_buy_price(ticker_name, day, target_buy_price_day)
        stop_loss_day = self._parse_stop_loss(ticker_name, day, target_buy_price_day, stop_loss_day)

        if target_buy_price_day == 0.0 or stop_loss_day == 0.0:
            return False

        business_data["day"] = day
        business_data["open_price_day"] = open_price_day
        business_data["max_price_day"] = max_price_day
        business_data["min_price_day"] = min_price_day
        business_data["close_price_day"] = close_price_day
        business_data["ema_17_day"] = ema_17_day
        business_data["ema_72_day"] = ema_72_day
        business_data["target_buy_price_day"] = target_buy_price_day
        business_data["stop_loss_day"] = stop_loss_day
        business_data["up_down_trend_status_day"] = up_down_trend_status_day
        business_data["peak_day"] = peak_day
        business_data["ema_72_week"] = ema_72_week

        return True

    def _parse_target_buy_price(self, ticker_name, day, default_target_price):

        if default_target_price <= 0.0:
            logger.warning(f"Ticker satisfies all purchase conditions, " \
                f"but no target price or stop loss is set. (\'{ticker_name}\'" \
                f", \'{day.strftime('%Y-%m-%d')}\')")
            return 0.0

        target_buy_price_day = round(default_target_price * (1 + self.purchase_margin), 2)

        return target_buy_price_day

    def _parse_stop_loss(self, ticker_name, day, target_buy_price, default_stop_loss):

        if default_stop_loss <= 0.0:
            logger.warning(f"Ticker satisfies all purchase conditions, " \
                f"but no target price or stop loss is set. (\'{ticker_name}\'" \
                f", \'{day.strftime('%Y-%m-%d')}\')")
            return 0.0

        # stop_risk = self._tickers_rcc_df.loc[
        #     self._tickers_rcc_df['ticker'] == ticker_name, ['rcc']].squeeze()
        stop_risk = None

        stop_loss_day = 0.0

        if stop_risk is None or isinstance(stop_risk, pd.Series):
            # print(f"Stop loss empty for ticker \'{ts.ticker}\'")
            stop_loss_day = round(default_stop_loss * (1 + self.stop_margin), 2)
            if (target_buy_price - stop_loss_day) / target_buy_price > self.max_risk:
                stop_loss_day = round(target_buy_price * (1 - self.max_risk), 2)
            if (target_buy_price - stop_loss_day) / target_buy_price < self.min_risk:
                stop_loss_day = round(target_buy_price * (1 - self.min_risk), 2)
        else:
            stop_loss_day = round(target_buy_price * (1 - stop_risk), 2)

        return stop_loss_day

    def _get_purchase_price(self, business_data):
        return business_data["target_buy_price_day"]

    def _get_stop_price(self, ticker_name, purchase_price, business_data):
        return business_data["stop_loss_day"]

    def _set_operation_purchase(self, ticker_name, purchase_price, stop_price,
        available_capital, tcks_priority, tck_idx, business_data):

        amount_withdrawn = 0.0

        max_vol = calculate_maximum_volume(
            purchase_price, get_capital_per_risk(
            self.risk_capital_product, available_capital, \
            (purchase_price - stop_price)/ \
            (purchase_price)), minimum_volume=self.min_order_volume)

        max_purchase_money = round(purchase_price * max_vol, 2)

        # Check if there is enough money for whole purchase
        if available_capital >= max_purchase_money:

            tcks_priority[tck_idx].operation = Operation(ticker_name)
            tcks_priority[tck_idx].operation.target_purchase_price = \
                purchase_price
            tcks_priority[tck_idx].operation.stop_loss = stop_price
            tcks_priority[tck_idx].operation.target_sale_price = \
                round(purchase_price + (purchase_price - stop_price) * self.gain_loss_ratio, 2)
            tcks_priority[tck_idx].operation.partial_sale_price = \
                round(purchase_price + (purchase_price - stop_price), 2)

            amount_withdrawn = round(max_purchase_money, 2)
            tcks_priority[tck_idx].operation.add_purchase(
                purchase_price, max_vol, business_data["day"])
            tcks_priority[tck_idx].ongoing_operation_flag = True
        # Otherwise as much as possible
        else:
            max_vol = calculate_maximum_volume(purchase_price,
                available_capital, minimum_volume=self.min_order_volume)
            if max_vol >= 1.0:

                tcks_priority[tck_idx].operation = Operation(ticker_name)
                tcks_priority[tck_idx].operation.target_purchase_price = \
                    purchase_price
                tcks_priority[tck_idx].operation.stop_loss = stop_price
                tcks_priority[tck_idx].operation.target_sale_price = \
                    round(purchase_price + (purchase_price - stop_price) * self.gain_loss_ratio, 2)
                tcks_priority[tck_idx].operation.partial_sale_price = \
                    round(purchase_price + (purchase_price - stop_price), 2)

                amount_withdrawn = round(purchase_price * max_vol, 2)
                tcks_priority[tck_idx].operation.add_purchase(
                    purchase_price, max_vol, business_data["day"])
                tcks_priority[tck_idx].ongoing_operation_flag = True

        return amount_withdrawn

    def _check_business_rules(self, business_data, tcks_priority, tck_idx,
        purchase_price):

        if not tcks_priority[tck_idx].last_business_data:
            return False

        if (business_data["up_down_trend_status_day"] == Trend.UPTREND.value) \
            and (tcks_priority[tck_idx].last_business_data['min_price_day'] < \
                max(business_data["ema_17_day"], business_data["ema_72_day"]) * \
                (1+self.ema_tolerance) \
                and tcks_priority[tck_idx].last_business_data['max_price_day'] > \
                min(business_data["ema_17_day"], business_data["ema_72_day"]) * \
                (1-self.ema_tolerance)) \
            and (tcks_priority[tck_idx].last_business_data['close_price_day'] > \
                business_data["ema_72_week"]) \
            and (purchase_price >= business_data["min_price_day"] \
                and purchase_price <= business_data["max_price_day"]):
            return True

        return False

    def _set_staircase_stop(self, tcks_priority, tck_idx):

        purchase_price = tcks_priority[tck_idx].operation.target_purchase_price
        stop_price = tcks_priority[tck_idx].operation.stop_loss

        tcks_priority[tck_idx].mark_1_stop_trigger = \
            round(purchase_price + (purchase_price - stop_price), 2)
        tcks_priority[tck_idx].mark_1_stop_loss = purchase_price

        tcks_priority[tck_idx].mark_2_stop_trigger = \
            round(purchase_price + 2 * (purchase_price - stop_price), 2)
        tcks_priority[tck_idx].mark_2_stop_loss = \
            round(purchase_price + (purchase_price - stop_price) / 2, 2)
        tcks_priority[tck_idx].current_mark = 0

    def _update_staircase_stop(self, tcks_priority, tck_idx, business_data):
        if tcks_priority[tck_idx].current_mark == 0:
            if business_data["close_price_day"] >= tcks_priority[tck_idx].mark_1_stop_trigger:
                tcks_priority[tck_idx].operation.stop_loss = \
                    tcks_priority[tck_idx].mark_1_stop_loss
                tcks_priority[tck_idx].current_mark = 1
        elif tcks_priority[tck_idx].current_mark == 1:
            if business_data["close_price_day"] >= tcks_priority[tck_idx].mark_2_stop_trigger:
                tcks_priority[tck_idx].operation.stop_loss = \
                    tcks_priority[tck_idx].mark_2_stop_loss
                tcks_priority[tck_idx].current_mark = 2

    def _sell_on_stop_hit(self, tcks_priority, tck_idx, business_data):

        sale_amount = 0.0

        # Check if the target STOP LOSS is skipped
        if tcks_priority[tck_idx].operation.stop_loss > business_data["open_price_day"]:

            sale_amount = round(business_data["open_price_day"] * \
                    (tcks_priority[tck_idx].operation.total_purchase_volume - tcks_priority[tck_idx].operation.total_sale_volume), 2)
            day = business_data["day"]

            tcks_priority[tck_idx].operation.add_sale(business_data["open_price_day"],
                tcks_priority[tck_idx].operation.total_purchase_volume - tcks_priority[tck_idx].operation.total_sale_volume,
                business_data["day"], stop_loss_flag=True)

            tcks_priority[tck_idx].days_after_fail_oper = 0

            logger.debug(f"Stop loss skipped: \'{tcks_priority[tck_idx].ticker}\', "
                f"\'{day.strftime('%Y-%m-%d')}\'.")
        # Check if the target STOP LOSS is hit
        elif tcks_priority[tck_idx].operation.stop_loss >= business_data["min_price_day"] and \
            tcks_priority[tck_idx].operation.stop_loss <= business_data["max_price_day"]:

            sale_amount = round(tcks_priority[tck_idx].operation.stop_loss * \
                (tcks_priority[tck_idx].operation.total_purchase_volume - tcks_priority[tck_idx].operation.total_sale_volume), 2)

            tcks_priority[tck_idx].operation.add_sale(
                tcks_priority[tck_idx].operation.stop_loss, tcks_priority[tck_idx].operation.total_purchase_volume \
                - tcks_priority[tck_idx].operation.total_sale_volume, business_data["day"], stop_loss_flag=True)

            tcks_priority[tck_idx].days_after_fail_oper = 0

        return sale_amount

    def _sell_on_partial_hit(self, tcks_priority, tck_idx, business_data):

        sale_amount = 0.0

        # Check if the PARTIAL SALE price is skipped
        if tcks_priority[tck_idx].partial_sale_flag is False and \
            tcks_priority[tck_idx].operation.partial_sale_price < business_data["open_price_day"]:
            sale_amount = round(business_data["open_price_day"] * \
                math.ceil(tcks_priority[tck_idx].operation.purchase_volume[0] / 2), 2)
            tcks_priority[tck_idx].operation.add_sale(
                business_data["open_price_day"], math.ceil(tcks_priority[tck_idx].operation.purchase_volume[0] / 2),
                business_data["day"], partial_sale_flag=True)
            tcks_priority[tck_idx].partial_sale_flag = True
            day = business_data["day"]
            logger.debug(f"Partial sale skipped: \'{tcks_priority[tck_idx].ticker}\', "
                f"\'{day.strftime('%Y-%m-%d')}\'.")
        # Check if the PARTIAL SALE price is hit
        elif tcks_priority[tck_idx].partial_sale_flag is False and \
            tcks_priority[tck_idx].operation.partial_sale_price >= business_data["min_price_day"] and \
            tcks_priority[tck_idx].operation.partial_sale_price <= business_data["max_price_day"]:
            sale_amount = round(tcks_priority[tck_idx].operation.partial_sale_price * \
                math.ceil(tcks_priority[tck_idx].operation.purchase_volume[0] / 2), 2)
            tcks_priority[tck_idx].operation.add_sale(
                tcks_priority[tck_idx].operation.partial_sale_price, math.ceil(
                tcks_priority[tck_idx].operation.purchase_volume[0] / 2), business_data["day"], partial_sale_flag=True)
            tcks_priority[tck_idx].partial_sale_flag = True

        return sale_amount

    def _sell_on_target_hit(self, tcks_priority, tck_idx, business_data):

        sale_amount = 0.0

        # Check if the TARGET SALE price is skipped
        if tcks_priority[tck_idx].operation.target_sale_price < business_data["open_price_day"]:

            sale_amount = round(business_data["open_price_day"] * \
                (tcks_priority[tck_idx].operation.total_purchase_volume - \
                tcks_priority[tck_idx].operation.total_sale_volume), 2)

            tcks_priority[tck_idx].operation.add_sale(business_data["open_price_day"],
            tcks_priority[tck_idx].operation.total_purchase_volume - \
                tcks_priority[tck_idx].operation.total_sale_volume,
            business_data["day"])
            day = business_data["day"]

            tcks_priority[tck_idx].days_after_suc_oper = 0

            logger.debug(f"Target sale skipped: \'{tcks_priority[tck_idx].ticker}\', "
                f"\'{day.strftime('%Y-%m-%d')}\'.")

        # Check if the TARGET SALE price is hit
        elif tcks_priority[tck_idx].operation.target_sale_price >= business_data["min_price_day"] and \
            tcks_priority[tck_idx].operation.target_sale_price <= business_data["max_price_day"]:

            sale_amount = round(tcks_priority[tck_idx].operation.target_sale_price * \
                (tcks_priority[tck_idx].operation.total_purchase_volume - \
                tcks_priority[tck_idx].operation.total_sale_volume), 2)

            tcks_priority[tck_idx].operation.add_sale(
                tcks_priority[tck_idx].operation.target_sale_price,
                tcks_priority[tck_idx].operation.total_purchase_volume \
                - tcks_priority[tck_idx].operation.total_sale_volume, business_data["day"])

            tcks_priority[tck_idx].days_after_suc_oper = 0

        return sale_amount

    def _sell_on_timeout_hit(self, tcks_priority, tck_idx, business_data):

        sale_amount = 0.0

        # If expiration date arrives
        if tcks_priority[tck_idx].days_on_operation > self.max_days_per_operation:

            sale_amount = round(business_data["close_price_day"] * \
                (tcks_priority[tck_idx].operation.total_purchase_volume - \
                    tcks_priority[tck_idx].operation.total_sale_volume), 2)

            tcks_priority[tck_idx].operation.add_sale(
                business_data["close_price_day"],
                tcks_priority[tck_idx].operation.total_purchase_volume \
                - tcks_priority[tck_idx].operation.total_sale_volume, business_data["day"],
                timeout_flag=True)
            day = business_data["day"]

            tcks_priority[tck_idx].days_after_fail_oper = 0

            logger.debug(f"Operation time expired({self.max_days_per_operation} days): " \
                f"\'{tcks_priority[tck_idx].ticker}\', \'{day.strftime('%Y-%m-%d')}\'.")

        return sale_amount

    def _save_and_reset_closed_operation(self, tcks_priority, tck_idx):
        self.operations.append(tcks_priority[tck_idx].operation)
        tcks_priority[tck_idx].operation = None
        tcks_priority[tck_idx].ongoing_operation_flag = False
        tcks_priority[tck_idx].partial_sale_flag = False
        tcks_priority[tck_idx].days_on_operation = 0


class MLDerivationStrategy(AdaptedAndreMoraesStrategy):

    def __init__(self, tickers, alias=None, comment=None, risk_capital_product=0.10,
        total_capital=100000, min_order_volume=1, partial_sale=False, ema_tolerance=0.01,
        min_risk=0.01, max_risk=0.15, purchase_margin=0.0, stop_margin=0.0,
        stop_type='normal', min_days_after_successful_operation=0,
        min_days_after_failure_operation=0, gain_loss_ratio=3, max_days_per_operation=90,
        tickers_bag='listed_first', tickers_number=0, total_strategies=0):

        super().__init__(tickers, alias, comment, risk_capital_product, total_capital,
            min_order_volume, partial_sale, ema_tolerance, min_risk, max_risk,
            purchase_margin, stop_margin, stop_type, min_days_after_successful_operation,
            min_days_after_failure_operation, gain_loss_ratio, max_days_per_operation,
            tickers_bag, tickers_number, total_strategies)

        self._name = "ML Derivation"
        self._db_strategy_model.name = self._name
        self._models = {}

        self._step_range_risk = 0.002
        self.risks = tuple(round(i, 3) for i in tuple(np.arange(self.min_risk,
                self.max_risk + self._step_range_risk, self._step_range_risk)))

        tickers_info_path = Path(__file__).parent / 'tickers_info.csv'
        ticker_datasets_path = Path(__file__).parent.parent / 'machine_learning' / 'datasets'
        ticker_dataset_suffix = '_dataset.csv'

        self._tickers_info = {}
        self._load_most_effective_risk_per_ticker(tickers_info_path, ticker_datasets_path,
                    ticker_dataset_suffix)


    @property
    def models(self):
        return self._models

    @property
    def min_risk(self):
        return self._min_risk

    @min_risk.setter
    def min_risk(self, min_risk):
        self._min_risk = min_risk
        self._db_strategy_model.min_risk = min_risk
        self.risks = tuple(round(i, 3) for i in tuple(np.arange(min_risk,
            self.max_risk + self._step_range_risk, self._step_range_risk)))

    @property
    def max_risk(self):
        return self._max_risk

    @max_risk.setter
    def max_risk(self, max_risk):
        self._max_risk = max_risk
        self._db_strategy_model.max_risk = max_risk
        self.risks = tuple(round(i, 3) for i in tuple(np.arange(self.min_risk,
            max_risk + self._step_range_risk, self._step_range_risk)))

    @property
    def tickers_info(self):
        return self._tickers_info

    @tickers_info.setter
    def tickers_info(self, tickers_info):
        self._tickers_info = tickers_info

    def load_models(self):

        for _, (ticker, _) in enumerate(self.tickers_and_dates.items()):
            self._models[ticker] = joblib.load(Path(__file__).parent.parent /
                c.MODELS_PATH / (ticker + c.MODEL_SUFFIX))

    def _load_most_effective_risk_per_ticker(self, tickers_info_path, ticker_datasets_path,
        ticker_dataset_suffix):

        columns = ['ticker', 'start_date', 'end_date', 'most_effective_risk']
        non_counted_holidays_tolerance = 5
        ticker_info_interval_days = 365 * 2

        update_file_data = {'ticker': [], 'start_date': [], 'end_date': [],
            'most_effective_risk': []}
        tck_info_df = None
        most_effective_risk = None

        if tickers_info_path.exists():

            tck_info_df = pd.read_csv(tickers_info_path, sep=",", usecols=columns)

            if tck_info_df.empty:
                tck_info_df = None

        for ticker, dates in self.tickers_and_dates.items():

            ticker_info_end_date = dates['end_date'] - BDay(
                self.max_days_per_operation + non_counted_holidays_tolerance)
            ticker_info_start_date = ticker_info_end_date - timedelta(
                days=ticker_info_interval_days)

            if tck_info_df is not None:
                most_effective_risk = tck_info_df.loc[(tck_info_df['ticker'] == ticker) & \
                    (tck_info_df['start_date'] == ticker_info_start_date.strftime('%Y-%m-%d')) & \
                    (tck_info_df['end_date'] == ticker_info_end_date.strftime('%Y-%m-%d')),
                    ['most_effective_risk']]

                if most_effective_risk.empty:
                    most_effective_risk = None
                else:
                    most_effective_risk = most_effective_risk.squeeze()

            if most_effective_risk is None:

                most_effective_risk = self._get_most_effective_risk_from_dataset(
                    ticker, ticker_info_start_date, ticker_info_end_date,
                    ticker_datasets_path, ticker_dataset_suffix, kind='peak')

                update_file_data['ticker'].append(ticker)
                update_file_data['start_date'].append(ticker_info_start_date.strftime('%Y-%m-%d'))
                update_file_data['end_date'].append(ticker_info_end_date.strftime('%Y-%m-%d'))
                update_file_data['most_effective_risk'].append(most_effective_risk)

            self.tickers_info[ticker] = most_effective_risk
            most_effective_risk = None

        if update_file_data['ticker']:
            update_df = pd.DataFrame(update_file_data)

            if tck_info_df is not None:
                update_df = pd.concat([tck_info_df, update_df])

            update_df.to_csv(tickers_info_path, mode='w', index=False, header=True)

    def _get_most_effective_risk_from_dataset(self, ticker, start_date, end_date,
        ticker_datasets_path, ticker_dataset_suffix, kind='peak'):

        if kind not in ['peak', 'average']:
            logger.error("\'kind\' parameter must be in ['peak', 'average']")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if kind == 'average':
            risk_avg = 0.0

        columns = ['ticker', 'day', 'risk', 'success_oper_flag', 'timeout_flag',
            'end_of_interval_flag']

        # Open dataset file
        dataset_path = ticker_datasets_path / (ticker+ticker_dataset_suffix)
        dataset_df_gen = pd.read_csv(dataset_path, sep=",", usecols=columns, chunksize=10000)

        dataset_df = pd.concat((x.query(f"ticker == '{ticker}' and " \
            f"day >= '{start_date.strftime('%Y-%m-%d')}' and " \
            f"day <= '{end_date.strftime('%Y-%m-%d')}'")
            for x in dataset_df_gen), ignore_index=True)

        # Create risk list from dataset
        first_date = dataset_df['day'].head(1).squeeze()
        risks = dataset_df.loc[dataset_df['day'] == first_date, ['risk']].squeeze().tolist()
        risks.sort()

        # Create histogram of successful operations risks
        risks_count = [] * len(risks)

        for risk in risks:
            risks_count.append(len(dataset_df.loc[(dataset_df['risk'] == risk) & \
                (dataset_df['success_oper_flag'] == 1) & \
                (dataset_df['timeout_flag'] == 0) & \
                (dataset_df['end_of_interval_flag'] == 0), ['risk']]))

            if kind == 'average':
                risk_avg += risk * risks_count[-1]

        if kind == 'average':
            risk_avg /= sum(risks_count)

        if kind == 'peak':
            f = interp1d(risks, risks_count, kind='cubic')
            x = np.linspace(risks[0], risks[-1], num=len(risks)*4)
            risk_peak = x[np.argmax(f(x))]

            # Debug
            # if ticker == 'ALPA4':
            #     plt.subplot(1, 1, 1)
            #     plt.plot(risks, risks_count, 'o', color='b')
            #     plt.plot(x, f(x), color='r')

            return round(risk_peak, 4)

        return round(risk_avg, 4)

    def _initialize_tcks_priority(self, tcks_priority):

        super()._initialize_tcks_priority(tcks_priority)

        for index in range(len(tcks_priority)):
            tcks_priority[index].extra_vars["current_max_delay"] = 0
            tcks_priority[index].extra_vars["current_min_delay"] = 0
            tcks_priority[index].extra_vars["upcoming_max_peak"] = 0.0
            tcks_priority[index].extra_vars["upcoming_min_peak"] = 0.0
            tcks_priority[index].extra_vars["last_max_peaks"] = []
            tcks_priority[index].extra_vars["last_max_peaks_days"] = []
            tcks_priority[index].extra_vars["last_min_peaks"] = []
            tcks_priority[index].extra_vars["last_min_peaks_days"] = []

    def _get_empty_ref_data(self):

        constants_dict = super()._get_empty_ref_data()

        constants_dict['peaks_number'] = 2
        constants_dict['peak_delay'] = 9

        return constants_dict

    def _process_auxiliary_data(self, ticker_name, tcks_priority,
        tck_idx, business_data, ref_data):

        data_validation_flag = super()._process_auxiliary_data(ticker_name,
            tcks_priority, tck_idx, business_data, ref_data)

        if data_validation_flag is False:
            return False

        MLDerivationStrategy._update_peaks_days(tcks_priority[tck_idx])

        # Peak analysis: Put first peaks in buffer
        tcks_priority[tck_idx].extra_vars['current_max_delay'] += 1
        tcks_priority[tck_idx].extra_vars['current_min_delay'] += 1

        if business_data['peak_day'] > 0.01:
            # Bug treatment 'if' statement
            if business_data['max_price_day'] == business_data['min_price_day']:
                # Choose the an alternating peak type
                # Lesser means most recent added, so now is the time for the other peak type
                if tcks_priority[tck_idx].extra_vars['current_max_delay'] < \
                    tcks_priority[tck_idx].extra_vars['current_min_delay']:

                    tcks_priority[tck_idx].extra_vars['upcoming_min_peak'] = business_data['peak_day']
                    tcks_priority[tck_idx].extra_vars['current_min_delay'] = 0
                else:
                    tcks_priority[tck_idx].extra_vars['upcoming_max_peak'] = business_data['peak_day']
                    tcks_priority[tck_idx].extra_vars['current_max_delay'] = 0
            elif business_data['max_price_day'] != business_data['min_price_day']:
                if business_data['peak_day'] == business_data['max_price_day']:
                    tcks_priority[tck_idx].extra_vars['upcoming_max_peak'] = \
                        business_data['peak_day']
                    tcks_priority[tck_idx].extra_vars['current_max_delay'] = 0
                else:
                    tcks_priority[tck_idx].extra_vars['upcoming_min_peak'] = business_data['peak_day']
                    tcks_priority[tck_idx].extra_vars['current_min_delay'] = 0

        if tcks_priority[tck_idx].extra_vars['current_max_delay'] >= \
            ref_data['peak_delay'] and tcks_priority[tck_idx].extra_vars['upcoming_max_peak'] != 0.0:

            tcks_priority[tck_idx].extra_vars['last_max_peaks'].append(tcks_priority[tck_idx].extra_vars['upcoming_max_peak'])
            tcks_priority[tck_idx].extra_vars['last_max_peaks_days'].append(-tcks_priority[tck_idx].extra_vars['current_max_delay'])

            if len(tcks_priority[tck_idx].extra_vars['last_max_peaks']) > ref_data['peaks_number']:
                tcks_priority[tck_idx].extra_vars['last_max_peaks'].pop(0)
                tcks_priority[tck_idx].extra_vars['last_max_peaks_days'].pop(0)

            tcks_priority[tck_idx].extra_vars['upcoming_max_peak'] = 0.0

        if tcks_priority[tck_idx].extra_vars['current_min_delay'] >= \
            ref_data['peak_delay'] and tcks_priority[tck_idx].extra_vars['upcoming_min_peak'] != 0.0:

            tcks_priority[tck_idx].extra_vars['last_min_peaks'].append(tcks_priority[tck_idx].extra_vars['upcoming_min_peak'])
            tcks_priority[tck_idx].extra_vars['last_min_peaks_days'].append(-tcks_priority[tck_idx].extra_vars['current_min_delay'])

            if len(tcks_priority[tck_idx].extra_vars['last_min_peaks']) > ref_data['peaks_number']:
                tcks_priority[tck_idx].extra_vars['last_min_peaks'].pop(0)
                tcks_priority[tck_idx].extra_vars['last_min_peaks_days'].pop(0)

            tcks_priority[tck_idx].extra_vars['upcoming_min_peak'] = 0.0
        # END-> Peak analysis: Put first peaks in buffer

        if len(tcks_priority[tck_idx].extra_vars['last_max_peaks']) < ref_data['peaks_number'] \
            or len(tcks_priority[tck_idx].extra_vars['last_min_peaks']) < ref_data['peaks_number']:
            return False

    @staticmethod
    def _update_peaks_days(ticker_state):

        for idx in range(len(ticker_state.extra_vars['last_max_peaks_days'])):
            ticker_state.extra_vars['last_max_peaks_days'][idx] -= 1

        for idx in range(len(ticker_state.extra_vars['last_min_peaks_days'])):
            ticker_state.extra_vars['last_min_peaks_days'][idx] -= 1

    def _get_purchase_price(self, business_data):
        return business_data['open_price_day']

    def _check_business_rules(self, business_data, tcks_priority, tck_idx,
        purchase_price):

        if not tcks_priority[tck_idx].last_business_data:
            return False

        ref_price = purchase_price

        # More negative day number means older
        order = 'max_first' \
            if tcks_priority[tck_idx].extra_vars['last_max_peaks_days'][0] < \
                tcks_priority[tck_idx].extra_vars['last_min_peaks_days'][0] \
            else 'min_first'

        if order == 'max_first':
            peak_1 = round(tcks_priority[tck_idx].extra_vars['last_max_peaks'][0] / ref_price, 4)
            day_1 = tcks_priority[tck_idx].extra_vars['last_max_peaks_days'][0]
            peak_2 = round(tcks_priority[tck_idx].extra_vars['last_min_peaks'][0] / ref_price, 4)
            day_2 = tcks_priority[tck_idx].extra_vars['last_min_peaks_days'][0]
            peak_3 = round(tcks_priority[tck_idx].extra_vars['last_max_peaks'][1] / ref_price, 4)
            day_3 = tcks_priority[tck_idx].extra_vars['last_max_peaks_days'][1]
            peak_4 = round(tcks_priority[tck_idx].extra_vars['last_min_peaks'][1] / ref_price, 4)
            day_4 = tcks_priority[tck_idx].extra_vars['last_min_peaks_days'][1]
        elif order == 'min_first':
            peak_1 = round(tcks_priority[tck_idx].extra_vars['last_min_peaks'][0] / ref_price, 4)
            day_1 = tcks_priority[tck_idx].extra_vars['last_min_peaks_days'][0]
            peak_2 = round(tcks_priority[tck_idx].extra_vars['last_max_peaks'][0] / ref_price, 4)
            day_2 = tcks_priority[tck_idx].extra_vars['last_max_peaks_days'][0]
            peak_3 = round(tcks_priority[tck_idx].extra_vars['last_min_peaks'][1] / ref_price, 4)
            day_3 = tcks_priority[tck_idx].extra_vars['last_min_peaks_days'][1]
            peak_4 = round(tcks_priority[tck_idx].extra_vars['last_max_peaks'][1] / ref_price, 4)
            day_4 = tcks_priority[tck_idx].extra_vars['last_max_peaks_days'][1]

        ema_17_day = round(tcks_priority[tck_idx].last_business_data['ema_17_day'] / ref_price, 4)
        ema_72_day = round(tcks_priority[tck_idx].last_business_data['ema_72_day'] / ref_price, 4)
        ema_72_week = round(tcks_priority[tck_idx].last_business_data['ema_72_week'] / ref_price, 4)

        # X_test = [[risk, peak_1, day_1, peak_2, day_2, peak_3, day_3, peak_4, day_4,
        #     ema_17_day, ema_72_day, ema_72_week] for risk in self.risks]
        # predictions = self.models[tcks_priority[tck_idx].ticker].predict(X_test)

        risk = self.tickers_info[tcks_priority[tck_idx].ticker]

        X_test = [[risk, peak_1, day_1, peak_2, day_2, peak_3, day_3, peak_4, day_4,
            ema_17_day, ema_72_day, ema_72_week]]

        prediction = self.models[tcks_priority[tck_idx].ticker].predict(X_test)

        if prediction == 1:
            # risk = self.risks[get_avg_index_of_first_burst_of_ones(predictions)]
            # business_data['stop_loss_day'] = round(purchase_price * (1 - risk), 2)
            business_data['stop_loss_day'] = round(purchase_price * (1 - risk), 2)
            return True

        return False


class BaselineStrategy(AdaptedAndreMoraesStrategy):

    def __init__(self, tickers, alias=None, comment=None, risk_capital_product=0.10,
        total_capital=100000, min_order_volume=1, partial_sale=False, ema_tolerance=0.01,
        min_risk=0.01, max_risk=0.15, purchase_margin=0.0, stop_margin=0.0,
        stop_type='normal', min_days_after_successful_operation=0,
        min_days_after_failure_operation=0, gain_loss_ratio=3, max_days_per_operation=90,
        tickers_bag='listed_first', tickers_number=0, min_operation_decision_coefficient=0,
        total_strategies=0):

        super().__init__(tickers, alias, comment, risk_capital_product, total_capital,
            min_order_volume, partial_sale, ema_tolerance, min_risk, max_risk,
            purchase_margin, stop_margin, stop_type, min_days_after_successful_operation,
            min_days_after_failure_operation, gain_loss_ratio, max_days_per_operation,
            tickers_bag, tickers_number, total_strategies)

        self._name = 'Baseline'
        self._db_strategy_model.name = self._name
        self._min_operation_decision_coefficient = min_operation_decision_coefficient
        self._db_strategy_model.min_baseline_coefficient = min_operation_decision_coefficient
        self._tickers_info = {}
        self._last_n_close_prices = {ticker: {'3': [], '17': [], '72': []} for ticker in tickers}

        tickers_info_path = Path(__file__).parent / 'tickers_info.csv'
        ticker_datasets_path = Path(__file__).parent.parent / 'machine_learning' / 'datasets'
        ticker_dataset_suffix = '_dataset.csv'
        self._load_most_effective_risk_per_ticker(tickers_info_path, ticker_datasets_path,
            ticker_dataset_suffix)

    @property
    def tickers_info(self):
        return self._tickers_info

    @tickers_info.setter
    def tickers_info(self, tickers_info):
        self._tickers_info = tickers_info

    @property
    def last_n_close_prices(self):
        return self._last_n_close_prices

    @last_n_close_prices.setter
    def last_n_close_prices(self, last_n_close_prices):
        self._last_n_close_prices = last_n_close_prices

    @property
    def min_operation_decision_coefficient(self):
        return self._min_operation_decision_coefficient

    @min_operation_decision_coefficient.setter
    def min_operation_decision_coefficient(self, min_operation_decision_coefficient):
        self._min_operation_decision_coefficient = min_operation_decision_coefficient

    def _load_most_effective_risk_per_ticker(self, tickers_info_path, ticker_datasets_path,
        ticker_dataset_suffix):

        columns = ['ticker', 'start_date', 'end_date', 'most_effective_risk']
        non_counted_holidays_tolerance = 5
        ticker_info_interval_days = 365 * 2

        update_file_data = {'ticker': [], 'start_date': [], 'end_date': [],
            'most_effective_risk': []}
        tck_info_df = None
        most_effective_risk = None

        if tickers_info_path.exists():

            tck_info_df = pd.read_csv(tickers_info_path, sep=",", usecols=columns)

            if tck_info_df.empty:
                tck_info_df = None

        for ticker, dates in self.tickers_and_dates.items():

            ticker_info_end_date = dates['end_date'] - BDay(
                self.max_days_per_operation + non_counted_holidays_tolerance)
            ticker_info_start_date = ticker_info_end_date - timedelta(
                days=ticker_info_interval_days)

            if tck_info_df is not None:
                most_effective_risk = tck_info_df.loc[(tck_info_df['ticker'] == ticker) & \
                    (tck_info_df['start_date'] == ticker_info_start_date.strftime('%Y-%m-%d')) & \
                    (tck_info_df['end_date'] == ticker_info_end_date.strftime('%Y-%m-%d')),
                    ['most_effective_risk']]

                if most_effective_risk.empty:
                    most_effective_risk = None
                else:
                    most_effective_risk = most_effective_risk.squeeze()

            if most_effective_risk is None:

                most_effective_risk = self._get_most_effective_risk_from_dataset(
                    ticker, ticker_info_start_date, ticker_info_end_date,
                    ticker_datasets_path, ticker_dataset_suffix, kind='peak')

                update_file_data['ticker'].append(ticker)
                update_file_data['start_date'].append(ticker_info_start_date.strftime('%Y-%m-%d'))
                update_file_data['end_date'].append(ticker_info_end_date.strftime('%Y-%m-%d'))
                update_file_data['most_effective_risk'].append(most_effective_risk)

            self.tickers_info[ticker] = most_effective_risk
            most_effective_risk = None

        if update_file_data['ticker']:
            update_df = pd.DataFrame(update_file_data)

            if tck_info_df is not None:
                update_df = pd.concat([tck_info_df, update_df])

            update_df.to_csv(tickers_info_path, mode='w', index=False, header=True)

    def _get_most_effective_risk_from_dataset(self, ticker, start_date, end_date,
        ticker_datasets_path, ticker_dataset_suffix, kind='peak'):

        if kind not in ['peak', 'average']:
            logger.error("\'kind\' parameter must be in ['peak', 'average']")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if kind == 'average':
            risk_avg = 0.0

        columns = ['ticker', 'day', 'risk', 'success_oper_flag', 'timeout_flag',
            'end_of_interval_flag']

        # Open dataset file
        dataset_path = ticker_datasets_path / (ticker+ticker_dataset_suffix)
        dataset_df_gen = pd.read_csv(dataset_path, sep=",", usecols=columns, chunksize=10000)

        dataset_df = pd.concat((x.query(f"ticker == '{ticker}' and " \
            f"day >= '{start_date.strftime('%Y-%m-%d')}' and " \
            f"day <= '{end_date.strftime('%Y-%m-%d')}'")
            for x in dataset_df_gen), ignore_index=True)

        # Create risk list from dataset
        first_date = dataset_df['day'].head(1).squeeze()
        risks = dataset_df.loc[dataset_df['day'] == first_date, ['risk']].squeeze().tolist()
        risks.sort()

        # Create histogram of successful operations risks
        risks_count = [] * len(risks)

        for risk in risks:
            risks_count.append(len(dataset_df.loc[(dataset_df['risk'] == risk) & \
                (dataset_df['success_oper_flag'] == 1) & \
                (dataset_df['timeout_flag'] == 0) & \
                (dataset_df['end_of_interval_flag'] == 0), ['risk']]))

            if kind == 'average':
                risk_avg += risk * risks_count[-1]

        if kind == 'average':
            risk_avg /= sum(risks_count)

        if kind == 'peak':
            f = interp1d(risks, risks_count, kind='cubic')
            x = np.linspace(risks[0], risks[-1], num=len(risks)*4)
            risk_peak = x[np.argmax(f(x))]

            # Debug
            # if ticker == 'ALPA4':
            #     plt.subplot(1, 1, 1)
            #     plt.plot(risks, risks_count, 'o', color='b')
            #     plt.plot(x, f(x), color='r')

            return round(risk_peak, 4)

        return round(risk_avg, 4)

    def _get_purchase_price(self, business_data):
        return business_data['open_price_day']

    def _process_auxiliary_data(self, ticker_name, tcks_priority,
        tck_idx, business_data, ref_data):

        data_validation_flag = super()._process_auxiliary_data(ticker_name,
            tcks_priority, tck_idx, business_data, ref_data)

        if data_validation_flag is False:
            return False

        if not tcks_priority[tck_idx].last_business_data:
            return False

        self.last_n_close_prices[tcks_priority[tck_idx].ticker]['3'].append(
            tcks_priority[tck_idx].last_business_data['close_price_day'])

        self.last_n_close_prices[tcks_priority[tck_idx].ticker]['17'].append(
            tcks_priority[tck_idx].last_business_data['close_price_day'])

        self.last_n_close_prices[tcks_priority[tck_idx].ticker]['72'].append(
            tcks_priority[tck_idx].last_business_data['close_price_day'])

        if len(self.last_n_close_prices[tcks_priority[tck_idx].ticker]['3']) > 3:
            self.last_n_close_prices[tcks_priority[tck_idx].ticker]['3'].pop(0)

        if len(self.last_n_close_prices[tcks_priority[tck_idx].ticker]['17']) > 17:
            self.last_n_close_prices[tcks_priority[tck_idx].ticker]['17'].pop(0)

        if len(self.last_n_close_prices[tcks_priority[tck_idx].ticker]['72']) > 72:
            self.last_n_close_prices[tcks_priority[tck_idx].ticker]['72'].pop(0)

    def _check_business_rules(self, business_data, tcks_priority, tck_idx,
        purchase_price):

        x = [i for i in range(72)]

        last_3_prices_corr = stats.spearmanr(x[:3],
            self.last_n_close_prices[tcks_priority[tck_idx].ticker]['3']).correlation

        if math.isnan(last_3_prices_corr):
            last_3_prices_corr = 0

        last_17_prices_corr = stats.spearmanr(x[:17],
            self.last_n_close_prices[tcks_priority[tck_idx].ticker]['17']).correlation

        last_72_prices_corr = stats.spearmanr(x[:72],
            self.last_n_close_prices[tcks_priority[tck_idx].ticker]['72']).correlation

        last_3_corr_weight = 3
        last_17_corr_weight = 2
        last_72_corr_weight = 1

        weights_sum = last_3_corr_weight + last_17_corr_weight + last_72_corr_weight

        coef = (last_3_prices_corr * last_3_corr_weight + \
            last_17_prices_corr * last_17_corr_weight + \
            last_72_prices_corr * last_72_corr_weight) / weights_sum

        if coef >= self.min_operation_decision_coefficient:
            risk = self.tickers_info[tcks_priority[tck_idx].ticker]
            business_data['stop_loss_day'] = round(purchase_price * (1 - risk), 2)
            return True

        return False
