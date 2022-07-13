from pathlib import Path
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from pandas.tseries.offsets import BDay
import random
from abc import ABC, abstractmethod
import sys
import numpy as np
import math
import joblib
from scipy import stats
import statsmodels.api as sm
from statsmodels.tools.eval_measures import rmse

import constants as c
from utils import RunTime, calculate_maximum_volume, calculate_yield_annualized, \
    get_capital_per_risk, State, Trend, find_candles_peaks
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
    def process_operations(self, days_before_start=120):
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
    def _set_operation_purchase(self, ticker_name, available_capital, rcc,
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

    def __init__(self, tickers, alias=None, comment=None, risk_capital_product=0.10,
        total_capital=100000, min_order_volume=1, partial_sale=False, ema_tolerance=0.01,
        min_risk=0.01, max_risk=0.15, purchase_margin=0.0, stop_margin=0.0,
        stop_type='normal', min_days_after_successful_operation=0,
        min_days_after_failure_operation=0, gain_loss_ratio=3, max_days_per_operation=90,
        tickers_bag='listed_first', tickers_number=0, strategy_number=1, total_strategies=1,
        stdout_prints=True, enable_frequency_normalization=False,
        enable_profit_compensation=False, enable_crisis_halt=False,
        enable_downtrend_halt=False, enable_dynamic_rcc=False, dynamic_rcc_reference=0.80,
        dynamic_rcc_k=3):

        if risk_capital_product < 1e-6 or risk_capital_product > 1.0:
            logger.error(f"Parameter \'risk_reference\' must be in the interval [1e-6, 1].")
            # sys.exit(c.INVALID_ARGUMENT_ERR)
            raise Exception

        if stop_type not in ['normal', 'staircase']:
            logger.error(f"Parameter \'stop_type\' must be ['normal', 'staircase'].")
            # sys.exit(c.INVALID_ARGUMENT_ERR)
            raise Exception

        if tickers_bag not in ['listed_first', 'random']:
            logger.error(f"Parameter \'tickers_bag\' must be ['listed_first', 'random'].")
            # sys.exit(c.INVALID_ARGUMENT_ERR)
            raise Exception

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

        self._enable_frequency_normalization = enable_frequency_normalization
        self._enable_profit_compensation = enable_profit_compensation
        self._enable_crisis_halt = enable_crisis_halt
        self._enable_downtrend_halt = enable_downtrend_halt
        self._enable_dynamic_rcc = enable_dynamic_rcc
        self._dynamic_rcc_reference = dynamic_rcc_reference
        self._dynamic_rcc_k = dynamic_rcc_k

        self._available_capital = total_capital
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
            max_days_per_operation=self._max_days_per_operation,
            enable_frequency_normalization=self._enable_frequency_normalization,
            enable_profit_compensation=self._enable_profit_compensation,
            enable_crisis_halt=self._enable_crisis_halt,
            enable_downtrend_halt=self._enable_downtrend_halt,
            enable_dynamic_rcc=self._enable_dynamic_rcc,
            dynamic_rcc_reference=self._dynamic_rcc_reference,
            dynamic_rcc_k=self._dynamic_rcc_k)

        self._db_generic_model = DBGenericModel()

        # For statistics calculations
        self.first_date = min(tickers.values(), key=lambda x: x['start_date'])['start_date']
        self.last_date = max(tickers.values(), key=lambda x: x['end_date'])['end_date']

        self._statistics_graph = None
        self._statistics_parameters = {}

        AdaptedAndreMoraesStrategy.total_strategies = total_strategies
        self.strategy_number = 1

        if strategy_number != 0:
            self.strategy_number = strategy_number

        self.stdout_prints = stdout_prints


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
    def available_capital(self):
        return self._available_capital

    @available_capital.setter
    def available_capital(self, available_capital):
        self._available_capital = available_capital

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

    @property
    def enable_frequency_normalization(self):
        return self._enable_frequency_normalization

    @property
    def enable_profit_compensation(self):
        return self._enable_profit_compensation

    @property
    def enable_crisis_halt(self):
        return self._enable_crisis_halt

    @property
    def enable_downtrend_halt(self):
        return self._enable_downtrend_halt

    @property
    def enable_dynamic_rcc(self):
        return self._enable_dynamic_rcc

    @property
    def dynamic_rcc_reference(self):
        return self._dynamic_rcc_reference

    @property
    def dynamic_rcc_k(self):
        return self._dynamic_rcc_k

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
            self.available_capital = self.total_capital

            ref_data = self._get_empty_ref_data()

            if self.stdout_prints:
                self._start_progress_bar(update_step=0.10)

            while True:
                try:
                    day_info, week_info = next(data_gen)

                    if day_info.empty:
                        continue

                    self._load_models(day_info.head(1)['day'].squeeze(), wfo=True)

                        # DEBUG
                        # if day_info.head(1)['day'].squeeze() >= pd.Timestamp('2020-06-26'):
                        #     print()

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

                        if self.stdout_prints:
                            self._update_progress_bar(business_data["day"])

                        data_validation_flag = self._process_auxiliary_data(ticker_name,
                            tcks_priority, index, business_data, ref_data)
                        if data_validation_flag is False:
                            tcks_priority[index].last_business_data = business_data.copy()
                            continue

                        if business_data["day"].date() < tcks_priority[index].initial_date \
                            or business_data["day"].date() > tcks_priority[index].final_date:
                            tcks_priority[index].last_business_data = business_data.copy()
                            continue

                        if not self._check_operation_freezetime(tcks_priority, index):
                            tcks_priority[index].last_business_data = business_data.copy()
                            continue

                        if (tcks_priority[index].ongoing_operation_flag is False):

                            purchase_price = self._get_purchase_price(business_data)

                            # Strategy business rules
                            if self._check_business_rules(business_data, tcks_priority,
                                index, purchase_price):

                                stop_price = self._get_stop_price(ticker_name, purchase_price,
                                    business_data)

                                capital_multiplier = self._get_capital_multiplier(tcks_priority,
                                    index, business_data)

                                purchase_amount = self._set_operation_purchase(ticker_name,
                                    purchase_price, stop_price, self.available_capital,
                                    self.risk_capital_product, tcks_priority, index,
                                    business_data, capital_multiplier)
                                self.available_capital = round(self.available_capital - purchase_amount, 2)

                                if purchase_amount >= 0.01 and self.stop_type == "staircase":
                                    self._set_staircase_stop(tcks_priority, index)
                        else:
                            if tcks_priority[index].operation.state == State.OPEN:

                                # If hits the stop loss, the operation is automatically closed
                                sale_amount = self._sell_on_stop_hit(tcks_priority,
                                    index, business_data)
                                self.available_capital = round(self.available_capital + sale_amount, 2)

                                if tcks_priority[index].operation.state == State.OPEN:

                                    if self.partial_sale is True:
                                        sale_amount = self._sell_on_partial_hit(tcks_priority,
                                            index, business_data)
                                        self.available_capital = round(self.available_capital + sale_amount, 2)

                                    sale_amount = self._sell_on_target_hit(tcks_priority,
                                        index, business_data)
                                    self.available_capital = round(self.available_capital + sale_amount, 2)

                                    sale_amount = self._sell_on_timeout_hit(tcks_priority,
                                        index, business_data)
                                    self.available_capital = round(self.available_capital + sale_amount, 2)

                            # Update stop loss threshold
                            if tcks_priority[index].operation.state == State.OPEN \
                                and self.stop_type == "staircase":
                                self._update_staircase_stop(tcks_priority, index, business_data)

                            if tcks_priority[index].operation.state == State.CLOSE:
                                self._save_and_reset_closed_operation(tcks_priority, index)

                        tcks_priority[index].last_business_data = business_data.copy()

                    tcks_priority = self._order_by_priority(tcks_priority, business_data['day'])
                    self._update_global_stats(business_data['day'])
                except StopIteration:
                    break

            # Insert remaining open operations
            for ts in tcks_priority:
                if ts.operation is not None and ts.operation.state == State.OPEN:
                    self.operations.append(ts.operation)

        except Exception as error:
            logger.exception(f"Error processing operations, error:\n{error}")
            # sys.exit(c.PROCESSING_OPERATIONS_ERR)
            raise error

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
            # sys.exit(c.PROCESSING_OPERATIONS_ERR)
            raise error

    def save(self):
        self._db_strategy_model.insert_strategy_results(self._statistics_parameters,
            self.operations, self._statistics_graph)

    def _calc_performance(self, days_batch=30):
        """
        Calculate time domain performance indicators.

        Capital, capital in use, tickers average, IBOV.

        Set _statistics_graph dataframe with columns 'day', 'capital', 'capital_in_use',
        'baseline', 'ibov'.

        Args
        ----------
        days_batch : int, default 30
            Data chunk size when requesting to database.
        """
        statistics = pd.DataFrame(columns=['day', 'capital', 'capital_in_use',
            'baseline', 'ibov'])

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
                        if day.date() >= tck_dates['start_date'] and day.date() <= tck_dates['end_date']:

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
        statistics['baseline'] = AdaptedAndreMoraesStrategy.get_tickers_yield(
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
        self._statistics_parameters['total_yield'] = \
            round(self._statistics_parameters['profit'] / self._total_capital,
                real_precision)

        # Annualized Yield
        bus_day_count = len(self._statistics_graph)

        self._statistics_parameters['total_yield_ann'] = round(
            calculate_yield_annualized(self._statistics_parameters['total_yield'],
                bus_day_count), real_precision)

        # IBOV Yield
        first_ibov_value = self._statistics_graph['ibov'].head(1).values[0]
        last_ibov_value = self._statistics_graph['ibov'].tail(1).values[0]
        total_ibov_yield = (last_ibov_value / first_ibov_value) - 1

        self._statistics_parameters['total_ibov_yield'] = round(total_ibov_yield, real_precision)

        # Annualized IBOV Yield
        self._statistics_parameters['total_ibov_yield_ann'] = round(
            calculate_yield_annualized(self._statistics_parameters['total_ibov_yield'],
                bus_day_count), real_precision)

        # Average Tickers Yield
        total_baseline_yield = self._statistics_graph['baseline'].tail(1).squeeze() - 1
        self._statistics_parameters['total_baseline_yield'] = round(total_baseline_yield,
            real_precision)

        # Annualized Average Tickers Yield
        self._statistics_parameters['total_baseline_yield_ann'] = round(
            calculate_yield_annualized(self._statistics_parameters['total_baseline_yield'],
                bus_day_count), real_precision)

        # Volatility
        # norm_capital = self._statistics_graph['capital'] / self._statistics_graph['capital'][0] - 1
        norm_capital = self._statistics_graph['capital'].pct_change()
        norm_baseline = self._statistics_graph['baseline'].pct_change()
        norm_capital.fillna(value=0.0, inplace=True)

        # TODO: Verify calculation
        self._statistics_parameters['total_volatility'] = round(norm_capital.std() * math.sqrt(len(norm_capital)), real_precision)
        self._statistics_parameters['baseline_total_volatility'] = round(norm_baseline.std() * math.sqrt(len(norm_baseline)), real_precision)

        self._statistics_parameters['volatility_ann'] = round(norm_capital.std() * math.sqrt(252), real_precision)
        self._statistics_parameters['baseline_volatility_ann'] = round(norm_baseline.std() * math.sqrt(252), real_precision)

        # self._statistics_parameters['mean_yield_ann'] = round(((1 + norm_capital.mean()) ** 252) - 1, real_precision)

        # Sharpe and Sortino Ratio
        # Risk-free yield by CDI index
        cdi_df = self._db_strategy_model.get_cdi_index(min(self._initial_dates),
            max(self._final_dates))

        self._statistics_parameters['sharpe_ratio'] = AdaptedAndreMoraesStrategy.sharpe_ratio(
            norm_capital, cdi_df['value'], precision=real_precision)

        self._statistics_parameters['baseline_sharpe_ratio'] = AdaptedAndreMoraesStrategy.sharpe_ratio(
            norm_baseline, cdi_df['value'], precision=real_precision)

        self._statistics_parameters['sortino_ratio'] = AdaptedAndreMoraesStrategy.sortino_ratio(
            norm_capital, cdi_df['value'], precision=real_precision)

        self._statistics_parameters['baseline_sortino_ratio'] = AdaptedAndreMoraesStrategy.sortino_ratio(
            norm_baseline, cdi_df['value'], precision=real_precision)

        # Correlations
        self._statistics_parameters['ibov_pearson_corr'] = AdaptedAndreMoraesStrategy.\
            get_correlation(self._statistics_graph['capital'], self._statistics_graph['ibov'],
            method='pearson', precision=real_precision)

        self._statistics_parameters['ibov_spearman_corr'] = AdaptedAndreMoraesStrategy.\
            get_correlation(self._statistics_graph['capital'], self._statistics_graph['ibov'],
            method='spearman', precision=real_precision)

        self._statistics_parameters['baseline_pearson_corr'] = AdaptedAndreMoraesStrategy.\
            get_correlation(self._statistics_graph['capital'],
            self._statistics_graph['baseline'], method='pearson', precision=real_precision)

        self._statistics_parameters['baseline_spearman_corr'] = AdaptedAndreMoraesStrategy.\
            get_correlation(self._statistics_graph['capital'],
            self._statistics_graph['baseline'], method='spearman', precision=real_precision)

    @staticmethod
    def sharpe_ratio(target, rf, precision=4):
        if target.std() <= 1e-5:
            return 0.0

        # mean = target.mean() - (rf.mean() - 1)
        mean_year = ((1 + target.mean() - (rf.mean() - 1)) ** 252) - 1
        # sigma = target.std()
        sigma_year = target.std() * math.sqrt(252)

        return round(mean_year / sigma_year, precision)

    @staticmethod
    def sortino_ratio(target, rf, precision=4):
        if target.std() <= 1e-5:
            return 0.0

        # mean = target.mean() - (rf.mean() - 1)
        mean_year = ((1 + target.mean() - (rf.mean() - 1)) ** 252) - 1

        # sigma_neg = target[target < target.mean()].std()
        sigma_neg_year = target[target < target.mean()].std() * math.sqrt(252)

        return round(mean_year / sigma_neg_year, precision)

    @staticmethod
    def get_correlation(serie_1, serie_2, method='pearson', precision=4):

        if method not in ['pearson', 'spearman']:
            logger.error("\'method\' parameter must be in ['pearson', 'spearman']")
            raise Exception

        if method == 'spearman':
            corr = stats.spearmanr(serie_1, serie_2).correlation
        elif method == 'pearson':
            corr = stats.pearsonr(serie_1, serie_2)[0]

        if np.isnan(corr):
            corr = 0.0

        return round(corr, precision)

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

            # if day >= pd.Timestamp("2020-01-30T00"):
            #     print()

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
            capital_in_use[day_index] = \
                round(current_capital_in_use / (current_capital + current_capital_in_use), 4)

        return capital, capital_in_use

    def _order_by_priority(self, tcks_priority, day):
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
        if day.date() >= self.first_date:
            new_list = sorted(tcks_priority, key=lambda ticker_state: \
                str(int(ticker_state.operation.state == State.OPEN
                if ticker_state.operation is not None else 0)) \
                + str(int(ticker_state.operation.state == State.NOT_STARTED
                if ticker_state.operation is not None else 0)), reverse=True)

            return new_list

        return tcks_priority

    def _update_global_stats(self, day):
        pass

    # Assumption: all tickers have the same length of the first one.
    @staticmethod
    def get_tickers_yield(close_prices, precision=4):
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
                capital.append(1.0)
            else:
                capital.append(round(total_money/initial_capital, precision))

        return capital

    class DataGen:
        def __init__(self, tickers, db_connection, days_batch=30, days_before_start=120,
            week=True, volume=False):
            self.tickers = tickers
            self.first_date = min(self.tickers.values(), key=lambda x: x['start_date'])['start_date']
            self.first_date = self.first_date - BDay(days_before_start)
            self.last_date = max(self.tickers.values(), key=lambda x: x['end_date'])['end_date']
            self.db_connection = db_connection
            self.days_batch = days_batch
            self.week = week
            self.volume = volume

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
                    self.dates[next_chunk_end_index], interval='1d', volume=self.volume)

                if self.week is True:
                    self.weekly_data = self.db_connection.get_data_chunk(self.tickers,
                        self.dates[self.current_date_index],
                        self.dates[next_chunk_end_index], interval='1wk', volume=self.volume)

            self.current_date_index += 1

            daily_data = self.daily_data[self.daily_data['day'] == self.dates[self.current_date_index-1]]

            if self.week:
                year, week, _ = (self.dates[self.current_date_index-1] \
                    - pd.Timedelta(days=7)).isocalendar()
                weekly_data = self.weekly_data[\
                    (self.weekly_data['week'].dt.isocalendar().year == year) \
                    & (self.weekly_data['week'].dt.isocalendar().week == week)] \
                    if not self.weekly_data.empty \
                    else self.weekly_data

                return daily_data, weekly_data

            return daily_data

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

            self._profit = 0.0
            self._loaned = 0.0
            self._op_count = 0
            self._op_suc_count = 0

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

        @property
        def profit(self):
            return self._profit

        @profit.setter
        def profit(self, profit):
            self._profit = profit

        @property
        def loaned(self):
            return self._loaned

        @loaned.setter
        def loaned(self, loaned):
            self._loaned = loaned

        @property
        def op_count(self):
            return self._op_count

        @op_count.setter
        def op_count(self, op_count):
            self._op_count = op_count

        @property
        def op_suc_count(self):
            return self._op_suc_count

        @op_suc_count.setter
        def op_suc_count(self, op_suc_count):
            self._op_suc_count = op_suc_count

    def _start_progress_bar(self, update_step=0.05):
        self._update_step = update_step
        self._next_update_percent = update_step
        self._total_days = (self.last_date - self.first_date).days
        print(f"\nStrategy {self.name} ({self.strategy_number} ", end='')

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

            sys.stdout.flush()
            self._next_update_percent += self._update_step

    def _load_models(self, day, wfo=True):
        pass

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

        stop_price = business_data["stop_loss_day"]
        max_stop_price = round(purchase_price * (1 - self.min_risk), 2)

        if stop_price > max_stop_price:
            stop_price = max_stop_price

        return stop_price

    def _set_operation_purchase(self, ticker_name, purchase_price, stop_price,
        available_capital, rcc, tcks_priority, tck_idx, business_data,
        capital_multiplier=1.0):

        amount_withdrawn = 0.0

        max_capital = get_capital_per_risk(
            rcc, available_capital, \
            (purchase_price - stop_price)/ \
            (purchase_price), capital_multiplier)
        max_vol = calculate_maximum_volume(purchase_price, max_capital,
            minimum_volume=self.min_order_volume)

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

    def _get_capital_multiplier(self, tcks_priority, index, business_data):
        return 1.0

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
        tickers_bag='listed_first', tickers_number=0, strategy_number=1, total_strategies=1,
        stdout_prints=True, enable_frequency_normalization=False,
        enable_profit_compensation=False, enable_crisis_halt=False,
        enable_downtrend_halt=False, enable_dynamic_rcc=False,
        dynamic_rcc_reference=0.80, dynamic_rcc_k=3):

        super().__init__(tickers, alias, comment, risk_capital_product, total_capital,
            min_order_volume, partial_sale, ema_tolerance, min_risk, max_risk,
            purchase_margin, stop_margin, stop_type, min_days_after_successful_operation,
            min_days_after_failure_operation, gain_loss_ratio, max_days_per_operation,
            tickers_bag, tickers_number, strategy_number, total_strategies,
            stdout_prints, enable_frequency_normalization, enable_profit_compensation,
            enable_crisis_halt, enable_downtrend_halt, enable_dynamic_rcc,
            dynamic_rcc_reference, dynamic_rcc_k)

        self._name = "ML Derivation"
        self._db_strategy_model.name = self._name
        self._models = {}
        self._current_model_tag = None
        self._max_capital = total_capital

        self.tickers_info_path = Path(__file__).parent.parent / c.TICKERS_INFO_PATH
        self.ticker_datasets_path = Path(__file__).parent.parent / c.DATASETS_PATH
        self.risks = None
        self.len_risks_in_datasets = None

        self._load_risks_and_trends_file()

        self.total_op_count = 0
        self.total_op_suc_count = 0
        self.total_profit = 0

        self.capital_in_use = 0.0
        self.capital_in_use_last_values = []
        self.der_lpf_alpha = 0.1
        self.capital_in_use_mavg = 0.0
        self.last_capital_in_use_mavg = 0.0
        self.n_avg = 10
        self.capital_in_use_dot = 0.0
        self.first_update = True

        self.dynamic_rcc_value = self.risk_capital_product
        self.last_error = 0.0

        self.spearman_correlations= (5, 10, 15, 20, 25, 30, 35, 40, 50, 60)
        self.last_prices_max_length = max(self.spearman_correlations)
        self.spearman_reference = tuple([i for i in range(self.last_prices_max_length)])

        self.last_data = {ticker: \
            {'open': 0.0, 'close': 0.0, 'mid': [], 'mid_dot': 0.0,
            'ols_slope': 0.0, 'min_slope': float('inf'), 'max_slope': -float('inf'),
            'ols_rmse': 0.0, 'min_rmse': float('inf'), 'max_rmse': -float('inf')} \
            for ticker in self.tickers_and_dates}
        self.mid_prices_lpf_alpha = 0.1

    @property
    def models(self):
        return self._models

    @property
    def tickers_info(self):
        return self._tickers_info

    @tickers_info.setter
    def tickers_info(self, tickers_info):
        self._tickers_info = tickers_info

    @property
    def max_capital(self):
        return self._max_capital

    @max_capital.setter
    def max_capital(self, max_capital):
        self._max_capital = max_capital


    def _load_risks_and_trends_file(self):

        columns = ['ticker', 'day', 'uptrend', 'downtrend', 'crisis', 'min_risk', 'max_risk']

        if self.tickers_info_path.exists():
            self.ticker_day_risks = pd.read_csv(self.tickers_info_path, sep=',',
                usecols=columns)
        else:
            # Trend parameters
            N_pri = 20
            N_vol = 60
            N_dot = 2
            lpf_alpha = 0.1
            spearman_up_threshold = 0.5
            downtrend_inertia = 3
            anomalies_inertia = 2
            crisis_halt_inertia = 8
            # Risk parameters
            N_delta = 20
            N_peak_window = 80
            peaks_window_size = 5
            min_peaks_for_analysis = 5
            climbs_lpf_alpha = 0.5
            gain_loss_ratio = 3
            risk_lpf_alpha = 0.3
            down_inertia_alpha = 0.10

            days_before_start = int(max(N_pri, N_vol, N_dot) * 1.5)
            data_gen = self.DataGen(self.tickers_and_dates, self._db_strategy_model,
                days_batch=30, days_before_start=days_before_start, week=False, volume=True)

            # General support variables
            last_mid_prices = {ticker: [] for ticker in self.tickers_and_dates}
            last_volumes = {ticker: [] for ticker in self.tickers_and_dates}
            last_max_prices = {ticker: [] for ticker in self.tickers_and_dates}
            last_min_prices = {ticker: [] for ticker in self.tickers_and_dates}
            last_open_prices = {ticker: [] for ticker in self.tickers_and_dates}
            last_close_prices = {ticker: [] for ticker in self.tickers_and_dates}
            last_deltas = {ticker: [] for ticker in self.tickers_and_dates}
            days = {ticker: [] for ticker in self.tickers_and_dates}
            ref_spear = [i for i in range(max(N_pri, N_vol))]

            # Variables of Trend identification
            cum_spearman = {ticker: [] for ticker in self.tickers_and_dates}
            cum_spearman = {ticker: [] for ticker in self.tickers_and_dates}
            avg_price = {ticker: [] for ticker in self.tickers_and_dates}
            std_price = {ticker: [] for ticker in self.tickers_and_dates}
            avg_volume = {ticker: [] for ticker in self.tickers_and_dates}
            std_volume = {ticker: [] for ticker in self.tickers_and_dates}
            mid_prices_dot = {ticker: [] for ticker in self.tickers_and_dates}
            volume_anomalies = {ticker: [] for ticker in self.tickers_and_dates}
            price_down_anomalies = {ticker: [] for ticker in self.tickers_and_dates}

            # Trend auxiliary variables
            downtrend_inertia_counter = {ticker: downtrend_inertia for ticker in self.tickers_and_dates}
            crisis_inertia_counter = {ticker: crisis_halt_inertia for ticker in self.tickers_and_dates}
            anomalies_counter = {ticker: 0 for ticker in self.tickers_and_dates}
            in_uptrend_flag = {ticker: False for ticker in self.tickers_and_dates}

            # Risk auxiliary variables
            mid_prices_dot_for_risk = {ticker: [] for ticker in self.tickers_and_dates}
            std_price_deltas = {ticker: [] for ticker in self.tickers_and_dates}
            avg_climbs = {ticker: [] for ticker in self.tickers_and_dates}
            std_climbs = {ticker: [] for ticker in self.tickers_and_dates}
            fixed_min_risk = {ticker: [] for ticker in self.tickers_and_dates}
            variable_min_risk = {ticker: [] for ticker in self.tickers_and_dates}

            # Trend Output Variables
            uptrend = {ticker: [] for ticker in self.tickers_and_dates}
            downtrend = {ticker: [] for ticker in self.tickers_and_dates}
            crisis = {ticker: [] for ticker in self.tickers_and_dates}

            # Risk Output Variables
            max_risk = {ticker: [] for ticker in self.tickers_and_dates}
            min_risk = {ticker: [] for ticker in self.tickers_and_dates}


            first_iteration = True
            start_date = pd.Timestamp( min([dates['start_date'] for _, dates in self.tickers_and_dates.items()]) )
            start_date_flag = False
            while True:
                try:
                    day_info = next(data_gen)

                    if day_info.empty:
                        continue

                    if start_date_flag is False and day_info['day'].head(1).squeeze() >= start_date:
                        start_date = day_info['day'].head(1).squeeze()
                        start_date_flag = True

                    for tck_idx, ticker in enumerate(self.tickers_and_dates):

                        if day_info[day_info['ticker'] == ticker].empty:
                            continue

                        open = day_info[(day_info['ticker'] == ticker)]['open_price'].squeeze()
                        close = day_info[(day_info['ticker'] == ticker)]['close_price'].squeeze()
                        high = day_info[(day_info['ticker'] == ticker)]['max_price'].squeeze()
                        low = day_info[(day_info['ticker'] == ticker)]['min_price'].squeeze()
                        volume = day_info[(day_info['ticker'] == ticker)]['volume'].squeeze()
                        day = day_info[(day_info['ticker'] == ticker)]['day'].squeeze()

                        if isinstance(open, pd.Series) or \
                            isinstance(close, pd.Series) or \
                            isinstance(high, pd.Series) or \
                            isinstance(low, pd.Series) or \
                            isinstance(volume, pd.Series) or \
                            isinstance(day, pd.Series):
                            continue

                        if first_iteration is True:
                            days[ticker].append( day )

                            # Support variables
                            last_mid_prices[ticker].append( (open+close)/2 )
                            last_max_prices[ticker].append( high )
                            last_min_prices[ticker].append( low )
                            last_open_prices[ticker].append( open )
                            last_close_prices[ticker].append( close )
                            last_deltas[ticker].append( high-low )
                            last_volumes[ticker].append( volume )

                            # Variables of trend identification
                            cum_spearman[ticker].append( 0.0 )
                            avg_price[ticker].append( 0.0 )
                            std_price[ticker].append( 0.0 )
                            avg_volume[ticker].append( 0.0 )
                            std_volume[ticker].append( 0.0 )
                            mid_prices_dot[ticker].append( 0.0 )
                            volume_anomalies[ticker].append( False )
                            price_down_anomalies[ticker].append( False )

                            # Variables of Risk identification
                            mid_prices_dot_for_risk[ticker].append( 0.0 )
                            std_price_deltas[ticker].append( 0.0 )
                            avg_climbs[ticker].append( 0.0 )
                            std_climbs[ticker].append( 0.0 )
                            variable_min_risk[ticker].append( 0.0 )
                            fixed_min_risk[ticker].append( 0.0 )
                            max_risk[ticker].append( 0.0 )
                            min_risk[ticker].append( 0.0 )

                            # Trend Output Variables
                            uptrend[ticker].append( False )
                            downtrend[ticker].append( True )
                            crisis[ticker].append( False )

                            if tck_idx == len(self.tickers_and_dates) - 1:
                                first_iteration = False

                        else:

                            # Variables of trend identification Section
                            if len(last_mid_prices[ticker]) >= N_pri:
                                cum_spearman[ticker].append( stats.spearmanr(
                                    ref_spear[0:N_pri],
                                    last_mid_prices[ticker][len(last_mid_prices[ticker]) - N_pri : len(last_mid_prices[ticker])]).correlation )
                                avg_price[ticker].append( np.mean(
                                    last_mid_prices[ticker][len(last_mid_prices[ticker]) - N_pri : len(last_mid_prices[ticker])]) )
                                std_price[ticker].append( np.std(
                                    last_mid_prices[ticker][len(last_mid_prices[ticker]) - N_pri : len(last_mid_prices[ticker])]) )
                            else:
                                cum_spearman[ticker].append( 0.0 )
                                avg_price[ticker].append( 0.0 )
                                std_price[ticker].append( 0.0 )

                            if len(last_volumes[ticker]) >= N_vol:
                                avg_volume[ticker].append( np.mean(
                                    last_volumes[ticker][len(last_volumes[ticker]) - N_vol : len(last_volumes[ticker])]) )
                                std_volume[ticker].append( np.std(
                                    last_volumes[ticker][len(last_volumes[ticker]) - N_vol : len(last_volumes[ticker])]) )
                            else:
                                avg_volume[ticker].append( 0.0 )
                                std_volume[ticker].append( 0.0 )

                            if len(last_mid_prices[ticker]) >= N_dot:
                                # LPF for prices derivative ( y[i] := α * x[i] + (1-α) * y[i-1] )
                                y0 = lpf_alpha * ( (last_mid_prices[ticker][-1] - last_mid_prices[ticker][-2])\
                                    / ((last_mid_prices[ticker][-2] + last_mid_prices[ticker][-1])/2) ) \
                                    + (1-lpf_alpha) * mid_prices_dot[ticker][-1]

                                y1 = risk_lpf_alpha * ( (last_mid_prices[ticker][-1] - last_mid_prices[ticker][-2])\
                                    / ((last_mid_prices[ticker][-2] + last_mid_prices[ticker][-1])/2) ) \
                                    + (1-risk_lpf_alpha) * mid_prices_dot_for_risk[ticker][-1]

                                mid_prices_dot[ticker].append( y0 )
                                mid_prices_dot_for_risk[ticker].append( y1 )
                            else:
                                mid_prices_dot[ticker].append( 0.0 )
                                mid_prices_dot_for_risk[ticker].append( 0.0 )

                            # Volume and Price Down Anomalies Section
                            if last_volumes[ticker][-1] > avg_volume[ticker][-1] + std_volume[ticker][-1]:
                                volume_anomalies[ticker].append( True )
                            else:
                                volume_anomalies[ticker].append( False )

                            if last_mid_prices[ticker][-1] < avg_price[ticker][-1] + std_price[ticker][-1]:
                                price_down_anomalies[ticker].append( True )
                            else:
                                price_down_anomalies[ticker].append( False )

                            # Standard Deviation of Price Deltas
                            if len(last_deltas[ticker]) < N_delta:
                                std_price_deltas[ticker].append( 0.0 )
                            else:
                                std_price_deltas[ticker].append( np.std( last_deltas[ticker][len(last_deltas[ticker]) - N_delta : len(last_deltas[ticker])] ) )

                            # Peaks for Climbs Identification
                            peaks = find_candles_peaks(
                                last_max_prices[ticker][max(0, len(last_max_prices[ticker])-N_peak_window) : len(last_max_prices[ticker])],
                                last_min_prices[ticker][max(0, len(last_min_prices[ticker])-N_peak_window) : len(last_min_prices[ticker])],
                                window_size=peaks_window_size)

                            if peaks is not None and len(peaks) >= min_peaks_for_analysis \
                                and len(avg_climbs) >= N_peak_window*0.75:
                                climbs = []
                                for idx in range(len(peaks)):
                                    if idx > 0:
                                        if peaks[idx]['type'] == 'max' and peaks[idx-1]['type'] == 'min':
                                            if peaks[idx]['magnitude'] > peaks[idx-1]['magnitude']:
                                                climbs.append( round((peaks[idx]['magnitude'] - peaks[idx-1]['magnitude']) \
                                                    / peaks[idx-1]['magnitude'], 4) )

                                y_mean = climbs_lpf_alpha * np.mean(np.array(climbs)) \
                                    + (1-climbs_lpf_alpha) * avg_climbs[ticker][-1]
                                y_std = climbs_lpf_alpha * np.std(np.array(climbs)) + \
                                    (1-climbs_lpf_alpha) * std_climbs[ticker][-1]

                                avg_climbs[ticker].append( y_mean )
                                std_climbs[ticker].append( y_std )
                            else:
                                avg_climbs[ticker].append( 0.0 )
                                std_climbs[ticker].append( 0.0 )


                            # Trend Analysis Section
                            # Trend Analysis: Downtrend Analysis
                            if len(mid_prices_dot[ticker]) <= N_dot:
                                downtrend[ticker].append( False )
                            else:
                                if mid_prices_dot[ticker][-1] < 0:
                                    downtrend[ticker].append( True )
                                    downtrend_inertia_counter[ticker] = 0
                                else:
                                    if downtrend_inertia_counter[ticker] < downtrend_inertia:
                                        downtrend[ticker].append( True )
                                        downtrend_inertia_counter[ticker] += 1
                                    else:
                                        downtrend[ticker].append( False )

                            # Trend Analysis: Crisis Analysis
                            if len(volume_anomalies[ticker]) < N_vol:
                                crisis[ticker].append( False )
                            else:
                                if volume_anomalies[ticker][-1] is True and price_down_anomalies[ticker][-1] is True:
                                    anomalies_counter[ticker] += 1
                                    if anomalies_counter[ticker] >= anomalies_inertia:
                                        crisis[ticker].append( True )
                                        crisis_inertia_counter[ticker] = 0
                                    else:
                                        if crisis_inertia_counter[ticker] < crisis_halt_inertia:
                                            crisis[ticker].append( True )
                                            crisis_inertia_counter[ticker] += 1
                                        else:
                                            crisis[ticker].append( False )
                                else:
                                    if crisis_inertia_counter[ticker] < crisis_halt_inertia:
                                        crisis[ticker].append( True )
                                        crisis_inertia_counter[ticker] += 1
                                    else:
                                        crisis[ticker].append( False )

                                    if anomalies_counter[ticker] != 0:
                                        anomalies_counter[ticker] = 0

                            # Trend Analysis: Uptrend Analysis
                            if len(cum_spearman[ticker]) < N_pri:
                                uptrend[ticker].append( False )
                            else:
                                if in_uptrend_flag[ticker] is False:
                                    if mid_prices_dot[ticker][-1] > 0 and cum_spearman[ticker][-1] >= spearman_up_threshold:
                                        uptrend[ticker].append( True )
                                        in_uptrend_flag[ticker] = True
                                    else:
                                        uptrend[ticker].append( False )
                                else:
                                    if mid_prices_dot[ticker][-1] > 0 and cum_spearman[ticker][-1] >= spearman_up_threshold:
                                        uptrend[ticker].append( True )
                                    else:
                                        uptrend[ticker].append( False )
                                        in_uptrend_flag[ticker] = False

                            # Risk Analysis
                            if len(last_mid_prices[ticker]) < N_delta:
                                fixed_min_risk[ticker].append( 0.0 )
                            else:
                                # Two times half standard deviation = standard deviation
                                fixed_min_risk[ticker].append(
                                    std_price_deltas[ticker][-1] / last_mid_prices[ticker][-1] )

                            if len(mid_prices_dot_for_risk[ticker]) <= N_dot:
                                variable_min_risk[ticker].append( 0.0 )
                            else:
                                variable_min_risk[ticker].append(
                                    max( -mid_prices_dot_for_risk[ticker][-1], 0) )

                            if len(fixed_min_risk[ticker]) < max(N_delta, N_dot):
                                min_risk[ticker].append( 0.0 )
                            else:
                                # down_inertia_alpha
                                new_min_risk = fixed_min_risk[ticker][-1] + variable_min_risk[ticker][-1]

                                if new_min_risk >= min_risk[ticker][-1]:
                                    min_risk[ticker].append( new_min_risk )
                                else:
                                    # LPF for downward inertia only ( y[i] := α * x[i] + (1-α) * y[i-1] )
                                    y = down_inertia_alpha * (new_min_risk) \
                                        + (1-down_inertia_alpha) * min_risk[ticker][-1]

                                    min_risk[ticker].append( y )

                            if len(avg_climbs) < N_peak_window * 0.75:
                                max_risk[ticker].append( 0.0 )
                            else:
                                max_risk[ticker].append( round(
                                    (avg_climbs[ticker][-1] - 0.5 * std_climbs[ticker][-1]) / gain_loss_ratio, 4))

                            # Support variables must be the last to avoid non-causality
                            last_mid_prices[ticker].append( (open+close)/2 )
                            last_max_prices[ticker].append( high )
                            last_min_prices[ticker].append( low )
                            last_open_prices[ticker].append( open )
                            last_close_prices[ticker].append( close )
                            last_deltas[ticker].append( high-low )
                            last_volumes[ticker].append( volume )
                            days[ticker].append( day )
                except StopIteration:
                    break

            first_write = True
            for idx, ticker in enumerate(self.tickers_and_dates):
                start_idx = days[ticker].index(start_date)

                pd.DataFrame({'ticker': ticker,
                    'day': days[ticker][start_idx:],
                    'last_mid_prices': last_mid_prices[ticker][start_idx:],
                    'last_deltas': last_deltas[ticker][start_idx:],
                    'last_volumes': last_volumes[ticker][start_idx:],
                    'last_max_prices': last_max_prices[ticker][start_idx:],
                    'last_min_prices': last_min_prices[ticker][start_idx:],
                    'last_open_prices': last_open_prices[ticker][start_idx:],
                    'last_close_prices': last_close_prices[ticker][start_idx:],

                    'avg_volume': avg_volume[ticker][start_idx:],
                    'std_volume': std_volume[ticker][start_idx:],
                    'mid_prices_dot': mid_prices_dot[ticker][start_idx:],
                    'mid_prices_dot_for_risk': mid_prices_dot_for_risk[ticker][start_idx:],
                    'avg_price': avg_price[ticker][start_idx:],
                    'std_price': std_price[ticker][start_idx:],
                    'std_price_deltas': std_price_deltas[ticker][start_idx:],

                    'volume_anomalies': volume_anomalies[ticker][start_idx:],
                    'price_down_anomalies': price_down_anomalies[ticker][start_idx:],
                    'crisis': crisis[ticker][start_idx:],

                    'downtrend': downtrend[ticker][start_idx:],

                    'uptrend': uptrend[ticker][start_idx:],

                    'fixed_min_risk': [round(risk, 4) for risk in fixed_min_risk[ticker][start_idx:]],
                    'variable_min_risk': [round(risk, 4) for risk in variable_min_risk[ticker][start_idx:]],
                    'min_risk': [round(risk, 4) for risk in min_risk[ticker][start_idx:]],

                    'avg_climbs': avg_climbs[ticker][start_idx:],
                    'std_climbs': std_climbs[ticker][start_idx:],
                    'max_risk': max_risk[ticker][start_idx:]}).\
                    to_csv(self.tickers_info_path, mode='w' if first_write else 'a',
                    index=False, header=True if first_write else False)

                if first_write:
                    first_write = False

            self.ticker_day_risks = pd.read_csv(self.tickers_info_path, sep=',',
                usecols=columns)

    def _load_models(self, day=None, wfo=True):
        """WFO = Walk Forward Optimization"""

        path_prefix = Path(__file__).parent.parent / c.MODELS_PATH

        if wfo is False and self._current_model_tag is None:
            if day >= pd.Timestamp(c.WFO_START_DATE):
                for key, value in c.WFO_MODEL_TAGS.items():
                    if day <= pd.Timestamp(year=value['end_year'], month=value['end_month'], \
                        day=value['end_day']):

                        self._current_model_tag = key

                        for _, (ticker, _) in enumerate(self.tickers_and_dates.items()):
                            self._models[ticker] = joblib.load(path_prefix / \
                            (ticker + '_' + self._current_model_tag + c.MODEL_SUFFIX))

                        break

        elif wfo is True:
            if self._current_model_tag is not None:
                if day > pd.Timestamp(year=c.WFO_MODEL_TAGS[self._current_model_tag]['end_year'],
                    month=c.WFO_MODEL_TAGS[self._current_model_tag]['end_month'],
                    day=c.WFO_MODEL_TAGS[self._current_model_tag]['end_day']):

                    next_index = list(c.WFO_MODEL_TAGS.keys()).index(self._current_model_tag) + 1
                    self._current_model_tag = list(c.WFO_MODEL_TAGS.keys())[next_index]

                    for _, (ticker, _) in enumerate(self.tickers_and_dates.items()):
                        self._models[ticker] = joblib.load(path_prefix / \
                        (ticker + '_' + self._current_model_tag + c.MODEL_SUFFIX))
            else:
                if day >= pd.Timestamp(c.WFO_START_DATE):
                    for key, value in c.WFO_MODEL_TAGS.items():
                        if day <= pd.Timestamp(year=value['end_year'], month=value['end_month'], \
                            day=value['end_day']):

                            self._current_model_tag = key

                            for _, (ticker, _) in enumerate(self.tickers_and_dates.items()):
                                self._models[ticker] = joblib.load(path_prefix / \
                                (ticker + '_' + self._current_model_tag + c.MODEL_SUFFIX))

                            break

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

        # Spearman corelations and mid prices derivative
        new_mid = round((self.last_data[ticker_name]['open'] + self.last_data[ticker_name]['close'])/2, 6)

        if new_mid >= 1e-2:
            self.last_data[ticker_name]['mid'].append( new_mid )

            if len(self.last_data[ticker_name]['mid']) >= 2:
                self.last_data[ticker_name]['mid_dot'] = \
                    self.mid_prices_lpf_alpha * ((self.last_data[ticker_name]['mid'][-1] - self.last_data[ticker_name]['mid'][-2]) / self.last_data[ticker_name]['mid'][-2]) + \
                    (1 - self.mid_prices_lpf_alpha) * self.last_data[ticker_name]['mid_dot']

            if len(self.last_data[ticker_name]['mid']) > self.last_prices_max_length:
                self.last_data[ticker_name]['mid'].pop(0)

        self.last_data[ticker_name]['open'] = business_data['open_price_day']
        self.last_data[ticker_name]['close'] = business_data['close_price_day']

        # # OLS
        # if len(self.last_data[ticker_name]['mid']) >= 60:
        #     X = sm.add_constant(self.spearman_reference[0:60])
        #     ols_model = sm.OLS(self.last_data[ticker_name]['mid'], X).fit()

        #     # slope = round(ols_model.params[1] / ols_model.params[0], 6)
        #     slope = round(ols_model.params[1] / ypred[-1], 6)
        #     self.last_data[ticker_name]['ols_slope'] = slope

        #     if slope < self.last_data[ticker_name]['min_slope']:
        #         self.last_data[ticker_name]['min_slope'] = slope

        #     if slope > self.last_data[ticker_name]['max_slope']:
        #         self.last_data[ticker_name]['max_slope'] = slope

        #     ypred = ols_model.predict(X)
        #     rms_error = round(rmse(self.last_data[ticker_name]['mid'], ypred), 6)

        #     self.last_data[ticker_name]['ols_rmse'] = rms_error

        #     if rms_error < self.last_data[ticker_name]['min_rmse']:
        #         self.last_data[ticker_name]['min_rmse'] = rms_error

        #     if rms_error > self.last_data[ticker_name]['max_rmse']:
        #         self.last_data[ticker_name]['max_rmse'] = rms_error

        # Peak analysis: Put first peaks in buffer
        # MLDerivationStrategy._update_peaks_days(tcks_priority[tck_idx])

        # tcks_priority[tck_idx].extra_vars['current_max_delay'] += 1
        # tcks_priority[tck_idx].extra_vars['current_min_delay'] += 1

        # if business_data['peak_day'] > 0.01:
        #     # Bug treatment 'if' statement
        #     if business_data['max_price_day'] == business_data['min_price_day']:
        #         # Choose the an alternating peak type
        #         # Lesser means most recent added, so now is the time for the other peak type
        #         if tcks_priority[tck_idx].extra_vars['current_max_delay'] < \
        #             tcks_priority[tck_idx].extra_vars['current_min_delay']:

        #             tcks_priority[tck_idx].extra_vars['upcoming_min_peak'] = business_data['peak_day']
        #             tcks_priority[tck_idx].extra_vars['current_min_delay'] = 0
        #         else:
        #             tcks_priority[tck_idx].extra_vars['upcoming_max_peak'] = business_data['peak_day']
        #             tcks_priority[tck_idx].extra_vars['current_max_delay'] = 0
        #     elif business_data['max_price_day'] != business_data['min_price_day']:
        #         if business_data['peak_day'] == business_data['max_price_day']:
        #             tcks_priority[tck_idx].extra_vars['upcoming_max_peak'] = \
        #                 business_data['peak_day']
        #             tcks_priority[tck_idx].extra_vars['current_max_delay'] = 0
        #         else:
        #             tcks_priority[tck_idx].extra_vars['upcoming_min_peak'] = business_data['peak_day']
        #             tcks_priority[tck_idx].extra_vars['current_min_delay'] = 0

        # if tcks_priority[tck_idx].extra_vars['current_max_delay'] >= \
        #     ref_data['peak_delay'] and tcks_priority[tck_idx].extra_vars['upcoming_max_peak'] != 0.0:

        #     tcks_priority[tck_idx].extra_vars['last_max_peaks'].append(tcks_priority[tck_idx].extra_vars['upcoming_max_peak'])
        #     tcks_priority[tck_idx].extra_vars['last_max_peaks_days'].append(-tcks_priority[tck_idx].extra_vars['current_max_delay'])

        #     if len(tcks_priority[tck_idx].extra_vars['last_max_peaks']) > ref_data['peaks_number']:
        #         tcks_priority[tck_idx].extra_vars['last_max_peaks'].pop(0)
        #         tcks_priority[tck_idx].extra_vars['last_max_peaks_days'].pop(0)

        #     tcks_priority[tck_idx].extra_vars['upcoming_max_peak'] = 0.0

        # if tcks_priority[tck_idx].extra_vars['current_min_delay'] >= \
        #     ref_data['peak_delay'] and tcks_priority[tck_idx].extra_vars['upcoming_min_peak'] != 0.0:

        #     tcks_priority[tck_idx].extra_vars['last_min_peaks'].append(tcks_priority[tck_idx].extra_vars['upcoming_min_peak'])
        #     tcks_priority[tck_idx].extra_vars['last_min_peaks_days'].append(-tcks_priority[tck_idx].extra_vars['current_min_delay'])

        #     if len(tcks_priority[tck_idx].extra_vars['last_min_peaks']) > ref_data['peaks_number']:
        #         tcks_priority[tck_idx].extra_vars['last_min_peaks'].pop(0)
        #         tcks_priority[tck_idx].extra_vars['last_min_peaks_days'].pop(0)

        #     tcks_priority[tck_idx].extra_vars['upcoming_min_peak'] = 0.0
        # # END-> Peak analysis: Put first peaks in buffer

        # if len(tcks_priority[tck_idx].extra_vars['last_max_peaks']) < ref_data['peaks_number'] \
        #     or len(tcks_priority[tck_idx].extra_vars['last_min_peaks']) < ref_data['peaks_number']:
        #     return False

    @staticmethod
    def _update_peaks_days(ticker_state):

        # for idx in range(len(ticker_state.extra_vars['last_max_peaks_days'])):
        #     ticker_state.extra_vars['last_max_peaks_days'][idx] -= 1

        # for idx in range(len(ticker_state.extra_vars['last_min_peaks_days'])):
        #     ticker_state.extra_vars['last_min_peaks_days'][idx] -= 1
        pass

    def _get_purchase_price(self, business_data):
        return business_data['open_price_day']

    def _check_business_rules(self, business_data, tcks_priority, tck_idx,
        purchase_price):

        # if not tcks_priority[tck_idx].last_business_data:
        #     return False

        # ref_price = purchase_price

        # # More negative day number means older
        # order = 'max_first' \
        #     if tcks_priority[tck_idx].extra_vars['last_max_peaks_days'][0] < \
        #         tcks_priority[tck_idx].extra_vars['last_min_peaks_days'][0] \
        #     else 'min_first'

        # if order == 'max_first':
        #     peak_1 = round(tcks_priority[tck_idx].extra_vars['last_max_peaks'][0] / ref_price, 4)
        #     day_1 = tcks_priority[tck_idx].extra_vars['last_max_peaks_days'][0]
        #     peak_2 = round(tcks_priority[tck_idx].extra_vars['last_min_peaks'][0] / ref_price, 4)
        #     day_2 = tcks_priority[tck_idx].extra_vars['last_min_peaks_days'][0]
        #     peak_3 = round(tcks_priority[tck_idx].extra_vars['last_max_peaks'][1] / ref_price, 4)
        #     day_3 = tcks_priority[tck_idx].extra_vars['last_max_peaks_days'][1]
        #     peak_4 = round(tcks_priority[tck_idx].extra_vars['last_min_peaks'][1] / ref_price, 4)
        #     day_4 = tcks_priority[tck_idx].extra_vars['last_min_peaks_days'][1]
        # elif order == 'min_first':
        #     peak_1 = round(tcks_priority[tck_idx].extra_vars['last_min_peaks'][0] / ref_price, 4)
        #     day_1 = tcks_priority[tck_idx].extra_vars['last_min_peaks_days'][0]
        #     peak_2 = round(tcks_priority[tck_idx].extra_vars['last_max_peaks'][0] / ref_price, 4)
        #     day_2 = tcks_priority[tck_idx].extra_vars['last_max_peaks_days'][0]
        #     peak_3 = round(tcks_priority[tck_idx].extra_vars['last_min_peaks'][1] / ref_price, 4)
        #     day_3 = tcks_priority[tck_idx].extra_vars['last_min_peaks_days'][1]
        #     peak_4 = round(tcks_priority[tck_idx].extra_vars['last_max_peaks'][1] / ref_price, 4)
        #     day_4 = tcks_priority[tck_idx].extra_vars['last_max_peaks_days'][1]

        # ema_17_day = round(tcks_priority[tck_idx].last_business_data['ema_17_day'] / ref_price, 4)
        # ema_72_day = round(tcks_priority[tck_idx].last_business_data['ema_72_day'] / ref_price, 4)
        # ema_72_week = round(tcks_priority[tck_idx].last_business_data['ema_72_week'] / ref_price, 4)

        if self.enable_crisis_halt:
            crisis_flag = self.ticker_day_risks.loc[
                (self.ticker_day_risks['ticker'] == tcks_priority[tck_idx].ticker) & \
                (self.ticker_day_risks['day'] == business_data['day'].strftime('%Y-%m-%d')), \
                ['crisis']].squeeze()

            if crisis_flag:
                return False

        # golden_purchase = self._check_golden_purchase(tcks_priority, tck_idx, purchase_price, business_data)
        # if golden_purchase:
        #     risk = self._get_risk(tcks_priority[tck_idx].ticker, business_data['day'], force=True)
        #     risk = risk * 2.1
        #     business_data['stop_loss_day'] = round(purchase_price * (1 - risk), 2)
        #     return True

        # if business_data['day'] == pd.Timestamp('2019-12-16T00'):
        #     print()

        risk = self._get_risk(tcks_priority[tck_idx].ticker, business_data['day'])
        if risk is None:
            return False

        if self.enable_downtrend_halt:
            downtrend_flag = self.ticker_day_risks.loc[
                (self.ticker_day_risks['ticker'] == tcks_priority[tck_idx].ticker) & \
                (self.ticker_day_risks['day'] == business_data['day'].strftime('%Y-%m-%d')), \
                ['downtrend']].squeeze()

            if downtrend_flag:
                return False


        mid_prices_dot = self.last_data[tcks_priority[tck_idx].ticker]['mid_dot']
        spearman_corrs = [0.0 for _ in self.spearman_correlations]

        for spear_idx, spear_n in enumerate(self.spearman_correlations):
            if len(self.last_data[tcks_priority[tck_idx].ticker]['mid']) >= spear_n:
                corr = stats.spearmanr(self.spearman_reference[:spear_n],
                    self.last_data[tcks_priority[tck_idx].ticker]['mid'][len(self.last_data[tcks_priority[tck_idx].ticker]['mid']) - spear_n : len(self.last_data[tcks_priority[tck_idx].ticker]['mid'])]).correlation

                if math.isnan(corr):
                    corr = 0.0

                spearman_corrs[spear_idx] = round(corr, 4)

        X_test = [[risk, mid_prices_dot, *spearman_corrs]]

        # X_test = [[risk, peak_1, day_1, peak_2, day_2, peak_3, day_3, peak_4, day_4,
        #     ema_17_day, ema_72_day, ema_72_week]]

        prediction = self.models[tcks_priority[tck_idx].ticker].predict(X_test)

        if prediction[0] == 1:
            # risk_boost = self._check_uptrend_risk_boost(tcks_priority, tck_idx, purchase_price, business_data)
            # if risk_boost:
            #     risk *= 1.1
            business_data['stop_loss_day'] = round(purchase_price * (1 - risk), 2)
            return True

        return False

    # def _check_golden_purchase(self, tcks_priority, tck_idx, purchase_price, business_data):

    #     if self.last_data[tcks_priority[tck_idx].ticker]['ols_rmse'] < 0.20:
    #         if 0 < self.last_data[tcks_priority[tck_idx].ticker]['ols_slope'] < 0.0027:
    #             if purchase_price > 1.10 * np.mean(self.last_data[tcks_priority[tck_idx].ticker]['mid']):
    #                 return True

    #     return False

    def _get_risk(self, ticker, day, force=False):

        min_risk = self.ticker_day_risks.loc[
            (self.ticker_day_risks['ticker'] == ticker) & \
            (self.ticker_day_risks['day'] == day.strftime('%Y-%m-%d')), ['min_risk']].squeeze()

        max_risk = self.ticker_day_risks.loc[
            (self.ticker_day_risks['ticker'] == ticker) & \
            (self.ticker_day_risks['day'] == day.strftime('%Y-%m-%d')), ['max_risk']].squeeze()

        if isinstance(min_risk, pd.Series) or isinstance(max_risk, pd.Series):
            return None

        if max_risk < min_risk:
            if force:
                return min_risk
            else:
                return None

        risk = (min_risk + max_risk) / 2

        return round(risk, 3)

    def _set_operation_purchase(self, ticker_name, purchase_price, stop_price,
        available_capital, rcc, tcks_priority, tck_idx, business_data,
        capital_multiplier=1.0):

        if self.enable_dynamic_rcc:
            rcc = self.dynamic_rcc_value

        amount = super()._set_operation_purchase(ticker_name, purchase_price,
            stop_price, available_capital, rcc, tcks_priority, tck_idx, business_data,
            capital_multiplier)
        tcks_priority[tck_idx].loaned = round(tcks_priority[tck_idx].loaned + amount, 2)

        return amount

    def _get_capital_multiplier(self, tcks_priority, tck_idx, business_data):

        multiplier = 1.0

        # Normalization due to operation frequency
        if self.enable_frequency_normalization:
            min_operations_for_freq_norm = 2 * len(self.tickers_and_dates)
            ticker_op_count = tcks_priority[tck_idx].op_count

            if ticker_op_count > 0 and self.total_op_count >= min_operations_for_freq_norm:

                ticker_freq = ticker_op_count / self.total_op_count
                target_avg_freq = 1 / len(self.tickers_and_dates)

                multiplier *= target_avg_freq / ticker_freq

        # Compensation due to relative individual profits
        if self.enable_profit_compensation:
            min_operations_for_profit_comp = 2 * len(self.tickers_and_dates)
            ticker_profit = tcks_priority[tck_idx].profit

            if self.total_op_count >= min_operations_for_profit_comp and \
                abs(ticker_profit) >= 1e-2:

                profit_mean, profit_std = self._get_profit_statistics(tcks_priority)

                partial_multiplier = MLDerivationStrategy._calc_profit_compensation(
                    ticker_profit, profit_mean, profit_std)

                multiplier *= partial_multiplier

        # Compensation due to uptrend
        # if self.enable_uptrend_compensation:
        #     uptrend_flag = self.ticker_day_risks.loc[
        #         (self.ticker_day_risks['ticker'] == tcks_priority[tck_idx].ticker) & \
        #         (self.ticker_day_risks['day'] == business_data['day'].strftime('%Y-%m-%d')), \
        #         ['uptrend']].squeeze()

        #     if uptrend_flag:
        #         multiplier *= 1.4
        #     # else:
        #     #     multiplier *= 0.8

        # if tcks_priority[tck_idx].ticker == 'ALPA4':
        #     if business_data['day'] == pd.Timestamp('2019-04-30T00') \
        #         or business_data['day'] == pd.Timestamp('2019-06-13T00') \
        #         or business_data['day'] == pd.Timestamp('2019-09-27T00') \
        #         or business_data['day'] == pd.Timestamp('2019-12-10T00'):
        #         if self.last_data[tcks_priority[tck_idx].ticker]['ols_rmse'] < 0.25:
        #             if self.last_data[tcks_priority[tck_idx].ticker]['ols_slope'] > 0.0045:
        #                 # multiplier *= 1.1
        #                 print()

        return multiplier

    @staticmethod
    def _calc_profit_compensation(target_profit, profit_mean, profit_std,
        max_bonus=0.6, start_std=0.2, end_std=2):

        # multiplier = 1.0
        # add_multiplier = 0.0

        # if profit_std != 0.0:
        #     target_std_equivalent = (target_profit - profit_mean) / profit_std

        #     if abs(target_std_equivalent) >= start_std:
        #         if target_profit > profit_mean:
        #             m = max_bonus / (end_std - start_std)
        #             n = -start_std * m

        #             add_multiplier = min(max_bonus,
        #                 target_std_equivalent * m + n)
        #         else:
        #             m = max_bonus / (end_std - start_std)
        #             n = start_std * m

        #             add_multiplier = max(-max_bonus,
        #                 target_std_equivalent * m + n)

        # return round(multiplier + add_multiplier, 5)

        if profit_std != 0.0:
            sigma_eq = (target_profit - profit_mean) / profit_std

            if sigma_eq > end_std:
                return 1.0 + max_bonus

            elif sigma_eq < -end_std:
                return 1.0 - max_bonus

            elif sigma_eq >= start_std and sigma_eq <= end_std:
                m1 = max_bonus / (end_std - start_std)
                n1 = 1 - start_std * m1
                return 1.0 + (m1 * sigma_eq + n1)

            elif sigma_eq >= -end_std and sigma_eq <= -start_std:
                m2 = max_bonus / (end_std - start_std)
                n2 = 1 + start_std * m2
                return 1.0 + (m2 * sigma_eq + n2)

        # Default: profit_std == 0.0 or abs(sigma_eq) < start_std
        return 1.0

    def _sell_on_stop_hit(self, tcks_priority, tck_idx, business_data):

        amount = super()._sell_on_stop_hit(tcks_priority, tck_idx, business_data)

        if amount >= 1e-2:
            profit = amount - tcks_priority[tck_idx].loaned
            tcks_priority[tck_idx].profit = round(tcks_priority[tck_idx].profit + profit, 2)
            self.total_profit = round(self.total_profit + profit, 2)
            tcks_priority[tck_idx].loaned = 0.0
            tcks_priority[tck_idx].op_count += 1

            self.total_op_count += 1
            self.max_capital = round(self.max_capital + profit, 2)

        return amount

    def _sell_on_partial_hit(self, tcks_priority, tck_idx, business_data):

        amount = super()._sell_on_partial_hit(tcks_priority, tck_idx, business_data)

        if amount >= 1e-2:
            profit = amount - tcks_priority[tck_idx].loaned
            tcks_priority[tck_idx].profit = round(tcks_priority[tck_idx].profit + profit, 2)
            self.total_profit = round(self.total_profit + profit, 2)
            tcks_priority[tck_idx].loaned = 0.0

            self.total_op_count += 1
            self.max_capital = round(self.max_capital + profit, 2)

        return amount

    def _sell_on_target_hit(self, tcks_priority, tck_idx, business_data):

        amount = super()._sell_on_target_hit(tcks_priority, tck_idx, business_data)

        if amount >= 1e-2:
            profit = amount - tcks_priority[tck_idx].loaned
            tcks_priority[tck_idx].profit = round(tcks_priority[tck_idx].profit + profit, 2)
            self.total_profit = round(self.total_profit + profit, 2)
            tcks_priority[tck_idx].loaned = 0.0
            tcks_priority[tck_idx].op_count += 1
            tcks_priority[tck_idx].op_suc_count += 1

            self.total_op_count += 1
            self.total_op_suc_count += 1
            self.max_capital = round(self.max_capital + profit, 2)

        return amount

    def _sell_on_timeout_hit(self, tcks_priority, tck_idx, business_data):

        amount = super()._sell_on_timeout_hit(tcks_priority, tck_idx, business_data)

        if amount >= 1e-2:
            profit = amount - tcks_priority[tck_idx].loaned
            tcks_priority[tck_idx].profit = round(tcks_priority[tck_idx].profit + profit, 2)
            self.total_profit = round(self.total_profit + profit, 2)
            tcks_priority[tck_idx].loaned = 0.0
            tcks_priority[tck_idx].op_count += 1

            self.total_op_count += 1
            self.max_capital = round(self.max_capital + profit, 2)

        return amount

    def _get_profit_statistics(self, tcks_priority):

        profits = np.array([card.profit for card in tcks_priority])
        mean = self.total_profit / len(self.tickers_and_dates)
        std = np.std(profits)

        return mean, std

    def _order_by_priority(self, tcks_priority, day):

        if day.date() >= self.first_date:
            # Method 1
            open_operation = 8
            # uptrend= 4
            # accuracy = 2
            # profit = 1

            pontuation = [0] * len(tcks_priority)

            # avg_profit, _ = self._get_profit_statistics(tcks_priority)
            # avg_acc = self.total_op_suc_count / self.total_op_count if self.total_op_count > 0 else 0

            for idx, ticker_card in enumerate(tcks_priority):
                if ticker_card.operation is not None and ticker_card.operation.state == State.OPEN:
                    pontuation[idx] += open_operation
                    continue

                # if isinstance(day, pd.Timestamp):
                #     uptrend_flag = self.ticker_day_risks.loc[
                #         (self.ticker_day_risks['ticker'] == ticker_card.ticker) & \
                #         (self.ticker_day_risks['day'] == day.strftime('%Y-%m-%d')), \
                #         ['uptrend']].squeeze()
                #     if (not isinstance(uptrend_flag, pd.Series)) and uptrend_flag:
                #         pontuation[idx] += uptrend

                # if self.total_op_count > 2 * len(self.tickers_and_dates):
                #     if ticker_card.op_count == 0 or \
                #         (ticker_card.op_count > 0 and (ticker_card.op_suc_count / ticker_card.op_count) >= avg_acc):
                #         pontuation[idx] += accuracy

                # if ticker_card.profit >= avg_profit:
                #     pontuation[idx] += profit
                # elif ticker_card.profit < 1e-2 and ticker_card.profit > (1 - 1e-2):
                #     pontuation[idx] += profit

            new_order = [tck for tck in sorted(tcks_priority,
                key=lambda card: pontuation[tcks_priority.index(card)], reverse=True)]

            # Method 2
            # new_order = sorted(tcks_priority, key=lambda ticker_state: \
            #     str(int(ticker_state.operation.state == State.OPEN
            #     if ticker_state.operation is not None else 0)) \
            #     + str(int(ticker_state.operation.state == State.NOT_STARTED
            #     if ticker_state.operation is not None else 0)), reverse=True)

            return new_order

        return tcks_priority

    def _update_global_stats(self, day):

        if day.date() >= self.first_date:
            self.capital_in_use = (self.max_capital - self.available_capital) / self.max_capital

            self.capital_in_use_last_values.append( self.capital_in_use )
            if len(self.capital_in_use_last_values) > self.n_avg:
                self.capital_in_use_last_values.pop(0)

            self.last_capital_in_use_mavg = self.capital_in_use_mavg

            if not self.first_update:

                self.capital_in_use_mavg = np.mean( self.capital_in_use_last_values )

                # self.capital_in_use_dot = self.der_lpf_alpha * (self.capital_in_use_mavg - self.last_capital_in_use_mavg) \
                #     + (1 - self.der_lpf_alpha) * (self.capital_in_use_dot)

                self._update_dynamic_rcc()
            else:
                self.capital_in_use_mavg = self.capital_in_use
                # self.capital_in_use_dot = 0.0
                self.first_update = False

    def _update_dynamic_rcc(self):

        error = self.dynamic_rcc_reference - self.capital_in_use_mavg
        # error_dot = max(0, -self.capital_in_use_dot)
        self.dynamic_rcc_value = self.risk_capital_product * (1 + self.dynamic_rcc_k * error)
