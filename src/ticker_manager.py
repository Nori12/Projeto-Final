from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import sys
import yfinance as yf
from scipy.signal import find_peaks
from scipy.signal import find_peaks
import math
from bisect import bisect

import constants as c
from utils import has_workdays_in_between, RunTime
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

class TickerManager:
    """
    Ticker manager responsible for requesting and updating data, as long as creating new features.

    Data source is Yahoo Finance and destination is database.

    Verify date intervals in database then requests missing data.
    If a new split is perceived, all previous data is updated.

    Currently working with candlesticks in daily and weekly intervals only.

    Features supported:
        - Peaks : maximum and minimum.
        - Exponential Moving Average (17 points)
        - Exponential Moving Average (72 points)
        - Up Down Trend Status : indicate whether in uptrend, downtrend or consolidation.

    Args
    ----------
    ticker : str
        Ticker name.
    start_date : `datetime.date`
        Start date.
    end_date : `datetime.date`
        End date.
    ordinary_ticker : bool, default True
        Indication whether is ordinary or not (e.g., index, curency).
        Non-ordinary tickers are not able to have features.
    holidays : list of `datetime.date`, optional
        Holidays.

    Attributes
    ----------
    _ticker : str
        Ticker name.
    _start_date : `datetime.date`
        Start date.
    _end_date : `datetime.date`
        End date.
    _ordinary_ticker : bool
        Indication whether is ordinary or not (e.g., index, curency).
        Non-ordinary tickers are not able to have features.
    _holidays : list of `datetime.date`
        Holidays.

    Attributes (class)
    ----------
    ticker_count : dict
        Number of tickers.
    db_ticker_model : 'DBTickerModel'
        Database connection class. Must be static to limit number of connections.
    """
    ticker_count = 0
    db_ticker_model = DBTickerModel()

    @RunTime('TickerManager.__init__')
    def __init__(self, ticker, start_date, end_date, ordinary_ticker=True, holidays=None):
        self._ticker = ticker.upper()
        self._start_date = start_date
        self._end_date = end_date
        self.ordinary_ticker = ordinary_ticker

        self._holidays = []
        if holidays is not None:
            self._holidays = holidays

        TickerManager.ticker_count += 1

    @property
    def holidays(self):
        """list of `datetime.date` : Holidays."""
        return self._holidays

    @holidays.setter
    def holidays(self, holidays):
        self._holidays = holidays

    @property
    def ticker(self):
        """str : Ticker name."""
        return self._ticker

    @property
    def start_date(self):
        """`datetime.date` : Start date."""
        return self._start_date

    @property
    def end_date(self):
        """`datetime.date` : End date."""
        return self._end_date

    def update(self):
        """
        Update ticker data.

        Currently working with candlesticks in daily and weekly intervals.
        """
        self._update_missing_data()

        # Only common tickers should have derived candlesticks
        # Indexes (IBOV, S&P500, ...) does not need it
        if self.ordinary_ticker == True:
            self._update_weekly_candles()

    def _update_missing_data(self):
        """
        Update ticker daily candlesticks.

        Three cases are handled:
                                ----------------------------> +
        Database interval:                |---------|
        Requested interval:     |-----------------------------|
                                |--Case1--|--Case2--|--Case3--|

        Case 1: Database lacks oldest data.
        Case 2: Database already contains the interval.
        Case 3: Database lacks most recent data.

        If Case 3 has a split, all previous data is updated.

        Returns
        ----------
        bool
            True if update was necessary, False if not.
        """
        last_update_candles = None
        start_date_in_db = None
        end_date_in_db = None

        try:
            # Get database interval range
            date_range_df = TickerManager.db_ticker_model.get_date_range(self.ticker)
            if not date_range_df.empty:
                last_update_candles = date_range_df['last_update_daily_candles'][0]
                if last_update_candles is not None:
                    last_update_candles = last_update_candles.to_pydatetime().date()

                start_date_in_db = date_range_df['start_date_daily_candles'][0]
                if start_date_in_db is not None:
                    start_date_in_db = start_date_in_db.to_pydatetime().date()

                end_date_in_db = date_range_df['end_date_daily_candles'][0]
                if end_date_in_db is not None:
                    end_date_in_db = end_date_in_db.to_pydatetime().date()

            # Some data exist in database
            if all([last_update_candles, start_date_in_db, end_date_in_db]):

                # Check if requested interval is already in datebase
                if (has_workdays_in_between(self.start_date, start_date_in_db,
                    self.holidays, consider_oldest_date=True) == False
                    and has_workdays_in_between(end_date_in_db, self.end_date,
                    self.holidays, consider_recent_date=True) == False):
                    logger.info(f"""Ticker \'{self.ticker}\' already updated.""")
                    return False

                # Database lacks most recent data
                if has_workdays_in_between(end_date_in_db, self.end_date, self.holidays,
                    consider_recent_date=True) == True:
                    self._update_candles_splits_dividends(end_date_in_db+timedelta(days=1), self.end_date,
                        split_nomalization_date=start_date_in_db,
                        last_update_date=last_update_candles)

                # Database lacks oldest data
                if has_workdays_in_between(self.start_date, start_date_in_db,
                    self.holidays, consider_oldest_date=True) == True:
                    self._update_candles_splits_dividends(self.start_date,
                        start_date_in_db-timedelta(days=1))
            else:
                self._update_candles_splits_dividends(self.start_date, self.end_date)

            logger.info(f"""Ticker \'{self.ticker}\' daily candles update finished.""")

        except Exception as error:
            logger.exception(f"""Error updating daily candles, error: {error}""")
            sys.exit(c.UPDATING_DB_ERR)

        return True

    # TODO: Mudar split_nomalization_date e last_update_date para None
    def _update_candles_splits_dividends(self, start_date, end_date,
        split_nomalization_date=datetime.max, last_update_date=datetime.max):
        """
        Update candlesticks, splits and dividends in database.

        Args
        ----------
        start_date : `datetime.date`
            Start date.
        end_date : `datetime.date`
            End date.
        split_nomalization_date : `datetime.date`,

        last_update_date : `datetime.date`,

        """
        update_flag = False

        if split_nomalization_date != datetime.max and split_nomalization_date > start_date:
            logger.error(f"Error argument \'split_nomalization_date\'=\'{split_nomalization_date}\' is greater than \'start_date\'={start_date}.")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if last_update_date != datetime.max and last_update_date < split_nomalization_date:
            logger.error(f"Error argument \'last_update_date\'=\'{last_update_date}\' is less than \'split_nomalization_date\'={split_nomalization_date}.")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if bool(split_nomalization_date != datetime.max) != bool(last_update_date != datetime.max):
            logger.error(f"Error arguments \'split_nomalization_date\' and \'last_update_date\' must be provided together.")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if split_nomalization_date != datetime.max and last_update_date != datetime.max:
            update_flag = True

        new_candles = self._get_data(start_date, end_date)

        if new_candles.empty:
            logger.warning(f"""yfinance has no data for ticker \'{self.ticker}\' (\'{start_date.strftime('%Y-%m-%d')}\', \'{end_date.strftime('%Y-%m-%d')}\').""")
            return False

        if update_flag == True:
            new_splits, cumulative_splits = self._verify_splits(new_candles, last_update_threshold=last_update_date)
        else:
            new_splits, cumulative_splits = self._verify_splits(new_candles)

        new_dividends = self._verify_dividends(new_candles)

        if self.ordinary_ticker == True:
            new_candles.drop(['Dividends', 'Stock Splits'], axis=1, inplace=True)
            new_candles.replace(0, np.nan, inplace=True)
            new_candles.dropna(axis=0, how='any', inplace=True)
        else:
            new_candles.drop(new_candles[(new_candles['Open'] == 0) | (new_candles['High'] == 0) | (new_candles['Low'] == 0) | (new_candles['Close'] == 0)].index, inplace=True)

        self._adjust_to_dataframe_constraints(new_candles)

        if new_candles.empty:
            logger.warning(f"""No valid data to update for ticker \'{self.ticker}\', (\'{start_date.strftime('%Y-%m-%d')}\', \'{end_date.strftime('%Y-%m-%d')}\').""")
            return False

        # Insert candles
        TickerManager.db_ticker_model.insert_daily_candles(self.ticker, new_candles)

        # Insert splits
        if not new_splits.empty:
            TickerManager.db_ticker_model.upsert_splits(self.ticker, new_splits)

        # Insert dividends
        if not new_dividends.empty:
            TickerManager.db_ticker_model.upsert_dividends(self.ticker, new_dividends)

        # Normalize candles
        if update_flag == True and cumulative_splits != 1.0:
            TickerManager.db_ticker_model.update_daily_candles_with_split(self.ticker, split_nomalization_date, start_date, cumulative_splits)

    def _get_data(self, start_date, end_date, interval='1d'):
        """
        Request Yahoo Finance candlesticks data.

        Closed interval.

        Args
        ----------
        start_date : `datetime.date`
            Start date.
        end_date : `datetime.date`
            End date.

        Returns
        ----------
        `pandas.DataFrame`
            DataFrame with candles.
        """
        ticker = self.ticker

        if self.ordinary_ticker == True:
            ticker += '.SA'

        try:
            msft = yf.Ticker(ticker)
            if interval == '1d':
                hist = msft.history(start=start_date.strftime('%Y-%m-%d'),
                    end=(end_date+timedelta(days=1)).strftime('%Y-%m-%d'), prepost=True, back_adjust=True,
                    rounding=True)
            else:
                hist = None
        except Exception as error:
            logger.error('Error getting yfinance data, error: {}'.format(error))
            sys.exit(c.YFINANCE_ERR)

        return hist

    def _verify_splits(self, data, last_update_threshold=datetime.max):

        cumulative_splits = 1.0
        data2 = data[data['Stock Splits'] != 0].copy()

        for index, row in data2.iterrows():
            if index.date() >= last_update_threshold.date():
                cumulative_splits = cumulative_splits * row['Stock Splits']

        return data2, cumulative_splits

    def _verify_dividends(self, data):
        return data[data['Dividends'] != 0].copy()

    def _update_weekly_candles(self):
        TickerManager.db_ticker_model.delete_weekly_candles(self.ticker)
        TickerManager.db_ticker_model.create_weekly_candles_from_daily(self.ticker)

        logger.info(f"""Ticker \'{self.ticker}\' weekly candles update finished.""")

    def _adjust_to_dataframe_constraints(self, df):

        df['Low'] = df.apply(lambda x: x['Close'] if x['Low'] > x['Close'] else x['Low'], axis=1)
        df['Low'] = df.apply(lambda x: x['Open'] if x['Low'] > x['Open'] else x['Low'], axis=1)
        df['High'] = df.apply(lambda x: x['Close'] if x['High'] < x['Close'] else x['High'], axis=1)
        df['High'] = df.apply(lambda x: x['Open'] if x['High'] < x['Open'] else x['High'], axis=1)

    def generate_features(self):

        if self.ordinary_ticker == True:
            self._generate_features(interval='1d', trend_method='ema_derivative', ema_derivative_alpha=0.95, consolidation_tolerance=0.05)
            self._generate_features(interval='1wk', trend_method='ema_derivative', ema_derivative_alpha=0.95, consolidation_tolerance=0.05)

    def _generate_features(self, interval = '1d', trend_method='ema_derivative', ema_derivative_alpha=0.9, consolidation_tolerance=0.01):

        if trend_method not in ['ema_derivative', 'peaks']:
            logger.error(f"Program aborted. Error argument \'trend_method\'=\'{trend_method}\' is not valid.")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if ema_derivative_alpha < 0 or ema_derivative_alpha > 1:
            logger.error(f"Program aborted. Error argument \'ema_72_weight\'=\'{ema_derivative_alpha}\' must be in interval [0,1].")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        candles_min_peak_distance = 17
        analysis_status = {'UPTREND': 1, 'DOWNTREND': -1, 'CONSOLIDATION': 0}

        candles = TickerManager.db_ticker_model.get_candles_dataframe(self.ticker, None, self.end_date, interval=interval)

        max_peaks_index = find_peaks(candles['max_price'], distance=candles_min_peak_distance)[0].tolist()
        min_peaks_index = find_peaks(1.0/candles['min_price'], distance=candles_min_peak_distance)[0].tolist()

        # Peaks Calculation - Max an min peaks must be altenate each other. So deleting duplicate sequences of ax or min...
        for i in range(1, len(max_peaks_index)):
            delete_candidates = [j for j in min_peaks_index if j >= max_peaks_index[i-1] and j < max_peaks_index[i]]
            if len(delete_candidates) > 1:
                delete_candidates_values = [candles.iloc[i, candles.columns.get_loc('min_price')] for i in delete_candidates]
                delete_candidates.remove(delete_candidates[delete_candidates_values.index(min(delete_candidates_values))])
                min_peaks_index = [i for i in min_peaks_index if i not in delete_candidates]

        for i in range(1, len(min_peaks_index)):
            delete_candidates = [j for j in max_peaks_index if j >= min_peaks_index[i-1] and j < min_peaks_index[i]]
            if len(delete_candidates) > 1:
                delete_candidates_values = [candles.iloc[i, candles.columns.get_loc('max_price')] for i in delete_candidates]
                delete_candidates.remove(delete_candidates[delete_candidates_values.index(max(delete_candidates_values))])
                max_peaks_index = [i for i in max_peaks_index if i not in delete_candidates]

        peaks = max_peaks_index + min_peaks_index
        peaks.sort()

        # Peaks Calculation - Remove monotonic peak sequences
        delete_candidates = []
        for i in range(len(peaks)):
            if i >= 2:
                current_value = 0
                if peaks[i] in max_peaks_index:
                    current_value = candles.iloc[peaks[i], candles.columns.get_loc('max_price')]
                else:
                    current_value = candles.iloc[peaks[i], candles.columns.get_loc('min_price')]

                ultimate_value = 0
                if peaks[i-1] in max_peaks_index:
                    ultimate_value = candles.iloc[peaks[i-1], candles.columns.get_loc('max_price')]
                else:
                    ultimate_value = candles.iloc[peaks[i-1], candles.columns.get_loc('min_price')]

                penultimate_value = 0
                if peaks[i-2] in max_peaks_index:
                    penultimate_value = candles.iloc[peaks[i-2], candles.columns.get_loc('max_price')]
                else:
                    penultimate_value = candles.iloc[peaks[i-2], candles.columns.get_loc('min_price')]

                if ((current_value > ultimate_value and ultimate_value > penultimate_value) or
                    (current_value < ultimate_value and ultimate_value < penultimate_value)):

                    if peaks[i-1] in max_peaks_index:
                        max_peaks_index.remove(peaks[i-1])
                    else:
                        min_peaks_index.remove(peaks[i-1])

        # Exponential Moving Average
        ema_17 = candles.iloc[:,candles.columns.get_loc('close_price')].ewm(span=17, adjust=False).mean()
        ema_72 = candles.iloc[:,candles.columns.get_loc('close_price')].ewm(span=72, adjust=False).mean()


        # Up-Down-Trend Status
        if trend_method == 'ema_derivative':

            # Calculate ema_72 derivative
            ema_72_derivative_raw = [0]
            ema_72_derivative_raw.extend([a - b for a, b in zip(ema_72[1:], ema_72[:-1])])

            # Soft low-pass-filter after derivative
            lpf_alpha = 0.80

            ema_72_derivative = [ema_72_derivative_raw[0]]
            last_value = ema_72_derivative_raw[0]
            for i in range(1, len(ema_72_derivative_raw)):
                value = lpf_alpha * ema_72_derivative_raw[i] + (1-lpf_alpha) * last_value
                ema_72_derivative.append(value)
                last_value = value

            # Calculate ema_17 derivative
            ema_17_derivative_raw = [0]
            ema_17_derivative_raw.extend([a - b for a, b in zip(ema_17[1:], ema_17[:-1])])

            # Soft low-pass-filter after derivative
            ema_17_derivative = [ema_17_derivative_raw[0]]
            last_value = ema_17_derivative_raw[0]
            for i in range(1, len(ema_17_derivative_raw)):
                value = lpf_alpha * ema_17_derivative_raw[i] + (1-lpf_alpha) * last_value
                ema_17_derivative.append(value)
                last_value = value

            udt_coef = [ema_derivative_alpha * ema_72_dot + (1 - ema_derivative_alpha) * ema_17_dot for ema_17_dot, ema_72_dot in zip(ema_17_derivative, ema_72_derivative)]

            # Equation: alpha * m_72_dot + (1-alpha) * m_17_dot = 0
            # If > tolerance: UP_TREND
            # If < -tolerance: DOWN_TREND
            # Else: CONSOLIDATION
            udt_status = [analysis_status['UPTREND'] if coef > consolidation_tolerance else analysis_status['DOWNTREND'] if coef < -consolidation_tolerance else analysis_status['CONSOLIDATION'] for coef in udt_coef]

        elif trend_method == 'peaks':
            udt_coef = []
            peaks_index_list = [line[1] for line in peaks]

            for index, (_, row) in enumerate(candles.iterrows()):
                # At least 3 peaks are required
                last_peak_index = bisect(peaks_index_list, index) - 1
                if last_peak_index >= 3:# and index != peaks[last_peak_index][1]:
                    if peaks[last_peak_index][0] == 'max_price':
                        max_peak_value_1 = peaks[last_peak_index-2][2]
                        min_peak_value_1 = peaks[last_peak_index-1][2]
                        max_peak_value_2 = peaks[last_peak_index][2]
                        min_peak_value_2 = row['close_price']
                    elif peaks[last_peak_index][0] == 'min_price':
                        min_peak_value_1 = peaks[last_peak_index-2][2]
                        max_peak_value_1 = peaks[last_peak_index-1][2]
                        min_peak_value_2 = peaks[last_peak_index][2]
                        max_peak_value_2 = row['close_price']

                    percent_max = max_peak_value_2 / max_peak_value_1 - 1 # x
                    percent_min = min_peak_value_2 / min_peak_value_1 - 1 # y

                    # d = abs((a * x1 + b * y1 + c)) / (math.sqrt(a * a + b * b))
                    # Line: x + y = 0
                    # d = percent_max + percent_min / (math.sqrt(2))
                    d = (percent_max + percent_min) / (math.sqrt(2))

                    udt_coef.append(math.tanh(d))

                else:
                    udt_coef.append(np.nan)

            udt_status = [analysis_status['UPTREND'] if coef > consolidation_tolerance else analysis_status['DOWNTREND'] if coef < -consolidation_tolerance else analysis_status['CONSOLIDATION'] for coef in udt_coef]

        peaks = [ ['max_price', index, candles.iloc[index, candles.columns.get_loc('max_price')]] for index in max_peaks_index ] + \
            [ ['min_price', index, candles.iloc[index, candles.columns.get_loc('min_price')]] for index in min_peaks_index ]
        peaks.sort(key=lambda x: x[1])

        candles['peak'] = [1 if index in max_peaks_index else -1 if index in min_peaks_index else 0 for index in range(candles.index.size)]
        candles['ema_17'] = ema_17
        candles['ema_72'] = ema_72
        candles['up_down_trend_status'] = udt_status

        TickerManager.db_ticker_model.upsert_features(candles, interval=interval)

        logger.info(f"""Ticker \'{self.ticker}\' {'daily' if interval=='1d' else 'weekly'} features generated.""")
