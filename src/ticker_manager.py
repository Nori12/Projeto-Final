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

    Properties
    ----------
    ticker : str
        Ticker name.
    start_date : `datetime.date`
        Start date.
    end_date : `datetime.date`
        End date.
    ordinary_ticker : bool
        Indication whether is ordinary or not (e.g., index, curency).
        Non-ordinary tickers are not able to have features.
    holidays : list of `datetime.date`
        Holidays.

    Methods
    ----------
    update()
        Update ticker data.
    generate_features(trend_status_ema_weight=0.95, consolidation_tolerance=0.05,
        lpf_alpha=0.8)
        Generate features from daily and weekly candlesticks and save in database.
    """
    db_ticker_model = DBTickerModel()

    def __init__(self, ticker, start_date, end_date, ordinary_ticker=True, holidays=None):
        self._ticker = ticker.upper()
        self._start_date = start_date
        self._end_date = end_date
        self._ordinary_ticker = ordinary_ticker

        self._holidays = []
        if holidays is not None:
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

    @property
    def ordinary_ticker(self):
        """bool : Ordinary ticker indicator."""
        return self._ordinary_ticker

    @property
    def holidays(self):
        """list of `datetime.date` : Holidays."""
        return self._holidays

    @holidays.setter
    def holidays(self, holidays):
        self._holidays = holidays

    @RunTime('TickerManager.update')
    def update(self):
        """
        Update ticker data.

        Currently working with candlesticks in daily and weekly intervals.

        Returns
        ----------
        bool
            True if any update was necessary, False if not.
        """
        try:
            update_happened = self._update_missing_daily_data()

            # Only common tickers should have derived candlesticks
            # Indexes (IBOV, S&P500, ...) does not need it
            if update_happened == True and self._ordinary_ticker == True:
                self._update_weekly_candles()
        except Exception as error:
            logger.exception(f"Error updating daily candles, error: {error}")
            sys.exit(c.UPDATING_DB_ERR)

        return update_happened

    def _update_missing_daily_data(self):
        """
        Update ticker daily candlesticks.

        Three cases are handled, in order:
                                ----------------------------> +
        Database interval:                |---------|
        Requested interval:     |-----------------------------|
                                |--Case3--|--Case2--|--Case1--|

        Case 1: Database lacks most recent data.
        Case 2: Database already contains the interval.
        Case 3: Database lacks oldest data.

        If Case 3 has a split, normalize Case2 data.

        Returns
        ----------
        bool
            True if any update was necessary, False if not.
        """
        last_update_in_db = None
        start_date_in_db = None
        end_date_in_db = None
        update_happened = False

        # Get database interval range
        date_range_df = TickerManager.db_ticker_model.get_date_range(self.ticker)
        if not date_range_df.empty:
            last_update_in_db = date_range_df['last_update_daily_candles'][0]
            if last_update_in_db is not None:
                last_update_in_db = last_update_in_db.to_pydatetime().date()

            start_date_in_db = date_range_df['start_date_daily_candles'][0]
            if start_date_in_db is not None:
                start_date_in_db = start_date_in_db.to_pydatetime().date()

            end_date_in_db = date_range_df['end_date_daily_candles'][0]
            if end_date_in_db is not None:
                end_date_in_db = end_date_in_db.to_pydatetime().date()

        # Some data exist in database
        if all([last_update_in_db, start_date_in_db, end_date_in_db]):

            # Check if requested interval is already in datebase
            if (has_workdays_in_between(self.start_date, start_date_in_db,
                self.holidays, consider_oldest_date=True) == False
                and has_workdays_in_between(end_date_in_db, self.end_date,
                self.holidays, consider_recent_date=True) == False):
                logger.info(f"Ticker \'{self.ticker}\' already updated.")
                return False

            # Database lacks most recent data
            if has_workdays_in_between(end_date_in_db, self.end_date, self.holidays,
                consider_recent_date=True) == True:
                update_flag = self._update_candles_splits_and_dividends(end_date_in_db+timedelta(days=1),
                    self.end_date, oldest_date_in_db=start_date_in_db,
                    last_update_in_db=last_update_in_db)
                if update_flag == True:
                    update_happened = True

            # Database lacks oldest data
            if has_workdays_in_between(self.start_date, start_date_in_db,
                self.holidays, consider_oldest_date=True) == True:
                update_flag = self._update_candles_splits_and_dividends(self.start_date,
                    start_date_in_db-timedelta(days=1))
                if update_flag == True:
                    update_happened = True
        else:
            update_flag = self._update_candles_splits_and_dividends(self.start_date, self.end_date)
            if update_flag == True:
                update_happened = True

        if update_happened == True:
            logger.info(f"Ticker \'{self.ticker}\' updated daily candlesticks.")
        else:
            logger.info(f"Ticker \'{self.ticker}\' did not updated daily candlesticks.")

        return update_happened

    def _update_candles_splits_and_dividends(self, start_date, end_date,
        oldest_date_in_db=None, last_update_in_db=None):
        """
        Update daily candlesticks, splits and dividends in database.

        Handle normalization due to presence of new splits after the last update
        date in database.

        If `oldest_date_in_db` or `last_update_in_db` is provided, check if database
        data until `oldest_date_in_db` needs normalization due to new splits.
        In this case, it is assumed that the updating interval is after the one in
        database, otherwise an error is launched.

        Notes
        ----------
        Check `_update_missing_daily_data` description.

        Args
        ----------
        start_date : `datetime.date`
            Start date.
        end_date : `datetime.date`
            End date.
        oldest_date_in_db : `datetime.date`, optional
            Oldest candlestick date in database.
            Must be provided with `last_update_in_db`.
        last_update_in_db : `datetime.date`, optional
            Last update date of database candlesticks.
            Must be provided with `oldest_date_in_db`.

        Returns
        ----------
        bool
            True if any update was necessary, False if not.
        """
        able_to_normalize = False

        # Optional arguments must be provided together.
        if (oldest_date_in_db is not None) != (last_update_in_db is not None):
            logger.error(f"Error arguments \'oldest_date_in_db\' and "
                f"\'last_update_in_db\' must be provided together.")
            sys.exit(c.INVALID_ARGUMENT_ERR)
        elif oldest_date_in_db is not None:
            able_to_normalize = True

        # start_date must be greater than oldest_date_in_db
        if oldest_date_in_db is not None and oldest_date_in_db >= start_date:
            logger.error(f"Error argument \'oldest_date_in_db\'=\'{oldest_date_in_db}\' "
                f"is greater than or equals to \'start_date\'={start_date}.")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        candles_df = self._get_yfinance_candles(start_date, end_date)

        if candles_df.empty:
            logger.warning(f"Yahoo Finance has no data for ticker \'{self.ticker}\' "
                f"(\'{start_date.strftime('%Y-%m-%d')}\', \'{end_date.strftime('%Y-%m-%d')}\').")
            return False

        # Get new splits to save in database, also multiply those which are after
        # the last update date to compute the normalization_factor.
        splits_df, normalization_factor = self._get_splits_and_norm_factor(candles_df,
            last_update_date=last_update_in_db)

        dividends_df = candles_df[candles_df['Dividends'] != 0].copy()

        # Yahoo Finance consistency on data changes if ticker is not ordinary
        if self._ordinary_ticker == True:
            candles_df.drop(['Dividends', 'Stock Splits'], axis=1, inplace=True)
            candles_df.replace(0, np.nan, inplace=True)
            candles_df.dropna(axis=0, how='any', inplace=True)
        else:
            candles_df.drop(candles_df[(candles_df['Open'] == 0) |
                (candles_df['High'] == 0) | (candles_df['Low'] == 0) |
                (candles_df['Close'] == 0)].index, inplace=True)

        self._adjust_to_database_constraints(candles_df)

        if candles_df.empty:
            logger.warning(f"No valid data to update for ticker \'{self.ticker}\', "
                f"(\'{start_date.strftime('%Y-%m-%d')}\', \'{end_date.strftime('%Y-%m-%d')}\').")
            return False

        # Insert candles
        TickerManager.db_ticker_model.insert_daily_candles(self.ticker, candles_df,
            ordinary_ticker=self.ordinary_ticker)
        # Insert splits
        if not splits_df.empty:
            TickerManager.db_ticker_model.upsert_splits(self.ticker, splits_df)
        # Insert dividends
        if not dividends_df.empty:
            TickerManager.db_ticker_model.upsert_dividends(self.ticker, dividends_df)
        # Normalize previous candles in database if needed
        if able_to_normalize == True and normalization_factor != 1.0:
            TickerManager.db_ticker_model.normalize_daily_candles(self.ticker,
                oldest_date_in_db, start_date-timedelta(days=1), normalization_factor)

        return True

    def _get_yfinance_candles(self, start_date, end_date):
        """
        Request Yahoo Finance daily candlesticks data.

        Closed interval.

        Yahoo Finance daily candlesticks are the most reliable interval.
        Greater time scales should be fabricated and not requested.

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
        hist = None

        if self._ordinary_ticker == True:
            ticker += '.SA'

        try:
            msft = yf.Ticker(ticker)
            hist = msft.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=(end_date+timedelta(days=1)).strftime('%Y-%m-%d'),
                prepost=True, back_adjust=True, rounding=True)
        except Exception as error:
            logger.error('Error getting yfinance data, error: {}'.format(error))
            sys.exit(c.YFINANCE_ERR)

        return hist

    def _get_splits_and_norm_factor(self, candles_df, last_update_date=None):
        """
        Get new dataframe of splits and calculate the normalization factor.

        Normalization factor is composed by the product of the splits after
        `last_update_date` date. So if not provided, the factor will be 1.0 by
        default.

        Args
        ----------
        candles_df : `pandas.DataFrame`
            DataFrame of candles with a column 'Stock Splits'.
        last_update_date : `datetime.date`, optional
            Last update date for normalization factor computation.

        Returns
        ----------
        `pandas.DataFrame`
            DataFrame only containing rows which have split data.
        float, default 1.0
            Normalization factor.
        """
        normalization_factor = 1.0
        splits_df = candles_df[candles_df['Stock Splits'] != 0].copy()

        if last_update_date is not None:
            for index, row in splits_df.iterrows():
                if index.date() >= last_update_date:
                    normalization_factor = normalization_factor * row['Stock Splits']

        return splits_df, normalization_factor

    def _adjust_to_database_constraints(self, candles_df):
        """
        Verify and correct candlestick data according to database constraints.

        Args
        ----------
        candles_df : `pandas.DataFrame`
            DataFrame of candles with columns 'Open', 'High', 'Low', 'Close'.
        """
        candles_df['Low'] = candles_df.apply(lambda x:
            x['Close'] if x['Low'] > x['Close'] else x['Low'], axis=1)
        candles_df['Low'] = candles_df.apply(lambda x:
            x['Open'] if x['Low'] > x['Open'] else x['Low'], axis=1)
        candles_df['High'] = candles_df.apply(lambda x:
            x['Close'] if x['High'] < x['Close'] else x['High'], axis=1)
        candles_df['High'] = candles_df.apply(lambda x:
            x['Open'] if x['High'] < x['Open'] else x['High'], axis=1)

    def _update_weekly_candles(self):
        """
        Update weekly candlestick data by deleting and recalculating.

        Weekly data is created from daily data to ensure consistency.
        """
        TickerManager.db_ticker_model.delete_weekly_candles(self.ticker)
        TickerManager.db_ticker_model.create_weekly_candles(self.ticker)

        logger.info(f"Ticker \'{self.ticker}\' weekly candles recreated.")

    @RunTime('TickerManager.generate_features')
    def generate_features(self, trend_status_ema_weight=0.75, consolidation_tolerance=0.001):
        """
        Generate features from daily and weekly candlesticks and save in database.

        Only works for ordinary tickers.
        Delete all previous features of given ticker.

        Args
        ----------
        trend_status_ema_weight : float, default 0.95
            Low-pass filter coefficient of exponential moving average derivative.
        consolidation_tolerance : float, default 0.05
            Tolerance bounds of Consolidation status. It is apllied after the calculation
            of the Up-Down Trend Coefficient [-1,1].
        """
        try:
            if self._ordinary_ticker == True:

                TickerManager.db_ticker_model.delete_features(self.ticker, interval='1d')
                self._generate_features(interval='1d', trend_status_ema_weight=trend_status_ema_weight,
                    consolidation_tolerance=consolidation_tolerance)

                TickerManager.db_ticker_model.delete_features(self.ticker, interval='1wk')
                self._generate_features(interval='1wk', trend_status_ema_weight=trend_status_ema_weight,
                    consolidation_tolerance=consolidation_tolerance)
        except Exception as error:
            logger.exception(f"Error generating features, error: {error}")
            sys.exit(c.UPDATING_DB_ERR)

    def _generate_features(self, interval = '1d', trend_status_ema_weight=0.9,
        consolidation_tolerance=0.05, min_peak_distance=17, lpf_alpha=0.98):
        """
        Generate features from given interval and save in database.

        Args
        ----------
        interval : str, default '1d'
            Selected interval: '1d', '1wk'.
        trend_status_ema_weight : float, default 0.95
            Weight coefficient of EMA_72 in trend identification.
        consolidation_tolerance : float, default 0.05
            Tolerance bounds of Consolidation status. It is apllied after the calculation
            of the Up-Down Trend Coefficient [-1,1].
        min_peak_distance : int, default 17
            Minimum distance between peaks.
        lpf_alpha : float, default 0.8
            Low-pass filter coefficient of derivatives.
        """
        if interval not in ['1d', '1wk']:
            logger.error(f"_Error argument \'interval\'=\'"
                f"{interval}\' must be \'1d\' or \'1wk\'.")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        if trend_status_ema_weight < 0 or trend_status_ema_weight > 1:
            logger.error(f"_Error argument \'trend_status_ema_weight\'=\'"
                f"{trend_status_ema_weight}\' must be in interval [0,1].")
            sys.exit(c.INVALID_ARGUMENT_ERR)

        analysis_status = {'UPTREND': 1, 'DOWNTREND': -1, 'CONSOLIDATION': 0}

        candles_df = TickerManager.db_ticker_model.get_candlesticks(self.ticker,
            None, self.end_date, interval=interval)

        max_peaks_index = find_peaks(candles_df['max_price'], distance=min_peak_distance)[0].tolist()
        min_peaks_index = find_peaks(1.0/candles_df['min_price'], distance=min_peak_distance)[0].tolist()

        # Max an min peaks must be altenate each other.
        # So delete duplicate sequences of max or min...
        for i in range(1, len(max_peaks_index)):
            delete_candidates = [j for j in min_peaks_index if j >= max_peaks_index[i-1] and j < max_peaks_index[i]]
            if len(delete_candidates) > 1:
                delete_candidates_values = [candles_df.iloc[i, candles_df.columns.get_loc('min_price')] for i in delete_candidates]
                delete_candidates.remove(delete_candidates[delete_candidates_values.index(min(delete_candidates_values))])
                min_peaks_index = [i for i in min_peaks_index if i not in delete_candidates]

        for i in range(1, len(min_peaks_index)):
            delete_candidates = [j for j in max_peaks_index if j >= min_peaks_index[i-1] and j < min_peaks_index[i]]
            if len(delete_candidates) > 1:
                delete_candidates_values = [candles_df.iloc[i, candles_df.columns.get_loc('max_price')] for i in delete_candidates]
                delete_candidates.remove(delete_candidates[delete_candidates_values.index(max(delete_candidates_values))])
                max_peaks_index = [i for i in max_peaks_index if i not in delete_candidates]

        peaks = max_peaks_index + min_peaks_index
        peaks.sort()

        # Remove monotonic peak sequences
        delete_candidates = []
        for i in range(len(peaks)):
            if i >= 2:
                current_value = 0
                if peaks[i] in max_peaks_index:
                    current_value = candles_df.iloc[peaks[i], candles_df.columns.get_loc('max_price')]
                else:
                    current_value = candles_df.iloc[peaks[i], candles_df.columns.get_loc('min_price')]

                ultimate_value = 0
                if peaks[i-1] in max_peaks_index:
                    ultimate_value = candles_df.iloc[peaks[i-1], candles_df.columns.get_loc('max_price')]
                else:
                    ultimate_value = candles_df.iloc[peaks[i-1], candles_df.columns.get_loc('min_price')]

                penultimate_value = 0
                if peaks[i-2] in max_peaks_index:
                    penultimate_value = candles_df.iloc[peaks[i-2], candles_df.columns.get_loc('max_price')]
                else:
                    penultimate_value = candles_df.iloc[peaks[i-2], candles_df.columns.get_loc('min_price')]

                if ((current_value > ultimate_value and ultimate_value > penultimate_value) or
                    (current_value < ultimate_value and ultimate_value < penultimate_value)):

                    if peaks[i-1] in max_peaks_index:
                        max_peaks_index.remove(peaks[i-1])
                    else:
                        min_peaks_index.remove(peaks[i-1])


        # Exponential Moving Average
        ema_17 = candles_df.iloc[:,candles_df.columns.get_loc('close_price')].ewm(span=17, adjust=False).mean()
        ema_72 = candles_df.iloc[:,candles_df.columns.get_loc('close_price')].ewm(span=72, adjust=False).mean()


        # Up-Down-Trend Status

        # Calculate ema_72 derivative
        ema_72_derivative_raw = [0]
        ema_72_derivative_raw.extend([a - b for a, b in zip(ema_72[1:], ema_72[:-1])])

        # Soft low-pass-filter after derivative
        # ema_72_derivative = [ema_72_derivative_raw[0]]
        # last_value = ema_72_derivative_raw[0]
        # for i in range(1, len(ema_72_derivative_raw)):
        #     value = lpf_alpha * ema_72_derivative_raw[i] + (1-lpf_alpha) * last_value
        #     ema_72_derivative.append(value)
        #     last_value = value
        ema_72_derivative = ema_72_derivative_raw

        # Calculate ema_17 derivative
        ema_17_derivative_raw = [0]
        ema_17_derivative_raw.extend([a - b for a, b in zip(ema_17[1:], ema_17[:-1])])

        # Soft low-pass-filter after derivative
        # ema_17_derivative = [ema_17_derivative_raw[0]]
        # last_value = ema_17_derivative_raw[0]
        # for i in range(1, len(ema_17_derivative_raw)):
        #     value = lpf_alpha * ema_17_derivative_raw[i] + (1-lpf_alpha) * last_value
        #     ema_17_derivative.append(value)
        #     last_value = value
        ema_17_derivative = ema_17_derivative_raw

        udt_coef = [trend_status_ema_weight * ema_72_dot + (1 - trend_status_ema_weight) * ema_17_dot for ema_17_dot, ema_72_dot in zip(ema_17_derivative, ema_72_derivative)]

        # Equation: alpha * m_72_dot + (1-alpha) * m_17_dot = 0
        # If > tolerance: UP_TREND
        # If < -tolerance: DOWN_TREND
        # Else: CONSOLIDATION
        udt_status = [analysis_status['UPTREND'] if coef > consolidation_tolerance else analysis_status['DOWNTREND'] if coef < -consolidation_tolerance else analysis_status['CONSOLIDATION'] for coef in udt_coef]


        peaks = [ ['max_price', index, candles_df.iloc[index, candles_df.columns.get_loc('max_price')]] for index in max_peaks_index ] + \
            [ ['min_price', index, candles_df.iloc[index, candles_df.columns.get_loc('min_price')]] for index in min_peaks_index ]
        peaks.sort(key=lambda x: x[1])

        candles_df['peak'] = [1 if index in max_peaks_index else -1 if index in min_peaks_index else 0 for index in range(candles_df.index.size)]
        candles_df['ema_17'] = ema_17
        candles_df['ema_72'] = ema_72
        candles_df['up_down_trend_status'] = udt_status

        TickerManager.db_ticker_model.upsert_features(candles_df, interval=interval)
