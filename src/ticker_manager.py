from pathlib import Path
import pandas as pd
import numpy as np
from datetime import timedelta
import logging
from logging.handlers import RotatingFileHandler
import sys
import yfinance as yf

import constants as c
from utils import PC, has_workdays_in_between, RunTime, Trend, compare_peaks
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
    min_risk : float
        Minimum risk per operation.
    min_risk : float
        Maximum risk per operation.

    total_tickers : int
        Total TickerManager's instanced.
    ticker_number : int
        Number of this instance.

    Methods
    ----------
    bool : update()
        Update ticker data.
    bool : generate_features(trend_status_ema_weight=0.95, consolidation_tolerance=0.05,
        lpf_alpha=0.8)
        Generate features from daily and weekly candlesticks and save in database.
    """
    db_ticker_model = DBTickerModel()
    total_tickers = 0

    def __init__(self, ticker, start_date, end_date, ordinary_ticker=True, holidays=None):
        self._ticker = ticker.upper()
        self._start_date = start_date
        self._end_date = end_date
        self._ordinary_ticker = ordinary_ticker
        self._min_risk = 0.01
        self._max_risk = 1.00

        TickerManager.total_tickers += 1
        self.ticker_number = TickerManager.total_tickers

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

    @property
    def min_risk(self):
        """float : Minimum risk per operation."""
        return self._min_risk

    @min_risk.setter
    def min_risk(self, min_risk):
        self._min_risk = min_risk

    @property
    def max_risk(self):
        """float : Maximum risk per operation."""
        return self._max_risk

    @max_risk.setter
    def max_risk(self, max_risk):
        self._max_risk = max_risk

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
            if update_happened is True and self._ordinary_ticker is True:
                self._update_weekly_candles()
        except Exception as error:
            logger.exception(f"Error updating daily candles, error:\n{error}")
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
                self.holidays, consider_oldest_date=True) is False
                and has_workdays_in_between(end_date_in_db, self.end_date,
                self.holidays, consider_recent_date=True) is False):
                logger.info(f"Ticker \'{self.ticker}\' already updated.")
                return False

            # Database lacks most recent data
            if has_workdays_in_between(end_date_in_db, self.end_date, self.holidays,
                consider_recent_date=True) is True:
                update_flag = self._update_candles_splits_and_dividends(end_date_in_db+timedelta(days=1),
                    self.end_date, oldest_date_in_db=start_date_in_db,
                    last_update_in_db=last_update_in_db)
                if update_flag is True:
                    update_happened = True

            # Database lacks oldest data
            if has_workdays_in_between(self.start_date, start_date_in_db,
                self.holidays, consider_oldest_date=True) is True:
                update_flag = self._update_candles_splits_and_dividends(self.start_date,
                    start_date_in_db-timedelta(days=1))
                if update_flag is True:
                    update_happened = True
        else:
            update_flag = self._update_candles_splits_and_dividends(self.start_date, self.end_date)
            if update_flag is True:
                update_happened = True

        if update_happened is True:
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
        if self._ordinary_ticker is True:
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
        if able_to_normalize is True and normalization_factor != 1.0:
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

        if self._ordinary_ticker is True:
            ticker += '.SA'

        try:
            msft = yf.Ticker(ticker)
            hist = msft.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=(end_date+timedelta(days=1)).strftime('%Y-%m-%d'),
                prepost=True, back_adjust=True, rounding=True)
        except Exception as error:
            logger.error('Error getting yfinance data, error:\n{}'.format(error))
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

        candles_df.drop(candles_df.loc[(candles_df['Open'] < 0.0) | \
            (candles_df['High'] < 0.0) | (candles_df['Low'] < 0.0) | \
            (candles_df['Close'] < 0.0)].index, inplace=True)

    def _update_weekly_candles(self):
        """
        Update weekly candlestick data by deleting and recalculating.

        Weekly data is created from daily data to ensure consistency.
        """
        TickerManager.db_ticker_model.delete_weekly_candles(self.ticker)
        TickerManager.db_ticker_model.create_weekly_candles(self.ticker)

        logger.info(f"Ticker \'{self.ticker}\' weekly candles recreated.")

    @RunTime('TickerManager.generate_features')
    def generate_features(self):
        """
        Generate features from daily and weekly candlesticks and save in database.

        Only works for ordinary tickers.
        Delete all previous features of given ticker.

        Returns
        ----------
        bool
            True if features were generate, False if not.
        """
        try:
            logger.info(f"Generating features for ticker \'{self.ticker}\'.")

            if self._ordinary_ticker is True:

                TickerManager.db_ticker_model.delete_features(self.ticker, interval='1d')
                TickerManager.db_ticker_model.delete_features(self.ticker, interval='1wk')

                # Prevent algorithm inertia in first values
                days_before_initial_date = 180

                # Day interval
                candles_df = TickerManager.db_ticker_model.get_candlesticks(
                    self.ticker, self.start_date, self.end_date,
                    days_before_initial_date, interval='1d')

                trends, target_prices, stop_losses, peaks = \
                    self.find_target_buy_price_and_trend(candles_df,
                    time_column_name='day', close_column_name='close_price',
                    max_colum_name='max_price', min_column_name='min_price')

                ema_17 = pd.Series([0])
                ema_17 = pd.concat([ema_17, candles_df['close_price'].ewm(span=17,
                    adjust=False).mean()], ignore_index=True)
                ema_17.drop(ema_17.index[-1], inplace=True)

                ema_72 = pd.Series([0])
                ema_72 = pd.concat([ema_72, candles_df['close_price'].ewm(span=72,
                    adjust=False).mean()], ignore_index=True)
                ema_72.drop(ema_72.index[-1], inplace=True)

                if trends is not None and \
                    target_prices is not None and stop_losses is not None and \
                    peaks is not None:
                    features_df = pd.DataFrame({'ticker': self._ticker,
                        'day': candles_df['day'], 'target_buy_price': target_prices,
                        'stop_loss': stop_losses, 'ema_17': ema_17, 'ema_72': ema_72,
                        'up_down_trend_status': trends, 'peak': peaks})
                    TickerManager.db_ticker_model.upsert_features(features_df,
                        interval='1d')
                else:
                    return False

                # Week interval
                candles_df = TickerManager.db_ticker_model.get_candlesticks(
                    self.ticker, self.start_date, self.end_date,
                    days_before_initial_date, interval='1wk')

                ema_17 = pd.Series([0])
                ema_17 = pd.concat([ema_17, candles_df['close_price'].ewm(span=17,
                    adjust=False).mean()], ignore_index=True)
                ema_17.drop(ema_17.index[-1], inplace=True)

                ema_72 = pd.Series([0])
                ema_72 = pd.concat([ema_72, candles_df['close_price'].ewm(span=72,
                    adjust=False).mean()], ignore_index=True)
                ema_72.drop(ema_72.index[-1], inplace=True)

                peaks = [0] * len(candles_df)
                peaks_raw = TickerManager.find_candles_peaks(candles_df['max_price'].to_list(),
                    candles_df['min_price'].to_list())
                if peaks_raw is not None:
                    for peak in peaks_raw:
                        peaks[peak['index']] = peak['magnitude']

                if candles_df.empty is False and peaks_raw is not None:
                    features_df = pd.DataFrame({'ticker': self._ticker,
                        'week': candles_df['week'], 'ema_17': ema_17,
                        'ema_72': ema_72, 'peak': peaks})
                    TickerManager.db_ticker_model.upsert_features(features_df,
                        interval='1wk')
                else:
                    return False
        except Exception as error:
            logger.exception(f"Error generating features, error:\n{error}")
            sys.exit(c.UPDATING_DB_ERR)
        return True

    def find_target_buy_price_and_trend(self, prices_df, close_column_name='Close',
        time_column_name = 'day', max_colum_name='High', min_column_name='Low',
        window_size=17, peaks_tolerance=0.01, outlier_tolerance=0.10):

        minimum_data_points = 2 * window_size

        if len(prices_df) < minimum_data_points:
            logger.info(f"Can not generate features for ticker \'{self.ticker}\'. "
                f"Less than {minimum_data_points} candles found.")
            return None, None, None, None

        df_length = len(prices_df)
        out_trends = [0] * df_length
        out_target_prices = [0] * df_length
        out_stop_losses = [0] * df_length
        out_peaks = [0] * df_length
        undefined_value = 0
        update_step = 0.10
        last_update_percent = update_step
        print(f"\nTicker : \'{self.ticker}\' ({self.ticker_number}/{TickerManager.total_tickers})")

        # For each day
        for index, row in prices_df.iterrows():

            completion_percentage = (index+1)/df_length
            if completion_percentage + 1e-5 >= last_update_percent:
                print(f"{last_update_percent * 100:.0f}%.")
                last_update_percent += update_step

            trend = Trend.CONSOLIDATION.value
            target_price = undefined_value
            stop_loss = undefined_value

            # if prices_df.loc[prices_df.index[index]][time_column_name] == pd.Timestamp('2017-07-13T00'):
            #     print()

            if index >= minimum_data_points:

                max_prices = prices_df.loc[prices_df.index <= index, [max_colum_name]] \
                    .squeeze().to_list()
                min_prices = prices_df.loc[prices_df.index <= index, [min_column_name]] \
                    .squeeze().to_list()

                peaks = TickerManager.find_candles_peaks(max_prices, min_prices,
                    window_size=window_size)

                if peaks is not None and len(peaks) > 3:
                    if peaks[-4]['type'] == 'max':
                        max_peak_1 = peaks[-4]['magnitude']
                        min_peak_1 = peaks[-3]['magnitude']
                        max_peak_2 = peaks[-2]['magnitude']
                        min_peak_2 = peaks[-1]['magnitude']
                    else:
                        min_peak_1 = peaks[-4]['magnitude']
                        max_peak_1 = peaks[-3]['magnitude']
                        min_peak_2 = peaks[-2]['magnitude']
                        max_peak_2 = peaks[-1]['magnitude']

                    price = row[close_column_name]

                    max_from_max_peaks = max(max_peak_1, max_peak_2)
                    min_from_max_peaks = min(max_peak_1, max_peak_2)
                    max_from_min_peaks = max(min_peak_1, min_peak_2)
                    min_from_min_peaks = min(min_peak_1, min_peak_2)

                    if (max_from_max_peaks <= max_from_min_peaks):
                        logger.error(f"Found min peak greater than or equal to max peak. "
                            f"(Ticker: \'{self.ticker}\', {time_column_name}: "
                            f"\'{pd.to_datetime(row[time_column_name], format='%d/%m/%Y')}\')")
                        sys.exit(c.INVALID_PEAK_ERR)
                    elif (min_from_max_peaks <= min_from_min_peaks):
                        logger.error(f"Found max peak less than or equal to min peak. "
                            f"(Ticker: \'{self.ticker}\', {time_column_name}: "
                            f"\'{pd.to_datetime(row[time_column_name], format='%d/%m/%Y')}\')")
                        sys.exit(c.INVALID_PEAK_ERR)

                    # target_price = round(max_from_max_peaks, 2)

                    # max_peaks_cmp = compare_peaks(max_peak_1, max_peak_2, tolerance=0.01)
                    # min_peaks_cmp = compare_peaks(min_peak_1, min_peak_2, tolerance=0.01)

                    # # if max_peaks_cmp < 0 and min_peaks_cmp < 0 \
                    # #     or (max_peaks_cmp == 0 and min_peaks_cmp < 0) \
                    # #     or (max_peaks_cmp < 0 and min_peaks_cmp == 0):
                    # if (max_peaks_cmp == PC.FIRST_IS_LESSER and min_peaks_cmp == PC.FIRST_IS_LESSER) \
                    #     or (max_peaks_cmp == PC.BOTH_ARE_CLOSE and min_peaks_cmp == PC.FIRST_IS_LESSER) \
                    #     or (max_peaks_cmp == PC.FIRST_IS_LESSER and min_peaks_cmp == PC.BOTH_ARE_CLOSE):

                    #     if price >= max_from_max_peaks*(1+peaks_tolerance):
                    #         target_price = round(max_from_max_peaks, 2)
                    #     elif price >= max(min_from_max_peaks, max_from_min_peaks):
                    #         target_price = round(max(min_from_max_peaks, max_from_min_peaks), 2)
                    #     else:
                    #         target_price = round(price)

                    #     trend = Trend.UPTREND.value
                    # else:
                    #      if price >= max_from_max_peaks*(1+outlier_tolerance):
                    #          trend = Trend.UPTREND.value

                    target_price = TickerManager.get_target_price(price, max_peak_1,
                        max_peak_2, min_peak_1, min_peak_2, peaks_tolerance=0.005)

                    trend = TickerManager.get_trend(price, max_peak_1, max_peak_2,
                        min_peak_1, min_peak_2, peaks_tolerance=0.005)

                    stop_loss = TickerManager.get_stop_loss(max_from_max_peaks,
                        min_from_max_peaks, max_from_min_peaks, min_from_min_peaks,
                        target_price, min_risk=self.min_risk, max_risk=self.max_risk)

            if index != df_length - 1:
                out_trends[index+1] = trend
                out_target_prices[index+1] = target_price
                out_stop_losses[index+1] = stop_loss

        if peaks is not None:
            for peak in peaks:
                out_peaks[peak['index']] = peak['magnitude']

        return out_trends, out_target_prices, out_stop_losses, out_peaks

    @staticmethod
    def find_candles_peaks(max_prices, min_prices, window_size=17):

        if len(max_prices) != len(min_prices):
            return None

        votes = np.zeros_like(max_prices)
        last_windows_index = window_size - 1

        # Create moving windows and detect local minima and local maxima
        if len(max_prices) > window_size:
            for i in range(len(max_prices) - window_size):
                max_subsequence = max_prices[i:i + window_size]
                min_subsequence = min_prices[i:i + window_size]

                # Vote only if value is not in the window border
                argmax = np.argmax(max_subsequence)
                if argmax not in [0, last_windows_index]:
                    votes[i + argmax] += 1

                argmin = np.argmin(min_subsequence)
                if argmin not in [0, last_windows_index]:
                    votes[i + argmin] -= 1

            votes = [vote if abs(vote) >= window_size // 2 else 0 for vote in votes]

            max_peaks_index = [index for index, vote in enumerate(votes) if vote > 0]
            min_peaks_index = [index for index, vote in enumerate(votes) if vote < 0]

            max_peaks_values = [max_prices[max_peak] for max_peak in max_peaks_index]
            min_peaks_values = [min_prices[min_peak] for min_peak in min_peaks_index]

        else:
            return None

        return TickerManager.analyze_peaks(max_peaks_index, min_peaks_index,
            max_peaks_values, min_peaks_values)

    @staticmethod
    def analyze_peaks(max_peaks_index, min_peaks_index, max_peaks_values, min_peaks_values):

        if (max_peaks_index is None or len(max_peaks_index) < 2) or \
            (min_peaks_index is None or len(min_peaks_index) < 2):
            return None

        # ordered_peaks = sorted(max_peaks_index + min_peaks_index)
        ordered_peaks = [{'type': 'max', 'index': p_index, 'value':
            max_peaks_values[max_peaks_index.index(p_index)]}
            if p_index in max_peaks_index
            else {'type': 'min', 'index': p_index, 'value':
            min_peaks_values[min_peaks_index.index(p_index)]}
            for p_index in sorted(max_peaks_index + min_peaks_index)]
        peaks_number = len(ordered_peaks)

        # print('Ordered Peaks:')
        # for peak in ordered_peaks:
        #     print(peak)
        # print()

        # Create sequences of max and min peaks
        first_peak = 'max' if max_peaks_index[0] < min_peaks_index[0] else 'min'
        sequences = []
        current_sequence = {'type': first_peak, 'index': [], 'magnitude': ordered_peaks[0]['value']}
        current_type = first_peak

        for i, peak in enumerate(ordered_peaks):
            if peak['type'] == current_type:
                current_sequence['index'].append(peak['index'])
                current_sequence['magnitude'] = max(current_sequence['magnitude'], peak['value']) \
                    if current_type == 'max' else min(current_sequence['magnitude'], peak['value'])
            else:
                sequences.append(current_sequence.copy())
                current_type = peak['type']
                current_sequence['type'] = peak['type']
                current_sequence['index'] = [peak['index']]
                current_sequence['magnitude'] = peak['value']

            if i == peaks_number - 1:
                sequences.append(current_sequence.copy())

        # print('Ordered Sequences:')
        # for sequence in sequences:
        #     print(sequence)
        # print()

        # Remove invalid peak sequences
        ok = False
        while not ok:
            last_sequence = {}
            for i, sequence in enumerate(sequences):
                if i != 0:
                    if sequence['type'] == 'min' and last_sequence['type'] == 'max':
                        if sequence['magnitude'] >= last_sequence['magnitude']:
                            del sequences[i]
                            break
                    elif sequence['type'] == 'max' and last_sequence['type'] == 'min':
                        if sequence['magnitude'] <= last_sequence['magnitude']:
                            del sequences[i]
                            break
                    # Last and current of same type
                    else:
                        sequences[i]['magnitude'] = max(sequence['magnitude'], last_sequence['magnitude']) \
                            if sequence['type'] == 'max' else \
                            min(sequence['magnitude'], last_sequence['magnitude'])
                        sequences[i]['index'].extend(last_sequence['index'])
                        del sequences[i-1]
                        break
                last_sequence = sequence
                if i == len(sequences) - 1:
                    ok = True

        # print('Filtered and Ordered Sequences:')
        # for sequence in sequences:
        #     print(sequence)
        # print()

        # Choose peak representative of each sequence
        for i in range(len(sequences)):
            if len(sequences[i]['index']) > 1:
                if sequences[i]['type'] == 'max':
                    selected_index = max_peaks_index[max_peaks_values.index(sequences[i]['magnitude'])]
                else:
                    selected_index = min_peaks_index[min_peaks_values.index(sequences[i]['magnitude'])]
                sequences[i]['index'] = selected_index
            else:
                sequences[i]['index'] = sequences[i]['index'][0]

        # print('Final Sequence:')
        # for sequence in sequences:
        #     print(sequence)
        # print()

        return sequences

    # TODO : Implement 'margin_from_peak'
    @staticmethod
    def get_stop_loss(max_from_max_peaks, min_from_max_peaks, max_from_min_peaks,
        min_from_min_peaks, target_buy_price, min_risk=0.01, max_risk=1.0):

        threshold_1 = max_from_max_peaks
        threshold_2 = max(min_from_max_peaks, max_from_min_peaks)
        threshold_3 = min(min_from_max_peaks, max_from_min_peaks)
        threshold_4 = min_from_min_peaks

        stop_loss = 0.0

        min_stop_loss = target_buy_price * (1 - min_risk)
        max_stop_loss = target_buy_price * (1 - max_risk)

        if max_stop_loss > threshold_1:
            stop_loss = max_stop_loss

        if min_stop_loss >= threshold_1 and threshold_1 >= max_stop_loss:
            stop_loss = threshold_1
        else:
            if max_stop_loss > threshold_2:
                stop_loss = max_stop_loss
            elif min_stop_loss >= threshold_2 and threshold_2 >= max_stop_loss:
                stop_loss = threshold_2
            else:
                if max_stop_loss > threshold_3:
                    stop_loss = max_stop_loss
                elif min_stop_loss >= threshold_3 and threshold_3 >= max_stop_loss:
                    stop_loss = threshold_3
                else:
                    if max_stop_loss > threshold_4:
                        stop_loss = max_stop_loss
                    elif min_stop_loss >= threshold_4 and threshold_4 >= max_stop_loss:
                        stop_loss = threshold_4
                    else:
                        stop_loss = min_stop_loss

        return round(stop_loss, 2)

    @staticmethod
    def get_target_price(price, max_peak_1, max_peak_2, min_peak_1, min_peak_2,
        peaks_tolerance=0.01):

        max_from_max_peaks = max(max_peak_1, max_peak_2)
        min_from_max_peaks = min(max_peak_1, max_peak_2)
        max_from_min_peaks = max(min_peak_1, min_peak_2)

        target_price = round(max_from_max_peaks * (1+peaks_tolerance), 2)

        max_peaks_cmp = compare_peaks(max_peak_1, max_peak_2, tolerance=peaks_tolerance)
        min_peaks_cmp = compare_peaks(min_peak_1, min_peak_2, tolerance=peaks_tolerance)

        if (max_peaks_cmp == PC.FIRST_IS_LESSER and min_peaks_cmp == PC.FIRST_IS_LESSER) \
            or (max_peaks_cmp == PC.BOTH_ARE_CLOSE and min_peaks_cmp == PC.FIRST_IS_LESSER) \
            or (max_peaks_cmp == PC.FIRST_IS_LESSER and min_peaks_cmp == PC.BOTH_ARE_CLOSE):

            if price >= max_from_max_peaks*(1+peaks_tolerance):
                target_price = round(
                    max_from_max_peaks * (1+peaks_tolerance), 2)
            else:
                target_price = round(
                    max(min_from_max_peaks, max_from_min_peaks)*(1+peaks_tolerance), 2)

        return target_price

    @staticmethod
    def get_trend(price, max_peak_1, max_peak_2, min_peak_1, min_peak_2,
        peaks_tolerance=0.01):

        trend = Trend.CONSOLIDATION.value

        max_peaks_cmp = compare_peaks(max_peak_1, max_peak_2, tolerance=peaks_tolerance)
        min_peaks_cmp = compare_peaks(min_peak_1, min_peak_2, tolerance=peaks_tolerance)

        if (max_peaks_cmp == PC.FIRST_IS_LESSER and min_peaks_cmp == PC.FIRST_IS_LESSER) \
            or (max_peaks_cmp == PC.BOTH_ARE_CLOSE and min_peaks_cmp == PC.FIRST_IS_LESSER) \
            or (max_peaks_cmp == PC.FIRST_IS_LESSER and min_peaks_cmp == PC.BOTH_ARE_CLOSE):

            trend = Trend.UPTREND.value

        elif (max_peaks_cmp == PC.FIRST_IS_GREATER and min_peaks_cmp == PC.FIRST_IS_GREATER) \
            or (max_peaks_cmp == PC.BOTH_ARE_CLOSE and min_peaks_cmp == PC.FIRST_IS_GREATER) \
            or (max_peaks_cmp == PC.FIRST_IS_GREATER and min_peaks_cmp == PC.BOTH_ARE_CLOSE):

            trend = Trend.DOWNTREND.value

        return trend