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
from utils import has_workdays_in_between, RunTime, Trend
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

        Args
        ----------

        """
        try:
            if self._ordinary_ticker == True:

                TickerManager.db_ticker_model.delete_features(self.ticker, interval='1d')
                TickerManager.db_ticker_model.delete_features(self.ticker, interval='1wk')

                # Prevent algorithm inertia in first values
                days_before_initial_date = 180

                for interval in ['1d', '1wk']:
                    time_column = 'week' if interval == '1wk' else 'day'

                    candles_df = TickerManager.db_ticker_model.get_candlesticks(
                        self.ticker, self.start_date, self.end_date, days_before_initial_date,
                        interval=interval)

                    trends, target_prices, stop_losses = \
                        TickerManager.find_target_buy_price_and_trend(candles_df,
                        close_column_name='close_price', max_colum_name='max_price',
                        min_column_name='min_price', target_price_margin=0, stop_loss_margin=0)

                    ema_17, ema_72 = TickerManager.find_emas(candles_df['close_price'])

                    features_df = pd.DataFrame({'ticker': self._ticker, time_column: candles_df[time_column],
                        'target_buy_price': target_prices, 'stop_loss': stop_losses,
                        'ema_17': ema_17, 'ema_72': ema_72, 'up_down_trend_status': trends})

                    TickerManager.db_ticker_model.upsert_features(features_df, interval=interval)
        except Exception as error:
            logger.exception(f"Error generating features, error:\n{error}")
            sys.exit(c.UPDATING_DB_ERR)

    @staticmethod
    def find_emas(prices_df):
        # Exponential Moving Average
        ema_17 = prices_df.ewm(span=17, adjust=False).mean()
        ema_72 = prices_df.ewm(span=72, adjust=False).mean()

        return ema_17, ema_72

    @staticmethod
    def find_target_buy_price_and_trend(prices_df, close_column_name='Close',
        max_colum_name='High', min_column_name='Low', target_price_margin=0,
        stop_loss_margin=0, window_size=17):

        minimum_data_points = 2 * window_size

        if len(prices_df) < minimum_data_points:
            return None

        df_length = len(prices_df)

        trends = [0] * df_length
        target_prices = [0] * df_length
        stop_losses = [0] * df_length

        undefined_value = 0

        update_step = 0.05
        last_update_percent = update_step

        for index, (date, row) in enumerate(prices_df.iterrows()):

            completion_percentage = (index+1)/df_length
            if completion_percentage >= last_update_percent:
                print(f"{last_update_percent * 100:.0f}%.")
                last_update_percent += update_step

            trend = Trend.UNDEFINED.value
            target_price = undefined_value
            stop_loss = undefined_value

            # if date == datetime.strptime("04/04/2019", '%d/%m/%Y'):
            #     print()

            if index >= minimum_data_points:

                max_prices = prices_df.loc[prices_df.index <= date, [max_colum_name]].squeeze().to_list()
                min_prices = prices_df.loc[prices_df.index <= date, [min_column_name]].squeeze().to_list()

                _, max_peaks, min_peaks, _, _ = \
                    TickerManager.find_candles_peaks(max_prices, min_prices, window_size=window_size)

                if len(max_peaks) > 1 and len(min_peaks) > 1:
                    max_peak_1 = max_peaks[-2]
                    max_peak_2 = max_peaks[-1]
                    min_peak_1 = min_peaks[-2]
                    min_peak_2 = min_peaks[-1]

                    price = row[close_column_name]

                    max_from_max_peaks = max(max_peak_1, max_peak_2)
                    min_from_max_peaks = min(max_peak_1, max_peak_2)
                    max_from_min_peaks = max(min_peak_1, min_peak_2)
                    min_from_min_peaks = min(min_peak_1, min_peak_2)

                    target_price = (1 - target_price_margin) * max_from_max_peaks
                    stop_loss = (1 - stop_loss_margin) * max_from_min_peaks

                    # Likely uptrend
                    if (max_peak_1 < max_peak_2 and min_peak_1 < min_peak_2) \
                        or (max_peak_1 == max_peak_2 and min_peak_1 < min_peak_2) \
                        or (max_peak_1 < max_peak_2 and min_peak_1 == min_peak_2):
                        if price > max_from_max_peaks:
                            trend = Trend.UPTREND.value
                            last_greater_max = [max_peak for max_peak in sorted(max_peaks)
                                if max_peak > max_from_max_peaks]
                            if len(last_greater_max) != 0:
                                target_price = last_greater_max[0]
                            else:
                                target_price = price
                        elif price >= max_from_min_peaks:
                            trend = Trend.UPTREND.value
                        elif price >= min_from_min_peaks:
                            trend = Trend.ALMOST_UPTREND.value
                        else:
                            trend = Trend.ALMOST_DOWNTREND.value
                    # Likely downtrend
                    elif (max_peak_1 > max_peak_2 and min_peak_1 > min_peak_2) \
                        or (max_peak_1 == max_peak_2 and min_peak_1 > min_peak_2) \
                        or (max_peak_1 > max_peak_2 and min_peak_1 == min_peak_2):
                        if price < min_from_max_peaks:
                            trend = Trend.DOWNTREND.value
                        elif price <= max_from_max_peaks:
                            trend = Trend.ALMOST_DOWNTREND.value
                        else:
                            trend = Trend.ALMOST_UPTREND.value
                    # Likely consolidation
                    else:
                        if price > max_from_max_peaks:
                            trend = Trend.ALMOST_UPTREND.value
                            last_greater_max = [max_peak for max_peak in sorted(max_peaks) if max_peak > max_from_max_peaks]
                            if len(last_greater_max) != 0:
                                target_price = last_greater_max[0]
                            else:
                                target_price = price
                        elif price < min_from_min_peaks:
                            trend = Trend.ALMOST_DOWNTREND.value
                        else:
                            trend = Trend.CONSOLIDATION.value

            trends[index] = trend
            target_prices[index] = target_price
            stop_losses[index] = stop_loss

            # Breakpoint
            # if date == datetime.strptime("18/04/2022", '%d/%m/%Y'):
            #     fig, axs = plt.subplots(3)
            #     x_data = hist.index[0:index+1]

            #     axs[0].plot(x_data, trends[0:index+1])
            #     axs[1].plot(x_data, target_prices[0:index+1], 'blue')
            #     axs[1].plot(x_data, stop_losses[0:index+1], 'red')
            #     axs[2].plot(x_data, max_prices[0:index+1], 'lightblue')
            #     axs[2].plot(x_data, min_prices[0:index+1], 'orange')
            #     axs[2].plot(x_data, hist.loc[hist.index[0:index+1], [close_column_name]], 'lightgreen')
            #     axs[2].plot(hist.index[max_indices], max_peaks, 'bo')
            #     axs[2].plot(hist.index[min_indices], min_peaks, 'rx')

            #     axs[0].xaxis.set_major_formatter(myFmt)
            #     axs[1].xaxis.set_major_formatter(myFmt)
            #     axs[2].xaxis.set_major_formatter(myFmt)

            #     plt.show()

        return trends, target_prices, stop_losses

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

            # Remove non-alternating min peaks
            for i in range(1, len(max_peaks_index)):
                min_peaks_between = [min_index for min_index in min_peaks_index
                    if min_index > max_peaks_index[i-1] and min_index < max_peaks_index[i]]
                if len(min_peaks_between) > 1:
                    most_valuabe_peak_votes = max([abs(votes[min_index]) for min_index in min_peaks_between])
                    for min_peak_between in min_peaks_between:
                        if abs(votes[min_peak_between]) < most_valuabe_peak_votes:
                            min_peaks_index.remove(min_peak_between)
                            votes[min_peak_between] = 0

            # Remove non-alternating max peaks
            for i in range(1, len(min_peaks_index)):
                max_peaks_between = [max_index for max_index in max_peaks_index
                    if max_index > min_peaks_index[i-1] and max_index < min_peaks_index[i]]
                if len(max_peaks_between) > 1:
                    most_valuabe_peak_votes = max([abs(votes[max_index]) for max_index in max_peaks_between])
                    for max_peak_between in max_peaks_between:
                        if abs(votes[max_peak_between]) < most_valuabe_peak_votes:
                            max_peaks_index.remove(max_peak_between)
                            votes[max_peak_between] = 0

            max_peaks_values = [max_prices[max_peak] for max_peak in max_peaks_index]
            min_peaks_values = [min_prices[min_peak] for min_peak in min_peaks_index]
        else:
            return None

        return votes, max_peaks_values, min_peaks_values, max_peaks_index, min_peaks_index
