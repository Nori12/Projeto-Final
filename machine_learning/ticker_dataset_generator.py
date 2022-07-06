import sys
from pathlib import Path
import pandas as pd
import numpy as np
from scipy import stats
import math

sys.path.insert(1, str(Path(__file__).parent.parent/'src'))
import ml_constants as mlc
from db_model import DBStrategyAnalyzerModel

class TickerDatasetGenerator:

    def __init__(self, ticker, start_date, end_date, buy_type='current_day_open_price',
        gain_loss_ratio=3, peaks_pairs_number=2, risk_option='range', fixed_risk=0.03,
        start_range_risk=0.01, step_range_risk=0.002, end_range_risk=0.12,
        max_days_per_operation=mlc.MAX_DAYS_PER_OPERATION, spearman_correlations=(3, 17, 72),
        dataset_dir=None):

        if buy_type not in ('current_day_open_price', 'last_day_close_price'):
            raise Exception("'buy_type' parameter options: 'current_day_open_price', 'last_day_close_price'.")

        if risk_option not in ('fixed', 'range'):
            raise Exception("'risk_option' parameter options: 'fixed', 'range'.")

        self._ticker = ticker
        self._start_date = start_date
        self._end_date = end_date
        self._buy_type = buy_type
        self._gain_loss_ratio = gain_loss_ratio
        self._peaks_pairs_number = peaks_pairs_number   # Number of past max and min peaks pairs
        self._risk_option = risk_option
        self._fixed_risk = fixed_risk
        self._max_days_per_operation = max_days_per_operation
        self._spearman_correlations = spearman_correlations

        self._risks = []
        if self._risk_option == 'range':
            self._risks = tuple(round(i, 3) for i in tuple(np.arange(start_range_risk,
                end_range_risk+step_range_risk, step_range_risk)))

        # Peaks identification variables
        self._peak_delay_days = 9

        if dataset_dir is None:
            self._dataset_dir = Path(__file__).parent / mlc.DATASETS_FOLDER
        else:
            self._dataset_dir = dataset_dir

        if not self._dataset_dir.exists():
            self._dataset_dir.mkdir(parents=True)
        else:
            # Remove existing dataset file
            (self._dataset_dir / (ticker+mlc.DATASET_FILE_SUFFIX)).unlink(missing_ok=True)

        # Spearman correlation derivate variable
        self._spearman_corr_column_names = tuple(['spearman_corr_' + str(n) + '_day' \
            for n in spearman_correlations])

        self._last_n_close_prices = []
        self._last_prices_max_length = max(spearman_correlations)

        self._spearman_benchmark = tuple([i for i in range(self._last_prices_max_length)])


    @property
    def ticker(self):
        return self._ticker

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date

    @property
    def buy_type(self):
        return self._buy_type

    @property
    def gain_loss_ratio(self):
        return self._gain_loss_ratio

    @property
    def max_days_per_operation(self):
        return self._max_days_per_operation

    @property
    def peaks_pairs_number(self):
        return self._peaks_pairs_number

    @property
    def peak_delay_days(self):
        return self._peak_delay_days

    @property
    def dataset_dir(self):
        return self._dataset_dir

    @property
    def risk_option(self):
        return self._risk_option

    @property
    def fixed_risk(self):
        return self._fixed_risk

    @property
    def risks(self):
        return self._risks

    @property
    def spearman_correlations(self):
        return self._spearman_correlations

    @property
    def spearman_corr_column_names(self):
        return self._spearman_corr_column_names

    @property
    def last_n_close_prices(self):
        return self._last_n_close_prices

    @last_n_close_prices.setter
    def last_n_close_prices(self, last_n_close_prices):
        self._last_n_close_prices = last_n_close_prices

    @property
    def last_prices_max_length(self):
        return self._last_prices_max_length

    @property
    def spearman_benchmark(self):
        return self._spearman_benchmark


    def generate_dataset(self, add_ref_price=False):

        db_model = DBStrategyAnalyzerModel()
        first_write_on_file_flg = True

        # Get daily and weekly candles
        candles_df_day = db_model.get_ticker_prices_and_features(self.ticker,
            self.start_date, self.end_date, interval='1d')
        candles_df_wk = db_model.get_ticker_prices_and_features(self.ticker,
            self.start_date, self.end_date, interval='1wk')

        peaks_data = TickerDatasetGenerator._init_peaks_data()

        # For each day
        for idx, row in candles_df_day.iterrows():

            business_data = self._init_business_data(add_ref_price)

            if self.buy_type == "current_day_open_price":
                purchase_price = row['open_price']
                idx_delay = 0
            elif self.buy_type == "last_day_close_price":
                purchase_price = row['close_price']
                idx_delay = 1

            data_row = self._init_data_row(self.ticker, row['day'], risk=0)

            ref_price_col_name = 'open_price' \
                if self.buy_type == 'current_day_open_price' \
                else 'close_price' if self.buy_type == 'last_day_close_price' \
                else ''

            self._fill_partially_data_row(data_row, row['ema_17'], row['ema_72'],
                TickerDatasetGenerator._get_ema_72_week(candles_df_day, idx, candles_df_wk),
                row[ref_price_col_name])

            TickerDatasetGenerator._update_peaks_days(peaks_data)
            peaks_ready_flg = self._process_and_check_last_peaks(peaks_data,
                row['peak'], row['max_price'], row['min_price'])
            if peaks_ready_flg is False:
                self._update_last_n_prices(row['close_price'])
                continue

            if not self._verify_business_data_integrity( (data_row['ema_17_day'],
                data_row['ema_72_day'], data_row['ema_72_week']), peaks_data ):
                self._update_last_n_prices(row['close_price'])
                continue

            if not self._verify_last_n_prices_threshold():
                self._update_last_n_prices(row['close_price'])
                continue

            spearman_corrs = self._process_spearman_correlations()

            if self.risk_option == 'fixed':

                data_row['risk']= self.fixed_risk

                data_row['success_oper_flag'], data_row['timeout_flag'], \
                    data_row['end_of_interval_flag'] = self._process_operation_result(
                    purchase_price, self.fixed_risk, candles_df_day, idx, idx_delay)

                self._fill_business_data(business_data, data_row, peaks_data,
                    spearman_corrs, add_ref_price)

            elif self.risk_option == 'range':

                for risk in self.risks:
                    data_row['risk']= risk

                    data_row['success_oper_flag'], data_row['timeout_flag'], \
                    data_row['end_of_interval_flag'] = self._process_operation_result(
                        purchase_price, risk, candles_df_day, idx, idx_delay)

                    self._fill_business_data(business_data, data_row, peaks_data,
                        spearman_corrs, add_ref_price)

            self._update_last_n_prices(row['close_price'])

            if first_write_on_file_flg is True:
                pd.DataFrame(business_data).to_csv(
                    self.dataset_dir / (self.ticker + mlc.DATASET_FILE_SUFFIX),
                    mode='w', index=False, header=True)
                first_write_on_file_flg = False
            else:
                pd.DataFrame(business_data).to_csv(
                    self.dataset_dir / (self.ticker + mlc.DATASET_FILE_SUFFIX),
                    mode='a', index=False, header=False)

    @staticmethod
    def _init_peaks_data():

        peaks_data = {
            'current_max_delay': 0,
            'current_min_delay': 0,
            'upcoming_max_peak': 0.0,
            'upcoming_min_peak': 0.0,
            'last_max_peaks': [],
            'last_max_peaks_days': [],
            'last_min_peaks': [],
            'last_min_peaks_days': []
        }

        return peaks_data

    def _init_business_data(self, add_ref_price):

        business_data = {
            "ticker": [],
            "day": [],
            "risk": [],
            "success_oper_flag": [],
            "timeout_flag": [],
            "end_of_interval_flag": [],
        }

        if add_ref_price:
            business_data['ref_price'] = []

        for i in range(self.peaks_pairs_number):
            business_data['peak_'+str(i*2+1)] = []
            business_data['day_'+str(i*2+1)] = []
            business_data['peak_'+str(i*2+2)] = []
            business_data['day_'+str(i*2+2)] = []

        business_data['ema_17_day'] = []
        business_data['ema_72_day'] = []
        business_data['ema_72_week'] = []

        for col in (self.spearman_corr_column_names):
            business_data[col] = []

        return business_data

    def _init_data_row(self, ticker, day, risk=0.0):

        # Clearify what should be inside '_init_data_row' and '_fill_partially_data_row'
        data_row = {'ticker': ticker, 'day': day, 'risk': risk, 'ema_17_day': 0.0,
            'ema_72_day': 0.0, 'ema_72_week': 0.0}

        for col in self.spearman_corr_column_names:
            data_row[col] = 0.0

        return data_row

    def _fill_partially_data_row(self, data_row, ema_17_day, ema_72_day, ema_72_week,
        ref_price):

        data_row['ema_17_day'] = ema_17_day
        data_row['ema_72_day'] = ema_72_day
        data_row['ema_72_week'] = ema_72_week
        data_row['ref_price'] = ref_price

        return data_row

    @staticmethod
    def _init_peaks_data():

        peaks_data = {
            'current_max_delay': 0,
            'current_min_delay': 0,
            'upcoming_max_peak': 0.0,
            'upcoming_min_peak': 0.0,
            'last_max_peaks': [],
            'last_max_peaks_days': [],
            'last_min_peaks': [],
            'last_min_peaks_days': []
        }

        return peaks_data

    @staticmethod
    def _get_ema_72_week(candles_df_day, idx, candles_df_wk):

        year, week, _ = (candles_df_day.loc[candles_df_day.index[idx-1], 'day'] \
        - pd.Timedelta(days=7)).isocalendar()

        ema_72_week = candles_df_wk.loc[
            (candles_df_wk['week'].dt.isocalendar().year == year) \
            & (candles_df_wk['week'].dt.isocalendar().week == week),
            ['ema_72']].tail(1).squeeze() \
            if not candles_df_wk.empty \
            else candles_df_wk

        return ema_72_week

    def _verify_business_data_integrity(self, features, peaks_data):

        for feature in features:
            if feature is None \
                or isinstance(feature, pd.Series) \
                or isinstance(feature, pd.DataFrame) \
                or feature <= 0.01:
                return False

        if not (len(peaks_data['last_max_peaks']) == self.peaks_pairs_number \
            and len(peaks_data['last_max_peaks_days']) == self.peaks_pairs_number \
            and len(peaks_data['last_min_peaks']) == self.peaks_pairs_number \
            and len(peaks_data['last_min_peaks_days']) == self.peaks_pairs_number):
            return False

        for max_peak, min_peak in peaks_data['last_max_peaks'], peaks_data['last_min_peaks']:
            if max_peak is None or max_peak <= 0.0 \
                or min_peak is None or min_peak <= 0.0:
                return False

        # Make sure peaks are alternating
        order = None
        for idx, (max_days, min_days) in enumerate(zip(peaks_data['last_max_peaks_days'],
            peaks_data['last_min_peaks_days'])):
            if idx == 0:
                if max_days < min_days:
                    order = True
                else:
                    order = False
            else:
                if order is True and max_days > min_days \
                    or order is False and max_days < min_days:
                    # Non-alternating peaks
                    return False

        return True

    def _verify_last_n_prices_threshold(self):
        if len(self.last_n_close_prices) < self.last_prices_max_length:
            return False

        return True

    @staticmethod
    def _update_peaks_days(peaks_data):

        for idx in range(len(peaks_data['last_max_peaks_days'])):
            peaks_data['last_max_peaks_days'][idx] -= 1

        for idx in range(len(peaks_data['last_min_peaks_days'])):
            peaks_data['last_min_peaks_days'][idx] -= 1

    def _update_last_n_prices(self, price):

        self.last_n_close_prices.append(price)

        if len(self.last_n_close_prices) > self.last_prices_max_length:
            self.last_n_close_prices.pop(0)

    def _process_and_check_last_peaks(self, peaks_data, peak_day, max_price_day,
        min_price_day):

        peaks_data['current_max_delay'] += 1
        peaks_data['current_min_delay'] += 1

        # Prepare next peak candidate
        if peak_day >= 0.01:
            # Bug treatment 'if' statement
            if max_price_day == min_price_day:
                # Choose the an alternating peak type
                # Lesser means most recent added, so now is the time for the other peak type
                if peaks_data['current_max_delay'] < peaks_data['current_min_delay']:
                    peaks_data['upcoming_min_peak'] = peak_day
                    peaks_data['current_min_delay'] = 0
                else:
                    peaks_data['upcoming_max_peak'] = peak_day
                    peaks_data['current_max_delay'] = 0
            elif max_price_day != min_price_day:
                if peak_day == max_price_day:
                    peaks_data['upcoming_max_peak'] = peak_day
                    peaks_data['current_max_delay'] = 0
                else:
                    peaks_data['upcoming_min_peak'] = peak_day
                    peaks_data['current_min_delay'] = 0

        # If delay time is done, update new max peak
        if peaks_data['current_max_delay'] == self.peak_delay_days \
            and peaks_data['upcoming_max_peak'] != 0.0:

            peaks_data['last_max_peaks'].append(peaks_data['upcoming_max_peak'])
            peaks_data['last_max_peaks_days'].append(-peaks_data['current_max_delay'])

            if len(peaks_data['last_max_peaks']) > self.peaks_pairs_number:
                peaks_data['last_max_peaks'].pop(0)
                peaks_data['last_max_peaks_days'].pop(0)

            peaks_data['upcoming_max_peak'] = 0.0

        # If delay time is done, update new min peak
        if peaks_data['current_min_delay'] == self.peak_delay_days \
            and peaks_data['upcoming_min_peak'] != 0.0:

            peaks_data['last_min_peaks'].append(peaks_data['upcoming_min_peak'])
            peaks_data['last_min_peaks_days'].append(-peaks_data['current_min_delay'])

            if len(peaks_data['last_min_peaks']) > self.peaks_pairs_number:
                peaks_data['last_min_peaks'].pop(0)
                peaks_data['last_min_peaks_days'].pop(0)

            peaks_data['upcoming_min_peak'] = 0.0

        # If minimum peaks quantity is not yet registered
        if len(peaks_data['last_max_peaks']) < self.peaks_pairs_number or \
            len(peaks_data['last_min_peaks']) < self.peaks_pairs_number:
            return False

        return True

    def _process_operation_result(self, purchase_price, risk, candles_df_day,
        curr_idx, idx_delay):

        candles_len = len(candles_df_day)
        days_counter = 0
        op_result = False
        end_of_interval_flg = False
        timeout_flg = False

        target_price = round(purchase_price * (1 + self.gain_loss_ratio * risk), 2)
        stop_price = round(purchase_price * (1 - risk), 2)

        # Verify the future
        for idx_fut, row_fut in candles_df_day.loc[candles_df_day.index[
            curr_idx + idx_delay : candles_len]].iterrows():

            days_counter += 1

            if row_fut['min_price'] <= stop_price:
                break

            if row_fut['max_price'] >= target_price:
                op_result = True
                break

            if days_counter == self.max_days_per_operation:
                timeout_flg = True
                break

            if idx_fut == candles_len - 1:
                end_of_interval_flg = True

        return op_result, timeout_flg, end_of_interval_flg

    def _process_spearman_correlations(self):

        spearman_data = []

        for idx in range(len(self.spearman_corr_column_names)):

            spearman = stats.spearmanr(self.spearman_benchmark[:self.spearman_correlations[idx]],
                self.last_n_close_prices[self.last_prices_max_length - \
                self.spearman_correlations[idx]:self.last_prices_max_length]).correlation

            if math.isnan(spearman):
                spearman = 0.0

            spearman_data.append(spearman)

        return spearman_data

    def _fill_business_data(self, business_data, data_row, peaks_data, spearman_data, add_ref_price):

        business_data['ticker'].append(data_row['ticker'])
        business_data['day'].append(data_row['day'])
        business_data['risk'].append(data_row['risk'])

        ref_price = data_row['ref_price']

        if add_ref_price:
            business_data['ref_price'].append(ref_price)

        business_data['success_oper_flag'].append(int(data_row['success_oper_flag']))
        business_data['timeout_flag'].append(int(data_row['timeout_flag']))
        business_data['end_of_interval_flag'].append(int(data_row['end_of_interval_flag']))
        business_data['ema_17_day'].append( round(data_row['ema_17_day'] / ref_price, 4))
        business_data['ema_72_day'].append( round(data_row['ema_72_day'] / ref_price, 4))
        business_data['ema_72_week'].append( round(data_row['ema_72_week'] / ref_price, 4))

        for idx, col in enumerate(self.spearman_corr_column_names):
            business_data[col].append(round(spearman_data[idx], 4))

        # More negative day number means older
        order = 'max_first' \
            if peaks_data['last_max_peaks_days'][0] < peaks_data['last_min_peaks_days'][0] \
            else 'min_first'

        for i in range(self.peaks_pairs_number):
            if order == 'max_first':
                business_data['peak_'+str(int(i*2+1))].append( round(peaks_data['last_max_peaks'][i] / ref_price, 4) )
                business_data['day_'+str(int(i*2+1))].append(peaks_data['last_max_peaks_days'][i])
                business_data['peak_'+str(int(i*2+2))].append( round(peaks_data['last_min_peaks'][i] / ref_price, 4) )
                business_data['day_'+str(int(i*2+2))].append(peaks_data['last_min_peaks_days'][i])
            elif order == 'min_first':
                business_data['peak_'+str(int(i*2+1))].append( round(peaks_data['last_min_peaks'][i] / ref_price, 4) )
                business_data['day_'+str(int(i*2+1))].append(peaks_data['last_min_peaks_days'][i])
                business_data['peak_'+str(int(i*2+2))].append( round(peaks_data['last_max_peaks'][i] / ref_price, 4) )
                business_data['day_'+str(int(i*2+2))].append(peaks_data['last_max_peaks_days'][i])

class TickerDatasetGenerator2:

    def __init__(self, ticker, start_date, end_date, start_range_risk=0.01,
        step_range_risk=0.002, end_range_risk=0.12,
        spearman_correlations=(5, 10, 15, 20, 25, 30, 40, 50, 60),
        lpf_alpha=0.2, dataset_dir=None, max_days_per_operation=mlc.MAX_DAYS_PER_OPERATION,
        gain_loss_ratio=3):
        # Purchase on current day open price

        self._ticker = ticker
        self._start_date = start_date
        self._end_date = end_date
        self._spearman_correlations = spearman_correlations
        self._lpf_alpha = lpf_alpha
        self._max_days_per_operation = max_days_per_operation
        self._gain_loss_ratio = gain_loss_ratio

        self._risks = []
        self._risks = tuple(round(i, 3) for i in tuple(np.arange(start_range_risk,
            end_range_risk+step_range_risk, step_range_risk)))

        if dataset_dir is None:
            self._dataset_dir = Path(__file__).parent / mlc.DATASETS_FOLDER
        else:
            self._dataset_dir = dataset_dir

        if not self._dataset_dir.exists():
            self._dataset_dir.mkdir(parents=True)
        else:
            # Remove existing dataset file
            (self._dataset_dir / (ticker+mlc.DATASET_FILE_SUFFIX)).unlink(missing_ok=True)

        # Spearman correlation derivate variable
        self._spearman_corr_column_names = tuple(['spearman_corr_' + str(n) + '_day' \
            for n in spearman_correlations])
        self._spearman_reference = tuple([i for i in range(max(spearman_correlations))])

        self._first_write_on_file = True


    @property
    def ticker(self):
        return self._ticker

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date

    @property
    def risks(self):
        return self._risks

    @property
    def spearman_correlations(self):
        return self._spearman_correlations

    @property
    def lpf_alpha(self):
        return self._lpf_alpha

    @property
    def dataset_dir(self):
        return self._dataset_dir

    @property
    def max_days_per_operation(self):
        return self._max_days_per_operation

    @property
    def gain_loss_ratio(self):
        return self._gain_loss_ratio

    @property
    def spearman_corr_column_names(self):
        return self._spearman_corr_column_names

    @property
    def spearman_reference(self):
        return self._spearman_reference

    @property
    def first_write_on_file(self):
        return self._first_write_on_file

    @first_write_on_file.setter
    def first_write_on_file(self, first_write_on_file):
        self._first_write_on_file = first_write_on_file

    # TODO: Stop accumulating data non-stop through lists
    def generate_dataset(self):
        # Trend parameters
        N_pri = 20
        N_vol = 60
        N_dot = 2
        spearman_up_threshold = 0.5
        downtrend_inertia = 3
        anomalies_inertia = 2
        crisis_halt_inertia = 8

        days_before_start = int(max(N_pri, N_vol, N_dot, max(self.spearman_correlations)) * 1.8)

        # Get daily and weekly candles
        db_model = DBStrategyAnalyzerModel()
        candles_df_day = db_model.get_ticker_prices_and_features(self.ticker,
                self.start_date-pd.Timedelta(days=days_before_start), self.end_date,
                interval='1d')

        # General support variables
        last_mid_prices = []
        last_volumes = []
        last_max_prices = []
        last_min_prices = []
        days = []

        # Variables of Trend identification
        cum_spearman = []
        cum_spearman = []
        avg_price = []
        std_price = []
        avg_volume = []
        std_volume = []
        mid_prices_dot = []
        volume_anomalies = 0
        price_down_anomalies = 0

        # Trend auxiliary variables
        downtrend_inertia_counter = downtrend_inertia
        crisis_inertia_counter = crisis_halt_inertia
        anomalies_counter = 0
        in_uptrend_flag = False

        # Trend Output Variables
        uptrend = 0
        downtrend = 0
        crisis = 0

        # Spearman Correlation Variables
        spearman_corrs = {name: 0.0 for name in self.spearman_corr_column_names}

        first_iteration = True
        for row_idx, row in candles_df_day.iterrows():

            open = row['open_price']
            close = row['close_price']
            high = row['max_price']
            low = row['min_price']
            volume = row['volume']
            day = row['day']

            if isinstance(open, pd.Series) or \
                isinstance(close, pd.Series) or \
                isinstance(high, pd.Series) or \
                isinstance(low, pd.Series) or \
                isinstance(volume, pd.Series) or \
                isinstance(day, pd.Series):
                continue

            if first_iteration is True:
                days.append( day )

                # Support variables
                last_mid_prices.append( (open+close)/2 )
                last_max_prices.append( high )
                last_min_prices.append( low )
                last_volumes.append( volume )

                mid_prices_dot = 0.0

                volume_anomalies = 0
                avg_volume.append( 0.0 )
                std_volume.append( 0.0 )

                price_down_anomalies = 0

                # Variables of trend identification
                cum_spearman.append( 0.0 )
                avg_price.append( 0.0 )
                std_price.append( 0.0 )

                # Trend Output Variables
                uptrend = 0
                downtrend = 0
                crisis = 0

                first_iteration = False

            else:
                # Variables of trend identification Section
                if len(last_mid_prices) >= N_pri:
                    cum_spearman.append( stats.spearmanr(
                        self.spearman_reference[0:N_pri],
                        last_mid_prices[len(last_mid_prices) - N_pri : len(last_mid_prices)]).correlation )
                    avg_price.append( np.mean(
                        last_mid_prices[len(last_mid_prices) - N_pri : len(last_mid_prices)]) )
                    std_price.append( np.std(
                        last_mid_prices[len(last_mid_prices) - N_pri : len(last_mid_prices)]) )
                else:
                    cum_spearman.append( 0.0 )
                    avg_price.append( 0.0 )
                    std_price.append( 0.0 )

                if len(last_volumes) >= N_vol:
                    avg_volume.append( np.mean(
                        last_volumes[len(last_volumes) - N_vol : len(last_volumes)]) )
                    std_volume.append( np.std(
                        last_volumes[len(last_volumes) - N_vol : len(last_volumes)]) )
                else:
                    avg_volume.append( 0.0 )
                    std_volume.append( 0.0 )

                if len(last_mid_prices) >= N_dot:
                    # LPF for prices derivative ( y[i] := α * x[i] + (1-α) * y[i-1] )
                    y0 = self.lpf_alpha * ( (last_mid_prices[-1] - last_mid_prices[-2])\
                        / ((last_mid_prices[-2] + last_mid_prices[-1])/2) ) \
                        + (1-self.lpf_alpha) * mid_prices_dot

                    mid_prices_dot = y0
                else:
                    mid_prices_dot = 0.0

                # Volume and Price Down Anomalies Section
                if len(last_volumes) >= N_vol and last_volumes[-1] > avg_volume[-1] + std_volume[-1]:
                    volume_anomalies = 1
                else:
                    volume_anomalies = 0

                if len(last_mid_prices) >= N_pri and last_mid_prices[-1] < avg_price[-1] + std_price[-1]:
                    price_down_anomalies = 1
                else:
                    price_down_anomalies = 0

                # Trend Analysis Section
                # Trend Analysis: Downtrend Analysis
                if len(last_mid_prices) <= N_dot:
                    downtrend = 0
                else:
                    if mid_prices_dot < 0:
                        downtrend = 1
                        downtrend_inertia_counter = 0
                    else:
                        if downtrend_inertia_counter < downtrend_inertia:
                            downtrend = 1
                            downtrend_inertia_counter += 1
                        else:
                            downtrend = 0

                # Trend Analysis: Crisis Analysis
                if len(last_volumes) < N_vol:
                    crisis = 0
                else:
                    if volume_anomalies is True and price_down_anomalies is True:
                        anomalies_counter += 1
                        if anomalies_counter >= anomalies_inertia:
                            crisis = 1
                            crisis_inertia_counter = 0
                        else:
                            if crisis_inertia_counter < crisis_halt_inertia:
                                crisis = 1
                                crisis_inertia_counter += 1
                            else:
                                crisis = 0
                    else:
                        if crisis_inertia_counter < crisis_halt_inertia:
                            crisis = 1
                            crisis_inertia_counter += 1
                        else:
                            crisis = 0

                        if anomalies_counter != 0:
                            anomalies_counter = 0

                # Trend Analysis: Uptrend Analysis
                if len(cum_spearman) < N_pri:
                    uptrend = 0
                else:
                    if in_uptrend_flag is False:
                        if mid_prices_dot > 0 and cum_spearman[-1] >= spearman_up_threshold:
                            uptrend = 1
                            in_uptrend_flag = True
                        else:
                            uptrend = 0
                    else:
                        if mid_prices_dot > 0 and cum_spearman[-1] >= spearman_up_threshold:
                            uptrend = 1
                        else:
                            uptrend = 0
                            in_uptrend_flag = False

                if row['day'] >= self.start_date:

                    for idx, spear_n in enumerate(self.spearman_correlations):
                        if len(last_mid_prices) >= spear_n:
                            corr = stats.spearmanr(self.spearman_reference[:spear_n],
                                last_mid_prices[len(last_mid_prices) - spear_n : len(last_mid_prices)]).correlation

                            if math.isnan(corr):
                                corr = 0.0

                            spearman_corrs[self.spearman_corr_column_names[idx]] = round(corr, 4)
                        else:
                            spearman_corrs[self.spearman_corr_column_names[idx]] = 0.0

                    success_oper_flags = []
                    timeout_flags = []
                    end_of_interval_flags = []
                    purchase_price = open
                    for risk in self.risks:
                        success_oper_flag_temp, timeout_flag_temp, end_of_interval_flag_temp = \
                            self._process_operation_result(purchase_price, risk, candles_df_day, row_idx, 0)

                        success_oper_flags.append( int(success_oper_flag_temp) )
                        timeout_flags.append( int(timeout_flag_temp) )
                        end_of_interval_flags.append( int(end_of_interval_flag_temp) )

                    mid_prices_dot_rounded = round(mid_prices_dot, 4)
                    self._write_on_file(day, success_oper_flags, timeout_flags, end_of_interval_flags,
                        uptrend, downtrend, crisis, mid_prices_dot_rounded, spearman_corrs)

                # Support variables must be the last to avoid non-causality
                last_mid_prices.append( (open+close)/2 )
                last_max_prices.append( high )
                last_min_prices.append( low )
                last_volumes.append( volume )
                days.append( day )


    def _process_operation_result(self, purchase_price, risk, candles_df_day,
        curr_idx, idx_delay=0):

        candles_len = len(candles_df_day)
        days_counter = 0
        op_result = False
        end_of_interval_flg = False
        timeout_flg = False

        target_price = round(purchase_price * (1 + self.gain_loss_ratio * risk), 2)
        stop_price = round(purchase_price * (1 - risk), 2)

        # Verify the future
        for idx_fut, row_fut in candles_df_day.loc[candles_df_day.index[
            curr_idx + idx_delay : candles_len]].iterrows():

            days_counter += 1

            if row_fut['min_price'] <= stop_price:
                break

            if row_fut['max_price'] >= target_price:
                op_result = True
                break

            if days_counter == self.max_days_per_operation:
                timeout_flg = True
                break

            if idx_fut == candles_len - 1:
                end_of_interval_flg = True

        return op_result, timeout_flg, end_of_interval_flg

    def _write_on_file(self, day, success_oper_flags, timeout_flags, end_of_interval_flags,
        uptrend, downtrend, crisis, mid_prices_dot, spearman_corrs):

        mode = 'w' if self.first_write_on_file else 'a'

        spearman_features = {col: spearman_corrs[col] \
            for col in self.spearman_corr_column_names}

        pd.DataFrame({'ticker': [self.ticker for _ in range(len(self.risks))],
            'day': [day for _ in range(len(self.risks))],
            'risk': [risk for risk in self.risks],
            'success_oper_flag': [flag for flag in success_oper_flags],
            'timeout_flag': [flag for flag in timeout_flags],
            'end_of_interval_flag': [flag for flag in end_of_interval_flags],
            'uptrend': [uptrend for _ in range(len(self.risks))],
            'downtrend': [downtrend for _ in range(len(self.risks))],
            'crisis': [crisis for _ in range(len(self.risks))],
            'mid_prices_dot': [mid_prices_dot for _ in range(len(self.risks))],
            **spearman_features}).\
            to_csv(self.dataset_dir / (self.ticker+mlc.DATASET_FILE_SUFFIX),
                mode=mode, index=False, header=self.first_write_on_file)

        self.first_write_on_file = False