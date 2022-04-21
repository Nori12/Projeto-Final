import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(1, str(Path(__file__).parent.parent/'src'))
import constants as c
import ml_constants as mlc
import config_reader as cr
from db_model import DBStrategyAnalyzerModel

class TickerDatasetGenerator:

    def __init__(self, ticker, start_date, end_date, buy_type='current_day_open_price',
        gain_loss_ratio=3, peaks_pairs_number=2, risk_option='range', fixed_risk=0.03,
        start_range_risk=0.01, step_range_risk=0.002, end_range_risk=0.12,
        max_days_per_operation=45, dataset_dir=None):

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

            data_row = TickerDatasetGenerator._init_data_row(self.ticker, row['day'], 0)

            ref_price_col_name = 'open_price' \
                if self.buy_type == 'current_day_open_price' \
                else 'close_price' if self.buy_type == 'last_day_close_price' \
                else ''

            TickerDatasetGenerator._fill_data_row(data_row, row['ema_17'], row['ema_72'],
                TickerDatasetGenerator._get_ema_72_week(candles_df_day, idx, candles_df_wk),
                row[ref_price_col_name])

            TickerDatasetGenerator._update_peaks_days(peaks_data)
            peaks_ready_flg = self._process_and_check_last_peaks(peaks_data,
                row['peak'], row['max_price'], row['min_price'])
            if peaks_ready_flg is False:
                continue

            if not self._verify_business_data_integrity( (data_row['ema_17_day'],
                data_row['ema_72_day'], data_row['ema_72_week']), peaks_data ):
                continue

            if self.risk_option == 'fixed':

                data_row['risk']= self.fixed_risk

                data_row['success_oper_flag'], data_row['timeout_flag'], \
                    data_row['end_of_interval_flag'] = self._process_operation_result(
                    purchase_price, self.fixed_risk, candles_df_day, idx, idx_delay)

                self._fill_business_data(business_data, data_row, peaks_data,
                        add_ref_price)

            elif self.risk_option == 'range':

                for risk in self.risks:
                    data_row['risk']= risk

                    data_row['success_oper_flag'], data_row['timeout_flag'], \
                    data_row['end_of_interval_flag'] = self._process_operation_result(
                        purchase_price, risk, candles_df_day, idx, idx_delay)

                    self._fill_business_data(business_data, data_row, peaks_data,
                        add_ref_price)

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

        return business_data

    @staticmethod
    def _init_data_row(ticker, day, risk=0.0):

        data_row = {'ticker': ticker, 'day': day, 'risk': risk, 'ema_17_day': 0.0,
            'ema_72_day': 0.0, 'ema_72_week': 0.0}

        return data_row

    @staticmethod
    def _fill_data_row(data_row, ema_17_day, ema_72_day, ema_72_week, ref_price):
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

    @staticmethod
    def _update_peaks_days(peaks_data):

        for idx in range(len(peaks_data['last_max_peaks_days'])):
            peaks_data['last_max_peaks_days'][idx] -= 1

        for idx in range(len(peaks_data['last_min_peaks_days'])):
            peaks_data['last_min_peaks_days'][idx] -= 1

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

    def _fill_business_data(self, business_data, data_row, peaks_data, add_ref_price):

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

