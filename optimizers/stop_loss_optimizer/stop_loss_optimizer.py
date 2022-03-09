import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import pandas as pd
import numpy as np

sys.path.insert(1, str(Path(__file__).parent.parent.parent/'src'))
import constants as c
import config_reader as cr
from db_model import DBStrategyAnalyzerModel

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

STD_FILE_SUFFIX = "_stop_loss_optimizer.csv"
RISKMAP_FILE_SUFFIX = "_risk_map.csv"

class StopLossOptimizer:

    def __init__(self, buy_type='current_day_open_price', min_risk=0.01,
        max_risk=0.15, gain_loss_ration=3, max_days_per_operation=90):

        if buy_type not in ['current_day_open_price', 'last_day_close_price']:
            raise Exception("'buy_type' parameter options: " \
                "'current_day_open_price', 'last_day_close_price'.")
        self._buy_type = buy_type

        self._min_risk = min_risk
        self._max_risk = max_risk
        self._gain_loss_ratio = gain_loss_ration
        self._max_days_per_operation = max_days_per_operation
        self.out_file_path_prefix = Path(__file__).parent / "out_csv_files"

        self._risk_step = 0.002
        self._risk_thresholds = [round(i, 3) for i in \
            np.arange(self.min_risk, self.max_risk + self._risk_step, self._risk_step)]

        cfg_path = Path(__file__).parent / "config.json"

        config_reader = cr.ConfigReader(config_file_path=cfg_path)
        self._tickers_and_dates = config_reader.tickers_and_dates

    @property
    def min_risk(self):
        return self._min_risk

    @min_risk.setter
    def min_risk(self, min_risk):
        self._min_risk = min_risk

    @property
    def max_risk(self):
        return self._max_risk

    @max_risk.setter
    def max_risk(self, max_risk):
        self._max_risk = max_risk

    @property
    def buy_type(self):
        return self._buy_type

    @buy_type.setter
    def buy_type(self, buy_type):
        self._buy_type = buy_type

    @property
    def gain_loss_ratio(self):
        return self._gain_loss_ratio

    @gain_loss_ratio.setter
    def gain_loss_ratio(self, gain_loss_ratio):
        self._gain_loss_ratio = gain_loss_ratio

    @property
    def max_days_per_operation(self):
        return self._max_days_per_operation

    @max_days_per_operation.setter
    def max_days_per_operation(self, max_days_per_operation):
        self._max_days_per_operation = max_days_per_operation

    @property
    def tickers_and_dates(self):
        return self._tickers_and_dates

    @tickers_and_dates.setter
    def tickers_and_dates(self, tickers_and_dates):
        self._tickers_and_dates = tickers_and_dates

    def run_simulation(self, max_tickers=0, standard_map = True, risk_map=False):

        if not any([standard_map, risk_map]):
            raise Exception("At least one simulation must be choosen.")

        db_model = DBStrategyAnalyzerModel()

        # For each Ticker
        for tck_index, (ticker, date) in enumerate(self.tickers_and_dates.items()):

            if tck_index == max_tickers and max_tickers != 0:
                break

            print(f"Processing Ticker '{ticker}' ({tck_index+1} of " \
                f"{len(self.tickers_and_dates)})")

            # Get daily candles
            candles_df_day = db_model.get_ticker_prices_and_features(ticker,
                pd.Timestamp(date['start_date']), pd.Timestamp(date['end_date']),
                interval='1d')

            purchase_price = None

            if standard_map is True:
                business_data = StopLossOptimizer._init_business_data(ticker, candles_df_day)
            if risk_map is True:
                riskmap_params, riskmap_data = self._init_risk_map(ticker,
                    candles_df_day)

            # For each Day
            for idx, row in candles_df_day.iterrows():

                if self.buy_type == "current_day_open_price":
                    purchase_price = row['open_price']
                    idx_delay = 0
                elif self.buy_type == "last_day_close_price":
                    purchase_price = row['close_price']
                    idx_delay = 1

                if standard_map is True:
                    self._process_major_data(business_data, purchase_price,
                        candles_df_day, idx, idx_delay)

                if risk_map is True:
                    self._process_risk_map_data(riskmap_params, riskmap_data,
                        purchase_price, candles_df_day, idx, idx_delay)

            if standard_map is True:
                pd.DataFrame(business_data).to_csv(
                    self.out_file_path_prefix / (ticker + STD_FILE_SUFFIX),
                    mode='w', index=False, header=True)
            if risk_map is True:
                pd.DataFrame(riskmap_data).to_csv(
                    self.out_file_path_prefix / (ticker + RISKMAP_FILE_SUFFIX),
                    mode='w', index=False, header=True)

    @staticmethod
    def _init_business_data(ticker, candles_df_day):

        length = len(candles_df_day)

        business_data = {
            "ticker": [ticker] * length,
            "day": candles_df_day['day'].to_list(),
            "success_oper_flag": [0] * length,
            "timeout_flag": [0] * length,
            "end_of_interval_flag": [0] * length,
            "best_risk": [0.0] * length,
            "best_risk_days": [0] * length,
            "min_risk": [0.0] * length,
            "min_risk_days": [0] * length,
            "max_risk": [0.0] * length,
            "max_risk_days": [0] * length,
        }

        return business_data

    def _init_risk_map(self, ticker, candles_df_day):

        length = len(candles_df_day)

        riskmap_params = {
            "ticker": ticker,
            "risk_column_names": [],
            "risk_column_values": []
        }

        riskmap_data = {
            "ticker": [ticker] * length,
            "day": candles_df_day['day'].to_list(),
        }

        for i in self._risk_thresholds:
            big_i = round(i * 100, 1)
            integer_part = int(big_i)
            decimal_part = int(round((big_i - integer_part) * 10, 0))

            column_name = str(integer_part)
            if decimal_part > 0:
                column_name += "_" + str(decimal_part)
            column_name += "_p"

            riskmap_data[column_name] = [0] * length
            riskmap_params['risk_column_names'].append(column_name)
            riskmap_params['risk_column_values'].append(i)

        return riskmap_params, riskmap_data

    def _process_major_data(self, business_data, purchase_price, candles_df_day,
        curr_idx, idx_delay):

        candles_len = len(candles_df_day)

        min_price = purchase_price
        max_price = purchase_price
        cur_risk = 0
        cur_risk_days = 0
        days_counter = 0
        min_risk_flag = False
        best_yield = 0.0
        cur_daily_yield = 0.0

        quit_stop_price = round(purchase_price * (1 - self.max_risk), 2)
        target_price = 0.0

        # Verify the future
        for idx_fut, row_fut in candles_df_day.loc[candles_df_day.index[
            curr_idx + idx_delay : candles_len]].iterrows():

            days_counter += 1
            changed_target_today = False

            if row_fut['min_price'] < quit_stop_price:
                min_price = quit_stop_price
                break

            if row_fut['min_price'] < min_price:
                min_price = row_fut['min_price']
                target_price = round(purchase_price +
                    self.gain_loss_ratio * (purchase_price - min_price), 2)
                changed_target_today = True

            if row_fut['max_price'] > max_price and target_price != 0.0 \
                and not changed_target_today:

                max_price = row_fut['max_price']

            if max_price >= target_price and not changed_target_today \
                and target_price != 0.0:

                cur_daily_yield = (max_price / purchase_price) ** (1 / days_counter)
                cur_risk = round(
                    (max_price / purchase_price - 1) / self.gain_loss_ratio, 4)
                cur_risk_days = days_counter

                if cur_daily_yield > best_yield:
                    best_yield = cur_daily_yield
                    business_data['best_risk'][curr_idx] = cur_risk
                    business_data['best_risk_days'][curr_idx] = cur_risk_days

                if not min_risk_flag:
                    business_data['success_oper_flag'][curr_idx] = 1
                    business_data['min_risk'][curr_idx] = round(1 - min_price / purchase_price, 4)
                    business_data['min_risk_days'][curr_idx] = cur_risk_days
                    min_risk_flag = True

                if cur_risk > business_data['max_risk'][curr_idx]:

                    business_data['max_risk'][curr_idx] = cur_risk
                    business_data['max_risk_days'][curr_idx] = cur_risk_days

                    if cur_risk >= self.max_risk:
                        business_data['max_risk'][curr_idx] = self.max_risk
                        break

            if days_counter == self.max_days_per_operation:
                if business_data['success_oper_flag'][curr_idx] != 1:
                    business_data['timeout_flag'][curr_idx] = 1
                break

            if idx_fut == candles_len - 1:
                business_data['end_of_interval_flag'][curr_idx] = 1

    def _process_risk_map_data(self, riskmap_params, riskmap_data, purchase_price,
        candles_df_day, curr_idx, idx_delay):

        candles_len = len(candles_df_day)

        min_price = purchase_price
        max_price = purchase_price
        days_counter = 0
        max_risk = 0.0

        # True if that risk range is already checked, either successfully or not
        riskmap_mask = [False] * len(riskmap_params['risk_column_values'])

        quit_stop_price = round(purchase_price * (1 - self.max_risk), 2)
        target_price = 0.0

        # Verify the future
        for idx_fut, row_fut in candles_df_day.loc[candles_df_day.index[
            curr_idx + idx_delay : candles_len]].iterrows():

            days_counter += 1
            changed_target_today = False

            # Price is below range
            if row_fut['min_price'] < min_price:

                min_price = row_fut['min_price']
                max_risk = round((purchase_price - min_price) / purchase_price, 4)

                target_price = round(purchase_price +
                    self.gain_loss_ratio * (purchase_price - min_price), 2)
                changed_target_today = True

                for idx, risk in enumerate(riskmap_params['risk_column_values']):
                    if max_risk >= risk:
                        if riskmap_mask[idx] is False:
                            # riskmap_data[ riskmap_params['risk_column_names'][idx] ][curr_idx] = 0
                            riskmap_mask[idx] = True
                    else:
                        break

            # Price is above range
            if row_fut['max_price'] > target_price and not changed_target_today \
                and target_price != 0.0:

                max_price = row_fut['max_price']
                max_risk = round(
                    (max_price / purchase_price - 1) / self.gain_loss_ratio, 4)

                for idx, risk in enumerate(riskmap_params['risk_column_values']):
                    if max_risk >= risk:
                        if riskmap_mask[idx] is False:
                            riskmap_data[ riskmap_params['risk_column_names'][idx] ][curr_idx] = days_counter
                            riskmap_mask[idx] = True
                    else:
                        break

                target_price = max_price
                min_price = round(
                    purchase_price * (1 - (target_price - 1) / self.gain_loss_ratio), 2)

            # Exit conditions
            if all(riskmap_mask) \
                or days_counter == self.max_days_per_operation \
                or idx_fut == candles_len - 1 \
                or row_fut['min_price'] < quit_stop_price:
                break

if __name__ == '__main__':
    logger.info('Stop Loss Optimizer started.')

    sl_opt = StopLossOptimizer()
    sl_opt.run_simulation(max_tickers=0, standard_map=True, risk_map=True)