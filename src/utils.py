import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from contextlib import ContextDecorator
from time import time
from datetime import timedelta
import numpy as np
from enum import Enum

import constants as c

# Configure Logging
logger = logging.getLogger(__name__)
log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME
file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

class State(Enum):
    NOT_STARTED = "NOT STARTED"
    OPEN = "OPEN"
    CLOSE = "CLOSE"

class Trend(Enum):
    UPTREND = 1
    ALMOST_UPTREND = 0.5
    CONSOLIDATION = 0
    ALMOST_DOWNTREND = -0.5
    DOWNTREND = -1

# Peak Comparison
class PC(Enum):
    FIRST_IS_GREATER = 1
    BOTH_ARE_CLOSE = 0
    FIRST_IS_LESSER = -1

class RunTime(ContextDecorator):
    """
    Timing decorator.

    Log the execution time of the specified function in milliseconds.
    """

    def __init__(self, function_name):
        self.function_name = function_name
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        self.start_time = time()

    def __exit__(self, *args):
        self.end_time = time()
        run_time = self.end_time - self.start_time
        if run_time >= 0.00001:
            logger.debug(f"The function '{self.function_name}' took {run_time*1000:.2f} milliseconds to run.")
        else:
            logger.debug(f"The function '{self.function_name}' took less than 10 microseconds to run.")

def has_workdays_in_between(oldest_date, recent_date, holidays,
    consider_oldest_date=False, consider_recent_date=False):
    """
    Check if date interval has any workday in between.

    Open interval by default.

    Args
    ----------
    oldest_date : `datetime.date`
        Oldest date.
    recent_date : `datetime.date`
        Most recent date.
    consider_oldest_date : bool, default False
        Indicate whether or not to consider the day of `oldest_date`
    consider_recent_date : bool, default False
        Indicate whether or not to consider the day of `recent_date`

    Returns
    ----------
    bool
        True  : At least one workday.
        False : No workday or `recent_date` is actually older than `oldest_date`.
    """
    if consider_oldest_date is False:
        oldest_date = oldest_date + timedelta(days=1)
    if consider_recent_date is True:
        recent_date = recent_date + timedelta(days=1)

    wokdays_in_between = int(np.busday_count(
        begindates=oldest_date.strftime('%Y-%m-%d'),
        enddates=(recent_date).strftime('%Y-%m-%d'),
        holidays=holidays))

    if wokdays_in_between > 0:
        return True

    return False

def calculate_maximum_volume(price, max_capital, minimum_volume=100):

    volume = max_capital // price
    remaining_volume = volume % minimum_volume
    volume = volume - remaining_volume

    return volume

def calculate_yield_annualized(in_yield, bus_day_count):

    total_bus_day_per_year = 252

    annualized_yield = (1+in_yield) ** (total_bus_day_per_year/bus_day_count)

    return round(annualized_yield - 1, 4)

def compare_peaks(peak_1, peak_2, tolerance=0.01):
    """
    Return  PC.FIRST_IS_GREATER if peak_1 > peak_2
            PC.BOTH_ARE_CLOSE if peak_1 = peak_2
            PC.FIRST_IS_LESSER if peak_1 < peak_2
    """

    if peak_1 > peak_2 * (1+tolerance):
        return PC.FIRST_IS_GREATER
    elif peak_1 >= peak_2 * (1-tolerance):
        return PC.BOTH_ARE_CLOSE
    else:
        return PC.FIRST_IS_LESSER

def get_capital_per_risk(risk_capital_coefficient, total_capital, operation_risk,
    capital_multiplier):
    return round(min(total_capital,
        total_capital * (risk_capital_coefficient / operation_risk) * capital_multiplier), 2)

def get_avg_index_of_first_burst_of_ones(some_list):

    start_idx = 0
    start_idx_flg = False
    end_idx = 0

    for idx, item in enumerate(some_list):
        if item == 1 and start_idx_flg is False:
            start_idx = idx
            end_idx = idx
            start_idx_flg = True
        elif item == 1 and start_idx_flg is True:
            end_idx = idx
        elif item == 0 and start_idx_flg is True:
            break

    avg_idx = start_idx + (end_idx - start_idx) // 2

    return avg_idx

def my_dynamic_cast(string_variable, dest_type=str, list_type=None):
    """
    Return pair o converted_value, convertion_flag.

    'list_type' is for supporting multiple parameters in list format.

    convertion_flag is True if convertion was successfull.
    If not, ignore converted_value.
    """
    if string_variable == 'None':
        return None, True

    result = string_variable

    if list_type is None:
        if dest_type != str:
            try:
                result = dest_type(string_variable)
            except Exception:
                print(f"Value '{string_variable}' must be of type '{str(dest_type.__name__)}'.")
                return None, False
    else:
        input = string_variable.replace('[', '')
        input = input.replace(']', '')
        input = input.replace("'", '')
        input = input.replace(" ", '')
        input = input.strip()

        result = input.split(',')

        for idx, value in enumerate(result):
            if list_type != str:
                try:
                    result[idx] = list_type(value)
                except Exception:
                    print(f"Value '{result[idx]}' must be of type '{str(dest_type.__name__)}'.")
                    return None, False

    return result, True

def my_to_list(var):
    """
    Convert any variable to unitary list. If variable is already a list, do not do anything.
    """
    if type(var) == list:
        return var
    if type(var) == tuple:
        return list(var)
    return [var]

def remove_row_from_last_n_peaks(training_df, backward_peaks=4):
    """
    Remove the n peaks from given DataFrame.
    Specific to application tickers dataset structure.
    """

    last_day = training_df['day'].tail(1).squeeze()
    last_day_1_distance = 0
    idx_counter = 0
    end_index = None

    for idx, row in training_df[::-1].iterrows():
        if last_day_1_distance \
            and row['day'] != last_day \
            and row['day_1'] != last_day_1_distance + 1:

            idx_counter += 1

            if idx_counter == backward_peaks:
                end_index = idx
                break

        last_day = row['day']
        last_day_1_distance = row['day_1']


    return training_df[0:end_index+1]

def take_best_n_indexes(series, n):

    best_values = sorted(series, reverse=True)
    best_indexes = []

    for idx in range(len(best_values)):
        new_value = series.index(best_values[idx])

        if new_value not in best_indexes:
            best_indexes.append(new_value)

        if len(best_indexes) == min(n, len(series)):
            break

    return tuple(best_indexes)

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

    return analyze_peaks(max_peaks_index, min_peaks_index,
        max_peaks_values, min_peaks_values)
