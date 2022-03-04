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

def get_capital_per_risk(risk_capital_coefficient, total_capital, operation_risk):
    return round(total_capital * risk_capital_coefficient / operation_risk, 2)

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


