import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import json
from contextlib import ContextDecorator
from time import time
import sys
from datetime import datetime, timedelta
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

class RunTime(ContextDecorator):
    """Timing decorator.

    Log the execution time of the specified function."""

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

def compare_dates(first, second, holidays):
    """
    first > second: positive
    first < second: negative
    second = first: zero
    """

    wokdays_in_between = int(np.busday_count(
        begindates=first.strftime('%Y-%m-%d'),
        enddates=(second).strftime('%Y-%m-%d'),
        holidays=holidays))

    if wokdays_in_between == 0 or wokdays_in_between == -1:
        return 0
    elif wokdays_in_between > 0:
        return 1

    return -1

def calculate_maximum_volume(price, max_capital, minimum_volume=100):

    volume = max_capital // price
    remaining_volume = volume % minimum_volume
    volume = volume - remaining_volume

    return volume

class State(Enum):
    NOT_STARTED = "NOT STARTED"
    OPEN = "OPEN"
    CLOSE = "CLOSE"