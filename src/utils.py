import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import json
from contextlib import ContextDecorator
from time import time
import sys
from datetime import datetime

import constants as c

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(c.LOG_PATH)
log_path = log_path / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

class RunTime(ContextDecorator):
    """Timing decorator."""

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

@RunTime('read_cfg')
def read_cfg():
    '''Read config.json file in the given path and returns it as a dictionary of parameters.'''
    
    config_path = Path('.') / c.CONFIG_PATH / c.CONFIG_FILENAME

    logger.debug("Searching for config file in '" + str(config_path) + "'.")

    try:
        with open(config_path, 'r') as cfg_file:
            try:
                config_json = json.load(cfg_file)
            except ValueError:
                logger.exception('Program aborted. Expected config file in JSON format. Is it corrupted?')
                sys.exit(c.CONFIG_FILE_ERR)
    except FileNotFoundError:
        logger.exception('Program aborted. Couldn\'t open configuration file: \'' + str(config_path) + '\'.')
        sys.exit(c.CONFIG_FILE_ERR)

    logger.debug('File found.')
    return config_json

@RunTime('get_ticker_data')
def get_ticker_data(configuration):
    '''Return ticker information collected from configuration file.
    
    Parameters:
        configuration (dict): Configuration file in form of dict
    
    Returns:
        ticker_names (list[str]): List of tickers
        initial_days (list[datetime]): List of initial days of tickers, respectively.
        final_days (list[datetime]): List of final days of tickers, respectively.
        
        * All return variables have the same length.
    '''

    ticker_names = [item['name'] for item in configuration['stock_targets'] if "name" in item.keys()]
    initial_days_str = [item['initial_date'] for item in configuration['stock_targets'] if "initial_date" in item.keys()]
    final_days_str = [item['final_date'] for item in configuration['stock_targets'] if "final_date" in item.keys()]

    if (ticker_names is None) or (initial_days_str is None) or (final_days_str is None):
        logging.error('Program aborted. Config file does not contain any ticker.')
        sys.exit()

    if not(len(ticker_names) == len(initial_days_str) == len(final_days_str)):
        logging.error('Program aborted. Missing values in parameter "stock_targets" of the config file.')
        sys.exit()

    if len(ticker_names) != len(set(ticker_names)):
        logging.error("Program aborted. Duplicate tickers not permitted.")
        sys.exit()

    initial_days = [datetime.strptime(day, '%d/%m/%Y') for day in initial_days_str]
    final_days = [datetime.strptime(day, '%d/%m/%Y') for day in final_days_str]

    for index, (start, end) in enumerate(zip(initial_days, final_days)):
        if start > end:
            logging.error('Program aborted. Final date greater than initial date for the stock "'+ticker_names[index]+'".')
            sys.exit()

    return ticker_names, initial_days, final_days



