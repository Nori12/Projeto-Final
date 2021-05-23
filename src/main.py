import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import utils
import constants as c
from ticker_manager import TickerManager

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

def run():

    logger.info('Program started.')

    # Read Config File ad store it in a dict
    config_json = utils.read_cfg()

    ticker_names, initial_days, final_days = utils.get_ticker_config_data(config_json)

    for ticker, initial_day, final_day in zip(ticker_names, initial_days, final_days):
        logger.info('Ticker: \''+ticker.ljust(6)+'\'\tInital date: '+initial_day.strftime('%d/%m/%Y')+'\t\tFinal date: '+final_day.strftime('%d/%m/%Y'))

    all_ticker_managers = [TickerManager(ticker_names[i], initial_days[i], final_days[i], config_json['input_files_path'], config_json['output_files_path']) for i in range(len(ticker_names))]

    for ticker in all_ticker_managers:
        ticker.update_interval()

    """
    Generate 3 candle datasets
        Input:  Yahoo Finance, CSV files;pwd
        Output: Weekly/Day/60min candle datasets;
    """

    """
    Plot candle datasets
    """

    """
    GenerateFeatures
        Input:  Weekly/Day/60min candle datasets;
                Features desired;
        Output: Weekly/Day/60min candle datasets + columns for features;
    """

    """
    RootStrategy
        Base class from which other strategies will inherit
        Output: Log file of operations

        AndreMorais
        CustomAndreMorais
    """

    """
    Analysis
        Compare results to IBOV
        Analyze yield
        Check Sharpe ration
        Plot graphs with results

        Input:  Log file of operations
        Output: Graphs
    """

if __name__ == '__main__':
    run()


