import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import utils as u
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

def run():
    
    logger.info('Program started.')
    logger.info('Reading config file.')
    
    # Read Config File ad store it in a dict
    config_json = u.read_cfg()
    
    ticker_names, initial_days, final_days = u.get_ticker_data(config_json)

    for ticker, initial_day, final_day in zip(ticker_names, initial_days, final_days):
        logger.info('Ticker: '+ticker.ljust(6)+'\tInital date: '+initial_day.strftime('%d/%m/%Y')+'\t\tFinal date: '+final_day.strftime('%d/%m/%Y'))

    '''
    Generate 3 candle datasets
        Input:  Yahoo Finance, CSV files;pwd
        Output: Weekly/Day/60min candle datasets;
    '''

    '''
    Plot candle datasets
    '''

    '''
    GenerateFeatures
        Input:  Weekly/Day/60min candle datasets;
                Features desired;
        Output: Weekly/Day/60min candle datasets + columns for features;
    '''

    '''
    RootStrategy
        Base class from which other strategies will inherit 
        Output: Log file of operations

        AndreMorais
        CustomAndreMorais
    '''

    '''
    Analysis
        Compare results to IBOV
        Analyze yield
        Check Sharpe ration
        Plot graphs with results

        Input:  Log file of operations
        Output: Graphs
    '''

if __name__ == '__main__':
    run()


