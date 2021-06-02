import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# import utils
import constants as c
import config_reader as cr
from db_model import DBGeneralModel
from ticker_manager import TickerManager
from strategy import AndreMoraesStrategy

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

    # Read Config File
    config = cr.ConfigReader()
    ticker_names, initial_dates, final_dates = config.tickers_and_dates

    general_info = DBGeneralModel()
    holidays = general_info.get_holidays(min(initial_dates), max(final_dates))

    for ticker, initial_day, final_day in zip(ticker_names, initial_dates, final_dates):
        logger.info('Ticker: \''+ticker.ljust(6)+'\'\tInital date: '+initial_day.strftime('%d/%m/%Y')+'\t\tFinal date: '+final_day.strftime('%d/%m/%Y'))

    all_ticker_managers = [TickerManager(ticker_names[i], initial_dates[i], final_dates[i]) for i in range(len(ticker_names))]

    for ticker_manager in all_ticker_managers:
        ticker_manager._holidays = holidays
        ticker_manager.update_interval()


    # Strategy section

    # moraes_strat = AndreMoraesStrategy( "André Moraes", ticker_names, initial_dates, final_dates)

    # moraes_strat.set_input_data(general_info.get_candles_dataframe(ticker_names, initial_dates, final_dates, interval='1wk'), interval='1wk')
    # moraes_strat.set_input_data(general_info.get_candles_dataframe(ticker_names, initial_dates, final_dates, interval='1d'), interval='1d')

if __name__ == '__main__':
    run()
