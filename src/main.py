import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# import utils
import constants as c
import config_reader as cr
from db_model import DBGenericModel, DBStrategyAnalyzerModel
from ticker_manager import TickerManager
from strategy import AndreMoraesStrategy
from strategy_analyzer import StrategyAnalyzer

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
    logger.info('\nProgram started.')

    general_info = DBGenericModel()

    # Read Config File
    config = cr.ConfigReader()
    ticker_managers = []

    # Create TickerManager objects to update and process then
    for ticker, date in config.tickers_and_dates.items():
        ticker_managers.append(TickerManager(ticker, date['start_date'], date['end_date']))

    ticker_managers.append(TickerManager('^BVSP', config.min_start_date,
        config.max_end_date, ordinary_ticker=False)) # IBOVESPA Index
    ticker_managers.append(TickerManager('BRL=X', config.min_start_date,
        config.max_end_date, ordinary_ticker=False)) # USD/BRL

    # Update and generate features
    for ticker_manager in ticker_managers:
        ticker_manager.holidays = config.holidays
        update = ticker_manager.update()
        # if update == True:
        ticker_manager.generate_features()

    # Strategy section
    for strategy in config.strategies:
        if strategy['name'] == 'Andre Moraes':
            andre_moraes = AndreMoraesStrategy(
                strategy['tickers'],
                total_capital=strategy['capital'],
                risk_capital_product=strategy['risk_capital_coefficient'])
            andre_moraes.alias = strategy['alias']
            andre_moraes.comment = strategy['comment']

            weekly_candles = general_info.get_candles_dataframe(config.tickers_and_dates,
                interval='1wk')
            daily_candles = general_info.get_candles_dataframe(config.tickers_and_dates,
                interval='1d', days_before_initial_dates=180)

            andre_moraes.set_input_data(weekly_candles, interval='1wk')
            andre_moraes.set_input_data(daily_candles, interval='1d')

            andre_moraes.process_operations()
            andre_moraes.calculate_statistics()
            andre_moraes.save()

    # Strategy Analysis section
    if config.show_results == True:
        analyzer = StrategyAnalyzer()
        analyzer.run()

if __name__ == '__main__':
    run()
