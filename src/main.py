import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# import utils
import constants as c
import config_reader as cr
from db_model import DBGeneralModel, DBStrategyAnalyzerModel
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
    logger.info('Program started.')

    general_info = DBGeneralModel()

    # Read Config File
    config = cr.ConfigReader()
    ticker_managers = []

    # Create TickerManager objects to update and process then
    for ticker, date in config.tickers_and_dates.items():
        ticker_managers.append(TickerManager(ticker, date['start_date'], date['end_date']))

    ticker_managers.append(TickerManager('^BVSP', config.min_start_date, config.max_end_date, common_ticker_flag=False)) # IBOVESPA Index
    ticker_managers.append(TickerManager('BRL=X', config.min_start_date, config.max_end_date, common_ticker_flag=False)) # USD/BRL

    # Update data accordingly
    for ticker_manager in ticker_managers:
        ticker_manager._holidays = config.holidays
        ticker_manager.update_interval()
        ticker_manager.generate_features()

    # Strategy section
    # for strategy in config.strategies:
    #     if strategy['name'] == "Andre Moraes":
    #         strategy = AndreMoraesStrategy(strategy['tickers'], total_capital=strategy['capital'], risk_capital_product=strategy['risk_capital_coefficient'])
    #         strategy.alias = strategy['alias']
    #         strategy.comment = strategy['comment']

    #         weekly_candles = general_info.get_candles_dataframe(ticker_names, initial_dates, final_dates, interval='1wk')
    #         daily_candles = general_info.get_candles_dataframe(ticker_names, initial_dates, final_dates, interval='1d', days_before_initial_dates=180)

    #         strategy.set_input_data(weekly_candles, interval='1wk')
    #         strategy.set_input_data(daily_candles, interval='1d')

    #         strategy.process_operations()
    #         strategy.calculate_statistics()
    #         strategy.save()

    # Strategy Analysis section
    analyzer = StrategyAnalyzer()
    analyzer.run()

if __name__ == '__main__':
    run()
