import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

# import utils
import constants as c
import config_reader as cr
from db_model import DBGenericModel, DBStrategyAnalyzerModel
from ticker_manager import TickerManager
from operation import Operation
from strategy import AndreMoraesStrategy, AndreMoraesAdaptedStrategy
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

    # Read Config File
    config_file = Path(__file__).parent.parent/c.CONFIG_PATH/'config.json'
    config = cr.ConfigReader(config_file)
    ticker_managers = []

    # Create TickerManager objects to update and process then
    for ticker, date in config.tickers_and_dates.items():
        ticker_managers.append(TickerManager(ticker, date['start_date'], date['end_date']))

    ticker_managers.append(TickerManager('^BVSP', config.min_start_date,
        config.max_end_date, ordinary_ticker=False)) # IBOVESPA Index
    ticker_managers.append(TickerManager('BRL=X', config.min_start_date,
        config.max_end_date, ordinary_ticker=False)) # USD/BRL

    # Update and generate features
    for tm in ticker_managers:
        tm.holidays = config.holidays
        tm.min_risk = config.min_risk_features
        tm.max_risk = config.max_risk_features

        update_ok = tm.update()
        if update_ok is True:
        # if True:
            features_ok = tm.generate_features()
            # Remove inconsistent tickers from all strategies
            if features_ok is False:
                for index in range(len(config.strategies)):
                    if tm.ticker in list(config.strategies[index]['tickers'].keys()):
                        config.strategies[index]['tickers'].pop(tm.ticker)

    # Strategy section
    for strategy in config.strategies:
        if strategy['name'] == 'Andre Moraes':
            am = AndreMoraesStrategy(
                strategy['tickers'],
                min_order_volume=strategy['min_order_volume'],
                total_capital=strategy['capital'],
                risk_capital_product=strategy['risk_capital_coefficient'],
                min_volume_per_year=strategy['ticker_min_ann_volume_filter'])
            am.alias = strategy['alias']
            am.comment = strategy['comment']
            am.partial_sale = strategy['partial_sale']
            am.ema_tolerance = strategy['ema_tolerance']
            am.min_risk = strategy['min_risk']
            am.max_risk = strategy['max_risk']
            am.purchase_margin = strategy['purchase_margin']
            am.stop_margin = strategy['stop_margin']
            am.stop_type = strategy['stop_type']
            am.min_days_after_successful_operation = strategy['min_days_after_successful_operation']
            am.min_days_after_failure_operation = strategy['min_days_after_failure_operation']
            am.max_days_per_operation = config.max_days_per_operation

            am.process_operations()
            am.calculate_statistics()
            am.save()

        if strategy['name'] == 'Andre Moraes Adapted':
            ama = AndreMoraesAdaptedStrategy(
                strategy['tickers'],
                min_order_volume=strategy['min_order_volume'],
                total_capital=strategy['capital'],
                risk_capital_product=strategy['risk_capital_coefficient'],
                min_volume_per_year=strategy['ticker_min_ann_volume_filter'])
            ama.alias = strategy['alias']
            ama.comment = strategy['comment']
            ama.partial_sale = strategy['partial_sale']
            ama.ema_tolerance = strategy['ema_tolerance']
            ama.min_risk = strategy['min_risk']
            ama.max_risk = strategy['max_risk']
            ama.purchase_margin = strategy['purchase_margin']
            ama.stop_margin = strategy['stop_margin']
            ama.stop_type = strategy['stop_type']
            ama.min_days_after_successful_operation = strategy['min_days_after_successful_operation']
            ama.min_days_after_failure_operation = strategy['min_days_after_failure_operation']
            ama.max_days_per_operation = config.max_days_per_operation

            ama.process_operations()
            ama.calculate_statistics()
            ama.save()

    # Strategy Analysis section
    if config.show_results is True:
        analyzer = StrategyAnalyzer(strategy_id=None)
        analyzer.run()

if __name__ == '__main__':
    run()
