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
        if update_ok:
            features_ok = tm.generate_features()
            # Remove inconsistent tickers from all strategies
            if features_ok is False:
                for index in range(len(config.strategies)):
                    if tm.ticker in list(config.strategies[index]['tickers'].keys()):
                        config.strategies[index]['tickers'].pop(tm.ticker)

    # Strategy section
    for strategy in config.strategies:
        if strategy['name'] == 'Andre Moraes':
            root_strategy = AndreMoraesStrategy(
                strategy['tickers'],
                alias=strategy['alias'],
                comment = strategy['comment'],
                risk_capital_product=strategy['risk_capital_coefficient'],
                total_capital=strategy['capital'],
                min_order_volume=strategy['min_order_volume'],
                partial_sale=strategy['partial_sale'],
                ema_tolerance=strategy['ema_tolerance'],
                min_risk=strategy['min_risk'],
                max_risk=strategy['max_risk'],
                purchase_margin=strategy['purchase_margin'],
                stop_margin=strategy['stop_margin'],
                stop_type=strategy['stop_type'],
                min_days_after_successful_operation=strategy['min_days_after_successful_operation'],
                min_days_after_failure_operation=strategy['min_days_after_failure_operation'],
                gain_loss_ratio=strategy['gain_loss_ratio'],
                max_days_per_operation=strategy['max_days_per_operation'],
                tickers_bag=strategy['tickers_bag'],
                tickers_number=strategy['tickers_number']
            )

            root_strategy.process_operations()
            root_strategy.calculate_statistics()
            root_strategy.save()

        if strategy['name'] == 'Andre Moraes Adapted':
            ml_strategy = AndreMoraesAdaptedStrategy(
                strategy['tickers'],
                alias=strategy['alias'],
                comment = strategy['comment'],
                risk_capital_product=strategy['risk_capital_coefficient'],
                total_capital=strategy['capital'],
                min_order_volume=strategy['min_order_volume'],
                partial_sale=strategy['partial_sale'],
                ema_tolerance=strategy['ema_tolerance'],
                min_risk=strategy['min_risk'],
                max_risk=strategy['max_risk'],
                purchase_margin=strategy['purchase_margin'],
                stop_margin=strategy['stop_margin'],
                stop_type=strategy['stop_type'],
                min_days_after_successful_operation=strategy['min_days_after_successful_operation'],
                min_days_after_failure_operation=strategy['min_days_after_failure_operation'],
                gain_loss_ratio=strategy['gain_loss_ratio'],
                max_days_per_operation=strategy['max_days_per_operation'],
                tickers_bag=strategy['tickers_bag'],
                tickers_number=strategy['tickers_number']
            )

            ml_strategy.process_operations()
            ml_strategy.calculate_statistics()
            ml_strategy.save()

    # Strategy Analysis section
    if config.show_results is True:
        analyzer = StrategyAnalyzer(strategy_id=None)
        analyzer.run()

if __name__ == '__main__':
    run()
