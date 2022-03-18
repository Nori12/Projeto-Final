import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from multiprocessing import Pool
import time
from tqdm import tqdm
import os
import argparse

import constants as c
import config_reader as cr
from ticker_manager import TickerManager
from strategy import AdaptedAndreMoraesStrategy, MLDerivationStrategy, BaselineStrategy

# Configure Logging
logger = logging.getLogger(__name__)
log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME
file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

accepted_strategies = ('Adapted Andre Moraes', 'ML Derivation', 'Baseline')
pbar = None

def update_tickers():
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

    return config.strategies

def run_strategy(strategy, strategy_number, total_strategies, stdout_prints=False):

    if strategy['name'] == 'Adapted Andre Moraes':
        root_strategy = AdaptedAndreMoraesStrategy(
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
            tickers_number=strategy['tickers_number'],
            strategy_number=strategy_number,
            total_strategies=total_strategies,
            stdout_prints=stdout_prints
        )

        root_strategy.process_operations()
        root_strategy.calculate_statistics()
        root_strategy.save()

    elif strategy['name'] == 'ML Derivation':
        ml_strategy = MLDerivationStrategy(
            strategy['tickers'],
            alias=strategy['alias'],
            comment = strategy['comment'],
            risk_capital_product=strategy['risk_capital_coefficient'],
            total_capital=strategy['capital'],
            min_order_volume=strategy['min_order_volume'],
            partial_sale=strategy['partial_sale'],
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
            tickers_number=strategy['tickers_number'],
            strategy_number=strategy_number,
            total_strategies=total_strategies,
            stdout_prints=stdout_prints
        )

        ml_strategy.load_models()
        ml_strategy.process_operations()
        ml_strategy.calculate_statistics()
        ml_strategy.save()

    elif strategy['name'] == 'Baseline':
        baseline_strategy = BaselineStrategy(
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
            tickers_number=strategy['tickers_number'],
            min_operation_decision_coefficient=strategy['min_operation_decision_coefficient'],
            strategy_number=strategy_number,
            total_strategies=total_strategies,
            stdout_prints=stdout_prints
        )

        baseline_strategy.process_operations(days_before_start=90)
        baseline_strategy.calculate_statistics()
        baseline_strategy.save()

if __name__ == '__main__':

    # Parse args
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--pools", type=int,
                    help="number of worker processes to run code in parallel")
    args = parser.parse_args()

    max_pools = os.cpu_count()

    if args.pools is not None:
        max_pools = args.pools

    # Update tickers if candles and features not present in database
    strategies = update_tickers()

    # Filter valid strategy names
    strategies = [strategy for strategy in strategies if strategy['name'] in accepted_strategies]

    total = len(strategies)

    print("Strategies execution started.")
    pbar = tqdm(total=total)
    start = time.perf_counter()

    with Pool(max_pools) as pool:
        for idx, strat in enumerate(strategies):
            pool.apply_async(run_strategy, (strat, idx+1, total), callback=lambda x: pbar.update())

        pool.close()
        pool.join()

    finish = time.perf_counter()
    pbar.close()

    print(f"Finished in {int((finish - start) // 60)}min " \
        f"{int((finish - start) % 60)}s.")
    logger.info(f"Strategies execution finished in {round(finish - start, 0)} second(s).")
