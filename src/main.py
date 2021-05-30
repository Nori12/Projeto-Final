import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import utils
import constants as c
from strategy import AndreMoraesStrategy
from ticker_manager import TickerManager
from db_model import DBGeneralModel

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

    general_info = DBGeneralModel()
    holidays = general_info.get_holidays(min(initial_days), max(final_days))

    # if "all" in ticker_names:
    #     index = ticker_names.index{"all"}
    #     all_initial_day = initial_days(index)
    #     all_final_day = final_days(index)
    #     ticker_names.remove("all")
    #     initial_days.remove("all")
    #     final_days.remove("all")

    #     general_info.

    # Continuar aqui

    for ticker, initial_day, final_day in zip(ticker_names, initial_days, final_days):
        logger.info('Ticker: \''+ticker.ljust(6)+'\'\tInital date: '+initial_day.strftime('%d/%m/%Y')+'\t\tFinal date: '+final_day.strftime('%d/%m/%Y'))

    all_ticker_managers = [TickerManager(ticker_names[i], initial_days[i], final_days[i]) for i in range(len(ticker_names))]

    for ticker_manager in all_ticker_managers:
        ticker_manager._holidays = holidays
        ticker_manager.update_interval()

    # moraes_strat = AndreMoraesStrategy( "André Moraes", ticker_names, initial_days, final_days)

    # moraes_strat.set_input_data(all_ticker_managers[0].get_candles_dataframe(interval='1wk'), interval='1wk')
    # moraes_strat.set_input_data(all_ticker_managers[0].get_candles_dataframe(interval='1d'), interval='1d')
    # moraes_strat.set_input_data(all_ticker_managers[0].get_candles_dataframe(interval='1h'), interval='1h')

    # print(all_ticker_managers[0].get_all_tickers(on_shares=True, pn_shares=True, units=True, fractional_market=True))


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
