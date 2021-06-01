import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import json
# from contextlib import ContextDecorator
# from time import time
import sys
from datetime import datetime, timedelta
# import numpy as np

import constants as c
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

class ConfigReader:

    def __init__(self, config_file_path=Path(__file__)/c.CONFIG_PATH/c.CONFIG_FILENAME):
        self._config_file_path = config_file_path
        self._tickers = []
        self._initial_dates = []
        self._final_dates = []
        self._read_cfg()
        self._read_individual_tickers()
        self._read_group_tickers()

        logger.info("Ticker, Inital Date, Final Date:")
        for ticker, initial_date, final_date in zip(self._tickers, self._initial_dates, self._final_dates):
            logger.info(f"""\'{ticker}\', \'{initial_date.strftime('%Y-%m-%d')}\', \'{final_date.strftime('%Y-%m-%d')}\'""")

    @property
    def tickers(self):
        return self._tickers

    @property
    def initial_dates(self):
        return self._initial_dates

    @property
    def final_dates(self):
        return self._final_dates

    @property
    def tickers_and_dates(self):
        return self._tickers, self._initial_dates, self._final_dates

    def _read_cfg(self):
        """
        ***NEED TO UPDATE TEXT***
        Read configuration file.

        Read configuration file in the given path and returns it as a dictionary.

        Returns:
            config_json (dict): A dict mapping the corresponding parameters.
        """

        config_path = Path(__file__).parent.parent / c.CONFIG_PATH / c.CONFIG_FILENAME

        logger.debug("Searching for config file in '" + str(config_path) + "'.")

        try:
            with open(config_path, 'r') as cfg_file:
                try:
                    self._config_json = json.load(cfg_file)
                except ValueError:
                    logger.exception('Program aborted. Expected config file in JSON format. Is it corrupted?')
                    sys.exit(c.CONFIG_FILE_ERR)
        except FileNotFoundError:
            logger.exception('Program aborted. Couldn\'t open configuration file: \'' + str(config_path) + '\'.')
            sys.exit(c.CONFIG_FILE_ERR)

        logger.debug('Config file found.')

    def _read_individual_tickers(self):

        if "stock_targets" in self._config_json:
            ticker_names = [item['name'] for item in self._config_json['stock_targets'] if "name" in item.keys()]
            initial_days_str = [item['initial_date'] for item in self._config_json['stock_targets'] if "initial_date" in item.keys()]
            final_days_str = [item['final_date'] if "final_date" in item.keys() else (datetime.now()).strftime('%d/%m/%Y') for item in self._config_json['stock_targets']]

            if not(len(ticker_names) == len(initial_days_str) == len(final_days_str)):
                logging.error('Program aborted. Missing values in parameter "stock_targets" of the config file.')
                sys.exit(c.CONFIG_FILE_ERR)

            if len(ticker_names) != len(set(ticker_names)):
                logging.error("Program aborted. Duplicate tickers not permitted.")
                sys.exit(c.CONFIG_FILE_ERR)

            initial_days = [datetime.strptime(day, '%d/%m/%Y') for day in initial_days_str]
            final_days = [datetime.strptime(day, '%d/%m/%Y') if day != 'today' else (datetime.now()) for day in final_days_str]

            for index, (start, end) in enumerate(zip(initial_days, final_days)):
                if start > end:
                    logging.error('Program aborted. Final date greater than initial date for the stock "'+ticker_names[index]+'".')
                    sys.exit(c.CONFIG_FILE_ERR)

            self._tickers.extend(ticker_names)
            self._initial_dates.extend(initial_days)
            self._final_dates.extend(final_days)

    def _read_group_tickers(self):

        if "group_target" in self._config_json:
            if isinstance(self._config_json['group_target'], list):
                logging.error(f"""Program aborted. Only one \"group_target\" parameter can be set in \'{c.CONFIG_FILENAME}\'.""")
                sys.exit(c.CONFIG_FILE_ERR)

            on_shares_raw = self._config_json['group_target']['on_shares'].lower()
            pn_shares_raw = self._config_json['group_target']['pn_shares'].lower()
            units_raw = self._config_json['group_target']['units'].lower()
            fractional_market_raw = self._config_json['group_target']['fractional_market'].lower()
            sectors_raw = self._config_json['group_target']['sector'].lower()
            subsectors_raw = self._config_json['group_target']['subsector'].lower()
            segments_raw = self._config_json['group_target']['segment'].lower()
            initial_date_raw = self._config_json['group_target']['initial_date'].lower()
            final_date_raw = self._config_json['group_target']['final_date'].lower()

            on_flag = False
            if on_shares_raw == 'true':
                on_flag = True
            elif on_shares_raw == 'false':
                on_flag = False
            else:
                logging.error(f"""Program aborted. Parameter \'on_shares\' in \'{c.CONFIG_FILENAME}\' can only be \"true\" or \"false\".""")
                sys.exit(c.CONFIG_FILE_ERR)


            pn_flag = False
            if pn_shares_raw == 'true':
                pn_flag = True
            elif pn_shares_raw == 'false':
                pn_flag = False
            else:
                logging.error(f"""Program aborted. Parameter \'pn_shares\' in \'{c.CONFIG_FILENAME}\' can only be \"true\" or \"false\".""")
                sys.exit(c.CONFIG_FILE_ERR)

            units_flag = False
            if units_raw == 'true':
                units_flag = True
            elif units_raw == 'false':
                units_flag = False
            else:
                logging.error(f"""Program aborted. Parameter \'units\' in \'{c.CONFIG_FILENAME}\' can only be \"true\" or \"false\".""")
                sys.exit(c.CONFIG_FILE_ERR)

            units_flag = False

            frac_market_flag = None
            if fractional_market_raw == 'true':
                frac_market_flag = True
            elif fractional_market_raw == 'false':
                frac_market_flag = False
            else:
                logging.error(f"""Program aborted. Parameter \'fractional_market\' in \'{c.CONFIG_FILENAME}\' can only be \"true\" or \"false\".""")
                sys.exit(c.CONFIG_FILE_ERR)

            sectors = []
            if sectors_raw != 'none':
                sectors = sectors_raw.split('|')

            subsectors = []
            if subsectors_raw != 'none':
                subsectors = subsectors_raw.split('|')

            segments = []
            if segments_raw != 'none':
                segments = segments_raw.split('|')

            initial_date = datetime.strptime(initial_date_raw, '%d/%m/%Y')

            final_date = None
            if final_date_raw == 'today':
                final_date = datetime.now()
            else:
                final_date = datetime.strptime(final_date_raw, '%d/%m/%Y')

            if initial_date == None or final_date == None:
                logging.error(f"""Program aborted. Parameters \'initial_date\' or \'final_date\' in \'{c.CONFIG_FILENAME}\' can not be \'none\'.""")
                sys.exit(c.CONFIG_FILE_ERR)

            db = DBGeneralModel()
            tickers_raw = db.get_tickers(on_flag, pn_flag, units_flag, fractional_market=frac_market_flag, sectors=sectors, subsectors=subsectors, segments=segments)

            tickers_raw = [ticker[0].strip() for ticker in tickers_raw]

            for ticker in tickers_raw:
                if not ticker in self._tickers:
                    self._tickers.append(ticker)
                    self._initial_dates.append(initial_date)
                    self._final_dates.append(final_date)

