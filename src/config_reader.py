import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import json
import sys
from datetime import datetime
# import numpy as np

import constants as c
from db_model import DBGeneralModel
from utils import RunTime

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

    @RunTime('ConfigReader.__init__')
    def __init__(self, config_file_path=Path(__file__)/c.CONFIG_PATH/c.CONFIG_FILENAME):
        self._config_file_path = config_file_path
        self._db_general_model = DBGeneralModel()

        self._strategies = []

        self._load_file()
        self._show_results = self._read_parameter('show_results', origin='root', is_boolean=True, can_be_missed=True, if_missed_default_value=True)
        self._read_strategies()

        self._holidays = self._db_general_model.get_holidays(self.min_start_date, self.max_end_date)

        # Check if program supports requested dates
        # This constraint is given by the most restrict intenval in cdi and holidays tables
        min_cdi_date, max_cdi_date = self._db_general_model.get_cdi_interval()
        min_holiday_date, max_holiday_date = self._db_general_model.get_holidays_interval()

        overall_min_date = max(min_cdi_date, min_holiday_date)
        overall_max_date = min(max_cdi_date, max_holiday_date)

        if self.min_start_date < overall_min_date:
            logger.error(f"""Ticker has start date less than the minimum available ({overall_min_date.strftime('%d/%m/%Y')}\'). Please check holidays and CDI data in database.""")
            sys.exit(c.CONFIG_FILE_ERR)

        if self.max_end_date > overall_max_date:
            logger.error(f"""Ticker has end date greater than the maximum available ({overall_max_date.strftime('%d/%m/%Y')}\'). Please check holidays and CDI data in database.""")
            sys.exit(c.CONFIG_FILE_ERR)

        # Logging
        logger.info(f"""Config file successfully read.""")
        logger.info(f"""Total strategies: {len(self._strategies)}""")
        logger.info(f"""Total tickers: {len(self.tickers_and_dates)}""")
        logger.info(f"""Oldest date: {self.min_start_date.strftime('%d/%m/%Y')}""")
        logger.info(f"""Most recent date: {self.max_end_date.strftime('%d/%m/%Y')}""")

        logger.info(f"""Strategies found:""")
        for strategy in self._strategies:
            logger.info(f"""  Name: \'{strategy['name']}\', Capital: {strategy['capital']}, Risk-Capital Coefficient: {strategy['risk_capital_coefficient']}, Ticker Min Ann Volume Filter: {strategy['ticker_min_ann_volume_filter']}, Min Order Volume: {strategy['min_order_volume']}, Tickers: {str(list(strategy['tickers'].keys()))}""")

    @property
    def strategies(self):
        return self._strategies

    @property
    def tickers_and_dates(self):

        tickers = {}

        for strategy in self._strategies:
            current_tickers = strategy['tickers']

            for ticker, dates in current_tickers.items():
                if ticker not in tickers:
                    tickers[ticker] = dates
                else:
                    tickers[ticker]['start_date'] = min(tickers[ticker]['start_date'], dates['start_date'])
                    tickers[ticker]['end_date'] = max(tickers[ticker]['end_date'], dates['end_date'])

        return tickers

    @property
    def min_start_date(self):

        minimum_date = datetime.max.date()

        for strategy in self._strategies:
            current_tickers = strategy['tickers']

            for _, dates in current_tickers.items():
                if dates['start_date'] < minimum_date:
                    minimum_date = dates['start_date']

        return minimum_date

    @property
    def max_end_date(self):

        maximum_date = datetime.min.date()

        for strategy in self._strategies:
            current_tickers = strategy['tickers']

            for _, dates in current_tickers.items():
                if dates['end_date'] > maximum_date:
                    maximum_date = dates['end_date']

        return maximum_date

    @property
    def show_results(self):
        return self._show_results

    @property
    def holidays(self):
        return self._holidays

    def _load_file(self):
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
                    logger.exception('Expected config file in JSON format. Is it corrupted?')
                    sys.exit(c.CONFIG_FILE_ERR)
        except FileNotFoundError:
            logger.exception('Could not open configuration file: \'' + str(config_path) + '\'.')
            sys.exit(c.CONFIG_FILE_ERR)

        logger.debug('Config file found.')

    def _read_parameter(self, parameter_name, origin='root', is_boolean=False, is_date=False, can_be_list=False, can_be_missed=False, can_be_none=False, if_missed_default_value=None, accept_today=False):

        if is_boolean == True and is_date == True:
            logger.error(f"""Parameter can not be boolean and date at the same time.""")
            sys.exit(c.CONFIG_FILE_ERR)

        parameter = None

        if origin == 'root':
            origin = self._config_json

        if parameter_name in origin:

            parameter_raw = origin[parameter_name]

            if isinstance(parameter_raw, list):

                if can_be_list == True:

                    parameter = []
                    for param_element in parameter_raw:
                        parameter.append(self._get_value(parameter_name, param_element, is_boolean, is_date, can_be_none, accept_today))

                else:
                    logger.error(f"""Parameter \'{parameter_name}\' can not be of type LIST.""")
                    sys.exit(c.CONFIG_FILE_ERR)

            # elif isinstance(parameter_raw, str) and is_date == False:
            #     parameter_raw_split = parameter_raw.split("|")
            #     if len(parameter_raw_split) > 1:
            #         if can_be_list == True:
            #             parameter = [value.strip() for value in parameter_raw_split if value.strip() != '']
            #         else:
            #             logger.error(f"""Parameter \'{parameter_name}\' can not be of type LIST. Character \'|\' is interpreted as a delimeter.""")
            #             sys.exit(c.CONFIG_FILE_ERR)
            #     else:
            #         if parameter_raw.strip() == "" and can_be_none == False:
            #             logger.error(f"""Parameter \'{parameter_name}\' can not be EMPTY or NULL.""")
            #             sys.exit(c.CONFIG_FILE_ERR)
            #         elif parameter_raw.lower() not in c.ACCEPTABLE_NONE_VALUES:
            #             # NOT IN because variable default value is already None
            #             parameter = parameter_raw

            else:
                parameter = self._get_value(parameter_name, parameter_raw, is_boolean, is_date, can_be_none, accept_today)

        elif can_be_missed == True and if_missed_default_value is not None:
            parameter = if_missed_default_value

        elif can_be_missed == False:
            logger.error(f"""Could not find parameter: \'{parameter_name}\'.""")
            sys.exit(c.CONFIG_FILE_ERR)

        return parameter

    def _get_value(self, parameter_name, input, is_boolean=False, is_date=False, can_be_none=False, accept_today=False):

        parameter = None

        if is_boolean == True:
            if input in c.ACCEPTABLE_TRUE_VALUES:
                parameter = True
            elif input in c.ACCEPTABLE_FALSE_VALUES:
                parameter = False

            if parameter == None:
                logger.error(f"""Parameter \'{parameter_name}\' has type of BOOLEAN and its value could not be identified.""")
                sys.exit(c.CONFIG_FILE_ERR)

        elif is_date == True:
            if accept_today == True and input.lower() == "today":
                parameter = datetime.now().date()
            else:
                try:
                    parameter = datetime.strptime(input, '%d/%m/%Y').date()
                except Exception:
                    logger.exception(f"""Parameter \'{parameter_name}\' has no valid convertion to date object.""")
                    sys.exit(c.CONFIG_FILE_ERR)
            if parameter == None:
                logger.error(f"""Parameter \'{parameter_name}\' has type of DATE (\'dd/mm/yyyy\') and its value could not be identified.""")
                sys.exit(c.CONFIG_FILE_ERR)

        elif isinstance(input, str):
            if input.lower() not in c.ACCEPTABLE_NONE_VALUES:
                parameter = input

        else:
            parameter = input

        if can_be_none == False and parameter is None:
            logger.error(f"""Parameter \'{parameter_name}\' does not accept NULL values.""")
            sys.exit(c.CONFIG_FILE_ERR)

        return parameter

    def _read_strategies(self):

        if "strategies" in self._config_json:

            for strategy_batch in self._config_json['strategies']:

                # This single strategy element can contain multiple strategies within
                # This is done through a list parameters

                strategies = []

                name = self._read_parameter('name', strategy_batch)
                alias = self._read_parameter('alias', strategy_batch)
                comment = self._read_parameter('comment', strategy_batch, can_be_missed=True)
                capital = self._read_parameter('capital', strategy_batch, can_be_list=True)
                risk_capital_coefficient = self._read_parameter('risk_capital_coefficient', strategy_batch, can_be_list=True)
                min_order_volume = self._read_parameter('min_order_volume', strategy_batch, can_be_list=True, can_be_missed=True, if_missed_default_value=1)
                ticker_min_ann_volume_filter = self._read_parameter('ticker_min_ann_volume_filter', strategy_batch, can_be_list=True, can_be_missed=True, if_missed_default_value=0)

                self._add_parameter_to_strategies('name', name, strategies)
                self._add_parameter_to_strategies('alias', alias, strategies)
                self._add_parameter_to_strategies('comment', comment, strategies)
                self._add_parameter_to_strategies('capital', capital, strategies)
                self._add_parameter_to_strategies('risk_capital_coefficient', risk_capital_coefficient, strategies)
                self._add_parameter_to_strategies('min_order_volume', min_order_volume, strategies)
                self._add_parameter_to_strategies('ticker_min_ann_volume_filter', ticker_min_ann_volume_filter, strategies)

                tickers, start_dates, end_dates = self._read_individual_tickers('stock_targets', origin=strategy_batch)

                for ticker, start_date, end_date in zip(tickers, start_dates, end_dates):
                    self._add_ticker_to_strategies('tickers', ticker, start_date, end_date, strategies, overwrite_ticker=True)

                tickers, start_dates, end_dates = self._read_tickers_group('group_target', output_parameter_name='tickers', origin=strategy_batch, strategies=strategies, overwrite_tickers=False)

                for ticker, start_date, end_date in zip(tickers, start_dates, end_dates):
                    self._add_ticker_to_strategies('tickers', ticker, start_date, end_date, strategies)

                for strategy in strategies:
                    if (('tickers' in strategy and not strategy['tickers']) or ('tickers' not in strategy)):
                        logger.error(f"""Any strategy must have at least one ticker.""")
                        sys.exit(c.NO_TICKER_FOR_STRATEGY_ERR)

                self._update_text(strategies)

                self._strategies.extend(strategies)

    def _add_parameter_to_strategies(self, parameter_name, parameter, strategies):

        # Then add to strategies, but...
        # strategies can have 0, 1 or more dict elements inside.
        # Also, parameter can be a single value or a list
        # So, care must be taken when merging parameter into strategies
        if isinstance(parameter, list):

            # If multiple strategies are implicitly being configured, they must agree in size
            if len(strategies) > 1 and len(parameter) != len(strategies):
                logger.error(f"""Implicit strategy executions does not match size.""")
                sys.exit(c.CONFIG_FILE_ERR)

            if len(strategies) == 0:
                for index, value in enumerate(parameter):
                    strategies.append({parameter_name: value})

            else:
                if len(strategies) == 1:
                    template_strategy = strategies[0].copy()
                    for _ in range(len(parameter) - 1):
                        strategies.append(template_strategy)
                # Finally len(strategies) == len(parameter)
                for index, value in enumerate(parameter):
                    strategies[index][parameter_name] = value

        else:
            if len(strategies) == 0:
                strategies.append({parameter_name: parameter})
            else:
                for index in range(len(strategies)):
                    strategies[index][parameter_name] = parameter

    def _read_individual_tickers(self, input_parameter_name, origin):

        ticker_names = []
        initial_dates = []
        final_dates = []

        if input_parameter_name in origin:

            if len(origin[input_parameter_name]) > 0:

                # Read variables
                ticker_names = [item['name'] for item in origin[input_parameter_name] if "name" in item.keys()]
                initial_dates_str = [item['start_date'] for item in origin[input_parameter_name] if 'start_date' in item.keys()]
                final_dates_str = [item['end_date'] if 'end_date' in item.keys() else (datetime.now()).strftime('%d/%m/%Y') for item in origin[input_parameter_name]]

                # Some errors handling
                if not(len(ticker_names) == len(initial_dates_str) == len(final_dates_str)):
                    logger.error(f"""Missing values in parameter {input_parameter_name} of the config file.""")
                    sys.exit(c.CONFIG_FILE_ERR)

                if len(ticker_names) != len(set(ticker_names)):
                    logger.error(f"""Duplicate tickers \'{input_parameter_name}\' in not permitted.""")
                    sys.exit(c.CONFIG_FILE_ERR)

                initial_dates = [datetime.strptime(day, '%d/%m/%Y').date() for day in initial_dates_str]
                final_dates = [datetime.strptime(day, '%d/%m/%Y').date() if day != 'today' else datetime.now().date() for day in final_dates_str]

                for index, (start, end) in enumerate(zip(initial_dates, final_dates)):
                    if start >= end:
                        logger.error('Final date greater than initial date for the stock "'+ticker_names[index]+'".')
                        sys.exit(c.CONFIG_FILE_ERR)

        return ticker_names, initial_dates, final_dates

    def _read_tickers_group(self, input_parameter_name, output_parameter_name, origin, strategies, overwrite_tickers=True):

        ticker_names = []
        initial_dates = []
        final_dates = []

        if input_parameter_name in origin:
            if isinstance(origin[input_parameter_name], list):
                logger.error(f"""Only one \'{input_parameter_name}\' parameter per strategy is allowed.""")
                sys.exit(c.CONFIG_FILE_ERR)

            group_object = origin[input_parameter_name]

            on_shares = self._read_parameter('on_shares', origin=group_object, is_boolean=True)
            pn_shares = self._read_parameter('pn_shares', origin=group_object, is_boolean=True)
            units = self._read_parameter('units', origin=group_object, is_boolean=True, can_be_missed=False)
            fractional_market = self._read_parameter('fractional_market', origin=group_object, is_boolean=True, can_be_missed=True, if_missed_default_value=False)
            sectors = self._read_parameter('sector', origin=group_object, can_be_missed=True, can_be_list=True, can_be_none=True, if_missed_default_value=None)
            subsectors = self._read_parameter('subsector', origin=group_object, can_be_missed=True, can_be_list=True, can_be_none=True, if_missed_default_value=None)
            segments = self._read_parameter('segment', origin=group_object, can_be_missed=True, can_be_list=True, can_be_none=True, if_missed_default_value=None)
            initial_date = self._read_parameter('start_date', origin=group_object, is_date=True, accept_today=False)
            final_date = self._read_parameter('end_date', origin=group_object, is_date=True, accept_today=True)

            if initial_date >= final_date:
                logger.error(f"""\'{input_parameter_name}\' has start date greater than end date.""")
                sys.exit(c.CONFIG_FILE_ERR)

            tickers_raw = self._db_general_model.get_tickers(on_shares, pn_shares, units, fractional_market=fractional_market, sectors=sectors, subsectors=subsectors, segments=segments)

            if tickers_raw.empty:
                logger.error(f"""\'{input_parameter_name}\' has no tickers in database.""")
                sys.exit(c.CONFIG_FILE_ERR)

            ticker_names = tickers_raw['ticker'].to_list()
            initial_dates = [initial_date] * tickers_raw.size
            final_dates = [final_date] * tickers_raw.size

        return ticker_names, initial_dates, final_dates

    def _add_ticker_to_strategies(self, ticker_parameter_name, ticker, start_date, end_date, strategies, overwrite_ticker=False):

        for index in range(len(strategies)):

            if ticker_parameter_name not in strategies[index]:
                strategies[index][ticker_parameter_name] = {}

            if ticker in strategies[index][ticker_parameter_name] and overwrite_ticker == True:
                strategies[index][ticker_parameter_name][ticker]['start_date'] = start_date
                strategies[index][ticker_parameter_name][ticker]['end_date'] = end_date

            elif ticker not in strategies[index][ticker_parameter_name]:
                strategies[index][ticker_parameter_name][ticker] = {'start_date': start_date, 'end_date': end_date}

    def _update_text(self, strategies):

        for strategy in strategies:

            strategy['alias'].replace('{capital}', str(strategy['capital']))
            strategy['comment'].replace('{capital}', str(strategy['capital']))

            strategy['alias'].replace('{number_of_tickers}', str(len(strategy['tickers'])))
            strategy['comment'].replace('{number_of_tickers}', str(len(strategy['tickers'])))

            strategy['alias'].replace('{risk_capital_coefficient}', str(strategy['risk_capital_coefficient']))
            strategy['comment'].replace('{risk_capital_coefficient}', str(strategy['risk_capital_coefficient']))

            strategy['alias'].replace('{min_order_volume}', str(strategy['min_order_volume']))
            strategy['comment'].replace('{min_order_volume}', str(strategy['min_order_volume']))

            strategy['alias'].replace('{ticker_min_ann_volume_filter}', str(strategy['ticker_min_ann_volume_filter']))
            strategy['comment'].replace('{ticker_min_ann_volume_filter}', str(strategy['ticker_min_ann_volume_filter']))

if __name__ == "__main__":
    ConfigReader()