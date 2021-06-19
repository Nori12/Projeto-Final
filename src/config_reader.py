import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import json
import sys
from datetime import datetime, timedelta

import constants as c
from db_model import DBGeneralModel
from utils import RunTime

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE,
    backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

class ConfigReader:
    """
    Configuration file interpetrer.

    Reads and validadates all parameters inside config file (JSON).

    Notes
    ----------
    A single strategy block in the file can contain MULTIPLE IMPLICIT
    STRATEGIES given by lists of parameters. But size must be consintent.
    This is done by parameter values as list.

    Ticker dates are currently limited by the interval intersection
    between holidays and CDI data.

    Args
    ----------
    config_file_path : `Path`, optional
        Configuration file path.

    Attributes
    ----------
    _strategies : list of `strategy`
        Result parameter that indicated all strategies found in file.
    _show_results : bool
        Result parameter that enables further visualization of
        processed data.
    _db_general_model : `DBGeneralModel`
        Database connector to get generic information (e.g.,
        holidays, CDI).
    _holidays : list of `datetime`
        Holidays
    """

    @RunTime('ConfigReader.__init__')
    def __init__(self,
        config_file_path=Path(__file__).parent.parent/c.CONFIG_PATH/c.CONFIG_FILENAME):
        self._db_general_model = DBGeneralModel()

        self._strategies = []

        self.load_file(config_file_path)
        self._show_results = self.read_parameter('show_results', origin='root',
            is_boolean=True, can_be_missed=True, if_missed_default_value=True)
        self._read_strategies()

        holidays = self._db_general_model.get_holidays(
            self.min_start_date, self.max_end_date)['day'].to_list()
        self._holidays = [holiday.to_pydatetime().date() for holiday in holidays]

        # Check if program supports requested dates
        # This constraint is given by the most restrict date interval in cdi and
        # holidays tables
        min_cdi_date, max_cdi_date = self._db_general_model.get_cdi_interval()
        min_holiday_date, max_holiday_date = self._db_general_model.\
            get_holidays_interval()

        overall_min_date = max(min_cdi_date, min_holiday_date)
        overall_max_date = min(max_cdi_date, max_holiday_date)

        if self.min_start_date < overall_min_date:
            logger.error(f"Ticker has start date less than the minimum "\
                f"available ({overall_min_date.strftime('%d/%m/%Y')}). "\
                f"Please check holidays and CDI data in database.")
            sys.exit(c.CONFIG_FILE_ERR)

        if self.max_end_date > overall_max_date:
            logger.error(f"Ticker has end date greater than the maximum "\
                f"available ({overall_max_date.strftime('%d/%m/%Y')}). "\
                f"Please check holidays and CDI data in database.")
            sys.exit(c.CONFIG_FILE_ERR)

        # Logging
        logger.info(f"Config file successfully read.")
        logger.info(f"Total strategies: {len(self._strategies)}")
        logger.info(f"Total tickers: {len(self.tickers_and_dates)}")
        logger.info(f"Oldest date: {self.min_start_date.strftime('%d/%m/%Y')}")
        logger.info(f"Most recent date: {self.max_end_date.strftime('%d/%m/%Y')}")

        logger.info(f"Strategies found:")
        for strategy in self.strategies:
            logger.info(f"  Name: \'{strategy['name']}\', Capital: "\
                f"{strategy['capital']}, Risk-Capital Coefficient: "\
                f"{strategy['risk_capital_coefficient']}, Ticker Min Ann "\
                f"Volume Filter: {strategy['ticker_min_ann_volume_filter']}, "\
                f"Min Order Volume: {strategy['min_order_volume']}, Tickers: "\
                f"{str(list(strategy['tickers'].keys()))}")

    @property
    def strategies(self):
        """
        `list` of `dict` : All strategies interpreted from config file.
            Structure of dict is similar to config file.
        """
        return self._strategies

    @property
    def tickers_and_dates(self):
        """
        `dict` : All strategies interpreted from config file.
            Format: {'ABCD1': {
                        'start_date': `datetime.date`,
                        'end_date': `datetime.date`}
                    , ... }
        """
        tickers = {}

        for strategy in self.strategies:
            current_tickers = strategy['tickers']

            for ticker, dates in current_tickers.items():
                if ticker not in tickers:
                    tickers[ticker] = dates
                else:
                    tickers[ticker]['start_date'] = \
                        min(tickers[ticker]['start_date'], dates['start_date'])
                    tickers[ticker]['end_date'] = \
                        max(tickers[ticker]['end_date'], dates['end_date'])

        return tickers

    @property
    def min_start_date(self):
        """`datetime.date` : Oldest start date between all tickers and strategies."""
        minimum_date = datetime.max.date()

        for strategy in self.strategies:
            current_tickers = strategy['tickers']

            for _, dates in current_tickers.items():
                if dates['start_date'] < minimum_date:
                    minimum_date = dates['start_date']

        return minimum_date

    @property
    def max_end_date(self):
        """`datetime.date` : Most recent end date between all tickers and strategies."""
        maximum_date = datetime.min.date()

        for strategy in self.strategies:
            current_tickers = strategy['tickers']

            for _, dates in current_tickers.items():
                if dates['end_date'] > maximum_date:
                    maximum_date = dates['end_date']

        return maximum_date

    @property
    def show_results(self):
        """bool : Indicates if show result dashboard by the end of the program."""
        return self._show_results

    @property
    def holidays(self):
        """list of `datetime.date` : Holidays between min_start_date and max_end_date."""
        return self._holidays

    def load_file(self, config_file_path):
        """Read configuration (JSON) file in the given path and save it as dictionary."""

        logger.debug(f"Searching for config file in path \'{str(config_file_path)}\'.")

        try:
            with open(config_file_path, 'r') as cfg_file:
                try:
                    self.config_json = json.load(cfg_file)
                except ValueError:
                    logger.exception(f"Expected config file in JSON format. "\
                        f"Is it corrupted?")
                    sys.exit(c.CONFIG_FILE_ERR)
        except FileNotFoundError:
            logger.exception(f"Could not open configuration file: "\
                f"\'{str(config_file_path)}\'.")
            sys.exit(c.CONFIG_FILE_ERR)

        logger.debug('Config file found.')

    def read_parameter(self, param_name, origin='root', is_boolean=False,
        is_date=False, can_be_list=False, can_be_missed=False, can_be_none=False,
        if_missed_default_value=None, accept_today=False):
        """
        Search, validate and return the given parameter.

        Generical method (swiss army knife) for searching, validating and
        returning a parameter in a given source (dict).

        Args
        ----------
        param_name : str
            Parameter name (key).
        origin : `dict`, default 'root' (i.e., saved config file)
            Where to search.
        is_boolean : bool, default False
            Flag if expected parameter value is of type bool.
            Do not work simultaneously with is_date.
        is_date : bool, default False
            Flag if expected parameter value is of type `datetime`.
            Do not work simultaneously with is_boolean.
        can_be_list : bool, default False
            Flag if expected parameter value is of type bool.
        can_be_missed : bool, default False
            Flag if expected parameter value can not be found.
        can_be_none : bool, default False
            Flag if expected parameter value can be none.
        if_missed_default_value : any, optional
            Parameter replace value in case is not found.
            Only works if can_be_missed=True.
        accept_today : bool, default False
            Indication to replace string 'today' for current date.
            Only works if is_date=True.

        Returns
        ----------
        any
            Parameter value.
        """
        if is_boolean == True and is_date == True:
            logger.error(f"Parameter can not be boolean and date at the same time.")
            sys.exit(c.CONFIG_FILE_ERR)

        parameter = None

        if origin == 'root':
            origin = self.config_json

        if param_name in origin:

            parameter_raw = origin[param_name]
            if isinstance(parameter_raw, list):

                if can_be_list == True:

                    parameter = []
                    for param_element in parameter_raw:
                        parameter.append(ConfigReader.get_value(param_name,
                            param_element, is_boolean, is_date, can_be_none,
                            accept_today))
                else:
                    logger.error(f"Parameter \'{param_name}\' can not "\
                        f"be of type LIST.")
                    sys.exit(c.CONFIG_FILE_ERR)
            else:
                parameter = ConfigReader.get_value(param_name, parameter_raw,
                is_boolean, is_date, can_be_none, accept_today)

        elif can_be_missed == True and if_missed_default_value is not None:
            parameter = if_missed_default_value

        elif can_be_missed == False:
            logger.error(f"Could not find parameter: \'{param_name}\'.")
            sys.exit(c.CONFIG_FILE_ERR)

        return parameter

    @staticmethod
    def get_value(param_name, origin, is_boolean=False, is_date=False,
        can_be_none=False, accept_today=False):
        """
        Validate and return the given parameter.

        Auxiliary method. More granular.

        Parameters
        ----------
        param_name : str
            Parameter name (key).
        origin : `dict`
            Where to search.
        is_boolean : bool, default False
            Flag if expected parameter value is of type bool.
            Do not work simultaneously with is_date.
        is_date : bool, default False
            Flag if expected parameter value is of type `datetime`.
            Do not work simultaneously with is_boolean.
        can_be_none : bool, default False
            Flag if expected parameter value can be none.
        accept_today : bool, default False
            Indication to replace string 'today' for current date.
            Only works if is_date=True.

        Returns
        ----------
        any
            Parameter value.
        """
        parameter = None

        if is_boolean == True:
            if origin in c.ACCEPTABLE_TRUE_VALUES:
                parameter = True
            elif origin in c.ACCEPTABLE_FALSE_VALUES:
                parameter = False

            if parameter == None:
                logger.error(f"Parameter \'{param_name}\' has type of "
                    f"BOOLEAN and its value could not be identified.")
                sys.exit(c.CONFIG_FILE_ERR)

        elif is_date == True:
            if accept_today == True and origin.lower() == "today":
                parameter = datetime.now().date()
            else:
                try:
                    parameter = datetime.strptime(origin, '%d/%m/%Y').date()
                except Exception:
                    logger.exception(f"Parameter \'{param_name}\' has no "
                        f"valid convertion to date object.")
                    sys.exit(c.CONFIG_FILE_ERR)
            if parameter == None:
                logger.error(f"Parameter \'{param_name}\' has type of DATE "
                    f"(\'dd/mm/yyyy\') and its value could not be identified.")
                sys.exit(c.CONFIG_FILE_ERR)

        elif isinstance(origin, str):
            if origin.lower() not in c.ACCEPTABLE_NONE_VALUES:
                parameter = origin.strip()
            # No need for else because it is already None
        else:
            parameter = origin

        if can_be_none == False and parameter is None:
            logger.error(f"Parameter \'{param_name}\' does not accept NULL values.")
            sys.exit(c.CONFIG_FILE_ERR)

        return parameter

    def _read_strategies(self):
        """
        Parse all strategies present in config file.

        Store strategies in attribute _strategies.

        A single strategy block in the file can contain MULTIPLE IMPLICIT
        STRATEGIES given by lists of parameters. But size must be consintent.
        This is done by parameter values as list.

        Strategy parameters `alias` and `comment` can have parts of its text
        replaced by the parameter value itself.

        Example : "alias" = "Tickers: {number_of_tickers}, Capital: {capital}"
            Text "{number_of_tickers}" and "{capital}" will be reaplced by the
            corresponding values.
        """
        if "strategies" in self.config_json:

            for strategy_batch in self.config_json['strategies']:

                strategies = []

                name = self.read_parameter('name', strategy_batch)
                alias = self.read_parameter('alias', strategy_batch)
                comment = self.read_parameter('comment', strategy_batch,
                    can_be_missed=True)
                capital = self.read_parameter('capital', strategy_batch,
                    can_be_list=True)
                rc_coef = self.read_parameter(
                    'risk_capital_coefficient', strategy_batch, can_be_list=True)
                min_order_volume = self.read_parameter('min_order_volume',
                    strategy_batch, can_be_list=True, can_be_missed=True,
                    if_missed_default_value=1)
                volume_filter = self.read_parameter(
                    'ticker_min_ann_volume_filter', strategy_batch, can_be_list=True,
                    can_be_missed=True, if_missed_default_value=0)

                ConfigReader.add_param_to_strategies('name', name, strategies)
                ConfigReader.add_param_to_strategies('alias', alias, strategies)
                ConfigReader.add_param_to_strategies('comment', comment, strategies)
                ConfigReader.add_param_to_strategies('capital', capital, strategies)
                ConfigReader.add_param_to_strategies('risk_capital_coefficient',
                    rc_coef, strategies)
                ConfigReader.add_param_to_strategies('min_order_volume', min_order_volume,
                    strategies)
                ConfigReader.add_param_to_strategies('ticker_min_ann_volume_filter',
                    volume_filter, strategies)

                individual_tickers = ConfigReader.read_individual_tickers('stock_targets',
                    origin=strategy_batch)

                ConfigReader.add_param_to_strategies('tickers', individual_tickers, strategies,
                    is_ticker=True, overwrite_ticker=True)

                group_tickers = self._read_tickers_group('group_target', origin=strategy_batch)

                ConfigReader.add_param_to_strategies('tickers', group_tickers, strategies, is_ticker=True)

                for strategy in strategies:
                    if (('tickers' in strategy and not strategy['tickers']) or
                        ('tickers' not in strategy)):
                        logger.error(f"Any strategy must have at least one ticker.")
                        sys.exit(c.NO_TICKER_FOR_STRATEGY_ERR)

                ConfigReader.replace_text('alias', strategies)
                ConfigReader.replace_text('comment', strategies)

                ConfigReader.subtract_last_end_date(strategies)

                self.strategies.extend(strategies)

    @staticmethod
    def add_param_to_strategies(param_name, param, strategies,
        is_ticker=False, overwrite_ticker=False):
        """
        Add parameter to given list of strategies.

        If `param` is not a list, then add it to all `strategies`.
        If `param` is a list, then add its elements to the corresponding
        elements of `strategies`.

        If the param_name does not exist in a strategy, it is created.

        Attention
        ----------
        If `strategies` is a list and has a length greater than one and if param is
        also a list, both lengths must be the same, otherwise an error occur.

        Args
        ----------
        param_name : str
            Parameter name (key) that will be created in `strategies`.
        param : any
            Parameter value. Can be a `list`.
        strategies : `list`
            List of strategies where to add.
        is_ticker : bool, default False
            Flag if expected parameter value is a dict of tickers (or a list of dict).
            This is necessary because multiple tickers are attached to a single
            param_name
        overwrite_ticker : bool, default False
            Flag if can oberwrite existing ticker dates. Only works if is_ticker=True.
        """
        if isinstance(param, list):

            # Check if dimension of strategies and param match.
            if len(strategies) > 1 and len(param) != len(strategies):
                logger.error(f"Implicit strategy executions do not match size.")
                sys.exit(c.CONFIG_FILE_ERR)

            # Create param_name if does not exist.
            if len(strategies) == 0:
                for index, value in enumerate(param):
                    strategies.append({param_name: value})

            else:
                # Clone strategies
                if len(strategies) == 1:
                    template_strategy = strategies[0].copy()
                    for _ in range(len(param) - 1):
                        strategies.append(template_strategy)

                # Finally len(strategies) == len(parameter)
                for index, value in enumerate(param):
                    if is_ticker == False:
                        strategies[index][param_name] = value
                    else:
                        if param_name not in strategies[index]:
                            strategies[index][param_name] = {}

                        for tck_name in list(param[index].keys()):
                            if tck_name in strategies[index][param_name] \
                                and overwrite_ticker == True:
                                strategies[index][param_name][tck_name]['start_date'] = \
                                    param[index][tck_name]['start_date']
                                strategies[index][param_name][tck_name]['end_date'] = \
                                    param[index][tck_name]['end_date']
                            elif tck_name not in strategies[index][param_name]:
                                strategies[index][param_name][tck_name] = \
                                    param[index][tck_name]
        else:
            if len(strategies) == 0:
                if not (is_ticker == True and param is None):
                    strategies.append({param_name: param})
            else:
                if not (is_ticker == True and param is None):
                    for index in range(len(strategies)):
                        strategies[index][param_name] = param

    @staticmethod
    def read_individual_tickers(param_name, origin):
        """
        Read, validate and return tickers.

        Handle multiple implicit strategies.

        Parameters
        ----------
        param_name : str
            Parameter name where tickers can be found.
        origin : `dict`
            Where to search.

        Returns
        ----------
        `dict` or `list` of `dict`
            All tickers found. If multiple strategies are implicit, return a type of list.
            Format: [{'ABCD1': {
                        'start_date': `datetime.date`,
                        'end_date': `datetime.date`}
                    , ... }, ... ]
        """
        tickers = None
        if param_name in origin:

            # Read variables
            ticker_names = [item['name'] for item in origin[param_name]
                if "name" in item.keys()]
            start_dates_raw = [item['start_date'] for item in origin[param_name]
                if 'start_date' in item.keys()]
            end_dates_raw = [item['end_date'] for item in origin[param_name]
                if 'end_date' in item.keys()]

            if not len(start_dates_raw) == len(end_dates_raw) == len(ticker_names):
                logger.error(f"Inconsistency on parameter \'stock_targets\'.")
                sys.exit(c.CONFIG_FILE_ERR)

            # Get length of implicit strategies while checking for inconsistencies
            strategies_len = 1
            for index in range(len(start_dates_raw)):

                if isinstance(start_dates_raw[index], list):
                    # Multiple implicit strategies must agree in number
                    if strategies_len > 1 and len(start_dates_raw[index]) != strategies_len:
                        logger.error(f"Implicit strategy executions do not match size.")
                        sys.exit(c.CONFIG_FILE_ERR)
                    if len(start_dates_raw[index]) == 1:
                        start_dates_raw[index] = start_dates_raw[index][0]
                    else:
                        strategies_len = max(strategies_len, len(start_dates_raw[index]))

                if isinstance(end_dates_raw[index], list):
                    # Multiple implicit strategies must agree in number
                    if strategies_len > 1 and len(end_dates_raw[index]) != strategies_len:
                        logger.error(f"Implicit strategy executions do not match size.")
                        sys.exit(c.CONFIG_FILE_ERR)
                    if len(end_dates_raw[index]) == 1:
                        end_dates_raw[index] = end_dates_raw[index][0]
                    else:
                        strategies_len = max(strategies_len, len(end_dates_raw[index]))

            # Process the output
            tickers = []
            for strat_index in range(strategies_len):

                tickers_wrapper = {}
                for tck_index, tck_name in enumerate(ticker_names):

                    if isinstance(start_dates_raw[tck_index], list):
                        start_date_value = datetime.strptime(
                            start_dates_raw[tck_index][strat_index], '%d/%m/%Y').date()
                    else:
                        start_date_value = datetime.strptime(
                            start_dates_raw[tck_index], '%d/%m/%Y').date()

                    if isinstance(end_dates_raw[tck_index], list):
                        if end_dates_raw[tck_index][strat_index] == 'today':
                            end_date_value = datetime.now().date()
                        else:
                            end_date_value = datetime.strptime(
                                end_dates_raw[tck_index][strat_index], '%d/%m/%Y').date()
                    else:
                        if end_dates_raw[tck_index] == 'today':
                            end_date_value = datetime.now().date()
                        else:
                            end_date_value = datetime.strptime(
                                end_dates_raw[tck_index], '%d/%m/%Y').date()

                    tickers_wrapper[tck_name] = {
                        'start_date': start_date_value,
                        'end_date': end_date_value
                    }

                tickers.append(tickers_wrapper)

            if len(tickers) == 1:
                tickers = tickers[0]

        return tickers

    def _read_tickers_group(self, param_name, origin):
        """
        Parse request of group of tickers and return them.

        Handle multiple implicit strategies.

        Parameters
        ----------
        param_name : str
            Parameter name where tickers can be found.
        origin : `dict`
            Where to search.

        Returns
        ----------
        `dict` or `list` of `dict`
            All tickers found.
            If multiple strategies are implicit, return a type of list.
            Format: [{'ABCD1': {
                        'start_date': `datetime.date`,
                        'end_date': `datetime.date`}
                    , ... }, ... ]
        """
        tickers = None
        if param_name in origin:
            if isinstance(origin[param_name], list):
                logger.error(f"Only one \'{param_name}\' parameter "
                    f"per strategy is allowed.")
                sys.exit(c.CONFIG_FILE_ERR)

            group_object = origin[param_name]

            group_params = dict(
                on_shares = self.read_parameter('on_shares', origin=group_object,
                    is_boolean=True),
                pn_shares = self.read_parameter('pn_shares', origin=group_object,
                    is_boolean=True),
                units = self.read_parameter('units', origin=group_object, is_boolean=True,
                    can_be_missed=False),
                fractional_market = self.read_parameter('fractional_market', origin=group_object,
                    is_boolean=True, can_be_missed=True, if_missed_default_value=False),
                sectors = self.read_parameter('sector', origin=group_object, can_be_missed=True,
                    can_be_list=True, can_be_none=True, if_missed_default_value=None),
                subsectors = self.read_parameter('subsector', origin=group_object,
                    can_be_missed=True, can_be_list=True, can_be_none=True, if_missed_default_value=None),
                segments = self.read_parameter('segment', origin=group_object, can_be_missed=True,
                    can_be_list=True, can_be_none=True, if_missed_default_value=None),
                start_date = self.read_parameter('start_date', origin=group_object,
                    is_date=True, accept_today=False),
                end_date = self.read_parameter('end_date', origin=group_object,
                    is_date=True, accept_today=True)
            )

            if group_params['start_date'] >= group_params['end_date']:
                logger.error(f"\'{param_name}\' has start date greater than end date.")
                sys.exit(c.CONFIG_FILE_ERR)

            # Get length of implicit strategies while checking for inconsistencies
            strategies_len = 1
            for key in group_params:
                if isinstance(group_params[key], list):
                    # Multiple implicit strategies must agree in number
                    if strategies_len > 1 and len(group_params[key]) != strategies_len:
                        logger.error(f"Implicit strategy executions do not match size.")
                        sys.exit(c.CONFIG_FILE_ERR)
                    if len(group_params[key]) == 1:
                        group_params[key] = group_params[key][0]
                    else:
                        strategies_len = max(strategies_len, len(group_params[key]))

            # Process the output
            tickers = []
            for strat_index in range(strategies_len):

                tickers_per_strat = self._db_general_model.get_tickers(
                    group_params['on_shares'] if not isinstance(group_params['on_shares'], list)
                        else group_params['on_shares'][strat_index],
                    group_params['pn_shares'] if not isinstance(group_params['pn_shares'], list)
                        else group_params['pn_shares'][strat_index],
                    group_params['units'] if not isinstance(group_params['units'], list)
                        else group_params['units'][strat_index],
                    group_params['fractional_market'] if not isinstance(
                        group_params['fractional_market'], list)
                        else group_params['fractional_market'][strat_index],
                    group_params['sectors'] if not isinstance(group_params['sectors'], list)
                        else group_params['sectors'][strat_index],
                    group_params['subsectors'] if not isinstance(group_params['subsectors'], list)
                        else group_params['subsectors'][strat_index],
                    group_params['segments'] if not isinstance(group_params['segments'], list)
                        else group_params['segments'][strat_index],
                )

                if tickers_per_strat.empty:
                    logger.error(f"\'{param_name}\' has no tickers in database.")
                    sys.exit(c.CONFIG_FILE_ERR)

                tickers_wrapper = {}
                for tck_name in tickers_per_strat['ticker'].to_list():
                    tickers_wrapper[tck_name] = {
                        'start_date': group_params['start_date']
                            if not isinstance(group_params['start_date'], list)
                            else group_params['start_date'][strat_index],
                        'end_date': group_params['end_date']
                            if not isinstance(group_params['end_date'], list)
                            else group_params['end_date'][strat_index]
                        }

                tickers.append(tickers_wrapper)

            if len(tickers) == 1:
                tickers = tickers[0]

        return tickers

    @staticmethod
    def replace_text(param_name, strategies):
        """
        Replace text in parameter with its respective value.

        Handle multiple implicit strategies.

        Parameters
        ----------
        param_name : str
            Parameter name to replace text.
        strategies : list
            List of strategies where to reaplce text.
        """
        for strategy in strategies:
            if param_name in strategy:

                strategy[param_name] = strategy[param_name].\
                    replace('{capital}', str(strategy['capital']))
                strategy[param_name] = strategy[param_name].\
                replace('{number_of_tickers}', str(len(strategy['tickers'])))
                strategy[param_name] = strategy[param_name].\
                replace('{risk_capital_coefficient}', str(strategy['risk_capital_coefficient']))
                strategy[param_name] = strategy[param_name].\
                replace('{min_order_volume}', str(strategy['min_order_volume']))
                strategy[param_name] = strategy[param_name].\
                replace('{ticker_min_ann_volume_filter}', str(strategy['ticker_min_ann_volume_filter']))

    @staticmethod
    def subtract_last_end_date(strategies):
        """
        Subtract one day of open interval `end_date` of parameter `tickers` through all strategies.
        """
        for strategy in strategies:
            if 'tickers' in strategy:
                for ticker in list(strategy['tickers'].keys()):
                    strategy['tickers'][ticker]['end_date'] = \
                        strategy['tickers'][ticker]['end_date'] - timedelta(days=1)

                    if strategy['tickers'][ticker]['start_date'] == \
                        strategy['tickers'][ticker]['end_date']:
                        logger.error(f"Start date and end date can not be equal.")

if __name__ == "__main__":
    ConfigReader()