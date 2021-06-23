import datetime

# Config
CONFIG_FILENAME = 'config.json'
CONFIG_PATH = 'config/'
ACCEPTABLE_TRUE_VALUES = ['true', 'yes', 'y', True]
ACCEPTABLE_FALSE_VALUES = ['false', 'no', 'n', False]
ACCEPTABLE_NONE_VALUES = ['none', 'null', None]

# Log
LOG_FILENAME = 'app.log'
LOG_PATH = 'logs/'
LOG_FORMATTER_STRING = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
LOG_FILE_MAX_SIZE = 1*1024*1024

# Candlegraphs processing
# MARKET_OPEN_TIME = datetime.time(hour=10, minute=0, second=0)
# MARKET_CLOSE_TIME = datetime.time(hour=17, minute=0, second=0)

# MAJOR_GRAPH_TIME = 'week'
# MAIN_GRAPH_TIME = 'day'
# MINOR_GRAPH_TIME = 'hour'


# Error codes
CONFIG_FILE_ERR = 1
DATA_SOURCE_ERR = 2
NO_VALID_DAYS_ERR = 3
DB_CONNECTION_ERR = 4
QUERY_ERR = 5
YFINANCE_ERR = 6
UPDATING_DB_ERR = 7
INVALID_ARGUMENT_ERR = 8
NO_HOLIDAYS_DATA_ERR = 9
NO_CDI_DATA_ERR = 10
NO_TICKER_FOR_STRATEGY_ERR = 11
PERMISSION_DENIED = 12
