import datetime

# Config
CONFIG_FILENAME = 'config.json'
CONFIG_PATH = 'config/'

# Log
LOG_FILENAME = 'app.log'
LOG_PATH = 'logs/'
LOG_FORMATTER_STRING = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'

# Candlegraphs processing
MARKET_OPEN_TIME = datetime.time(hour=10, minute=0, second=0)
MARKET_CLOSE_TIME = datetime.time(hour=17, minute=0, second=0)

MAJOR_GRAPH_TIME = 'week'
MAIN_GRAPH_TIME = 'day'
MINOR_GRAPH_TIME = 'hour'

# Error codes
CONFIG_FILE_ERR = 1
DATA_SOURCE_ERR = 2
NO_VALID_DAYS_ERR = 3
DB_CONNECTION_ERR = 4