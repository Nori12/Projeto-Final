import datetime

# Config
CONFIG_FILENAME = 'config.json'
CONFIG_PATH = 'config/'
ACCEPTABLE_TRUE_VALUES = ['true', 'yes', 'y', True]
ACCEPTABLE_FALSE_VALUES = ['false', 'no', 'n', False]
ACCEPTABLE_NONE_VALUES = ['none', 'null', None]
MODELS_PATH = 'machine_learning/models/ticker_oriented_models/random_forest_classifier'
MODEL_SUFFIX = '_rnd_fst_model.joblib'
TICKERS_OPER_OPT_PATH = 'optimizers/stop_loss_optimizer/out_csv_files/tickers_oper_opt.csv'
DATASETS_PATH = 'machine_learning/datasets/'

# Log
LOG_FILENAME = 'app.log'
LOG_PATH = 'logs/'
LOG_FORMATTER_STRING = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
LOG_FILE_MAX_SIZE = 1*1024*1024

MAX_DAYS_PER_OPERATION = 45

# Exit error/warning codes
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
PROCESSING_OPERATIONS_ERR = 12
NO_STRATEGY_ERR = 13
INVALID_PEAK_ERR = 14
