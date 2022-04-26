# Config
CONFIG_FILENAME = 'config.json'
CONFIG_PATH = 'config/'
ACCEPTABLE_TRUE_VALUES = ['true', 'yes', 'y', True]
ACCEPTABLE_FALSE_VALUES = ['false', 'no', 'n', False]
ACCEPTABLE_NONE_VALUES = ['none', 'null', None]
MODELS_PATH = 'machine_learning/models/ticker_oriented_models/random_forest_classifier'
MODEL_SUFFIX = '_model.joblib'
TICKERS_OPER_OPT_PATH = 'optimizers/stop_loss_optimizer/out_csv_files/tickers_oper_opt.csv'
DATASETS_PATH = 'machine_learning/datasets/'
DATASET_SUFFIX = '_dataset.csv'

# Log
LOG_FILENAME = 'app.log'
LOG_PATH = 'logs/'
LOG_FORMATTER_STRING = '%(asctime)s:%(levelname)s:%(name)s:%(message)s'
LOG_FILE_MAX_SIZE = 3*1024*1024

# Walk Forward Optimization
WFO_START_DATE = '2019-01-01T00'
WFO_MODEL_TAGS = {
    '2019_1': {'end_year': 2019, 'end_month': 3,  'end_day': 31},
    '2019_2': {'end_year': 2019, 'end_month': 6,  'end_day': 30},
    '2019_3': {'end_year': 2019, 'end_month': 9,  'end_day': 30},
    '2019_4': {'end_year': 2019, 'end_month': 12, 'end_day': 31},
    '2020_1': {'end_year': 2020, 'end_month': 3,  'end_day': 31},
    '2020_2': {'end_year': 2020, 'end_month': 6,  'end_day': 30},
    '2020_3': {'end_year': 2020, 'end_month': 9,  'end_day': 30},
    '2020_4': {'end_year': 2020, 'end_month': 12, 'end_day': 31},
    '2021_1': {'end_year': 2021, 'end_month': 3,  'end_day': 31},
    '2021_2': {'end_year': 2021, 'end_month': 6,  'end_day': 30},
    '2021_3': {'end_year': 2021, 'end_month': 9,  'end_day': 30},
    '2021_4': {'end_year': 2023, 'end_month': 12, 'end_day': 31}, # Proposital extended duration
}

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
DATASET_GENERATION_ERR = 15
MODEL_CREATION_ERR = 16
UNIDENTIFIED_OPERATION_STATUS_ERR = 17
