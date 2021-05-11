import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import constants as c
from db_model import DBModel

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

class DBManager:

    def __init__(self, ticker, initial_date, final_date):
        self._ticker = ticker.upper()
        self._initial_date = initial_date
        self._final_date = final_date

        self._db_model = DBModel()