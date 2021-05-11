import psycopg2
import os
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

import constants as c

# Database macros
DB_USER = os.environ.get('STOCK_MARKET_DB_USER')
DB_PASS = os.environ.get('STOCK_MARKET_DB_PASS')
DB_NAME = 'StockMarket'
DB_PORT = 5433
DB_HOST =  'localhost'

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)


class DBModel:

    def __init__(self):
        try:
            connection = psycopg2.connect(f"dbname='{DB_NAME}' user={DB_USER} host='{DB_HOST}' password={DB_PASS} port='{DB_PORT}'")
            logger.debug(f'Database \'{DB_NAME}\' connected successfully.')
        except:
            logger.error(f'Database \'{DB_NAME}\' connection failed.')
            sys.exit(c.DB_CONNECTION_ERR)

        self._connection = connection
        self._cursor = self._connection.cursor()

    def __del__(self):
        self._connection.close()
        self._cursor.close()

    def query(self, query, params=None):
        try:
            self._cursor.execute(query, params)
        except Exception as error:
            logger.error('Error executing query "{}", error: {}'.format(query, error))
            return None

        return self._cursor.fetchall()

    def get_all_classifications(self):
        # self._cursor.execute("""SELECT economic_sector, economic_subsector, economic_segment from classification""")
        # result = self._cursor.fetchall()
        result = self.query("""SELECT economic_sector, economic_subsector, economic_segment FROM company_classification""")

        return(result)



    # def __enter__(self):
    #     return self

    # # Hã?
    # def __exit__(self, ext_type, exc_value, traceback):
    #     if isinstance(exc_value, Exception):
    #         self.connection.rollback()
    #     else:
    #         self.connection.commit()
    #     self.connection.close()

