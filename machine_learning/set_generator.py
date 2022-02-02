import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import constants as c

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent.parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

def run():
    logger.info('Set Generator started.')


if __name__ == "__main__":
    run()
