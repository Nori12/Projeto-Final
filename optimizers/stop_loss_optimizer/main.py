import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import pandas as pd

from stop_loss_optimizer import StopLossOptimizer
from stop_loss_data_analyzer import StopLossDataAnalyzer
sys.path.insert(1, '/Users/atcha/Github/Projeto-Final/src')
import constants as c

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

def run():
    logger.info('Stop Loss Optimizer started.')

    sl_opt = StopLossOptimizer()
    sl_opt.run_simulation(standard_map=True, risk_map=True)

    # sl_analyzer = StopLossDataAnalyzer(ticker="ABEV3")
    # sl_analyzer.show_all_graphs()

if __name__ == '__main__':
    run()
