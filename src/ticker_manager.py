from pathlib import Path
import pandas as pd
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import re

import constants as c
from utils import RunTime

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=5*1024*1024, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

class TickerManager:
    """The class that implements all necessary pre-processing.

    Attributes:
        ticker (str): Ticker name.
        initial_date (datetime): Start date of the time interval.
        final_date (datetime): End date of the time interval.
        input_files_path (Path, optional): Path to input files folder.
        output_files_path (Path, optional): Path to output files folder.
        major_graph_time_source (str): Source of data to create the major graph time, if required.
            Options are ['yfinance', 'ticks_file'].
        main_graph_time_source (str): Source of data to create the main graph time, if required.
            Options are ['yfinance', 'ticks_file'].
        minor_graph_time_source (str): Source of data to create the minor graph time, if required.
            Options are ['yfinance', 'ticks_file'].
    """


    def __init__(self, ticker, initial_date, final_date, input_files_path='input_files', output_files_path='output_files', major_graph_time_source='yfinance', main_graph_time_source='yfinance', minor_graph_time_source='ticks_file'):
        self._ticker = ticker
        self._initial_date = initial_date
        self._final_date = final_date

        if input_files_path == 'input_files':
            self.ticker_input_path = Path(__file__).parent.parent / input_files_path / self._ticker
        else:
            self.ticker_input_path = Path(input_files_path)
            if self.ticker_input_path.is_dir() == False:
                logger.error(f'Program aborted. Input path \'{self.ticker_input_path}\' does not exist or is not a folder.')

        if output_files_path == 'output_files':
            self.ticker_output_path = Path(__file__).parent.parent / output_files_path / self._ticker
        else:
            self.ticker_output_path = Path(output_files_path)
            if self.ticker_output_path.is_dir() == False:
                logger.error(f'Program aborted. Output path \'{self.ticker_output_path}\' does not exist or is not a folder.')

        available_sources=['yfinance', 'ticks_file']

        if major_graph_time_source in available_sources:
            self.major_graph_time_source = major_graph_time_source
        else:
            logger.error(f'Program aborted. Data source \'{major_graph_time_source}\' not available.')
            sys.exit(c.DATA_SOURCE_ERR)
        
        if main_graph_time_source in available_sources:
            self.main_graph_time_source = main_graph_time_source
        else:
            logger.error(f'Program aborted. Data source \'{main_graph_time_source}\' not available.')
            sys.exit(c.DATA_SOURCE_ERR)

        if minor_graph_time_source in available_sources:
            self.minor_graph_time_source = minor_graph_time_source
        else:
            logger.error(f'Program aborted. Data source \'{minor_graph_time_source}\' not available.')
            sys.exit(c.DATA_SOURCE_ERR)

    @property
    def ticker(self):
        """Return the ticker name."""
        return self._ticker

    @ticker.setter
    def ticker(self, ticker):
        self._ticker = ticker

    @property
    def initial_date(self):
        """Return the start date of the time interval."""
        return self._initial_date

    @initial_date.setter
    def initial_date(self, initial_date):
        self._initial_date = initial_date

    @property
    def final_date(self):
        """Return the end date of the time interval."""
        return self._final_date

    @final_date.setter
    def final_date(self, final_date):
        self._final_date = final_date

    @RunTime('analyze_output_candles')
    def analyze_output_candles(self):
        output_files = sorted(self.ticker_output_path.glob('*.csv'))

        major_intervals_status = False
        main_intervals_status = False
        minor_intervals_status = False

        intervals_translation = {'week': '1W', 'day': '1D', 'hour': '1H'}

        major_re = re.compile(r"^"+self._ticker+"_CANDLES_"+intervals_translation[c.MAJOR_GRAPH_TIME]+r"_(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])_(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])\.csv$")
        main_re = re.compile(r"^"+self._ticker+"_CANDLES_"+intervals_translation[c.MAIN_GRAPH_TIME]+r"_(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])_(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])\.csv$")
        minor_re = re.compile(r"^"+self._ticker+"_CANDLES_"+intervals_translation[c.MINOR_GRAPH_TIME]+r"_(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])_(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])\.csv$")

        major_file = [re.match(major_re, item.name) for item in output_files if re.match(major_re, item.name) is not None]
        main_file = [re.match(main_re, item.name) for item in output_files if re.match(main_re, item.name) is not None]
        minor_file = [re.match(minor_re, item.name) for item in output_files if re.match(minor_re, item.name) is not None]

        if major_file != []:
            major_file_begin = datetime(int(major_file[0].group(1)), int(major_file[0].group(2)), int(major_file[0].group(3)))
            major_file_end = datetime(int(major_file[0].group(4)), int(major_file[0].group(5)), int(major_file[0].group(6)))

            if self._initial_date >= major_file_begin and self._final_date <= major_file_end:
                major_intervals_status = True

        if main_file != []:
            main_file_begin = datetime(int(main_file[0].group(1)), int(main_file[0].group(2)), int(main_file[0].group(3)))
            main_file_end = datetime(int(main_file[0].group(4)), int(main_file[0].group(5)), int(main_file[0].group(6)))

            if self._initial_date >= main_file_begin and self._final_date <= main_file_end:
                main_intervals_status = True

        if minor_file != []:
            minor_file_begin = datetime(int(minor_file[0].group(1)), int(minor_file[0].group(2)), int(minor_file[0].group(3)))
            minor_file_end = datetime(int(minor_file[0].group(4)), int(minor_file[0].group(5)), int(minor_file[0].group(6)))

            if self._initial_date >= minor_file_begin and self._final_date <= minor_file_end:
                minor_intervals_status = True    

        logger.debug(f'\'{self._ticker}\' major interval output file was {"NOT " if major_intervals_status == False else ""}found.')
        logger.debug(f'\'{self._ticker}\' main interval output file was {"NOT " if main_intervals_status == False else ""}found.')
        logger.debug(f'\'{self._ticker}\' minor interval output file was {"NOT " if minor_intervals_status == False else ""}found.')
        
        return major_intervals_status, main_intervals_status, minor_intervals_status
        










    def analyze_input_candles(self, missing_intervals):
        pass
    
    def create_candlegraphs(self, target_intervals = 'week/day/hour'):
        pass


    # TODO: Tip: normalize data



