# %%

# Read variables from Config file

import json
import logging
from datetime import datetime
import os
import sys

config_filename = 'Config.json'
log_path = '/Users/atcha/Github/Projeto-Final/Logs'
log_filename = 'Log'

# Configure logging
log_filename = datetime.now().strftime(log_filename+'_%d-%m-%Y.txt')

logger = logging.getLogger()

while logger.hasHandlers():
    logger.removeHandler(logger.handlers[0])

fhandler = logging.FileHandler(filename=os.path.join(log_path, log_filename), mode='a')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
fhandler.setFormatter(formatter)
logger.addHandler(fhandler)
logger.setLevel(logging.DEBUG)

logging.info('\n')
logging.info('Preprocessing started')

logging.info('Reading '+config_filename+' file in path: '+log_path)

try:
    cfg_file = open('../Config/Config.json', 'r')
except:
    logging.error('Program aborted: Couldn''t open configuration file: '+config_filename)

try:
    config_json = json.load(cfg_file)
except ValueError:
    logging.error('Program aborted: Expected config file in JSON format. Is it corrupted?')

# Get configuration variables
stock_targets = config_json['stock_targets']
ticks_files_path = config_json['ticks_files_path']
holidays = config_json['holidays']
cfg_file.close()

# Store on local variables
stock_names = [item['name'] for item in stock_targets if "name" in item.keys()]
initial_days_str = [item['initial_date'] for item in stock_targets if "initial_date" in item.keys()]
final_days_str = [item['final_date'] for item in stock_targets if "final_date" in item.keys()]

if (stock_names is None) or (initial_days_str is None) or (final_days_str is None):
    logging.error('Program aborted: Config file does not contain any valid stock.')
    sys.exit()

if not(len(stock_names) == len(initial_days_str) == len(final_days_str)):
    logging.error('Program aborted: Missing values in parameter "stock_targets" of the config file.')
    sys.exit()

if len(stock_names) != len(set(stock_names)):
    print("Program aborted: Can't handle duplicate stocks for now.")

initial_days = [datetime.strptime(day, '%d/%m/%Y') for day in initial_days_str]
final_days = [datetime.strptime(day, '%d/%m/%Y') for day in final_days_str]

for index, (start, end) in enumerate(zip(initial_days, final_days)):
    if start > end:
        logging.error('Program aborted: Final date greater than initial date for the stock "'+stock_names[index]+'".')
        sys.exit()

logging.info('Stocks which will be processed:')
for target in stock_targets:
    logging.info('Stock: '+target['name']+'\t\tInital date: '+target['initial_date']+'\t\tFinal date: '+target['final_date'])

# Output important variables:
# stock_names, initial_days, final_days

# %%

# Validate stock ticks files

from os import listdir
from os.path import dirname, join, isfile, pardir
from datetime import datetime
import re
import pandas as pd

logging.info('Checking existence of stock files.')

# Get all file names in the folder
ticks_files_path_abs = join(dirname(__file__), pardir, ticks_files_path)
files_in_folder = [f for f in listdir(ticks_files_path_abs) if isfile(join(ticks_files_path_abs, f))]

# Generate list of all expected days for each stock
stock_valid_days = []
holidays_datetime = [datetime.strptime(item, '%d/%m/%Y') for item in holidays]
for i, (stock, start_day, end_day) in enumerate(zip(stock_names, initial_days, final_days)):

    days_list = pd.date_range(start_day, end_day, freq='d').to_pydatetime().tolist()
    stock_valid_days.append([item for item in days_list if (item.isoweekday() < 6) and (holidays_datetime.count(item) == 0)])

    if not stock_valid_days[i]:
        logging.error('Program aborted: No valid day for the stock "'+stock+'". Did you consider weekends and holidays?')
        sys.exit()

# Check if the expected days have their ticks files

found_files = []    # Auxiliary for data manipulation only
stock_valid_files = []
# For each stock
for stock_index, days_given_stock in enumerate(stock_valid_days):
    cumulative_files_flags = []
    cumulative_filenames = []

    # For each day
    for day_index, days_in_stock in enumerate(days_given_stock):

        filename_re = re.compile(r"^"+stock_names[stock_index]+"_"+str(days_in_stock.year)+str(days_in_stock.month).zfill(2)+str(days_in_stock.day).zfill(2)+r"\d\d\d\d_"+str(days_in_stock.year)+str(days_in_stock.month).zfill(2)+str(days_in_stock.day).zfill(2)+r"[012]\d\d\d\.csv$")
        results = [re.match(filename_re, item) for item in files_in_folder]

        files_per_day = 0
        for results_index, regex_match in enumerate(results):
            if regex_match:
                cumulative_filenames.append(regex_match.group(0))
                files_per_day = files_per_day + 1

        if files_per_day > 1:
            logging.error('Program aborted: Only one file per day is allowed. Found '+str(sum(results))+' files for the stock "'+stock_names[stock_index]+'" on the date "'+days_in_stock.strftime('%d-%m-%Y')+'". Which one should be used?')
            sys.exit()

        cumulative_files_flags.append(any(results))
    found_files.append(cumulative_files_flags)
    stock_valid_files.append(cumulative_filenames)

# Analyse if the number of files found is the same as the expected
number_of_files_found = sum(map(sum, found_files))
total_files_searched = sum(map(len, found_files))

if number_of_files_found == total_files_searched:
    logging.info('All files were found.')
else:
    logging.error('Missing '+str(total_files_searched-number_of_files_found)+' file(s): ')
    for stock_index, days_given_stock in enumerate(stock_valid_days):
        for day_index, days_in_stock in enumerate(days_given_stock):
            if found_files[stock_index][day_index] == False:
                logging.error(stock_names[stock_index]+': '+days_in_stock.strftime('%d-%m-%Y'))
    sys.exit()

# Output important variables:
# stock_valid_files

# %%

# Create Dataframes from the files
# Implemented only for stock_names[0]

import pandas as pd
from os.path import join

for f in stock_valid_files[0]:
    # df_raw = pd.concat(pd.read_csv(join(ticks_files_path_abs, f), sep='\t'))
    df_raw = pd.read_csv(join(ticks_files_path_abs, f), sep='\t')

df_raw.columns = ['Date', 'Time', 'Bid', 'Ask', 'Last', 'Volume', 'Flags']
df_raw['Datetime'] = pd.to_datetime(df_raw['Date'] + " " + df_raw['Time'])
df_raw = df_raw.drop(columns=['Time', 'Date'])
df_raw['Bid'].fillna(method='ffill', inplace=True)
df_raw['Ask'].fillna(method='ffill', inplace=True)
df_raw['Last'].fillna(method='ffill', inplace=True)
df_raw['Volume'].fillna(value=0, inplace=True)
df_raw.set_index('Datetime', inplace=True)

print(df_raw)

# Output important variables:
# df_raw

# %%

# Create Dataframe of candles
# TODO:
# Maximum and minimum values must exclude last closing price
# Volume has little error: Check if flag 96 alter volume for candle

from datetime import datetime, timedelta
import numpy as np

# Input variables

time_division = timedelta(days=1, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0)
start_frame = datetime(2020, 8, 10, 10, 0, 0)
end_frame = datetime(2020, 8, 10, 18, 0, 0)

# Sequence of candles
candle_sec = time_division.total_seconds()

candles_index = pd.date_range(start=start_frame, end=end_frame, freq=str(candle_sec)+"S") # 'min' or 'T'

# Clean empty candles
indexes_to_remove = []
for i in range(len(candles_index)-1):
    if len(df_raw.loc[candles_index[i]:candles_index[i+1]]) == 0:
        indexes_to_remove.append(i)
    elif len(df_raw.loc[(df_raw.index >= candles_index[i]) & (df_raw.index < candles_index[i+1]) & (df_raw['Volume'] != 0.0)]) == 0:
        indexes_to_remove.append(i)

# Special verification for the last one
if (end_frame-start_frame).total_seconds() % candle_sec == 0:
    indexes_to_remove.append(len(candles_index)-1)

candles_index = candles_index.delete(indexes_to_remove)


# Create list of maximum prices for each candle
max_prices = [df_raw.loc[candles_index[i]:candles_index[i+1]]['Last'].max() for i in range(len(candles_index)-1)]
# Insert last candle
if (df_raw.loc[candles_index[-1]:end_frame]['Last'].max() != np.nan):
    max_prices.append(df_raw.loc[candles_index[-1]:end_frame]['Last'].max())


# Create list of minimum prices for each candle
min_prices = [df_raw.loc[candles_index[i]:candles_index[i+1]]['Last'].min() for i in range(len(candles_index)-1)]
# Insert last candle
if (df_raw.loc[candles_index[-1]:end_frame]['Last'].min() != np.nan):
    min_prices.append(df_raw.loc[candles_index[-1]:end_frame]['Last'].min())


# Index list for closing prices of each candles
close_prices_index = [df_raw.loc[df_raw.index <= candles_index[i+1], ['Last']].tail(1).index.values[0] for i in range(len(candles_index)-1)]
# Insert last candle
close_prices_index.append(df_raw.loc[df_raw.index < end_frame, ['Last']].tail(1).index.values[0])
# Get values list and remove duplicates
close_prices = df_raw.loc[close_prices_index, ['Last']]
close_prices = close_prices[~close_prices.index.duplicated(keep='last')]['Last'].to_list()


# Index price list of opening prices for each candle
open_prices_index = [df_raw.loc[(df_raw.index >= candles_index[i]) & (df_raw.index < candles_index[i]+time_division) & (df_raw['Last'] != close_prices[i-1]), ['Last']].head(1).index.values[0] if len(df_raw.loc[(df_raw.index >= candles_index[i]) & (df_raw.index < candles_index[i]+time_division) & (df_raw['Last'] != close_prices[i-1]), ['Last']]) != 0 else df_raw.loc[(df_raw.index >= candles_index[i]), ['Last']].head(1).index.values[0] for i in range(1, len(candles_index))]
# Insert first candle
open_prices_index.insert(0, df_raw.loc[df_raw.index >= start_frame].head(1).index.values[0])
# Get values list and remove duplicates
open_prices = df_raw.loc[open_prices_index, ['Last']]
open_prices = open_prices[~open_prices.index.duplicated(keep='first')]['Last'].to_list()


# Calculate volumes
volumes = [df_raw.loc[candles_index[i]:candles_index[i+1]]['Volume'].sum() for i in range(len(candles_index)-1)]
if (df_raw.loc[candles_index[-1]:end_frame]['Last'].min() != np.nan):   #min here is just a way to check for null
    volumes.append(df_raw.loc[candles_index[-1]:end_frame]['Volume'].sum())


# Output: DataFrame de candles
candles = pd.DataFrame({'Open':open_prices, 'Max':max_prices, 'Min':min_prices, 'Close':close_prices, 'Volume':volumes}, index=[candles_index])

# Save to file
candles.to_csv(r'/Users/atcha/Github/Projeto-Final/Processed Files/teste.csv', header=True)
print(candles)

# Output important variables:
# candles
# %%
