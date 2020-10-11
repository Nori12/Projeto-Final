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
processed_files_path = config_json['processed_files_path']
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

print('stock_names:\n\t'+str(stock_names))
print('initial_days:\n\t'+str(initial_days))
print('final_days:\n\t'+str(final_days))


# %%

# Validate stock ticks files

from os import listdir
from os.path import dirname, join, isfile, pardir # delete later
from pathlib import Path
import glob
import datetime
import re
import pandas as pd

market_open_time = datetime.time(hour=10, minute=0, second=0)
market_close_time = datetime.time(hour=17, minute=0, second=0)

logging.info('Checking existence of stock files.')

# Get all file names in the folder
data_folder = Path(__file__).parents[1] / ticks_files_path
files_in_data_folder = [[f for f in data_folder.glob(stock+'*.csv')] for stock in stock_names]

# Generate list of all expected days for each stock
stock_valid_days = []
holidays_datetime = [datetime.datetime.strptime(item, '%d/%m/%Y') for item in holidays]
for i, (stock, start_day, end_day) in enumerate(zip(stock_names, initial_days, final_days)):

    days_list = pd.date_range(start_day, end_day, freq='d').to_pydatetime().tolist()
    stock_valid_days.append([item for item in days_list if (item.isoweekday() < 6) and (holidays_datetime.count(item) == 0)])

    if not stock_valid_days[i]:
        logging.error('Program aborted: No valid day for the stock "'+stock+'". Did you consider weekends and holidays?')
        sys.exit()

stock_days_file_pointer = []

for stock_index, stock_name in enumerate(stock_names):
    filename_re = re.compile(r"^"+stock_names[stock_index]+"_"+r"(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])([0-1]\d|2[0-3])([0-5]\d)_(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])([0-1]\d|2[0-3])([0-5]\d)\.csv$")
    results = [re.match(filename_re, item.name) for item in files_in_data_folder[stock_index]]

    stock_days_file_pointer.append([None for i in range(len(stock_valid_days[stock_index]))])

    for index, item in enumerate(results):
        if item != None:
            file_first_day = datetime.date(year=int(item.group(1)), month=int(item.group(2)), day=int(item.group(3)))
            file_last_day = datetime.date(year=int(item.group(6)), month=int(item.group(7)), day=int(item.group(8)))

            for day_index, day in enumerate(stock_valid_days[stock_index]):
                if day.date() >= file_first_day and day.date() <= file_last_day:
                    if stock_days_file_pointer[stock_index][day_index] is None:
                        stock_days_file_pointer[stock_index][day_index] = item.group(0)
                    else:
                        logging.error('Program aborted: Day "'+str(day.date().strftime('%d/%m/%Y'))+'" overlapped for stock "'+stock_name+'". Please remove ambiguity.')
                        sys.exit()                        

for stock_index, stock_name in enumerate(stock_names):
    for day_index, file_pointer in enumerate(stock_days_file_pointer[stock_index]):
        if file_pointer is None:
            logging.error('Program aborted: "'+stock_name+'" -> Missing data for day "'+str(stock_valid_days[stock_index][day_index].date().strftime('%d/%m/%Y'))+'".')
            sys.exit()                        

logging.info('Input ticks files found.')

# Output important variables:
# stock_valid_days, stock_days_file_pointer

print('stock_valid_days:\n\t'+str(stock_valid_days))
print('stock_days_file_pointer:\n\t'+str(stock_days_file_pointer))

# %%

# Verify if files were already processed

from pathlib import Path

stocks_ok = [False]*len(stock_names)

# Get all file names in the folder
data_folder = Path(__file__).parents[1] / processed_files_path
processed_files = [[f for f in (data_folder / stock).glob(stock+'*.csv')] for stock in stock_names]

for stock_index, stock_name in enumerate(stock_names):
    filename_week_re = re.compile(r"^"+stock_names[stock_index]+"_CANDLES_W_"+r"(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])_(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])\.csv$")
    filename_day_re = re.compile(r"^"+stock_names[stock_index]+"_CANDLES_D_"+r"(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])_(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])\.csv$")
    filename_60m_re = re.compile(r"^"+stock_names[stock_index]+"_CANDLES_60m_"+r"(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])_(\d\d\d\d)(1[0-2]|0[1-9])(3[01]|[12][0-9]|0[1-9])\.csv$")
 
    results_week = [re.match(filename_week_re, item.name) for item in processed_files[stock_index]]
    results_day = [re.match(filename_day_re, item.name) for item in processed_files[stock_index]]
    results_60m = [re.match(filename_60m_re, item.name) for item in processed_files[stock_index]]

    if (results_week.count(None) != len(results_week) and
            results_day.count(None) != len(results_day) and
            results_60m.count(None) != len(results_60m)):
        stocks_ok[stock_index] = True

    if stocks_ok[stock_index] == True:
        logging.info('Could not find processed files for stock "'+stock_name+'".')
    else:
        logging.info('Processed files found for stock "'+stock_name+'".')

# Output important variables:
# stocks_ok

print('stock_names:\n\t'+str(stock_names))
print('stocks_ok:\n\t'+str(stocks_ok))

# %%
import pandas as pd
from pathlib import Path
from datetime import datetime, time, timedelta
import numpy as np

# Input variables

greater_time_division = timedelta(weeks=1, days=0, hours=0, minutes=0, seconds=0, microseconds=0, milliseconds=0)
middle_time_division = timedelta(weeks=0, days=1, hours=0, minutes=0, seconds=0, microseconds=0, milliseconds=0)
smaller_time_division = timedelta(weeks=0, days=0, hours=1, minutes=0, seconds=0, microseconds=0, milliseconds=0)
market_open_time = time(hour=10, minute=0, second=0)
market_close_time = time(hour=17, minute=0, second=0)

all_time_divisions = [smaller_time_division, middle_time_division, greater_time_division]

data_file_path = Path(__file__).parents[1] / ticks_files_path

for index, (stock, status) in enumerate(zip(stock_names, stocks_ok)):
    if status == True:

        last_file_pointer= None

        for day, file_pointer in zip(stock_valid_days[index], stock_days_file_pointer[index]):

            start_frame = datetime.combine(day, market_open_time)
            end_frame = datetime.combine(day, market_close_time)

            # If file change, needs to reload dataframe
            if last_file_pointer != file_pointer:
                if data_file_path.name != ticks_files_path:
                    data_file_path = data_file_path.parent / file_pointer
                    # print("Change:"+str(data_file_path))
                else: # When it enters in this loop
                    data_file_path = data_file_path / file_pointer
                    # print("First:"+str(data_file_path))

                last_file_pointer = file_pointer
                df_raw = pd.read_csv(data_file_path, sep='\t')

                df_raw.columns = ['Date', 'Time', 'Bid', 'Ask', 'Last', 'Volume', 'Flags']
                df_raw['Datetime'] = pd.to_datetime(df_raw['Date'] + " " + df_raw['Time'])
                df_raw = df_raw.drop(columns=['Time', 'Date'])
                df_raw['Bid'].fillna(method='ffill', inplace=True)
                df_raw['Ask'].fillna(method='ffill', inplace=True)
                df_raw['Last'].fillna(method='ffill', inplace=True)
                df_raw['Volume'].fillna(value=0, inplace=True)
                df_raw.set_index('Datetime', inplace=True)
            
            # Create and store candle data for each time division
            for time_division in all_time_divisions:

                # Sequence of candles
                candles_index = pd.date_range(start=start_frame, end=end_frame, freq=str(time_division.total_seconds())+"S")

                # Clean empty candles - For 15min candles of more this is nor necessary
                indexes_to_remove = []
                for i in range(len(candles_index)-1):
                    if len(df_raw.loc[candles_index[i]:candles_index[i+1]]) == 0: # Remove data out of candles coverage
                        indexes_to_remove.append(i)
                    elif len(df_raw.loc[(df_raw.index >= candles_index[i]) & (df_raw.index < candles_index[i+1]) 
                        & (df_raw['Volume'] != 0)]) == 0: # Remove beginning empty candle values 
                        indexes_to_remove.append(i)

                # Special verification for the last one
                if (end_frame-start_frame).total_seconds() % time_division.total_seconds() == 0:
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

                # Index list for closing prices of each candle
                close_prices_index = [df_raw.loc[df_raw.index <= candles_index[i+1], ['Last']].tail(1).index.values[0] for i in range(len(candles_index)-1)]
                # Insert last candle
                close_prices_index.append(df_raw.loc[df_raw.index < end_frame, ['Last']].tail(1).index.values[0])
                # Get values list and remove duplicates
                close_prices = df_raw.loc[close_prices_index, ['Last']]
                close_prices = close_prices[~close_prices.index.duplicated(keep='last')]['Last'].to_list()

                # Index price list of opening prices for each candle
                open_prices_index = [df_raw.loc[(df_raw.index >= candles_index[i]) & (df_raw.index < candles_index[i]+time_division) 
                    & (df_raw['Last'] != close_prices[i-1]), ['Last']].head(1).index.values[0] 
                    if len(df_raw.loc[(df_raw.index >= candles_index[i]) & (df_raw.index < candles_index[i]+time_division) & (df_raw['Last'] != close_prices[i-1]), ['Last']]) != 0 else df_raw.loc[(df_raw.index >= candles_index[i]), ['Last']].head(1).index.values[0] for i in range(1, len(candles_index))]
                # Insert first candle
                open_prices_index.insert(0, df_raw.loc[df_raw.index >= start_frame].head(1).index.values[0])
                # Get values list and remove duplicates
                open_prices = df_raw.loc[open_prices_index, ['Last']]
                open_prices = open_prices[~open_prices.index.duplicated(keep='first')]['Last'].to_list()

                # Calculate volumes
                volumes = [df_raw.loc[candles_index[i]:candles_index[i+1]]['Volume'].sum() for i in range(len(candles_index)-1)]
                if (df_raw.loc[candles_index[-1]:end_frame]['Last'].min() != np.nan):   #min here is just a way to check for null
                    volumes.append(df_raw.loc[candles_index[-1]:end_frame]['Volume'].sum())

                candles = pd.DataFrame({'Open':open_prices, 'Max':max_prices, 'Min':min_prices, 'Close':close_prices, 'Volume':volumes}, index=[candles_index])

                # Continue here -> save dataframes

                # Save to file
                candles.to_csv(r'/Users/atcha/Github/Projeto-Final/Processed Files/teste.csv', header=True)

                




# %%


# Create Dataframes from the files
# Implemented only for stock_names[0]

import pandas as pd
from os.path import join

for f in stock_valid_files[0]:
    df_raw = pd.concat(pd.read_csv(join(ticks_files_path_abs, f), sep='\t'))

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

time_division = timedelta(days=0, seconds=30, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0)
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

from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split

X = candles.values.tolist()
X.pop(-1)

y = candles['Close'].values.tolist()
y.pop(0)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, random_state=0)

ridge = Ridge(alpha=10).fit(X_train, y_train)

print("Test set score: {:.2f}".format(ridge.score(X_test, y_test)))

print(ridge.predict([X_test[2]]))

print(X_test[2])



# %%
