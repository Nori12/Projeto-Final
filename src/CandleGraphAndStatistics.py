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

logging.info('Stocks parsed:')
for target in stock_targets:
    logging.info('Stock: '+target['name']+'\t\tInital date: '+target['initial_date']+'\t\tFinal date: '+target['final_date'])

# %%

# Look for stock ticks files

from os import listdir
from os.path import dirname, join, isfile, pardir
from datetime import datetime
import re
import pandas as pd

logging.info('Checking existence of stock files')

project_directory = dirname(__file__)
ticks_files = join(project_directory, pardir, ticks_files_path)
files_in_folder = [f for f in listdir(ticks_files) if isfile(join(ticks_files, f))]

stock_valid_days = []

for i, (stock, start_day, end_day) in enumerate(zip(stock_names, initial_days, final_days)):
    days_list = pd.date_range(start_day, end_day, freq='d').to_pydatetime().tolist()
    stock_valid_days.append([item for item in days_list if item.isoweekday() < 6])

found_files = []
found_files.append([])
found_files.append([])

for stock_index, days_given_stock in enumerate(stock_valid_days):
    for day_index, days_in_stock in enumerate(days_given_stock):
        filename_re = re.compile(r"^"+stock_names[stock_index]+"_"+str(days_in_stock.year)+str(days_in_stock.month).zfill(2)+str(days_in_stock.day).zfill(2)+r"\d\d\d\d_"+str(days_in_stock.year)+str(days_in_stock.month).zfill(2)+str(days_in_stock.day).zfill(2)+r"[012]\d\d\d\.csv$")
        results = [bool(re.match(filename_re, item)) for item in files_in_folder]
        
        if sum(results) > 1:
            logging.error('Program aborted: Only one file per day is allowed. Found '+str(sum(results))+' files for the stock "'+stock_names[stock_index]+'" on the date "'+days_in_stock.strftime('%d-%m-%Y')+'". Which one should be used?')
            sys.exit()

        found_files[stock_index].append(any(results))

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

# %%
# Create Dataframe from CSV file

import pandas as pd

filename = "MGLU3_202008130600_202008131945.csv"

df_raw = pd.read_csv(filename, sep='\t')

# Change column names
df_raw.columns = ['Date', 'Time', 'Bid', 'Ask', 'Last', 'Volume', 'Flags']

# Combine Date and Time columns together and drop Time
df_raw['Datetime'] = pd.to_datetime(df_raw['Date'] + " " + df_raw['Time'])
df_raw = df_raw.drop(columns=['Time', 'Date'])
df_raw.fillna(method='ffill', inplace=True)
df_raw.dropna(inplace=True)
df_raw.set_index('Datetime', inplace=True)

print(df_raw)

# %%

# Gerar Dataframe de Candles
from datetime import datetime
import numpy as np

# Variáveis de entrada
candle_min = 1
start_frame = datetime(2020, 8, 13, 10, 0, 0)
end_frame = datetime(2020, 8, 13, 17, 0, 0)

# Cria sequencia de candles
candles_index = pd.date_range(start=start_frame, end=end_frame, freq=str(candle_min)+"min") # 'min' or 'T'

# Limpa candles sem dados
while (len(df_raw.loc[candles_index[0]:candles_index[1]]) == 0):
    # print("Candle {} vazio removido".format(candles_index[0]))
    candles_index = candles_index.delete(0)

while (len(df_raw.loc[candles_index[-1]:end_frame]) == 0):
    # print("Candle {} vazio removido".format(candles_index[-1]))
    candles_index = candles_index.delete(-1)

# Gera lista de preços máximos para cada candle
max_prices = [df_raw.loc[candles_index[i]:candles_index[i+1]]['Last'].max() for i in range(len(candles_index)-1)]

# Último candle precisa se inserido
if (df_raw.loc[candles_index[-1]:end_frame]['Last'].max() != np.nan):  
    max_prices.append(df_raw.loc[candles_index[-1]:end_frame]['Last'].max())

# Gera lista de preços mínimos para cada candle
min_prices = [df_raw.loc[candles_index[i]:candles_index[i+1]]['Last'].min() for i in range(len(candles_index)-1)]

# Último candle precisa se inserido
if (df_raw.loc[candles_index[-1]:end_frame]['Last'].min() != np.nan):
    min_prices.append(df_raw.loc[candles_index[-1]:end_frame]['Last'].min())

# Lista de índices do preço de fechamento em cada candle
close_prices_index = [df_raw.loc[df_raw.index <= candles_index[i+1], ['Last']].tail(1).index.values[0] for i in range(len(candles_index)-1)]
# Último índice precisa se inserido manualmente
close_prices_index.append(df_raw.loc[df_raw.index < end_frame, ['Last']].tail(1).index.values[0])
# Gera lista de valores e remove duplicatas
close_prices = df_raw.loc[close_prices_index, ['Last']]
close_prices = close_prices[~close_prices.index.duplicated(keep='last')]['Last'].to_list()

# Lista de índices do preço de abertura em cada candle
open_prices_index = [df_raw.loc[(df_raw.index >= candles_index[i]) & (df_raw['Last'] != close_prices[i-1]), ['Last']].head(1).index.values[0] for i in range(1, len(candles_index))]
# Primeiro índice precisa se inserido manualmente
open_prices_index.insert(0, df_raw.index.values[0])
# Gera lista de valores e remove duplicatas
open_prices = df_raw.loc[open_prices_index, ['Last']]
open_prices = open_prices[~open_prices.index.duplicated(keep='first')]['Last'].to_list()

# Calcular volumes
# volumes = [df_raw.loc[candles_index[i]:candles_index[i+1]]['Volume'].sum() for i in range(len(candles_index)-1)]
# if (df_raw.loc[candles_index[-1]:end_frame]['Last'].min() != np.nan):
# volumes.append(df_raw.loc[candles_index[-1]:end_frame]['Volume'].sum())

# Output: DataFrame de candles
candles = pd.DataFrame({'Open':open_prices, 'Max':max_prices, 'Min':min_prices, 'Close':close_prices, 'Volume':volumes}, index=[candles_index])

print(candles)

# %%

# Flags
TICK_FLAG_BID = 0b00000010  # Tick has changed a bid price
TICK_FLAG_ASK = 0b00000100 # Tick has changed a ask price
TICK_FLAG_LAST = 0b00001000 # Tick has changed the last deal price
TICK_FLAG_VOLUME = 0b00010000   # Tick has changed a volume
TICK_FLAG_BUY = 0b00100000  # Tick is a result of a buy deal
TICK_FLAG_SELL = 0b01000000 # Tick is a result of a sell deal

# %%
