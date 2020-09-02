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
