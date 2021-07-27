# %%
# Input parameters

maximum_risk = 0.20
minimum_risk = 0.05
max_workdays_per_operation = 126
gain_loss_ratio = 3
filename = 'features.csv'
DEBUG = False

if DEBUG == True:
    filename = 'features_dbg.csv'

# %%
# Get tickers and dates

import config_reader as cr

config = cr.ConfigReader()
tickers = config.tickers_and_dates

print('Tickers:')
msg = ""
for index, ticker in enumerate(tickers):
    if index > 0:
        msg += ", "
    msg += ticker
print(msg)


# %%
# Generate features
from db_model import DBStrategyAnalyzerModel
import pandas as pd

db_model = DBStrategyAnalyzerModel()
peaks_number = 5
peak_delay = 9

DEFAULT_VALUE = 0.0
UNDEFINED = 0
UPTREND = 1
NOT_UPTREND = 0

for tck_index, (ticker, date) in enumerate(config.tickers_and_dates.items()):
    candles_df = db_model.get_ticker_prices_and_features(ticker,
        pd.Timestamp(date['start_date']), pd.Timestamp(date['end_date']),
        interval='1d')

    print(f"Processing Ticker {tck_index+1} of {len(config.tickers_and_dates)}")

    candles_len = len(candles_df)

    if DEBUG == True:
        days = []
    udt_status = []
    buy_prices = []
    min_risks = []
    max_peaks_1 = []
    max_peaks_2 = []
    max_peaks_3 = []
    max_peaks_4 = []
    max_peaks_5 = []
    min_peaks_1 = []
    min_peaks_2 = []
    min_peaks_3 = []
    min_peaks_4 = []
    min_peaks_5 = []
    ema_17 = []
    ema_72 = []
    last_max_peaks = []
    last_min_peaks = []
    current_max_delay = 0
    current_min_delay = 0
    upcoming_max_peak = 0.0
    upcoming_min_peak = 0.0
    end_flag = False

    for index, row in candles_df.iterrows():

        if end_flag == False:

            current_max_delay += 1
            current_min_delay += 1

            if row['peak'] > 0.01:
                if row['peak'] == row['max_price']:
                    upcoming_max_peak = row['peak']
                    current_max_delay = 0
                else:
                    upcoming_min_peak = row['peak']
                    current_min_delay = 0

            if current_max_delay >= peak_delay and upcoming_max_peak != 0.0:
                last_max_peaks.append(upcoming_max_peak)
                if len(last_max_peaks) > peaks_number:
                    last_max_peaks.pop(0)
                upcoming_max_peak = 0.0
            if current_min_delay >= peak_delay and upcoming_min_peak != 0.0:
                last_min_peaks.append(upcoming_min_peak)
                if len(last_min_peaks) > peaks_number:
                    last_min_peaks.pop(0)
                upcoming_min_peak = 0.0

            if len(last_max_peaks) < peaks_number or len(last_min_peaks) < peaks_number:
                continue

            purchase_price = row['close_price']
            max_price = 0.0
            min_price = purchase_price
            min_stop = round(purchase_price * (1 - maximum_risk), 2)
            max_stop = round(purchase_price * (1 - minimum_risk), 2)
            max_target = round(purchase_price + gain_loss_ratio * (purchase_price - min_stop), 2)
            min_target = round(purchase_price + gain_loss_ratio * (purchase_price - max_stop), 2)

            oper_presence_flag = False
            ultimate_index = candles_df.index[-1]
            for index_2, row_2 in candles_df.loc[candles_df.index[
                index+1:min(index+1+max_workdays_per_operation, candles_len)]].iterrows():

                if row_2['min_price'] <= min_stop:
                    min_price = min_stop
                    break

                if row_2['max_price'] >= max_target:
                    max_price = max_target
                    break

                if row_2['max_price'] > max_price:
                    max_price = row_2['max_price']
                if row_2['min_price'] < min_price:
                    min_price = row_2['min_price']

                if max_price >= min_target and (max_price - purchase_price) >= \
                    gain_loss_ratio * (purchase_price - min_price):
                    oper_presence_flag = True
                    break

                if index_2 == ultimate_index:
                    end_flag = True

            if end_flag == True:
                continue

            max_peaks_1.append(round(last_max_peaks[0]/purchase_price, 4))
            max_peaks_2.append(round(last_max_peaks[1]/purchase_price, 4))
            max_peaks_3.append(round(last_max_peaks[2]/purchase_price, 4))
            max_peaks_4.append(round(last_max_peaks[3]/purchase_price, 4))
            max_peaks_5.append(round(last_max_peaks[4]/purchase_price, 4))
            min_peaks_1.append(round(last_min_peaks[0]/purchase_price, 4))
            min_peaks_2.append(round(last_min_peaks[1]/purchase_price, 4))
            min_peaks_3.append(round(last_min_peaks[2]/purchase_price, 4))
            min_peaks_4.append(round(last_min_peaks[3]/purchase_price, 4))
            min_peaks_5.append(round(last_min_peaks[4]/purchase_price, 4))
            ema_17.append(round(row['ema_17']/purchase_price, 4))
            ema_72.append(round(row['ema_72']/purchase_price, 4))
            if DEBUG == True:
                days.append(row['day'])
            # buy_prices.append(1.0)
            # buy_prices.append(purchase_price)

            # if min_price == min_stop:
            if oper_presence_flag == False:
                udt_status.append(NOT_UPTREND)
                min_risks.append(DEFAULT_VALUE)
            else:
                udt_status.append(UPTREND)
                min_risks.append(round((purchase_price-min_price)/purchase_price, 4))

    features_df = pd.DataFrame({
        'max_peak_1': max_peaks_1,
        'min_peak_1': min_peaks_1,
        'max_peak_2': max_peaks_2,
        'min_peak_2': min_peaks_2,
        'max_peak_3': max_peaks_3,
        'min_peak_3': min_peaks_3,
        'max_peak_4': max_peaks_4,
        'min_peak_4': min_peaks_4,
        'max_peak_5': max_peaks_5,
        'min_peak_5': min_peaks_5,
        'ema_17': ema_17,
        'ema_72': ema_72,
        'udt_status': udt_status,
        'min_risk': min_risks
    })
    if DEBUG == True:
        features_df['day'] = days

    if tck_index == 0:
        features_df.to_csv(filename, mode='a', index=False)
    else:
        features_df.to_csv(filename, mode='a', index=False, header=False)


# %%
# DecisionTreeClassifier for UDT Status

import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
import joblib

test_size = 0.10

df = pd.read_csv('features.csv', sep=',')
df_test_only = pd.read_csv('features_test.csv', sep=',')

X_train, X_test, y_train, y_test = train_test_split(df[['max_peak_1', 'min_peak_1',
    'max_peak_2', 'min_peak_2', 'max_peak_3', 'min_peak_3', 'max_peak_4', 'min_peak_4',
    'max_peak_5', 'min_peak_5', 'ema_17', 'ema_72']], df['udt_status'], test_size=test_size,
    random_state=42, shuffle=True)

udt_trees = []
training_accuracy = []
test_accuracy = []
complexity = []

for depth in range(15, 35):
    udt_tree = DecisionTreeClassifier(max_depth=depth, random_state=0)
    udt_tree.fit(X_train, y_train)
    udt_trees.append(udt_tree)
    print(f"\nDecisionTreeClassifier: max_depth = {depth}")
    print("Accuracy on training set: {:.3f}".format(udt_tree.score(X_train, y_train)))
    print("Accuracy on test set: {:.3f}".format(udt_tree.score(X_test, y_test)))
    training_accuracy.append(udt_tree.score(X_train, y_train))
    test_accuracy.append(udt_tree.score(X_test, y_test))
    complexity.append(depth)
    # Save model
    if depth == 35:
        joblib.dump(udt_tree, 'model.joblib')

plt.plot(complexity, training_accuracy, label="Training accuracy")
plt.plot(complexity, test_accuracy, label="Test accuracy")
plt.ylabel("Accuracy")
plt.xlabel("Complexity")
plt.legend()

# udt_tree = DecisionTreeClassifier(max_depth=31, random_state=0)
# udt_tree.fit(X_train, y_train)
# print("Accuracy on training set: {:.3f}".format(udt_tree.score(X_train, y_train)))
# print("Accuracy on test set: {:.3f}".format(udt_tree.score(X_test, y_test)))
# print("Feature importances:\n{}".format(udt_tree.feature_importances_))

# n_features = 12
# plt.barh(range(n_features), udt_tree.feature_importances_, align='center')
# plt.yticks(np.arange(n_features), list(df.columns)[0:-2])
# plt.xlabel("Feature importance")
# plt.ylabel("Feature")

_, X_test2, _, y_test2 = train_test_split(df_test_only[['max_peak_1', 'min_peak_1',
    'max_peak_2', 'min_peak_2', 'max_peak_3', 'min_peak_3', 'max_peak_4', 'min_peak_4',
    'max_peak_5', 'min_peak_5', 'ema_17', 'ema_72']], df_test_only['udt_status'], test_size=0.99,
    random_state=42, shuffle=True)

print(f"\nDecisionTreeClassifier: max_depth = 39")
# print("Accuracy on training set: {:.3f}".format(udt_trees[-1].score(X_train, y_train)))
print("Accuracy on test set: {:.3f}".format(udt_trees[-1].score(X_test2, y_test2)))

# %%
# KNeighborsClassifier for UDT Status

from sklearn.neighbors import KNeighborsClassifier

# knn = KNeighborsClassifier(n_neighbors=2)
# knn.fit(X_train, y_train)
# print("Test set score: {:.2f}".format(knn.score(X_test, y_test)))

training_accuracy = []
test_accuracy = []
complexity = []

for n_neighbors in range(1, 3):
    knn = KNeighborsClassifier(n_neighbors=n_neighbors)
    knn.fit(X_train, y_train)
    print(f"\nKNeighborsClassifier: n_neighbors = {n_neighbors}")
    print("Accuracy on training set: {:.3f}".format(knn.score(X_train, y_train)))
    print("Accuracy on test set: {:.3f}".format(knn.score(X_test, y_test)))
    training_accuracy.append(knn.score(X_train, y_train))
    test_accuracy.append(knn.score(X_test, y_test))
    complexity.append(n_neighbors)

plt.plot(complexity, training_accuracy, label="Training accuracy")
plt.plot(complexity, test_accuracy, label="Test accuracy")
plt.ylabel("Accuracy")
plt.xlabel("Complexity")
plt.legend()

# %%
# Min risk histogram

from scipy.stats import norm
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

n_bins = 200

df = pd.read_csv(filename, sep=',')
plt.hist(df.loc[(df['udt_status'] == 1) & (df['min_risk'] != 0.0), ['min_risk']], bins = n_bins)
# mu, std = norm.fit(df.loc[(df['udt_status'] == 1) & (df['min_risk'] != 0.0), ['min_risk']])

# xmin, xmax = plt.xlim()
# x = np.linspace(xmin, xmax, n_bins)
# p = norm.pdf(x, mu, std)
# plt.plot(x, p, 'k', linewidth=2)
# title = "Fit results: mu = %.2f,  std = %.2f" % (mu, std)
# plt.title(title)

plt.show()



# %%
