# %%
# Input parameters

min_risk = 0.05
max_risk = 0.12
gain_loss_ratio = 3
emas_tolerance = 0.015
purpose = 'test'
# purpose = 'training'

training_filename = 'features_training.csv'
test_filename = 'features_test.csv'

# Derived
filename = training_filename if purpose == 'training' else test_filename

# %%
# Get tickers and dates
import sys
from pathlib import Path
sys.path.insert(1, '/Users/atcha/Github/Projeto-Final/src')
import config_reader as cr

training_set_cfg_path = Path(__file__).parent.parent.parent/'config'/'config_training_set.json'
test_set_cfg_path = Path(__file__).parent.parent.parent/'config'/'config_test_set.json'

config_train = cr.ConfigReader(config_file_path=training_set_cfg_path)
config_test = cr.ConfigReader(config_file_path=test_set_cfg_path)
tickers = config_train.tickers_and_dates

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

peaks_number = 2
peak_delay = 9
db_model = DBStrategyAnalyzerModel()

tickers = config_train.tickers_and_dates.items() if purpose == 'training' else \
    config_test.tickers_and_dates.items()

for tck_index, (ticker, date) in enumerate(tickers):
    candles_df_day = db_model.get_ticker_prices_and_features(ticker,
        pd.Timestamp(date['start_date']), pd.Timestamp(date['end_date']),
        interval='1d')
    candles_df_wk = db_model.get_ticker_prices_and_features(ticker,
        pd.Timestamp(date['start_date']), pd.Timestamp(date['end_date']),
        interval='1wk')

    print(f"Processing Ticker {tck_index+1} of {len(config_train.tickers_and_dates)}")

    candles_len = len(candles_df_day)

    days = []
    max_peaks_1 = []
    max_peaks_2 = []
    min_peaks_1 = []
    min_peaks_2 = []
    close_prices = []
    # emas_day_flag = []
    ema_17_days = []
    ema_72_days = []
    ema_72_weeks = []
    last_max_peaks = []
    last_min_peaks = []
    operation_flag = []
    stop_losses = []
    current_max_delay = 0
    current_min_delay = 0
    upcoming_max_peak = 0.0
    upcoming_min_peak = 0.0
    end_flag = False

    for index, row in candles_df_day.iterrows():

        if index == 0:
            last_close_price = row['close_price']
            last_ema_17_day = row['ema_17']
            last_ema_72_day = row['ema_72']
            last_target_buy_price = row['target_buy_price']
            last_stop_loss = row['stop_loss']
        else:
            year, week, _ = (candles_df_day.loc[candles_df_day.index[index-1], 'day'] \
                - pd.Timedelta(days=7)).isocalendar()
            ema_72_week = candles_df_wk[(candles_df_wk['week'].dt.isocalendar().year == year) \
                    & (candles_df_wk['week'].dt.isocalendar().week == week)].tail(1) \
                    if not candles_df_wk.empty \
                    else candles_df_wk

            if ema_72_week.empty or ema_72_week['ema_72'].squeeze() == 0.0 \
                or last_stop_loss == 0.0 or last_target_buy_price == 0.0 \
                or last_ema_17_day == 0.0 or last_ema_72_day == 0.0:
                last_close_price = row['close_price']
                last_ema_17_day = row['ema_17']
                last_ema_72_day = row['ema_72']
                last_target_buy_price = row['target_buy_price']
                last_stop_loss = row['stop_loss']
                continue
            else:
                ema_72_week = ema_72_week['ema_72'].squeeze()

            open_price_day = row['open_price']
            max_price_day = row['max_price']
            peak_day = row['peak']

            current_max_delay += 1
            current_min_delay += 1

            if peak_day > 0.01:
                if peak_day == max_price_day:
                    upcoming_max_peak = peak_day
                    current_max_delay = 0
                else:
                    upcoming_min_peak = peak_day
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
                last_close_price = row['close_price']
                last_ema_17_day = row['ema_17']
                last_ema_72_day = row['ema_72']
                last_target_buy_price = row['target_buy_price']
                last_stop_loss = row['stop_loss']
                continue

            # try:
            purchase_price = open_price_day
            stop_loss_price = round(last_stop_loss * purchase_price / last_target_buy_price, 2)
            if (purchase_price - stop_loss_price) / purchase_price > max_risk:
                stop_loss_price = round(purchase_price * (1 - max_risk), 2)
            if (purchase_price - stop_loss_price) / purchase_price < min_risk:
                stop_loss_price = round(purchase_price * (1 - min_risk), 2)
            target_sale_price = round(purchase_price + gain_loss_ratio * (purchase_price - stop_loss_price), 2)
            # except Exception:
            #     print()

            max_price = target_sale_price
            min_price = purchase_price

            success_flg = None
            stop_looping_flg = False
            for index_2, row_2 in candles_df_day.loc[candles_df_day.index[
                index+1:candles_len]].iterrows():

                if row_2['max_price'] >= target_sale_price:
                    max_price = target_sale_price
                    success_flg = 1
                    break

                if row_2['min_price'] <= stop_loss_price:
                    min_price = stop_loss_price
                    success_flg = 0
                    break

                if row_2['max_price'] > max_price:
                    max_price = row_2['max_price']
                if row_2['min_price'] < min_price:
                    min_price = row_2['min_price']

            stop_loss_perc = 1 - last_stop_loss / last_target_buy_price
            if stop_loss_perc < min_risk:
                stop_loss_perc = min_risk
            elif stop_loss_perc > max_risk:
                stop_loss_perc = max_risk

            if success_flg is not None:
                max_peaks_1.append(round(last_max_peaks[0]/last_close_price, 4))
                max_peaks_2.append(round(last_max_peaks[1]/last_close_price, 4))
                min_peaks_1.append(round(last_min_peaks[0]/last_close_price, 4))
                min_peaks_2.append(round(last_min_peaks[1]/last_close_price, 4))
                # emas_day_flag.append(
                #     1 if last_close_price < max(last_ema_17_day, last_ema_72_day) * (1+emas_tolerance) \
                #     and last_close_price > min(last_ema_17_day, last_ema_72_day) * (1-emas_tolerance) else 0)
                ema_17_days.append(round(last_ema_17_day/last_close_price, 4))
                ema_72_days.append(round(last_ema_72_day/last_close_price, 4))
                ema_72_weeks.append(round(ema_72_week/last_close_price, 4))
                operation_flag.append(success_flg)
                stop_losses.append(round(stop_loss_perc, 4))
                days.append(row['day'])

            last_close_price = row['close_price']
            last_ema_17_day = row['ema_17']
            last_ema_72_day = row['ema_72']
            last_target_buy_price = row['target_buy_price']
            last_stop_loss = row['stop_loss']

            features_df = pd.DataFrame({
                'ticker': [ticker]*len(operation_flag),
                'day': days,
                'max_peak_1': max_peaks_1,
                'min_peak_1': min_peaks_1,
                'max_peak_2': max_peaks_2,
                'min_peak_2': min_peaks_2,
                # 'emas_day_flag': emas_day_flag,
                'ema_17_day': ema_17_days,
                'ema_72_day': ema_72_days,
                'ema_72_week': ema_72_weeks,
                'stop_loss': stop_losses,
                'operation_flag': operation_flag
            })

    if tck_index == 0:
        features_df.to_csv(filename, mode='a', index=False)
    else:
        features_df.to_csv(filename, mode='a', index=False, header=False)

# %%
# Load training and test sets

import pandas as pd
# from sklearn.model_selection import train_test_split

df_train = pd.read_csv(training_filename, sep=',')
df_test = pd.read_csv(test_filename, sep=',')

test_size = 0.10
X_train2, X_test2, y_train2, y_test2 = train_test_split(df_train[['max_peak_1', 'min_peak_1',
    'max_peak_2', 'min_peak_2', 'ema_17_day', 'ema_72_day', 'ema_72_week', 'stop_loss']],
    df_train['operation_flag'], test_size=test_size, random_state=42, shuffle=True)
# _, X_test2, _, y_test2 = train_test_split(df_test[['max_peak_1', 'min_peak_1',
#     'max_peak_2', 'min_peak_2', 'ema_17_day', 'ema_72_day', 'ema_72_week', 'stop_loss']],
#     df_test['operation_flag'], test_size=0.99, random_state=42, shuffle=True)

X_train = df_train[['max_peak_1', 'min_peak_1', 'max_peak_2', 'min_peak_2',
    'ema_17_day', 'ema_72_day', 'ema_72_week', 'stop_loss']]
y_train = df_train['operation_flag']

X_test = df_test[['max_peak_1', 'min_peak_1', 'max_peak_2', 'min_peak_2',
    'ema_17_day', 'ema_72_day', 'ema_72_week', 'stop_loss']]
y_test = df_test['operation_flag']


# %%
# KNeighborsClassifier

import matplotlib.pyplot as plt
from sklearn.neighbors import KNeighborsClassifier
import pandas as pd

# Parameters
n_neighbors_list = [1, 2, 3]

knn_training_accuracy = []
knn_test_accuracy = []
best_knn_traning_accuracy = 0
best_knn_test_accuracy = 0
best_knn_model = None

for n_neighbors in n_neighbors_list:
    knn = KNeighborsClassifier(n_neighbors=n_neighbors)
    knn.fit(X_train, y_train)
    print(f"\n- KNeighborsClassifier: n_neighbors = {n_neighbors}")
    print("   Accuracy on training set: {:.3f}".format(knn.score(X_train, y_train)))
    print("   Accuracy on test set: {:.3f}".format(knn.score(X_test, y_test)))
    knn_training_accuracy.append(knn.score(X_train, y_train))
    knn_test_accuracy.append(knn.score(X_test, y_test))

    if knn_test_accuracy[-1] > best_knn_test_accuracy:
        best_knn_test_accuracy = knn_test_accuracy[-1]
        best_knn_training_accuracy = knn_training_accuracy[-1]
        best_knn_model = knn

print(f"\n* Best KNeighborsClassifier: n_neighbors = {best_knn_model.n_neighbors}")
print("   Accuracy on training set: {:.3f}".format(best_knn_training_accuracy))
print("   Accuracy on test set: {:.3f}".format(best_knn_test_accuracy))

plt.plot(n_neighbors_list, knn_training_accuracy, label="Training accuracy")
plt.plot(n_neighbors_list, knn_test_accuracy, label="Test accuracy")
plt.ylabel("Accuracy")
plt.xlabel("Complexity")
plt.title("KNeighborsClassifier")
plt.legend()

# Output
# best_knn_model

# %%
# Linear Model: LogisticRegression

import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
import pandas as pd

# Parameters
c_list = [0.1, 0.5, 1, 5, 10]

logreg_training_accuracy = []
logreg_test_accuracy = []
best_logreg_traning_accuracy = 0
best_logreg_test_accuracy = 0
best_logreg_model = None

for c in c_list:
    logreg = LogisticRegression(C=c)
    logreg.fit(X_train, y_train)
    print(f"\n- LogisticRegression: C = {c}")
    print("   Accuracy on training set: {:.3f}".format(logreg.score(X_train, y_train)))
    print("   Accuracy on test set: {:.3f}".format(logreg.score(X_test, y_test)))
    logreg_training_accuracy.append(logreg.score(X_train, y_train))
    logreg_test_accuracy.append(logreg.score(X_test, y_test))

    if logreg_test_accuracy[-1] > best_logreg_test_accuracy:
        best_logreg_test_accuracy = logreg_test_accuracy[-1]
        best_logreg_training_accuracy = logreg_training_accuracy[-1]
        best_logreg_model = logreg

print(f"\n* Best LogisticRegression Model: C = {best_logreg_model.C}")
print("   Accuracy on training set: {:.3f}".format(best_logreg_training_accuracy))
print("   Accuracy on test set: {:.3f}".format(best_logreg_test_accuracy))

plt.plot(c_list, logreg_training_accuracy, label="Training accuracy")
plt.plot(c_list, logreg_test_accuracy, label="Test accuracy")
plt.ylabel("Accuracy")
plt.xlabel("Complexity")
plt.title("LogisticRegression")
plt.legend()

# %%
# Linear Model: Support Vector Machines

import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC
import pandas as pd

# Parameters
c_list = [0.1, 0.5, 1, 5, 10]

svm_training_accuracy = []
svm_test_accuracy = []
best_svm_traning_accuracy = 0
best_svm_test_accuracy = 0
best_svm_model = None

for c in c_list:
    svm = LinearSVC(C=c)
    svm.fit(X_train, y_train)
    print(f"\n- LinearSVC: C = {c}")
    print("   Accuracy on training set: {:.3f}".format(svm.score(X_train, y_train)))
    print("   Accuracy on test set: {:.3f}".format(svm.score(X_test, y_test)))
    svm_training_accuracy.append(svm.score(X_train, y_train))
    svm_test_accuracy.append(svm.score(X_test, y_test))

    if svm_test_accuracy[-1] > best_svm_test_accuracy:
        best_svm_test_accuracy = svm_test_accuracy[-1]
        best_svm_training_accuracy = svm_training_accuracy[-1]
        best_svm_model = svm

print(f"\n* Best LinearSVC Model: C = {best_svm_model.C}")
print("   Accuracy on training set: {:.3f}".format(best_svm_training_accuracy))
print("   Accuracy on test set: {:.3f}".format(best_svm_test_accuracy))

plt.plot(c_list, svm_training_accuracy, label="Training accuracy")
plt.plot(c_list, svm_test_accuracy, label="Test accuracy")
plt.ylabel("Accuracy")
plt.xlabel("Complexity")
plt.title("LinearSVC")
plt.legend()

# %%
# Naive Bayes: BernoulliNB

import matplotlib.pyplot as plt
from sklearn.naive_bayes import BernoulliNB
from sklearn.svm import LinearSVC
import pandas as pd

bnb = BernoulliNB()
bnb.fit(X_train, y_train)
print(f"\n- BernoulliNB:")
print("   Accuracy on training set: {:.3f}".format(bnb.score(X_train, y_train)))
print("   Accuracy on test set: {:.3f}".format(bnb.score(X_test, y_test)))


# %%
# DecisionTreeClassifier

import numpy as np
import pandas as pd
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
# import joblib

# Parameters
depth_list = [i for i in range(2, 40, 2)]

udt_tree_training_accuracy = []
udt_tree_test_accuracy = []
best_udt_tree_traning_accuracy = 0
best_udt_tree_test_accuracy = 0
best_udt_tree_model = None

for depth in depth_list:
    udt_tree = DecisionTreeClassifier(max_depth=depth, random_state=0)
    udt_tree.fit(X_train, y_train)
    print(f"\n- DecisionTreeClassifier: max_depth = {depth}")
    print("   Accuracy on training set: {:.3f}".format(udt_tree.score(X_train, y_train)))
    print("   Accuracy on test set: {:.3f}".format(udt_tree.score(X_test, y_test)))
    udt_tree_training_accuracy.append(udt_tree.score(X_train, y_train))
    udt_tree_test_accuracy.append(udt_tree.score(X_test, y_test))

    if udt_tree_test_accuracy[-1] > best_udt_tree_test_accuracy:
        best_udt_tree_test_accuracy = udt_tree_test_accuracy[-1]
        best_udt_tree_training_accuracy = udt_tree_training_accuracy[-1]
        best_udt_tree_model = udt_tree
    # Save model
    # if depth == max_depth-1:
    #     joblib.dump(udt_tree, 'model.joblib')

print(f"\n* Best DecisionTreeClassifier: n_neighbors = {best_udt_tree_model.max_depth}")
print("   Accuracy on training set: {:.3f}".format(best_udt_tree_training_accuracy))
print("   Accuracy on test set: {:.3f}".format(best_udt_tree_test_accuracy))

plt.plot(depth_list, udt_tree_training_accuracy, label="Training accuracy")
plt.plot(depth_list, udt_tree_test_accuracy, label="Test accuracy")
plt.ylabel("Accuracy")
plt.xlabel("Complexity")
plt.title("DecisionTreeClassifier")
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
