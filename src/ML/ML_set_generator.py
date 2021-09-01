# %%
# Input parameters

min_risk = 0.05
max_risk = 0.20
gain_loss_ratio = 3
emas_tolerance = 0.015
# purpose = 'test'
purpose = 'training'
compact_mode = False

MAX_DAYS_PER_OPERATION = 120

training_filename = 'features_training.csv'
test_filename = 'features_test.csv'

# Derived
filename = training_filename if purpose == 'training' else test_filename

print(f"max_risk: {max_risk}")
print(f"gain_loss_ratio: {gain_loss_ratio}")
print(f"purpose: {purpose}")
print(f"compact_mode: {compact_mode}")
print(f"MAX_DAYS_PER_OPERATION: {MAX_DAYS_PER_OPERATION}")

# %%
# Get tickers and dates
import sys
from pathlib import Path
sys.path.insert(1, '/Users/atcha/Github/Projeto-Final/src')
import config_reader as cr

if compact_mode == False:
    training_set_cfg_path = Path(__file__).parent.parent.parent/'config'/'config_training_set.json'
    test_set_cfg_path = Path(__file__).parent.parent.parent/'config'/'config_test_set.json'
else:
    training_set_cfg_path = Path(__file__).parent.parent.parent/'config'/'config_training_set_compact.json'
    test_set_cfg_path = Path(__file__).parent.parent.parent/'config'/'config_test_set_compact.json'


config_train = cr.ConfigReader(config_file_path=training_set_cfg_path)
config_test = cr.ConfigReader(config_file_path=test_set_cfg_path)
tickers = config_train.tickers_and_dates if purpose == 'training' else \
    config_test.tickers_and_dates

len_of_tickers = len(config_train.tickers_and_dates) if purpose == 'training' else \
    len(config_test.tickers_and_dates)

print(f'Tickers ({len_of_tickers}):')
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

for tck_index, (ticker, date) in enumerate(tickers.items()):
    candles_df_day = db_model.get_ticker_prices_and_features(ticker,
        pd.Timestamp(date['start_date']), pd.Timestamp(date['end_date']),
        interval='1d')
    candles_df_wk = db_model.get_ticker_prices_and_features(ticker,
        pd.Timestamp(date['start_date']), pd.Timestamp(date['end_date']),
        interval='1wk')

    print(f"Processing Ticker {tck_index+1} of {len_of_tickers}")

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
    operation_flag_2 = [] # used for stop loss optimization
    stop_losses = []
    best_stop_losses = []
    best_sl_days = []   # companion for best_stop_losses
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
            days_counter = 0

            for index_2, row_2 in candles_df_day.loc[candles_df_day.index[
                index+1:candles_len]].iterrows():

                if row_2['min_price'] <= stop_loss_price:
                    min_price = stop_loss_price
                    success_flg = 0
                    break

                if row_2['max_price'] >= target_sale_price:
                    max_price = target_sale_price
                    success_flg = 1
                    break

                if row_2['max_price'] > max_price:
                    max_price = row_2['max_price']
                if row_2['min_price'] < min_price:
                    min_price = row_2['min_price']

                days_counter += 1
                if days_counter >= MAX_DAYS_PER_OPERATION:
                    success_flg = 0
                    break

            # *************** Testing best stop loss **************
            if success_flg is not None:
                min_price_2 = purchase_price
                max_price_2 = purchase_price
                success_flg_2 = 0   # Mean failure
                days_counter_2 = 0
                best_stop_loss = 0
                best_yield = 0.0
                best_time_interval = 0
                cur_daily_yield = 0.0

                quit_price = round(purchase_price * (1 - max_risk), 2)
                target_price = 0.0

                for index_3, row_3 in candles_df_day.loc[candles_df_day.index[
                    index+1:candles_len]].iterrows():

                    changed_target_today = False

                    if row_3['min_price'] <= quit_price:
                        min_price_2 = quit_price
                        break

                    if row_3['min_price'] < min_price_2:
                        min_price_2 = row_3['min_price']
                        target_price = round(purchase_price + gain_loss_ratio * (purchase_price - min_price_2), 2)
                        changed_target_today = True
                    if row_3['max_price'] > max_price_2 and target_price != 0.0 and changed_target_today == False:
                        max_price_2 = row_3['max_price']

                    if target_price != 0.0 and max_price_2 >= target_price and changed_target_today == False:
                        cur_daily_yield = (target_price / purchase_price) ** (1 / (days_counter_2 + 1))
                        if cur_daily_yield > best_yield and days_counter_2 > 1:
                            best_yield = cur_daily_yield
                            best_time_interval = days_counter_2 + 1
                            best_stop_loss = round(1 - min_price_2 / purchase_price, 4)
                            success_flg_2 = 1

                    days_counter_2 += 1
                    if days_counter_2 >= MAX_DAYS_PER_OPERATION:
                        break
            # *****************************************************

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
                # stop_losses.append(round(stop_loss_perc, 4))
                best_stop_losses.append(best_stop_loss)
                best_sl_days.append(best_time_interval)
                operation_flag_2.append(success_flg_2)
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
                # 'stop_loss': stop_losses,
                'best_stop_losses': best_stop_losses,
                'best_sl_days': best_sl_days,
                # 'operation_flag': operation_flag,
                'operation_flag_2': operation_flag_2
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

# Output
# best_logreg_model


# %%
# Linear Model: Support Vector Machines

import matplotlib.pyplot as plt
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

# Output
# best_svm_model


# %%
# Naive Bayes: BernoulliNB

import matplotlib.pyplot as plt
from sklearn.naive_bayes import BernoulliNB
from sklearn.svm import LinearSVC
import pandas as pd

bnb_model = BernoulliNB()
bnb_model.fit(X_train, y_train)
print(f"\n- BernoulliNB:")
print("   Accuracy on training set: {:.3f}".format(bnb_model.score(X_train, y_train)))
print("   Accuracy on test set: {:.3f}".format(bnb_model.score(X_test, y_test)))


# Output
# bnb_model


# %%
# DecisionTreeClassifier

import pandas as pd
from sklearn.tree import DecisionTreeClassifier
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

print(f"\n* Best DecisionTreeClassifier: max_depth = {best_udt_tree_model.max_depth}")
print("   Accuracy on training set: {:.3f}".format(best_udt_tree_training_accuracy))
print("   Accuracy on test set: {:.3f}".format(best_udt_tree_test_accuracy))

plt.plot(depth_list, udt_tree_training_accuracy, label="Training accuracy")
plt.plot(depth_list, udt_tree_test_accuracy, label="Test accuracy")
plt.ylabel("Accuracy")
plt.xlabel("Complexity")
plt.title("DecisionTreeClassifier")
plt.legend()

# Output
# best_udt_tree_model


# %%
# RandomForestClassifier

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import matplotlib.pyplot as plt
import joblib

plots_per_column = 3

# Parameters
max_features = 8
n_estimators_list = [i for i in range(1, max_features+1, 1)]
depth_list = [i for i in range(2, 20, 2)]

rnd_frt_training_accuracy = []
rnd_frt_test_accuracy = []
best_rnd_frt_traning_accuracy = 0
best_rnd_frt_test_accuracy = 0
best_rnd_frt_model = None

for n_estimator in n_estimators_list:
    for depth in depth_list:
        rnd_frt = RandomForestClassifier(n_estimators=n_estimator, max_depth=depth,
            random_state=0, n_jobs=-1)
        rnd_frt.fit(X_train, y_train)
        print(f"\n- RandomForestClassifier: n_estimators = {n_estimator}, max_depth = {depth}")
        print("   Accuracy on training set: {:.3f}".format(rnd_frt.score(X_train, y_train)))
        print("   Accuracy on test set: {:.3f}".format(rnd_frt.score(X_test, y_test)))
        rnd_frt_training_accuracy.append(rnd_frt.score(X_train, y_train))
        rnd_frt_test_accuracy.append(rnd_frt.score(X_test, y_test))

        if rnd_frt_test_accuracy[-1] > best_rnd_frt_test_accuracy:
            best_rnd_frt_test_accuracy = rnd_frt_test_accuracy[-1]
            best_rnd_frt_training_accuracy = rnd_frt_training_accuracy[-1]
            best_rnd_frt_model = rnd_frt

print(f"\n* Best RandomForestClassifier: n_estimators = {best_rnd_frt_model.n_estimators}, " \
    f"max_depth = {depth}")
print("   Accuracy on training set: {:.3f}".format(best_rnd_frt_training_accuracy))
print("   Accuracy on test set: {:.3f}".format(best_rnd_frt_test_accuracy))

fig, ax = plt.subplots(len(n_estimators_list), sharex=True, sharey=True, figsize=(4, 7))
# fig, ax = plt.subplots(nrows=plots_per_column, ncols=len(n_estimators_list) // plots_per_column + 1,
#     sharex=True, sharey=True, figsize=(4, 7))
index = 0
plt.tight_layout()

# Save model
joblib.dump(best_rnd_frt_model, 'best_rnd_frt_model.joblib')

for i in range(0, len(n_estimators_list)):
    ax[i].plot(depth_list, rnd_frt_training_accuracy[index:index+len(depth_list)], label="Training accuracy")
    ax[i].plot(depth_list, rnd_frt_test_accuracy[index:index+len(depth_list)], label="Test accuracy")
    ax[i].set_title(f"RandomForestClassifier: n_estimators={n_estimators_list[i]}")
    ax[i].set_xlabel("Max Depth")
    ax[i].set_ylabel("Accuracy")
    ax[i].legend()
    # ax[i % plots_per_column, i // plots_per_column].plot(depth_list, rnd_frt_training_accuracy[index:index+len(depth_list)], label="Training accuracy")
    # ax[i % plots_per_column, i // plots_per_column].plot(depth_list, rnd_frt_test_accuracy[index:index+len(depth_list)], label="Test accuracy")
    # ax[i % plots_per_column, i // plots_per_column].set_title(f"RandomForestClassifier: n_estimators={n_estimators_list[i]}")
    # ax[i % plots_per_column, i // plots_per_column].set_xlabel("Max Depth")
    # ax[i % plots_per_column, i // plots_per_column].set_ylabel("Accuracy")
    # ax[i % plots_per_column, i // plots_per_column].legend()
    # fig.suptitle("RandomForestClassifier: n_estimators={n_estimator}")
    # fig.ylabel("Accuracy")
    # fig.xlabel("Max Depth")
    # fig.legend()

    index += len(depth_list)

# plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.tight_layout()

# plt.plot(n_estimators_list, rnd_frt_training_accuracy, label="Training accuracy")
# plt.plot(n_estimators_list, rnd_frt_test_accuracy, label="Test accuracy")
# plt.ylabel("Accuracy")
# plt.xlabel("Max Depth")
# plt.title("RandomForestClassifier")
# plt.legend()

# Output
# best_rnd_frt_model


# %%
# MLPClassifier

import pandas as pd
from sklearn.neural_network import MLPClassifier
import matplotlib.pyplot as plt

# Parameters
# hidden_layer_sizes_list = [i for i in range(10, 30, 10)]
hidden_layer_sizes_list = [[10, 10], [15, 15]]
solver = 'lbfgs'

mlpc_training_accuracy = []
mlpc_test_accuracy = []
best_mlpc_traning_accuracy = 0
best_mlpc_test_accuracy = 0
best_mlpc_model = None

for hidden_layer_sizes in hidden_layer_sizes_list:
    mlpc = MLPClassifier(solver=solver, random_state=0, hidden_layer_sizes=hidden_layer_sizes)
    mlpc.fit(X_train, y_train)
    print(f"\n- MLPClassifier: solver = {solver}, hidden_layer_sizes = {hidden_layer_sizes}")
    print("   Accuracy on training set: {:.3f}".format(mlpc.score(X_train, y_train)))
    print("   Accuracy on test set: {:.3f}".format(mlpc.score(X_test, y_test)))
    mlpc_training_accuracy.append(mlpc.score(X_train, y_train))
    mlpc_test_accuracy.append(mlpc.score(X_test, y_test))

    if mlpc_test_accuracy[-1] > best_mlpc_test_accuracy:
        best_mlpc_test_accuracy = mlpc_test_accuracy[-1]
        best_mlpc_training_accuracy = mlpc_training_accuracy[-1]
        best_mlpc_model = mlpc

print(f"\n* Best MLPClassifier: solver = {best_mlpc_model.solver}")
print("   Accuracy on training set: {:.3f}".format(best_mlpc_training_accuracy))
print("   Accuracy on test set: {:.3f}".format(best_mlpc_test_accuracy))

plt.plot(hidden_layer_sizes_list, mlpc_training_accuracy, label="Training accuracy")
plt.plot(hidden_layer_sizes_list, mlpc_test_accuracy, label="Test accuracy")
plt.ylabel("Accuracy")
plt.xlabel("Complexity")
plt.title("MLPClassifier")
plt.legend()

# Output
# best_mlpc_model


# %%
# Stop Loss analysis histogram

import matplotlib.pyplot as plt
import numpy as np
import math

ticker = 'MGLU3'

n_bins = 15
sl_on_success = df_train.loc[(df_train['ticker'] == ticker) & (df_train['operation_flag_2'] == 1), ['best_stop_losses']]
days_on_success = df_train.loc[(df_train['ticker'] == ticker) & (df_train['operation_flag_2'] == 1), ['best_sl_days']]

(n, bins, patches) = plt.hist(sl_on_success, bins=n_bins, color='b')
plt.title('Stop Loss on Success Histogram')

days_per_sl = [None] * (len(bins) - 1)
max_days_per_sl = [None] * (len(bins) - 1)
min_days_per_sl = [None] * (len(bins) - 1)
avg_days_per_sl = [None] * (len(bins) - 1)
sl_per_sl = [None] * (len(bins) - 1)
avg_sl_per_sl = [None] * (len(bins) - 1)
avg_daily_yield = [None] * (len(bins) - 1)

for i in range(0, len(bins)-1):
    days_per_sl[i] = df_train.loc[(df_train['ticker'] == ticker) & (df_train['best_stop_losses'] >= bins[i]) \
        & (df_train['best_stop_losses'] < bins[i+1]) & (df_train['operation_flag_2'] == 1), ['best_sl_days']].squeeze()
    sl_per_sl[i] = df_train.loc[(df_train['ticker'] == ticker) & (df_train['best_stop_losses'] >= bins[i]) \
        & (df_train['best_stop_losses'] < bins[i+1]) & (df_train['operation_flag_2'] == 1), ['best_stop_losses']].squeeze()
    avg_days_per_sl[i] = round(np.average(days_per_sl[i]), 1)
    avg_sl_per_sl[i] = round(np.average(sl_per_sl[i]), 4)
    avg_daily_yield[i] = round((1 + gain_loss_ratio * avg_sl_per_sl[i]) ** (1 / avg_days_per_sl[i]), 4)

for x, y, days in zip(bins, n, avg_days_per_sl):
    days = f"{days} d"
    plt.text(x, y+3, days, fontsize=11, rotation=-90)

# Remove values after first occurence of nan
find_nan = False
for i in range(0, len(avg_daily_yield)):
    if math.isnan(avg_daily_yield[i]):
        find_nan = True
    if find_nan == True:
        avg_daily_yield[i] = np.nan

# %%

plt.plot(avg_sl_per_sl, avg_daily_yield, label="Raw")
plt.title('Average Daily Yield per Bin')

window_size = n_bins // 3

numbers_series = pd.Series(avg_daily_yield)
windows = numbers_series.rolling(window_size)
moving_averages = windows.mean()

moving_averages_list = moving_averages.tolist()
without_nans = moving_averages_list[window_size - 1:]
without_nans = [round(num, 4) for num in without_nans]

plt.plot(avg_sl_per_sl[window_size//2:-(window_size//2)], without_nans, label=f"Moving avg n={window_size}")
plt.legend()

print(f"Best Risk-Capital Coefficient for \'{ticker}\': {avg_sl_per_sl[without_nans.index(max(without_nans))+window_size//2]}")

# %%
# Create file of best RCC coefficients

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import math

# tickers_rcc = {tck: np.nan for tck in tickers}
tcks = [tck for tck, _ in tickers.items()]
rccs = []
n_bins = 15

days_per_sl = [None] * (len(bins) - 1)
max_days_per_sl = [None] * (len(bins) - 1)
min_days_per_sl = [None] * (len(bins) - 1)
avg_days_per_sl = [None] * (len(bins) - 1)
sl_per_sl = [None] * (len(bins) - 1)
avg_sl_per_sl = [None] * (len(bins) - 1)
avg_daily_yield = [None] * (len(bins) - 1)

for tck_index, (ticker, date) in enumerate(tickers.items()):

    sl_on_success = df_train.loc[(df_train['ticker'] == ticker) & (df_train['operation_flag_2'] == 1), ['best_stop_losses']]
    (n, bins, patches) = plt.hist(sl_on_success, bins=n_bins, color='b')
    plt.title(f'{ticker} - Stop Loss on Success Histogram')

    for i in range(0, len(bins)-1):
        days_per_sl[i] = df_train.loc[(df_train['ticker'] == ticker) & (df_train['best_stop_losses'] >= bins[i]) \
            & (df_train['best_stop_losses'] < bins[i+1]) & (df_train['operation_flag_2'] == 1), ['best_sl_days']].squeeze()
        sl_per_sl[i] = df_train.loc[(df_train['ticker'] == ticker) & (df_train['best_stop_losses'] >= bins[i]) \
            & (df_train['best_stop_losses'] < bins[i+1]) & (df_train['operation_flag_2'] == 1), ['best_stop_losses']].squeeze()
        avg_days_per_sl[i] = round(np.average(days_per_sl[i]), 1)
        avg_sl_per_sl[i] = round(np.average(sl_per_sl[i]), 4)
        avg_daily_yield[i] = round((1 + gain_loss_ratio * avg_sl_per_sl[i]) ** (1 / avg_days_per_sl[i]), 4)

    # Remove values after first occurence of nan
    find_nan = False
    for i in range(0, len(avg_daily_yield)):
        if math.isnan(avg_daily_yield[i]):
            find_nan = True
        if find_nan == True:
            avg_daily_yield[i] = np.nan

    window_size = n_bins // 3

    numbers_series = pd.Series(avg_daily_yield)
    windows = numbers_series.rolling(window_size)
    moving_averages = windows.mean()

    moving_averages_list = moving_averages.tolist()
    without_nans = moving_averages_list[window_size - 1:]
    without_nans = [round(num, 4) for num in without_nans]

    best_rcc = avg_sl_per_sl[without_nans.index(max(without_nans))+window_size//2]
    print(f"Best Risk-Capital Coefficient for \'{ticker}\': {best_rcc}")

    rccs.append(best_rcc)

rcc_df = pd.DataFrame({'ticker': tcks, 'rcc': rccs})
rcc_df.to_csv('tickers_rcc.csv', mode='a', index=False)

# %%
