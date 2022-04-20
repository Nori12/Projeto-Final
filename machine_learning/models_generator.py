import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import pandas as pd
from multiprocessing import Pool
import time
from tqdm import tqdm
import psutil
import argparse
import itertools
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import MinMaxScaler
from imblearn.under_sampling import TomekLinks
from imblearn.over_sampling import SMOTE
import numpy as np

sys.path.insert(1, str(Path(__file__).parent.parent/'src'))
import constants as c
import config_reader as cr
from utils import my_dynamic_cast, my_to_list, remove_row_from_last_n_peaks
import ml_constants as mlc
from model import KNeighbors, RandomForest, MLP_scikit, MLP_keras

# Configure Logging
logger = logging.getLogger(__name__)
log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME
file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

pbar = None

def manage_ticker_models(model_type, ticker, input_features, output_feature, X_train,
    y_train, X_test, y_test, datasets_info, models_dir, models_params=None,
    variable_params=None):

    # Output store variables
    models = []
    training_accuracies = []
    training_confusions = []
    training_profit_indexes = []
    training_profit_indexes_zeros = []
    test_accuracies = []
    test_confusions = []
    test_profit_indexes = []
    test_profit_indexes_zeros = []

    if models_params is None:
        models_params = {}

    if variable_params is None:
        variable_params = []

    if model_type == 'MLPClassifier':

        for model_params in models_params:
            my_model = MLP_scikit(ticker=ticker, input_features=input_features,
                output_feature=output_feature, X_train=X_train, y_train=y_train,
                X_test=X_test, y_test=y_test, model_dir=models_dir, parameters=model_params)

            my_model.create_model()

            models.append(my_model)
            training_accuracies.append(my_model.get_accuracy(X_train, y_train))
            test_accuracies.append(my_model.get_accuracy(X_test, y_test))

            training_confusions.append(my_model.get_confusion(X_train, y_train))
            test_confusions.append(my_model.get_confusion(X_test, y_test))

            index, zero = my_model.get_profit_index(X_train, y_train)
            training_profit_indexes.append(index)
            training_profit_indexes_zeros.append(zero)

            index, zero = my_model.get_profit_index(X_test, y_test)
            test_profit_indexes.append(index)
            test_profit_indexes_zeros.append(zero)

        idx_of_best = test_profit_indexes.index(max(test_profit_indexes))
        models[idx_of_best].save()

        MLP_scikit.save_results(model_type, ticker, models_params, variable_params,
            y_train, y_test, datasets_info, training_accuracies, test_accuracies,
            training_confusions, test_confusions, models[idx_of_best].specs_dir,
            training_profit_indexes, training_profit_indexes_zeros, test_profit_indexes,
            test_profit_indexes_zeros)

    elif model_type == 'MLPKerasClassifier':

        for model_params in models_params:
            my_model = MLP_keras(ticker=ticker, input_features=input_features,
                output_feature=output_feature, X_train=X_train, y_train=y_train,
                X_test=X_test, y_test=y_test, model_dir=models_dir, parameters=model_params)

            my_model.create_model()

            models.append(my_model)
            training_accuracies.append(my_model.get_accuracy(X_train, y_train))
            test_accuracies.append(my_model.get_accuracy(X_test, y_test))

            training_confusions.append(my_model.get_confusion(X_train, y_train))
            test_confusions.append(my_model.get_confusion(X_test, y_test))

            index, zero = my_model.get_profit_index(X_train, y_train)
            training_profit_indexes.append(index)
            training_profit_indexes_zeros.append(zero)

            index, zero = my_model.get_profit_index(X_test, y_test)
            test_profit_indexes.append(index)
            test_profit_indexes_zeros.append(zero)

        idx_of_best = test_profit_indexes.index(max(test_profit_indexes))
        models[idx_of_best].save()

        MLP_keras.save_results(model_type, ticker, models_params, variable_params,
            y_train, y_test, datasets_info, training_accuracies, test_accuracies,
            training_confusions, test_confusions, models[idx_of_best].specs_dir,
            training_profit_indexes, training_profit_indexes_zeros, test_profit_indexes,
            test_profit_indexes_zeros)

    elif model_type == 'RandomForestClassifier':

        for model_params in models_params:
            my_model = RandomForest(ticker=ticker, input_features=input_features,
                output_feature=output_feature, X_train=X_train, y_train=y_train,
                X_test=X_test, y_test=y_test, model_dir=models_dir, parameters=model_params)

            my_model.create_model()

            models.append(my_model)
            training_accuracies.append(my_model.get_accuracy(X_train, y_train))
            test_accuracies.append(my_model.get_accuracy(X_test, y_test))

            training_confusions.append(my_model.get_confusion(X_train, y_train))
            test_confusions.append(my_model.get_confusion(X_test, y_test))

            index, zero = my_model.get_profit_index(X_train, y_train)
            training_profit_indexes.append(index)
            training_profit_indexes_zeros.append(zero)

            index, zero = my_model.get_profit_index(X_test, y_test)
            test_profit_indexes.append(index)
            test_profit_indexes_zeros.append(zero)

        idx_of_best = test_profit_indexes.index(max(test_profit_indexes))
        models[idx_of_best].save()
        models[idx_of_best].save_auxiliary_files()

        RandomForest.save_results(model_type, ticker, models_params, variable_params,
            y_train, y_test, datasets_info, training_accuracies, test_accuracies,
            training_confusions, test_confusions, models[idx_of_best].specs_dir,
            training_profit_indexes, training_profit_indexes_zeros, test_profit_indexes,
            test_profit_indexes_zeros)

    elif model_type == 'KNeighborsClassifier':

        for model_params in models_params:
            my_model = KNeighbors(ticker=ticker, input_features=input_features,
                output_feature=output_feature, X_train=X_train, y_train=y_train,
                X_test=X_test, y_test=y_test, model_dir=models_dir, parameters=model_params)

            my_model.create_model()

            models.append(my_model)
            training_accuracies.append(my_model.get_accuracy(X_train, y_train))
            test_accuracies.append(my_model.get_accuracy(X_test, y_test))

            training_confusions.append(my_model.get_confusion(X_train, y_train))
            test_confusions.append(my_model.get_confusion(X_test, y_test))

            index, zero = my_model.get_profit_index(X_train, y_train)
            training_profit_indexes.append(index)
            training_profit_indexes_zeros.append(zero)

            index, zero = my_model.get_profit_index(X_test, y_test)
            test_profit_indexes.append(index)
            test_profit_indexes_zeros.append(zero)

        idx_of_best = test_profit_indexes.index(max(test_profit_indexes))
        models[idx_of_best].save()

        KNeighbors.save_results(model_type, ticker, models_params, variable_params,
            y_train, y_test, datasets_info, training_accuracies, test_accuracies,
            training_confusions, test_confusions, models[idx_of_best].specs_dir,
            training_profit_indexes, training_profit_indexes_zeros, test_profit_indexes,
            test_profit_indexes_zeros)


def filter_dataset(training_df, test_df, input_features, output_feature,
    filter_data_risk_margin=0.03, sampling_method='CSL', scaling_method=None):

    if scaling_method is not None \
        and scaling_method not in ('standard', 'robust', 'min_max'):
        print("Invalid scaling method.")
        sys.exit()

    if sampling_method not in ('CSL', 'oversample'):
        print("Invalid sampling method.")
        sys.exit()

    # Filtering unecessary datapoints
    risks = tuple(np.sort(training_df['risk'].squeeze().unique()))
    risk_histogram = [len(training_df[(training_df[output_feature]==0) & \
        (training_df['risk'] == risk)]) for risk in risks]

    filter_risk_threshold = (1 - filter_data_risk_margin) * \
        (len(training_df) // len(risks))
    delete_risks = [risks[idx] for idx, count in enumerate(risk_histogram) \
        if count > filter_risk_threshold]

    training_df = training_df[~training_df['risk'].isin(delete_risks)]

    X_train = training_df[input_features]
    y_train = training_df[output_feature]

    columns = X_train.columns.tolist()
    columns.append(output_feature)

    # Almost irrelevant undersampling
    under_tomek = TomekLinks(sampling_strategy='majority')
    X_train, y_train = under_tomek.fit_resample(X_train, y_train)

    if sampling_method == 'oversample':
        # Oversampling
        over_smote = SMOTE(sampling_strategy='minority', random_state=1, k_neighbors=5)
        X_train, y_train = over_smote.fit_resample(X_train, y_train)

    if scaling_method is not None:
        scaler = None
        if scaling_method == 'standard':
            scaler = StandardScaler(copy=True, with_mean=True, with_std=True)
        elif scaling_method == 'robust':
            scaler = RobustScaler(with_centering=True, with_scaling=True,
                quantile_range=(25.0, 75.0), copy=True, unit_variance=False)
        elif scaling_method == 'min_max':
            scaler = MinMaxScaler(feature_range=(0,1), copy=True, clip=False)

        scaler.fit(X_train)
        training_df_scaled = scaler.transform(X_train)
        test_df_scaled = scaler.transform(test_df.drop(output_feature, axis=1))

        X_train_scaled = pd.concat([training_df_scaled, y_train], axis=1)
        X_train_scaled.columns = columns

        X_test_formated = pd.concat([test_df_scaled, y_test], axis=1)
        X_test_formated.columns = columns

        return X_train_scaled, X_test_formated
    else:
        X_train_scaled = pd.concat([X_train, y_train], axis=1)

    X_train_scaled.columns = columns

    return X_train_scaled, test_df

def load_dataset(ticker, min_date_filter, max_date_filter, datasets_dir, input_features,
    output_feature, sampling_method='CSL', scaling_method=None, test_set_ratio=0.2):

    datasets_info = {}
    file_path = datasets_dir / (ticker + mlc.DATASET_FILE_SUFFIX)
    filter_only_columns = ['day', 'end_of_interval_flag']
    columns = filter_only_columns.copy()
    columns.extend([output_feature])
    columns.extend(input_features)

    df = pd.read_csv(file_path, sep=',', usecols=columns)

    if min_date_filter is not None:
        df = df[df['day'] >= min_date_filter]
    if max_date_filter is not None:
        df = df[df['day'] <= max_date_filter]

    # End of interval set rows may pollute the models
    df.drop(df[df['end_of_interval_flag'] == 1].index, inplace=True)

    training_df, test_df = train_test_split(df, test_size=test_set_ratio, shuffle=False)

    training_df.reset_index(drop=True, inplace=True)
    test_df.reset_index(drop=True, inplace=True)

    training_df = remove_row_from_last_n_peaks(training_df)

    datasets_info['ticker'] = ticker
    datasets_info['training_set_start_date'] = training_df['day'].head(1).squeeze()
    datasets_info['training_set_end_date'] = training_df['day'].tail(1).squeeze()
    datasets_info['test_set_start_date'] = test_df['day'].head(1).squeeze()
    datasets_info['test_set_end_date'] = test_df['day'].tail(1).squeeze()

    # Drop filter-only columns
    training_df.drop(filter_only_columns, axis=1, inplace=True)
    test_df.drop(filter_only_columns, axis=1, inplace=True)

    filter_dataset(training_df, test_df, input_features, output_feature,
        filter_data_risk_margin=0.03, sampling_method=sampling_method,
        scaling_method=scaling_method)

    training_df.reset_index(drop=True, inplace=True)
    test_df.reset_index(drop=True, inplace=True)

    datasets_info['training_set_class_0'] = len(training_df[training_df[output_feature] == 0])
    datasets_info['training_set_class_1'] = len(training_df[training_df[output_feature] == 1])
    datasets_info['test_set_class_0'] = len(test_df[test_df[output_feature] == 0])
    datasets_info['test_set_class_1'] = len(test_df[test_df[output_feature] == 1])

    return training_df[input_features].to_numpy(), training_df[output_feature].to_numpy(), \
        test_df[input_features].to_numpy(), test_df[output_feature].to_numpy(), \
        datasets_info


if __name__ == '__main__':

    print('Model Generator started.')

    datasets_dir = Path(__file__).parent / mlc.DATASETS_FOLDER

    # Parse args
    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--pools", type=int,
        help="Number of worker processes to run code in parallel.")
    parser.add_argument("-s", "--start-on-ticker", type=int, default=1,
        help="Number of ticker to start. Default is 1.")
    parser.add_argument("-t", "--max-tickers", type=int, default=None,
        help="Maximum number of tickers to evaluate.")
    parser.add_argument("-e", "--end-on-ticker", type=int, default=None,
        help="Number of ticker to end.")
    parser.add_argument("-m", "--model", default='RandomForestClassifier',
        choices=['MLPClassifier', 'MLPKerasClassifier', 'RandomForestClassifier', 'KNeighborsClassifier'],
        help="Machine learning model to be created. Default is \"MLPClassifier\".")
    parser.add_argument("-a", "--params",
        help="Substitute default parameters. String of 'key=value' semicolon " \
        f"separated values for model creation. Ex: \"param1=1;param2=2\".")
    parser.add_argument("-o", "--output-feature", default='success_oper_flag',
        help="Dataset consumed output feature name. Default is \"success_oper_flag\".")
    parser.add_argument("-i", "--input-features", default=None,
        help="Input features name list (python compatible). Default is \"['risk', " \
            f"'peak_1', 'day_1', 'peak_2', 'day_2', 'peak_3', 'day_3', 'peak_4', " \
            f"'day_4', 'ema_17_day', 'ema_72_day', 'ema_72_week']\".")
    parser.add_argument("-d", "--sampling-method", default='CSL',
        choices=['oversample', 'CSL'],
        help="Sampling method for handling imbalanced datasets. Default is \"CSL\", " \
            f"but if not possible it is \"oversample\".")
    parser.add_argument("-c", "--scaling-method", default = None,
        choices=['standard', 'robust', 'min_max'],
        help="Method for dataset feature scaling, if necessary.")

    args = parser.parse_args()

    # ************************ Check 'pools' argument **************************
    max_pools = psutil.cpu_count(logical=False)

    if args.pools is not None:
        max_pools = args.pools

    print(f"Using maximum of {max_pools} worker processes.")
    # **************************************************************************

    # **** Check 'start_on_ticker', 'max_tickers', 'end_on_ticker' arguments ***
    cfg_path = Path(__file__).parent / 'config.json'
    config_reader = cr.ConfigReader(config_file_path=cfg_path)
    tickers_and_dates = config_reader.tickers_and_dates

    if len(tickers_and_dates) < 1:
        print("Config file must have at least one ticker to evaluate.")
        sys.exit()

    start_on_ticker = 1
    max_tickers = None
    end_on_ticker = len(tickers_and_dates)

    if args.start_on_ticker is not None:
        if args.start_on_ticker < 1:
            print("'start_on_ticker' minimum value is 1.")
            sys.exit()
        elif args.start_on_ticker > len(tickers_and_dates):
            print(f"'start_on_ticker' maximum value is {len(tickers_and_dates)} " \
                f"due to Config file.")
            sys.exit()
        else:
            start_on_ticker = args.start_on_ticker

    if args.max_tickers is not None:
        if args.max_tickers < 1:
            print("'max_tickers' minimum value is 1.")
            sys.exit()
        else:
            max_tickers = args.max_tickers
            end_on_ticker = start_on_ticker + min(max_tickers - 1, len(tickers_and_dates))

    if args.end_on_ticker is not None:
        if args.end_on_ticker < 1:
            print("'end_on_ticker' minimum value is 1.")
            sys.exit()
        elif args.end_on_ticker < start_on_ticker:
            print("'end_on_ticker' must be greater than or equal to " \
                "'start_on_ticker'.")
            sys.exit()
        elif args.end_on_ticker > len(tickers_and_dates):
            print(f"'end_on_ticker' maximum value is {len(tickers_and_dates)} " \
                f"due to Config file.")
            sys.exit()

        if max_tickers is not None:
            end_on_ticker = min(args.end_on_ticker, start_on_ticker + max_tickers - 1)
        else:
            end_on_ticker = args.end_on_ticker

    # Filter tickers_and_dates to only get tickers to evaluate
    tickers = {}
    for idx, (ticker, dates) in enumerate(tickers_and_dates.items()):
        if idx + 1 >= start_on_ticker and idx + 1 <= end_on_ticker:
            tickers[ticker] = dates.copy()
    # **************************************************************************

    # ************************ Check 'model' argument **************************
    model_type = args.model
    # **************************************************************************

    # ********** Check 'input_features' and 'output_feature' arguments *********
    input_features = ['risk', 'peak_1', 'day_1', 'peak_2', 'day_2', 'peak_3',
        'day_3', 'peak_4', 'day_4', 'ema_17_day', 'ema_72_day', 'ema_72_week']

    if args.input_features is not None:
        casted_value, validation = my_dynamic_cast(args.input_features,
            list_type=str)

        if validation is False:
            sys.exit()

        input_features = casted_value

    output_feature = args.output_feature
    # **************************************************************************

    # ************************ Check 'params' argument *************************
    param_default_options = {
        'MLPClassifier': {'hidden_layers': [1, 2], 'hidden_layers_neurons': len(input_features),
        'activation': 'relu', 'solver': 'adam', 'alpha': [0.0001, 0.001, 0.01],
        'batch_size': 'auto', 'learning_rate': 'constant', 'learning_rate_init': 0.001,
        'power_t': 0.5, 'max_iter': 200, 'shuffle': True, 'random_state': 1,
        'tol': 1e-4, 'warm_start': False, 'momentum': 0.9, 'nesterovs_momentum': True,
        'early_stopping': False, 'validation_fraction': 0.1, 'beta_1': 0.9,
        'beta_2': 0.999, 'epsilon': 1e-8, 'n_iter_no_change': 10, 'max_fun': 15000},

        'MLPKerasClassifier': {'hidden_layers': [1, 2, 3, 4, 5],
        'hidden_layers_neurons': len(input_features), 'activation': 'relu',
        'optimizer': 'adam', 'loss': 'binary_crossentropy', 'metrics': 'accuracy',
        'epochs': 4, 'overweight_min_class': [0.5, 0.8, 1.0, 1.2, 1.5]},

        'RandomForestClassifier': {'n_estimators': 50, 'criterion': 'gini',
        'max_depth': [15, 16, 17, 18, 19, 20], 'min_samples_split': 2, 'min_samples_leaf': 1,
        'min_weight_fraction_leaf': 0.0, 'max_features': [6, 5, 4], 'max_leaf_nodes': None,
        'min_impurity_decrease': 0.0, 'bootstrap': True, 'oob_score': False,
        'random_state': 1, 'warm_start': False, 'class_weight': 'balanced_subsample',
        'ccp_alpha': 0.0, 'max_samples': None, 'overweight_min_class': [0.5, 0.8, 1.0, 1.2, 1.5]},

        'KNeighborsClassifier': {'n_neighbors': [1, 2, 3, 4, 5], 'weights': ['uniform', 'distance'],
        'algorithm': 'auto', 'leaf_size': 30, 'p': 2, 'metric': 'minkowski', 'metric_params': None}}

    models_params = param_default_options[model_type].copy()
    model_variable_params = []

    if args.params is not None:

        params_list = args.params.split(';')

        for key_value_param in params_list:
            if key_value_param == '':
                continue

            if '=' not in key_value_param or key_value_param.count('=') != 1:
                print(f"Parameter '{key_value_param}' must be 'key=value' compatible.")
                sys.exit()

            key, value = key_value_param.split('=')
            key = key.strip()
            value = value.strip()

            if key not in list(param_default_options[model_type].keys()):
                print(f"Parameter '{key}' does not exist.\nParameters available for " \
                    f"model \'{model_type}\': {list(param_default_options[model_type].keys())}.")
                sys.exit()

            # If value is a list
            if value.startswith('['):
                casted_value, validation = my_dynamic_cast(value,
                    list_type=type(param_default_options[model_type][key]))
            else:
                casted_value, validation = my_dynamic_cast(value,
                    dest_type=type(param_default_options[model_type][key]))

            if validation is False:
                sys.exit()

            models_params[key] = casted_value

    for key, value in models_params.items():
        if isinstance(value, list):
            model_variable_params.append(key)

    # Output variables: models_params (dict), model_variable_params (list)
    # **************************************************************************

    # *********************** Create simulation profiles ***********************
    models_params_in_list = {}
    for key in models_params:
        models_params_in_list[key] = my_to_list(models_params[key])

    models_params_product = [dict(zip(models_params_in_list.keys(), values)) \
        for values in itertools.product(*models_params_in_list.values())]
    models_per_ticker = {ticker: models_params_product.copy() for ticker in list(tickers.keys())}

    if len(tickers) > 1:
        print(f"Found {len(tickers)} tickers between \'{list(tickers.keys())[0]}\' " \
            f"and \'{list(tickers.keys())[-1]}\' (inclusively) with " \
            f"{len(models_params_product)} model executions each.")
    else:
        print(f"Found {len(tickers)} ticker \'{list(tickers.keys())[0]}\' with " \
            f"{len(models_params_product)} model executions.")

    # print(models_per_ticker)
    # **************************************************************************

    # ******************** Check 'sampling_method' arguments *******************
    sampling_method = args.sampling_method
    # **************************************************************************

    # ******************** Check 'scaling_method' arguments *******************
    scaling_method = args.scaling_method
    # **************************************************************************

    # Distribuir 'models_per_ticker' para pools

    # total_runs = len(models_params_product) * len(tickers)

    pbar = tqdm(total=len(tickers))
    start = time.perf_counter()

    with Pool(max_pools) as pool:
        for ticker, dates in tickers.items():

            X_train, y_train, X_test, y_test, datasets_info = load_dataset(ticker,
                dates['start_date'].strftime('%Y-%m-%d'), dates['end_date'].strftime('%Y-%m-%d'),
                datasets_dir, input_features, output_feature, sampling_method=sampling_method,
                scaling_method=scaling_method, test_set_ratio=0.2)

            models_dir = Path(__file__).parent / mlc.MODELS_DIRECTORY / \
                mlc.TICKER_ORIENTED_MODELS_DIRECTORY / mlc.MODEL_CONSTS[model_type]['MODEL_DIRECTORY']

            pool.apply_async(manage_ticker_models, (model_type, ticker, input_features,
                output_feature, X_train, y_train, X_test, y_test, datasets_info, models_dir,
                models_per_ticker[ticker], model_variable_params), callback=lambda x: pbar.update())

        pool.close()
        pool.join()

    finish = time.perf_counter()
    pbar.close()

    print(f"Finished in {int((finish - start) // 60)}min " \
        f"{int((finish - start) % 60)}s.")


# Execute MLP (1 ticker)
# python3 -Wi machine_learning/models_generator.py --start-on-ticker 1 --end-on-ticker 1 --model 'MLPClassifier'
# python3 -Wi machine_learning/models_generator.py --start-on-ticker 1 --end-on-ticker 1 --model 'MLPKerasClassifier'

# Execute Random Forest (1 ticker)
# python3 -Wi machine_learning/models_generator.py --start-on-ticker 1 --end-on-ticker 1 --model 'RandomForestClassifier'

# Execute k-NN (1 ticker)
# python3 -Wi machine_learning/models_generator.py --start-on-ticker 1 --end-on-ticker 1 --model 'KNeighborsClassifier'

# Test params
# python3 -Wi machine_learning/models_generator.py -s 2 -e 5 -t 20 --model 'MLPClassifier' -a "hidden_layers=10; hidden_layers_neurons=12; activation='tanh';"