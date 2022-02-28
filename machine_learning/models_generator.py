import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
import joblib
import matplotlib.pyplot as plt
import numpy as np
import os, shutil
from sklearn.tree import export_graphviz
from subprocess import call

sys.path.insert(1, '/Users/atcha/Github/Projeto-Final/src')
import constants as c
import config_reader as cr
from db_model import DBStrategyAnalyzerModel

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

SPECS_DIRECTORY_SUFFIX = '_rnd_fst_model_specs'
SPECS_TEXT_FILE_SUFFIX = '_model_specs.txt'
RANDOM_FOREST_MODEL_SUFFIX = '_rnd_fst_model.joblib'
RANDOM_FOREST_FIG_SUFFIX = '_rnd_fst_model.png'

class ModelGenerator:

    def __init__(self, min_date_filter=None, max_date_filter=None):

        self._min_date_filter = min_date_filter
        self._max_date_filter = max_date_filter

        try:
            if min_date_filter is not None:
                pd.Timestamp(min_date_filter)
            if max_date_filter is not None:
                pd.Timestamp(max_date_filter)
        except Exception as error:
            logger.error("\'min_date_filter\' and \'max_date_filter\' " \
                "formats are \'YYYY-MM-DD\', error:\n{}".format(error))
            sys.exit(c.MODEL_CREATION_ERR)

        cfg_path = Path(__file__).parent / 'config.json'
        config_reader = cr.ConfigReader(config_file_path=cfg_path)

        self._tickers_and_dates = config_reader.tickers_and_dates
        self.datasets_path_prefix = Path(__file__).parent / 'datasets'
        self.models_path_prefix = Path(__file__).parent / 'models'

        self.supported_models = ('KNeighborsClassifier', 'RandomForestClassifier')
        self.models_folder = ('kneighbors_classifier', 'random_forest_classifier')
        self.model_type_folder = 'ticker_oriented_models'

        self.feature_columns = ['risk', 'peak_1', 'day_1', 'peak_2', 'day_2', 'peak_3',
                'day_3', 'peak_4', 'day_4', 'ema_17_day', 'ema_72_day', 'ema_72_week']

    @property
    def tickers_and_dates(self):
        return self._tickers_and_dates

    @tickers_and_dates.setter
    def tickers_and_dates(self, tickers_and_dates):
        self._tickers_and_dates = tickers_and_dates

    @property
    def min_date_filter(self):
        return self._min_date_filter

    @min_date_filter.setter
    def min_date_filter(self, min_date_filter):
        self._min_date_filter = min_date_filter

    @property
    def max_date_filter(self):
        return self._max_date_filter

    @max_date_filter.setter
    def max_date_filter(self, max_date_filter):
        self._max_date_filter = max_date_filter

    @property
    def test_set_start_date(self):
        return self._test_set_start_date

    @test_set_start_date.setter
    def test_set_start_date(self, test_set_start_date):
        self._test_set_start_date = test_set_start_date

    def create_ticker_oriented_models(self, max_tickers=0, start_on_ticker=1,
        end_on_ticker=0, model_type='RandomForestClassifier', test_set_ratio=0.2):

        if start_on_ticker <= 0:
            logger.error("'start_on_ticker' minimum value is 1.")
            sys.exit(c.DATASET_GENERATION_ERR)

        if end_on_ticker != 0 and start_on_ticker >= end_on_ticker:
            logger.error("'start_on_ticker' must be lesser than 'end_on_ticker'.")
            sys.exit(c.DATASET_GENERATION_ERR)

        if end_on_ticker == 0:
            end_on_ticker = len(self.tickers_and_dates) + 1

        try:
            if model_type not in self.supported_models:
                raise Exception("'objective' parameter options: 'train', 'test'.")

            tickers_delta = end_on_ticker - start_on_ticker if end_on_ticker != 0 \
                else len(self.tickers_and_dates) - start_on_ticker + 1 if start_on_ticker != 0 \
                else len(self.tickers_and_dates)
            total_tickers = min(len(self.tickers_and_dates), max_tickers, tickers_delta) \
                if max_tickers != 0 else min(len(self.tickers_and_dates), tickers_delta)

            # For each Ticker
            for tck_index, (ticker, date) in enumerate(self.tickers_and_dates.items()):

                if tck_index == max_tickers and max_tickers != 0:
                    break
                if tck_index + 1 < start_on_ticker:
                    continue
                if tck_index + 1 >= end_on_ticker:
                    continue

                print(f"\nProcessing Ticker '{ticker}' ({tck_index+1} of " \
                    f"{total_tickers})")

                training_df, test_df, real_test_df = self._load_datasets(ticker, test_set_ratio)

                if model_type == 'KNeighborsClassifier':
                    model = self._get_kneighbors_classifier(training_df, test_df)

                    # Save model
                    joblib.dump(model, self.models_path_prefix / self.model_type_folder /
                        self.models_folder[self.supported_models.index(model_type)] /
                        f'{ticker}_knn_model.joblib')

                elif model_type == 'RandomForestClassifier':
                    self.reset_specs_directory(ticker)

                    model = self._get_random_forest_classifier(ticker, training_df,
                        test_df, real_test_df)

                    self.save_feature_importances(ticker, model)
                    self.visualize_trees(ticker, model, max_estimators=3, max_depth=3)

                    # Save model
                    joblib.dump(model, self.models_path_prefix / self.model_type_folder /
                        self.models_folder[self.supported_models.index(model_type)] /
                        (f'{ticker}' + RANDOM_FOREST_MODEL_SUFFIX))

        except Exception as error:
            logger.error('Error creating ticker oriented models, error:\n{}'.format(error))
            sys.exit(c.MODEL_CREATION_ERR)

    def _load_datasets(self, ticker, test_set_ratio=0.2):

        file_path = self.datasets_path_prefix / (ticker + '_dataset.csv')
        columns = ['day', 'risk', 'success_oper_flag', 'timeout_flag',
            'end_of_interval_flag'].extend(self.feature_columns)

        df = pd.read_csv(file_path, sep=',', usecols=columns)

        real_test_df = df[df['day'] > self.max_date_filter]

        if self.min_date_filter is not None:
            df = df[df['day'] >= self.min_date_filter]
        if self.max_date_filter is not None:
            df = df[df['day'] <= self.max_date_filter]

        training_df, test_df = train_test_split(df, test_size=test_set_ratio,
            shuffle=False)

        training_df.reset_index(drop=True, inplace=True)
        test_df.reset_index(drop=True, inplace=True)
        real_test_df.reset_index(drop=True, inplace=True)

        training_df = ModelGenerator._remove_row_from_last_n_peaks(training_df)

        # End of interval set rows may pollute the models
        training_df = training_df.drop(
            training_df[training_df['end_of_interval_flag'] == 1].index)
        test_df = test_df.drop(
            test_df[test_df['end_of_interval_flag'] == 1].index)
        real_test_df = real_test_df.drop(
            real_test_df[real_test_df['end_of_interval_flag'] == 1].index)

        training_df.reset_index(drop=True, inplace=True)
        test_df.reset_index(drop=True, inplace=True)
        real_test_df.reset_index(drop=True, inplace=True)

        return training_df, test_df, real_test_df

    @staticmethod
    def _remove_row_from_last_n_peaks(training_df, backward_peaks=4):

        last_day = training_df['day'].tail(1).squeeze()
        last_day_1_distance = 0
        idx_counter = 0
        end_index = None

        for idx, row in training_df[::-1].iterrows():
            if last_day_1_distance \
                and row['day'] != last_day \
                and row['day_1'] != last_day_1_distance + 1:

                idx_counter += 1

                if idx_counter == backward_peaks:
                    end_index = idx
                    break

            last_day = row['day']
            last_day_1_distance = row['day_1']


        return training_df[0:end_index+1]

    def _get_kneighbors_classifier(self, training_df, test_df,
        n_neighbors_list=[1, 2, 3, 4]):

        knn_training_accuracy = []
        knn_test_accuracy = []
        best_knn_training_accuracy = 0
        best_knn_test_accuracy = 0.0
        best_knn_model = None

        print("\n- KNeighborsClassifier")

        for n_neighbors in n_neighbors_list:
            knn = KNeighborsClassifier(n_neighbors=n_neighbors)
            knn.fit(training_df[self.feature_columns],
                training_df[['success_oper_flag']].squeeze())
            print(f"   n_neighbors = {str(n_neighbors).rjust(2)}", end='')

            training_set_acc = knn.score(training_df[self.feature_columns],
                training_df[['success_oper_flag']].squeeze())

            test_set_acc = knn.score(test_df[self.feature_columns],
                test_df[['success_oper_flag']].squeeze())

            print("\t Training acc: {:.4f}%".format(training_set_acc), end='')
            print(", Test acc: {:.4f}%".format(test_set_acc))
            knn_training_accuracy.append(training_set_acc)
            knn_test_accuracy.append(test_set_acc)

            if knn_test_accuracy[-1] > best_knn_test_accuracy:
                best_knn_training_accuracy = knn_training_accuracy[-1]
                best_knn_test_accuracy = knn_test_accuracy[-1]
                best_knn_model = knn

        print(f"\n* Best KNeighborsClassifier")
        print(f"   n_neighbors = {str(best_knn_model.n_neighbors).rjust(2)}", end='')
        print("\t Training acc: {:.4f}%".format(best_knn_training_accuracy), end='')
        print(", Test acc: {:.4f}%".format(best_knn_test_accuracy))

        return best_knn_model

    def _get_random_forest_classifier(self, ticker, training_df, test_df, real_test_df):

        number_of_max_features = len(self.feature_columns)

        # *************** Parameters ***************
        n_estimators_list = [50]
        max_depth_list = [i for i in range(2, 31, 1)]
        max_features_list = [number_of_max_features]
        min_samples_split = 2
        min_samples_leaf = 1
        bootstrap = True
        class_weight = 'balanced_subsample'
        # ******************************************

        rnd_frt_training_accuracy = []
        rnd_frt_test_accuracy = []
        best_rnd_frt_training_accuracy = 0
        best_rnd_frt_test_accuracy = 0
        best_rnd_frt_model = None

        self.write_model_specs(ticker, f"RandomForestClassifier (\'{ticker}\')",
            reset_file=True, std_out_also=False)

        self.write_model_specs(ticker, "\n   Common configuration\n")
        self.write_model_specs(ticker, f"Bootstrap: \t\t\t\t\t{str(bootstrap)}")
        self.write_model_specs(ticker, f"Class weight: \t\t\t\t{class_weight}")
        self.write_model_specs(ticker, f"Min samples split: \t\t{min_samples_split}")
        self.write_model_specs(ticker, f"Min samples leaf: \t\t{min_samples_leaf}")

        self.write_model_specs(ticker, "\n   Dataset\n")
        self.write_model_specs(ticker, f"Training set samples: \t\t{len(training_df)} " \
            f"({100 * len(training_df) / (len(training_df)+len(test_df)):.2f}%)")
        self.write_model_specs(ticker, f"Test set samples: \t\t\t{len(test_df)} " \
            f"({100 * len(test_df) / (len(training_df)+len(test_df)):.2f}%)")
        self.write_model_specs(ticker, f"Real test set samples: \t\t{len(real_test_df)}")

        training_set_failures = len(training_df.loc[training_df['success_oper_flag'] == 0])
        total_training_set = len(training_df)
        test_set_failures = len(test_df.loc[test_df['success_oper_flag'] == 0])
        total_test_set = len(test_df)
        real_real_test_set_failures = len(real_test_df.loc[real_test_df['success_oper_flag'] == 0])
        total_real_test_set = len(real_test_df)
        self.write_model_specs(ticker, f"\nTraining set failure operations: \t{round(training_set_failures/total_training_set, 4)}%")
        self.write_model_specs(ticker, f"Test set failure operations: \t\t\t{round(test_set_failures/total_test_set, 4)}%")
        self.write_model_specs(ticker, f"Real test set failure operations: \t{round(real_real_test_set_failures/total_real_test_set, 4)}%")

        self.write_model_specs(ticker, f"\nTraining set start date: \t\'{training_df['day'].head(1).squeeze()}\'")
        self.write_model_specs(ticker, f"Training set end date: \t\t\'{training_df['day'].tail(1).squeeze()}\'")
        self.write_model_specs(ticker, f"Test set start date: \t\t\'{test_df['day'].head(1).squeeze()}\'")
        self.write_model_specs(ticker, f"Test set end date: \t\t\t\'{test_df['day'].tail(1).squeeze()}\'")
        self.write_model_specs(ticker, f"Real test set start date: \t\'{real_test_df['day'].head(1).squeeze()}\'")
        self.write_model_specs(ticker, f"Real test set end date: \t\'{real_test_df['day'].tail(1).squeeze()}\'")

        self.write_model_specs(ticker, "\n   Models\n")

        for n_estimator in n_estimators_list:
            for max_depth in max_depth_list:
                for max_features in max_features_list:

                    rnd_frt = RandomForestClassifier(n_estimators=n_estimator,
                        criterion='gini', max_features=max_features, max_depth=max_depth,
                        min_samples_split=min_samples_split, min_samples_leaf=min_samples_leaf,
                        bootstrap=bootstrap, class_weight=class_weight, random_state=0,
                        n_jobs=-1)

                    rnd_frt.fit(training_df[self.feature_columns],
                        training_df[['success_oper_flag']].squeeze())

                    self.write_model_specs(ticker, f"n_estimators = " \
                        f"{str(n_estimator).rjust(2)}, max_depth = " \
                        f"{str(max_depth).rjust(2)}, max_features = " \
                        f"{str(max_features).rjust(2)}", end='')

                    training_set_acc = rnd_frt.score(training_df[self.feature_columns],
                        training_df[['success_oper_flag']].squeeze())

                    test_set_acc = rnd_frt.score(test_df[self.feature_columns],
                        test_df[['success_oper_flag']].squeeze())

                    real_test_set_acc = rnd_frt.score(real_test_df[self.feature_columns],
                        real_test_df[['success_oper_flag']].squeeze())

                    self.write_model_specs(ticker, f"\t Training acc: {training_set_acc:.4f}%", end='')
                    self.write_model_specs(ticker, f", Test acc: {test_set_acc:.4f}%", end='')
                    self.write_model_specs(ticker, f", Real test acc: {real_test_set_acc:.4f}%")

                    rnd_frt_training_accuracy.append(training_set_acc)
                    rnd_frt_test_accuracy.append(test_set_acc)

                    if rnd_frt_test_accuracy[-1] > best_rnd_frt_test_accuracy:
                        best_rnd_frt_training_accuracy = rnd_frt_training_accuracy[-1]
                        best_rnd_frt_test_accuracy = rnd_frt_test_accuracy[-1]
                        best_rnd_frt_model = rnd_frt

        self.write_model_specs(ticker, "\n   Best model\n")
        self.write_model_specs(ticker, f"n_estimators = " \
                        f"{str(best_rnd_frt_model.n_estimators).rjust(2)}, max_depth = " \
                        f"{str(best_rnd_frt_model.max_depth).rjust(2)}, max_features = " \
                        f"{str(best_rnd_frt_model.max_features).rjust(2)}", end='')
        self.write_model_specs(ticker, f"\t Training acc: {best_rnd_frt_training_accuracy:.4f}%", end='')
        self.write_model_specs(ticker, f", Test acc: {best_rnd_frt_test_accuracy:.4f}%")

        return best_rnd_frt_model

    def write_model_specs(self, ticker, message, end='\n', reset_file=False,
        std_out_also=True):

        ticker_specs_path = self.get_specs_directory_path(ticker)
        mode = 'a' if reset_file is False else 'w'

        if not ticker_specs_path.exists():
            os.mkdir(ticker_specs_path)

        with open(ticker_specs_path / (f'{ticker}' + SPECS_TEXT_FILE_SUFFIX), mode) as file:
            file.write(message + end)

        if std_out_also is True:
            print(message, end=end)

    def save_feature_importances(self, ticker, model):

        n_features = len(self.feature_columns)

        ticker_specs_path = self.get_specs_directory_path(ticker)

        fig=plt.figure()
        plt.barh(range(n_features), model.feature_importances_, align='center')
        plt.yticks(np.arange(n_features), self.feature_columns)
        plt.title(f"\'{ticker}\' Random Forest (n_estimators={model.n_estimators}, " \
            f"max_depth={model.max_depth}, max_features={model.max_features})")
        plt.xlabel("Feature importance")
        plt.ylabel("Feature")
        plt.savefig(ticker_specs_path / (f'{ticker}' + RANDOM_FOREST_FIG_SUFFIX), bbox_inches='tight')
        plt.close(fig)

    def visualize_trees(self, ticker, model, max_estimators=3, max_depth=3):

        if model.max_depth <= max_depth:
            ticker_specs_path = self.get_specs_directory_path(ticker)

            for n in range(model.n_estimators):
                if n >= max_estimators:
                    break

                export_graphviz(model.estimators_[n], out_file=str(ticker_specs_path / \
                    f"{ticker}_tree_{n+1}.dot"), class_names=["Fail", "Success"],
                    feature_names=self.feature_columns, impurity=False, filled=True)

                call(['dot', '-Tpng', str(ticker_specs_path / f"{ticker}_tree_{n+1}.dot"),
                    '-o', str(ticker_specs_path /  f"{ticker}_tree_{n+1}.png"), '-Gdpi=600'])

        self.delete_dot_files(ticker)

    def get_specs_directory_path(self, ticker):

        ticker_specs_path = self.models_path_prefix / self.model_type_folder / \
                self.models_folder[self.supported_models.index('RandomForestClassifier')] / \
                (f'{ticker}' + SPECS_DIRECTORY_SUFFIX)

        return ticker_specs_path

    def reset_specs_directory(self, ticker):

        ticker_specs_path = self.get_specs_directory_path(ticker)

        if ticker_specs_path.exists():
            shutil.rmtree(ticker_specs_path)

        os.mkdir(ticker_specs_path)

    def delete_dot_files(self, ticker):

        ticker_specs_path = self.get_specs_directory_path(ticker)

        if ticker_specs_path.exists():
            dot_ended_files = [file for file in ticker_specs_path.glob('*.dot')]

            for file in dot_ended_files:
                path_to_file = ticker_specs_path / file
                os.remove(path_to_file)

if __name__ == '__main__':
    logger.info('Model Generator started.')

    model_gen = ModelGenerator(min_date_filter='2013-01-01', max_date_filter='2018-07-01')

    model_gen.create_ticker_oriented_models(max_tickers=0, start_on_ticker=1,
        end_on_ticker=0, model_type='RandomForestClassifier', test_set_ratio=0.15)
