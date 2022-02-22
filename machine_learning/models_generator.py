import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
import joblib

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

class ModelGenerator:

    def __init__(self):

        cfg_path = Path(__file__).parent / 'config.json'
        config_reader = cr.ConfigReader(config_file_path=cfg_path)

        self._tickers_and_dates = config_reader.tickers_and_dates
        self.datasets_path_prefix = Path(__file__).parent / 'datasets'
        self.models_path_prefix = Path(__file__).parent / 'models'

        self.supported_models = ('KNeighborsClassifier', 'LogisticRegression',
            'LinearSVC', 'BernoulliNB', 'DecisionTreeClassifier', 'RandomForestClassifier')
        self.models_folder = ('kneighbors_classifier', 'logistic_regression',
            'linear_svc', 'bernoulli_nb', 'decision_tree_classifier', 'random_forest_classifier')

    @property
    def tickers_and_dates(self):
        return self._tickers_and_dates

    @tickers_and_dates.setter
    def tickers_and_dates(self, tickers_and_dates):
        self._tickers_and_dates = tickers_and_dates

    def create_ticker_oriented_model(self, max_tickers=0, model_type='RandomForestClassifier',
        test_set_ratio=0.20):

        try:
            model_folder = 'ticker_oriented_models'

            if model_type not in self.supported_models:
                raise Exception("'objective' parameter options: 'train', 'test'.")

            # For each Ticker
            for tck_index, (ticker, date) in enumerate(self.tickers_and_dates.items()):

                if tck_index == max_tickers and max_tickers != 0:
                    break

                total_tickers = min(len(self.tickers_and_dates), max_tickers) \
                        if max_tickers != 0 else len(self.tickers_and_dates)
                print(f"Processing Ticker '{ticker}' ({tck_index+1} of " \
                    f"{total_tickers})")

                training_df, test_df = self._load_datasets(ticker, test_set_ratio=test_set_ratio)

                if model_type == 'KNeighborsClassifier':
                    model = ModelGenerator._get_KNeighborsClassifier_model(training_df, test_df)

                    # Save model
                    joblib.dump(model, self.models_path_prefix / model_folder /
                        self.models_folder[self.supported_models.index(model_type)] /
                        f'{ticker}_knn_model.joblib')

                elif model_type == 'RandomForestClassifier':
                    model = ModelGenerator._get_RandomForestClassifier(training_df, test_df)

                    # Save model
                    joblib.dump(model, self.models_path_prefix / model_folder /
                        self.models_folder[self.supported_models.index(model_type)] /
                        f'{ticker}_rnd_fst_model.joblib')

        except Exception as error:
            logger.error('Error creating ticker oriented models, error:\n{}'.format(error))
            sys.exit(c.MODEL_CREATION_ERR)

    def _load_datasets(self, ticker, test_set_ratio=0.20):

        file_path = self.datasets_path_prefix / (ticker + '_dataset.csv')
        columns = ['risk', 'success_oper_flag', 'timeout_flag', 'end_of_interval_flag',
            'peak_1', 'day_1', 'peak_2', 'day_2', 'peak_3', 'day_3', 'peak_4', 'day_4',
            'ema_17_day', 'ema_72_day', 'ema_72_week']

        df = pd.read_csv(file_path, sep=',', usecols=columns)

        training_df, test_df = train_test_split(df, test_size=test_set_ratio,
            shuffle=False)

        training_df = ModelGenerator._remove_row_from_last_n_peaks(training_df)

        # End of interval set rows may pollute the models
        training_df = training_df.drop(
            training_df[training_df['end_of_interval_flag'] == 1].index)
        test_df = test_df.drop(
            test_df[test_df['end_of_interval_flag'] == 1].index)

        return training_df, test_df

    @staticmethod
    def _remove_row_from_last_n_peaks(training_df, backward_peaks=4):

        last_day_1 = 0
        idx_counter = 0
        end_index = None

        for idx, row in training_df[::-1].iterrows():
            if last_day_1 \
                and row['day_1'] != last_day_1 + 1:

                idx_counter += 1

                if idx_counter == backward_peaks:
                    end_index = idx
                    break

            last_day_1 = row['day_1']

        return training_df[0:end_index]

    @staticmethod
    def _get_KNeighborsClassifier_model(training_df, test_df,
        n_neighbors_list=[1, 2, 3, 4]):

        knn_training_accuracy = []
        knn_test_accuracy = []
        best_knn_training_accuracy = 0
        best_knn_test_accuracy = 0.0
        best_knn_model = None

        print("\n- KNeighborsClassifier")

        for n_neighbors in n_neighbors_list:
            knn = KNeighborsClassifier(n_neighbors=n_neighbors)
            knn.fit(training_df[['peak_1', 'day_1', 'peak_2', 'day_2', 'peak_3',
                'day_3', 'peak_4', 'day_4', 'ema_17_day', 'ema_72_day', 'ema_72_week']],
                training_df[['success_oper_flag']].squeeze())
            print(f"   n_neighbors = {str(n_neighbors).rjust(2)}", end='')

            training_set_acc = knn.score(training_df[['peak_1', 'day_1', 'peak_2',
                'day_2', 'peak_3', 'day_3', 'peak_4', 'day_4', 'ema_17_day',
                'ema_72_day', 'ema_72_week']],
                training_df[['success_oper_flag']].squeeze())

            test_set_acc = knn.score(test_df[['peak_1', 'day_1', 'peak_2',
                'day_2', 'peak_3', 'day_3', 'peak_4', 'day_4', 'ema_17_day',
                'ema_72_day', 'ema_72_week']],
                test_df[['success_oper_flag']].squeeze())

            print("\t Training set acc: {:.3f}".format(training_set_acc), end='')
            print(", Test set acc: {:.3f}".format(test_set_acc))
            knn_training_accuracy.append(training_set_acc)
            knn_test_accuracy.append(test_set_acc)

            if knn_test_accuracy[-1] > best_knn_test_accuracy:
                best_knn_training_accuracy = knn_training_accuracy[-1]
                best_knn_test_accuracy = knn_test_accuracy[-1]
                best_knn_model = knn

        print(f"\n* Best KNeighborsClassifier")
        print(f"   n_neighbors = {str(best_knn_model.n_neighbors).rjust(2)}", end='')
        print("\t Training set acc: {:.3f}".format(best_knn_training_accuracy), end='')
        print(", Test set acc: {:.3f}".format(best_knn_test_accuracy))

        return best_knn_model

    @staticmethod
    def _get_RandomForestClassifier(training_df, test_df):

        # Parameters
        max_features = 11
        n_estimators_list = [i for i in range(1, max_features+1, 1)]
        depth_list = [i for i in range(2, 20, 2)]

        rnd_frt_training_accuracy = []
        rnd_frt_test_accuracy = []
        best_rnd_frt_training_accuracy = 0
        best_rnd_frt_test_accuracy = 0
        best_rnd_frt_model = None

        print("\n- RandomForestClassifier")

        for n_estimator in n_estimators_list:
            for depth in depth_list:
                rnd_frt = RandomForestClassifier(n_estimators=n_estimator, max_depth=depth,
                    random_state=0, n_jobs=-1)
                rnd_frt.fit(training_df[['peak_1', 'day_1', 'peak_2', 'day_2', 'peak_3',
                    'day_3', 'peak_4', 'day_4', 'ema_17_day', 'ema_72_day', 'ema_72_week']],
                    training_df[['success_oper_flag']].squeeze())

                print(f"   n_estimators = {str(n_estimator).rjust(2)}, " \
                    f"max_depth = {str(depth).rjust(2)}", end='')

                training_set_acc = rnd_frt.score(training_df[['peak_1', 'day_1', 'peak_2',
                    'day_2', 'peak_3', 'day_3', 'peak_4', 'day_4', 'ema_17_day',
                    'ema_72_day', 'ema_72_week']],
                    training_df[['success_oper_flag']].squeeze())

                test_set_acc = rnd_frt.score(test_df[['peak_1', 'day_1', 'peak_2',
                    'day_2', 'peak_3', 'day_3', 'peak_4', 'day_4', 'ema_17_day',
                    'ema_72_day', 'ema_72_week']],
                    test_df[['success_oper_flag']].squeeze())

                print("\t Training set acc: {:.3f}".format(training_set_acc), end='')
                print(", Test set acc: {:.3f}".format(test_set_acc))
                rnd_frt_training_accuracy.append(training_set_acc)
                rnd_frt_test_accuracy.append(test_set_acc)

                if rnd_frt_test_accuracy[-1] > best_rnd_frt_test_accuracy:
                    best_rnd_frt_training_accuracy = rnd_frt_training_accuracy[-1]
                    best_rnd_frt_test_accuracy = rnd_frt_test_accuracy[-1]
                    best_rnd_frt_model = rnd_frt

        print(f"\n* Best RandomForestClassifier")
        print(f"   n_estimators = {str(best_rnd_frt_model.n_estimators).rjust(2)}, " \
                f"max_depth = {str(best_rnd_frt_model.max_depth).rjust(2)}", end='')
        print("\t Training set acc: {:.3f}".format(best_rnd_frt_training_accuracy), end='')
        print(", Test set acc: {:.3f}".format(best_rnd_frt_test_accuracy))

        return best_rnd_frt_model

if __name__ == '__main__':
    logger.info('Model Generator started.')

    model_gen = ModelGenerator()

    model_gen.create_ticker_oriented_model(max_tickers=1,
        model_type='KNeighborsClassifier', test_set_ratio=0.20)