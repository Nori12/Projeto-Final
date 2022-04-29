import sys
from pathlib import Path
from abc import ABC, abstractmethod
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import RidgeClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_auc_score
from keras.layers import Dense
from keras.models import Sequential
from scipy.interpolate import interp1d
import joblib
import matplotlib.pyplot as plt
import numpy as np

sys.path.insert(1, str(Path(__file__).parent.parent/'src'))
import constants as c
import ml_constants as mlc

class PseudoModel(ABC):

    @abstractmethod
    def __init__(self, model_type, ticker, input_features, output_feature, X_train, y_train,
        X_test, y_test, parameters=None):
        pass

    @property
    @abstractmethod
    def model_type(self):
        pass

    @model_type.setter
    @abstractmethod
    def model_type(self, model_type):
        pass

    @property
    @abstractmethod
    def ticker(self):
        pass

    @ticker.setter
    @abstractmethod
    def ticker(self, ticker):
        pass

    @property
    @abstractmethod
    def input_features(self):
        pass

    @input_features.setter
    @abstractmethod
    def input_features(self, input_features):
        pass

    @property
    @abstractmethod
    def output_feature(self):
        pass

    @output_feature.setter
    @abstractmethod
    def output_feature(self, output_feature):
        pass

    @property
    @abstractmethod
    def parameters(self):
        pass

    @parameters.setter
    @abstractmethod
    def parameters(self, parameters):
        pass

    @property
    @abstractmethod
    def specs_dir(self):
        pass

    @property
    @abstractmethod
    def model(self):
        pass


    @abstractmethod
    def create_model(self):
        pass

    @abstractmethod
    def get_accuracy(self, X_test, y_test):
        pass

    @abstractmethod
    def get_confusion(self, X_test, y_test):
        pass

    @abstractmethod
    def save (self):
        pass

    @abstractmethod
    def save_results(self, model_type, ticker, all_params, variable_params,
        y_train, y_test, dataset_info, training_accuracies, test_accuracies,
        training_confusions, test_confusions, specs_dir, training_profit_indexes,
        training_profit_indexes_zeros, test_profit_indexes, test_profit_indexes_zeros):
        pass

    @abstractmethod
    def save_auxiliary_files(self, model):
        pass


class Model(PseudoModel):

    def __init__(self, model_type, ticker, input_features, output_feature,
        X_train, y_train, X_test, y_test, model_dir, parameters=None, model_tag=None):

        self._model_type = model_type
        self._ticker = ticker
        self._input_features = input_features
        self._output_feature = output_feature
        self._parameters = parameters
        self._model_dir = model_dir

        self._X_train = X_train
        self._y_train = y_train
        self._X_test = X_test
        self._y_test = y_test

        self._specs_dir = Path(__file__).parent / mlc.MODELS_DIRECTORY / \
            mlc.TICKER_ORIENTED_MODELS_DIRECTORY / mlc.MODEL_CONSTS[model_type]['MODEL_DIRECTORY'] / \
            (f'{ticker}' + mlc.SPECS_DIRECTORY_SUFFIX)

        self._model = None
        self._model_tag = model_tag

    @property
    def model_type(self):
        return self._model_type

    @model_type.setter
    def model_type(self, model_type):
        self._model_type = model_type

    @property
    def ticker(self):
        return self._ticker

    @ticker.setter
    def ticker(self, ticker):
        self._ticker = ticker

    @property
    def input_features(self):
        return self._input_features

    @input_features.setter
    def input_features(self, input_features):
        self._input_features = input_features

    @property
    def output_feature(self):
        return self._output_feature

    @output_feature.setter
    def output_feature(self, output_feature):
        self._output_feature = output_feature

    @property
    def parameters(self):
        return self._parameters

    @parameters.setter
    def parameters(self, parameters):
        self._parameters = parameters

    @property
    def model_dir(self):
        return self._model_dir

    @model_dir.setter
    def model_dir(self, model_dir):
        self._model_dir = model_dir

    @property
    def X_train(self):
        return self._X_train

    @X_train.setter
    def X_train(self, X_train):
        self._X_train = X_train

    @property
    def y_train(self):
        return self._y_train

    @y_train.setter
    def y_train(self, y_train):
        self._y_train = y_train

    @property
    def X_test(self):
        return self._X_test

    @X_test.setter
    def X_test(self, X_test):
        self._X_test = X_test

    @property
    def y_test(self):
        return self._y_test

    @y_test.setter
    def y_test(self, y_test):
        self._y_test = y_test

    @property
    def specs_dir(self):
        return self._specs_dir

    @property
    def model(self):
        return self._model

    @model.setter
    def model(self, model):
        self._model = model

    @property
    def model_tag(self):
        return self._model_tag


    def create_model(self):
        pass

    def get_accuracy(self, X_test, y_test):
        pass

    def get_confusion(self, X_test, y_test):
        pass

    def save(self):
        if not self.model_dir.exists():
            self.model_dir.mkdir(parents=True)

        if self.model_tag is not None and self.model_tag != '':
            target_dir = self.model_dir / (f'{self.ticker}' + '_' + self.model_tag + \
                 mlc.MODEL_FILE_SUFFIX)
        else:
            target_dir = self.model_dir / (f'{self.ticker}' + mlc.MODEL_FILE_SUFFIX)

        joblib.dump(self.model, target_dir)

    @staticmethod
    def create_results_message(model_type, ticker, all_params, variable_params,
        y_train, y_test, dataset_info, training_accuracies, test_accuracies,
        training_confusions, test_confusions, training_profit_indexes,
        training_profit_indexes_zeros, test_profit_indexes, test_profit_indexes_zeros,
        idx_of_bests, avg_over_std_of_bests, best_overweights,
        avg_over_std_of_best_ow, coefs=None):

        common_cfg_ljust = 31
        dataset_ljust = 31
        param_rjust = 3

        # Write general configs
        message = f"{model_type} (\'{ticker}\')"

        message += "\n\nCommon configuration"

        for param, value in all_params[0].items():
            if param not in variable_params:
                message += f"\n   {str(param).ljust(common_cfg_ljust)}: {value}"

        message += "\n\nDataset"

        message += f"\n   {'Training set samples'.ljust(dataset_ljust)}: {len(y_train)} " \
            f"({100 * len(y_train) / (len(y_train)+len(y_test)):.2f}%)"
        message += f"\n   {'Test set samples'.ljust(dataset_ljust)}: {len(y_test)} " \
            f"({100 * len(y_test) / (len(y_train)+len(y_test)):.2f}%)"
        message += "\n"

        train_failure_operations = round(100 * len(y_train[y_train == 0]) / \
            (len(y_train[y_train == 0]) + len(y_train[y_train == 1])), 2)
        train_success_operations = round(100 * len(y_train[y_train == 1]) / \
            (len(y_train[y_train == 0]) + len(y_train[y_train == 1])), 2)
        test_failure_operations = round(100 * len(y_test[y_test == 0]) / \
            (len(y_test[y_test == 0]) + len(y_test[y_test == 1])), 2)
        test_success_operations = round(100 * len(y_test[y_test == 1]) / \
            (len(y_test[y_test == 0]) + len(y_test[y_test == 1])), 2)

        message += f"\n   {'Training set failure operations'.ljust(dataset_ljust)}: " \
            f"{len(y_train[y_train == 0])} ({train_failure_operations}%)"
        message += f"\n   {'Training set success operations'.ljust(dataset_ljust)}: " \
            f"{len(y_train[y_train == 1])} ({train_success_operations}%)"

        message += "\n"

        message += f"\n   {'Test set failure operations'.ljust(dataset_ljust)}: " \
            f"{len(y_test[y_test == 0])} ({test_failure_operations}%)"
        message += f"\n   {'Test set success operations'.ljust(dataset_ljust)}: " \
            f"{len(y_test[y_test == 1])} ({test_success_operations}%)"

        message += "\n"

        message += f"\n   {'Training set start date'.ljust(dataset_ljust)}: \'{dataset_info['training_set_start_date']}\'"
        message += f"\n   {'Training set end date'.ljust(dataset_ljust)}: \'{dataset_info['training_set_end_date']}\'"
        message += f"\n   {'Test set start date'.ljust(dataset_ljust)}: \'{dataset_info['test_set_start_date']}\'"
        message += f"\n   {'Test set end date'.ljust(dataset_ljust)}: \'{dataset_info['test_set_end_date']}\'"

        message += "\n\nModels"

        models_result = []

        for idx, (training_acc, test_acc, train_confusion, test_confusion,
            training_profit_index, training_profit_indexes_zero, test_profit_index,
            test_profit_indexes_zero) in enumerate(zip(training_accuracies, test_accuracies,
            training_confusions, test_confusions, training_profit_indexes,
            training_profit_indexes_zeros, test_profit_indexes, test_profit_indexes_zeros)):

            true_positive_train = train_confusion[1,1]
            true_negative_train = train_confusion[0,0]
            true_positive_test = test_confusion[1,1]
            true_negative_test = test_confusion[0,0]

            model_msg_line = \
                f"\n   ({idx+1:03d}) " \
                f"Profit_index(test):{100 * test_profit_index:6.2f}% (zero:{100 * test_profit_indexes_zero:6.2f}%), " \
                f"Profit_index(train):{100 * training_profit_index:6.2f}% (zero:{100 * training_profit_indexes_zero:6.2f}%) || " \
                f"Acc(test):{round(100 * test_acc, 2):6.2f}%, " \
                f"Acc(train):{round(100 * training_acc, 2):6.2f}% || " \
                f"TP(test):{round(100 * true_positive_test/np.sum(test_confusion, axis=1)[1], 1):5.1f}%, " \
                f"TN(test):{round(100 * true_negative_test/np.sum(test_confusion, axis=1)[0], 1):5.1f}%, " \
                f"TP(train):{round(100 * true_positive_train/np.sum(train_confusion, axis=1)[1], 1):5.1f}%, " \
                f"TN(train):{round(100 * true_negative_train/np.sum(train_confusion, axis=1)[0], 1):5.1f}%"

            if variable_params:
                model_msg_line += " ||"
                for i, param_name in enumerate(variable_params):
                    if i != 0:
                        model_msg_line += ","
                    model_msg_line += f" {param_name}:{str(all_params[idx][param_name]).rjust(param_rjust)}"

            if coefs is not None:
                model_msg_line += " || "
                model_msg_line += f"Coefficients: {str(coefs[idx])}"

            message += model_msg_line
            models_result.append(model_msg_line)

        message += "\n\nBest models"

        for i, idx_of_best in enumerate(idx_of_bests):
            message += models_result[idx_of_best][:10] + 'Avg/Std_Profit_index(test):' + f"{avg_over_std_of_bests[i]:4.0f} || " + models_result[idx_of_best][10:]
            if i == 0:
                for overweight, avg_over_std in zip(best_overweights, avg_over_std_of_best_ow):
                    message += f"\n      Avg/Std_Profit_index(test):{avg_over_std:4.0f} || Overweight:{overweight:5.2f}"

        return message

    @staticmethod
    def save_results(model_type, ticker, all_params, variable_params,
        y_train, y_test, dataset_info, training_accuracies, test_accuracies,
        training_confusions, test_confusions, specs_dir, training_profit_indexes,
        training_profit_indexes_zeros, test_profit_indexes, test_profit_indexes_zeros,
        idx_of_bests, avg_over_std_of_bests, best_overweights,
        avg_over_std_of_best_ow, coefs=None, model_tag=None):
        """
            'coefs': list of np.array to linear model coefficients.
        """

        message = Model.create_results_message(model_type, ticker, all_params,
            variable_params, y_train, y_test, dataset_info, training_accuracies,
            test_accuracies, training_confusions, test_confusions, training_profit_indexes,
            training_profit_indexes_zeros, test_profit_indexes, test_profit_indexes_zeros,
            idx_of_bests, avg_over_std_of_bests, best_overweights,
            avg_over_std_of_best_ow, coefs=coefs)

        # Create folder and store model results in file
        if not specs_dir.exists():
            specs_dir.mkdir(parents=True)

        if model_tag is not None and model_tag != '':
            target_dir = specs_dir / (f'{ticker}' + '_' + model_tag + mlc.SPECS_FILE_SUFFIX)
        else:
            target_dir = specs_dir / (f'{ticker}' + mlc.SPECS_FILE_SUFFIX)

        with open(target_dir, 'w') as file:
            file.write(message)

    def save_auxiliary_files(self, model):
        pass

    def get_accuracy(self, X_test, y_test):
        return self.model.score(X_test, y_test)

    def get_confusion(self, X_test, y_test):
        yhat = self.model.predict(X_test)
        yhat = yhat.squeeze()
        yhat[yhat >= 0.5] = 1
        yhat[yhat < 0.5] = 0

        return confusion_matrix(y_test, yhat)

    def get_profit_index(self, X_test, y_test):

        min_possible_profit = -sum(X_test[:, self.input_features.index('risk')][y_test==0])
        max_possible_profit = 3 * sum(X_test[:, self.input_features.index('risk')][y_test==1])

        prof_interp_test = interp1d([min_possible_profit, max_possible_profit], [0, 1])

        yhat = self.model.predict(X_test)
        yhat = yhat.squeeze()
        yhat[yhat >= 0.5] = 1
        yhat[yhat < 0.5] = 0

        profit = 3 * sum(X_test[:,self.input_features.index('risk')][(yhat==1) & (y_test==1)]) \
            - sum(X_test[:,self.input_features.index('risk')][(yhat==1) & (y_test==0)])

        return round(float(prof_interp_test(profit)), 4), round(float(prof_interp_test(0)), 4)


class RandomForest(Model):

    def __init__(self, ticker, input_features, output_feature, X_train, y_train,
        X_test, y_test, model_dir, parameters=None, model_tag=None, overweight=1.0,
        random_state=1):

        super().__init__('RandomForestClassifier', ticker=ticker, input_features=input_features,
            output_feature=output_feature, X_train=X_train, y_train=y_train, X_test=X_test,
            y_test=y_test, model_dir=model_dir, parameters=parameters, model_tag=model_tag)

        self.random_state = random_state
        self.overweight = overweight

    def create_model(self):

        weight_0 = self.overweight * len(self.y_train[self.y_train==1]) / \
            (self.overweight * len(self.y_train[self.y_train==1]) + len(self.y_train[self.y_train==0]))
        weight_1 = len(self.y_train[self.y_train==0]) / \
            (self.overweight * len(self.y_train[self.y_train==1]) + len(self.y_train[self.y_train==0]))

        class_weights = {0: weight_0, 1: weight_1}

        model = RandomForestClassifier(
            n_estimators=self.parameters['n_estimators'],
            criterion=self.parameters['criterion'],
            max_depth=self.parameters['max_depth'],
            min_samples_split=self.parameters['min_samples_split'],
            min_samples_leaf=self.parameters['min_samples_leaf'],
            min_weight_fraction_leaf=self.parameters['min_weight_fraction_leaf'],
            max_features=self.parameters['max_features'],
            max_leaf_nodes=self.parameters['max_leaf_nodes'],
            min_impurity_decrease=self.parameters['min_impurity_decrease'],
            bootstrap=self.parameters['bootstrap'],
            oob_score=self.parameters['oob_score'],
            random_state = self.random_state,
            warm_start=self.parameters['warm_start'],
            class_weight=class_weights,
            ccp_alpha=self.parameters['ccp_alpha'],
            max_samples=self.parameters['max_samples'],
            n_jobs=None)

        model.fit(self.X_train, self.y_train)

        self.model = model

    def save_auxiliary_files(self):

        n_features = len(self.input_features)

        if self.model_tag is not None and self.model_tag != '':
            destination = self.specs_dir / (f'{self.ticker}' + '_' + self.model_tag + \
                mlc.RANDOM_FOREST_FIG_SUFFIX)
        else:
            destination = self.specs_dir / (f'{self.ticker}' + mlc.RANDOM_FOREST_FIG_SUFFIX)

        if not self.specs_dir.exists():
            self.specs_dir.mkdir(parents=True)

        fig=plt.figure()
        plt.barh(range(n_features), self.model.feature_importances_, align='center')
        plt.yticks(np.arange(n_features), self.input_features)
        plt.title(f"\'{self.ticker}\' Best Random Forest Model - Features Importance")
        plt.xlabel("Feature importance")
        plt.ylabel("Feature")
        plt.savefig(destination, bbox_inches='tight')
        plt.close(fig)


class KNeighbors(Model):

    def __init__(self, ticker, input_features, output_feature, X_train, y_train,
        X_test, y_test, model_dir, parameters=None, model_tag=None):

        super().__init__('KNeighborsClassifier', ticker=ticker, input_features=input_features,
            output_feature=output_feature, X_train=X_train, y_train=y_train, X_test=X_test,
            y_test=y_test, model_dir=model_dir, parameters=parameters, model_tag=model_tag)

    def create_model(self):

        model = KNeighborsClassifier(
            n_neighbors=self.parameters['n_neighbors'],
            weights=self.parameters['weights'],
            algorithm=self.parameters['algorithm'],
            leaf_size=self.parameters['leaf_size'],
            p=self.parameters['p'],
            metric=self.parameters['metric'],
            metric_params=self.parameters['metric_params'],
            n_jobs=None)

        model.fit(self.X_train, self.y_train)

        self.model = model


class MLPScikit(Model):

    def __init__(self, ticker, input_features, output_feature, X_train, y_train,
        X_test, y_test, model_dir, parameters=None, model_tag=None):

        super().__init__('MLPClassifier', ticker=ticker, input_features=input_features,
            output_feature=output_feature, X_train=X_train, y_train=y_train, X_test=X_test,
            y_test=y_test, model_dir=model_dir, parameters=parameters, model_tag=model_tag)

    def create_model(self):

        hidden_layer_sizes = tuple([self.parameters['hidden_layers_neurons'] \
            for _ in range(self.parameters['hidden_layers'])])

        model = MLPClassifier(
            hidden_layer_sizes=hidden_layer_sizes,
            activation=self.parameters['activation'],
            solver=self.parameters['solver'],
            alpha=self.parameters['alpha'],
            batch_size=self.parameters['batch_size'],
            learning_rate=self.parameters['learning_rate'],
            learning_rate_init=self.parameters['learning_rate_init'],
            power_t=self.parameters['power_t'],
            max_iter=self.parameters['max_iter'],
            shuffle=self.parameters['shuffle'],
            random_state=self.parameters['random_state'],
            tol = self.parameters['tol'],
            warm_start=self.parameters['warm_start'],
            momentum=self.parameters['momentum'],
            nesterovs_momentum=self.parameters['nesterovs_momentum'],
            early_stopping=self.parameters['early_stopping'],
            validation_fraction=self.parameters['validation_fraction'],
            beta_1=self.parameters['beta_1'],
            beta_2=self.parameters['beta_2'],
            epsilon=self.parameters['epsilon'],
            n_iter_no_change=self.parameters['n_iter_no_change'],
            max_fun=self.parameters['max_fun'])

        model.fit(self.X_train, self.y_train)

        self.model = model


class MLPKeras(Model):

    def __init__(self, ticker, input_features, output_feature, X_train, y_train,
        X_test, y_test, model_dir, parameters=None, model_tag=None):

        super().__init__('MLPKerasClassifier', ticker=ticker, input_features=input_features,
            output_feature=output_feature, X_train=X_train, y_train=y_train, X_test=X_test,
            y_test=y_test, model_dir=model_dir, parameters=parameters, model_tag=model_tag)

        if parameters['activation'] not in ('relu', 'tanh', 'sigmoid', 'selu',
            'softmax', 'softplus', 'softsign', 'elu', 'exponential'):
            print(f"Activation function \'{parameters['activation']}\' not available.")
            sys.exit()

    def create_model(self):

        model = Sequential()
        for idx, _ in enumerate(range(self.parameters['hidden_layers'])):
            if idx == 0:
                model.add(Dense(self.parameters['hidden_layers_neurons'],
                    input_dim=len(self.input_features),
                    activation=self.parameters['activation'],
                    kernel_initializer='he_uniform'))
            else:
                model.add(Dense(self.parameters['hidden_layers_neurons'],
                    activation=self.parameters['activation'],
                    kernel_initializer='he_uniform'))

        model.add(Dense(1, activation='sigmoid'))

        model.compile(loss=self.parameters['loss'], metrics=self.parameters['metrics'],
            optimizer=self.parameters['optimizer'])

        weight_0 = self.parameters['overweight_min_class'] * len(self.y_train[self.y_train==1]) / \
            (self.parameters['overweight_min_class'] * len(self.y_train[self.y_train==1]) + len(self.y_train[self.y_train==0]))
        weight_1 = len(self.y_train[self.y_train==0]) / \
            (self.parameters['overweight_min_class'] * len(self.y_train[self.y_train==1]) + len(self.y_train[self.y_train==0]))

        class_weights = {0: weight_0, 1: weight_1}

        model.fit(self.X_train, self.y_train, epochs=self.parameters['epochs'],
            class_weight=class_weights, verbose=0)

        self.model = model

    def get_accuracy(self, X_test, y_test):
        yhat = self.model.predict(X_test)
        yhat = yhat.squeeze()
        yhat[yhat >= 0.5] = 1
        yhat[yhat < 0.5] = 0

        score = roc_auc_score(y_test, yhat)

        return score

    def save(self):
        if not self.model_dir.exists():
            self.model_dir.mkdir(parents=True)

        self.model.save(self.model_dir / (f'{self.ticker}' + mlc.MODEL_FILE_SUFFIX))


class RidgeScikit(Model):

    def __init__(self, ticker, input_features, output_feature, X_train, y_train,
        X_test, y_test, model_dir, parameters=None, model_tag=None):

        super().__init__('RidgeClassifier', ticker=ticker, input_features=input_features,
            output_feature=output_feature, X_train=X_train, y_train=y_train, X_test=X_test,
            y_test=y_test, model_dir=model_dir, parameters=parameters, model_tag=model_tag)

    def create_model(self):

        model = RidgeClassifier(
            alpha=self.parameters['alpha'],
            fit_intercept=self.parameters['fit_intercept'],
            copy_X=self.parameters['copy_X'],
            max_iter=self.parameters['max_iter'],
            tol=self.parameters['tol'],
            class_weight=self.parameters['class_weight'],
            solver=self.parameters['solver'],
            positive=self.parameters['positive'],
            random_state=self.parameters['random_state'])

        model.fit(self.X_train, self.y_train)

        self.model = model
