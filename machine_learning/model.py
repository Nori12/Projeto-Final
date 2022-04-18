import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import pandas as pd
from abc import ABC, abstractmethod
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import RobustScaler
from sklearn.preprocessing import MinMaxScaler
from imblearn.under_sampling import TomekLinks
from imblearn.under_sampling import EditedNearestNeighbours
from imblearn.over_sampling import SMOTE
from sklearn.metrics import confusion_matrix
from sklearn.metrics import roc_auc_score
from keras.layers import Dense
from keras.models import Sequential
import joblib
import matplotlib.pyplot as plt
import numpy as np
import os, shutil
from sklearn.tree import export_graphviz
from subprocess import call

sys.path.insert(1, str(Path(__file__).parent.parent/'src'))
import constants as c
import config_reader as cr
import ml_constants as mlc

# Configure Logging
logger = logging.getLogger(__name__)

log_path = Path(__file__).parent.parent / c.LOG_PATH / c.LOG_FILENAME

file_handler = RotatingFileHandler(log_path, maxBytes=c.LOG_FILE_MAX_SIZE, backupCount=10)
formatter = logging.Formatter(c.LOG_FORMATTER_STRING)
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

file_handler.setLevel(logging.DEBUG)
logger.setLevel(logging.DEBUG)

class PseudoModel(ABC):

    @abstractmethod
    def __init__(self, model_type, ticker, features, output_feature, X_train, y_train,
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
    def features(self):
        pass

    @features.setter
    @abstractmethod
    def features(self, features):
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
    def save_best(self):
        pass

    @save_best.setter
    @abstractmethod
    def save_best(self, save_best):
        pass

    @property
    @abstractmethod
    def generate_specs_file(self):
        pass

    @generate_specs_file.setter
    @abstractmethod
    def generate_specs_file(self, generate_specs_file):
        pass

    @property
    @abstractmethod
    def specs_directory_path(self):
        pass

    @property
    @abstractmethod
    def model(self):
        pass


    @abstractmethod
    def create_model(self):
        pass

    @abstractmethod
    def _reset_specs_directory(self):
        pass

    @abstractmethod
    def save (self):
        pass

    @abstractmethod
    def test_accuracy(X_test, y_test):
        pass

class Model(PseudoModel):

    def __init__(self, model_type, ticker, features, output_feature, X_train, y_train,
        X_test, y_test, parameters=None):

        self._model_type = model_type
        self._ticker = ticker
        self._features = features
        self._output_feature = output_feature
        self._parameters = parameters

        self._X_train = X_train
        self._y_train = y_train
        self._X_test = X_test
        self._y_test = y_test

        self._specs_directory_path = Path(__file__).parent / mlc.MODELS_DIRECTORY / \
            mlc.TICKER_ORIENTED_MODELS_DIRECTORY / self.model_type / \
            (f'{ticker}' + mlc.SPECS_DIRECTORY_SUFFIX)

        self._model = None
        self._statistics = {'training_set_acc': None, 'test_set_acc': None, 'training_false_neg': None,
            'training_true_neg': None, 'training_false_pos': None, 'training_true_pos': None,
            'test_false_neg': None, 'test_true_neg': None, 'test_false_pos': None,
            'test_true_pos': None}

    @property
    def model_type(self):
        return self._model_type

    @model_type.setter
    def model_type(self, model_type):
        self._model_type = model_type

    @property
    def ticker(self):
        return self.ticker

    @ticker.setter
    def ticker(self, ticker):
        self._ticker = ticker

    @property
    def features(self):
        return self._features

    @features.setter
    def features(self, features):
        self._features = features

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
    def save_best(self):
        return self._save_best

    @save_best.setter
    def save_best(self, save_best):
        self._save_best = save_best

    @property
    def generate_specs_file(self):
        return self._generate_specs_file

    @generate_specs_file.setter
    def generate_specs_file(self, generate_specs_file):
        self._generate_specs_file = generate_specs_file

    @property
    def specs_directory_path(self):
        return self._specs_directory_path

    @property
    def model(self):
        return self._model


    def create_model(self):
        pass

    def save(self):
        shortname = '_'.join(c for c in self.model_type if c.isupper())

        joblib.dump(self.model, self.specs_directory_path /
            (f'{self.ticker}' + shortname + mlc.MODEL_FILE_SUFFIX))

    def test_accuracy(X_test, y_test):
        pass

    def _reset_specs_directory(self):
        if self.specs_directory_path.exists():
            shutil.rmtree(self.specs_directory_path)

        self.specs_directory_path.mkdir(parents=True)



class MLP(Model):

    def __init__(self, ticker, features, output_feature, X_train, y_train,
        X_test, y_test, parameters=None):

        super.__init__('MLPClassifier', ticker, features, output_feature, X_train, y_train,
        X_test, y_test, parameters=parameters)

    def create_model(self):
        pass

    def test_accuracy(X_test, y_test):
        pass