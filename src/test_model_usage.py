from os import close
from pathlib import Path
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import random
from abc import ABC, abstractmethod
import sys
import numpy as np
import math
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
# from yfinance import ticker
# import time
from operator import add

from pandas._libs.tslibs.timestamps import Timestamp

import constants as c
from utils import RunTime, calculate_maximum_volume, calculate_yield_annualized, State, Trend
from db_model import DBStrategyModel, DBGenericModel

import joblib

filename = '/Users/atcha/Github/Projeto-Final/src/model.joblib'
# filename = 'model.joblib'

loaded_model = joblib.load(filename)

result = loaded_model.score([[0.9329,0.8388,1.0161,0.9066,0,0.8388],
[0.9501,0.8543,1.0348,0.9233,0,0.8543],
[0.9712,0.8733,1.0579,0.9438,1,0.8733],
[0.9878,0.8882,1.0759,0.9599,1,0.8882],
[0.9968,0.8963,1.0858,0.9687,1,0.8963],
[0.981,0.882,1.0685,0.9533,1,0.882],
[0.9951,0.8947,1.0839,0.967,1,0.8947],
[0.9679,0.8703,1.0543,0.9406,1,0.9406],
[1.063,0.8775,1.0616,0.9484,1,0.9439],
[1.0307,0.8509,1.0294,0.9196,0,0.9196],
[1.0297,0.85,1.0283,0.9187,0,0.9187],
[1.0436,0.8615,1.0422,0.9311,1,0.9311],
[1.0317,0.8517,1.0304,0.9205,0,0.9205],
[1.0839,0.8947,1.0825,0.967,1,0.9498],
[1.1274,0.9307,1.1259,1.0058,0,0.9307],
[1.129,0.932,1.1276,1.0073,0,0.932],
[1.1526,0.9515,1.1511,1.0284,0,0.9515],
[1.1509,0.9501,1.1494,1.0268,0,0.9344],
[1.1927,0.9846,1.1911,1.0641,0,0.9591],
[1.1441,0.9444,1.1426,1.0207,0,0.9444],
[1.1357,0.9375,1.1342,1.0132,0,0.9375],
[1.1403,0.9413,1.1388,1.0173,0,0.9413],
[1.1526,0.9515,1.1511,1.0284,0,0.9515],
[1.1683,0.9644,1.1668,1.0424,0,0.944],
[1.1569,0.9551,1.1554,1.0322,0,0.9551],
[1.1556,0.954,1.1541,1.0311,0,0.954],
[1.1032,0.9843,1.1018,0.9089,0,0.9518],
[1.108,0.9885,1.1065,0.9128,1,0.9419],
[1.1108,0.991,1.1093,0.9151,1,0.9579],
[1.0997,0.9811,1.0983,0.906,0,0.9487]], [[0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [0], [1], [1], [1], [1], [1], [1], [1], [1], [1], [0], [0], [0], [0]] )
print(result)

print(loaded_model.predict([[1.1556,0.954,1.1541,1.0311,0,0.954]]))
