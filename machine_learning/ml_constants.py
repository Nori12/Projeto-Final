# General
DATASETS_FOLDER = 'datasets'
MODELS_DIRECTORY = 'models'
TICKER_ORIENTED_MODELS_DIRECTORY = 'ticker_oriented_models'
SPECS_DIRECTORY_SUFFIX = '_model_specs'
SPECS_FILE_SUFFIX = '_model_specs.txt'
MODEL_FILE_SUFFIX = '_model.joblib'
DATASET_FILE_SUFFIX = '_dataset.csv'

MODEL_CONSTS = {'MLPClassifier': {'MODEL_DIRECTORY': 'mlp_classifier'},
    'MLPKerasClassifier': {'MODEL_DIRECTORY': 'mlp_keras_classifier'},
    'RandomForestClassifier': {'MODEL_DIRECTORY': 'random_forest_classifier'},
    'KNeighborsClassifier': {'MODEL_DIRECTORY': 'kneighbors_classifier'}}

# Random Forest
RANDOM_FOREST_FIG_SUFFIX = '_rf_features_importance.png'
