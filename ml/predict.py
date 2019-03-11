import os
import numpy as np
import pandas as pd
from keras.models import load_model
from math import pi
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.externals import joblib
from sklearn.metrics import mean_squared_error
from pandas import DataFrame
import argparse
pd.options.mode.chained_assignment = None


class ColumnSelector(BaseEstimator, TransformerMixin):

    def __init__(self, columns=None):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        return X.loc[:, self.columns]


class YearExtractor(BaseEstimator, TransformerMixin):

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        return X.applymap(lambda x: float(pd.to_datetime(x).year))


class CosExtractor(BaseEstimator, TransformerMixin):

    def fit(self, X, y=None):
        self.unique = X.nunique()
        return self

    def transform(self, X, y=None):
        return X.apply(lambda x: np.round(np.cos(x * pi * 2 / self.unique), 5), axis=1)


class SinExtractor(BaseEstimator, TransformerMixin):

    def fit(self, X, y=None):
        self.unique = X.nunique()
        return self

    def transform(self, X, y=None):
        return X.apply(lambda x: np.round(np.sin(x * pi * 2 / self.unique), 5), axis=1)


def predict(data):
    # constants
    ONE_DAY = 24
    lookback = 28 * ONE_DAY
    lag = 2 * ONE_DAY

    # load models
    xg = joblib.load(os.path.join('.', 'model', 'xg.pkl'))
    all_pipeline = joblib.load(os.path.join('.', 'model', 'all_pipeline.pkl'))
    time_pipeline = joblib.load(os.path.join('.', 'model', 'time_pipeline.pkl'))
    model = load_model(os.path.join('.', 'model', 'dl.h5'))

    # modify
    data['actual'] = data['rides']
    data['lag7'] = data['rides'].shift(7*24)
    data['lag1'] = data['rides'].shift(2*24)

    xg_predictions = xg.predict(data[lookback+lag:])

    all_X = all_pipeline.transform(data)
    time_X = time_pipeline.transform(data)

    start, stop = lookback+lag, len(time_X)
    input_time_X = np.zeros((stop - start, lookback // ONE_DAY, len(time_X[0])))
    input_all_X = np.zeros((stop - start, len(all_X[0])))

    for i in range(start, stop):
        input_time_X[i - start] = time_X[i - lookback - lag:i - lag:ONE_DAY]
        input_all_X[i - start] = all_X[i]

    dl_predictions = model.predict([input_time_X, input_all_X])

    return (xg_predictions + dl_predictions[:, 0]) / 2


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Return CitiBike Predictions')
    parser.add_argument('-f', dest='file', help='File of Observations to Make Predictions On')

    results = parser.parse_args()
    if results.file is None:
        print('Using historica data for demonstrative purposes')
        data = pd.read_csv(os.path.join('.', 'historic_data.csv'))
        sample = int(len(data)*0.9)

        predictions = predict(data.iloc[sample:, ])
        print('MSE: {}'.format(mean_squared_error(predictions, data.iloc[sample+30*24:, 10])))
    else:
        print('Predicting on: {}'.format(results.file))
        predictions = predict(pd.read_csv(results.file))
        np.savetxt('predictions.csv', predictions, delimiter=',')
        print('Complete')



