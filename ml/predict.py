import numpy as np
import pandas as pd
import os
from keras.models import load_model
from math import pi
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.externals import joblib
from sklearn.metrics import mean_squared_error


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


def generator(predictors, response, start, stop, lookback, lag, batch_size=2):
    index = start
    while True:
        if index + batch_size > stop:
            samples = np.zeros((stop - index, lookback, 1))
            meta = np.zeros((stop - index, len(predictors[0]) - 1))
            targets = np.zeros(stop - index)
        else:
            samples = np.zeros((batch_size, lookback, 1))
            meta = np.zeros((batch_size, len(predictors[0]) - 1))
            targets = np.zeros(batch_size)
        for i in range(samples.shape[0]):
            samples[i] = predictors[index - lookback - lag + i:index - lag + i, -1].reshape(-1, 1)
            meta[i] = predictors[index + i, :-1]
            targets[i] = response[index + i]

        index += batch_size
        if index >= stop:
            index = start

        yield [samples, meta], targets


def predict(data, start, stop):
    # load models
    xg = joblib.load(os.path.join('.', 'model', 'xg.pkl'))
    dl_X_pipeline = joblib.load(os.path.join('.', 'model', 'dl_X_pipeline.pkl'))
    dl_y_pipeline = joblib.load(os.path.join('.', 'model', 'dl_y_pipeline.pkl'))
    model = load_model(os.path.join('.', 'model', 'dl.h5'))

    # set constants
    lookback = 7*24 # seven days
    lag = 1*24 # one day
    batch_size = 64
    val_steps = (stop - start) // batch_size + 1

    # modify
    data['actual'] = data['rides']
    data['lag7'] = data['rides'].shift(lookback)
    data['lag1'] = data['rides'].shift(lag)

    xg_preds = xg.predict(data.iloc[start:stop, :])

    validate_gen = generator(dl_X_pipeline.transform(data), dl_y_pipeline.transform(data[['rides']]),
                             start, stop,
                             lookback, lag, batch_size)

    dl_preds = dl_y_pipeline.inverse_transform(model.predict_generator(validate_gen, steps=val_steps))

    return (xg_preds + dl_preds[:, 0]) / 2


if __name__ == '__main__':
    data = pd.read_csv('historic_data.csv')
    start = int(len(data) * 0.8)
    stop = int(len(data) * 0.9)

    results = predict(data, start, stop)
    print(mean_squared_error(results, data.iloc[start:stop, 10]))



