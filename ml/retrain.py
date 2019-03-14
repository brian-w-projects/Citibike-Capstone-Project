import os
import numpy as np
import pandas as pd
from keras.models import load_model
from math import pi
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.externals import joblib
from sklearn.metrics import mean_squared_error
import argparse
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
import numpy as np
import pandas as pd
from math import pi
from sklearn.pipeline import Pipeline, make_pipeline, FeatureUnion
from sklearn.base import BaseEstimator, ClassifierMixin, TransformerMixin
from sklearn.preprocessing import StandardScaler, OneHotEncoder, FunctionTransformer, MinMaxScaler, StandardScaler
import keras
from keras.callbacks import EarlyStopping, ModelCheckpoint, TensorBoard, ReduceLROnPlateau
from keras.models import Model, load_model
from keras.layers import GRU, Dense, Dropout, concatenate, Input
from keras.optimizers import RMSprop
from random import shuffle
import warnings
import holidays
from datetime import timedelta
pd.options.mode.chained_assignment = None


class HolidaySelector(BaseEstimator, TransformerMixin):

    def __init__(self):
        hd = [date for date, name in holidays.US(years=[2013, 2014, 2015, 2016, 2017, 2018]).items()
              if name.startswith(("New Year's Day", "Washington's Birthday", "Memorial Day",
                                  "Independence Date", "Labor Day", "Thanksgiving", "Christmas Day"))]
        hd_eve = [day - timedelta(days=1) for day in hd]
        hd.extend(hd_eve)
        self.h = [str(date) for date in hd]

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        return X[['date']].applymap(lambda x: int(pd.to_datetime(x).strftime('%Y-%m-%d') in self.h))


class ColumnSelector(BaseEstimator, TransformerMixin):

    def __init__(self, columns=None):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        return X.loc[:, self.columns]


class DateTimeExtractor(BaseEstimator, TransformerMixin):

    def __init__(self, extract):
        self.extract = extract

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        return X[['date']].applymap(lambda x: float(getattr(pd.to_datetime(x), self.extract)))


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


class LagTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, amount):
        self.amount = amount

    def fit(self, X, y=None):
        return self

    def transform(self, X, y=None):
        return X[['rides']].shift(self.amount, fill_value=0)


def retrain(data):
    # constants
    ONE_DAY = 24
    lookback = 28 * ONE_DAY
    lag = 2 * ONE_DAY

    # load models
    xg = joblib.load(os.path.join('.', 'model', 'xg.pkl'))
    all_pipeline = joblib.load(os.path.join('.', 'model', 'all_pipeline.pkl'))
    time_pipeline = joblib.load(os.path.join('.', 'model', 'time_pipeline.pkl'))

    params = {'xgbregressor__eta': [.005],
              'xgbregressor__max_depth': [5],
              'xgbregressor__n_estimators': [900],
              'xgbregressor__colsample_bytree': [0.5],
              }

    print('Training XG Boost')
    clf = GridSearchCV(xg, params, cv=TimeSeriesSplit(5), scoring='neg_mean_squared_error', n_jobs=-1, verbose=5)
    clf.fit(data, data['rides'])
    joblib.dump(clf.best_estimator_, 'xg.pkl', compress=1)
    print('Training Finished')

    print('Training DL Model')
    all_X = all_pipeline.fit_transform(data)
    time_X = time_pipeline.fit_transform(data)

    joblib.dump(all_pipeline, 'all_pipeline.pkl', compress=1)
    joblib.dump(time_pipeline, 'time_pipeline.pkl', compress=1)

    start, stop = lookback+lag, len(time_X)
    input_time_X = np.zeros((stop - start, lookback // ONE_DAY, len(time_X[0])))
    input_all_X = np.zeros((stop - start, len(all_X[0])))

    for i in range(start, stop):
        input_time_X[i - start] = time_X[i - lookback - lag:i - lag:ONE_DAY]
        input_all_X[i - start] = all_X[i]

    model = load(input_time_X, input_all_X)
    model.compile(optimizer='adam', loss='mse')

    callbacks_list = [
        EarlyStopping(
            monitor='loss',
            patience=10
        ),
        ReduceLROnPlateau(
            monitor='loss',
            factor=0.1,
            patience=5
        ),
        ModelCheckpoint(
            filepath='dl.h5',
            monitor='loss',
            save_best_only=True
        )
    ]

    model.fit([input_time_X, input_all_X], data['rides'][start:stop].to_numpy(), epochs=100, batch_size=64,
              callbacks=callbacks_list, verbose=2)

    print('Training Finished')


def load(input_time_X, input_all_X):
    temporal = Input(shape=(len(input_time_X[0]), len(input_time_X[0][0])), name='temporal')
    temporal1 = GRU(32, dropout=0.5, recurrent_dropout=0.5, name='temporal_gru_1')(temporal)
    temporal1 = Dense(64, name='temporal_dense_1')(temporal1)
    temporal1 = Dropout(0.5, name='temporal_dropout_1')(temporal1)

    weather = Input(shape=(len(input_all_X[0]),), name='weather')
    weather1 = Dense(64, activation='relu', name='weather_dense_1')(weather)
    weather1 = Dropout(0.5, name='weather_dropout_1')(weather1)

    concat = concatenate([temporal1, weather1], name='concat')
    concat1 = Dense(128, activation='relu', name='concat_dense_1')(concat)
    concat1 = Dropout(0.5, name='concat_dropout_1')(concat1)
    concat1 = Dense(128, activation='relu', name='concat_dense_2')(concat1)
    concat1 = Dropout(0.5, name='concat_dropout_2')(concat1)
    output = Dense(1, name='output')(concat1)

    model = Model([temporal, weather], output)
    return model


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Retrain Citi Bike Predictions')
    parser.add_argument('-f', dest='file', help='File of Observations to Retrain On')

    results = parser.parse_args()
    print('ReTraining on: {}'.format(results.file))
    retrain(pd.read_csv(results.file))
    print('Complete')



