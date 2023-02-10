import json
from glob import glob

import pandas as pd
from catboost import CatBoostRegressor
from loguru import logger
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline

from .orderbook import OrderBookWatcher
from .datacollect import DataCollector

pricing_config = json.load(open('config.json'))['pricing']
aim_above_min = pricing_config['aim_above_min']
aim_below_max = pricing_config['aim_below_max']
balance_guess_min = pricing_config['balance_guess_min']
balance_guess_max = pricing_config['balance_guess_max']


class FeatureEngineer(BaseEstimator, TransformerMixin):
    def fit(self, X, y):
        return self

    def transform(self, X, y=None):
        df = X.copy()

        for col in df.columns:
            if 'past' in col:
                df.drop(col, axis=1, inplace=True)
            if '_spread' == col[1:]:
                df.drop(col, axis=1, inplace=True)
        return df


class PriceEngine:
    def __init__(self, data_collector: DataCollector, order_book_watcher: OrderBookWatcher):
        self.data_collector = data_collector
        self.order_book_watcher = order_book_watcher

        cat_filenames = glob('*.cat')
        min_filename = [x for x in cat_filenames if 'min' in x][0]
        max_filename = [x for x in cat_filenames if 'max' in x][0]
        self.model_min = CatBoostRegressor()
        self.model_min.load_model(min_filename)
        self.model_max = CatBoostRegressor()
        self.model_max.load_model(max_filename)
        logger.log('ml', 'catboost models loaded for min: {} and for max: {}', min_filename, max_filename)

        self.pipeline_min = Pipeline(steps=[
            ('preprocessor', FeatureEngineer()),
            ('model', self.model_min)
        ])
        self.pipeline_max = Pipeline(steps=[
            ('preprocessor', FeatureEngineer()),
            ('model', self.model_max)
        ])

    async def predict(self, stabilized_hint, frames, stabilized_at_ms, gasp_stabilized):
        frames_data, stabilized_data, _ = await DataCollector.collect_wave_data(frames, stabilized_at_ms, stabilized_hint, gasp_stabilized)
        fresh_data = dict(sorted(frames_data.items() | stabilized_data.items() | self.data_collector.past_prices().items()))
        wave_direction = fresh_data.pop('wave_direction')
        logger.log('ml', 'predict input: {}', fresh_data)

        df = pd.DataFrame([fresh_data])
        if wave_direction == 'min':
            pipeline = self.pipeline_min
        elif wave_direction == 'max':
            pipeline = self.pipeline_max
        else:
            raise AssertionError('no wave_direction received for prediction!!')
        guess = pipeline.predict(df)[0]
        logger.log('ml', 'guess: {}', guess)

        stabilized_frame = frames.tail(1)
        price_mean = stabilized_frame['price_mean'][0]
        price_last = stabilized_frame['price_last'][0]

        bid = self.order_book_watcher.last_bid
        ask = self.order_book_watcher.last_ask

        match stabilized_hint:
            case 'min':  # raising price?
                low_price = price_last + aim_above_min
                if low_price < bid:
                    logger.warning('matching low price to last bid')
                    low_price = bid + aim_above_min

                high_price = low_price + (guess * balance_guess_min)

            case 'max':  # lowering price?
                high_price = price_last - aim_below_max
                if high_price > ask:
                    logger.warning('matching high price to last ask')
                    high_price = ask - aim_below_max

                low_price = high_price + (guess * balance_guess_max)
            case _:
                raise AssertionError('missing stabilized_hint')

        logger.success('wave {} - bid {}, low {}, high {}, ask {} - spread {}', wave_direction, self.order_book_watcher.last_bid, low_price, high_price, self.order_book_watcher.last_ask, high_price - low_price)

        if low_price > high_price:
            raise Exception(f'predicted price anomaly, low_price: {low_price}, high price: {high_price}, skip placing order')

        return round(low_price, 2), round(high_price, 2)
