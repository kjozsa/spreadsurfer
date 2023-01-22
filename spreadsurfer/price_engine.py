import json
from glob import glob

import pandas as pd
from catboost import CatBoostRegressor
from loguru import logger
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline

from .datacollect import DataCollector

order_config = json.load(open('config.json'))['orders']
aim_above_min = order_config['aim_above_min']
aim_below_max = aim_above_min


class FeatureEngineer(BaseEstimator, TransformerMixin):
    def fit(self, X, y):
        return self

    def transform(self, X, y=None):
        df = X.copy()
        df.loc[df['wave_direction'] == 'min', 'wave_direction'] = 1
        df.loc[df['wave_direction'] == 'max'] = -1
        df = df.astype({"wave_direction": 'float64'})
        return df


class PriceEngine:
    def __init__(self):
        cat_filename = glob('*.cat')[0]
        self.model = CatBoostRegressor()
        self.model.load_model(cat_filename)
        logger.log('ml', 'catboost model loaded from {}', cat_filename)

        self.pipeline = Pipeline(steps=[
            ('preprocessor', FeatureEngineer()),
            # ('model', RandomForestRegressor(n_estimators=50, random_state=0))
            ('model', self.model)
        ])

    async def predict(self, stabilized_hint, frames, stabilized_at_ms):
        frames_data, stabilized_data, _ = await DataCollector.collect_wave_data(frames, stabilized_at_ms, stabilized_hint)
        fresh_data = dict(sorted(frames_data.items() | stabilized_data.items()))
        logger.log('ml', 'predict input: {}', fresh_data)

        df = pd.DataFrame([fresh_data])
        guess = self.pipeline.predict(df)[0]
        logger.log('ml', 'guess: {}', guess)

        stabilized_frame = frames.tail(1)
        price_min = stabilized_frame['price_min'][0]
        price_max = stabilized_frame['price_max'][0]

        match stabilized_hint:
            case 'min':  # raising price?
                low_price = price_max + aim_above_min
                high_price = low_price + guess

            case 'max':  # lowering price?
                high_price = price_min - aim_below_max
                low_price = high_price - guess
            case _:
                raise AssertionError('missing stabilized_hint')

        if low_price > high_price:
            logger.critical('pricing anomaly detected, low_price is higher than high_price')
            return None, None

        return round(low_price, 2), round(high_price, 2)

