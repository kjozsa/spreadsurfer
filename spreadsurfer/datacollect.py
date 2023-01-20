import pickle
import signal

import asyncio
import json
import os
from datetime import datetime

import pandas as pd
import atexit
from loguru import logger

datacollect_config = json.load(open('config.json'))['datacollect']

dump_batch_size = datacollect_config['dump_batch_size']
data_files_path = datacollect_config['data_files_path']
round_prices = datacollect_config['round_prices']


def r(num):
    return round(num, round_prices)


class DataCollector:
    def __init__(self, datacollect_queue):
        logger.log('data', 'datacollect config: {}', datacollect_config)

        os.makedirs(data_files_path, exist_ok=True)
        filename = f'{datetime.utcnow().replace(microsecond=0).isoformat()}.parquet'
        self.data_file = os.path.join(data_files_path, filename)
        logger.log('data', 'writing output to {}', self.data_file)

        self.datacollect_queue = datacollect_queue
        self.df = pd.DataFrame(columns=[
            '0_amount_mean',
            '0_nr_trades',
            '0_price_delta',
            '0_spread',
            '1_amount_mean',
            '1_nr_trades',
            '1_price_delta',
            '1_spread',
            '2_amount_mean',
            '2_nr_trades',
            '2_price_delta',
            '2_spread',
            '3_amount_mean',
            '3_nr_trades',
            '3_price_delta',
            '3_spread',
            '4_amount_mean',
            '4_nr_trades',
            '4_price_delta',
            '4_spread',
            'last_price_delta_since_stabilized',
            'stabilized_amount_mean',
            'stabilized_at_ms',
            'stabilized_nr_trades',
            'stabilized_spread',
            'wave_direction'
        ])

    async def start(self):
        atexit.register(self.dump_data_to_file)

        while True:
            (wave_stabilized, stabilized_ms, frames, end_ms, end_frame) = await self.datacollect_queue.get()
            stabilized_frame = frames.tail(1)
            if wave_stabilized == 'min':
                raising = True
                stabilized_price = stabilized_frame['price_max'].max()
                last_price_delta = r(end_frame['price_max'].max() - stabilized_price)
            else:
                raising = False
                stabilized_price = stabilized_frame['price_min'].min()
                last_price_delta = r(end_frame['price_min'].min() - stabilized_price)

            frames_data_list = [{
                f'{i}_nr_trades': frame['nr_trades'],
                f'{i}_amount_mean': r(frame['amount_mean'].mean()),
                f'{i}_price_delta': r(stabilized_price - frame['price_max'].max()) if raising else r(frame['price_min'].min() - stabilized_price),
                f'{i}_spread': r(frame['spread'].max()),
            } for i, frame in enumerate([frames.iloc[i] for i in range(len(frames) - 1)])]

            frames_data = {k: v for obj in frames_data_list for (k, v) in obj.items()}

            stabilized_data = {
                'stabilized_at_ms': stabilized_ms,
                'stabilized_nr_trades': stabilized_frame['nr_trades'].max(),
                'stabilized_amount_mean': r(stabilized_frame['amount_mean'].mean()),
                'stabilized_spread': r(stabilized_frame['spread'].max()),
                'wave_direction': wave_stabilized,
                'last_price_delta_since_stabilized': last_price_delta
            }

            fresh_data = dict(sorted(frames_data.items() | stabilized_data.items()))

            logger.log('data', 'wave collected: {}', fresh_data)
            self.df = pd.concat([self.df, pd.DataFrame([fresh_data])])

            if len(self.df) >= dump_batch_size:
                self.dump_data_to_file()

    def dump_data_to_file(self):
        logger.log('data', '## dumping data to parquet file')
        save_df = self.df
        self.df = self.df.head(0)
        append = os.path.isfile(self.data_file)
        save_df.to_parquet(self.data_file, engine='fastparquet', append=append)
