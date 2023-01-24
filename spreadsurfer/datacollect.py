import atexit
import json
import os

import pandas as pd
from loguru import logger

from spreadsurfer import now_isoformat

config = json.load(open('config.json'))
datacollect_config = config['datacollect']

dump_batch_size = datacollect_config['dump_batch_size']
data_files_path = datacollect_config['data_files_path']
round_prices = datacollect_config['round_prices']
datacollect_disabled = datacollect_config['datacollect_disabled']
collect_last_n_wave_prices = config['wave']['collect_last_n_wave_prices']


def r(num):
    return round(num, round_prices)


class DataCollector:
    def __init__(self, datacollect_queue):
        logger.log('data', 'datacollect config: {}', datacollect_config)
        logger.log('data', 'collecting last {} waves\' final prices', collect_last_n_wave_prices)
        self.past_waves_final_prices = None

        if not datacollect_disabled:
            os.makedirs(data_files_path, exist_ok=True)
            filename = f'{now_isoformat()}.parquet'
            self.data_file = os.path.join(data_files_path, filename)
            logger.log('data', 'writing output to {}', self.data_file)

        self.datacollect_queue = datacollect_queue
        self.waves_collected = 0
        columns = [
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
        ]
        self.past_price_columns = [f'past_final_price_{x}' for x in range(0, collect_last_n_wave_prices)]
        columns += self.past_price_columns
        self.df = pd.DataFrame(columns=columns)

    async def start(self):
        if not datacollect_disabled:
            atexit.register(self.dump_data_to_file)

        while True:
            (wave_stabilized, stabilized_ms, frames, end_ms, end_frame, past_waves_final_prices) = await self.datacollect_queue.get()
            self.past_waves_final_prices = past_waves_final_prices

            if datacollect_disabled:
                continue

            if None in past_waves_final_prices:
                logger.log('data', 'skip collecting wave, past_final_prices not filled up yet')
                continue

            frames_data, stabilized_data, stabilized_price = await self.collect_wave_data(frames, stabilized_ms, wave_stabilized)
            last_price = end_frame['price_max'].max() if wave_stabilized == 'min' else end_frame['price_min'].min()
            last_price_delta = r(last_price - stabilized_price)
            last_price_obj = {'last_price_delta_since_stabilized': last_price_delta}

            fresh_data = dict(sorted(frames_data.items() | stabilized_data.items() | last_price_obj.items() | self.past_prices().items()))

            self.waves_collected += 1
            logger.log('data', 'wave {} collected: {}', self.waves_collected, fresh_data)
            self.df = pd.concat([self.df, pd.DataFrame([fresh_data])])

            if len(self.df) >= dump_batch_size:
                self.dump_data_to_file()

    def past_prices(self):
        if None in self.past_waves_final_prices:
            raise Exception("can't predict, past_final_prices not filled up yet")
        last_known_price = self.past_waves_final_prices[-1]
        corrected_past_waves_final_prices = [last_known_price - x for x in self.past_waves_final_prices]
        return dict(zip(self.past_price_columns, corrected_past_waves_final_prices))

    @staticmethod
    async def collect_wave_data(frames, stabilized_ms, wave_stabilized):
        stabilized_frame = frames.tail(1)
        if wave_stabilized == 'min':
            raising = True
            stabilized_price = stabilized_frame['price_max'].max()
        else:
            raising = False
            stabilized_price = stabilized_frame['price_min'].min()

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
        }
        return frames_data, stabilized_data, stabilized_price

    def dump_data_to_file(self):
        logger.log('data', '## dumping data to parquet file')
        save_df = self.df
        self.df = self.df.head(0)
        append = os.path.isfile(self.data_file)
        save_df.to_parquet(self.data_file, engine='fastparquet', append=append)
