import signal

import asyncio
import json
import os
from datetime import datetime

import pandas as pd
from loguru import logger

datacollect_config = json.load(open('config.json'))['datacollect']

dump_batch_size = datacollect_config['dump_batch_size']
data_files_path = datacollect_config['data_files_path']
round_prices = datacollect_config['round_prices']


class DataCollector:
    def __init__(self, datacollect_queue):
        logger.log('data', 'datacollect config: {}', datacollect_config)

        os.makedirs(data_files_path, exist_ok=True)
        filename = f'{datetime.utcnow().replace(microsecond=0).isoformat()}.parquet'
        self.data_file = os.path.join(data_files_path, filename)
        logger.log('data', 'writing output to {}', self.data_file)

        self.datacollect_queue = datacollect_queue
        self.df = pd.DataFrame(columns=['stabilized_at_ms', 'stabilized_nr_trades', 'stabilized_amount_mean', 'stabilized_amount_max', 'stabilized_spread', 'wave_direction', 'last_price_delta_since_stabilized'])

    async def start(self):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGTERM, loop.create_task, self.dump_data_to_file())

        while True:
            (wave_stabilized, stabilized_ms, stabilized_frame, end_ms, end_frame) = await self.datacollect_queue.get()

            if wave_stabilized == 'min':
                last_price_delta = round(end_frame['price_max'].max() - stabilized_frame['price_max'].max(), round_prices)
            else:
                last_price_delta = round(end_frame['price_min'].min() - stabilized_frame['price_min'].min(), round_prices)

            fresh_data = [{
                'stabilized_at_ms': stabilized_ms,
                'stabilized_nr_trades': stabilized_frame['nr_trades'].max(),
                'stabilized_amount_mean': round(stabilized_frame['amount_mean'].mean(), round_prices),
                'stabilized_spread': round(stabilized_frame['spread'].max(), round_prices),
                'wave_direction': wave_stabilized,
                'last_price_delta_since_stabilized': last_price_delta
            }]
            logger.log('data', 'wave collected: {}', fresh_data)
            self.df = pd.concat([self.df, pd.DataFrame(fresh_data)])

            if len(self.df) >= dump_batch_size:
                self.dump_data_to_file()

    def dump_data_to_file(self):
        logger.log('data', '## dumping data to parquet file')
        save_df = self.df
        self.df = self.df.head(0)
        append = os.path.isfile(self.data_file)
        save_df.to_parquet(self.data_file, engine='fastparquet', append=append)
