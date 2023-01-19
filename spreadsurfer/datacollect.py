from loguru import logger
import pandas as pd
from fastparquet import write
import os

from spreadsurfer import now

dump_batch_size = 2
data_files_path = './data'


class DataCollector:
    def __init__(self, datacollect_queue):
        os.makedirs(data_files_path, exist_ok=True)
        filename = f'{now()}.parquet'
        self.data_file = os.path.join(data_files_path, filename)
        logger.log('data', 'writing output to {}', self.data_file)

        self.datacollect_queue = datacollect_queue
        self.df = pd.DataFrame(columns=['stabilized_at_ms', 'stabilized_nr_trades', 'stabilized_spread', 'wave_direction', 'last_price_delta_since_stabilized'])

    async def start(self):
        while True:
            (wave_stabilized, stabilized_ms, stabilized_frame, end_ms, end_frame) = await self.datacollect_queue.get()

            if wave_stabilized == 'min':
                last_price_delta = round(end_frame['price_max'].max() - stabilized_frame['price_max'].max(), 3)
            else:
                last_price_delta = round(end_frame['price_min'].min() - stabilized_frame['price_min'].min(), 3)

            fresh_data = [{
                'stabilized_at_ms': stabilized_ms,
                'stabilized_nr_trades': stabilized_frame['nr_trades'].max(),
                'stabilized_spread': stabilized_frame['spread'].max(),
                'wave_direction': wave_stabilized,
                'last_price_delta_since_stabilized': last_price_delta
            }]
            logger.log('data', 'wave collected: {}', fresh_data)
            self.df = pd.concat([self.df, pd.DataFrame(fresh_data)])

            if len(self.df) >= dump_batch_size:
                save_df = self.df
                self.df = self.df.head(0)
                append = os.path.isfile(self.data_file)
                save_df.to_parquet(self.data_file, engine='fastparquet', append=append)
