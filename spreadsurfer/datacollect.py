from loguru import logger
import pandas as pd

batch_size = 10_000


class DataCollector:
    def __init__(self, datacollect_queue):
        self.datacollect_queue = datacollect_queue
        self.df = pd.DataFrame(columns=['stabilized_at_ms', 'stabilized_nr_trades', 'stabilized_spread', 'wave_direction', 'last_price_delta_since_stabilized'])

    async def start(self):
        while True:
            (wave_stabilized, stabilized_ms, stabilized_frame, end_ms, end_frame) = await self.datacollect_queue.get()

            if wave_stabilized == 'min':
                last_price_delta = end_frame['price_max'].max() - stabilized_frame['price_max'].max()
            else:
                last_price_delta = end_frame['price_min'].min() - stabilized_frame['price_min'].min()

            fresh_data = [{
                'stabilized_at_ms': stabilized_ms,
                'stabilized_nr_trades': stabilized_frame['nr_trades'].max(),
                'stabilized_spread': stabilized_frame['spread'].max(),
                'wave_direction': wave_stabilized,
                'last_price_delta_since_stabilized': last_price_delta
            }]
            logger.log('data', 'wave collected: {}', fresh_data)
            self.df = pd.concat([self.df, pd.DataFrame(fresh_data)])
