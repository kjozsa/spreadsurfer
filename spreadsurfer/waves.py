import collections

import asyncio
import json

import pandas as pd
import shortuuid
from loguru import logger

from .orderbook import OrderBookWatcher
from .timeutils import now, timedelta_ms

config = json.load(open('config.json'))
wave_config = config['wave']
logger.info('wave detection config: {}', wave_config)
wave_min_length = wave_config['min_length']
wave_investigate_length = wave_config['investigate_length']
wave_stabilized_threshold = wave_config['stabilized_threshold']
max_delta_ms_to_create_order = wave_config['max_delta_ms_to_create_order']
collect_last_n_frames = wave_config['collect_last_n_frames']
collect_last_n_wave_prices = wave_config['collect_last_n_wave_prices']

skip_order_on_spread_below = config['orders']['skip_order_on_spread_below']
skip_order_on_spread_above = config['orders']['skip_order_on_spread_above']


class WaveHandler:
    def __init__(self, order_book: OrderBookWatcher, wave_events_queue: asyncio.Queue, orders_queue: asyncio.Queue, datacollect_queue: asyncio.Queue):
        self.order_book = order_book
        self.wave_events_queue = wave_events_queue
        self.orders_queue = orders_queue
        self.datacollect_queue = datacollect_queue
        self.past_waves_final_prices = collections.deque(collect_last_n_wave_prices * [None], collect_last_n_wave_prices)

        self.wave_start = None
        self.wave = pd.DataFrame(columns=['ms' 'price', 'amount'])
        self.wave_id = None
        self.gasp_stabilized = None
        self.wave_stabilized = None
        self.wave_stabilized_at_ms = None
        self.wave_stabilized_at_frame = None
        self.wave_stabilized_frame = None
        self.wave_running = True

    async def start(self):
        while True:
            await asyncio.sleep(0)

            (event_name, wave_frame) = await self.wave_events_queue.get()
            match event_name:
                case 'start':
                    self.start_wave()
                case 'frame':
                    await self.receive_frame(wave_frame)
                case 'end':
                    await self.end_wave(self.wave.tail(1))
                case _:
                    raise AssertionError(f'invalid event received: {event_name}')

    def start_wave(self):
        self.wave_start = now()
        self.wave_id = shortuuid.uuid()
        logger.warning('starting new wave {}', self.wave_id)
        self.wave = self.wave.head(0)
        self.wave_running = True

    async def receive_frame(self, wave_frame):
        self.wave = pd.concat([self.wave, wave_frame])
        await self.check_stabilized(wave_frame)

    async def end_wave(self, end_frame):
        last_frame = self.wave.tail(1)
        await self.orders_queue.put((self.wave_id, 'cancel', last_frame, None, None, None))
        wave_length_ms = timedelta_ms(now(), self.wave_start)
        if last_frame.empty:
            return
        last_price = last_frame['price_max'].max() if self.wave_stabilized == 'min' else last_frame['price_min'].min()
        logger.warning('ending wave {}, wave length was {} ms', self.wave_id, wave_length_ms)

        if self.wave_stabilized:
            await self.send_to_datacollect(end_frame, wave_length_ms)
        else:
            logger.warning('$$$ skip collecting wave data, stabilized {}', self.wave_stabilized)

        self.past_waves_final_prices.append(last_price)
        self.wave_stabilized = None
        self.wave_stabilized_at_ms = None
        self.wave_stabilized_frame = None
        self.wave_running = False

    async def send_to_datacollect(self, wave_end_frame, wave_length_ms):
        end = self.wave_stabilized_at_frame
        start = end - collect_last_n_frames
        frames = self.wave[start:end]
        logger.trace('$$$$$$ sending {} frames', len(frames))
        await self.datacollect_queue.put((self.wave_stabilized, self.wave_stabilized_at_ms, self.gasp_stabilized, frames, wave_length_ms, wave_end_frame, self.past_waves_final_prices))

    async def check_stabilized(self, wave_frame):
        if not self.wave_running or len(self.wave) <= wave_min_length:
            return

        delta_ms = timedelta_ms(now(), self.wave_start) if self.wave_start else 0
        last_frames = self.wave[-wave_investigate_length:]
        if len(last_frames) != wave_investigate_length: raise AssertionError
        wave_min_stabilized = abs(last_frames['price_min'].min() - last_frames['price_min'].mean()) < wave_stabilized_threshold
        wave_max_stabilized = abs(last_frames['price_max'].max() - last_frames['price_max'].mean()) < wave_stabilized_threshold

        if not self.wave_stabilized and not (wave_min_stabilized and wave_max_stabilized):
            if wave_min_stabilized:
                await self.stabilized('min', delta_ms, wave_frame)

            if wave_max_stabilized:
                await self.stabilized('max', delta_ms, wave_frame)

        if (self.wave_stabilized == 'min' and not wave_min_stabilized) or (self.wave_stabilized == 'max' and not wave_max_stabilized):
            await self.orders_queue.put((self.wave_id, 'cancel_instant', wave_frame, None, None, None))
            self.wave_stabilized = None
            logger.log('magenta', 'WAVE IS CHANGING, cancelling orders, attempting to restabilize..')
            await self.send_to_datacollect(wave_frame, delta_ms)

    async def stabilized(self, min_or_max, delta_ms, stabilized_frame):
        self.wave_stabilized = min_or_max
        self.wave_stabilized_at_ms = delta_ms
        self.wave_stabilized_at_frame = len(self.wave)
        self.gasp_stabilized = self.order_book.last_gasp

        logger.info('wave {} stabilized in {} ms', min_or_max.upper(), delta_ms)
        self.wave_stabilized_frame = stabilized_frame

        if self.shall_create_order(stabilized_frame, delta_ms):
            end = self.wave_stabilized_at_frame
            start = end - collect_last_n_frames
            frames = self.wave[start:end]
            await self.orders_queue.put((self.wave_id, 'create', frames, min_or_max, delta_ms, self.gasp_stabilized))

    def shall_create_order(self, stabilized_frame, delta_ms):
        create_order = True
        if delta_ms > max_delta_ms_to_create_order:
            logger.log('magenta', 'delta_ms {} is larger than {}, making no order in this wave', delta_ms, max_delta_ms_to_create_order)
            create_order = False

        elif stabilized_frame['spread'].mean() < skip_order_on_spread_below:
            logger.log('magenta', 'skipping order, stabilized spread is too small: {}', stabilized_frame['spread'])
            create_order = False

        elif stabilized_frame['spread'].mean() > skip_order_on_spread_above:
            logger.log('magenta', 'skipping order, stabilized spread is too large: {}', stabilized_frame['spread'])
            create_order = False

        else:
            start_frame = self.wave.head(1)
            if self.wave_stabilized == 'min':
                stabilized_price = stabilized_frame['price_max'].max()
                start_delta = stabilized_price - start_frame['price_max'].max()

            elif self.wave_stabilized == 'max':
                stabilized_price = stabilized_frame['price_min'].min()
                start_delta = start_frame['price_min'].min() - stabilized_price
            else:
                raise AssertionError(f'wave stabilized is expected to be min or max, not {self.wave_stabilized}')

            # if start_delta > 4:
            #     logger.log('magenta', 'skipping order, stabilized price delta from start_frame is already too high')
            #     create_order = False

        return create_order
