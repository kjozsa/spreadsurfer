import asyncio
import json

import pandas as pd
import shortuuid
from loguru import logger

from .timeutils import now, timedelta_ms

wave_config = json.load(open('config.json'))['wave']
logger.info('wave detection config: {}', wave_config)
wave_min_length = wave_config['min_length']
wave_investigate_length = wave_config['investigate_length']
wave_stabilized_threshold = wave_config['stabilized_threshold']
max_delta_ms_to_create_order = wave_config['max_delta_ms_to_create_order']


class WaveHandler:
    def __init__(self, wave_events_queue: asyncio.Queue, orders_queue: asyncio.Queue):
        self.wave_events_queue = wave_events_queue
        self.orders_queue = orders_queue

        self.wave_start = None
        self.wave = pd.DataFrame(columns=['ms' 'price', 'amount'])
        self.wave_id = None
        self.wave_stabilized = None
        self.wave_stabilized_frame = None
        self.wave_running = True
        self.wave_length_ms = None

    async def start(self):
        while True:
            (event_name, wave_frame) = await self.wave_events_queue.get()
            match event_name:
                case 'start':
                    self.start_wave()
                case 'frame':
                    await self.receive_frame(wave_frame)
                case 'end':
                    await self.end_wave(wave_frame)
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

    async def end_wave(self, wave_frame):
        last_wave = self.wave.tail(1)
        await self.orders_queue.put((self.wave_id, 'cancel', last_wave, None))
        self.wave_length_ms = timedelta_ms(now(), self.wave_start)
        logger.warning('ending wave {}, wave length was {} ms', self.wave_id, self.wave_length_ms)

        self.wave_stabilized = None
        self.wave_stabilized_frame = None
        self.wave_running = False

    async def check_stabilized(self, wave_frame):
        if self.wave_running and len(self.wave) > wave_min_length:
            delta_ms = timedelta_ms(now(), self.wave_start) if self.wave_start else 0
            if delta_ms > max_delta_ms_to_create_order:
                logger.trace('delta_ms is {}, making no order in this wave', delta_ms)
                return

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
                logger.log('magenta', 'WAVE IS CHANGING, cancelling orders..')
                await self.orders_queue.put((self.wave_id, 'cancel', wave_frame, None))

    async def stabilized(self, min_or_max, delta_ms, wave_frame):
        self.wave_stabilized = min_or_max
        logger.info('wave {} stabilized in {} ms', min_or_max.upper(), delta_ms)
        self.wave_stabilized_frame = wave_frame
        await self.orders_queue.put((self.wave_id, 'create', wave_frame, min_or_max))
