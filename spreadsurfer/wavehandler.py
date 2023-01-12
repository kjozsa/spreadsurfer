import asyncio
import json

import pandas as pd
from loguru import logger

from .timeutils import now, timedelta_ms

wave_config = json.load(open('config.json'))['wave']
logger.info('wave detection config: {}', wave_config)
wave_min_length = wave_config['min_length']
wave_investigate_length = wave_config['investigate_length']
wave_stabilized_threshold = wave_config['stabilized_threshold']
wave_long_running_length = wave_config['long_running_length']


class WaveHandler:
    def __init__(self, wave_events_queue: asyncio.Queue):
        self.wave_events_queue = wave_events_queue

        self.wave_start = None
        self.wave = pd.DataFrame(columns=['ms' 'price', 'amount'])
        self.wave_stabilized = None
        self.wave_stabilized_at_frame = None
        self.wave_stabilized_at_price = None
        self.wave_running = True
        self.wave_long_running = False
        self.wave_length_ms = None

    async def start(self):
        while True:
            (event_name, data) = await self.wave_events_queue.get()
            match event_name:
                case 'start':
                    self.start_wave(data)
                case 'frame':
                    self.receive_frame(data)
                case 'end':
                    self.end_wave(data)
                case _:
                    raise AssertionError(f'invalid event received: {event_name}')

    def start_wave(self, data):
        logger.warning('starting new wave')
        self.wave = self.wave.head(0)
        self.wave_start = now()
        self.wave_running = True

    def receive_frame(self, wave_frame):
        self.wave = pd.concat([self.wave, wave_frame])
        self.check_stabilized(wave_frame)

    def end_wave(self, data):
        self.wave_length_ms = timedelta_ms(now(), self.wave_start)
        logger.warning(f'ending wave, wave length was {self.wave_length_ms} ms')
        last_price = self.wave.tail(1)['price_mean'][0]
        if self.wave_stabilized_at_price is not None:
            logger.error(f'delta(end_price - last_stabilized) = {round(last_price - self.wave_stabilized_at_price, 4)}')

        self.wave_stabilized = None
        self.wave_stabilized_at_frame = None
        self.wave_stabilized_at_price = None
        self.wave_running = False
        self.wave_long_running = False

    def check_stabilized(self, wave_frame):
        # check wave stabilization

        if self.wave_running and len(self.wave) > wave_min_length:
            last_frames = self.wave[-wave_investigate_length:]
            if len(last_frames) != wave_investigate_length: raise AssertionError
            wave_min_stabilized = abs(last_frames['price_min'].min() - last_frames['price_min'].mean()) < wave_stabilized_threshold
            wave_max_stabilized = abs(last_frames['price_max'].max() - last_frames['price_max'].mean()) < wave_stabilized_threshold

            if not self.wave_stabilized and not (wave_min_stabilized and wave_max_stabilized):
                if wave_min_stabilized:
                    self.wave_stabilized = "min"
                    logger.info('wave MIN stabilized in {} ms', timedelta_ms(now(), self.wave_start))
                    self.wave_stabilized_at_price = wave_frame['price_mean'][0]
                    self.wave_stabilized_at_frame = len(self.wave)
                if wave_max_stabilized:
                    self.wave_stabilized = "max"
                    logger.info('wave MAX stabilized in {} ms', timedelta_ms(now(), self.wave_start))
                    self.wave_stabilized_at_price = wave_frame['price_mean'][0]
                    self.wave_stabilized_at_frame = len(self.wave)

            if self.wave_stabilized and not self.wave_long_running:
                if len(self.wave) - self.wave_stabilized_at_frame > wave_long_running_length:
                    logger.info("LONG RUNNING WAVE! Attempting to restabilize..")
                    self.wave_long_running = True
                    self.wave_stabilized = False
