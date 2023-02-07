from datetime import datetime

import asyncio
from dateutil.relativedelta import relativedelta
from loguru import logger

from spreadsurfer.bookkeeper import Bookkeeper
from spreadsurfer.connector_binance_wss import BinanceWebsocketConnector


class TimeTracker:
    def __init__(self, bookkeeper: Bookkeeper, binance_wss_connector: BinanceWebsocketConnector):
        self.bookkeeper = bookkeeper
        self.binance_wss_connector = binance_wss_connector

    async def start(self):
        start = datetime.now()
        logger.info(f'## starting at {start}')

        while True:
            try:
                await asyncio.sleep(5)
                now = datetime.now()
                logger.info(f'## running for {relativedelta(now, start)}, sweeping past orders..')

                past_orders = self.bookkeeper.past_orders_to_cancel()
                if len(past_orders) > 0:
                    logger.info('found {} past orders to cancel', len(past_orders))

            except Exception as e:
                logger.exception(e)
