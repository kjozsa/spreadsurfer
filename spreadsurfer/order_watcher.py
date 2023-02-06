import asyncio
import ccxt.pro as ccxt
from loguru import logger

from spreadsurfer.bookkeeper import Bookkeeper


class OrderWatcher:
    def __init__(self, exchange: ccxt.Exchange, bookkeeper: Bookkeeper):
        self.exchange = exchange
        self.bookkeeper = bookkeeper

    async def start(self):
        while True:
            await asyncio.sleep(0)

            orders = await self.exchange.watch_orders('BTC/USDT')
            for order in orders:
                logger.info('$$$$ watched order: {}', order)
                if order['info']['X'] != 'FILLED':
                    continue

                await self.bookkeeper.fulfill_order(order['info']['c'])
