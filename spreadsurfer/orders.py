import json

import asyncio
import ccxt.pro as ccxt
from loguru import logger

from spreadsurfer import *
from spreadsurfer.connector_binance_wss import BinanceWebsocketConnector
from spreadsurfer.price_engine import PriceEngine
from spreadsurfer.bookkeeper import Bookkeeper

# enable ccxt debug logging:
# import logging
# logging.basicConfig(level=logging.DEBUG)

order_config = json.load(open('config.json'))['orders']
logger.info('order config: {}', order_config)

orders_disabled = order_config['orders_disabled']
max_nr_orders_limited = order_config['max_nr_orders_limited']
max_nr_orders_created = order_config['max_nr_orders_created']
base_amount = order_config['base_amount']


class OrderMaker:
    def __init__(self, exchange: ccxt.Exchange, orders_queue: asyncio.Queue, balance_watcher: BalanceWatcher, bookkeeper: Bookkeeper):
        self.exchange = exchange
        self.orders_queue = orders_queue
        self.balance_watcher = balance_watcher
        self.bookkeeper = bookkeeper

        self.nr_orders_created = 0
        self.connector_wss = BinanceWebsocketConnector()
        self.price_engine = PriceEngine()

    async def start(self):
        await self.connector_wss.start()

        while True:
            (wave_id, event_name, frames, stabilized_hint, stabilized_at_ms) = await self.orders_queue.get()
            if orders_disabled:
                continue

            match event_name:
                case 'create':
                    await self.create_orders(wave_id, frames, stabilized_hint, stabilized_at_ms)
                    self.nr_orders_created += 1

                case 'cancel':
                    await self.cancel_orders(wave_id)
                    if max_nr_orders_limited and self.nr_orders_created >= max_nr_orders_created:
                        logger.critical('max nr of orders created already, exiting')
                        quit(1)

    async def create_orders(self, wave_id, frames, stabilized_hint, stabilized_at_ms):
        stabilized_frame = frames.tail(1)
        price_mean = stabilized_frame['price_mean'][0]

        low_price, high_price = await self.price_engine.predict(stabilized_hint, frames, stabilized_at_ms)
        if low_price is None or high_price is None:
            logger.critical('not creating order, no clear direction received ({})', stabilized_hint)
            return

        buy_amount = max(base_amount, round(base_amount + base_amount * self.balance_watcher.percentage_usd(price_mean), 5))
        sell_amount = max(base_amount, round(base_amount + base_amount * self.balance_watcher.percentage_btc(price_mean), 5))

        logger.success('creating orders in wave {}, buy {} at {}, sell {} at {}. Spread: {}', wave_id, buy_amount, low_price, sell_amount, high_price, round(high_price - low_price, 3))
        new_orders = []
        match stabilized_hint:
            case 'min':  # price is raising
                await self.connector_wss.send_sell_order(self.nr_orders_created, wave_id, high_price, sell_amount, new_orders, limit=True)
                await self.connector_wss.send_buy_order(self.nr_orders_created, wave_id, low_price, buy_amount, new_orders, limit=True)
            case 'max':  # price is dropping
                await self.connector_wss.send_buy_order(self.nr_orders_created, wave_id, low_price, buy_amount, new_orders, limit=True)
                await self.connector_wss.send_sell_order(self.nr_orders_created, wave_id, high_price, sell_amount, new_orders, limit=True)
        self.bookkeeper.save_orders(wave_id, new_orders)

    async def cancel_orders(self, wave_id):
        if self.bookkeeper.remove_orders_from_wave(wave_id):
            logger.success('cancelling all orders in wave {}', wave_id)
            try:
                await self.connector_wss.cancel_orders(wave_id)
            except:
                pass  # ignore errors on cancelling orders because they might got fulfilled already
