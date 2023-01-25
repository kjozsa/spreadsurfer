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
low_spread_limit = order_config['low_spread_limit']
base_amount = order_config['base_amount']
recv_window = order_config['recv_window']


class OrderMaker:
    def __init__(self, exchange: ccxt.Exchange, orders_queue: asyncio.Queue, balance_watcher: BalanceWatcher, bookkeeper: Bookkeeper, price_engine: PriceEngine):
        self.exchange = exchange
        self.orders_queue = orders_queue
        self.balance_watcher = balance_watcher
        self.bookkeeper = bookkeeper

        self.nr_orders_created = 0
        self.connector_wss = BinanceWebsocketConnector()
        self.price_engine = price_engine

    async def start(self):
        await self.connector_wss.start()

        while True:
            (wave_id, event_name, frames, stabilized_hint, stabilized_at_ms) = await self.orders_queue.get()
            if orders_disabled:
                continue

            match event_name:
                case 'create':
                    try:
                        await self.create_orders(wave_id, frames, stabilized_hint, stabilized_at_ms)
                        self.nr_orders_created += 1
                    except Exception as e:
                        logger.critical('not creating order: {}', str(e))

                case 'cancel':
                    await self.cancel_orders(wave_id)
                    if max_nr_orders_limited and self.nr_orders_created >= max_nr_orders_created:
                        logger.critical('max nr of orders created already, exiting')
                        quit(1)

    async def create_orders(self, wave_id, frames, stabilized_hint, stabilized_at_ms):
        stabilized_frame = frames.tail(1)
        price_mean = stabilized_frame['price_mean'][0]

        low_price, high_price = await self.price_engine.predict(stabilized_hint, frames, stabilized_at_ms)

        buy_amount = max(base_amount, round(base_amount + base_amount * self.balance_watcher.percentage_usd(price_mean), 5))
        sell_amount = max(base_amount, round(base_amount + base_amount * self.balance_watcher.percentage_btc(price_mean), 5))

        spread = round(high_price - low_price, 3)
        if spread <= low_spread_limit:
            logger.success('order spread would be lower than configured spread limit ({}), not placing orders', low_spread_limit)
            return

        logger.success('creating orders in wave {}, buy {} at {}, sell {} at {}. Spread: {}', wave_id, buy_amount, low_price, sell_amount, high_price, spread)
        match stabilized_hint:
            case 'min':  # price is raising
                buy_order_id = await self.connector_wss.send_buy_order(self.nr_orders_created, wave_id, low_price, buy_amount, limit=True)
                sell_order_id = await self.connector_wss.send_sell_order(self.nr_orders_created, wave_id, high_price, sell_amount, limit=True, recv_window=recv_window)
                near_order = {'order_id': buy_order_id, 'price': low_price, 'type': 'LIMIT BUY', 'amount': -1 * buy_amount}
                far_order = {'order_id': sell_order_id, 'price': high_price, 'type': 'LIMIT SELL', 'amount': sell_amount}
            case 'max':  # price is dropping
                sell_order_id = await self.connector_wss.send_sell_order(self.nr_orders_created, wave_id, high_price, sell_amount, limit=True)
                buy_order_id = await self.connector_wss.send_buy_order(self.nr_orders_created, wave_id, low_price, buy_amount, limit=True, recv_window=recv_window)
                near_order = {'order_id': sell_order_id, 'price': high_price, 'type': 'LIMIT SELL', 'amount': sell_amount}
                far_order = {'order_id': buy_order_id, 'price': low_price, 'type': 'LIMIT BUY', 'amount': -1 * buy_amount}
            case _:
                raise AssertionError(f'attempt to create order with stabilized hint {stabilized_hint}')

        self.bookkeeper.save_orders(wave_id, [near_order, far_order])

    async def cancel_orders(self, wave_id):
        removed_orders = self.bookkeeper.remove_orders_by_wave(wave_id)
        if removed_orders and removed_orders[0] is not None:
            remove_order_id = removed_orders[0]['order_id']
            logger.success('cancelling near order {} in wave {}', remove_order_id, wave_id)
            try:
                await self.connector_wss.cancel_order(wave_id, remove_order_id)
            except Exception:
                pass  # ignore errors on cancelling orders because they might got fulfilled already
