import json

import asyncio
import ccxt.pro as ccxt
from loguru import logger

from spreadsurfer import *
from spreadsurfer.bookkeeper import Bookkeeper
from spreadsurfer.connector_binance_wss import BinanceWebsocketConnector
from spreadsurfer.price_engine import PriceEngine

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
cancel_far_order_after_ms = order_config['cancel_far_order_after_ms']


class OrderMaker:
    def __init__(self, exchange: ccxt.Exchange, orders_queue: asyncio.Queue, balance_watcher: BalanceWatcher, bookkeeper: Bookkeeper, price_engine: PriceEngine, binance_wss_connector: BinanceWebsocketConnector):
        self.exchange = exchange
        self.orders_queue = orders_queue
        self.balance_watcher = balance_watcher
        self.bookkeeper = bookkeeper

        self.nr_orders_created = 0
        self.connector_wss = binance_wss_connector
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
                        logger.error('failed to create order: {}', str(e))

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
                buy_order_id, timestamp_created_ms = await self.connector_wss.send_buy_order(self.nr_orders_created, wave_id, low_price, buy_amount, limit=True, recv_window=recv_window)
                sell_order_id, timestamp_created_ms = await self.connector_wss.send_sell_order(self.nr_orders_created, wave_id, high_price, sell_amount, limit=True, recv_window=recv_window)
                near_order = {'order_id': buy_order_id, 'price': low_price, 'type': 'LIMIT BUY', 'amount': -1 * buy_amount, 'timestamp_created_ms': timestamp_created_ms, 'wave_id': wave_id, 'near_far': 'near'}
                far_order = {'order_id': sell_order_id, 'price': high_price, 'type': 'LIMIT SELL', 'amount': sell_amount, 'timestamp_created_ms': timestamp_created_ms, 'wave_id': wave_id, 'near_far': 'far'}
            case 'max':  # price is dropping
                sell_order_id, timestamp_created_ms = await self.connector_wss.send_sell_order(self.nr_orders_created, wave_id, high_price, sell_amount, limit=True, recv_window=recv_window)
                buy_order_id, timestamp_created_ms = await self.connector_wss.send_buy_order(self.nr_orders_created, wave_id, low_price, buy_amount, limit=True, recv_window=recv_window)
                near_order = {'order_id': sell_order_id, 'price': high_price, 'type': 'LIMIT SELL', 'amount': sell_amount, 'timestamp_created_ms': timestamp_created_ms, 'wave_id': wave_id, 'near_far': 'near'}
                far_order = {'order_id': buy_order_id, 'price': low_price, 'type': 'LIMIT BUY', 'amount': -1 * buy_amount, 'timestamp_created_ms': timestamp_created_ms, 'wave_id': wave_id, 'near_far': 'far'}
            case _:
                raise AssertionError(f'attempt to create order with stabilized hint {stabilized_hint}')

        self.bookkeeper.save_orders([near_order, far_order])

    async def cancel_orders(self, wave_id):
        for order in self.bookkeeper.remove_orders_by_wave(wave_id):
            order_id = order['order_id']

            if order['near_far'] == 'near':
                await self.cancel_single_order(order)

            elif order['near_far'] == 'far':
                asyncio.create_task(self.delay(cancel_far_order_after_ms, self.cancel_single_order(order)))
                pass
            else:
                raise AssertionError(f'order {order_id} near_far parameters is unknown: {order["near_far"]}')

    async def cancel_single_order(self, order):
        order_id = order['order_id']
        wave_id = order['wave_id']
        try:
            logger.success('cancelling {} order {} in wave {}', order['near_far'], order_id, wave_id)
            await self.connector_wss.cancel_order(order_id)
        except Exception:
            logger.error('failed to cancel order {} in wave {}, cancelling ALL orders instead', order_id, wave_id)
            await self.connector_wss.cancel_all_orders(wave_id)
            pass  # ignore errors on cancelling orders because they might got fulfilled already

    @staticmethod
    async def delay(ms, coro):
        await asyncio.sleep(ms / 1000)
        await coro
