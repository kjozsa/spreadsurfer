import json

import asyncio
import ccxt.pro as ccxt
from loguru import logger

from spreadsurfer import *
from spreadsurfer.connector_binance_wss import BinanceWebsocketConnector

# enable ccxt debug logging:
# import logging
# logging.basicConfig(level=logging.DEBUG)

order_config = json.load(open('config.json'))['orders']
logger.info('order config: {}', order_config)

test_mode = order_config['test_mode']
max_nr_orders_limited = order_config['max_nr_orders_limited']
max_nr_orders_created = order_config['max_nr_orders_created']
base_amount = order_config['base_amount']
hint_buff_factor = order_config['hint_buff_factor']
aim_above_min = order_config['aim_above_min']
aim_below_max = aim_above_min


def scientific_price_calculation(price_mean, price_min, price_max, spread, stabilized_hint):
    low_price = None
    high_price = None

    match stabilized_hint:
        case 'min':  # raising price?
            high_price = price_max * (1 + hint_buff_factor)
            low_price = price_min + aim_above_min
            if low_price >= high_price:
                low_price = price_min
        case 'max':  # lowering price?
            low_price = price_min * (1 - hint_buff_factor)
            high_price = price_max - aim_below_max
            if high_price <= low_price:
                high_price = price_max
    return round(low_price, 2), round(high_price, 2)


class OrderMaker:
    def __init__(self, exchange: ccxt.Exchange, orders_queue: asyncio.Queue, balance_watcher: BalanceWatcher):
        self.exchange = exchange
        self.orders_queue = orders_queue
        self.balance_watcher = balance_watcher

        self.active_orders = {}
        self.nr_orders_created = 0
        self.connector_wss = BinanceWebsocketConnector()

    async def start(self):
        await self.connector_wss.start()

        while True:
            (wave_id, event_name, wave_frame, stabilized_hint) = await self.orders_queue.get()
            match event_name:
                case 'create':
                    await self.create_orders(wave_id, wave_frame, stabilized_hint)
                    self.nr_orders_created += 1

                case 'cancel':
                    await self.cancel_orders(wave_id, wave_frame)
                    if max_nr_orders_limited and self.nr_orders_created >= max_nr_orders_created:
                        logger.critical('max nr of orders created already, exiting')
                        quit(1)

    async def create_orders(self, wave_id, wave_frame, stabilized_hint):
        price_mean = wave_frame['price_mean'][0]
        price_min = wave_frame['price_min'][0]
        price_max = wave_frame['price_max'][0]
        spread = wave_frame['spread'][0]

        low_price, high_price = scientific_price_calculation(price_mean, price_min, price_max, spread, stabilized_hint)
        if low_price is None or high_price is None:
            logger.error('not creating order, no clear direction received ({})', stabilized_hint)
            return

        buy_amount = max(base_amount, round(base_amount + base_amount * self.balance_watcher.percentage_usd(price_mean), 5))
        sell_amount = max(base_amount, round(base_amount + base_amount * self.balance_watcher.percentage_btc(price_mean), 5))

        logger.success('creating orders in wave {}, buy {} at {}, sell {} at {}. Spread: {}', wave_id, buy_amount, low_price, sell_amount, high_price, round(high_price - low_price, 3))
        new_orders = []
        match stabilized_hint:
            case 'min':
                await self.connector_wss.send_buy_order(wave_id, low_price, buy_amount, new_orders)
                await self.connector_wss.send_sell_order(wave_id, high_price, sell_amount, new_orders)
            case 'max':
                await self.connector_wss.send_buy_order(wave_id, low_price, buy_amount, new_orders)
                await self.connector_wss.send_sell_order(wave_id, high_price, sell_amount, new_orders)
        self.active_orders[wave_id] = new_orders

    async def cancel_orders(self, wave_id, wave_frame):
        if wave_id in self.active_orders.keys():
            self.active_orders.pop(wave_id)
            logger.success('cancelling all orders in wave {}', wave_id)
            try:
                await self.connector_wss.cancel_orders(wave_id)
            except:
                pass  # ignore errors on cancelling orders because they might got fulfilled already
