import json
import asyncio
from loguru import logger
import ccxt.pro as ccxt
from spreadsurfer import *

# enable ccxt debug logging:
# import logging
# logging.basicConfig(level=logging.DEBUG)

test_mode = True
max_nr_orders_created = 50


def scientific_price_calculation(price_mean, price_min, price_max, spread, stabilized_hint):
    low_price = price_min
    high_price = price_max
    hint_buff_factor = 0.000065

    match stabilized_hint:
        case 'min':  # raising price?
            logger.warning("buffing high_price")
            high_price = high_price * (1 + hint_buff_factor)
        case 'max':  # lowering price?
            logger.warning("buffing low_price")
            low_price = low_price * (1 - hint_buff_factor)
    return low_price, high_price


class OrderMaker:
    def __init__(self, exchange: ccxt.Exchange, orders_queue: asyncio.Queue, balance_watcher: BalanceWatcher):
        self.exchange = exchange
        self.orders_queue = orders_queue
        self.balance_watcher = balance_watcher

        self.active_orders = {}
        self.nr_orders_created = 0
        self.nr_orders_fulfilled = 0

    async def start(self):
        while True:
            (wave_id, event_name, wave_frame, stabilized_hint) = await self.orders_queue.get()
            match event_name:
                case 'create':
                    if self.nr_orders_created < max_nr_orders_created:
                        await self.create_orders(wave_id, wave_frame, stabilized_hint)
                        self.nr_orders_created += 1
                    else:
                        logger.critical('max nr of orders created already')
                case 'cancel':
                    await self.cancel_orders(wave_id, wave_frame)

    async def create_orders(self, wave_id, wave_frame, stabilized_hint):
        price_mean = wave_frame['price_mean'][0]
        price_min = wave_frame['price_min'][0]
        price_max = wave_frame['price_max'][0]
        spread = wave_frame['spread'][0]

        low_price, high_price = scientific_price_calculation(price_mean, price_min, price_max, spread, stabilized_hint)
        base_amount = 0.001  # ~ $18.1

        buy_amount = round(base_amount + 0.001 * self.balance_watcher.percentage_usd(price_mean), 8)
        sell_amount = round(base_amount + 0.001 * self.balance_watcher.percentage_btc(price_mean), 8)

        logger.success('creating orders, buy {} at {}, sell {} at {}. Spread: {}', buy_amount, low_price, sell_amount, high_price, round(high_price - low_price, 3))
        new_orders = []
        try:
            buy_order = await self.exchange.create_order('BTC/USDT', 'limit', 'buy', buy_amount, low_price, {'test': test_mode})
            logger.success('BUY ORDER PLACED!! - {} {}', type(buy_order), buy_order)
            if test_mode:
                buy_order['id'] = 17253897422
            new_orders.append(buy_order)
        except Exception as e:
            logger.error(e)

        try:
            sell_order = await self.exchange.create_order('BTC/USDT', 'limit', 'sell', sell_amount, high_price, {'test': test_mode})
            logger.success('SELL ORDER PLACED!! - {} {}', type(sell_order), sell_order)
            if test_mode:
                sell_order['id'] = 17253897423
            new_orders.append(sell_order)

        except Exception as e:
            logger.error(e)
        self.active_orders[wave_id] = new_orders

    async def cancel_orders(self, wave_id, wave_frame):
        if wave_id in self.active_orders.keys():
            clear_orders = self.active_orders.pop(wave_id)
            logger.success('cancelling potential {} orders in wave', len(clear_orders))
            for order in clear_orders:
                try:
                    await self.exchange.cancel_order(order['id'], symbol=order['symbol'])
                except:
                    pass  # ignore errors on cancelling orders because they might got fulfilled already