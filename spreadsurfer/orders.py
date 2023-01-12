import asyncio
from loguru import logger
import ccxt.pro as ccxt
from spreadsurfer import *

# enable ccxt debug logging:
# import logging
# logging.basicConfig(level=logging.DEBUG)


def scientific_price_calculation(price_mean, price_min, price_max, spread, stabilized_hint):
    low_price = price_min
    high_price = price_max
    hint_buff_factor = 0.000075

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
        self.max_nr_orders_created = 5

    async def start(self):
        while True:
            (wave_id, event_name, wave_frame, stabilized_hint) = await self.orders_queue.get()
            match event_name:
                case 'create':
                    if self.nr_orders_created < self.max_nr_orders_created:
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
        amount = 0.001  # ~ $18.1

        buy_amount = round(amount + 0.001 * self.balance_watcher.percentage_usd(price_mean), 8)
        sell_amount = round(amount + 0.001 * self.balance_watcher.percentage_btc(price_mean), 8)

        logger.success('creating orders at mean price {}. Buy {} at {}, Sell {} at {}. Spread: {}', price_mean, buy_amount, low_price, sell_amount, high_price, round(high_price - low_price, 3))
        try:
            buy_order = await self.exchange.create_order('BTC/USDT', 'limit', 'buy', buy_amount, low_price, {'test': True})
            logger.success('ORDER1 PLACED!!')
            logger.debug(buy_order)
            sell_order = await self.exchange.create_order('BTC/USDT', 'limit', 'sell', sell_amount, high_price, {'test': True})
            logger.debug(sell_order)
            logger.success('ORDER2 PLACED!!')
            self.active_orders[wave_id] = (buy_order, sell_order)
        except Exception as e:
            logger.error(e)

    async def cancel_orders(self, wave_id, wave_frame):
        if wave_id in self.active_orders.keys():
            buy_order, sell_order = self.active_orders.pop(wave_id)
            logger.success('cancelling orders made at price {} / {} ', buy_order['price'], sell_order['price'])
            try:
                await self.exchange.cancel_order(buy_order['id'], 'BTC/USDT', {'test': True})
                await self.exchange.cancel_order(sell_order['id'], 'BTC/USDT', {'test': True})
                logger.success('CANCELS done!!')
            except Exception as e:
                logger.error(e)
