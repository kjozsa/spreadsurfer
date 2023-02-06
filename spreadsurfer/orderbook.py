import asyncio

import ccxt.pro as ccxt
from loguru import logger


def calculate_gasp(order_book):
    bids = order_book['bids']
    asks = order_book['asks']

    numerator = 0
    denominator = 0
    bid_idx = 0
    ask_idx = 0

    while True:
        try:
            bid_price = bids[bid_idx][0]
            bid_qty = bids[bid_idx][1]

            ask_price = asks[ask_idx][0]
            ask_qty = asks[ask_idx][1]

            if bid_qty < ask_qty:
                bid_idx += 1
                numerator += bid_qty * bid_price + bid_qty * ask_price
                denominator += 2 * bid_qty
                asks[ask_idx][1] = ask_qty - bid_qty
            elif ask_qty < bid_qty:
                ask_idx += 1
                numerator += ask_qty * bid_price + ask_qty * ask_price
                denominator += 2 * ask_qty
                bids[bid_idx][1] = bid_qty - ask_qty
            else:
                bid_idx += 1
                ask_idx += 1
                numerator += bid_qty * bid_price + ask_qty * ask_price
                denominator += bid_qty + ask_qty
        except IndexError:
            break
    return numerator / denominator


class OrderBookWatcher:
    def __init__(self, exchange: ccxt.Exchange):
        self.exchange = exchange
        self.last_bid = None
        self.last_ask = None
        self.last_gasp = None

    async def start(self):
        while True:
            await asyncio.sleep(0)

            order_book = await self.exchange.watch_order_book('BTC/USDT', limit=10)
            self.last_gasp = calculate_gasp(order_book)
            logger.debug('orderbook gasp: {}', self.last_gasp)

            self.last_bid = order_book['bids'][0][0]
            self.last_ask = order_book['asks'][0][0]

