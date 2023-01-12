import asyncio
from loguru import logger
import ccxt.pro as ccxt


def scientific_price_calculation(price_mean, price_min, price_max, spread, stabilized_hint):
    low_price = price_min
    high_price = price_max

    match stabilized_hint:
        case 'min':  # raising price?
            logger.warning("buffing high_price")
            high_price = high_price * (1 + 0.00008)
        case 'max':  # lowering price?
            logger.warning("buffing low_price")
            low_price = low_price * (1 - 0.00008)
    return low_price, high_price


class OrderMaker:
    def __init__(self, exchange: ccxt.Exchange, orders_queue: asyncio.Queue):
        self.orders_queue = orders_queue

        self.active_orders = {}

    async def start(self):
        while True:
            (wave_id, event_name, wave_frame, stabilized_hint) = await self.orders_queue.get()
            match event_name:
                case 'create':
                    self.create_orders(wave_id, wave_frame, stabilized_hint)

                case 'cancel':
                    self.cancel_orders(wave_id, wave_frame)

    def create_orders(self, wave_id, wave_frame, stabilized_hint):
        price_mean = wave_frame['price_mean'][0]
        price_min = wave_frame['price_min'][0]
        price_max = wave_frame['price_max'][0]
        spread = wave_frame['spread'][0]

        low_price, high_price = scientific_price_calculation(price_mean, price_min, price_max, spread, stabilized_hint)
        logger.success('creating orders at mean price {} at spread {}. Buy at {}, Sell at {}. Stabilized_hint: {}', price_mean, spread, low_price, high_price, stabilized_hint)

        self.active_orders[wave_id] = (price_mean, price_mean)
        return None, None

    def cancel_orders(self, wave_id, wave_frame):
        if wave_id in self.active_orders.keys():
            buy_order, sell_order = self.active_orders.pop(wave_id)
            spread = wave_frame['spread'][0]
            price_delta = buy_order - wave_frame['price_mean'][0]
            logger.success('cancelling orders made at mean price {}. Price delta with end: {}. Delta / Spread == {}', buy_order, price_delta, price_delta / spread)
