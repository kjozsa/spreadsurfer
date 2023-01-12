import asyncio
from loguru import logger
import ccxt.pro as ccxt


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
        spread = wave_frame['spread'][0]
        logger.success('creating orders at mean price {} at spread {}', price_mean, spread)
        self.active_orders[wave_id] = (price_mean, price_mean)
        return None, None

    def cancel_orders(self, wave_id, wave_frame):
        if wave_id in self.active_orders.keys():
            buy_order, sell_order = self.active_orders.pop(wave_id)
            spread = wave_frame['spread'][0]
            price_delta = buy_order - wave_frame['price_mean'][0]
            logger.success('cancelling orders made at mean price {}. Price delta with end: {}. Delta / Spread == {}', buy_order, price_delta, price_delta / spread)
