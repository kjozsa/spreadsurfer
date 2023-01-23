from loguru import logger


class Bookkeeper:
    def __init__(self):
        self.active_orders_by_price = {}  # <price, order>
        self.past_orders_by_price = {}  # <price, order>

        self.wave_orders = {}  # <wave_id, list<order>>

    def start(self):
        pass

    def save_orders(self, wave_id, new_orders):
        self.wave_orders[wave_id] = new_orders
        for order in new_orders:
            self.active_orders_by_price[order['price']] = order

    def remove_orders_by_wave(self, wave_id):
        self.past_orders_by_price = {}

        if wave_id in self.wave_orders:
            orders = self.wave_orders.pop(wave_id)
            for order in orders:
                self.active_orders_by_price.pop(order['price'])
                self.past_orders_by_price[order['price']] = order
            return True
        else:
            return False

    def _remove_orders_by_price(self, order_id):
        self.active_orders_by_price.pop(order_id)
        for wave_id in list(self.wave_orders.keys()):
            orders = self.wave_orders[wave_id]
            for order in orders:
                if order_id == order['price']:
                    orders.remove(order)
                    if len(orders) == 0:
                        self.wave_orders.pop(wave_id)
                    return order
        return None

    def fulfill_order(self, order_price):
        if order_price in self.active_orders_by_price:
            order = self._remove_orders_by_price(order_price)
            logger.log('bookkeeper', '$$$ FULFILLED {} ORDER {}', order['type'], order_price)

        if order_price in self.past_orders_by_price:
            order = self.past_orders_by_price.pop(order_price)
            logger.log('bookkeeper', '$$$ FULFILLED {} ORDER {}', order['type'], order_price)

    def report(self):
        logger.log('bookkeeper', '{} active_orders, {} wave_orders', len(self.active_orders_by_price), len(self.wave_orders))
