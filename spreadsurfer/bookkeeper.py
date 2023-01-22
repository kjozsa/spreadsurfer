from loguru import logger


class Bookkeeper:
    def __init__(self):
        self.active_orders = {}  # <order_id, order>
        self.wave_orders = {}  # <wave_id, list<order>>

    def start(self):
        pass

    def save_orders(self, wave_id, new_orders):
        self.wave_orders[wave_id] = new_orders
        for order in new_orders:
            self.active_orders[order['id']] = order

    def remove_orders_by_wave(self, wave_id):
        if wave_id in self.wave_orders:
            orders = self.wave_orders.pop(wave_id)
            for order in orders:
                self.active_orders.pop(order['id'])
            return True
        else:
            return False

    def remove_orders_by_id(self, order_id):
        self.active_orders.pop(order_id)
        for wave_id in list(self.wave_orders.keys()):
            orders = self.wave_orders[wave_id]
            for order in orders:
                if order_id == order['id']:
                    orders.remove(order)
                    if len(orders) == 0:
                        self.wave_orders.pop(wave_id)
                    break

    def fulfill_order(self, order_id):
        if order_id in self.active_orders:
            logger.log('bookkeeper', '$$$ FULFILLED ORDER {}', order_id)
            self.remove_orders_by_id(order_id)

    def report(self):
        logger.log('bookkeeper', '{} active_orders, {} wave_orders', len(self.active_orders), len(self.wave_orders))
