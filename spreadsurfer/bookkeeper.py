from loguru import logger


class Bookkeeper:
    def __init__(self):
        self.active_orders = {}  # <order_id, order>
        self.wave_orders = {}  # <wave_id, list<order>>

    def start(self):
        pass

    def save_orders(self, wave_id, new_orders):
        logger.error('saving orders for wave {}: {}', wave_id, new_orders)
        self.wave_orders[wave_id] = new_orders
        for order in new_orders:
            self.active_orders[order['id']] = order

    def remove_orders_by_wave(self, wave_id):
        logger.error('removing orders from wave {}', wave_id)
        if wave_id in self.wave_orders:
            orders = self.wave_orders.pop(wave_id)
            for order in orders:
                self.active_orders.pop(order['id'])
            return True
        else:
            return False
