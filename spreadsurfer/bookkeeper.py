import pandas as pd
from loguru import logger


class Bookkeeper:
    def __init__(self):
        self.df_active = pd.DataFrame(columns=['timestamp_created_ms', 'price', 'wave_id', 'order_id'])
        self.df_past = pd.DataFrame(columns=['timestamp_created_ms', 'price', 'wave_id', 'order_id'])
        self.total_balance = 0
        self.nr_orders = 0
        self.nr_fulfilled_orders = 0

    def save_orders(self, new_orders):
        self.df_active = pd.concat([self.df_active, (pd.DataFrame(new_orders))])
        self.nr_orders += len(new_orders)

    def remove_orders_by_wave(self, wave_id):
        active_orders_in_wave = self.df_active[self.df_active.wave_id == wave_id]
        if not active_orders_in_wave.empty:
            order_ids = active_orders_in_wave.order_id.to_list()
            self.df_active = self.df_active[self.df_active.wave_id != wave_id]
            self.df_past = pd.concat([self.df_past, active_orders_in_wave])
            return order_ids
        else:
            return []

    def _remove_orders_by_price(self, price):
        orders = self.df_active[self.df_active.price == price].to_dict('records')
        if orders:
            self.df_active = self.df_active[self.df_active.price != price]
        if not orders:
            orders = self.df_past[self.df_past.price == price].to_dict('records')
            if orders:
                self.df_past = self.df_past[self.df_past.price != price]

        return orders

    def fulfill_order(self, order_price):
        orders = self._remove_orders_by_price(order_price)
        for order in orders:
            logger.log('bookkeeper', '$$$ FULFILLED {} ORDER {}', order['type'], order_price)
            self.nr_fulfilled_orders += 1
            self.total_balance += order['price'] * order['amount']
            self.report()

    def report(self):
        # logger.log('bookkeeper', '{} active_orders, {} wave_orders', len(self.active_orders_by_price), len(self.wave_orders))
        logger.trace('$$$ TOTAL BALANCE: {} -- {} total orders, {} fulfilled orders ({} %)', round(self.total_balance, 3), self.nr_orders, self.nr_fulfilled_orders, round(100 * self.nr_fulfilled_orders / self.nr_orders))
