import pandas as pd
from loguru import logger
import json

from .timeutils import timestamp_now_ms

order_config = json.load(open('config.json'))['orders']
cancel_far_order_after_ms = order_config['cancel_far_order_after_ms']


class Bookkeeper:
    def __init__(self):
        cols = ['timestamp_created_ms', 'price', 'wave_id', 'order_id', 'near_far']
        self.df_active = pd.DataFrame(columns=cols)
        self.df_past = pd.DataFrame(columns=cols)
        self.total_balance = 0
        self.nr_orders = 0
        self.nr_fulfilled_orders = 0

    def save_orders(self, new_orders):
        self.df_active = pd.concat([self.df_active, (pd.DataFrame(new_orders))])
        self.nr_orders += len(new_orders)

    def remove_orders_by_wave(self, wave_id):
        active_orders_in_wave = self.df_active[self.df_active.wave_id == wave_id]
        if not active_orders_in_wave.empty:
            self.df_active = self.df_active[self.df_active.wave_id != wave_id]
            self.df_past = pd.concat([self.df_past, active_orders_in_wave])
            return active_orders_in_wave.to_dict('records')
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
        logger.info('$$$ TOTAL BALANCE: {} -- {} total orders, {} fulfilled orders ({} %)', round(self.total_balance, 3), self.nr_orders, self.nr_fulfilled_orders, round(100 * self.nr_fulfilled_orders / self.nr_orders))

    # def orders_to_cancel(self, wave_id):
    #     near_orders = self.df_active[(self.df_active.wave_id == wave_id) & (self.df_active.near_far == 'near')].to_dict('records')
    #     return near_orders + self.past_orders_to_cancel()
    #
    # def past_orders_to_cancel(self):
    #     drop_before_time_ms = timestamp_now_ms() - cancel_far_order_after_ms
    #     past_orders = self.df_past[self.df_past.timestamp_created_ms < drop_before_time_ms].to_dict('records')
    #     self.df_past = self.df_past[self.df_past.timestamp_created_ms >= drop_before_time_ms]
    #     return past_orders
