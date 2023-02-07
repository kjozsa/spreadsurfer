import json

import pandas as pd
from loguru import logger

order_config = json.load(open('config.json'))['orders']
cancel_far_order_after_ms = order_config['cancel_far_order_after_ms']


class Bookkeeper:
    def __init__(self):
        cols = ['timestamp_created_ms', 'price', 'wave_id', 'order_id', 'client_order_id', 'near_far']
        self.df = pd.DataFrame(columns=cols)
        self.nr_orders = 0
        self.nr_fulfilled_orders = 0
        self.fulfilled_orders = {x: 0 for x in ['NB', 'FS', 'NS', 'FB']}

    def save_orders(self, new_orders):
        self.df = pd.concat([self.df, (pd.DataFrame(new_orders))])
        self.nr_orders += len(new_orders)

    def remove_orders_by_wave(self, wave_id):
        active_orders_in_wave = self.df[self.df.wave_id == wave_id]
        if not active_orders_in_wave.empty:
            self.df = self.df[self.df.wave_id != wave_id]
            return active_orders_in_wave.to_dict('records')
        else:
            return []

    async def fulfill_order(self, client_order_id):
        orders = self.df[self.df.client_order_id == client_order_id].to_dict('records')
        for order in orders:
            logger.log('bookkeeper', '$$$ FULFILLED {} ORDER {}', order['type'], order['client_order_id'])
            self.nr_fulfilled_orders += 1
            order_type = order['client_order_id'][:2]
            self.fulfilled_orders[order_type] += 1
            self.report()

    def report(self):
        percentage = round(100 * self.nr_fulfilled_orders / self.nr_orders)
        logger.info('$$$ total/fulfilled orders: {} / {} ({} %). In detail: {}', self.nr_orders, self.nr_fulfilled_orders, percentage, self.fulfilled_orders)

    # def orders_to_cancel(self, wave_id):
    #     near_orders = self.df_active[(self.df_active.wave_id == wave_id) & (self.df_active.near_far == 'near')].to_dict('records')
    #     return near_orders + self.past_orders_to_cancel()
    #
    # def past_orders_to_cancel(self):
    #     drop_before_time_ms = timestamp_now_ms() - cancel_far_order_after_ms
    #     past_orders = self.df_past[self.df_past.timestamp_created_ms < drop_before_time_ms].to_dict('records')
    #     self.df_past = self.df_past[self.df_past.timestamp_created_ms >= drop_before_time_ms]
    #     return past_orders
