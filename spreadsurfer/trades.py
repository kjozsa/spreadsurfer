import asyncio

import ccxt.pro as ccxt
import pandas as pd
from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta
from loguru import logger

import spreadsurfer


class TradeWatcher:
    def __init__(self, exchange: ccxt.Exchange, wave_events_queue: asyncio.Queue):
        self.exchange = exchange
        self.wave_events_queue = wave_events_queue
        self.wave_running = False

    async def start(self):
        df = pd.DataFrame(columns=['ms', 'nr_trades', 'price', 'amount'])
        last_trade_count = 0

        while True:
            trades = await self.exchange.watch_trades('BTC/USDT')
            fresh_data = []
            for trade in trades:
                fresh_data.append({'ms': dateparser.parse(trade['datetime']), 'price': trade['price'], 'amount': trade['amount']})
            df = pd.concat([df, pd.DataFrame(fresh_data)])

            # cut frame to latest X seconds
            start = spreadsurfer.now() - relativedelta(microsecond=100 * 1000)
            df = df[df.ms >= start]

            # collect mean/min/max/spread
            nr_trades = len(df)  # nr of trades in defined timewindow
            price_mean = df['price'].mean()
            price_last = trades[-1]['price']
            price_min = df['price'].min()
            price_max = df['price'].max()
            spread = price_max - price_min
            amount_mean = df["amount"].mean()

            # analyze wave start/end, collect wave data
            if nr_trades > 0 and not self.wave_running:
                # start wave
                self.wave_running = True
                await self.wave_events_queue.put(("start", None))

            elif nr_trades < last_trade_count:
                # end wave
                self.wave_running = False
                await self.wave_events_queue.put(("end", None))

            else:
                # mid-wave
                logger.debug(f'{nr_trades} trades, last price: {price_last}, spread: {spread}, min: {price_min}, max: {price_max}, amount: {amount_mean}')
                wave_frame = pd.DataFrame([{'nr_trades': nr_trades, 'price_mean': price_mean, 'spread': spread, 'price_min': price_min, 'price_max': price_max, 'amount_mean': amount_mean}])
                await self.wave_events_queue.put(("frame", wave_frame))

            last_trade_count = nr_trades
