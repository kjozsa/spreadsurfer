import json

import asyncio
import ccxt.pro as ccxt
from loguru import logger

wave_config = json.load(open('config.json'))['balance']
logger.info('balance config: {}', wave_config)
panic_below_total = wave_config['panic_below_total']
panic_countdown_from = wave_config['panic_countdown_from']


class BalanceWatcher:
    def __init__(self, exchange: ccxt.Exchange):
        self.exchange = exchange
        self.balance_btc = None
        self.balance_usd = None
        self.last_btc_usd_rate = None
        self.panic_countdown = panic_countdown_from

    async def start(self):
        balance = await self.exchange.fetch_balance()
        self.balance_btc, self.balance_usd = [float(x['free']) for x in balance['info']['balances'] if x['asset'] in ['BTC', 'USDT']]
        logger.info('starting balance: BTC: {}, USDT: {}', self.balance_btc, self.balance_usd)

        while True:
            await asyncio.sleep(0)

            try:
                balance = await self.exchange.watch_balance()
                self.balance_btc, self.balance_usd = balance['BTC']['free'], balance['USDT']['free']
                balance_total = round(self.last_btc_usd_rate * self.balance_btc + self.balance_usd, 2)
                logger.info('total balance: {}  (BTC: {}, USDT: {})', balance_total, self.balance_btc, self.balance_usd)

                if balance_total < panic_below_total:
                    self.panic_countdown -= 1
                else:
                    self.panic_countdown = panic_countdown_from

                if self.panic_countdown <= 0:
                    logger.critical('total balance {} below panic level ({}), EXITING..', balance_total, panic_below_total)
                    quit(1)

            except Exception as e:
                logger.exception(e)

    def sum(self, btc_usd_rate):
        return (self.balance_btc * btc_usd_rate) + self.balance_usd

    def percentage_btc(self, btc_usd_rate):
        self.last_btc_usd_rate = btc_usd_rate
        return self.balance_btc * btc_usd_rate / self.sum(btc_usd_rate)

    def percentage_usd(self, btc_usd_rate):
        self.last_btc_usd_rate = btc_usd_rate
        return self.balance_usd / self.sum(btc_usd_rate)
