import json

import asyncio
import ccxt.pro as ccxt
import shortuuid
from loguru import logger

wave_config = json.load(open('config.json'))['balance']
logger.info('balance config: {}', wave_config)
panic_below_total = wave_config['panic_below_total']
panic_countdown_from = wave_config['panic_countdown_from']


class BalanceWatcher:
    def __init__(self, exchange: ccxt.Exchange, connector_wss):
        self.exchange = exchange
        self.connector_wss = connector_wss
        self.balance_btc = None
        self.balance_usd = None
        self.last_btc_usd_rate = None
        self.panic_countdown = panic_countdown_from

    async def start(self):
        balance = await self.exchange.fetch_balance()
        self.balance_btc, self.balance_usd = [float(x['free']) + float(x['locked']) for x in balance['info']['balances'] if x['asset'] in ['BTC', 'USDT']]
        logger.info('starting balance: BTC: {}, USDT: {}', self.balance_btc, self.balance_usd)

        while True:
            await asyncio.sleep(0)

            balance = await self.exchange.watch_balance()
            self.balance_btc = float(balance['BTC']['free']) + float(balance['BTC']['locked'])
            self.balance_usd = float(balance['USDT']['free']) + float(balance['USDT']['locked'])
            balance_total = round(self.last_btc_usd_rate * self.balance_btc + self.balance_usd, 2)
            logger.info('total balance: {}  (BTC: {}, USDT: {}) at rate {}', balance_total, self.balance_btc, self.balance_usd, self.last_btc_usd_rate)

            if balance_total < panic_below_total:
                self.panic_countdown -= 1
            else:
                self.panic_countdown = panic_countdown_from

            if self.panic_countdown <= 0:
                logger.critical('total balance {} below panic level ({}), EXITING..', balance_total, panic_below_total)
                await self.connector_wss.cancel_all_orders(f'P-{shortuuid.uuid()}')
                await asyncio.sleep(1)
                quit(1)

    def sum(self, btc_usd_rate):
        return (self.balance_btc * btc_usd_rate) + self.balance_usd

    def percentage_btc(self, btc_usd_rate):
        self.last_btc_usd_rate = btc_usd_rate
        return self.balance_btc * btc_usd_rate / self.sum(btc_usd_rate)

    def percentage_usd(self, btc_usd_rate):
        self.last_btc_usd_rate = btc_usd_rate
        return self.balance_usd / self.sum(btc_usd_rate)
