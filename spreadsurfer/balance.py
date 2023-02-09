import json

import asyncio
import ccxt.pro as ccxt
import shortuuid
from loguru import logger

wave_config = json.load(open('config.json'))['balance']
logger.info('balance config: {}', wave_config)
panic_below_total = wave_config['panic_below_total']
panic_countdown_from = wave_config['panic_countdown_from']
panic_below_profitability = wave_config['panic_below_profitability']


class BalanceWatcher:
    def __init__(self, exchange: ccxt.Exchange, connector_wss):
        self.exchange = exchange
        self.connector_wss = connector_wss
        self.start_balance = {}
        self.checkpoint_balance = {}
        self.balance = {}
        self.last_btc_usd_rate = None
        self.panic_countdown = panic_countdown_from

    async def start(self):
        balance = await self.exchange.fetch_balance()
        self.start_balance['BTC'], self.start_balance['USDT'], = [float(x['free']) + float(x['locked']) for x in balance['info']['balances'] if x['asset'] in ['BTC', 'USDT']]
        self.balance['BTC'] = self.start_balance['BTC']
        self.balance['USDT'] = self.start_balance['USDT']
        logger.info('starting balance: BTC: {}, USDT: {}', self.start_balance['BTC'], self.start_balance['USDT'])

        prev = None
        while True:
            try:
                await asyncio.sleep(0)

                balance = await self.exchange.watch_balance()
                self.balance['BTC'] = float(balance['BTC']['total'])
                self.balance['USDT'] = float(balance['USDT']['total'])
                balance_total = round(self.last_btc_usd_rate * self.balance['BTC'] + self.balance['USDT'], 2)

                profitability = await self.calc_profitability()
                p_per_rate = round(profitability / self.last_btc_usd_rate * 1e6, 5)

                if prev != (p_per_rate, balance_total, self.balance['BTC'], self.balance['USDT'], self.last_btc_usd_rate):
                    logger.info('PROFIT_PER_RATE {} - total balance: {}  (BTC: {}, USDT: {}) at rate {}', p_per_rate, balance_total, self.balance['BTC'], self.balance['USDT'], self.last_btc_usd_rate)
                prev = (p_per_rate, balance_total, self.balance['BTC'], self.balance['USDT'], self.last_btc_usd_rate)
                await self.check_panic_level(balance_total, p_per_rate)

            except Exception as e:
                logger.exception(e)

    async def calc_profitability(self):
        # profitability == (endBTC - startBTC) * endRate + (endUSD-startUSD)   /   endBTC * endRate + endUSD
        delta_btc = self.balance['BTC'] - self.start_balance['BTC']
        delta_usdt = self.balance['USDT'] - self.start_balance['USDT']

        n = delta_btc * self.last_btc_usd_rate + delta_usdt
        d = self.balance['BTC'] * self.last_btc_usd_rate + self.balance['USDT']
        return n / d

    async def check_panic_level(self, balance_total, p_per_rate):
        if balance_total < panic_below_total:
            self.panic_countdown -= 1
        else:
            self.panic_countdown = panic_countdown_from

        if p_per_rate < panic_below_profitability or self.panic_countdown <= 0:
            logger.critical('p_per_rate {} or total balance {} below panic level, EXITING..', p_per_rate, balance_total)
            await self.connector_wss.cancel_all_orders(f'P-{shortuuid.uuid()}')
            await asyncio.sleep(1)
            quit(1)

    def total(self):
        return (self.balance['BTC'] * self.last_btc_usd_rate) + self.balance['USDT']

    def percentage_btc(self, btc_usd_rate):
        self.last_btc_usd_rate = btc_usd_rate
        return self.balance['BTC'] * btc_usd_rate / self.total()

    def percentage_usd(self, btc_usd_rate):
        self.last_btc_usd_rate = btc_usd_rate
        return self.balance['USDT'] / self.total()
