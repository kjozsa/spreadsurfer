import ccxt.pro as ccxt
from loguru import logger


class BalanceWatcher:
    def __init__(self, exchange: ccxt.Exchange):
        self.exchange = exchange
        self.balance_btc = None
        self.balance_usd = None
        self.balance_total = None
        self.last_btc_usd_rate = None

    async def start(self):
        balance = await self.exchange.fetch_balance()
        self.balance_btc, self.balance_usd = [float(x['free']) for x in balance['info']['balances'] if x['asset'] in ['BTC', 'USDT']]
        logger.info('starting balance: BTC: {}, USDT: {}', self.balance_btc, self.balance_usd)

        while True:
            try:
                balance = await self.exchange.watch_balance()
                self.balance_btc, self.balance_usd = balance['BTC']['free'], balance['USDT']['free']
                logger.info('total balance: {}  (BTC: {}, USDT: {})', self.balance_total, self.balance_btc, self.balance_usd)

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
