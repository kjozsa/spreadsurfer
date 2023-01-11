from datetime import timezone
import asyncio
import datetime
from asyncio import create_task
from datetime import datetime

import ccxt.pro as ccxt
import pandas as pd
from dateutil.relativedelta import relativedelta
from loguru import logger
from dateutil import parser as dateparser

exchange = ccxt.binance({
    'apiKey': 'VpL8VBKlFjOjbhKdyB9TYP9bCxLKKE2mG27cEHpQJaHKX7UScuUBU5EbQ2EGbhuM',
    'secret': 'fol15IWAcudEFQtPbJydHfdj8Rr9fAa4tJwh8f6CSe36kZYjCOaf4W2e3BZ2hrA8',
    'newUpdates': True,
    'enableRateLimit': True
})


def now():
    return datetime.now(timezone.utc)


# trades on websocket
async def scrape_trades():
    df = pd.DataFrame([{'ms': now() - relativedelta(days=1), 'price': 0}])
    while True:
        try:
            trades = await exchange.watch_trades('BTC/USDT')
            fresh_data = []
            for trade in trades:
                # logger.debug(f'{exchange.iso8601(exchange.milliseconds())}, {trade["symbol"]}, {trade["datetime"]}, {trade["price"]}, {trade["amount"]}')
                fresh_data.append({'ms': dateparser.parse(trade['datetime']), 'price': trade['price']})

            # cut to latest X seconds
            df = pd.concat([df, pd.DataFrame(fresh_data)])
            start = now() - relativedelta(microsecond=100 * 1000)
            df = df[df.ms >= start]

            # collect stats
            nr_trades = len(df)
            mean_price = df['price'].mean()
            min = df['price'].min()
            max = df['price'].max()
            spread = max - min
            logger.debug(f'{nr_trades} trades, mean price: {mean_price}, spread: {spread}, min: {min}, max: {max}')

        except Exception as e:
            logger.error('error scraping trades: {}', e)
            raise e


# orders on websocket
async def scrape_order_book():
    while True:
        try:
            order_book = await exchange.watch_order_book('BTC/USDT')
            print(type(order_book))
            print(order_book)
        except Exception as e:
            logger.warning('error {}', e)


# time logger
async def timer():
    start = datetime.now()
    logger.info(f'## starting at {start}')

    while True:
        await asyncio.sleep(30)
        now = datetime.now()
        logger.info(f'## running for {relativedelta(now, start)}')


async def main():
    timelogger = create_task(timer())
    trades = create_task(scrape_trades())
    # order_book = create_task(scrape_order_book())

    try:
        await timelogger
        await trades
        # await order_book

    finally:
        await exchange.close()


try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
