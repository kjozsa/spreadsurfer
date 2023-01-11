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
import json

config = json.load(open('./config.json'))
print(config)

exchange = ccxt.binance({
    'apiKey': config['binance']['apiKey'],
    'secret': config['binance']['secret'],
    'newUpdates': True,
    'enableRateLimit': True
})


def now():
    return datetime.now(timezone.utc)


# trades on websocket
@logger.catch()
async def scrape_trades():
    anywhere_in_the_past = now() - relativedelta(days=1)
    wave = pd.DataFrame([{'ms': anywhere_in_the_past, 'price': 0, 'amount': 0}])
    df = pd.DataFrame([{'ms': anywhere_in_the_past, 'nr_trades': 0, 'price': 0, 'amount': 0}])
    last_trade_count = 0

    while True:
        try:
            trades = await exchange.watch_trades('BTC/USDT')
            fresh_data = []
            for trade in trades:
                # logger.debug(f'{exchange.iso8601(exchange.milliseconds())}, {trade["symbol"]}, {trade["datetime"]}, {trade["price"]}, {trade["amount"]}')
                fresh_data.append({'ms': dateparser.parse(trade['datetime']), 'price': trade['price'], 'amount': trade['amount']})
            frame = pd.DataFrame(fresh_data)
            df = pd.concat([df, frame])

            # cut frame to latest X seconds
            start = now() - relativedelta(microsecond=100 * 1000)
            df = df[df.ms >= start]

            # collect stats
            nr_trades = len(df)  # nr of trades in defined timewindow
            price_mean = df['price'].mean()
            price_min = df['price'].min()
            price_max = df['price'].max()
            spread = price_max - price_min

            # analyze wave start/end
            if last_trade_count == 0 and nr_trades > 0:
                logger.warning('starting new wave')
                wave = wave.head(0)

            if nr_trades < last_trade_count:
                logger.warning(f'ending wave')
            last_trade_count = nr_trades

            wave = pd.concat([wave, frame])

            logger.debug(f'{nr_trades} trades, mean price: {price_mean}, spread: {spread}, min: {price_min}, max: {price_max}, amount: {df["amount"].mean()}')

        except Exception as e:
            logger.exception('error scraping trades')
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
