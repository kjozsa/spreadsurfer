import asyncio
import datetime

import ccxt.pro as ccxt
from loguru import logger
from datetime import datetime
from dateutil.relativedelta import relativedelta

exchange = ccxt.binance({'newUpdates': True})


# websocket
async def scrape():
    while True:
        try:
            trades = await exchange.watch_trades('BTC/USDT')
            last_id = None
            for trade in trades:
                logger.debug(f'{exchange.iso8601(exchange.milliseconds())}, {trade["symbol"]}, {trade["datetime"]}, {trade["price"]}, {trade["amount"]}')
                last_id = trade['id']

        except Exception as e:
            logger.warning(f'stopped at last_id {last_id}')
            await exchange.close()
            raise e


# time logger
async def timer():
    start = datetime.now()
    logger.info(f'## starting at {start}')

    while True:
        await asyncio.sleep(30)
        now = datetime.now()
        logger.info(f'## running for {relativedelta(now, start)}')


async def main():
    timelogger = asyncio.create_task(timer())
    scraper = asyncio.create_task(scrape())
    await timelogger
    await scraper


asyncio.run(main())
