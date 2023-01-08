import asyncio
import datetime
from asyncio import create_task
from datetime import datetime

import ccxt.pro as ccxt
from dateutil.relativedelta import relativedelta
from loguru import logger

exchange = ccxt.binance({'newUpdates': True, 'enableRateLimit': True})


# trades on websocket
async def scrape_trades():
    while True:
        try:
            trades = await exchange.watch_trades('BTC/USDT')
            last_id = None
            for trade in trades:
                logger.debug(f'{exchange.iso8601(exchange.milliseconds())}, {trade["symbol"]}, {trade["datetime"]}, {trade["price"]}, {trade["amount"]}')
                last_id = trade['id']

        except Exception as e:
            logger.warning(f'stopped at last_id {last_id}')
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
    timelogger = create_task(timer())
    trades = create_task(scrape_trades())

    try:
        await timelogger
        await trades

    finally:
        await exchange.close()


try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
