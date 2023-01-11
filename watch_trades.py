import asyncio
import datetime
import json
from asyncio import create_task
from datetime import datetime
from datetime import timezone

import ccxt.pro as ccxt
import pandas as pd
from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta
from loguru import logger

config = json.load(open('./config.json'))

exchange = ccxt.binance({
    'apiKey': config['binance']['apiKey'],
    'secret': config['binance']['secret'],
    'newUpdates': True,
    'enableRateLimit': True
})


def now():
    return datetime.now(timezone.utc)


# trades on websocket
def timedelta_ms(start, end):
    return round(abs((end - start).total_seconds()) * 1000)


@logger.catch()
async def scrape_trades():
    anywhere_in_the_past = now() - relativedelta(days=1)
    wave_start = anywhere_in_the_past
    wave = pd.DataFrame(columns=['ms' 'price', 'amount'])
    wave_stabilized = None
    wave_stabilized_at_frame = None
    wave_stabilized_at_price = None
    wave_long_running = False
    df = pd.DataFrame(columns=['ms', 'nr_trades', 'price', 'amount'])
    last_trade_count = 0

    while True:
        trades = await exchange.watch_trades('BTC/USDT')
        fresh_data = []
        for trade in trades:
            fresh_data.append({'ms': dateparser.parse(trade['datetime']), 'price': trade['price'], 'amount': trade['amount']})
        df = pd.concat([df, pd.DataFrame(fresh_data)])

        # cut frame to latest X seconds
        start = now() - relativedelta(microsecond=100 * 1000)
        df = df[df.ms >= start]

        # collect stats
        nr_trades = len(df)  # nr of trades in defined timewindow
        price_mean = df['price'].mean()
        price_min = df['price'].min()
        price_max = df['price'].max()
        spread = price_max - price_min
        amount_mean = df["amount"].mean()

        # analyze wave start/end, collect wave data
        if last_trade_count == 0 and nr_trades > 0:  # START WAVE
            logger.warning('starting new wave')
            wave = wave.head(0)
            wave_start = now()

        if nr_trades < last_trade_count:  # END WAVE
            wave_length_ms = timedelta_ms(now(), wave_start)
            last_price = wave.tail(1)['price_mean'][0]
            logger.warning(f'ending wave, wave length was {wave_length_ms} ms')
            if wave_stabilized_at_price is not None:
                logger.info(f'delta(end-stabilized) = {round(last_price - wave_stabilized_at_price, 4)}')
            wave_stabilized = None
            wave_stabilized_at_frame = None
            wave_stabilized_at_price = None
            wave_long_running = False
        else:
            wave_frame = pd.DataFrame([{'nr_trades': nr_trades, 'price_mean': price_mean, 'spread': spread, 'price_min': price_min, 'price_max': price_max, 'amount_mean': amount_mean}])
            wave = pd.concat([wave, wave_frame])
        last_trade_count = nr_trades

        logger.debug(f'{nr_trades} trades, mean price: {price_mean}, spread: {spread}, min: {price_min}, max: {price_max}, amount: {amount_mean}')

        # check wave stabilization
        wave_min_length = 6  # TODO config
        wave_length_to_investigate = 4  # TODO config
        wave_stabilized_threshold = 0.01  # TODO config
        if len(wave) > wave_min_length:
            last_waves = wave[-wave_length_to_investigate:]
            if len(last_waves) != wave_length_to_investigate: raise AssertionError
            wave_min_stabilized = abs(last_waves['price_min'].min() - last_waves['price_min'].mean()) < wave_stabilized_threshold
            wave_max_stabilized = abs(last_waves['price_max'].max() - last_waves['price_max'].mean()) < wave_stabilized_threshold

            if not wave_stabilized and not (wave_min_stabilized and wave_max_stabilized):
                if wave_min_stabilized:
                    wave_stabilized = "min"
                    logger.error('wave MIN stabilized in {} ms', timedelta_ms(now(), wave_start))
                    wave_stabilized_at_price = price_mean
                    wave_stabilized_at_frame = len(wave)
                if wave_max_stabilized:
                    wave_stabilized = "max"
                    logger.error('wave MAX stabilized in {} ms', timedelta_ms(now(), wave_start))
                    wave_stabilized_at_price = price_mean
                    wave_stabilized_at_frame = len(wave)

            if wave_stabilized is not None and not wave_long_running:
                if len(wave) - wave_stabilized_at_frame > 8:
                    logger.error("WARNING! LONG RUNNING WAVE!")
                    wave_long_running = True


# orders on websocket
@logger.catch()
async def scrape_order_book():
    while True:
        try:
            order_book = await exchange.watch_order_book('BTC/USDT')
            print(type(order_book))
            print(order_book)
        except Exception as e:
            logger.warning('error {}', e)


# time logger
@logger.catch()
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
