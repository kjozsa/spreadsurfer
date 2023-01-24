import sys

from spreadsurfer import *
import asyncio
from loguru import logger
from spreadsurfer.bookkeeper import Bookkeeper
from spreadsurfer.price_engine import PriceEngine

sys.tracebacklimit = 3

logger.remove()
# logger.add(sys.stdout, level=35)
logger.add(sys.stdout)

logger.level("magenta", color='<magenta>', no=15)
logger.level("data", color='<light-blue>', no=35)
logger.level("ml", color='<light-cyan>', no=37)
logger.level("bookkeeper", color='<light-green><bold>', no=37)
logger.add("console.log", rotation="500 MB")


@logger.catch
async def main():
    wave_events_queue = asyncio.Queue(maxsize=1)
    orders_queue = asyncio.Queue(maxsize=1)
    datacollect_queue = asyncio.Queue(maxsize=1)
    exchange = connect_exchange()

    try:
        balance_watcher = BalanceWatcher(exchange)
        bookkeeper = Bookkeeper()
        data_collector = DataCollector(datacollect_queue)
        coroutines = [
            TimeTracker(),
            balance_watcher,
            TradeWatcher(exchange, wave_events_queue, bookkeeper),
            WaveHandler(wave_events_queue, orders_queue, datacollect_queue),
            OrderMaker(exchange, orders_queue, balance_watcher, bookkeeper, PriceEngine(data_collector)),
            data_collector
        ]
        tasks = [asyncio.create_task(x.start()) for x in coroutines]
        await asyncio.gather(*tasks)

    finally:
        await exchange.close()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
