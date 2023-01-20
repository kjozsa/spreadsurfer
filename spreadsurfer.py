import sys

from spreadsurfer import *
import asyncio
from loguru import logger

sys.tracebacklimit = 3

logger.remove()
# logger.add(sys.stdout, level=35)
logger.add(sys.stdout)

logger.level("magenta", color='<magenta>', no=15)
logger.level("data", color='<light-blue>', no=35)
logger.add("console.log", rotation="500 MB")

@logger.catch
async def main():
    wave_events_queue = asyncio.Queue(maxsize=1)
    orders_queue = asyncio.Queue(maxsize=1)
    datacollect_queue = asyncio.Queue(maxsize=1)
    exchange = connect_exchange()

    try:
        balance_watcher = BalanceWatcher(exchange)
        coroutines = [
            TimeTracker(),
            balance_watcher,
            TradeWatcher(exchange, wave_events_queue),
            WaveHandler(wave_events_queue, orders_queue, datacollect_queue),
            OrderMaker(exchange, orders_queue, balance_watcher),
            DataCollector(datacollect_queue)
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
