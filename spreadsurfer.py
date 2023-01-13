from spreadsurfer import *
import sys
import asyncio
from loguru import logger

# logger.remove()
# logger.add(sys.stdout, level="INFO")
logger.level("magenta", color='<magenta>', no=25)
logger.add("console.log", rotation="500 MB")
sys.tracebacklimit = 1

@logger.catch
async def main():
    wave_events_queue = asyncio.Queue(maxsize=1)
    orders_queue = asyncio.Queue(maxsize=1)
    exchange = connect_exchange()

    try:
        balance_watcher = BalanceWatcher(exchange)
        coroutines = [
            TimeTracker(),
            balance_watcher,
            TradeWatcher(exchange, wave_events_queue),
            WaveHandler(wave_events_queue, orders_queue),
            OrderMaker(exchange, orders_queue, balance_watcher)
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
