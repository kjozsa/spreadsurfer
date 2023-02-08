import sys
import threading
import traceback

from spreadsurfer import *
import asyncio
import signal
from loguru import logger
from spreadsurfer.bookkeeper import Bookkeeper
from spreadsurfer.price_engine import PriceEngine
from spreadsurfer.connector_binance_wss import BinanceWebsocketConnector

# sys.tracebacklimit = 3

logger.remove()
# logger.add(sys.stdout, level=35)
logger.add(sys.stdout)

logger.level("magenta", color='<magenta>', no=15)
logger.level("data", color='<light-blue>', no=35)
logger.level("ml", color='<light-cyan>', no=37)
logger.level("bookkeeper", color='<light-green><bold>', no=37)
logger.add("console.log", rotation="500 MB")


def thread_dump(signum, stack):
    stacktrace = ""
    for _thread in threading.enumerate():
        stacktrace += str(_thread)
        stacktrace += "".join(traceback.format_stack(sys._current_frames()[_thread.ident]))
        stacktrace += "\n"
    logger.info('\n-- dump stacktrace start -- \n{}\n-- dump stacktrace end -- ', stacktrace)

@logger.catch
async def main():
    signal.signal(signal.SIGUSR1, thread_dump)

    wave_events_queue = asyncio.Queue(maxsize=1)
    orders_queue = asyncio.Queue(maxsize=1)
    datacollect_queue = asyncio.Queue(maxsize=1)
    exchange = connect_exchange()
    # exchange2 = connect_exchange()

    try:
        bookkeeper = Bookkeeper()
        data_collector = DataCollector(datacollect_queue)
        binance_wss_connector = BinanceWebsocketConnector()
        balance_watcher = BalanceWatcher(exchange, binance_wss_connector)
        order_book_watcher = OrderBookWatcher(exchange)
        price_engine = PriceEngine(data_collector, order_book_watcher)
        coroutines = [
            TimeTracker(bookkeeper, binance_wss_connector),
            balance_watcher,
            OrderWatcher(exchange, bookkeeper),
            order_book_watcher,
            TradeWatcher(exchange, wave_events_queue),
            WaveHandler(order_book_watcher, wave_events_queue, orders_queue, datacollect_queue),
            OrderMaker(exchange, orders_queue, balance_watcher, bookkeeper, price_engine, binance_wss_connector),
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
