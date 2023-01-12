from spreadsurfer import *
import asyncio
from loguru import logger


# logger.remove()
# logger.add(sys.stdout, level="INFO")

@logger.catch
async def main():
    wave_events_queue = asyncio.Queue()
    exchange = connect_exchange()

    try:
        coroutines = [
            TimeTracker(),
            TradeWatcher(exchange, wave_events_queue),
            WaveHandler(wave_events_queue)
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
