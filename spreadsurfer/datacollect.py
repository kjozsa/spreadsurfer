from loguru import logger


class DataCollector:
    def __init__(self, datacollect_queue):
        self.datacollect_queue = datacollect_queue

    async def start(self):
        while True:
            (stabilized_ms, stabilized_frame, end_ms, end_frame) = await self.datacollect_queue.get()
            logger.debug('$$$$ received wave data')
