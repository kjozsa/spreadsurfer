import asyncio
from datetime import datetime

from dateutil.relativedelta import relativedelta
from loguru import logger


class TimeTracker:
    @staticmethod
    async def start():
        start = datetime.now()
        logger.info(f'## starting at {start}')

        while True:
            try:
                await asyncio.sleep(30)
                now = datetime.now()
                logger.info(f'## running for {relativedelta(now, start)}')

            except Exception as e:
                logger.exception(e)
