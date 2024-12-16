# app/utils.py

import asyncio
import logging
from datetime import datetime, date, time, timedelta
from typing import Callable

from .cache import flush_cache
from .settings import async_settings

logger = logging.getLogger("app.utils")


def get_seconds_until_flush() -> int:
    """
    Вычисляет количество секунд до следующего сброса кэша в 14:11.

    Returns:
        int: Количество секунд до сброса кэша.
    """
    now = datetime.now()
    flush_time = datetime.combine(now.date(), time(14, 11))
    if now > flush_time:
        flush_time += timedelta(days=1)
    wait_seconds = (flush_time - now).total_seconds()
    return int(wait_seconds)


async def schedule_cache_flush() -> None:
    """
    Планирует сброс кэша Redis каждый день в 14:11.
    """
    while True:
        wait_seconds = get_seconds_until_flush()
        logger.info(
            f"Запланирован сброс кэша через {wait_seconds:.2f} секунд.")
        await asyncio.sleep(wait_seconds)
        await flush_cache()
        logger.info("Кэш Redis был сброшен в 14:11.")
