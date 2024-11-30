# app/cache.py
import aioredis
import json
from .settings import async_settings
from typing import Any, Optional

redis = None


async def get_redis_pool() -> aioredis.Redis:
    """
    Получает пул соединений Redis.

    Returns:
        aioredis.Redis: Клиент Redis.
    """
    global redis
    if not redis:
        redis = await aioredis.from_url(
            async_settings.redis_url,
            encoding="utf-8",
            decode_responses=True
        )
    return redis


async def set_cache(key: str, value: Any, expire: int = 0) -> None:
    """
    Устанавливает значение в кэш Redis.

    Args:
        key (str): Ключ кэша.
        value (Any): Значение для сохранения. Может быть словарем или списком.
        expire (int, optional): Время жизни кэша в секундах. Defaults to 0 (без истечения).
    """
    redis_client = await get_redis_pool()
    if isinstance(value, (dict, list)):
        value = json.dumps(value)
    await redis_client.set(key, value, ex=expire)


async def get_cache(key: str) -> Optional[Any]:
    """
    Получает значение из кэша Redis по ключу.

    Args:
        key (str): Ключ кэша.

    Returns:
        Optional[Any]: Значение из кэша или None, если ключ не найден.
    """
    redis_client = await get_redis_pool()
    value = await redis_client.get(key)
    if value:
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return None


async def flush_cache() -> None:
    """
    Очищает весь кэш Redis.
    """
    redis_client = await get_redis_pool()
    await redis_client.flushall()
