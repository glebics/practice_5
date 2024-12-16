# app/cache.py

import aioredis
import json
from typing import Any, Optional
import logging
from datetime import date, datetime

from .settings import async_settings

logger = logging.getLogger(__name__)

redis_client: Optional[aioredis.Redis] = None


class CustomJSONEncoder(json.JSONEncoder):
    """
    Кастомный JSON энкодер для сериализации объектов date и datetime.
    """

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()
        return super().default(obj)


async def get_redis_pool() -> Optional[aioredis.Redis]:
    """
    Получает пул соединений Redis.

    Returns:
        Optional[aioredis.Redis]: Клиент Redis или None, если подключение не удалось.
    """
    global redis_client
    if not redis_client:
        try:
            redis_client = await aioredis.from_url(
                async_settings.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            await redis_client.ping()
            logger.info("Подключение к Redis успешно установлено.")
        except Exception as e:
            logger.error(f"Не удалось подключиться к Redis: {e}")
            redis_client = None
    return redis_client


async def set_cache(key: str, value: Any, expire: int = 0) -> None:
    """
    Устанавливает значение в кэш Redis.

    Args:
        key (str): Ключ кэша.
        value (Any): Значение для сохранения. Может быть словарем или списком.
        expire (int, optional): Время жизни кэша в секундах. Defaults to 0 (без истечения).
    """
    redis = await get_redis_pool()
    if not redis:
        logger.warning("Redis недоступен. Кэширование не выполнено.")
        return
    try:
        if isinstance(value, (dict, list, date, datetime)):
            value = json.dumps(value, cls=CustomJSONEncoder)
        await redis.set(key, value, ex=expire if expire > 0 else None)
        logger.info(f"Кэш установлен: {key}")
    except Exception as e:
        logger.error(f"Ошибка при установке кэша для {key}: {e}")


async def get_cache(key: str) -> Optional[Any]:
    """
    Получает значение из кэша Redis по ключу.

    Args:
        key (str): Ключ кэша.

    Returns:
        Optional[Any]: Значение из кэша или None, если ключ не найден или Redis недоступен.
    """
    redis = await get_redis_pool()
    if not redis:
        logger.warning("Redis недоступен. Получение кэша невозможно.")
        return None
    try:
        value = await redis.get(key)
        if value:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        return None
    except Exception as e:
        logger.error(f"Ошибка при получении кэша для {key}: {e}")
        return None


async def flush_cache() -> None:
    """
    Очищает весь кэш Redis.
    """
    redis = await get_redis_pool()
    if not redis:
        logger.warning("Redis недоступен. Очистка кэша невозможна.")
        return
    try:
        await redis.flushall()
        logger.info("Кэш Redis очищен.")
    except Exception as e:
        logger.error(f"Ошибка при очистке кэша Redis: {e}")
