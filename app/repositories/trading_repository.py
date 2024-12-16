# app/repositories/trading_repository.py

import logging
from typing import List, Optional
from datetime import datetime, date, time, timedelta

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from ..models import SpimexTradingResultAsync
from ..cache import get_cache, set_cache, flush_cache
from ..schemas import TradingResult
from ..utils import get_seconds_until_flush

logger = logging.getLogger("app.repositories.trading_repository")


class TradingRepository:
    """
    Репозиторий для работы с торговыми данными.
    """

    def __init__(self, db: AsyncSession) -> None:
        """
        Инициализация репозитория.

        Args:
            db (AsyncSession): Асинхронная сессия базы данных.
        """
        self.db = db

    async def get_last_trading_dates(self, limit: int) -> List[date]:
        """
        Получает список дат последних торговых дней.

        Args:
            limit (int): Количество последних торговых дней.

        Returns:
            List[date]: Список дат последних торговых дней.
        """
        cache_key = f"last_trading_dates:{limit}"
        cached = await get_cache(cache_key)
        if cached:
            logger.info(f"Кэш найден для ключа {cache_key}: {cached}")
            try:
                return [datetime.fromisoformat(date_str).date() for date_str in cached]
            except ValueError as ve:
                logger.error(f"Ошибка преобразования дат из кэша: {ve}")
                await flush_cache()
                logger.info("Некорректный кэш был очищен.")

        # Получение данных из базы данных
        query = select(SpimexTradingResultAsync.date).distinct().order_by(
            SpimexTradingResultAsync.date.desc()
        ).limit(limit)
        logger.info(f"Выполнение запроса: {query}")
        result = await self.db.execute(query)
        dates = [row[0].date() for row in result.fetchall()]
        logger.info(f"Получено дат из БД: {dates}")

        # Кэширование данных
        dates_str = [d.isoformat() for d in dates]
        await set_cache(cache_key, dates_str, expire=get_seconds_until_flush())
        logger.info(f"Кэш установлен для ключа {cache_key}")

        return dates

    async def get_dynamics(
        self,
        oil_id: Optional[str],
        delivery_type_id: Optional[str],
        delivery_basis_id: Optional[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ) -> List[TradingResult]:
        """
        Получает список торговых результатов с учетом фильтров.

        Args:
            oil_id (Optional[str]): Идентификатор типа нефти.
            delivery_type_id (Optional[str]): Идентификатор типа доставки.
            delivery_basis_id (Optional[str]): Идентификатор условия доставки.
            start_date (Optional[datetime]): Начальная дата периода.
            end_date (Optional[datetime]): Конечная дата периода.

        Returns:
            List[TradingResult]: Список торговых результатов.
        """
        cache_key = f"dynamics:{oil_id}:{delivery_type_id}:{delivery_basis_id}:{start_date}:{end_date}"
        cached = await get_cache(cache_key)
        if cached:
            logger.info(f"Кэш найден для ключа {cache_key}")
            try:
                return [TradingResult(**item) for item in cached]
            except Exception as e:
                logger.error(f"Ошибка преобразования данных из кэша: {e}")
                await flush_cache()
                logger.info("Некорректный кэш был очищен.")

        # Формирование запроса с фильтрами
        query = select(SpimexTradingResultAsync)
        filters = []

        if oil_id:
            filters.append(SpimexTradingResultAsync.oil_id == oil_id)
        if delivery_type_id:
            filters.append(
                SpimexTradingResultAsync.delivery_type_id == delivery_type_id)
        if delivery_basis_id:
            filters.append(
                SpimexTradingResultAsync.delivery_basis_id == delivery_basis_id)
        if start_date:
            filters.append(SpimexTradingResultAsync.date >= start_date)
        if end_date:
            filters.append(SpimexTradingResultAsync.date <= end_date)

        if filters:
            query = query.where(*filters)
            logger.info(f"Применены фильтры: {[str(f) for f in filters]}")
        else:
            logger.info("Фильтры не применены.")

        query = query.order_by(SpimexTradingResultAsync.date.desc())
        logger.info(f"Выполнение запроса: {query}")
        result = await self.db.execute(query)
        trading_results = result.scalars().all()
        logger.info(
            f"Получено торговых результатов из БД: {len(trading_results)}")

        # Преобразование в схемы Pydantic
        results = [TradingResult.from_orm(tr) for tr in trading_results]
        results_dict = [result.dict() for result in results]

        # Кэширование
        await set_cache(cache_key, results_dict, expire=get_seconds_until_flush())
        logger.info(f"Кэш установлен для ключа {cache_key}")

        return results

    async def get_trading_results(
        self,
        oil_id: Optional[str],
        delivery_type_id: Optional[str],
        delivery_basis_id: Optional[str],
        limit: int
    ) -> List[TradingResult]:
        """
        Получает список последних торговых результатов с учетом фильтров.

        Args:
            oil_id (Optional[str]): Идентификатор типа нефти.
            delivery_type_id (Optional[str]): Идентификатор типа доставки.
            delivery_basis_id (Optional[str]): Идентификатор условия доставки.
            limit (int): Количество последних торгов.

        Returns:
            List[TradingResult]: Список торговых результатов.
        """
        cache_key = f"trading_results:{oil_id}:{delivery_type_id}:{delivery_basis_id}:{limit}"
        cached = await get_cache(cache_key)
        if cached:
            logger.info(f"Кэш найден для ключа {cache_key}")
            try:
                return [TradingResult(**item) for item in cached]
            except Exception as e:
                logger.error(f"Ошибка преобразования данных из кэша: {e}")
                await flush_cache()
                logger.info("Некорректный кэш был очищен.")

        # Формирование запроса с фильтрами
        query = select(SpimexTradingResultAsync)
        filters = []

        if oil_id:
            filters.append(SpimexTradingResultAsync.oil_id == oil_id)
        if delivery_type_id:
            filters.append(
                SpimexTradingResultAsync.delivery_type_id == delivery_type_id)
        if delivery_basis_id:
            filters.append(
                SpimexTradingResultAsync.delivery_basis_id == delivery_basis_id)

        if filters:
            query = query.where(*filters)
            logger.info(f"Применены фильтры: {[str(f) for f in filters]}")
        else:
            logger.info("Фильтры не применены.")

        query = query.order_by(
            SpimexTradingResultAsync.date.desc()).limit(limit)
        logger.info(f"Выполнение запроса: {query}")
        result = await self.db.execute(query)
        trading_results = result.scalars().all()
        logger.info(
            f"Получено торговых результатов из БД: {len(trading_results)}")

        # Преобразование в схемы Pydantic
        results = [TradingResult.from_orm(tr) for tr in trading_results]
        results_dict = [result.dict() for result in results]

        # Кэширование
        await set_cache(cache_key, results_dict, expire=get_seconds_until_flush())
        logger.info(f"Кэш установлен для ключа {cache_key}")

        return results
