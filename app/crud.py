# app/crud.py

import logging
from sqlalchemy.future import select
from typing import List, Optional
from datetime import datetime, date
from .models import SpimexTradingResultAsync
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("app.crud")


async def get_last_trading_dates(db: AsyncSession, limit: int) -> List[date]:
    """
    Получает список дат последних торговых дней.

    Args:
        db (AsyncSession): Асинхронная сессия базы данных.
        limit (int): Количество последних торговых дней.

    Returns:
        List[date]: Список дат последних торговых дней.
    """
    logger.info("Формирование запроса для получения последних торговых дат.")
    query = select(SpimexTradingResultAsync.date).distinct().order_by(
        SpimexTradingResultAsync.date.desc()).limit(limit)
    logger.info(f"Выполнение запроса: {query}")
    result = await db.execute(query)
    dates = [row[0].date() for row in result.fetchall()]
    logger.info(f"Получено дат: {dates}")
    return dates


async def get_dynamics(
    db: AsyncSession,
    oil_id: Optional[str],
    delivery_type_id: Optional[str],
    delivery_basis_id: Optional[str],
    start_date: Optional[datetime],
    end_date: Optional[datetime]
) -> List[SpimexTradingResultAsync]:
    """
    Получает список торговых результатов с учетом фильтров.

    Args:
        db (AsyncSession): Асинхронная сессия базы данных.
        oil_id (Optional[str]): Идентификатор типа нефти.
        delivery_type_id (Optional[str]): Идентификатор типа доставки.
        delivery_basis_id (Optional[str]): Идентификатор условия доставки.
        start_date (Optional[datetime]): Начальная дата периода.
        end_date (Optional[datetime]): Конечная дата периода.

    Returns:
        List[SpimexTradingResultAsync]: Список торговых результатов.
    """
    logger.info("Формирование запроса для получения динамики торгов.")

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
    result = await db.execute(query)
    trading_results = result.scalars().all()
    logger.info(f"Получено торговых результатов: {len(trading_results)}")
    return trading_results


async def get_trading_results(
    db: AsyncSession,
    oil_id: Optional[str],
    delivery_type_id: Optional[str],
    delivery_basis_id: Optional[str],
    limit: int
) -> List[SpimexTradingResultAsync]:
    """
    Получает список последних торговых результатов с учетом фильтров.

    Args:
        db (AsyncSession): Асинхронная сессия базы данных.
        oil_id (Optional[str]): Идентификатор типа нефти.
        delivery_type_id (Optional[str]): Идентификатор типа доставки.
        delivery_basis_id (Optional[str]): Идентификатор условия доставки.
        limit (int): Количество последних торгов.

    Returns:
        List[SpimexTradingResultAsync]: Список торговых результатов.
    """
    logger.info(
        "Формирование запроса для получения последних торговых результатов.")

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

    query = query.order_by(SpimexTradingResultAsync.date.desc()).limit(limit)
    logger.info(f"Выполнение запроса: {query}")
    result = await db.execute(query)
    trading_results = result.scalars().all()
    logger.info(f"Получено торговых результатов: {len(trading_results)}")
    return trading_results
