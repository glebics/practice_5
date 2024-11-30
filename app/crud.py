# app/crud.py
from typing import List, Optional
import logging
from sqlalchemy.future import select
from sqlalchemy import and_
from typing import List
from .models import SpimexTradingResultAsync
from .schemas import TradingResult
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession


logger = logging.getLogger("app.crud")


async def get_last_trading_dates(db: AsyncSession, limit: int) -> List[datetime]:
    """
    Получает список дат последних торговых дней.

    Args:
        db (AsyncSession): Асинхронная сессия базы данных.
        limit (int): Количество последних торговых дней.

    Returns:
        List[datetime]: Список дат последних торговых дней.
    """
    result = await db.execute(
        select(SpimexTradingResultAsync.date)
        .distinct()
        .order_by(SpimexTradingResultAsync.date.desc())
        .limit(limit)
    )
    dates = [row[0].date() for row in result.fetchall()]
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

    query = query.order_by(SpimexTradingResultAsync.date.desc())

    result = await db.execute(query)
    trading_results = result.scalars().all()
    logger.info(f"Получено {len(trading_results)} результатов из базы данных.")
    return trading_results


async def get_trading_results(
    db: AsyncSession,
    oil_id: str,
    delivery_type_id: str,
    delivery_basis_id: str,
    limit: int = 10
) -> List[SpimexTradingResultAsync]:
    """
    Получает список последних торгов с фильтрацией.

    Args:
        db (AsyncSession): Асинхронная сессия базы данных.
        oil_id (str): Идентификатор типа нефти.
        delivery_type_id (str): Идентификатор типа доставки.
        delivery_basis_id (str): Идентификатор условия доставки.
        limit (int, optional): Количество последних торгов. Defaults to 10.

    Returns:
        List[SpimexTradingResultAsync]: Список торговых результатов.
    """
    query = select(SpimexTradingResultAsync).where(
        and_(
            SpimexTradingResultAsync.oil_id == oil_id,
            SpimexTradingResultAsync.delivery_type_id == delivery_type_id,
            SpimexTradingResultAsync.delivery_basis_id == delivery_basis_id
        )
    ).order_by(SpimexTradingResultAsync.date.desc()).limit(limit)
    result = await db.execute(query)
    return result.scalars().all()
