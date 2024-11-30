# app/crud.py
from sqlalchemy.future import select
from sqlalchemy import and_
from typing import List
from .models import SpimexTradingResultAsync
from .schemas import TradingResult
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession


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
    oil_id: str,
    delivery_type_id: str,
    delivery_basis_id: str,
    start_date: datetime,
    end_date: datetime
) -> List[SpimexTradingResultAsync]:
    """
    Получает список торгов за заданный период с фильтрацией.

    Args:
        db (AsyncSession): Асинхронная сессия базы данных.
        oil_id (str): Идентификатор типа нефти.
        delivery_type_id (str): Идентификатор типа доставки.
        delivery_basis_id (str): Идентификатор условия доставки.
        start_date (datetime): Начальная дата периода.
        end_date (datetime): Конечная дата периода.

    Returns:
        List[SpimexTradingResultAsync]: Список торговых результатов.
    """
    query = select(SpimexTradingResultAsync).where(
        and_(
            SpimexTradingResultAsync.oil_id == oil_id,
            SpimexTradingResultAsync.delivery_type_id == delivery_type_id,
            SpimexTradingResultAsync.delivery_basis_id == delivery_basis_id,
            SpimexTradingResultAsync.date >= start_date,
            SpimexTradingResultAsync.date <= end_date
        )
    ).order_by(SpimexTradingResultAsync.date.desc())
    result = await db.execute(query)
    return result.scalars().all()


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
