# app/services/trading_service.py

import logging
from typing import List, Optional
from datetime import datetime, date

from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories import TradingRepository
from app.schemas import TradingResult

logger = logging.getLogger("app.services.trading_service")


class TradingService:
    """
    Сервис для обработки бизнес-логики торговли.
    """

    def __init__(self, db: AsyncSession) -> None:
        """
        Инициализация сервиса.

        Args:
            db (AsyncSession): Асинхронная сессия базы данных.
        """
        self.repository = TradingRepository(db)

    async def get_last_trading_dates(self, limit: int) -> List[date]:
        """
        Получает последние торговые даты.

        Args:
            limit (int): Количество последних торговых дней.

        Returns:
            List[date]: Список дат.
        """
        logger.info(
            f"Сервис: получение последних торговых дат с лимитом {limit}")
        return await self.repository.get_last_trading_dates(limit)

    async def get_dynamics(
        self,
        oil_id: Optional[str],
        delivery_type_id: Optional[str],
        delivery_basis_id: Optional[str],
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ) -> List[TradingResult]:
        """
        Получает динамику торгов за заданный период с фильтрацией.

        Args:
            oil_id (Optional[str]): Идентификатор типа нефти.
            delivery_type_id (Optional[str]): Идентификатор типа доставки.
            delivery_basis_id (Optional[str]): Идентификатор условия доставки.
            start_date (Optional[datetime]): Начальная дата периода.
            end_date (Optional[datetime]): Конечная дата периода.

        Returns:
            List[TradingResult]: Список торговых результатов.
        """
        logger.info("Сервис: получение динамики торгов")
        return await self.repository.get_dynamics(
            oil_id,
            delivery_type_id,
            delivery_basis_id,
            start_date,
            end_date
        )

    async def get_trading_results(
        self,
        oil_id: Optional[str],
        delivery_type_id: Optional[str],
        delivery_basis_id: Optional[str],
        limit: int
    ) -> List[TradingResult]:
        """
        Получает последние торговые результаты с фильтрацией.

        Args:
            oil_id (Optional[str]): Идентификатор типа нефти.
            delivery_type_id (Optional[str]): Идентификатор типа доставки.
            delivery_basis_id (Optional[str]): Идентификатор условия доставки.
            limit (int): Количество последних торгов.

        Returns:
            List[TradingResult]: Список торговых результатов.
        """
        logger.info("Сервис: получение торговых результатов")
        return await self.repository.get_trading_results(
            oil_id,
            delivery_type_id,
            delivery_basis_id,
            limit
        )
