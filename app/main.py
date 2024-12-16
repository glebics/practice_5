# app/main.py

import asyncio
import logging
from datetime import datetime, date, time, timedelta
from typing import List, Optional

from fastapi import Depends, FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from .database import get_db
from .cache import flush_cache
from .schemas import TradingResult
from .services.trading_service import TradingService
from .utils import schedule_cache_flush

from sqlalchemy.ext.asyncio import AsyncSession

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("app")

app = FastAPI(title="Spimex Trading Results API")


def get_trading_service(db: AsyncSession = Depends(get_db)) -> TradingService:
    """
    Зависимость для получения экземпляра TradingService.

    Args:
        db (AsyncSession): Асинхронная сессия базы данных.

    Returns:
        TradingService: Экземпляр сервиса торговли.
    """
    return TradingService(db)


@app.on_event("startup")
async def startup_event() -> None:
    """
    Обработчик события запуска приложения.
    Запускает асинхронную задачу для планирования сброса кэша каждый день в 14:11.
    """
    logger.info("Запуск приложения и инициализация задач.")
    asyncio.create_task(schedule_cache_flush())


@app.get(
    "/get_last_trading_dates",
    response_model=List[date],
    summary="Получить даты последних торговых дней",
    description="Возвращает список дат последних торговых дней с ограничением по количеству."
)
async def get_last_trading_dates(
    limit: int = Query(
        5,
        ge=1,
        le=100,
        description="Количество последних торговых дней (от 1 до 100)"
    ),
    service: TradingService = Depends(get_trading_service)
) -> List[date]:
    """
    Возвращает список дат последних торговых дней.

    Args:
        limit (int): Количество последних торговых дней (от 1 до 100).
        service (TradingService): Сервис для обработки запроса.

    Returns:
        List[date]: Список дат последних торговых дней.
    """
    return await service.get_last_trading_dates(limit)


@app.get(
    "/get_dynamics",
    response_model=List[TradingResult],
    summary="Получить динамику торгов за период",
    description="Возвращает список торгов за заданный период с фильтрацией по параметрам."
)
async def get_dynamics(
    oil_id: Optional[str] = Query(
        None, description="Идентификатор типа нефти"),
    delivery_type_id: Optional[str] = Query(
        None, description="Идентификатор типа доставки"),
    delivery_basis_id: Optional[str] = Query(
        None, description="Идентификатор условия доставки"),
    start_date: Optional[datetime] = Query(
        None, description="Начальная дата периода (формат: YYYY-MM-DD)"),
    end_date: Optional[datetime] = Query(
        None, description="Конечная дата периода (формат: YYYY-MM-DD)"),
    service: TradingService = Depends(get_trading_service)
) -> List[TradingResult]:
    """
    Возвращает список торгов за заданный период с фильтрацией по параметрам.

    Args:
        oil_id (Optional[str]): Идентификатор типа нефти.
        delivery_type_id (Optional[str]): Идентификатор типа доставки.
        delivery_basis_id (Optional[str]): Идентификатор условия доставки.
        start_date (Optional[datetime]): Начальная дата периода.
        end_date (Optional[datetime]): Конечная дата периода.
        service (TradingService): Сервис для обработки запроса.

    Returns:
        List[TradingResult]: Список торговых результатов за указанный период.
    """
    return await service.get_dynamics(
        oil_id,
        delivery_type_id,
        delivery_basis_id,
        start_date,
        end_date
    )


@app.get(
    "/get_trading_results",
    response_model=List[TradingResult],
    summary="Получить последние торговые результаты",
    description="Возвращает список последних торгов с фильтрацией по параметрам и ограничением по количеству."
)
async def get_trading_results(
    oil_id: Optional[str] = Query(
        None, description="Идентификатор типа нефти"),
    delivery_type_id: Optional[str] = Query(
        None, description="Идентификатор типа доставки"),
    delivery_basis_id: Optional[str] = Query(
        None, description="Идентификатор условия доставки"),
    limit: int = Query(
        10, ge=1, le=100, description="Количество последних торгов (от 1 до 100)"),
    service: TradingService = Depends(get_trading_service)
) -> List[TradingResult]:
    """
    Возвращает список последних торгов с фильтрацией по параметрам.

    Args:
        oil_id (Optional[str]): Идентификатор типа нефти.
        delivery_type_id (Optional[str]): Идентификатор типа доставки.
        delivery_basis_id (Optional[str]): Идентификатор условия доставки.
        limit (int): Количество последних торгов (от 1 до 100).
        service (TradingService): Сервис для обработки запроса.

    Returns:
        List[TradingResult]: Список последних торговых результатов.
    """
    return await service.get_trading_results(
        oil_id,
        delivery_type_id,
        delivery_basis_id,
        limit
    )

# Глобальные обработчики исключений


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc: StarletteHTTPException) -> JSONResponse:
    """
    Обработчик HTTP исключений.

    Args:
        request: Запрос, вызвавший исключение.
        exc (StarletteHTTPException): Исключение HTTP.

    Returns:
        JSONResponse: Ответ с деталями исключения.
    """
    logger.error(f"HTTP исключение: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc: RequestValidationError) -> JSONResponse:
    """
    Обработчик ошибок валидации данных.

    Args:
        request: Запрос, вызвавший исключение.
        exc (RequestValidationError): Исключение валидации.

    Returns:
        JSONResponse: Ответ с деталями ошибок валидации.
    """
    logger.error(f"Ошибка валидации данных: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors()},
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request, exc: Exception) -> JSONResponse:
    """
    Обработчик всех остальных исключений.

    Args:
        request: Запрос, вызвавший исключение.
        exc (Exception): Исключение.

    Returns:
        JSONResponse: Ответ с общей ошибкой.
    """
    logger.error(f"Необработанное исключение: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Внутренняя ошибка сервера"},
    )
