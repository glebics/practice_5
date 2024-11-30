# app/main.py
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi import FastAPI, Depends, HTTPException, Query
from typing import List, Optional
from datetime import datetime, time, timedelta
from .database import get_db
from . import crud, models, schemas
from sqlalchemy.ext.asyncio import AsyncSession
from .cache import set_cache, get_cache, flush_cache
import asyncio

app = FastAPI(title="Spimex Trading Results API")


@app.on_event("startup")
async def startup_event() -> None:
    """
    Обработчик события запуска приложения.

    Запускает асинхронную задачу для планирования сброса кэша каждый день в 14:11.
    """
    asyncio.create_task(schedule_cache_flush())


async def schedule_cache_flush() -> None:
    """
    Планирует сброс кэша Redis каждый день в 14:11.
    """
    while True:
        now = datetime.now()
        flush_time = datetime.combine(now.date(), time(14, 11))
        if now > flush_time:
            flush_time += timedelta(days=1)
        wait_seconds = (flush_time - now).total_seconds()
        await asyncio.sleep(wait_seconds)
        await flush_cache()
        print("Кэш Redis был сброшен в 14:11")


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


@app.get("/get_last_trading_dates", response_model=List[datetime], summary="Получить даты последних торговых дней")
async def get_last_trading_dates(
    limit: int = Query(
        5, ge=1, le=100, description="Количество последних торговых дней"),
    db: AsyncSession = Depends(get_db)
) -> List[datetime]:
    """
    Возвращает список дат последних торговых дней.

    Args:
        limit (int): Количество последних торговых дней (от 1 до 100).
        db (AsyncSession): Асинхронная сессия базы данных.

    Returns:
        List[datetime]: Список дат последних торговых дней.
    """
    cache_key = f"last_trading_dates:{limit}"
    cached = await get_cache(cache_key)
    if cached:
        return cached
    dates = await crud.get_last_trading_dates(db, limit)
    await set_cache(cache_key, dates, expire=get_seconds_until_flush())
    return dates


@app.get("/get_dynamics", response_model=List[schemas.TradingResult], summary="Получить динамику торгов за период")
async def get_dynamics(
    oil_id: str = Query(..., description="Идентификатор типа нефти"),
    delivery_type_id: str = Query(...,
                                  description="Идентификатор типа доставки"),
    delivery_basis_id: str = Query(...,
                                   description="Идентификатор условия доставки"),
    start_date: datetime = Query(...,
                                 description="Начальная дата периода (формат: YYYY-MM-DD)"),
    end_date: datetime = Query(...,
                               description="Конечная дата периода (формат: YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db)
) -> List[schemas.TradingResult]:
    """
    Возвращает список торгов за заданный период с фильтрацией по параметрам.

    Args:
        oil_id (str): Идентификатор типа нефти.
        delivery_type_id (str): Идентификатор типа доставки.
        delivery_basis_id (str): Идентификатор условия доставки.
        start_date (datetime): Начальная дата периода.
        end_date (datetime): Конечная дата периода.
        db (AsyncSession): Асинхронная сессия базы данных.

    Returns:
        List[schemas.TradingResult]: Список торговых результатов за указанный период.
    """
    cache_key = f"dynamics:{oil_id}:{delivery_type_id}:{delivery_basis_id}:{start_date}:{end_date}"
    cached = await get_cache(cache_key)
    if cached:
        return cached
    trading_results = await crud.get_dynamics(db, oil_id, delivery_type_id, delivery_basis_id, start_date, end_date)
    results = [schemas.TradingResult.from_orm(tr) for tr in trading_results]
    await set_cache(cache_key, [result.dict() for result in results], expire=get_seconds_until_flush())
    return results


@app.get("/get_trading_results", response_model=List[schemas.TradingResult], summary="Получить последние торговые результаты")
async def get_trading_results(
    oil_id: str = Query(..., description="Идентификатор типа нефти"),
    delivery_type_id: str = Query(...,
                                  description="Идентификатор типа доставки"),
    delivery_basis_id: str = Query(...,
                                   description="Идентификатор условия доставки"),
    limit: int = Query(
        10, ge=1, le=100, description="Количество последних торгов"),
    db: AsyncSession = Depends(get_db)
) -> List[schemas.TradingResult]:
    """
    Возвращает список последних торгов с фильтрацией по параметрам.

    Args:
        oil_id (str): Идентификатор типа нефти.
        delivery_type_id (str): Идентификатор типа доставки.
        delivery_basis_id (str): Идентификатор условия доставки.
        limit (int, optional): Количество последних торгов (от 1 до 100). Defaults to 10.
        db (AsyncSession): Асинхронная сессия базы данных.

    Returns:
        List[schemas.TradingResult]: Список последних торговых результатов.
    """
    cache_key = f"trading_results:{oil_id}:{delivery_type_id}:{delivery_basis_id}:{limit}"
    cached = await get_cache(cache_key)
    if cached:
        return cached
    trading_results = await crud.get_trading_results(db, oil_id, delivery_type_id, delivery_basis_id, limit)
    results = [schemas.TradingResult.from_orm(tr) for tr in trading_results]
    await set_cache(cache_key, [result.dict() for result in results], expire=get_seconds_until_flush())
    return results

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
    return JSONResponse(
        status_code=500,
        content={"detail": "Внутренняя ошибка сервера"},
    )
