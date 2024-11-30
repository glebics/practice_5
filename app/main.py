# app/main.py

from . import crud, schemas
from datetime import datetime
import asyncio
import logging
from datetime import datetime, date, time, timedelta
from typing import List, Optional

from fastapi import Depends, FastAPI, Query
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.exceptions import HTTPException as StarletteHTTPException

from .cache import set_cache, get_cache, flush_cache
from .database import get_db
from . import crud, models, schemas

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


@app.on_event("startup")
async def startup_event() -> None:
    """
    Обработчик события запуска приложения.

    Запускает асинхронную задачу для планирования сброса кэша каждый день в 14:11.
    """
    logger.info("Запуск приложения и инициализация задач.")
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
        logger.info(f"Запланирован сброс кэша через {wait_seconds} секунд.")
        await asyncio.sleep(wait_seconds)
        await flush_cache()
        logger.info("Кэш Redis был сброшен в 14:11.")


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
    db: AsyncSession = Depends(get_db)
) -> List[date]:
    """
    Возвращает список дат последних торговых дней.

    Args:
        limit (int): Количество последних торговых дней (от 1 до 100).
        db (AsyncSession): Асинхронная сессия базы данных.

    Returns:
        List[date]: Список дат последних торговых дней.
    """
    cache_key = f"last_trading_dates:{limit}"
    cached = await get_cache(cache_key)
    if cached:
        logger.info(f"Кэш найден для ключа {cache_key}")
        try:
            # Преобразование строковых дат обратно в объекты date
            return [datetime.fromisoformat(date_str).date() for date_str in cached]
        except ValueError as ve:
            logger.error(f"Ошибка преобразования дат из кэша: {ve}")
            # В случае ошибки преобразования удаляем некорректный кэш
            await flush_cache()
            logger.info("Некорректный кэш был очищен.")

    # Получение данных из базы данных
    dates = await crud.get_last_trading_dates(db, limit)
    # Преобразование объектов date в строковый формат для кэширования
    dates_str = [d.isoformat() for d in dates]
    await set_cache(cache_key, dates_str, expire=get_seconds_until_flush())
    logger.info(f"Кэш установлен для ключа {cache_key}")
    return dates


@app.get(
    "/get_dynamics",
    response_model=List[schemas.TradingResult],
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
    db: AsyncSession = Depends(get_db)
) -> List[schemas.TradingResult]:
    """
    Возвращает список торгов за заданный период с фильтрацией по параметрам.

    Args:
        oil_id (Optional[str]): Идентификатор типа нефти.
        delivery_type_id (Optional[str]): Идентификатор типа доставки.
        delivery_basis_id (Optional[str]): Идентификатор условия доставки.
        start_date (Optional[datetime]): Начальная дата периода.
        end_date (Optional[datetime]): Конечная дата периода.
        db (AsyncSession): Асинхронная сессия базы данных.

    Returns:
        List[schemas.TradingResult]: Список торговых результатов за указанный период.
    """
    logger.info(
        f"Вызов эндпоинта /get_dynamics с параметрами oil_id={oil_id}, delivery_type_id={delivery_type_id}, delivery_basis_id={delivery_basis_id}, start_date={start_date}, end_date={end_date}")

    trading_results = await crud.get_dynamics(
        db,
        oil_id,
        delivery_type_id,
        delivery_basis_id,
        start_date,
        end_date
    )
    logger.info(f"Получены торговые результаты: {trading_results}")

    # Преобразование объектов модели в Pydantic схемы
    results = [schemas.TradingResult.from_orm(tr) for tr in trading_results]
    return results


@app.get(
    "/get_trading_results",
    response_model=List[schemas.TradingResult],
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
        10,
        ge=1,
        le=100,
        description="Количество последних торгов (от 1 до 100)"
    ),
    db: AsyncSession = Depends(get_db)
) -> List[schemas.TradingResult]:
    """
    Возвращает список последних торгов с фильтрацией по параметрам.

    Args:
        oil_id (Optional[str]): Идентификатор типа нефти.
        delivery_type_id (Optional[str]): Идентификатор типа доставки.
        delivery_basis_id (Optional[str]): Идентификатор условия доставки.
        limit (int, optional): Количество последних торгов (от 1 до 100). Defaults to 10.
        db (AsyncSession): Асинхронная сессия базы данных.

    Returns:
        List[schemas.TradingResult]: Список последних торговых результатов.
    """
    cache_key = f"trading_results:{oil_id}:{delivery_type_id}:{delivery_basis_id}:{limit}"
    cached = await get_cache(cache_key)
    if cached:
        logger.info(f"Кэш найден для ключа {cache_key}")
        try:
            return [schemas.TradingResult(**item) for item in cached]
        except Exception as e:
            logger.error(f"Ошибка преобразования данных из кэша: {e}")
            await flush_cache()
            logger.info("Некорректный кэш был очищен.")

    trading_results = await crud.get_trading_results(
        db,
        oil_id,
        delivery_type_id,
        delivery_basis_id,
        limit
    )
    results = [schemas.TradingResult.from_orm(tr) for tr in trading_results]
    results_dict = [result.dict() for result in results]
    await set_cache(cache_key, results_dict, expire=get_seconds_until_flush())
    logger.info(f"Кэш установлен для ключа {cache_key}")
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
