# app/database.py
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from .settings import async_settings
from sqlalchemy.ext.declarative import declarative_base
from typing import AsyncGenerator

# Создание асинхронного движка
engine = create_async_engine(
    async_settings.async_database_url,
    echo=False,
    future=True,
    pool_pre_ping=True,
)

# Создание базового класса для моделей
Base = declarative_base()

# Создание фабрики сессий
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    Зависимость для получения асинхронной сессии базы данных.

    Yields:
        AsyncSession: Асинхронная сессия SQLAlchemy.
    """
    async with AsyncSessionLocal() as session:
        yield session
