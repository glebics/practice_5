# app/models.py
from typing import Optional
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.sql import func
from .database import Base  # Импорт базового класса
import datetime


class SpimexTradingResultAsync(Base):
    """
    Модель SQLAlchemy для таблицы spimex_trading_results_async.

    Атрибуты:
        pk_spimex_id (int): Уникальный идентификатор записи.
        exchange_product_id (str): Идентификатор биржевого продукта.
        exchange_product_name (Optional[str]): Название биржевого продукта.
        oil_id (Optional[str]): Идентификатор типа нефти.
        delivery_basis_id (Optional[str]): Идентификатор условия доставки.
        delivery_basis_name (Optional[str]): Название условия доставки.
        delivery_type_id (Optional[str]): Идентификатор типа доставки.
        volume (Optional[float]): Объем торгового продукта.
        total (Optional[float]): Общая стоимость сделки.
        count (Optional[int]): Количество сделок.
        date (datetime): Дата проведения торгов.
        created_on (datetime): Дата создания записи.
        updated_on (datetime): Дата последнего обновления записи.
    """
    __tablename__ = 'spimex_trading_results_async'
    __table_args__ = {
        "comment": "Таблица, содержащая результаты торгов СПбМТСБ (Асинхронная версия)"
    }

    pk_spimex_id: int = Column(
        Integer, primary_key=True, index=True,
        comment="Уникальный идентификатор записи"
    )
    exchange_product_id: str = Column(
        String, nullable=False, comment="Идентификатор биржевого продукта"
    )
    exchange_product_name: Optional[str] = Column(
        String, nullable=True, comment="Название биржевого продукта"
    )
    oil_id: Optional[str] = Column(
        String, nullable=True, comment="Идентификатор типа нефти")
    delivery_basis_id: Optional[str] = Column(
        String, nullable=True, comment="Идентификатор условия доставки"
    )
    delivery_basis_name: Optional[str] = Column(
        String, nullable=True, comment="Название условия доставки"
    )
    delivery_type_id: Optional[str] = Column(
        String, nullable=True, comment="Идентификатор типа доставки"
    )
    volume: Optional[float] = Column(
        Float, nullable=True, comment="Объем торгового продукта")
    total: Optional[float] = Column(
        Float, nullable=True, comment="Общая стоимость сделки")
    count: Optional[int] = Column(
        Integer, nullable=True, comment="Количество сделок")
    date: datetime = Column(
        DateTime, nullable=False, default=func.now(),
        comment="Дата проведения торгов"
    )
    created_on: datetime = Column(
        DateTime, server_default=func.now(),
        comment="Дата создания записи"
    )
    updated_on: datetime = Column(
        DateTime, server_default=func.now(),
        onupdate=func.now(), comment="Дата последнего обновления записи"
    )
