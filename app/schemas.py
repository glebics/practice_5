# app/schemas.py
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List


class TradingResultBase(BaseModel):
    """
    Базовая Pydantic схема для торгового результата.

    Атрибуты:
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
    """
    exchange_product_id: str = Field(...,
                                     description="Идентификатор биржевого продукта")
    exchange_product_name: Optional[str] = Field(
        None, description="Название биржевого продукта")
    oil_id: Optional[str] = Field(None, description="Идентификатор типа нефти")
    delivery_basis_id: Optional[str] = Field(
        None, description="Идентификатор условия доставки")
    delivery_basis_name: Optional[str] = Field(
        None, description="Название условия доставки")
    delivery_type_id: Optional[str] = Field(
        None, description="Идентификатор типа доставки")
    volume: Optional[float] = Field(
        None, description="Объем торгового продукта")
    total: Optional[float] = Field(None, description="Общая стоимость сделки")
    count: Optional[int] = Field(None, description="Количество сделок")
    date: datetime = Field(..., description="Дата проведения торгов")


class TradingResultCreate(TradingResultBase):
    """
    Pydantic схема для создания торгового результата.
    """
    pass


class TradingResult(TradingResultBase):
    """
    Pydantic схема для отображения торгового результата.

    Атрибуты:
        pk_spimex_id (int): Уникальный идентификатор записи.
        created_on (datetime): Дата создания записи.
        updated_on (datetime): Дата последнего обновления записи.
    """
    pk_spimex_id: int = Field(...,
                              description="Уникальный идентификатор записи")
    created_on: datetime = Field(..., description="Дата создания записи")
    updated_on: datetime = Field(...,
                                 description="Дата последнего обновления записи")

    class Config:
        from_attributes = True
