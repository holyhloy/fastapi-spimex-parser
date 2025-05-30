import datetime

from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from src.database import BaseModel


class SpimexTradingResult(BaseModel):
	__tablename__ = 'spimex_trading_results'

	id: Mapped[int] = mapped_column(primary_key=True)
	exchange_product_id: Mapped[str] = mapped_column()
	exchange_product_name: Mapped[str] = mapped_column(String(255))
	oil_id: Mapped[str] = mapped_column(String(10))
	delivery_basis_id: Mapped[str] = mapped_column(String(10))
	delivery_basis_name: Mapped[str] = mapped_column(String(64))
	delivery_type_id: Mapped[str] = mapped_column(String(10))
	volume: Mapped[str] = mapped_column(String(255))
	total: Mapped[str] = mapped_column(String(255))
	count: Mapped[str] = mapped_column(String(255))
	date: Mapped[datetime.date] = mapped_column()
	created_on: Mapped[datetime.date] = mapped_column()
	updated_on: Mapped[datetime.date] = mapped_column(nullable=True)
