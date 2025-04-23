import datetime
from typing import Optional

import sqlalchemy
from sqlalchemy import select, desc, and_, func

from src.models.spimex_trading_results import SpimexTradingResult
from src.parser.spimex_trading_results import URLManager
from src.api.dependencies import SessionDep


async def parse_spimex(parser: URLManager()):
    relevant = await parser.get_data_from_query()
    if not relevant:
        await parser.download_tables()
        parser.convert_to_df()
        parser.validate_tables()
        parser.add_columns()
        await parser.load_to_db()
    else:
        print('Database has relevant data')


# список дат последних торговых дней (фильтрация по кол-ву последних торговых дней).
async def get_last_trading_dates(session: SessionDep, amount_of_days: int):
    stmt = await session.execute(select(SpimexTradingResult.date)
                                 .group_by(SpimexTradingResult.date)
                                 .order_by(desc(SpimexTradingResult.date))
                                 .limit(amount_of_days))
    result = stmt.scalars().all()
    return result

# список торгов за заданный период (фильтрация по oil_id, delivery_type_id,
# delivery_basis_id, start_date, end_date).
async def get_dynamics(session: SessionDep,
                       start_date: datetime.date,
                       end_date: datetime.date,
                       oil_id: Optional[str | None] = None,
                       delivery_type_id: Optional[str | None] = None,
                       delivery_basis_id: Optional[str | None] = None,
                       ):
    if start_date >= end_date:
        raise ValueError('Start date must be less than end date.')

    conditions = [SpimexTradingResult.date.between(start_date, end_date)]

    filters = {
        SpimexTradingResult.oil_id: oil_id,
        SpimexTradingResult.delivery_type_id: delivery_type_id,
        SpimexTradingResult.delivery_basis_id: delivery_basis_id
    }

    conditions += [column == value for column, value in filters.items() if value is not None]

    stmt = await session.execute(select(SpimexTradingResult).where(and_(*conditions)))
    results = stmt.scalars().all()
    return results

# список последних торгов (фильтрация по oil_id, delivery_type_id, delivery_basis_id)
async def get_trading_results(session: SessionDep,
                              oil_id: Optional[str | None] = None,
                              delivery_type_id: Optional[str | None] = None,
                              delivery_basis_id: Optional[str | None] = None
                              ):
    newest_date_subquery = select(func.max(SpimexTradingResult.date)).subquery()
    conditions = [SpimexTradingResult.date == newest_date_subquery]

    filters = {
        SpimexTradingResult.oil_id: oil_id,
        SpimexTradingResult.delivery_type_id: delivery_type_id,
        SpimexTradingResult.delivery_basis_id: delivery_basis_id
    }

    conditions += [column == value for column, value in filters.items() if value is not None]

    stmt = await session.execute(select(SpimexTradingResult).where(and_(*conditions)))
    results = stmt.scalars().all()
    return results