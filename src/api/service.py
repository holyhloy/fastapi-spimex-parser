import datetime
from typing import Optional

from sqlalchemy import select, desc, and_, func, Sequence, RowMapping, Row

from src.models.spimex_trading_results import SpimexTradingResult
from src.parser.spimex_trading_results import URLManager
from src.api.dependencies import SessionDep


async def parse_spimex(parser: URLManager()) -> None:
    """
    Parsing spimex.com to get a trading results data for
    a period from 2023-01-09 until last result
    Checks if database date is relevant: if the last date in db
    is equal to last date on spimex.com, only prints a message

    :param parser: URLManager class object that provides parsing methods

    :return: None
    move data directly to the database
    """
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
async def get_last_trading_dates(session: SessionDep, amount_of_days: int) -> Sequence[Row | RowMapping]:
    """
    Receiving a specified amount of last trading dates from database

    :param session: database AsyncSession
    :param amount_of_days: integer value of amount of dates need to get

    :return: database response - Sequence[Row | RowMapping]
    """
    if amount_of_days <= 0:
        raise ValueError("Amount of days must be positive.")

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
                       ) -> Sequence[Row | RowMapping]:
    """
    Receiving trading result for a specified period from database

    :param session: database AsyncSession
    :param start_date: datetime object, provides the upper border of a period
    :param end_date: datetime object, provides the lower border of a period
    :param oil_id: value from database, using to filter the result, not required
    :param delivery_type_id: value from database, using to filter the result, not required
    :param delivery_basis_id: value from database, using to filter the result, not required

    :return: database response - Sequence[Row | RowMapping]
    """
    if start_date >= end_date:
        raise ValueError('Start date must be less or equal to the end date.')

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
                              ) -> Sequence[Row | RowMapping]:
    """
    Receiving last trading results from database

    :param session: database AsyncSession
    :param oil_id: value from database, using to filter the result, not required
    :param delivery_type_id: value from database, using to filter the result, not required
    :param delivery_basis_id: value from database, using to filter the result, not required

    :return: database response - Sequence[Row | RowMapping]
    """
    newest_date_subquery = select(func.max(SpimexTradingResult.date)).scalar_subquery()
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
