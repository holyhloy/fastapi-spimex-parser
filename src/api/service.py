import sqlalchemy
from sqlalchemy import select, desc, and_

from src.api.schemas import DynamicsResultsSchema
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
async def get_dynamics(session: SessionDep, result_params: DynamicsResultsSchema):
    conditions = [SpimexTradingResult.date.between(result_params.start_date, result_params.end_date)]

    if result_params.oil_id:
        conditions.append(SpimexTradingResult.oil_id == result_params.oil_id)

    if result_params.delivery_type_id:
        conditions.append(SpimexTradingResult.delivery_type_id == result_params.delivery_type_id)

    if result_params.delivery_basis_id:
        conditions.append(SpimexTradingResult.delivery_basis_id == result_params.delivery_basis_id)

    stmt = await session.execute(select(SpimexTradingResult).where(and_(*conditions)))
    results = stmt.scalars().all()
    return results

# список последних торгов (фильтрация по oil_id, delivery_type_id, delivery_basis_id)
async def get_trading_results(session: SessionDep):
    ...