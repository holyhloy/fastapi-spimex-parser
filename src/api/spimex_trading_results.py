from fastapi import APIRouter

from src.api.service import get_last_trading_dates as get_dates
from src.api.dependencies import SessionDep

router = APIRouter()

# список дат последних торговых дней (фильтрация по кол-ву последних торговых дней).
@router.get('/last_dates')
async def get_last_trading_dates(session: SessionDep):
    results = await get_dates(session)
    return {'success': True, 'last_trading_dates': results}

# список торгов за заданный период (фильтрация по oil_id, delivery_type_id,
# delivery_basis_id, start_date, end_date).
@router.get('/dynamics')
async def get_dynamics():
    ...

# список последних торгов (фильтрация по oil_id, delivery_type_id, delivery_basis_id)
@router.get('/last_results')
async def get_trading_results():
    ...