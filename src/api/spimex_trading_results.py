from fastapi import APIRouter

from src.api import service
from src.api.dependencies import SessionDep

router = APIRouter()

# список дат последних торговых дней (фильтрация по кол-ву последних торговых дней).
@router.get('/last_dates',
            tags=['Операции с результатами торгов'],
            summary='Получить список дат последний торговый дней')
async def get_last_trading_dates(session: SessionDep):
    last_dates = await service.get_last_trading_dates(session)
    return {'success': True, 'last_trading_dates': last_dates}

# список торгов за заданный период (фильтрация по oil_id, delivery_type_id,
# delivery_basis_id, start_date, end_date).
@router.get('/dynamics',
            tags=['Операции с результатами торгов'],
            summary='Получить список торгов за заданный период'
            )
async def get_dynamics(session: SessionDep):
    dynamics = await service.get_dynamics(session)
    return {'success': True, 'dynamics': dynamics}

# список последних торгов (фильтрация по oil_id, delivery_type_id, delivery_basis_id)
@router.get('/last_results',
            tags=['Операции с результатами торгов'],
            summary='Получить список последних торгов'
            )
async def get_trading_results(session: SessionDep):
    results = await service.get_trading_results(session)
    return {'success': True, 'results': results}