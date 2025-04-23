from fastapi import APIRouter

from src.api import service
from src.api.dependencies import SessionDep
from src.api.schemas import DynamicsResultsSchema

router = APIRouter()

# список дат последних торговых дней (фильтрация по кол-ву последних торговых дней).
@router.get('/last{amount}dates',
            tags=['Операции с результатами торгов'],
            summary='Получить список дат последний торговый дней')
async def get_last_trading_dates(session: SessionDep, amount: int):
    last_dates = await service.get_last_trading_dates(session, amount)
    return {'success': True, 'last_trading_dates': last_dates}

# список торгов за заданный период (фильтрация по oil_id, delivery_type_id,
# delivery_basis_id, start_date, end_date).
@router.post('/dynamics',
            tags=['Операции с результатами торгов'],
            summary='Получить список торгов за заданный период'
            )
async def get_dynamics(session: SessionDep, result_params: DynamicsResultsSchema):
    dynamics = await service.get_dynamics(session, result_params)
    return {'success': True, 'dynamics': dynamics}

# список последних торгов (фильтрация по oil_id, delivery_type_id, delivery_basis_id)
@router.get('/last_results',
            tags=['Операции с результатами торгов'],
            summary='Получить список последних торгов'
            )
async def get_trading_results(session: SessionDep):
    results = await service.get_trading_results(session)
    return {'success': True, 'results': results}