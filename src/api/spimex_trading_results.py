import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException

from src.api import service
from src.api.dependencies import SessionDep

router = APIRouter()

# список дат последних торговых дней (фильтрация по кол-ву последних торговых дней).
@router.get('/last_dates',
            tags=['Операции с результатами торгов'],
            summary='Получить список дат последний торговый дней')
async def get_last_trading_dates(session: SessionDep, amount: int):
    last_dates = await service.get_last_trading_dates(session, amount)
    return {'success': True, 'last_trading_dates': last_dates}

# список торгов за заданный период (фильтрация по oil_id, delivery_type_id,
# delivery_basis_id, start_date, end_date).
@router.get('/dynamics',
            tags=['Операции с результатами торгов'],
            summary='Получить список торгов за заданный период'
            )
async def get_dynamics(session: SessionDep,
                       start_date: datetime.date, end_date: datetime.date,
                       oil_id: Optional[str | None] = None,
                       delivery_type_id: Optional[str | None] = None,
                       delivery_basis_id: Optional[str | None] = None
                       ):
    try:
        dynamics = await service.get_dynamics(session,
                                              start_date, end_date,
                                              oil_id, delivery_type_id, delivery_basis_id
                                              )
        return {'success': True, 'dynamics': dynamics}
    except ValueError as e:
        return HTTPException(status_code=400, detail=f'{e}')

# список последних торгов (фильтрация по oil_id, delivery_type_id, delivery_basis_id)
@router.get('/last_results',
            tags=['Операции с результатами торгов'],
            summary='Получить список последних торгов'
            )
async def get_trading_results(session: SessionDep,
                              oil_id: Optional[str | None] = None,
                              delivery_type_id: Optional[str | None] = None,
                              delivery_basis_id: Optional[str | None] = None
                              ):
    results = await service.get_trading_results(session,
                                                oil_id,
                                                delivery_type_id,
                                                delivery_basis_id)
    return {'success': True, 'results': results}