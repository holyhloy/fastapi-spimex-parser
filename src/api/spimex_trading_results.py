import datetime
from typing import Optional
from urllib.parse import urlparse

from starlette.requests import Request
from starlette.responses import Response

from fastapi import APIRouter, HTTPException

from fastapi_cache.decorator import cache

from src.api import service
from src.api.dependencies import SessionDep

router = APIRouter()


def cache_key_builder(
    func,
    namespace: Optional[str] = "",
    request: Request = None,
    response: Response = None,
    *args,
    **kwargs
) -> str:
    """
    Creates a key for a cache note in Redis, making it unique.
    Solves an adding a note in Redis when it already exists problem.

    :param func: function to wrap
    :param namespace: a name for a note in Redis
    :param request: request object
    :param response: response object
    :param args: unnamed args of the wrapped function
    :param kwargs: keyword args of the wrapped function

    :return: string cache key used for name a note in Redis
    """
    try:
        del kwargs['kwargs']['session']
    except KeyError: # pragma: no cover
        pass
    if request:
        parsed = urlparse(str(request.url))
        cache_key = f"{namespace}:{func.__module__}:{func.__name__}:{parsed.path}:{args}:{kwargs}"
    else:
        cache_key = f"{namespace}:{func.__module__}:{func.__name__}:{args}:{kwargs}" # pragma: no cover
    return cache_key


# список дат последних торговых дней (фильтрация по кол-ву последних торговых дней).
@router.get('/last_dates',
            tags=['Операции с результатами торгов'],
            summary='Получить список дат последних торговый дней')
@cache(key_builder=cache_key_builder)
async def get_last_trading_dates(session: SessionDep, amount: int) -> dict[str, bool]:
    """
    Endpoint that provides GET-query to get a list of last trading days dates

    :param session: database AsyncSession
    :param amount: amount of entries that will be shown

    :return: a dictionary that will be serialized into a JSON,
    containing a bool value of success and dates in string format
    """
    last_dates = await service.get_last_trading_dates(session, amount)
    return {'success': True, 'last_trading_dates': last_dates}


# список торгов за заданный период (фильтрация по oil_id, delivery_type_id,
# delivery_basis_id, start_date, end_date).
@router.get('/dynamics',
            tags=['Операции с результатами торгов'],
            summary='Получить список торгов за заданный период'
            )
@cache(key_builder=cache_key_builder)
async def get_dynamics(session: SessionDep,
                       start_date: datetime.date, end_date: datetime.date,
                       oil_id: Optional[str | None] = None,
                       delivery_type_id: Optional[str | None] = None,
                       delivery_basis_id: Optional[str | None] = None
                       ) -> dict[str, bool] | HTTPException:
    """
    Endpoint that provides GET-query to get a list of trades in some period,
    filtering by oil_id, delivery_type_id, delivery_basis_id optionally.
    Start_date and end_date params are required, because it's impossible to return
    a list of trades for a specified period without specifying this period

    :param session: database AsyncSession
    :param start_date: starting date of a period, must be less than end_date, required
    :param end_date: ending date of a period, required
    :param oil_id: value from database, using to filter the result, not required
    :param delivery_type_id: value from database, using to filter the result, not required
    :param delivery_basis_id: value from database, using to filter the result, not required

    :return: a dictionary that will be serialized into a JSON,
    containing a bool value of success and trades data for
    a specified period in string format
    """
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
@cache(key_builder=cache_key_builder)
async def get_trading_results(session: SessionDep,
                              oil_id: Optional[str | None] = None,
                              delivery_type_id: Optional[str | None] = None,
                              delivery_basis_id: Optional[str | None] = None
                              ) -> dict[str, bool]:
    """
    Endpoint that provides GET-query to get a list of last trades (nearest to today),
    filtering by oil_id, delivery_type_id, delivery_basis_id optionally
    There are no required params, because there is no need of them:
     it's ok to get a list of last trades

    :param session: database AsyncSession
    :param oil_id: value from database, using to filter the result, not required
    :param delivery_type_id: value from database, using to filter the result, not required
    :param delivery_basis_id: value from database, using to filter the result, not required

    :return: a dictionary that will be serialized into a JSON,
    containing a bool value of success and last trades data in string format
    """
    results = await service.get_trading_results(session,
                                                oil_id,
                                                delivery_type_id,
                                                delivery_basis_id)
    return {'success': True, 'last_trading_results': results}
