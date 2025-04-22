from fastapi import APIRouter

router = APIRouter()

# список дат последних торговых дней (фильтрация по кол-ву последних торговых дней).
@router.get()
async def get_last_trading_dates():
    ...

# список торгов за заданный период (фильтрация по oil_id, delivery_type_id,
# delivery_basis_id, start_date, end_date).
@router.get()
async def get_dynamics():
    ...

# список последних торгов (фильтрация по oil_id, delivery_type_id, delivery_basis_id)
@router.get()
async def get_trading_results():
    ...