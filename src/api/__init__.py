from fastapi import APIRouter

from src.api.spimex_trading_results import router as trading_router

main_router = APIRouter()
main_router.include_router(trading_router)
