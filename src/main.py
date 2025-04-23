import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.service import parse_spimex
from src.database import create_db
from src.api import main_router
from src.parser.spimex_trading_results import URLManager


@asynccontextmanager
async def on_startup(application: FastAPI):
    await create_db()
    await parse_spimex(URLManager())
    yield


app = FastAPI(lifespan=on_startup)
app.include_router(main_router)
