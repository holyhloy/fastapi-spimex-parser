from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend

from redis import asyncio as aioredis

from src.api.service import parse_spimex
from src.database import create_db
from src.api import main_router
from src.parser.spimex_trading_results import URLManager


async def clear_cache():
    await FastAPICache.clear()

scheduler = AsyncIOScheduler()
scheduler.add_job(clear_cache, 'cron', hour=12, minute=15)


@asynccontextmanager
async def lifespan(application: FastAPI):
    redis = aioredis.from_url("redis://localhost")
    FastAPICache.init(RedisBackend(redis), prefix="api:cache")
    scheduler.start()
    await create_db()
    await parse_spimex(URLManager())
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(main_router)
