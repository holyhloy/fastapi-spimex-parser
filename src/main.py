from contextlib import asynccontextmanager
from typing import AsyncGenerator, Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend

from redis import asyncio as aioredis

from src.api.service import parse_spimex
from src.config import settings
from src.database import create_db
from src.api import main_router
from src.parser.spimex_trading_results import URLManager


async def clear_cache():
    await FastAPICache.clear()

scheduler = AsyncIOScheduler()
scheduler.add_job(clear_cache, 'cron', hour=14, minute=11)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncGenerator[Any, Any | None]:
    """
    Function provides Postgres and Redis pulling before
    app is started

    :param _: does nothing, using just to put it
    into lifespan param of app initialization

    :return: AsyncGenerator[Any | None]
    """
    redis = aioredis.from_url(f"redis://{settings.REDIS_HOST}")
    FastAPICache.init(RedisBackend(redis), prefix="api:cache")
    scheduler.start()
    await create_db()
    await parse_spimex(URLManager())
    yield


app = FastAPI(lifespan=lifespan)
app.include_router(main_router)
