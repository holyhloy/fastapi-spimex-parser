import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from httpx import AsyncClient, ASGITransport
from sqlalchemy import NullPool
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession, create_async_engine

from src.api import main_router
from src.config import settings
from src.database import BaseModel
from src.models.spimex_trading_results import SpimexTradingResult

DATABASE_URL = settings.DB_URL

test_engine = create_async_engine(DATABASE_URL, pool_pre_ping=True, poolclass=NullPool)
test_session = async_sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.include_router(main_router)
    return app


@pytest_asyncio.fixture
async def client(app: FastAPI):
    async with AsyncClient(transport=ASGITransport(app=app),
                           base_url='http://test') as client:
        yield client


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_db():
    async with test_engine.begin() as conn:
        assert settings.MODE == 'TEST'
        await conn.run_sync(BaseModel.metadata.drop_all)
        await conn.run_sync(BaseModel.metadata.create_all)


@pytest_asyncio.fixture
async def session():
    assert settings.DB_NAME == 'spimex_test'
    async with test_session() as session:
        yield session


@pytest.fixture(autouse=True, scope="session")
def init_cache():
    FastAPICache.init(InMemoryBackend(), prefix="test-cache")
