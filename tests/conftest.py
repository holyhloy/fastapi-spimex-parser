import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from httpx import AsyncClient, ASGITransport

from src.api import main_router
from src.config import settings
from src.database import BaseModel, engine


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
    async with engine.begin() as conn:
        await conn.run_sync(BaseModel.metadata.drop_all)
        await conn.run_sync(BaseModel.metadata.create_all)


@pytest.fixture(autouse=True, scope="session")
def init_cache():
    FastAPICache.init(InMemoryBackend(), prefix="test-cache")
