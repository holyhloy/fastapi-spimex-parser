import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from httpx import AsyncClient, ASGITransport

from src.api import main_router


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

@pytest.fixture(autouse=True, scope="session")
def init_cache():
    FastAPICache.init(InMemoryBackend(), prefix="test-cache")