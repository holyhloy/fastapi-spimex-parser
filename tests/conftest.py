import pytest
from fastapi import FastAPI
from httpx import AsyncClient, ASGITransport

from src.api import main_router


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.include_router(main_router)
    return app

@pytest.fixture
def client(app: FastAPI):
    async with AsyncClient(transport=ASGITransport(app=app),
                           base_url='http://test') as client:
        yield client