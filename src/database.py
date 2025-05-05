from typing import Any, AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from src.config import settings

DATABASE_URL = settings.DB_URL


class BaseModel(AsyncAttrs, DeclarativeBase):
    __abstract__ = True


engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def create_db() -> None:  # pragma: no cover
    """
    Creates tables in database.
    Does nothing if tables are already exist

    :return: None
    Just creates table if it doesn't exist and returns nothing
    """
    async with engine.begin() as conn:
        print('Creating databases')
        await conn.run_sync(BaseModel.metadata.create_all)


async def get_session() -> AsyncGenerator[AsyncSession, Any]:
    """
    Yields session for manipulating with database data

    :return: session object
    """
    async with Session() as session:
        yield session
