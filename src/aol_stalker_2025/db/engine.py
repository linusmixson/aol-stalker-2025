from collections.abc import AsyncGenerator, Generator
from typing import Annotated

from fastapi import Depends
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import Session
from sqlmodel.ext.asyncio.session import AsyncSession

from aol_stalker_2025.config import config

# Create both sync and async engines
engine = create_engine(config.psycopg_postgres_connection_url())
async_engine = create_async_engine(config.asyncpg_postgres_connection_url())


def get_session() -> Generator[Session]:
    with Session(engine) as session:
        yield session


async def get_async_session() -> AsyncGenerator[AsyncSession]:
    async with AsyncSession(async_engine) as session:
        yield session


SessionDependency = Annotated[Session, Depends(get_session)]
AsyncSessionDependency = Annotated[AsyncSession, Depends(get_async_session)]
