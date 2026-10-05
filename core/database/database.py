import ssl
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from core.config import settings


# Base model for the CRM database
class CrmBase(DeclarativeBase):
    pass


# Base model for the local database
class LocalBase(DeclarativeBase):
    pass


def _build_connect_args(setting_db_ssl: bool) -> dict:
    """Build asyncpg connect_args, enabling SSL/TLS for non-local environments."""
    if not setting_db_ssl:
        return {}
    ctx = ssl.create_default_context()
    return {"ssl": ctx}


crm_engine = create_async_engine(
    settings.CRM_DATABASE_URL,
    pool_pre_ping=True,
    pool_size=settings.CRM_DB_POOL_SIZE,
    max_overflow=settings.CRM_DB_MAX_OVERFLOW,
    pool_recycle=settings.CRM_DB_POOL_RECYCLE,
    pool_timeout=settings.CRM_DB_POOL_TIMEOUT,
    connect_args=_build_connect_args(settings.CRM_DB_SSL),
)

local_engine = create_async_engine(
    settings.LOCAL_DATABASE_URL,
    pool_pre_ping=True,
    pool_size=settings.LOCAL_DB_POOL_SIZE,
    max_overflow=settings.LOCAL_DB_MAX_OVERFLOW,
    pool_recycle=settings.LOCAL_DB_POOL_RECYCLE,
    pool_timeout=settings.LOCAL_DB_POOL_TIMEOUT,
    connect_args=_build_connect_args(settings.LOCAL_DB_SSL),
)


AsyncSessionCRMFactory = async_sessionmaker(
    crm_engine,
    expire_on_commit=False,  # prevents lazy-load errors after commit in async context
)

AsyncSessionLocalFactory = async_sessionmaker(
    local_engine,
    expire_on_commit=False,  # prevents lazy-load errors after commit in async context
)


async def get_crm_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionCRMFactory() as session:
        yield session


async def get_local_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocalFactory() as session:
        yield session
