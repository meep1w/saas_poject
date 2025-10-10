from __future__ import annotations

from typing import AsyncGenerator

from sqlalchemy import event
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.settings import settings


# -----------------------------------------------------------------------------
# Engine & Session
# -----------------------------------------------------------------------------
# ВАЖНО: settings.DATABASE_URL должен быть ASYNC-URL,
# например:
#  - SQLite:  "sqlite+aiosqlite:///./saas.db"
#  - Postgres: "postgresql+asyncpg://user:pass@host:5432/dbname"
DATABASE_URL = settings.DATABASE_URL

engine: AsyncEngine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,  # полезно для долгоживущих коннектов
    future=True,
)

SessionLocal = async_sessionmaker(
    bind=engine,
    autoflush=False,
    expire_on_commit=False,
    class_=AsyncSession,
)


class Base(DeclarativeBase):
    """База для всех моделей."""
    pass


# -----------------------------------------------------------------------------
# SQLite PRAGMAs (если используем SQLite)
# -----------------------------------------------------------------------------
# Для async-движка слушаем sync_engine, чтобы выполнить PRAGMA на низком уровне.
if URL.create(DATABASE_URL).get_backend_name().startswith("sqlite"):

    @event.listens_for(engine.sync_engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):  # type: ignore[no-redef]
        # WAL — лучше для конкурентного доступа.
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA synchronous=NORMAL;")
            cursor.execute("PRAGMA foreign_keys=ON;")
        finally:
            cursor.close()


# -----------------------------------------------------------------------------
# Init / Shutdown helpers
# -----------------------------------------------------------------------------
async def init_db() -> None:
    """
    Создаёт таблицы, если их ещё нет.
    Вызывай один раз при старте приложения (до работы воркеров).
    """
    # Импортируем модели, чтобы они зарегистрировались в Base.metadata
    from app import models  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def shutdown_db() -> None:
    """Аккуратно закрыть соединения при остановке приложения."""
    await engine.dispose()


# -----------------------------------------------------------------------------
# FastAPI dependency helper (если нужно)
# -----------------------------------------------------------------------------
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Зависимость для FastAPI:
        async def endpoint(session: AsyncSession = Depends(get_session)):
            ...
    """
    async with SessionLocal() as session:
        yield session
