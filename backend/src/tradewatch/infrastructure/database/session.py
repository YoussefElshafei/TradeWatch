"""Engine and session management.

One engine per process, created at startup and disposed at shutdown. The engine owns the
connection pool, so creating engines ad hoc is how a process ends up with far more
PostgreSQL connections than anyone intended.

Transaction boundaries are explicit. :func:`session_scope` commits on success and rolls
back on any exception, which is the only sane default: a half-applied batch of trades is
worse than no batch at all. Nothing here calls ``commit()`` on behalf of a repository —
repositories issue statements, and whoever owns the unit of work decides when it ends.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Final

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool, QueuePool
from sqlalchemy.sql import text

from tradewatch.config.settings import DatabaseSettings
from tradewatch.observability.logging import get_logger
from tradewatch.observability.metrics import DB_POOL_CONNECTIONS

log = get_logger(__name__)

#: Statement timeout applied to every connection. A runaway analytical query against the
#: bars table should fail rather than hold a connection until someone notices.
DEFAULT_STATEMENT_TIMEOUT_MS: Final = 30_000


def create_engine(
    settings: DatabaseSettings,
    *,
    echo: bool = False,
    use_pool: bool = True,
) -> AsyncEngine:
    """Create the async engine.

    Args:
        settings: Connection URL and pool sizing.
        echo: Log every statement. Useful when debugging a query, noisy otherwise.
        use_pool: Set False to disable pooling. Tests do this so that a dropped database
            is not held open by an idle pooled connection.
    """
    kwargs: dict[str, object] = {
        "echo": echo,
        "future": True,
        # Verify a pooled connection before handing it out. PostgreSQL closes idle
        # connections, and without this the first query after an outage fails rather than
        # transparently reconnecting (failure F8).
        "pool_pre_ping": True,
        "connect_args": {
            "server_settings": {
                "application_name": "tradewatch",
                "timezone": "UTC",
                "statement_timeout": str(DEFAULT_STATEMENT_TIMEOUT_MS),
            }
        },
    }
    if use_pool:
        kwargs |= {
            "pool_size": settings.database_pool_size,
            "max_overflow": settings.database_pool_size,
            "pool_timeout": settings.database_pool_timeout_s,
            "pool_recycle": 1800,
        }
    else:
        kwargs["poolclass"] = NullPool

    return create_async_engine(settings.database_url, **kwargs)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Build the session factory for an engine.

    ``expire_on_commit=False`` because the alternative is that every attribute access
    after a commit issues a fresh SELECT — which inside an async request handler means a
    lazy load at exactly the point where there is no longer a transaction to load it in.
    """
    return async_sessionmaker(
        bind=engine,
        expire_on_commit=False,
        autoflush=False,
        class_=AsyncSession,
    )


@asynccontextmanager
async def session_scope(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """A transactional unit of work: commit on success, roll back on anything else."""
    session = factory()
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
    finally:
        await session.close()


def _export_pool_gauges(engine: AsyncEngine) -> None:
    """Point `tradewatch_db_pool_connections` at this engine's pool, read at scrape time.

    One engine per process in practice, so the last one connected owns the series. A
    `NullPool` (migrations, tests) has no size to report and is skipped.
    """
    pool = engine.sync_engine.pool
    if not isinstance(pool, QueuePool):
        return
    DB_POOL_CONNECTIONS.labels(state="size").set_function(pool.size)
    DB_POOL_CONNECTIONS.labels(state="checked_out").set_function(pool.checkedout)
    DB_POOL_CONNECTIONS.labels(state="idle").set_function(pool.checkedin)
    # `overflow()` counts from -pool_size while the pool fills; only the part above zero is
    # connections opened beyond `pool_size`.
    DB_POOL_CONNECTIONS.labels(state="overflow").set_function(lambda: max(pool.overflow(), 0))


async def check_connection(engine: AsyncEngine) -> str | None:
    """Readiness check for `/ready`.

    Returns None when the database is usable, or a short reason when it is not. It does
    not raise: a readiness probe that raises tells the caller nothing useful, and the
    reason string is what appears in the probe's response.
    """
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except SQLAlchemyError as error:
        log.warning("database.unreachable", error=str(error))
        return type(error).__name__
    return None


class Database:
    """Owns the engine and session factory for the lifetime of a process.

    Held on ``app.state`` by the API and constructed directly by workers. The lifecycle is
    explicit — :meth:`connect` at startup, :meth:`disconnect` at shutdown — because an
    engine that is garbage collected while connections are checked out produces warnings
    that are very hard to trace back to their cause.
    """

    def __init__(self, settings: DatabaseSettings, *, echo: bool = False) -> None:
        self._settings = settings
        self._echo = echo
        self._engine: AsyncEngine | None = None
        self._session_factory: async_sessionmaker[AsyncSession] | None = None

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError("Database.connect() has not been called")
        return self._engine

    @property
    def session_factory(self) -> async_sessionmaker[AsyncSession]:
        if self._session_factory is None:
            raise RuntimeError("Database.connect() has not been called")
        return self._session_factory

    async def connect(self) -> None:
        """Create the engine. Does not open a connection — the pool is lazy."""
        if self._engine is not None:
            return
        self._engine = create_engine(self._settings, echo=self._echo)
        self._session_factory = create_session_factory(self._engine)
        _export_pool_gauges(self._engine)
        log.info("database.engine_created", pool_size=self._settings.database_pool_size)

    async def disconnect(self) -> None:
        """Dispose the engine and close every pooled connection."""
        if self._engine is None:
            return
        await self._engine.dispose()
        self._engine = None
        self._session_factory = None
        log.info("database.engine_disposed")

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        """Open a transactional session. Use as ``async with database.session() as s:``."""
        return session_scope(self.session_factory)

    async def check(self) -> str | None:
        """Readiness check bound to this instance."""
        if self._engine is None:
            return "not connected"
        return await check_connection(self._engine)
