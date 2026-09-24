"""Fixtures for tests that need a real PostgreSQL.

`ARCHITECTURE.md` §11.2: *no mocking of the database*. An in-memory SQLite substitute would
test a different database — one with no native enums, no JSONB, no partial indexes, no
`ON CONFLICT`, and different transaction semantics — which is to say it would test none of
what this schema relies on.

**Where the database comes from.** `TW_TEST_DATABASE_URL` if set, otherwise the application
database URL with `_test` appended. If nothing is reachable, every test in this package
skips with an explanation rather than failing — a developer without Docker running should
get a clear message, not a wall of connection errors.

**Isolation.** The schema is built once per session, by running the real Alembic migrations
rather than `metadata.create_all`. That matters: `create_all` would test the models against
themselves and never notice that a migration disagrees with them. Each test then runs inside
a transaction that is rolled back afterwards, so tests share a schema but never share a row,
and they can run in any order.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from tradewatch.domain.entities import NewBar, NewQuote, NewTrade
from tradewatch.domain.enums import AssetClass, UserRole
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import Instrument, Portfolio, Strategy, User

# Read before the root `clean_env` fixture strips every TW_ variable. Import time is the
# only moment this is reliably visible.
_ENV_URL = os.environ.get("TW_TEST_DATABASE_URL") or os.environ.get("TW_DATABASE_URL", "")

BACKEND_ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.integration


def _test_database_url() -> str:
    """The database these tests run against, which is never the application database."""
    if os.environ.get("TW_TEST_DATABASE_URL"):
        return os.environ["TW_TEST_DATABASE_URL"]
    if not _ENV_URL:
        return ""
    base, _, name = _ENV_URL.rpartition("/")
    if name.endswith("_test"):
        return _ENV_URL
    return f"{base}/{name}_test"


DATABASE_URL = _test_database_url()

SKIP_REASON = (
    "No test database. Start one with "
    "`docker compose -f docker/docker-compose.yml up -d postgres`, create the "
    "`tradewatch_test` database, and set TW_TEST_DATABASE_URL (or TW_DATABASE_URL)."
)


def _run_migrations(connection: Connection) -> None:
    """Apply every migration to the connection Alembic is handed.

    Passing the connection through `config.attributes` is what `alembic/env.py` looks for;
    without it Alembic would open its own connection and its own event loop, which cannot
    work from inside a running one.
    """
    config = Config(str(BACKEND_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(BACKEND_ROOT / "alembic"))
    config.attributes["connection"] = connection
    command.upgrade(config, "head")


@pytest_asyncio.fixture(scope="session")
async def engine() -> AsyncIterator[AsyncEngine]:
    """A session-wide engine with the schema built by the real migrations."""
    if not DATABASE_URL:
        pytest.skip(SKIP_REASON)

    engine = create_async_engine(DATABASE_URL, poolclass=None, future=True)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    except Exception as error:
        await engine.dispose()
        pytest.skip(f"{SKIP_REASON}\nConnection error: {type(error).__name__}: {error}")

    # Start from nothing, so a schema left behind by an interrupted run cannot make a
    # broken migration look like it works.
    try:
        async with engine.begin() as connection:
            await connection.execute(text("DROP SCHEMA public CASCADE"))
            await connection.execute(text("CREATE SCHEMA public"))
        async with engine.begin() as connection:
            await connection.run_sync(_run_migrations)
    except Exception:
        # Undisposed here means every test in the session leaks this engine's connections,
        # which is what turned one migration failure into ~200 unrelated ResourceWarning
        # failures (filterwarnings turns them into errors) on top of the real one.
        await engine.dispose()
        raise

    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def session(engine: AsyncEngine) -> AsyncIterator[AsyncSession]:
    """A session inside a transaction that is rolled back when the test ends.

    ``join_transaction_mode="create_savepoint"`` means a ``commit()`` inside the code under
    test releases a savepoint rather than committing for real, so repositories can be
    exercised exactly as they are in production while the outer transaction still rolls
    everything back.
    """
    connection = await engine.connect()
    transaction = await connection.begin()
    session = AsyncSession(
        bind=connection,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    try:
        yield session
    finally:
        await session.close()
        await transaction.rollback()
        await connection.close()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# --- Data builders ---------------------------------------------------------
# Every fixture below builds its own rows. Nothing depends on data another test wrote, and
# nothing resembles production data (ARCHITECTURE.md §11.2).


@pytest.fixture
def instrument_factory() -> Iterator[Any]:
    """Build instruments with unique symbols, so parallel tests cannot collide."""
    counter = {"n": 0}

    def build(**overrides: Any) -> Instrument:
        counter["n"] += 1
        defaults: dict[str, Any] = {
            "id": new_id(),
            "symbol": f"TEST{counter['n']:03d}",
            "name": "Test Instrument",
            "asset_class": AssetClass.EQUITY,
            "exchange": "XTST",
            "currency": "USD",
            "tick_size": Decimal("0.01"),
            "lot_size": Decimal("1"),
        }
        return Instrument(**(defaults | overrides))

    yield build


@pytest.fixture
def user_factory() -> Iterator[Any]:
    counter = {"n": 0}

    def build(**overrides: Any) -> User:
        counter["n"] += 1
        defaults: dict[str, Any] = {
            "id": new_id(),
            "email": f"user{counter['n']}@test.invalid",
            # Not a real hash and not a real password — nothing here is a credential.
            "password_hash": "not-a-real-hash",
            "role": UserRole.TRADER,
        }
        return User(**(defaults | overrides))

    yield build


@pytest_asyncio.fixture
async def instrument(session: AsyncSession, instrument_factory: Any) -> Instrument:
    row: Instrument = instrument_factory()
    session.add(row)
    await session.flush()
    return row


@pytest_asyncio.fixture
async def user(session: AsyncSession, user_factory: Any) -> User:
    row: User = user_factory()
    session.add(row)
    await session.flush()
    return row


@pytest_asyncio.fixture
async def other_user(session: AsyncSession, user_factory: Any) -> User:
    """A second user, for the cross-user access tests that every owner-scoped query needs."""
    row: User = user_factory()
    session.add(row)
    await session.flush()
    return row


@pytest_asyncio.fixture
async def portfolio(session: AsyncSession, user: User) -> Portfolio:
    row = Portfolio(
        id=new_id(),
        user_id=user.id,
        name="Test Portfolio",
        base_currency="USD",
        initial_cash=Decimal("100000"),
        cash_balance=Decimal("100000"),
    )
    session.add(row)
    await session.flush()
    return row


@pytest_asyncio.fixture
async def strategy(session: AsyncSession, user: User) -> Strategy:
    row = Strategy(
        id=new_id(),
        user_id=user.id,
        name="Test Strategy",
        strategy_type="sma_crossover",
        parameters={"fast": 10, "slow": 30},
        symbols=["TEST001"],
    )
    session.add(row)
    await session.flush()
    return row


# --- A store bound to the test's transaction --------------------------------


class SessionBoundStore:
    """The real repositories and the real error translation, on the test's transaction.

    :class:`~tradewatch.infrastructure.database.market_store.SqlMarketDataStore` takes a
    session *factory* and opens a transaction per write. That is right in production and
    useless here: the fixtures never commit, so a separate transaction would not see the
    rows a test just wrote.

    Only the transaction boundary moves. The repositories, the SQL and `_translate` are the
    production ones, which is what keeps these tests meaningful.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def resolve_instrument(self, symbol: str) -> uuid.UUID:
        from tradewatch.infrastructure.repositories import InstrumentRepository

        wanted = symbol.strip().upper()
        repository = InstrumentRepository(self._session)
        existing = await repository.get_by_symbol(wanted)
        if existing is not None:
            return existing.id

        created = await repository.upsert(
            Instrument(
                id=new_id(),
                symbol=wanted,
                asset_class=AssetClass.EQUITY,
                exchange="XTST",
                currency="USD",
                tick_size=Decimal("0.01"),
                lot_size=Decimal("1"),
            )
        )
        return created.id

    async def save_trades(self, trades: Sequence[NewTrade]) -> int:
        from tradewatch.infrastructure.repositories import TradeRepository

        return await self._write(
            "trades", trades, lambda rows: TradeRepository(self._session).bulk_insert(rows)
        )

    async def save_quotes(self, quotes: Sequence[NewQuote]) -> int:
        from tradewatch.infrastructure.repositories import QuoteRepository

        return await self._write(
            "quotes", quotes, lambda rows: QuoteRepository(self._session).bulk_insert(rows)
        )

    async def save_bars(self, bars: Sequence[NewBar]) -> int:
        from tradewatch.infrastructure.repositories import BarRepository

        return await self._write(
            "bars", bars, lambda rows: BarRepository(self._session).upsert_many(rows)
        )

    async def health_check(self) -> str | None:
        return None

    @staticmethod
    async def _write[RowT](
        what: str,
        rows: Sequence[RowT],
        writer: Callable[[Sequence[RowT]], Awaitable[int]],
    ) -> int:
        from sqlalchemy.exc import SQLAlchemyError

        from tradewatch.infrastructure.database.market_store import _translate

        if not rows:
            return 0
        try:
            return await writer(rows)
        except SQLAlchemyError as error:
            raise _translate(error, f"writing {len(rows)} {what}") from error
