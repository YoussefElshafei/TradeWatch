"""PostgreSQL implementation of the market data store port.

Thin on purpose. The repositories already know how to write idempotently — `ON CONFLICT
(event_id) DO NOTHING` for trades and quotes, a revision-guarded upsert for bars — so this
adapter's job is to own the **transaction boundary** and to translate SQLAlchemy's
exceptions into the two the processor understands.

One `save_*` call is one transaction. That is what makes a failed batch leave nothing
half-written, which matters more here than anywhere else in the system: a partially written
batch of trades is a corrupted tape, and every number computed from it afterwards is wrong
in a way nobody can see.

The exception translation is the other half of the port's value. A `DBAPIError` reaching the
processor would put its retry logic in the position of having to understand SQLAlchemy —
which error codes mean "try again" and which mean "this will never work". Deciding that here
means the processor asks one question: `retryable`.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable, Sequence
from decimal import Decimal
from typing import Final

from sqlalchemy.exc import DBAPIError, IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from tradewatch.application.processing.errors import StorageError, UnprocessableEventError
from tradewatch.domain.entities import NewBar, NewQuote, NewTrade
from tradewatch.domain.enums import AssetClass
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import Instrument
from tradewatch.infrastructure.database.session import session_scope
from tradewatch.infrastructure.repositories import (
    BarRepository,
    InstrumentRepository,
    QuoteRepository,
    TradeRepository,
)
from tradewatch.observability.logging import get_logger

__all__ = ["SqlMarketDataStore"]

log = get_logger(__name__)

#: Failures the driver raises *before* SQLAlchemy can wrap them: a refused connection, a
#: connect timeout, a host that does not resolve. They never reach `SQLAlchemyError`, so
#: without naming them here a database outage would escape this adapter unclassified — and
#: the processor's retry loop, which asks only "is this retryable?", would not retry the
#: most retryable failure there is.
_DRIVER_ERRORS: Final[tuple[type[Exception], ...]] = (TimeoutError, OSError)

#: Reference details for a symbol seen on the wire before anyone registered it. Deliberately
#: minimal and obviously provisional: a later instrument sync fills in the real values, and
#: guessing an exchange here would be worse than admitting we do not know it.
_PROVISIONAL_EXCHANGE = "UNKNOWN"
_PROVISIONAL_TICK_SIZE = Decimal("0.01")
_PROVISIONAL_LOT_SIZE = Decimal("1")


class SqlMarketDataStore:
    """Writes market data to PostgreSQL.

    Implements :class:`~tradewatch.application.ports.market_store.MarketDataStore`.

    Takes a session *factory*, not a session: each call opens and closes its own
    transaction, which is what lets one batch fail without poisoning the next.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def resolve_instrument(self, symbol: str) -> uuid.UUID:
        """Find the instrument, registering a provisional row if it is unknown.

        Registering on first sight is deliberate. A symbol appearing mid-stream is normal —
        a provider adds a listing, a watchlist grows — and refusing its data until somebody
        runs a sync script would produce a silent gap that looks exactly like a quiet
        market.
        """
        wanted = symbol.strip().upper()
        try:
            async with session_scope(self._session_factory) as session:
                repository = InstrumentRepository(session)
                existing = await repository.get_by_symbol(wanted)
                if existing is not None:
                    return existing.id

                created = await repository.upsert(
                    Instrument(
                        id=new_id(),
                        symbol=wanted,
                        asset_class=AssetClass.EQUITY,
                        exchange=_PROVISIONAL_EXCHANGE,
                        currency="USD",
                        tick_size=_PROVISIONAL_TICK_SIZE,
                        lot_size=_PROVISIONAL_LOT_SIZE,
                        provider_metadata={"provisional": True},
                    )
                )
                log.info("store.instrument_registered", symbol=wanted, provisional=True)
                return created.id
        except SQLAlchemyError as error:
            raise _translate(error, f"resolving instrument {wanted}") from error
        except _DRIVER_ERRORS as error:
            raise _connection_failure(error, f"resolving instrument {wanted}") from error

    async def save_trades(self, trades: Sequence[NewTrade]) -> int:
        return await self._save("trades", trades, self._write_trades)

    async def save_quotes(self, quotes: Sequence[NewQuote]) -> int:
        return await self._save("quotes", quotes, self._write_quotes)

    async def save_bars(self, bars: Sequence[NewBar]) -> int:
        return await self._save("bars", bars, self._write_bars)

    async def health_check(self) -> str | None:
        try:
            async with session_scope(self._session_factory) as session:
                await InstrumentRepository(session).count()
        except SQLAlchemyError as error:
            return type(error).__name__
        except _DRIVER_ERRORS as error:
            return type(error).__name__
        return None

    # --- internals ---------------------------------------------------------

    async def _save[RowT](
        self,
        what: str,
        rows: Sequence[RowT],
        writer: Callable[[AsyncSession, Sequence[RowT]], Awaitable[int]],
    ) -> int:
        """Run one write inside one transaction, translating whatever comes back."""
        if not rows:
            return 0
        context = f"writing {len(rows)} {what}"
        try:
            async with session_scope(self._session_factory) as session:
                return await writer(session, rows)
        except SQLAlchemyError as error:
            raise _translate(error, context) from error
        except _DRIVER_ERRORS as error:
            raise _connection_failure(error, context) from error

    @staticmethod
    async def _write_trades(session: AsyncSession, rows: Sequence[NewTrade]) -> int:
        return await TradeRepository(session).bulk_insert(rows)

    @staticmethod
    async def _write_quotes(session: AsyncSession, rows: Sequence[NewQuote]) -> int:
        return await QuoteRepository(session).bulk_insert(rows)

    @staticmethod
    async def _write_bars(session: AsyncSession, rows: Sequence[NewBar]) -> int:
        return await BarRepository(session).upsert_many(rows)


def _translate(error: SQLAlchemyError, context: str) -> StorageError | UnprocessableEventError:
    """Decide whether the caller should retry, or give up on this batch.

    A constraint violation is the data's fault and will fail identically forever, so it is
    unprocessable. Everything else — a dropped connection, a deadlock, a timeout — is the
    database's current state and is worth trying again.

    The boundary is not perfect: a foreign key violation caused by a row that has not
    committed *yet* would be classified as permanent. That case does not arise here, because
    instruments are committed before the batch that references them.
    """
    if isinstance(error, IntegrityError):
        return UnprocessableEventError(
            f"{context} violated a database constraint",
            detail=_safe_detail(error),
        )
    if isinstance(error, DBAPIError) and not error.connection_invalidated:
        # A statement-level error on a healthy connection: bad data, or a bad statement.
        # Retrying it produces the same failure.
        return UnprocessableEventError(f"{context} failed", detail=_safe_detail(error))
    return StorageError(f"{context} failed: {type(error).__name__}", detail=_safe_detail(error))


def _connection_failure(error: BaseException, context: str) -> StorageError:
    """A driver-level failure is always the database's current state, never the data's."""
    return StorageError(f"{context} failed: {type(error).__name__}", detail=str(error)[:200])


def _safe_detail(error: SQLAlchemyError) -> str:
    """The first line of the database's complaint.

    Truncated and first-line-only because a SQLAlchemy error carries the full statement and
    its bound parameters, and those contain market data that has no business being repeated
    at error level in a log aggregator.
    """
    text = str(getattr(error, "orig", error))
    return text.splitlines()[0][:200] if text else type(error).__name__
