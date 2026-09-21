"""The market data persistence port.

The processor lives in `application`, so it cannot import a repository — repositories are
adapters, and adapters live in `infrastructure`. This is the interface it depends on
instead, implemented by
:class:`~tradewatch.infrastructure.database.market_store.SqlMarketDataStore`.

The separation buys two things. The processor's batching, deduplication accounting and
failure handling are unit-testable against an in-memory store with no database in sight. And
the transaction boundary lives on *this* side of the interface — one `save_*` call is one
transaction, all-or-nothing — so the processor does not have to know what a session is in
order to guarantee that a failed batch leaves nothing half-written.

Every method returns **how many rows were actually written**, which is always less than or
equal to what was offered. The difference is the duplicate count, and reporting it is what
lets the processor distinguish "nothing to do" from "silently lost".
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from tradewatch.domain.entities import NewBar, NewQuote, NewTrade

__all__ = ["InstrumentRef", "MarketDataStore"]


class InstrumentRef(Protocol):
    """The minimum the processor needs to know about an instrument: its id and symbol."""

    @property
    def id(self) -> uuid.UUID: ...

    @property
    def symbol(self) -> str: ...


@runtime_checkable
class MarketDataStore(Protocol):
    """Durable storage for normalized market data.

    Implementations raise :class:`~tradewatch.application.processing.errors.StorageError`
    and nothing else. A SQLAlchemy exception crossing this boundary would put the
    processor's retry logic in the position of having to understand SQLAlchemy, which is
    exactly what the port exists to prevent.
    """

    async def resolve_instrument(self, symbol: str) -> uuid.UUID:
        """Return the instrument id for ``symbol``, registering it if it is unknown.

        Registering on first sight rather than failing is deliberate: a symbol appearing
        mid-stream is normal — a provider adds a listing, a watchlist grows — and dropping
        its data until somebody runs a sync script would be a silent gap. The row created
        this way is minimal; a later instrument sync fills in the reference details.

        Raises:
            StorageError: If the instrument could not be resolved or created.
        """
        ...

    async def save_trades(self, trades: Sequence[NewTrade]) -> int:
        """Persist trades idempotently. Returns the number of rows written.

        One transaction. A batch either lands completely or not at all — a half-written
        batch of trades is a corrupted tape, which is worse than no batch.

        Raises:
            StorageError: If the batch could not be written.
        """
        ...

    async def save_quotes(self, quotes: Sequence[NewQuote]) -> int:
        """Persist quotes idempotently. Returns the number of rows written."""
        ...

    async def save_bars(self, bars: Sequence[NewBar]) -> int:
        """Persist bars, replacing any existing bar with the same key and an older revision.

        Upsert rather than insert-or-ignore, because a bar legitimately changes: a late
        trade produces a correction (`ARCHITECTURE.md` F11).
        """
        ...

    async def health_check(self) -> str | None:
        """None when the store is usable, or a short reason when it is not."""
        ...
