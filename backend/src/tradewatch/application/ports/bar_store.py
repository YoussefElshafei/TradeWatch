"""The bar storage port.

Phase 3 adds three things that need durable bars: the aggregator that builds them from
trades, the backfill that fetches them from a provider, and the quality checks that look for
holes in them. All three live in `application`, so none of them may import a repository.

**Why this is not more methods on `MarketDataStore`.** That port belongs to the market data
processor, whose job is "write what arrives, idempotently, and tell me how many landed". It
has no concept of reading anything back, and it should not grow one: the processor would
then depend on an interface twice the size of what it uses, and every fake in its test suite
would have to implement methods it never calls. A second port with its own reason to exist
is cheaper than a shared one with two.

One adapter — :class:`~tradewatch.infrastructure.database.bar_store.SqlBarStore` — implements
this, and the overlap with `MarketDataStore` is two method signatures, not a design.

Implementations raise :class:`~tradewatch.application.processing.errors.StorageError` and
:class:`~tradewatch.application.processing.errors.UnprocessableEventError`, and nothing else.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Mapping, Sequence
from typing import Protocol, runtime_checkable

from tradewatch.domain.entities import Bar, NewBar
from tradewatch.domain.enums import BarInterval

__all__ = ["BarStore"]


@runtime_checkable
class BarStore(Protocol):
    """Durable storage for OHLCV bars, read and written."""

    async def resolve_instrument(self, symbol: str) -> uuid.UUID:
        """Return the instrument id for ``symbol``, registering it if unknown.

        Registering on first sight rather than failing: a symbol appearing mid-stream is
        normal, and dropping its data until somebody runs a sync script is a silent gap
        that looks exactly like a quiet market.
        """
        ...

    async def save_bars(self, bars: Sequence[NewBar]) -> int:
        """Persist bars idempotently. Returns rows written.

        One transaction. The upsert is guarded on ``revision``, so a correction replaces
        the bar it corrects and a stale revision never overwrites a newer one.
        """
        ...

    async def save_bars_with_progress(
        self,
        bars: Sequence[NewBar],
        *,
        consumer_group: str,
        positions: Mapping[str, int],
    ) -> int:
        """Persist bars **and** the consumer's position, in one transaction.

        The atomicity is the entire point. Bars and progress written separately can disagree
        after a crash between them, and the two ways they can disagree are "a bar counted an
        event the progress says was never applied" (double counting on redelivery) and "the
        progress claims an event that never reached a bar" (a hole). One transaction makes
        both impossible.

        Raises:
            StorageError: If the write could not be completed.
        """
        ...

    async def load_progress(self, consumer_group: str) -> Mapping[str, int]:
        """How far ``consumer_group`` has applied each stream. Empty when it has never run."""
        ...

    async def load_bar(
        self, instrument_id: uuid.UUID, interval: BarInterval, bar_start: dt.datetime
    ) -> Bar | None:
        """One bar by its natural key, or None.

        The aggregator needs this for a trade that belongs to a bar it has already closed
        and evicted — after a restart, or after a long quiet period. Without it, the only
        options are to drop the trade or to write a fresh bar containing that trade alone,
        and the second one silently destroys a correct bar.
        """
        ...

    async def load_open_bars(
        self, interval: BarInterval, *, symbols: Sequence[str] | None = None
    ) -> Sequence[tuple[str, Bar]]:
        """Bars still marked ``is_final=False``, each with its symbol.

        The symbol comes back alongside the row because a :class:`~tradewatch.domain.entities.Bar`
        is keyed by instrument id and the aggregator works in symbols. Resolving the id back
        to a symbol is a join the store is already doing; making the caller do it would mean
        a second query per bar to recover something the first one had in hand.

        This is how the aggregator survives a restart (failure F13). A bar in progress is
        written on every flush with ``is_final=False``; on startup they are read back and
        accumulation continues. Without it, a restart mid-minute either loses the trades
        already folded into the open bar or writes a truncated bar over them.
        """
        ...

    async def latest_bar_start(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        *,
        within: tuple[dt.datetime, dt.datetime] | None = None,
    ) -> dt.datetime | None:
        """The newest stored ``bar_start``, optionally inside ``[start, end)``.

        The backfill's resume point. Deriving it from the data rather than from a job table
        means there is one source of truth about what has been fetched, and it cannot
        disagree with the rows themselves.
        """
        ...

    async def earliest_bar_start(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        *,
        within: tuple[dt.datetime, dt.datetime] | None = None,
    ) -> dt.datetime | None:
        """The oldest stored ``bar_start``, optionally inside ``[start, end)``.

        Paired with :meth:`latest_bar_start` so the backfill can tell *where* in a range the
        stored data sits. The two together answer "does this look like a prefix somebody was
        filling forward, or a patch of data somewhere else entirely?" — which is the
        difference between resuming and silently skipping a month.
        """
        ...

    async def count_bars(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> int:
        """How many bars are stored in ``[start, end)``."""
        ...

    async def find_gaps(
        self,
        instrument_id: uuid.UUID,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> Sequence[tuple[dt.datetime, dt.datetime]]:
        """Holes in a bar series, as ``(gap_start, gap_end)`` pairs.

        A gap is not necessarily missing data — the market closes — so the caller applies
        session hours before concluding anything is wrong.
        """
        ...

    async def roll_up(
        self,
        instrument_id: uuid.UUID,
        source: BarInterval,
        target: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> int:
        """Derive coarser bars from finer ones and store them. Returns rows written.

        `ARCHITECTURE.md` §3.3: higher intervals come from 1m bars, never from a second
        pass over trades. Two independent paths to the same number is two chances to
        disagree, and the disagreement would surface as a backtest that cannot be
        reproduced.
        """
        ...

    async def health_check(self) -> str | None:
        """None when the store is usable, or a short reason when it is not."""
        ...
