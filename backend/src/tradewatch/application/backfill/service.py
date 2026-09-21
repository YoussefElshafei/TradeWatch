"""Historical backfill: fetch bars for a date range and store them.

The job that gives Phase 6 something to backtest against. Without it the database holds
whatever happened to stream past while a worker was running, which is not a history.

**Idempotent, because it has to be re-runnable.** Bars are written through the same
revision-guarded upsert as everything else, keyed on
``(instrument_id, interval, bar_start)``. Running the same range twice writes the same rows
twice and the table is identical afterwards. That is what makes "just run it again over a
generous window" the correct way to recover from a provider outage, rather than a way to
double-count volume.

**Resumable from the data, not from a job table.** A job table would be a second statement
about what has been fetched, and the interesting question would immediately become which of
the two is right when they disagree — usually after a crash, which is exactly when the answer
matters. Deriving it from the rows means there is nothing to disagree with.

Resuming after ``max(bar_start)`` looks like the obvious rule and is wrong. A database that
already holds a few hours of *recent* bars — written by the aggregator, or by an earlier
backfill of a different range — would make a request for the previous month resume from
today, fetch almost nothing, and report success while leaving the month empty. That is
exactly what this job did the first time it was run for real.

So the rule is: **resume only when the stored data looks like a prefix somebody was filling
forward.** If the oldest bar inside the range is at the beginning of it, the newest bar is a
genuine high-water mark and fetching continues from there. If the oldest bar sits somewhere
in the middle, the data is a patch rather than a prefix and the range is fetched from its
start. "At the beginning" allows one chunk of slack, because a range that opens on a weekend
legitimately has no bars until Monday.

Being wrong in the cautious direction costs a re-fetch, and re-fetching is idempotent. Being
wrong in the other direction costs a month of history and says nothing.

**Chunked, because a month is not one request.** The range is walked in windows, each
fetched and written before the next is requested. A crash costs the current window, never
the whole job, and memory is bounded by a window rather than by the range.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from tradewatch.application.ports.bar_store import BarStore
from tradewatch.application.ports.market_data import MarketDataProvider
from tradewatch.domain.entities import NewBar
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import BarEvent
from tradewatch.observability.logging import get_logger

__all__ = ["BackfillResult", "BackfillService", "SymbolResult", "default_chunk_for"]

log = get_logger(__name__)

#: How much of a range to fetch at once, per interval. Sized so one window is a few thousand
#: bars: comfortably inside a provider's page limits and small enough that losing one to a
#: crash costs seconds rather than the job.
_CHUNKS: dict[BarInterval, dt.timedelta] = {
    BarInterval.ONE_MINUTE: dt.timedelta(days=1),
    BarInterval.FIVE_MINUTES: dt.timedelta(days=5),
    BarInterval.ONE_HOUR: dt.timedelta(days=30),
    BarInterval.ONE_DAY: dt.timedelta(days=365),
}


def default_chunk_for(interval: BarInterval) -> dt.timedelta:
    return _CHUNKS.get(interval, dt.timedelta(days=1))


@dataclass(slots=True)
class SymbolResult:
    """What the backfill did for one symbol."""

    symbol: str
    requested_start: dt.datetime
    requested_end: dt.datetime
    #: Where fetching actually began. Later than `requested_start` on a resumed run.
    effective_start: dt.datetime
    fetched: int = 0
    #: Rows the database accepted.
    #:
    #: **Not a count of new data.** Bars are written with an upsert guarded on ``revision``,
    #: so re-fetching a range the database already holds reports the same number as the
    #: first run did — each row is rewritten with identical values. The table is unchanged;
    #: this number simply does not measure that.
    #:
    #: A "duplicates" figure was tried here and removed: for trades it would be real, since
    #: those are `ON CONFLICT DO NOTHING`, but for bars it would always have read zero and
    #: quietly implied that every re-run had fetched fresh history.
    written: int = 0
    chunks: int = 0
    skipped_existing: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "fetched": self.fetched,
            "written": self.written,
            "chunks": self.chunks,
            "effective_start": self.effective_start.isoformat(),
            "skipped_existing": self.skipped_existing,
        }


@dataclass(slots=True)
class BackfillResult:
    """The whole job."""

    interval: BarInterval
    symbols: list[SymbolResult] = field(default_factory=list)

    @property
    def fetched(self) -> int:
        return sum(item.fetched for item in self.symbols)

    @property
    def written(self) -> int:
        return sum(item.written for item in self.symbols)

    def as_dict(self) -> dict[str, Any]:
        return {
            "interval": self.interval.value,
            "symbols": len(self.symbols),
            "fetched": self.fetched,
            "written": self.written,
        }


class BackfillService:
    """Fetches historical bars from a provider and stores them."""

    def __init__(
        self,
        provider: MarketDataProvider,
        store: BarStore,
        *,
        chunk: dt.timedelta | None = None,
        clock: Callable[[], dt.datetime] | None = None,
    ) -> None:
        self._provider = provider
        self._store = store
        self._chunk = chunk
        self._clock = clock or (lambda: dt.datetime.now(dt.UTC))

    async def run(
        self,
        symbols: Sequence[str],
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
        *,
        resume: bool = True,
    ) -> BackfillResult:
        """Backfill ``[start, end)`` for each symbol.

        Args:
            resume: Begin after the newest bar already stored in the range. With this off
                the whole range is re-fetched, which is the right thing when the provider
                has *corrected* history — the upsert overwrites, so it repairs rather than
                duplicates.

        Raises:
            ValueError: If the range is empty or the provider cannot serve the interval.
            ProviderError: If fetching fails after the provider's own retries.
            StorageError: If writing fails.
        """
        if end <= start:
            raise ValueError(f"end {end.isoformat()} is not after start {start.isoformat()}")
        if not self._provider.capabilities.historical_bars:
            raise ValueError(f"{self._provider.name} cannot serve historical bars")
        if not self._provider.capabilities.supports_interval(interval):
            raise ValueError(f"{self._provider.name} does not serve {interval.value} bars")

        result = BackfillResult(interval=interval)
        for symbol in symbols:
            result.symbols.append(
                await self._one_symbol(symbol, interval, start, end, resume=resume)
            )

        log.info("backfill.complete", **result.as_dict())
        return result

    async def _resume_point(
        self,
        instrument_id: Any,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
        chunk: dt.timedelta,
    ) -> dt.datetime:
        """Where fetching should begin. See the module docstring for the rule."""
        latest = await self._store.latest_bar_start(instrument_id, interval, within=(start, end))
        if latest is None:
            return start

        earliest = await self._store.earliest_bar_start(
            instrument_id, interval, within=(start, end)
        )
        if earliest is None or earliest - start > chunk:
            # The stored bars are a patch somewhere inside the range, not a prefix being
            # filled forward. Resuming from the newest of them would skip everything before.
            log.info(
                "backfill.not_resuming",
                interval=interval.value,
                requested_start=start.isoformat(),
                earliest_stored=earliest.isoformat() if earliest else None,
                detail="stored bars do not begin at the start of the range; fetching it all",
            )
            return start

        return max(start, latest + interval.duration)

    async def _one_symbol(
        self,
        symbol: str,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
        *,
        resume: bool,
    ) -> SymbolResult:
        instrument_id = await self._store.resolve_instrument(symbol)
        chunk = self._chunk or default_chunk_for(interval)
        effective_start = start

        if resume:
            effective_start = await self._resume_point(instrument_id, interval, start, end, chunk)

        outcome = SymbolResult(
            symbol=symbol,
            requested_start=start,
            requested_end=end,
            effective_start=effective_start,
        )

        if effective_start >= end:
            outcome.skipped_existing = True
            log.info(
                "backfill.already_complete",
                symbol=symbol,
                interval=interval.value,
                detail="every bar in the requested range is already stored",
            )
            return outcome

        window_start = effective_start

        while window_start < end:
            window_end = min(window_start + chunk, end)
            bars = await self._provider.fetch_bars(symbol, interval, window_start, window_end)
            outcome.chunks += 1
            outcome.fetched += len(bars)

            if bars:
                rows = [_to_row(bar, instrument_id) for bar in bars]
                outcome.written += await self._store.save_bars(rows)

            log.info(
                "backfill.window",
                symbol=symbol,
                interval=interval.value,
                start=window_start.isoformat(),
                end=window_end.isoformat(),
                fetched=len(bars),
                total_written=outcome.written,
            )
            window_start = window_end

        log.info("backfill.symbol_complete", **outcome.as_dict())
        return outcome


def _to_row(bar: BarEvent, instrument_id: Any) -> NewBar:
    """A provider's bar event as a row.

    ``is_final=True``: a historical bar is finished by definition. The aggregator is the
    only thing that writes a bar still in progress, and it is the only thing that should.
    """
    return NewBar(
        instrument_id=instrument_id,
        interval=bar.interval,
        bar_start=bar.bar_start,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        vwap=bar.vwap,
        trade_count=bar.trade_count,
        is_final=True,
        revision=bar.revision,
    )
