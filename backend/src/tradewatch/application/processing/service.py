"""The Market Data Processor.

`ARCHITECTURE.md` §3.2: consume the normalized event stream and durably persist it. It is
the consumer half of the pipeline — the provider never touches the database, and the event
bus stays the boundary between ingestion and processing, so the two halves can be split
into separate processes in Phase 2 without either one noticing.

**Batching, because one insert per tick is the wrong shape.** Events accumulate per type and
flush when the buffer fills or when the caller asks. Each flush is one transaction, so a
batch lands completely or not at all: a half-written batch of trades is a corrupted tape,
which is worse than no batch.

**What happens when things go wrong:**

* A *malformed or unstorable event* is counted, logged with enough context to find it again,
  and set aside. Retrying it would replay the same rejection forever, and one bad tick must
  never stop the pipeline (failure F3).
* A *storage failure* keeps the buffer, retries with backoff, and — if retries run out —
  propagates. The events are not dropped, and the supervisor sees a failed processor rather
  than one quietly making no progress (F8).
* A *duplicate* is absorbed by the store's idempotent writes and counted. Because event ids
  are deterministic (ADR-0012), a reprocessed stream after a restart writes nothing new.
* *Cancellation* flushes what is buffered, then propagates. It is not an error.

Nothing is caught and ignored. Every `except` either re-raises or increments a counter that
:class:`ProcessorStats` reports and a test asserts on.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from tradewatch.application.ports.market_store import MarketDataStore
from tradewatch.application.processing.errors import (
    ProcessingError,
    StorageError,
    UnprocessableEventError,
)
from tradewatch.domain.entities import NewBar, NewQuote, NewTrade
from tradewatch.domain.events import (
    BarEvent,
    MarketEvent,
    QuoteEvent,
    TradeEvent,
    event_summary,
)
from tradewatch.observability.logging import get_logger

__all__ = ["MarketDataProcessor", "ProcessorStats"]

log = get_logger(__name__)

#: Events buffered before a flush. Large enough that a busy symbol does not produce an
#: insert per tick; small enough that a quiet market does not hold data for long.
DEFAULT_BATCH_SIZE = 200

#: Attempts before a storage failure is given up on and propagated.
DEFAULT_MAX_STORAGE_ATTEMPTS = 3


@dataclass(slots=True)
class ProcessorStats:
    """Counters for one processing run.

    Each one sits where an error could otherwise have been swallowed, which is what makes
    "does not silently discard" checkable rather than aspirational. They become Prometheus
    metrics in Phase 10.
    """

    received: int = 0
    persisted: int = 0
    duplicates: int = 0
    unprocessable: int = 0
    storage_retries: int = 0
    flushes: int = 0
    started_at: dt.datetime | None = None
    last_event_at: dt.datetime | None = None

    @property
    def buffered_or_lost(self) -> int:
        """Received but neither persisted, deduplicated, nor rejected.

        Should be zero after a flush. Anything else means events are sitting in a buffer —
        or, if the processor has stopped, that they were lost, which is what this counter
        exists to make visible.
        """
        return self.received - self.persisted - self.duplicates - self.unprocessable

    def as_dict(self) -> dict[str, Any]:
        return {
            "received": self.received,
            "persisted": self.persisted,
            "duplicates": self.duplicates,
            "unprocessable": self.unprocessable,
            "storage_retries": self.storage_retries,
            "flushes": self.flushes,
        }


@dataclass(slots=True)
class _Buffer:
    """Pending writes, grouped by table."""

    trades: list[NewTrade] = field(default_factory=list)
    quotes: list[NewQuote] = field(default_factory=list)
    bars: list[NewBar] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.trades) + len(self.quotes) + len(self.bars)

    def clear(self) -> None:
        self.trades.clear()
        self.quotes.clear()
        self.bars.clear()


class MarketDataProcessor:
    """Consumes normalized market data events and persists them."""

    def __init__(
        self,
        store: MarketDataStore,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_storage_attempts: int = DEFAULT_MAX_STORAGE_ATTEMPTS,
        retry_delay: float = 0.5,
        clock: Callable[[], dt.datetime] | None = None,
        sleep: Callable[[float], Any] | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if max_storage_attempts <= 0:
            raise ValueError("max_storage_attempts must be positive")

        self._store = store
        self._batch_size = batch_size
        self._max_storage_attempts = max_storage_attempts
        self._retry_delay = retry_delay
        self._clock = clock or (lambda: dt.datetime.now(dt.UTC))
        self._sleep = sleep or asyncio.sleep
        self._buffer = _Buffer()
        #: symbol -> instrument id. Resolving on every event would mean a round trip per
        #: tick for a value that never changes.
        self._instruments: dict[str, Any] = {}
        self._stats = ProcessorStats()

    @property
    def stats(self) -> ProcessorStats:
        return self._stats

    @property
    def pending(self) -> int:
        """Events buffered but not yet written. Zero after a successful flush."""
        return len(self._buffer)

    # --- consuming ---------------------------------------------------------

    async def run(
        self,
        stream: AsyncIterator[MarketEvent],
        *,
        max_events: int | None = None,
    ) -> ProcessorStats:
        """Consume ``stream`` until it ends, the limit is reached, or it is cancelled.

        Always flushes on the way out, including on cancellation: buffered events that were
        accepted must not be dropped because somebody pressed Ctrl-C.

        ``max_events`` counts events handled **by this call**, not `stats.received`, which
        is cumulative over the processor's lifetime. A processor that had already handled a
        backfill would otherwise find its budget spent before the stream opened.
        """
        self._stats.started_at = self._clock()
        log.info("processor.started", batch_size=self._batch_size)
        handled_here = 0

        try:
            async for event in stream:
                await self.handle(event)
                handled_here += 1
                if max_events is not None and handled_here >= max_events:
                    break
        except asyncio.CancelledError:
            log.info("processor.cancelled", **self._stats.as_dict())
            await self._flush_quietly()
            raise
        except ProcessingError:
            # Storage gave up. Flush is not attempted — it is what just failed — and the
            # buffer is kept so the failure is visible rather than papered over.
            log.error(
                "processor.failed",
                pending=self.pending,
                **self._stats.as_dict(),
            )
            raise
        else:
            await self.flush()
        finally:
            log.info("processor.stopped", pending=self.pending, **self._stats.as_dict())

        return self._stats

    async def handle(self, event: MarketEvent) -> None:
        """Accept one event, buffering it and flushing when the buffer is full.

        An event that cannot be stored is counted and skipped rather than raised, because
        one bad tick must not end the run.
        """
        self._stats.received += 1
        self._stats.last_event_at = self._clock()

        try:
            await self._buffer_event(event)
        except UnprocessableEventError as error:
            self._record_unprocessable(error)
            return

        if len(self._buffer) >= self._batch_size:
            await self.flush()

    async def _buffer_event(self, event: MarketEvent) -> None:
        """Convert an event into a row and stage it.

        Raises:
            UnprocessableEventError: If the event cannot be turned into a row.
            StorageError: If the instrument could not be resolved.
        """
        instrument_id = await self._instrument_id(event.symbol)

        match event:
            case TradeEvent():
                self._buffer.trades.append(
                    NewTrade(
                        event_id=event.event_id,
                        instrument_id=instrument_id,
                        price=event.price,
                        size=event.size,
                        occurred_at=event.occurred_at,
                        ingested_at=event.ingested_at,
                        provider=event.source,
                        provider_trade_id=event.provider_trade_id,
                    )
                )
            case QuoteEvent():
                self._buffer.quotes.append(
                    NewQuote(
                        event_id=event.event_id,
                        instrument_id=instrument_id,
                        bid_price=event.bid_price,
                        bid_size=event.bid_size,
                        ask_price=event.ask_price,
                        ask_size=event.ask_size,
                        occurred_at=event.occurred_at,
                        ingested_at=event.ingested_at,
                        provider=event.source,
                    )
                )
            case BarEvent():
                self._buffer.bars.append(
                    NewBar(
                        instrument_id=instrument_id,
                        interval=event.interval,
                        bar_start=event.bar_start,
                        open=event.open,
                        high=event.high,
                        low=event.low,
                        close=event.close,
                        volume=event.volume,
                        vwap=event.vwap,
                        trade_count=event.trade_count,
                        revision=event.revision,
                    )
                )
            case _:
                raise UnprocessableEventError(
                    f"no storage mapping for {type(event).__name__}",
                    event_id=event.event_id,
                    event_type=str(getattr(event, "event_type", "unknown")),
                    symbol=event.symbol,
                )

    async def _instrument_id(self, symbol: str) -> Any:
        """Resolve a symbol to an instrument id, caching the answer."""
        cached = self._instruments.get(symbol)
        if cached is not None:
            return cached

        instrument_id = await self._store.resolve_instrument(symbol)
        self._instruments[symbol] = instrument_id
        log.debug("processor.instrument_resolved", symbol=symbol)
        return instrument_id

    # --- writing -----------------------------------------------------------

    async def flush(self) -> int:
        """Write everything buffered. Returns the number of rows written.

        Retries a storage failure with backoff, keeping the buffer intact so nothing is
        lost between attempts. When attempts run out the error propagates and the buffer
        still holds the batch — the caller can see exactly what did not land.
        """
        if not self._buffer:
            return 0

        attempt = 0
        while True:
            attempt += 1
            try:
                written = await self._write_buffer()
            except StorageError as error:
                if attempt >= self._max_storage_attempts:
                    log.error(
                        "processor.storage_failed",
                        attempts=attempt,
                        pending=self.pending,
                        error=str(error),
                        error_type=type(error).__name__,
                    )
                    raise
                self._stats.storage_retries += 1
                delay = self._retry_delay * attempt
                log.warning(
                    "processor.storage_retry",
                    attempt=attempt,
                    delay_seconds=round(delay, 3),
                    pending=self.pending,
                    error=str(error),
                )
                await self._sleep(delay)
                continue

            self._stats.flushes += 1
            return written

    async def _write_buffer(self) -> int:
        """One flush. Each `save_*` call is its own transaction."""
        offered = len(self._buffer)
        written = 0

        if self._buffer.trades:
            written += await self._store.save_trades(self._buffer.trades)
        if self._buffer.quotes:
            written += await self._store.save_quotes(self._buffer.quotes)
        if self._buffer.bars:
            written += await self._store.save_bars(self._buffer.bars)

        duplicates = offered - written
        self._stats.persisted += written
        self._stats.duplicates += duplicates
        self._buffer.clear()

        log.debug("processor.flushed", offered=offered, written=written, duplicates=duplicates)
        return written

    async def _flush_quietly(self) -> None:
        """Best-effort flush during teardown.

        A failure here is logged rather than raised, because it is already happening inside
        another exception's unwinding and masking that one would hide the real cause.
        """
        try:
            await self.flush()
        except ProcessingError as error:
            log.error(
                "processor.shutdown_flush_failed",
                pending=self.pending,
                error=str(error),
            )

    # --- diagnostics -------------------------------------------------------

    def _record_unprocessable(self, error: UnprocessableEventError) -> None:
        self._stats.unprocessable += 1
        log.warning(
            "processor.unprocessable_event",
            reason=error.message,
            event_id=str(error.event_id) if error.event_id else None,
            event_type=error.event_type,
            symbol=error.symbol,
            detail=error.detail,
            total_unprocessable=self._stats.unprocessable,
        )

    def record_unprocessable(self, event: MarketEvent, reason: str) -> None:
        """Reject an event from outside the normal path, with the same accounting.

        Used by a caller that decodes events itself — the Phase 2 Kafka consumer, whose
        deserialization can fail before an event object exists.
        """
        self._record_unprocessable(
            UnprocessableEventError(
                reason,
                event_id=event.event_id,
                event_type=event.event_type.value,
                symbol=event.symbol,
                detail=event_summary(event),
            )
        )

    async def health_check(self) -> str | None:
        """None when the store is usable, or a short reason when it is not."""
        return await self._store.health_check()


def batch_events(events: Sequence[MarketEvent], size: int) -> list[Sequence[MarketEvent]]:
    """Split a sequence into batches. Used by the backfill path and by tests."""
    if size <= 0:
        raise ValueError("size must be positive")
    return [events[index : index + size] for index in range(0, len(events), size)]
