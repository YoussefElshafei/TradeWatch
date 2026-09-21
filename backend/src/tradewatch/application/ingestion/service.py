"""The ingestion service.

`ARCHITECTURE.md` §3.1: the only part of the system that knows a provider exists. It pulls
from a :class:`~tradewatch.application.ports.market_data.MarketDataProvider`, deduplicates,
and publishes to an :class:`~tradewatch.application.ports.event_bus.EventPublisher`. It
writes to no database and applies no business rules — aggregation, storage and strategy are
downstream consumers' jobs.

Living in `application` means it depends on ports only, so it is fully testable with a fake
provider and an in-memory bus. `import-linter` enforces that it cannot reach for an adapter.

**The failure posture, which is most of the design:**

* A *malformed message* (F3) is counted, logged with enough context to find it again, and
  skipped. One bad tick must never end a session. From Phase 2 it also goes to the DLQ.
* A *dropped connection* (F1) is handled by
  :func:`~tradewatch.infrastructure.providers.resilience.reconnecting`, which reopens with
  backoff and resets its attempt counter after any successful message.
* A *silent stall* — connection open, no data — is the failure nothing else notices, so
  :class:`~tradewatch.infrastructure.providers.resilience.StalenessMonitor` watches for it.
* A *publish failure* (F4) never blocks trade ingestion. Quotes are shed first, because
  losing a quote costs a tick of book detail while losing a trade corrupts the tape.
* *Cancellation* propagates immediately. It is not an error and must never be retried, or
  shutdown hangs.

Nothing is caught and ignored. Every `except` either re-raises, or increments a counter that
:class:`IngestionStats` reports and a test asserts on.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from typing import Any

from tradewatch.application.ingestion.dedupe import EventDeduplicator
from tradewatch.application.ports.event_bus import EventPublisher
from tradewatch.application.ports.market_data import (
    InstrumentSnapshot,
    MarketDataProvider,
    PriceSnapshot,
)
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import BarEvent, MarketEvent, QuoteEvent, event_summary
from tradewatch.observability.logging import get_logger

__all__ = ["IngestionService", "IngestionStats", "StreamKind"]

log = get_logger(__name__)

StreamKind = str  # "trades" | "quotes"


@dataclass(slots=True)
class IngestionStats:
    """Counters for one ingestion run.

    Every one of these is incremented somewhere an error could otherwise have been
    swallowed. They are what makes "do not silently discard errors" checkable rather than
    aspirational, and they become Prometheus metrics in Phase 10.
    """

    received: int = 0
    published: int = 0
    duplicates: int = 0
    malformed: int = 0
    publish_failures: int = 0
    reconnects: int = 0
    stale_warnings: int = 0
    started_at: dt.datetime | None = None
    last_event_at: dt.datetime | None = None

    @property
    def dropped(self) -> int:
        """Events received but never published, for any reason."""
        return self.duplicates + self.malformed + self.publish_failures

    def as_dict(self) -> dict[str, Any]:
        return {
            "received": self.received,
            "published": self.published,
            "duplicates": self.duplicates,
            "malformed": self.malformed,
            "publish_failures": self.publish_failures,
            "reconnects": self.reconnects,
            "stale_warnings": self.stale_warnings,
        }


class IngestionService:
    """Provider → deduplicate → publish."""

    def __init__(
        self,
        provider: MarketDataProvider,
        publisher: EventPublisher,
        *,
        deduplicator: EventDeduplicator | None = None,
        clock: Callable[[], dt.datetime] | None = None,
        shed_quotes_on_failure: bool = True,
    ) -> None:
        self._provider = provider
        self._publisher = publisher
        self._deduplicator = deduplicator or EventDeduplicator()
        self._clock = clock or (lambda: dt.datetime.now(dt.UTC))
        #: Failure F4's shedding order. Dropping a quote costs a tick of book detail;
        #: dropping a trade corrupts the tape, so trades are never shed.
        self._shed_quotes_on_failure = shed_quotes_on_failure
        self._stats = IngestionStats()

    @property
    def stats(self) -> IngestionStats:
        return self._stats

    @property
    def provider_name(self) -> str:
        return self._provider.name

    # --- one-shot operations ----------------------------------------------

    async def sync_instruments(self) -> Sequence[InstrumentSnapshot]:
        """Fetch the provider's instrument universe.

        Returns snapshots rather than writing them: persistence is the processor's job, and
        keeping this service free of a database is what lets it be tested without one.
        """
        if not self._provider.capabilities.instrument_list:
            log.info("ingestion.instrument_list_unsupported", provider=self._provider.name)
            return []
        instruments = await self._provider.list_instruments()
        log.info(
            "ingestion.instruments_synced",
            provider=self._provider.name,
            count=len(instruments),
        )
        return instruments

    async def latest_price(self, symbol: str) -> PriceSnapshot | None:
        """The current price for one symbol, or None if it has never traded."""
        if not self._provider.capabilities.latest_price:
            raise RuntimeError(f"{self._provider.name} cannot report a latest price")
        return await self._provider.fetch_latest_price(symbol)

    async def backfill_bars(
        self,
        symbol: str,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> int:
        """Fetch historical bars and publish them. Returns how many were published.

        Overlapping a previous backfill is safe and expected: event ids are deterministic
        (ADR-0012), so re-fetched bars deduplicate instead of duplicating. That is what makes
        "fetch a generous window around the gap" the right way to recover from an outage
        rather than a way to corrupt history.
        """
        if not self._provider.capabilities.historical_bars:
            raise RuntimeError(f"{self._provider.name} cannot serve historical bars")

        bars = await self._provider.fetch_bars(symbol, interval, start, end)
        self._stats.received += len(bars)

        fresh: list[BarEvent] = []
        for bar in bars:
            if self._deduplicator.is_duplicate(bar.event_id):
                self._stats.duplicates += 1
                continue
            fresh.append(bar)

        published = await self._publish_batch(fresh)
        log.info(
            "ingestion.backfill_complete",
            provider=self._provider.name,
            symbol=symbol,
            interval=interval.value,
            fetched=len(bars),
            published=published,
            duplicates=len(bars) - len(fresh),
        )
        return published

    # --- streaming ---------------------------------------------------------

    async def run_stream(
        self,
        symbols: Sequence[str],
        *,
        kind: StreamKind = "trades",
        stream: AsyncIterator[MarketEvent] | None = None,
        max_events: int | None = None,
        on_event: Callable[[MarketEvent], None] | None = None,
    ) -> IngestionStats:
        """Consume a live stream until it ends, the limit is reached, or it is cancelled.

        Args:
            symbols: What to subscribe to.
            kind: `"trades"` or `"quotes"`.
            stream: Use this iterator instead of opening one. This is where reconnection is
                injected — the caller wraps the provider's stream in `reconnecting` — and
                where tests inject a scripted sequence.
            max_events: Stop after this many events published **by this call**. Counted
                locally rather than from `stats.published`, which is cumulative across the
                service's lifetime — a service that had already backfilled would otherwise
                find its budget spent before the stream opened.
            on_event: Called for each published event. Used by tests and by the Phase 2
                broadcaster; it must not raise.
        """
        self._stats.started_at = self._clock()
        source = stream if stream is not None else self._open_stream(symbols, kind)
        published_here = 0

        log.info(
            "ingestion.stream_started",
            provider=self._provider.name,
            kind=kind,
            symbols=len(symbols),
        )

        try:
            async for event in source:
                self._stats.received += 1
                self._stats.last_event_at = self._clock()

                if self._deduplicator.is_duplicate(event.event_id):
                    self._stats.duplicates += 1
                    log.debug(
                        "ingestion.duplicate_skipped",
                        **event_summary(event),
                    )
                    continue

                if await self._publish_one(event):
                    published_here += 1
                    if on_event is not None:
                        on_event(event)
                    if max_events is not None and published_here >= max_events:
                        break
        except asyncio.CancelledError:
            # Shutdown, not failure. Never retried, never counted as an error.
            log.info(
                "ingestion.stream_cancelled",
                provider=self._provider.name,
                **self._stats.as_dict(),
            )
            raise
        finally:
            log.info(
                "ingestion.stream_finished",
                provider=self._provider.name,
                kind=kind,
                **self._stats.as_dict(),
            )

        return self._stats

    def _open_stream(self, symbols: Sequence[str], kind: StreamKind) -> AsyncIterator[MarketEvent]:
        capabilities = self._provider.capabilities
        if kind == "trades":
            if not capabilities.streaming_trades:
                raise RuntimeError(f"{self._provider.name} cannot stream trades")
            return self._provider.stream_trades(symbols)
        if kind == "quotes":
            if not capabilities.streaming_quotes:
                raise RuntimeError(f"{self._provider.name} cannot stream quotes")
            return self._provider.stream_quotes(symbols)
        raise ValueError(f"unknown stream kind {kind!r}")

    # --- publishing --------------------------------------------------------

    async def _publish_one(self, event: MarketEvent) -> bool:
        """Publish one event. Returns whether it was accepted.

        A failure here is counted and logged, never raised, because one unroutable event
        must not end the session. The id is forgotten so a later redelivery of the same
        event gets another chance — otherwise the deduplicator would itself become a source
        of data loss.
        """
        try:
            await self._publisher.publish(event)
        except Exception as error:
            self._stats.publish_failures += 1
            self._deduplicator.forget(event.event_id)

            if isinstance(event, QuoteEvent) and self._shed_quotes_on_failure:
                log.warning(
                    "ingestion.quote_shed",
                    error=str(error),
                    error_type=type(error).__name__,
                    **event_summary(event),
                )
                return False

            # A trade is not shed quietly. Losing one corrupts the tape, and the operator
            # needs to know it happened.
            log.error(
                "ingestion.publish_failed",
                error=str(error),
                error_type=type(error).__name__,
                **event_summary(event),
            )
            return False

        self._stats.published += 1
        return True

    async def _publish_batch(self, events: Sequence[MarketEvent]) -> int:
        if not events:
            return 0
        try:
            accepted = await self._publisher.publish_many(events)
        except Exception as error:
            self._stats.publish_failures += len(events)
            for event in events:
                self._deduplicator.forget(event.event_id)
            log.error(
                "ingestion.batch_publish_failed",
                count=len(events),
                error=str(error),
                error_type=type(error).__name__,
            )
            return 0

        self._stats.published += accepted
        return accepted

    # --- malformed data ----------------------------------------------------

    def record_malformed(self, error: Exception, *, context: str = "") -> None:
        """Count and log a message that could not be normalized (failure F3).

        Called by whatever is draining the provider when normalization raises. It is a
        method rather than a bare `except: pass` precisely so the count exists, is asserted
        on in tests, and becomes a metric in Phase 10.
        """
        self._stats.malformed += 1
        log.warning(
            "ingestion.malformed_message",
            provider=self._provider.name,
            context=context,
            error=str(error),
            error_type=type(error).__name__,
            raw=getattr(error, "raw", None),
            total_malformed=self._stats.malformed,
        )

    def record_reconnect(self, attempt: int) -> None:
        """Note a reconnection attempt. Wired into `reconnecting`'s callback."""
        self._stats.reconnects += 1
        log.info(
            "ingestion.reconnect",
            provider=self._provider.name,
            attempt=attempt,
            total_reconnects=self._stats.reconnects,
        )

    def record_stale(self, symbols: Sequence[str]) -> None:
        """Note symbols that have gone quiet.

        A warning, not an error: markets close, and only a caller that knows the session
        calendar can tell silence from a stall.
        """
        if not symbols:
            return
        self._stats.stale_warnings += 1
        log.warning(
            "ingestion.stale_symbols",
            provider=self._provider.name,
            symbols=list(symbols),
        )
