"""The Phase 1 pipeline: ``uv run tradewatch-pipeline``.

Runs ingestion and processing **in one process**, sharing one in-memory event bus:

```
SyntheticProvider → normalize → InMemoryEventBus → MarketDataProcessor → PostgreSQL
```

The single process is not a design preference, it is a property of the bus. An in-process
queue cannot be read from another process, so today the producer and the consumer have to
live together. That is precisely the limitation ADR-0001 cites as the reason for Kafka:
independent failure domains, restart-safe offsets, and replay are all unavailable while this
is the transport. When Phase 2 lands, this module splits into two entry points and nothing
above the ports changes.

`tradewatch-ingest` still exists and still runs ingestion alone, publishing to a bus nothing
drains. That is useful for watching normalization, and useless for getting data into the
database — which is why this exists.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import signal
import sys
from collections.abc import AsyncIterator, Sequence

from tradewatch.application.ingestion import EventDeduplicator, IngestionService
from tradewatch.application.processing import MarketDataProcessor
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import MarketEvent
from tradewatch.infrastructure.database.market_store import SqlMarketDataStore
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.events.memory import InMemoryEventBus
from tradewatch.infrastructure.providers.factory import build_provider, retry_policy_from_settings
from tradewatch.infrastructure.providers.resilience import reconnecting
from tradewatch.observability.logging import configure_logging, get_logger

log = get_logger(__name__)

__all__ = ["main", "run_pipeline"]

#: Bounded so a stalled processor produces backpressure rather than an out-of-memory kill.
#: When the bus fills, ingestion counts the rejection and sheds — it does not block.
DEFAULT_BUS_SIZE = 10_000

#: Minute bars fetched per symbol at startup, so a chart has history the moment it opens.
#:
#: Bars come from the provider's historical endpoint rather than from the live trade stream,
#: because the component that aggregates trades into bars is Phase 3 (`ARCHITECTURE.md`
#: §3.3). Until it exists, the trade stream fills `trades` and this fills `bars`, and the two
#: are independent. A chart built on live-aggregated bars is what closes that gap.
DEFAULT_BACKFILL_BARS = 240


async def run_pipeline(
    settings: Settings,
    *,
    max_events: int | None = None,
    stop: asyncio.Event | None = None,
    database: Database | None = None,
    backfill_bars: int = DEFAULT_BACKFILL_BARS,
) -> tuple[IngestionService, MarketDataProcessor]:
    """Run ingestion and processing until cancelled, or until ``max_events`` are published.

    Returns both services so a caller — a test, or a demo script — can read their stats.
    """
    owns_database = database is None
    database = database or Database(settings.database)
    if owns_database:
        await database.connect()

    bus = InMemoryEventBus(max_size=DEFAULT_BUS_SIZE)
    provider = build_provider(settings.provider, settings.market)
    ingestion = IngestionService(
        provider,
        bus,
        deduplicator=EventDeduplicator(
            max_entries=settings.provider.dedupe_max_entries,
            ttl_seconds=settings.provider.dedupe_window_seconds,
        ),
    )
    processor = MarketDataProcessor(SqlMarketDataStore(database.session_factory))
    symbols = list(settings.market.symbols)

    log.info(
        "pipeline.starting",
        provider=provider.name,
        symbols=symbols,
        bus_size=DEFAULT_BUS_SIZE,
    )

    # Register the instruments the provider knows about before any data arrives. Not
    # required — the store registers a provisional row for an unknown symbol — but it means
    # the reference data is real rather than provisional from the first tick.
    await _register_instruments(ingestion, database)

    # History first, so the chart is not empty while the live stream warms up. Publishing
    # through the same bus and processor as everything else means the backfill path is not
    # a second, separately-tested way into the database.
    if backfill_bars:
        await _backfill(ingestion, processor, bus, symbols, bars=backfill_bars)

    def trade_stream() -> AsyncIterator[MarketEvent]:
        return reconnecting(
            lambda: provider.stream_trades(symbols),
            policy=retry_policy_from_settings(settings.provider),
            description=f"{provider.name}:trades",
            on_reconnect=ingestion.record_reconnect,
        )

    tasks = [
        asyncio.create_task(
            ingestion.run_stream(
                symbols, kind="trades", stream=trade_stream(), max_events=max_events
            ),
            name="pipeline-ingest",
        ),
        asyncio.create_task(
            processor.run(bus.consume(), max_events=max_events), name="pipeline-process"
        ),
    ]
    if stop is not None:
        tasks.append(asyncio.create_task(stop.wait(), name="pipeline-stop"))

    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

        # Ingestion finishing first is the normal end of a bounded run. Give the processor
        # a moment to drain what is still queued before cancelling it, or the last batch
        # would be counted as published and never stored.
        await _drain(bus)

        for task in pending:
            task.cancel()
        for task in pending:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        for task in done:
            if task.get_name() != "pipeline-stop" and not task.cancelled():
                task.result()
    finally:
        await processor.flush()
        await provider.aclose()
        if owns_database:
            await database.disconnect()

    log.info(
        "pipeline.complete",
        ingested=ingestion.stats.as_dict(),
        processed=processor.stats.as_dict(),
    )
    return ingestion, processor


async def _backfill(
    ingestion: IngestionService,
    processor: MarketDataProcessor,
    bus: InMemoryEventBus,
    symbols: Sequence[str],
    *,
    bars: int,
) -> None:
    """Fetch recent bars for each symbol and run them through the normal path.

    Overlapping a previous run is safe and expected: event ids are deterministic
    (ADR-0012), so re-fetched bars deduplicate rather than duplicating.
    """
    end = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
    start = end - dt.timedelta(minutes=bars)

    for symbol in symbols:
        await ingestion.backfill_bars(symbol, BarInterval.ONE_MINUTE, start, end)

    # Drain what the backfill just queued, so the bars are stored before the live stream
    # starts competing for the same bus.
    async for event in _take(bus.drain_iter(), bus.depth):
        await processor.handle(event)
    await processor.flush()
    log.info("pipeline.backfill_complete", symbols=len(symbols), bars=bars)


async def _take(stream: AsyncIterator[MarketEvent], count: int) -> AsyncIterator[MarketEvent]:
    taken = 0
    if count <= 0:
        return
    async for event in stream:
        yield event
        taken += 1
        if taken >= count:
            return


async def _register_instruments(ingestion: IngestionService, database: Database) -> None:
    """Seed reference data from the provider, so symbols are not provisional."""
    from tradewatch.infrastructure.database.models import Instrument
    from tradewatch.infrastructure.repositories import InstrumentRepository

    snapshots = await ingestion.sync_instruments()
    if not snapshots:
        return

    async with database.session() as session:
        repository = InstrumentRepository(session)
        for snapshot in snapshots:
            await repository.upsert(
                Instrument(
                    symbol=snapshot.symbol,
                    name=snapshot.name,
                    asset_class=snapshot.asset_class,
                    exchange=snapshot.exchange,
                    currency=snapshot.currency,
                    tick_size=snapshot.tick_size,
                    lot_size=snapshot.lot_size,
                    provider_metadata=dict(snapshot.provider_metadata),
                )
            )
    log.info("pipeline.instruments_registered", count=len(snapshots))


async def _drain(bus: InMemoryEventBus, *, seconds: float = 5.0) -> None:
    """Let the processor catch up with the queue before shutdown.

    Without this, a bounded run ends with events counted as published by ingestion and never
    seen by the processor — the two services' stats would disagree for no reason other than
    the order the tasks happened to be cancelled in.

    Bounded, because if the processor has already died there is nothing to wait for and
    hanging on shutdown is worse than reporting the shortfall.
    """
    with contextlib.suppress(TimeoutError):
        async with asyncio.timeout(seconds):
            await bus.wait_until_empty()
            return
    log.warning("pipeline.drain_incomplete", remaining=bus.depth)


def main() -> int:
    """Entry point. Returns a process exit code."""
    load_env_file()

    try:
        settings = get_settings()
    except ConfigurationError as error:
        print(error, file=sys.stderr)  # noqa: T201 — logging is not configured yet
        return 2

    configure_logging(level=settings.app.log_level, log_format=settings.app.log_format)

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signal_name in ("SIGINT", "SIGTERM"):
            handler = getattr(signal, signal_name, None)
            if handler is None:
                continue
            with contextlib.suppress(NotImplementedError):
                # Windows' proactor loop has no signal handlers; KeyboardInterrupt still works.
                loop.add_signal_handler(handler, stop.set)
        await run_pipeline(settings, stop=stop)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("pipeline.interrupted")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
