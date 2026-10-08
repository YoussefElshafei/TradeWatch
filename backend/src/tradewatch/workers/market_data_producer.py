"""The market data producer: ``uv run tradewatch-producer``.

The first half of the Phase 2 split.

```
SyntheticProvider → normalize → IngestionService → Kafka
```

It is the only process that talks to a market data provider, and it never reads from Kafka.
When it dies, the consumer keeps draining whatever is already on the topics; when the
consumer dies, this keeps producing. That independence is the entire reason the broker is
here, and it is what an in-process queue could not offer at any price.

**How little of this is new.** The provider, normalization, deduplication, reconnection,
staleness watching and the failure counters are all Phase 1 code, unchanged. The single
difference is which :class:`~tradewatch.application.ports.event_bus.EventPublisher` is
handed to :func:`~tradewatch.workers.ingestion.run_ingestion`. That is what the port was
for, and this module is the smallest honest demonstration that it worked.

**The instrument bootstrap is the one wart.** Before streaming, this writes the provider's
instrument list to PostgreSQL, so symbols have real names and exchanges instead of the
provisional rows the store creates on first sight. Reference data is not market data and
does not belong on a market data topic, so there is nowhere else for it to go yet — but it
does mean this process opens a database connection, which weakens the independence claimed
above. It is therefore **best effort**: if the database is unreachable the producer logs and
carries on, because a producer that will not start without PostgreSQL would be a worse
violation of the split than one that occasionally cannot pretty up a symbol name. The proper
fix is a reference-data sync of its own, and it is listed as a remaining concern rather than
smuggled in here.

**Backfill** publishes recent 1-minute bars through the same producer, the same topic and
the same consumer as live data, so there is exactly one path into the database. Overlapping
a previous run is safe: event ids are deterministic (ADR-0012), so re-fetched bars
deduplicate instead of duplicating.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import signal
import sys
from collections.abc import Sequence

from tradewatch.application.ingestion import IngestionService
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.domain.enums import BarInterval
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.events.kafka import KafkaEventPublisher, Topics, ensure_topics
from tradewatch.infrastructure.providers.factory import build_provider
from tradewatch.observability.logging import configure_logging, get_logger
from tradewatch.observability.metrics import start_metrics_server
from tradewatch.workers.ingestion import build_service, run_ingestion

log = get_logger(__name__)

__all__ = ["main", "run_producer"]

#: Minute bars fetched per symbol at startup, so a chart has history the moment it opens.
#:
#: Bars come from the provider's historical endpoint rather than from the live trade stream,
#: because the component that aggregates trades into bars is Phase 3 (`ARCHITECTURE.md`
#: §3.3). Until it exists, the trade stream fills `trades` and this fills `bars`.
DEFAULT_BACKFILL_BARS = 240


async def run_producer(
    settings: Settings,
    *,
    max_events: int | None = None,
    stop: asyncio.Event | None = None,
    backfill_bars: int = DEFAULT_BACKFILL_BARS,
    bootstrap_instruments: bool = True,
) -> IngestionService:
    """Publish market data to Kafka until cancelled, or until ``max_events`` per stream.

    Returns the ingestion service so a caller — a test, or a demo script — can read its
    counters.
    """
    topics = Topics(settings.kafka.kafka_topic_prefix)

    if settings.kafka.kafka_ensure_topics:
        await ensure_topics(settings.kafka, topics=topics)

    provider = build_provider(settings.provider, settings.market)
    publisher = KafkaEventPublisher(settings.kafka, topics=topics)
    service, _ = build_service(settings, publisher, provider=provider)
    symbols = list(settings.market.symbols)

    await publisher.start()
    log.info(
        "producer.starting",
        provider=provider.name,
        symbols=symbols,
        topics=list(topics.market),
        bootstrap=settings.kafka.kafka_bootstrap,
    )

    try:
        if bootstrap_instruments:
            await _bootstrap_instruments(service, settings)

        if backfill_bars:
            await _backfill(service, symbols, bars=backfill_bars)

        await run_ingestion(
            settings,
            publisher,
            provider=provider,
            service=service,
            max_events=max_events,
            stop=stop,
        )
    finally:
        # Flushes first, so the tail of the session is acknowledged by the broker rather
        # than lost in a close nobody checked.
        await publisher.stop()

    log.info(
        "producer.complete",
        ingested=service.stats.as_dict(),
        published=publisher.stats.as_dict(),
    )
    return service


async def _backfill(service: IngestionService, symbols: Sequence[str], *, bars: int) -> None:
    """Publish recent 1-minute bars for each symbol.

    Only 1-minute bars are published: they are the only interval anything produces until
    the Phase 3 aggregator exists, and `tw.market.bars.1m.v1` is the only bar topic.
    """
    end = dt.datetime.now(dt.UTC).replace(second=0, microsecond=0)
    start = end - dt.timedelta(minutes=bars)

    published = 0
    for symbol in symbols:
        published += await service.backfill_bars(symbol, BarInterval.ONE_MINUTE, start, end)

    log.info("producer.backfill_complete", symbols=len(symbols), bars=bars, published=published)


async def _bootstrap_instruments(service: IngestionService, settings: Settings) -> None:
    """Write the provider's instrument list to PostgreSQL. Best effort, by design.

    See the module docstring: this is the one place the producer touches the database, and
    failing to do it must not stop market data from flowing. The store registers provisional
    instruments for any symbol it has not seen, so the pipeline is correct without this —
    just less readable.
    """
    from tradewatch.infrastructure.database.models import Instrument
    from tradewatch.infrastructure.repositories import InstrumentRepository

    database = Database(settings.database)
    try:
        snapshots = await service.sync_instruments()
        if not snapshots:
            return

        await database.connect()
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
        log.info("producer.instruments_registered", count=len(snapshots))
    except Exception as error:
        log.warning(
            "producer.instrument_bootstrap_skipped",
            error=str(error),
            error_type=type(error).__name__,
            detail="symbols will be registered provisionally by the consumer on first sight",
        )
    finally:
        with contextlib.suppress(Exception):
            await database.disconnect()


def main() -> int:
    """Entry point. Returns a process exit code."""
    load_env_file()

    try:
        settings = get_settings()
    except ConfigurationError as error:
        print(error, file=sys.stderr)  # noqa: T201 — logging is not configured yet
        return 2

    configure_logging(level=settings.app.log_level, log_format=settings.app.log_format)
    start_metrics_server("producer", settings.app)

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
        await run_producer(settings, stop=stop)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("producer.interrupted")
    except Exception as error:
        log.error("producer.failed", error=str(error), error_type=type(error).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
