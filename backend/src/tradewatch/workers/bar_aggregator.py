"""The bar aggregator: ``uv run tradewatch-aggregator``.

```
Kafka (tw.market.trades.v1) → BarAggregator → PostgreSQL → rollups to 5m/1h/1d
```

**The second consumer group.** It reads the same topic as `processor-persist`, in its own
group `bar-aggregator`, with its own offsets. Neither service can affect the other: kill
this one and trades keep being persisted; kill that one and bars keep being built. That
independence is the whole argument for a broker (ADR-0001), and until this existed the
argument was unproven — Kafka had exactly one consumer, which is the condition the ADR names
for revisiting the decision.

Two tasks run side by side:

* the **consumer**, folding trades into 1-minute bars and writing them;
* the **rollup**, deriving 5m/1h/1d bars from the stored 1m bars on a timer.

The rollup is on a timer rather than on every flush because it re-reads a window of minutes
each pass, and that window only changes when a bar closes — once a minute at most, for a
result nothing reads more often than that.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import signal
import sys

from tradewatch.application.aggregation import BarAggregationService, BarAggregator
from tradewatch.application.ports.bar_store import BarStore
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.domain.enums import BarInterval
from tradewatch.infrastructure.database.bar_store import SqlBarStore
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.events.kafka import (
    KafkaEventConsumer,
    Topics,
    ensure_topics,
    open_dead_letter_sink,
)
from tradewatch.observability.logging import configure_logging, get_logger

log = get_logger(__name__)

__all__ = ["main", "run_aggregator", "run_rollups"]


async def run_aggregator(
    settings: Settings,
    *,
    max_batches: int | None = None,
    stop: asyncio.Event | None = None,
    database: Database | None = None,
    rollups: bool = True,
) -> BarAggregationService:
    """Build bars from the trade stream until cancelled or until ``max_batches``."""
    topics = Topics(settings.kafka.kafka_topic_prefix)

    if settings.kafka.kafka_ensure_topics:
        await ensure_topics(settings.kafka, topics=topics)

    owns_database = database is None
    database = database or Database(settings.database)
    if owns_database:
        await database.connect()

    store = SqlBarStore(database.session_factory)
    aggregator = BarAggregator(
        store,
        lateness=dt.timedelta(seconds=settings.market.late_data_grace_seconds),
    )

    # Before a single trade is handled: whatever the last run left open is picked up where
    # it stopped (failure F13).
    await aggregator.restore(list(settings.market.symbols))

    dlq_producer = None
    dead_letters = None
    if settings.kafka.dlq_enabled:
        dead_letters, dlq_producer = await open_dead_letter_sink(
            settings.kafka,
            topics=topics,
            consumer_group=settings.kafka.kafka_aggregator_group,
            client_id="tradewatch-aggregator-dlq",
        )

    consumer = KafkaEventConsumer(
        settings.kafka,
        topics=topics,
        dead_letters=dead_letters,
        group_id=settings.kafka.kafka_aggregator_group,
        # Trades only. Quotes and bars are on the same broker and are none of its business.
        subscribe_to=(topics.trades,),
    )
    service = BarAggregationService(
        consumer,
        aggregator,
        # The same group name Kafka commits offsets under, so the progress rows and the
        # committed offsets describe one consumer rather than two.
        consumer_group=settings.kafka.kafka_aggregator_group,
    )

    await consumer.start()
    log.info(
        "aggregator.starting",
        group_id=consumer.group_id,
        topics=list(consumer.subscribed),
        lateness_seconds=settings.market.late_data_grace_seconds,
        symbols=list(settings.market.symbols),
    )

    rollup_task: asyncio.Task[None] | None = None
    if rollups:
        rollup_task = asyncio.create_task(
            _rollup_loop(settings, store, stop=stop), name="bar-rollups"
        )

    try:
        await service.run(max_batches=max_batches, stop=stop)
    finally:
        if rollup_task is not None:
            rollup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await rollup_task
        await service.close()
        await consumer.stop()
        if dlq_producer is not None:
            with contextlib.suppress(Exception):
                await dlq_producer.stop()
        if owns_database:
            await database.disconnect()

    log.info(
        "aggregator.complete",
        consumed=service.stats.as_dict(),
        aggregated=aggregator.stats.as_dict(),
    )
    return service


async def run_rollups(
    settings: Settings,
    store: BarStore,
    *,
    now: dt.datetime | None = None,
) -> dict[str, int]:
    """One rollup pass: derive every configured interval above 1m. Returns rows per interval.

    Idempotent. Each pass recomputes a trailing window from the stored minutes and upserts
    the result, so a pass that was missed costs nothing and a pass that runs twice writes
    the same rows twice.
    """
    end = now or dt.datetime.now(dt.UTC)
    start = end - dt.timedelta(minutes=settings.market.rollup_lookback_minutes)
    targets = [
        BarInterval(value)
        for value in settings.market.bar_intervals
        if BarInterval(value) is not BarInterval.ONE_MINUTE
    ]
    if not targets:
        return {}

    written: dict[str, int] = {}
    for symbol in settings.market.symbols:
        instrument_id = await store.resolve_instrument(symbol)
        for target in targets:
            rows = await store.roll_up(instrument_id, BarInterval.ONE_MINUTE, target, start, end)
            written[target.value] = written.get(target.value, 0) + rows

    if any(written.values()):
        log.info(
            "aggregator.rollups_written",
            window_start=start.isoformat(),
            window_end=end.isoformat(),
            **{f"rows_{name}": count for name, count in written.items()},
        )
    return written


async def _rollup_loop(settings: Settings, store: BarStore, *, stop: asyncio.Event | None) -> None:
    """Run rollups on a timer until cancelled.

    A failure here is logged and the loop continues. Rollups are derived data: a pass that
    fails costs a minute of freshness in the 5m series and is corrected by the next one,
    and taking the aggregator down over it would cost the 1m series instead — which is the
    one that cannot be recomputed from anything else.
    """
    period = settings.market.rollup_interval_seconds
    while True:
        if stop is None:
            await asyncio.sleep(period)
        else:
            # Wait for the period, but wake immediately on shutdown rather than holding it
            # open for up to a full rollup interval.
            try:
                async with asyncio.timeout(period):
                    await stop.wait()
            except TimeoutError:
                pass
            else:
                return

        try:
            await run_rollups(settings, store)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            log.error(
                "aggregator.rollup_failed",
                error=str(error),
                error_type=type(error).__name__,
                detail="derived data; the next pass recomputes the same window",
            )


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
                loop.add_signal_handler(handler, stop.set)
        await run_aggregator(settings, stop=stop)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("aggregator.interrupted")
    except Exception as error:
        log.error("aggregator.failed", error=str(error), error_type=type(error).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
