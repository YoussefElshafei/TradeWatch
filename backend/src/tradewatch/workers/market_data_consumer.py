"""The market data consumer: ``uv run tradewatch-consumer``.

The second half of the Phase 2 split.

```
Kafka → KafkaEventConsumer → MarketDataConsumerService → MarketDataProcessor → PostgreSQL
```

It never talks to a market data provider and never produces to a market topic. Its only
write to Kafka is a dead-letter record, which is why it owns a small producer of its own.

**Restarting this process is uneventful, and that is the headline.** Offsets live in the
broker under the consumer group, so a restart resumes where the last successful commit left
off. Whatever was in flight is redelivered, the writes behind it are idempotent (ADR-0012),
and the database ends up in the same state either way. Phase 1 could not do that: an
in-process queue that stopped took its contents with it.

**Who owns the retry budget.** ``TW_CONSUMER_MAX_RETRIES`` becomes the processor's storage
attempts, and nothing else retries. One owner, one bounded budget — nesting a retry loop
inside another retry loop produces a number of attempts nobody can predict from
configuration, and a database outage that lasts far longer than the operator expected.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys

from aiokafka import AIOKafkaProducer

from tradewatch.application.processing import MarketDataConsumerService, MarketDataProcessor
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.infrastructure.database.market_store import SqlMarketDataStore
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.events.kafka import (
    KafkaDeadLetterSink,
    KafkaEventConsumer,
    Topics,
    ensure_topics,
    open_dead_letter_sink,
)
from tradewatch.observability.logging import configure_logging, get_logger
from tradewatch.observability.metrics import start_metrics_server

log = get_logger(__name__)

__all__ = ["main", "run_consumer"]


async def run_consumer(
    settings: Settings,
    *,
    max_batches: int | None = None,
    stop: asyncio.Event | None = None,
    database: Database | None = None,
) -> MarketDataConsumerService:
    """Consume market data into PostgreSQL until cancelled or until ``max_batches``.

    Returns the service so a caller — a test, or a demo script — can read its counters.
    """
    topics = Topics(settings.kafka.kafka_topic_prefix)

    if settings.kafka.kafka_ensure_topics:
        await ensure_topics(settings.kafka, topics=topics)

    owns_database = database is None
    database = database or Database(settings.database)
    if owns_database:
        await database.connect()

    processor = MarketDataProcessor(
        SqlMarketDataStore(database.session_factory),
        batch_size=settings.kafka.consumer_batch_size,
        # The processor is the only retrier. See the module docstring.
        max_storage_attempts=settings.kafka.consumer_max_retries + 1,
        retry_delay=settings.kafka.consumer_retry_backoff_s,
    )

    dlq_producer: AIOKafkaProducer | None = None
    dead_letters: KafkaDeadLetterSink | None = None
    if settings.kafka.dlq_enabled:
        dead_letters, dlq_producer = await open_dead_letter_sink(
            settings.kafka,
            topics=topics,
            consumer_group=settings.kafka.kafka_consumer_group,
        )

    consumer = KafkaEventConsumer(
        settings.kafka,
        topics=topics,
        dead_letters=dead_letters,
    )
    service = MarketDataConsumerService(consumer, processor, dead_letters=dead_letters)

    await consumer.start()
    log.info(
        "consumer.starting",
        bootstrap=settings.kafka.kafka_bootstrap,
        group_id=consumer.group_id,
        topics=list(topics.market),
        dlq=topics.dead_letter if dead_letters else None,
        batch_size=settings.kafka.consumer_batch_size,
    )

    try:
        await service.run(max_batches=max_batches, stop=stop)
    finally:
        await consumer.stop()
        if dlq_producer is not None:
            with contextlib.suppress(Exception):
                await dlq_producer.stop()
        if owns_database:
            await database.disconnect()

    log.info(
        "consumer.complete",
        consumed=service.stats.as_dict(),
        processed=processor.stats.as_dict(),
        transport=consumer.stats.as_dict(),
    )
    return service


def main() -> int:
    """Entry point. Returns a process exit code."""
    load_env_file()

    try:
        settings = get_settings()
    except ConfigurationError as error:
        print(error, file=sys.stderr)  # noqa: T201 — logging is not configured yet
        return 2

    configure_logging(level=settings.app.log_level, log_format=settings.app.log_format)
    start_metrics_server("consumer", settings.app)

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
        await run_consumer(settings, stop=stop)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("consumer.interrupted")
    except Exception as error:
        # A storage failure that outlived its retries lands here. The offsets were not
        # committed, so a non-zero exit is the honest report: the supervisor restarts the
        # process and the broker redelivers what never landed.
        log.error("consumer.failed", error=str(error), error_type=type(error).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
