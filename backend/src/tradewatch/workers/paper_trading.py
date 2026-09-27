"""The Paper Trading Engine: `uv run tradewatch-paper-trading`.

```
Kafka (tw.market.bars.1m.v1) → PaperTradingService → PostgreSQL (orders, fills, positions)
```

The fourth independent reader of the bar topic, in its own group `paper-trading` — killing
this worker affects no other consumer, and vice versa (ADR-0001). See
`application/trading/live_service.py` for what it actually does with each bar; this module
is only Kafka/process wiring, matching the shape of every other worker in this package.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys

from tradewatch.application.trading.live_service import PaperTradingService
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.infrastructure.database.session import Database
from tradewatch.infrastructure.events.kafka import (
    KafkaEventConsumer,
    Topics,
    ensure_topics,
    open_dead_letter_sink,
)
from tradewatch.observability.logging import configure_logging, get_logger

log = get_logger(__name__)

__all__ = ["main", "run_paper_trading"]


async def run_paper_trading(
    settings: Settings,
    *,
    max_batches: int | None = None,
    stop: asyncio.Event | None = None,
    database: Database | None = None,
) -> PaperTradingService:
    """Consume bars and run the Paper Trading Engine until cancelled, `stop`, or `max_batches`."""
    topics = Topics(settings.kafka.kafka_topic_prefix)
    if settings.kafka.kafka_ensure_topics:
        await ensure_topics(settings.kafka, topics=topics)

    owns_database = database is None
    database = database or Database(settings.database)
    if owns_database:
        await database.connect()

    dlq_producer = None
    dead_letters = None
    if settings.kafka.dlq_enabled:
        dead_letters, dlq_producer = await open_dead_letter_sink(
            settings.kafka,
            topics=topics,
            consumer_group=settings.kafka.kafka_paper_trading_group,
            client_id="tradewatch-paper-trading-dlq",
        )

    consumer = KafkaEventConsumer(
        settings.kafka,
        topics=topics,
        dead_letters=dead_letters,
        group_id=settings.kafka.kafka_paper_trading_group,
        # Bars only. This service has no business with trades or quotes.
        subscribe_to=(topics.bars_1m,),
    )
    service = PaperTradingService(consumer, database)

    await consumer.start()
    await service.warm_up()
    log.info(
        "paper_trading.starting",
        group_id=consumer.group_id,
        topics=list(consumer.subscribed),
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

    log.info("paper_trading.stopped")
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

    async def _run() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signal_name in ("SIGINT", "SIGTERM"):
            handler = getattr(signal, signal_name, None)
            if handler is None:
                continue
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(handler, stop.set)
        await run_paper_trading(settings, stop=stop)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("paper_trading.interrupted")
    except Exception as error:
        log.error("paper_trading.failed", error=str(error), error_type=type(error).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
