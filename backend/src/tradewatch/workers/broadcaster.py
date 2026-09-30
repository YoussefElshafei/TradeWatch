"""The broadcaster: ``uv run tradewatch-broadcaster``.

```
Kafka (trades, quotes, bars.1m) -> MarketBroadcastService -> Redis Pub/Sub -> /ws/market
Kafka (tw.alerts.v1)            -> MarketBroadcastService -> Redis Pub/Sub -> /ws/alerts
```

**The third consumer group.** `processor-persist` writes to PostgreSQL, `bar-aggregator`
folds trades into candles, and this one — `TW_KAFKA_BROADCAST_GROUP` — relays live updates to
connected browsers and invalidates their cached reads. All three read the same topics with
independent offsets; killing this one only stops live updates, exactly as killing the
aggregator only stops new bars (`ARCHITECTURE.md` §3.3, ADR-0001).

**Phase 9 added `tw.alerts.v1`.** `ARCHITECTURE.md` §7.4 names one logical "API broadcaster"
consumer for every topic a WebSocket route relays — this file, not a second one — so alerts
join the same consumer group and the same `MarketBroadcastService` rather than getting a
parallel implementation. It gets its own Redis channel (`ALERT_BROADCAST_CHANNEL`) because
`/ws/alerts` fans out by `user_id`, not `symbol` — see `api/routes/websocket.py`.

Unlike the aggregator, this process touches no database at all — see
`application/broadcasting/service.py` for why it needs no `consumer_progress` tracking
either.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import sys

from tradewatch.application.broadcasting import ALERT_BROADCAST_CHANNEL, MarketBroadcastService
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import ConfigurationError, Settings, get_settings
from tradewatch.infrastructure.cache import RedisCache
from tradewatch.infrastructure.events.kafka import (
    KafkaEventConsumer,
    Topics,
    ensure_topics,
    open_dead_letter_sink,
)
from tradewatch.observability.logging import configure_logging, get_logger

log = get_logger(__name__)

__all__ = ["main", "run_broadcaster"]


async def run_broadcaster(
    settings: Settings,
    *,
    max_batches: int | None = None,
    stop: asyncio.Event | None = None,
    cache: RedisCache | None = None,
) -> MarketBroadcastService:
    """Relay trades, quotes and bars to Redis until cancelled or until ``max_batches``."""
    topics = Topics(settings.kafka.kafka_topic_prefix)

    if settings.kafka.kafka_ensure_topics:
        await ensure_topics(settings.kafka, topics=topics)

    owns_cache = cache is None
    cache = cache or RedisCache(settings.redis)
    if owns_cache:
        await cache.connect()

    dlq_producer = None
    dead_letters = None
    if settings.kafka.dlq_enabled:
        dead_letters, dlq_producer = await open_dead_letter_sink(
            settings.kafka,
            topics=topics,
            consumer_group=settings.kafka.kafka_broadcast_group,
            client_id="tradewatch-broadcaster-dlq",
        )

    consumer = KafkaEventConsumer(
        settings.kafka,
        topics=topics,
        dead_letters=dead_letters,
        group_id=settings.kafka.kafka_broadcast_group,
        # Every channel a browser can subscribe to: trades, quotes, closed 1-minute bars
        # (higher intervals are derived server-side and never published of their own
        # accord — §9 of PROJECT_STATE.md), and Phase 9's alerts.
        subscribe_to=(*topics.market, topics.alerts),
    )
    service = MarketBroadcastService(
        consumer, broadcaster=cache, cache=cache, alerts_channel=ALERT_BROADCAST_CHANNEL
    )

    await consumer.start()
    log.info(
        "broadcaster.starting",
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
        if owns_cache:
            await cache.disconnect()

    log.info("broadcaster.complete", **service.stats.as_dict())
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
        await run_broadcaster(settings, stop=stop)

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        log.info("broadcaster.interrupted")
    except Exception as error:
        log.error("broadcaster.failed", error=str(error), error_type=type(error).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
