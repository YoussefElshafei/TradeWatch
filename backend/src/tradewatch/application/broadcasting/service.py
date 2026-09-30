"""Fan out market events to live subscribers, and invalidate their cached reads.

```
Kafka (trades, quotes, bars.1m) -> MarketBroadcastService -> Redis Pub/Sub -> /ws/market
                                                           -> Redis cache invalidation
```

**Why this is not the Bar Aggregator's problem.** This service reads the *same* topics the
aggregator and the persistence consumer already read, in its own consumer group
(`TW_KAFKA_BROADCAST_GROUP`) — a third independent reader, proving ADR-0001's fan-out claim a
third time. It does not accumulate anything: publishing "AAPL just traded" to Redis and
deleting a cache key are both *idempotent by nature* (publishing the same notification twice,
or deleting an absent key twice, changes nothing), so unlike the aggregator (`ADR-0016`) this
service needs no `consumer_progress` tracking at all. A redelivered event after a restart
just republishes a live-update message nobody but a currently-open browser tab would even
notice arrived twice, and re-deletes a cache key that may already be gone. There is nothing
here for "idempotent write" versus "idempotent consumer" (see the domain event docs) to
diverge over, because there is no accumulation — see the project's `learning.md` for the
contrast with the aggregator's bug.

**Why failures here never block the offset commit.** A publish or a cache delete that cannot
reach Redis is *already* a no-op by the time it returns (failure F9 is handled once, inside
`RedisCache`/the `Cache` and `EventBroadcaster` ports — see their docstrings). This service
therefore has nothing to catch: every event in a batch is always "processed" from its point
of view, and the batch is always committed. There is no poison-message concept here, unlike
`MarketDataConsumerService` — a live-update notification cannot violate a database
constraint, so there is nothing to isolate or quarantine.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final

from tradewatch.application.ports.broadcaster import EventBroadcaster
from tradewatch.application.ports.cache import Cache
from tradewatch.application.ports.event_bus import EventBatchConsumer
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import AlertEvent, BarEvent, MarketEvent, QuoteEvent, TradeEvent
from tradewatch.observability.logging import get_logger

__all__ = [
    "ALERT_BROADCAST_CHANNEL",
    "BROADCAST_CHANNEL",
    "BroadcastStats",
    "MarketBroadcastService",
    "bars_cache_key",
    "cache_keys_for",
    "quote_cache_key",
]

log = get_logger(__name__)

#: One shared Redis Pub/Sub channel for every symbol (`ARCHITECTURE.md` §10's
#: `tw:ws:{channel}` shape, with `market` as the one channel Phase 4 defines). A connected
#: API process subscribes to this once and fans out in-process by symbol — see
#: `api/routes/websocket.py` — rather than one Redis subscription per symbol, which would
#: grow without bound as clients pick different watchlists.
BROADCAST_CHANNEL: Final = "tw:ws:market"

#: Phase 9's channel, same `tw:ws:{channel}` shape, fanned out in-process by `user_id`
#: instead of `symbol` — see `api/routes/websocket.py`'s `/ws/alerts` route.
ALERT_BROADCAST_CHANNEL: Final = "tw:ws:alerts"


def quote_cache_key(symbol: str) -> str:
    return f"tw:quote:{symbol}"


def bars_cache_key(symbol: str, interval: BarInterval) -> str:
    return f"tw:bars:{symbol}:{interval.value}"


def cache_keys_for(event: MarketEvent) -> tuple[str, ...]:
    """Which cached reads this event makes stale, per the key shapes in `ARCHITECTURE.md` §10.

    A trade or quote invalidates the "latest price" cache (`get_latest`). A bar invalidates
    the recent-window bars cache for its own interval only — a new 1-minute bar does not make
    a cached 1-hour series stale.
    """
    if isinstance(event, TradeEvent | QuoteEvent):
        return (quote_cache_key(event.symbol),)
    if isinstance(event, BarEvent):
        return (bars_cache_key(event.symbol, event.interval),)
    return ()


@dataclass(slots=True)
class BroadcastStats:
    batches: int = 0
    events: int = 0
    published: int = 0
    invalidations: int = 0
    started_at: dt.datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "batches": self.batches,
            "events": self.events,
            "published": self.published,
            "invalidations": self.invalidations,
        }


class MarketBroadcastService:
    """Consume trades, quotes and bars; publish live updates and invalidate their cache."""

    def __init__(
        self,
        consumer: EventBatchConsumer,
        broadcaster: EventBroadcaster,
        cache: Cache,
        *,
        channel: str = BROADCAST_CHANNEL,
        alerts_channel: str | None = None,
        clock: Callable[[], dt.datetime] | None = None,
    ) -> None:
        self._consumer = consumer
        self._broadcaster = broadcaster
        self._cache = cache
        self._channel = channel
        #: `None` unless the caller also subscribed this consumer to `tw.alerts.v1` — a
        #: worker that never sees an `AlertEvent` has no reason to name a second channel.
        self._alerts_channel = alerts_channel
        self._clock = clock or (lambda: dt.datetime.now(dt.UTC))
        self._stats = BroadcastStats()

    @property
    def stats(self) -> BroadcastStats:
        return self._stats

    async def run(
        self,
        *,
        max_batches: int | None = None,
        stop: asyncio.Event | None = None,
    ) -> BroadcastStats:
        self._stats.started_at = self._clock()
        log.info("broadcaster.started", channel=self._channel)

        try:
            async for batch in self._consumer.batches():
                if stop is not None and stop.is_set():
                    log.info("broadcaster.stop_requested", **self._stats.as_dict())
                    break
                if not batch:
                    continue

                for event in batch:
                    await self._broadcast_one(event)

                await self._consumer.commit()
                self._stats.batches += 1
                self._stats.events += len(batch)

                if max_batches is not None and self._stats.batches >= max_batches:
                    break
        except asyncio.CancelledError:
            log.info("broadcaster.cancelled", **self._stats.as_dict())
            raise
        finally:
            log.info("broadcaster.stopped", **self._stats.as_dict())

        return self._stats

    async def _broadcast_one(self, event: MarketEvent) -> None:
        channel = self._channel
        if isinstance(event, AlertEvent) and self._alerts_channel is not None:
            channel = self._alerts_channel
        message = event.model_dump(mode="json")
        await self._broadcaster.publish(channel, message)
        self._stats.published += 1

        keys = cache_keys_for(event)
        if keys:
            await self._cache.delete(*keys)
            self._stats.invalidations += len(keys)
