"""``MarketBroadcastService``: every event is published and its cache key invalidated,
the batch is always committed, and nothing here ever needs to isolate a poison event —
unlike ``MarketDataConsumerService``, there is no way for a live-update notification to be
rejected the way a database write can be.

No broker and no Redis. The consumer is a scripted fake implementing ``EventBatchConsumer``
(mirrors `tests/unit/test_market_data_consumer.py`), and the broadcaster/cache are in-memory
recorders implementing ``EventBroadcaster``/``Cache``. The properties proved against a real
Redis live in `tests/integration/test_redis_cache.py`.
"""

from __future__ import annotations

import asyncio
import datetime as dt
from collections.abc import AsyncIterator, Sequence
from decimal import Decimal

import pytest

from tests.kafka_support import (
    MOMENT,
    FakeConsumerClient,
    alert,
    bar,
    factory,
    kafka_settings,
    quote,
    trade,
)
from tradewatch.application.broadcasting import (
    ALERT_BROADCAST_CHANNEL,
    BROADCAST_CHANNEL,
    MarketBroadcastService,
    bars_cache_key,
    quote_cache_key,
)
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import MarketEvent
from tradewatch.infrastructure.events.kafka import KafkaEventConsumer

pytestmark = pytest.mark.unit


class ScriptedConsumer:
    """An ``EventBatchConsumer`` that yields a fixed script, then goes quiet."""

    def __init__(self, script: Sequence[Sequence[MarketEvent]]) -> None:
        self.script = list(script)
        self.commits: list[int] = []
        self._delivered = 0

    async def batches(self) -> AsyncIterator[Sequence[MarketEvent]]:
        for batch in self.script:
            await asyncio.sleep(0)
            self._delivered += len(batch)
            yield batch
        while True:
            await asyncio.sleep(0)
            yield []

    async def commit(self) -> None:
        self.commits.append(self._delivered)


class RecordingBroadcaster:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    async def publish(self, channel: str, message: dict[str, object]) -> None:
        self.published.append((channel, message))


class RecordingCache:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    async def get_json(self, key: str) -> object | None:
        return None

    async def set_json(self, key: str, value: object, *, ttl_seconds: int) -> None:
        raise AssertionError("the broadcaster never writes to the cache, only invalidates it")

    async def delete(self, *keys: str) -> None:
        self.deleted.extend(keys)


async def _run_one_batch(
    events: Sequence[MarketEvent],
) -> tuple[ScriptedConsumer, RecordingBroadcaster, RecordingCache]:
    consumer = ScriptedConsumer([events])
    broadcaster = RecordingBroadcaster()
    cache = RecordingCache()
    service = MarketBroadcastService(consumer, broadcaster, cache)
    await service.run(max_batches=1)
    return consumer, broadcaster, cache


class TestPublishing:
    async def test_a_trade_is_published_on_the_shared_channel(self) -> None:
        event = trade(symbol="AAPL")
        _, broadcaster, _ = await _run_one_batch([event])

        assert len(broadcaster.published) == 1
        channel, message = broadcaster.published[0]
        assert channel == BROADCAST_CHANNEL
        assert message["symbol"] == "AAPL"
        assert message["event_type"] == "market.trade"

    async def test_money_is_a_string_in_the_published_message(self) -> None:
        """The same ADR-0009 rule the Kafka envelope follows: no JSON number for a price."""
        event = trade(symbol="AAPL", price="187.42")
        _, broadcaster, _ = await _run_one_batch([event])

        _, message = broadcaster.published[0]
        assert isinstance(message["price"], str)
        assert message["price"] == "187.42"

    async def test_every_event_in_a_batch_is_published(self) -> None:
        events = [trade(symbol="AAPL"), quote(symbol="AAPL"), bar(symbol="AAPL")]
        _, broadcaster, _ = await _run_one_batch(events)

        assert len(broadcaster.published) == 3
        assert [msg["event_type"] for _, msg in broadcaster.published] == [
            "market.trade",
            "market.quote",
            "market.bar.closed",
        ]


class TestAlertRouting:
    """Phase 9: the same service relays `tw.alerts.v1`, but to its own channel — `/ws/alerts`
    fans out by `user_id`, not `symbol`, so it cannot share `/ws/market`'s channel.
    """

    async def test_an_alert_goes_to_the_alerts_channel_when_one_is_configured(self) -> None:
        consumer = ScriptedConsumer([[alert()]])
        broadcaster = RecordingBroadcaster()
        service = MarketBroadcastService(
            consumer, broadcaster, RecordingCache(), alerts_channel=ALERT_BROADCAST_CHANNEL
        )

        await service.run(max_batches=1)

        assert len(broadcaster.published) == 1
        channel, message = broadcaster.published[0]
        assert channel == ALERT_BROADCAST_CHANNEL
        assert channel != BROADCAST_CHANNEL
        assert message["event_type"] == "alert.raised"

    async def test_an_alert_falls_back_to_the_market_channel_when_none_is_configured(
        self,
    ) -> None:
        """A worker that never subscribed to `tw.alerts.v1` never passes `alerts_channel` —
        this is what keeps that the safe default rather than a silent misroute."""
        _, broadcaster, _ = await _run_one_batch([alert()])

        channel, _ = broadcaster.published[0]
        assert channel == BROADCAST_CHANNEL

    async def test_market_events_are_unaffected_by_an_alerts_channel_being_configured(
        self,
    ) -> None:
        consumer = ScriptedConsumer([[trade(symbol="AAPL")]])
        broadcaster = RecordingBroadcaster()
        service = MarketBroadcastService(
            consumer, broadcaster, RecordingCache(), alerts_channel=ALERT_BROADCAST_CHANNEL
        )

        await service.run(max_batches=1)

        channel, _ = broadcaster.published[0]
        assert channel == BROADCAST_CHANNEL


class TestCacheInvalidation:
    async def test_a_trade_invalidates_the_symbols_quote_cache(self) -> None:
        _, _, cache = await _run_one_batch([trade(symbol="AAPL")])

        assert cache.deleted == [quote_cache_key("AAPL")]

    async def test_a_quote_invalidates_the_same_key_a_trade_does(self) -> None:
        """Both trades and quotes feed `GET /market/{symbol}/latest` — one cache key."""
        _, _, cache = await _run_one_batch([quote(symbol="AAPL")])

        assert cache.deleted == [quote_cache_key("AAPL")]

    async def test_a_bar_invalidates_only_its_own_interval(self) -> None:
        one_minute = bar(symbol="AAPL", interval=BarInterval.ONE_MINUTE)
        _, _, cache = await _run_one_batch([one_minute])

        assert cache.deleted == [bars_cache_key("AAPL", BarInterval.ONE_MINUTE)]
        assert bars_cache_key("AAPL", BarInterval.FIVE_MINUTES) not in cache.deleted

    async def test_different_symbols_invalidate_different_keys(self) -> None:
        events = [trade(symbol="AAPL"), trade(symbol="MSFT")]
        _, _, cache = await _run_one_batch(events)

        assert cache.deleted == [quote_cache_key("AAPL"), quote_cache_key("MSFT")]


class TestCommitOrdering:
    async def test_an_empty_batch_is_not_committed(self) -> None:
        """Mirrors `MarketDataConsumerService`: an empty poll has nothing to acknowledge.

        `max_batches` counts only *non-empty* batches (otherwise a bounded run in a quiet
        market would finish having done nothing), so the script needs a real batch after the
        empty one for `run(max_batches=1)` to ever return — an empty batch alone would spin
        on the consumer's infinite empty tail forever.
        """
        consumer = ScriptedConsumer([[], [trade(symbol="AAPL")]])
        service = MarketBroadcastService(consumer, RecordingBroadcaster(), RecordingCache())

        await service.run(max_batches=1)

        assert consumer.commits == [1]

    async def test_a_non_empty_batch_is_committed_after_broadcasting(self) -> None:
        consumer, _, _ = await _run_one_batch([trade(symbol="AAPL"), trade(symbol="MSFT")])

        assert consumer.commits == [2]

    async def test_stats_count_publishes_and_invalidations(self) -> None:
        consumer = ScriptedConsumer([[trade(symbol="AAPL"), bar(symbol="MSFT")]])
        broadcaster = RecordingBroadcaster()
        cache = RecordingCache()
        service = MarketBroadcastService(consumer, broadcaster, cache)

        stats = await service.run(max_batches=1)

        assert stats.batches == 1
        assert stats.events == 2
        assert stats.published == 2
        assert stats.invalidations == 2


class SeekableConsumer(ScriptedConsumer):
    """A `SkipsToLatest` consumer: records the seek, then serves what is left of the script."""

    def __init__(self, script: Sequence[Sequence[MarketEvent]]) -> None:
        super().__init__(script)
        self.seeks = 0

    async def skip_to_latest(self) -> None:
        self.seeks += 1


class TestStaleBacklog:
    """Phase 10 found a 2-million-message backlog relayed to browsers as live after downtime."""

    LIMIT = dt.timedelta(seconds=30)

    def _service(
        self, consumer: ScriptedConsumer, *, now: dt.datetime
    ) -> tuple[MarketBroadcastService, RecordingBroadcaster]:
        broadcaster = RecordingBroadcaster()
        service = MarketBroadcastService(
            consumer, broadcaster, RecordingCache(), clock=lambda: now, max_staleness=self.LIMIT
        )
        return service, broadcaster

    async def test_a_fresh_batch_is_relayed(self) -> None:
        consumer = SeekableConsumer([[trade()]])
        service, broadcaster = self._service(consumer, now=MOMENT + dt.timedelta(seconds=2))

        await service.run(max_batches=1)

        assert len(broadcaster.published) == 1
        assert consumer.seeks == 0

    async def test_a_stale_backlog_is_skipped_to_latest_and_live_relaying_resumes(self) -> None:
        later = MOMENT + dt.timedelta(hours=2)
        stale = [trade(trade_id="old-1"), trade(trade_id="old-2")]
        fresh = [factory(later).trade(
            symbol="AAPL", price=Decimal(101), size=Decimal(1), occurred_at=later,
            provider_trade_id="new-1",
        )]  # fmt: skip
        consumer = SeekableConsumer([stale, fresh])
        service, broadcaster = self._service(consumer, now=later + dt.timedelta(seconds=1))

        await service.run(max_batches=1)

        assert consumer.seeks == 1
        assert [message["event_id"] for _, message in broadcaster.published] == [
            str(fresh[0].event_id)
        ]
        assert service.stats.stale_skipped == 2
        assert consumer.commits[0] == 2  # the skip itself is acknowledged

    async def test_the_kafka_adapter_seeks_its_client_to_the_log_end(self) -> None:
        client = FakeConsumerClient([])
        consumer = KafkaEventConsumer(kafka_settings(), client=client)
        await consumer.start()

        await consumer.skip_to_latest()

        assert client.seeks == 1
