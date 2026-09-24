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
from collections.abc import AsyncIterator, Sequence

import pytest

from tests.kafka_support import bar, quote, trade
from tradewatch.application.broadcasting import (
    BROADCAST_CHANNEL,
    MarketBroadcastService,
    bars_cache_key,
    quote_cache_key,
)
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import MarketEvent

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
