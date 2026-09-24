"""``RedisCache`` against a real Redis: hit, miss, expiration, invalidation, and the F9
"degrade, never fail" contract against a Redis that is not there at all.

Same reasoning as `tests/integration/conftest.py`'s database fixture: `ARCHITECTURE.md`
§11.2 says not to mock real infrastructure, and a fake in-memory dict would not prove the
one thing worth proving here — that `redis-py`'s actual TTL and Pub/Sub behave the way the
adapter assumes.

**Where the database comes from.** `TW_TEST_REDIS_URL` if set, otherwise
`redis://localhost:6379/15` — database index 15, kept apart from whatever a developer's own
`.env` points other tools at on index 0. If nothing is reachable, every test here skips with
an explanation, exactly like the PostgreSQL fixtures.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

from tradewatch.config.settings import RedisSettings
from tradewatch.infrastructure.cache import RedisCache

pytestmark = pytest.mark.integration

TEST_REDIS_URL = os.environ.get("TW_TEST_REDIS_URL", "redis://localhost:6379/15")

#: Port 1 is privileged and unassigned — refused immediately rather than timing out
#: (same trick as `tests/conftest.py`'s `UNREACHABLE_DATABASE_URL`).
UNREACHABLE_REDIS_URL = "redis://127.0.0.1:1/0"

SKIP_REASON = (
    "No test Redis. Start one with `docker compose -f docker/docker-compose.yml up -d "
    "redis` and optionally set TW_TEST_REDIS_URL."
)


@pytest_asyncio.fixture
async def cache() -> AsyncIterator[RedisCache]:
    """A connected cache on a scratch database, flushed before and after each test."""
    instance = RedisCache(RedisSettings(redis_url=TEST_REDIS_URL))
    await instance.connect()
    if await instance.check() is not None:
        await instance.disconnect()
        pytest.skip(SKIP_REASON)

    await instance.client.flushdb()
    yield instance
    await instance.client.flushdb()
    await instance.disconnect()


@pytest_asyncio.fixture
async def unreachable_cache() -> AsyncIterator[RedisCache]:
    """A cache pointed at a closed port by construction, not by circumstance."""
    instance = RedisCache(RedisSettings(redis_url=UNREACHABLE_REDIS_URL))
    await instance.connect()
    yield instance
    await instance.disconnect()


class TestHitMissExpirationInvalidation:
    async def test_a_key_that_was_never_set_is_a_miss(self, cache: RedisCache) -> None:
        assert await cache.get_json("tw:quote:NOPE") is None

    async def test_a_set_value_round_trips_through_get(self, cache: RedisCache) -> None:
        await cache.set_json("tw:quote:AAPL", {"price": "187.42"}, ttl_seconds=30)

        assert await cache.get_json("tw:quote:AAPL") == {"price": "187.42"}

    async def test_the_ttl_is_actually_applied(self, cache: RedisCache) -> None:
        await cache.set_json("tw:quote:AAPL", {"price": "1"}, ttl_seconds=30)

        ttl = await cache.client.ttl("tw:quote:AAPL")

        assert 0 < ttl <= 30

    async def test_a_key_expires_and_becomes_a_miss(self, cache: RedisCache) -> None:
        await cache.set_json("tw:quote:AAPL", {"price": "1"}, ttl_seconds=1)
        assert await cache.get_json("tw:quote:AAPL") is not None

        await asyncio.sleep(1.3)

        assert await cache.get_json("tw:quote:AAPL") is None

    async def test_delete_invalidates_immediately_regardless_of_ttl(
        self, cache: RedisCache
    ) -> None:
        await cache.set_json("tw:quote:AAPL", {"price": "1"}, ttl_seconds=3600)

        await cache.delete("tw:quote:AAPL")

        assert await cache.get_json("tw:quote:AAPL") is None

    async def test_deleting_several_keys_at_once(self, cache: RedisCache) -> None:
        await cache.set_json("tw:bars:AAPL:1m", [1], ttl_seconds=30)
        await cache.set_json("tw:bars:AAPL:5m", [2], ttl_seconds=30)

        await cache.delete("tw:bars:AAPL:1m", "tw:bars:AAPL:5m")

        assert await cache.get_json("tw:bars:AAPL:1m") is None
        assert await cache.get_json("tw:bars:AAPL:5m") is None

    async def test_deleting_a_key_that_was_never_set_does_not_raise(
        self, cache: RedisCache
    ) -> None:
        await cache.delete("tw:quote:NEVER-SET")  # must not raise

    async def test_check_reports_healthy_against_a_reachable_redis(self, cache: RedisCache) -> None:
        assert await cache.check() is None


class TestPublishSubscribe:
    async def test_a_subscriber_receives_a_published_message(self, cache: RedisCache) -> None:
        messages: list[dict[str, object]] = []

        async def collect_one() -> None:
            async for message in cache.subscribe("tw:ws:market"):
                messages.append(message)
                return

        collector = asyncio.create_task(collect_one())
        # Give the subscription time to attach before publishing — Pub/Sub delivers only to
        # subscribers that were already listening, unlike a durable Kafka topic.
        await asyncio.sleep(0.2)

        await cache.publish("tw:ws:market", {"symbol": "AAPL", "event_type": "market.trade"})

        await asyncio.wait_for(collector, timeout=2.0)
        assert messages == [{"symbol": "AAPL", "event_type": "market.trade"}]


class TestRedisUnavailableFallback:
    """Failure F9: a cache that cannot reach Redis degrades, it never raises."""

    async def test_get_is_a_miss_not_an_exception(self, unreachable_cache: RedisCache) -> None:
        assert await unreachable_cache.get_json("tw:quote:AAPL") is None

    async def test_set_does_not_raise(self, unreachable_cache: RedisCache) -> None:
        await unreachable_cache.set_json("tw:quote:AAPL", {"price": "1"}, ttl_seconds=30)

    async def test_delete_does_not_raise(self, unreachable_cache: RedisCache) -> None:
        await unreachable_cache.delete("tw:quote:AAPL")

    async def test_publish_does_not_raise(self, unreachable_cache: RedisCache) -> None:
        await unreachable_cache.publish("tw:ws:market", {"symbol": "AAPL"})

    async def test_check_reports_unreachable_rather_than_raising(
        self, unreachable_cache: RedisCache
    ) -> None:
        assert await unreachable_cache.check() is not None
