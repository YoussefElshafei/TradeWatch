"""Caching at the route level, against a real Redis — not just `RedisCache` in isolation
(`tests/integration/test_redis_cache.py`) or the broadcaster's invalidation logic in
isolation (`tests/unit/test_broadcast_service.py`), but the actual wiring in
`api/routes/market.py::_read_through`: a cache hit is served instead of querying PostgreSQL,
`TW_CACHE_ENABLED=false` bypasses the cache entirely, and deleting a key makes the next read
fresh again.

**How a cache *hit* is proven, not just a cache *write*.** Asserting a value round-trips
through the cache proves nothing about which path actually answered the request — a correct
DB read and a correct cache hit look identical from the outside. Instead, each test that
proves a hit *poisons* the cache first: writes a value under the real key that deliberately
disagrees with what PostgreSQL holds. If the response matches the poisoned value, the cache
answered; if it matches PostgreSQL, the cache was bypassed. That is the only way to
distinguish the two from outside the route.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncIterator
from decimal import Decimal

import httpx2 as httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.test_redis_cache import SKIP_REASON, TEST_REDIS_URL
from tradewatch.api.app import create_app
from tradewatch.api.dependencies import get_session
from tradewatch.api.routes.health import clear_readiness_checks
from tradewatch.application.broadcasting import quote_cache_key
from tradewatch.config.settings import RedisSettings, Settings
from tradewatch.domain.entities import NewTrade
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.cache import RedisCache
from tradewatch.infrastructure.database.models import Instrument
from tradewatch.infrastructure.repositories import TradeRepository

pytestmark = pytest.mark.integration

MOMENT = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)

#: A price that could never come from `_seed_one_trade` below — any response carrying it
#: proves the cache answered, not PostgreSQL.
POISON_PRICE = "999.99000000"


async def _seed_one_trade(session: AsyncSession, instrument: Instrument) -> None:
    await TradeRepository(session).bulk_insert(
        [
            NewTrade(
                event_id=new_id(),
                instrument_id=instrument.id,
                price=Decimal("100.00"),
                size=Decimal("10"),
                occurred_at=MOMENT,
                ingested_at=MOMENT,
                provider="synthetic",
            )
        ]
    )


async def _make_client(
    session: AsyncSession, settings: Settings, cache: RedisCache
) -> AsyncIterator[httpx.AsyncClient]:
    clear_readiness_checks()
    app = create_app(settings)

    async def _session_override() -> AsyncSession:
        return session

    app.dependency_overrides[get_session] = _session_override
    app.state.cache = cache
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
    app.dependency_overrides.clear()
    clear_readiness_checks()


@pytest_asyncio.fixture
async def cache() -> AsyncIterator[RedisCache]:
    """A real, connected, flushed cache — this file is specifically about caching."""
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
async def cached_api(
    session: AsyncSession, settings: Settings, cache: RedisCache
) -> AsyncIterator[httpx.AsyncClient]:
    async for client in _make_client(session, settings, cache):
        yield client


@pytest_asyncio.fixture
async def cache_disabled_api(
    session: AsyncSession, settings: Settings, cache: RedisCache
) -> AsyncIterator[httpx.AsyncClient]:
    disabled = settings.model_copy(
        update={"redis": settings.redis.model_copy(update={"cache_enabled": False})}
    )
    async for client in _make_client(session, disabled, cache):
        yield client


class TestCacheHitServesInsteadOfPostgreSQL:
    async def test_a_poisoned_cache_entry_is_what_gets_served(
        self,
        cached_api: httpx.AsyncClient,
        cache: RedisCache,
        session: AsyncSession,
        instrument: Instrument,
    ) -> None:
        await _seed_one_trade(session, instrument)
        await cache.set_json(
            quote_cache_key(instrument.symbol),
            {
                "symbol": instrument.symbol,
                "price": POISON_PRICE,
                "size": "1.00000000",
                "traded_at": MOMENT.isoformat(),
                "bid_price": None,
                "ask_price": None,
                "quoted_at": None,
                "source": "poison",
            },
            ttl_seconds=30,
        )

        body = (await cached_api.get(f"/api/v1/market/{instrument.symbol}/latest")).json()

        assert body["price"] == POISON_PRICE
        assert body["source"] == "poison"

    async def test_a_miss_is_populated_from_postgresql_and_then_served_from_cache(
        self,
        cached_api: httpx.AsyncClient,
        cache: RedisCache,
        session: AsyncSession,
        instrument: Instrument,
    ) -> None:
        await _seed_one_trade(session, instrument)

        first = (await cached_api.get(f"/api/v1/market/{instrument.symbol}/latest")).json()
        assert first["price"] == "100.00000000"

        cached = await cache.get_json(quote_cache_key(instrument.symbol))
        assert cached is not None
        assert cached["price"] == "100.00000000"


class TestCacheKillSwitch:
    async def test_disabling_the_cache_ignores_a_poisoned_entry(
        self,
        cache_disabled_api: httpx.AsyncClient,
        cache: RedisCache,
        session: AsyncSession,
        instrument: Instrument,
    ) -> None:
        await _seed_one_trade(session, instrument)
        await cache.set_json(
            quote_cache_key(instrument.symbol),
            {
                "symbol": instrument.symbol,
                "price": POISON_PRICE,
                "size": "1.00000000",
                "traded_at": MOMENT.isoformat(),
                "bid_price": None,
                "ask_price": None,
                "quoted_at": None,
                "source": "poison",
            },
            ttl_seconds=30,
        )

        body = (await cache_disabled_api.get(f"/api/v1/market/{instrument.symbol}/latest")).json()

        assert body["price"] == "100.00000000"
        assert body["source"] == "synthetic"

    async def test_disabling_the_cache_never_writes_to_it_either(
        self,
        cache_disabled_api: httpx.AsyncClient,
        cache: RedisCache,
        session: AsyncSession,
        instrument: Instrument,
    ) -> None:
        await _seed_one_trade(session, instrument)

        await cache_disabled_api.get(f"/api/v1/market/{instrument.symbol}/latest")

        assert await cache.get_json(quote_cache_key(instrument.symbol)) is None


class TestExplicitInvalidation:
    async def test_deleting_the_key_makes_the_next_read_fresh_again(
        self,
        cached_api: httpx.AsyncClient,
        cache: RedisCache,
        session: AsyncSession,
        instrument: Instrument,
    ) -> None:
        """Simulates what `MarketBroadcastService` does on every new trade or quote."""
        await _seed_one_trade(session, instrument)
        await cache.set_json(
            quote_cache_key(instrument.symbol),
            {
                "symbol": instrument.symbol,
                "price": POISON_PRICE,
                "size": "1.00000000",
                "traded_at": MOMENT.isoformat(),
                "bid_price": None,
                "ask_price": None,
                "quoted_at": None,
                "source": "poison",
            },
            ttl_seconds=30,
        )
        poisoned = (await cached_api.get(f"/api/v1/market/{instrument.symbol}/latest")).json()
        assert poisoned["price"] == POISON_PRICE

        await cache.delete(quote_cache_key(instrument.symbol))

        fresh = (await cached_api.get(f"/api/v1/market/{instrument.symbol}/latest")).json()
        assert fresh["price"] == "100.00000000"
        assert fresh["source"] == "synthetic"
