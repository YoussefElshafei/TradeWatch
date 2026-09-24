"""The end-to-end vertical slice.

```
SyntheticProvider → normalize → InMemoryEventBus → MarketDataProcessor → PostgreSQL → API
```

Every other test in the suite covers one link. This covers the joins, which is where a
system like this actually breaks: a field that normalizes fine and violates a constraint, a
timestamp that survives the bus and loses its timezone in the ORM, an id that deduplicates
in memory and duplicates in the database.

The assertion that matters most is **provenance**: the prices the API returns are checked
against the exact values the provider generated, event id by event id. Without that, a test
can pass on data that happened to already be there, which is the failure mode of every
end-to-end test that only counts rows.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncIterator, Callable
from decimal import Decimal

import httpx2 as httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.conftest import SessionBoundStore
from tradewatch.api.app import create_app
from tradewatch.api.dependencies import get_session
from tradewatch.api.routes.health import clear_readiness_checks
from tradewatch.application.ingestion import IngestionService
from tradewatch.application.ports.market_store import MarketDataStore
from tradewatch.application.processing import MarketDataProcessor
from tradewatch.config.settings import Settings
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import BarEvent, MarketEvent, TradeEvent
from tradewatch.infrastructure.cache import RedisCache
from tradewatch.infrastructure.events.memory import InMemoryEventBus
from tradewatch.infrastructure.providers.normalization import EventFactory
from tradewatch.infrastructure.providers.synthetic import SyntheticProvider, SyntheticScenario

pytestmark = pytest.mark.integration

#: A window in the past, so the API's default "ending now" window definitely contains it and
#: the test does not depend on what time it runs.
WINDOW_END = dt.datetime(2026, 3, 2, 16, 0, tzinfo=dt.UTC)
WINDOW_START = WINDOW_END - dt.timedelta(minutes=30)

SYMBOL = "SLICE1"


@pytest_asyncio.fixture
async def api(session: AsyncSession, settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    """An async ASGI client sharing the test's transaction and event loop.

    `app.state.cache` is attached but left unconnected — see the identical note in
    `tests/integration/test_market_api.py::api` for why.
    """
    clear_readiness_checks()
    app = create_app(settings)

    async def _session_override() -> AsyncSession:
        return session

    app.dependency_overrides[get_session] = _session_override
    app.state.cache = RedisCache(settings.redis)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
    app.dependency_overrides.clear()
    clear_readiness_checks()


def _advancing_clock(
    step: dt.timedelta = dt.timedelta(seconds=1),
) -> Callable[[], dt.datetime]:
    """A clock that moves forward one step per call, like a real feed's timestamps."""
    state = {"now": WINDOW_START}

    def tick() -> dt.datetime:
        state["now"] = state["now"] + step
        return state["now"]

    return tick


@pytest.fixture
def provider() -> SyntheticProvider:
    """A seeded provider on a frozen clock, so the whole run is reproducible."""
    return SyntheticProvider(
        [SYMBOL],
        seed=4242,
        factory=EventFactory(source="synthetic", producer="slice-test", clock=lambda: WINDOW_END),
    )


async def run_slice(
    provider: SyntheticProvider,
    store: MarketDataStore,
    *,
    trades: int = 0,
    bars: bool = False,
) -> tuple[IngestionService, MarketDataProcessor, list[MarketEvent]]:
    """Drive provider → bus → processor, returning what the provider produced.

    The published events are returned so the test can compare what came out of the API
    against what went in, rather than against a hard-coded expectation that would drift.
    """
    bus = InMemoryEventBus()
    ingestion = IngestionService(provider, bus)
    processor = MarketDataProcessor(store, batch_size=50)

    if bars:
        await ingestion.backfill_bars(SYMBOL, BarInterval.ONE_MINUTE, WINDOW_START, WINDOW_END)
    if trades:
        await ingestion.run_stream([SYMBOL], max_events=trades)

    published = await bus.drain()
    for event in published:
        await processor.handle(event)
    await processor.flush()

    return ingestion, processor, published


class TestTheCompletePath:
    async def test_bars_travel_from_provider_to_api(
        self, session: AsyncSession, api: httpx.AsyncClient, provider: SyntheticProvider
    ) -> None:
        """The whole slice, with provenance checked value by value.

        Not a row count: the exact OHLCV the provider generated has to come back out of the
        API, or the test would pass on data that happened to already be there.
        """
        _, processor, published = await run_slice(provider, SessionBoundStore(session), bars=True)
        generated = {event.bar_start: event for event in published if isinstance(event, BarEvent)}
        assert len(generated) == 30, "the provider did not generate the expected window"
        assert processor.stats.persisted == 30

        response = await api.get(
            f"/api/v1/market/{SYMBOL}/bars",
            params={
                "interval": "1m",
                "start": WINDOW_START.isoformat(),
                "end": WINDOW_END.isoformat(),
                "limit": 100,
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["count"] == 30

        for row in body["bars"]:
            moment = dt.datetime.fromisoformat(row["bar_start"].replace("Z", "+00:00"))
            source = generated[moment]
            assert Decimal(row["open"]) == source.open
            assert Decimal(row["high"]) == source.high
            assert Decimal(row["low"]) == source.low
            assert Decimal(row["close"]) == source.close
            assert Decimal(row["volume"]) == source.volume
            assert row["trade_count"] == source.trade_count

    async def test_trades_travel_from_provider_to_the_latest_price_endpoint(
        self, session: AsyncSession, api: httpx.AsyncClient
    ) -> None:
        """A ticking clock, not a frozen one.

        With every trade sharing a timestamp, "the newest" is whatever the database happens
        to return, and the test would be asserting on a tie-break rather than on the path.
        """
        ticking = SyntheticProvider(
            [SYMBOL],
            seed=4242,
            factory=EventFactory(
                source="synthetic", producer="slice-test", clock=_advancing_clock()
            ),
        )
        _, processor, published = await run_slice(ticking, SessionBoundStore(session), trades=25)
        trades = [event for event in published if isinstance(event, TradeEvent)]
        assert len(trades) == 25
        assert processor.stats.persisted == 25

        body = (await api.get(f"/api/v1/market/{SYMBOL}/latest")).json()

        newest = max(trades, key=lambda event: event.occurred_at)
        assert Decimal(body["price"]) == newest.price
        assert body["source"] == "synthetic"

    async def test_the_symbol_appears_in_the_instrument_list(
        self, session: AsyncSession, api: httpx.AsyncClient, provider: SyntheticProvider
    ) -> None:
        """The processor registers a symbol it has never seen, so the selector is populated.

        Without this the frontend would have a chart and no way to choose what it charts.
        """
        await run_slice(provider, SessionBoundStore(session), trades=5)

        body = (await api.get("/api/v1/instruments")).json()

        assert SYMBOL in {row["symbol"] for row in body["instruments"]}

    async def test_prices_are_exact_from_end_to_end(
        self, session: AsyncSession, api: httpx.AsyncClient, provider: SyntheticProvider
    ) -> None:
        """ADR-0009 across every hop: provider → event → ORM → NUMERIC → JSON string.

        A float anywhere on that path would show up here as a value that no longer compares
        equal to the one the provider produced.
        """
        _, _, published = await run_slice(provider, SessionBoundStore(session), bars=True)
        source = min(
            (event for event in published if isinstance(event, BarEvent)),
            key=lambda event: event.bar_start,
        )

        body = (
            await api.get(
                f"/api/v1/market/{SYMBOL}/bars",
                params={
                    "start": WINDOW_START.isoformat(),
                    "end": WINDOW_END.isoformat(),
                    "limit": 1,
                },
            )
        ).json()

        returned = body["bars"][0]
        assert isinstance(returned["close"], str), "money left as a JSON number"
        assert Decimal(returned["close"]) == source.close

    async def test_timestamps_stay_utc_from_end_to_end(
        self, session: AsyncSession, api: httpx.AsyncClient, provider: SyntheticProvider
    ) -> None:
        await run_slice(provider, SessionBoundStore(session), bars=True)

        body = (
            await api.get(
                f"/api/v1/market/{SYMBOL}/bars",
                params={"start": WINDOW_START.isoformat(), "end": WINDOW_END.isoformat()},
            )
        ).json()

        for row in body["bars"]:
            parsed = dt.datetime.fromisoformat(row["bar_start"].replace("Z", "+00:00"))
            assert parsed.tzinfo is not None
            assert parsed.utcoffset() == dt.timedelta(0)


class TestSliceIdempotency:
    async def test_running_the_pipeline_twice_stores_each_bar_once(
        self, session: AsyncSession, api: httpx.AsyncClient
    ) -> None:
        """A restart, or an overlapping backfill, must not duplicate history.

        The whole chain of guarantees is exercised here: deterministic event ids
        (ADR-0012), the provider's addressable price path, and `UNIQUE`/upsert in the
        schema. Any one of them missing shows up as 60 bars instead of 30.
        """
        store = SessionBoundStore(session)

        for _ in range(2):
            provider = SyntheticProvider(
                [SYMBOL],
                seed=4242,
                factory=EventFactory(
                    source="synthetic", producer="slice-test", clock=lambda: WINDOW_END
                ),
            )
            await run_slice(provider, store, bars=True)

        body = (
            await api.get(
                f"/api/v1/market/{SYMBOL}/bars",
                params={
                    "start": WINDOW_START.isoformat(),
                    "end": WINDOW_END.isoformat(),
                    "limit": 200,
                },
            )
        ).json()

        assert body["count"] == 30, "the second run duplicated history"

    async def test_a_duplicate_from_the_provider_is_stored_once(
        self, session: AsyncSession, api: httpx.AsyncClient
    ) -> None:
        """The provider repeats a print; the deduplicator and the constraint both see it."""
        provider = SyntheticProvider(
            [SYMBOL],
            seed=99,
            scenario=SyntheticScenario(duplicate_at=frozenset({2, 5})),
            factory=EventFactory(
                source="synthetic", producer="slice-test", clock=lambda: WINDOW_END
            ),
        )
        ingestion, processor, published = await run_slice(
            provider, SessionBoundStore(session), trades=10
        )

        assert ingestion.stats.duplicates == 2, "ingestion did not catch the repeats"
        assert processor.stats.persisted == 10
        assert len({event.event_id for event in published}) == 10


class TestSliceFailures:
    async def test_a_malformed_event_does_not_stop_the_slice(
        self, session: AsyncSession, api: httpx.AsyncClient, provider: SyntheticProvider
    ) -> None:
        """Failure F3, end to end: the good data either side of a bad event still arrives."""
        bus = InMemoryEventBus()
        ingestion = IngestionService(provider, bus)
        processor = MarketDataProcessor(SessionBoundStore(session), batch_size=5)

        await ingestion.backfill_bars(SYMBOL, BarInterval.ONE_MINUTE, WINDOW_START, WINDOW_END)
        events = await bus.drain()

        class Unstorable(MarketEvent):
            pass

        broken = Unstorable(
            event_id=events[0].event_id,
            event_type="market.trade",  # type: ignore[arg-type]
            symbol=SYMBOL,
            occurred_at=WINDOW_END,
            ingested_at=WINDOW_END,
            source="synthetic",
            producer="slice-test",
            correlation_id=events[0].correlation_id,
        )

        for event in [*events[:10], broken, *events[10:]]:
            await processor.handle(event)
        await processor.flush()

        body = (
            await api.get(
                f"/api/v1/market/{SYMBOL}/bars",
                params={
                    "start": WINDOW_START.isoformat(),
                    "end": WINDOW_END.isoformat(),
                    "limit": 100,
                },
            )
        ).json()

        assert processor.stats.unprocessable == 1
        assert body["count"] == 30, "good events were lost alongside the bad one"

    async def test_an_empty_window_reaches_the_api_as_an_empty_series(
        self, session: AsyncSession, api: httpx.AsyncClient, provider: SyntheticProvider
    ) -> None:
        """The state the frontend renders as "no data yet" rather than as an error."""
        await run_slice(provider, SessionBoundStore(session), bars=True)

        response = await api.get(
            f"/api/v1/market/{SYMBOL}/bars",
            params={"start": "2019-01-01T00:00:00Z", "end": "2019-01-02T00:00:00Z"},
        )

        assert response.status_code == 200
        assert response.json()["count"] == 0
