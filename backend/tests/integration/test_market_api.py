"""The market data API, against a real database.

These are integration tests rather than in-process API tests because the endpoints exist to
read stored data — mocking the repository away would leave nothing worth asserting.

The session dependency is overridden so the API reads through the test's own transaction,
which is rolled back afterwards. That is what lets a test write rows and immediately query
them over HTTP without committing anything.

Most of the assertions are about **failure**, because the difference between a good market
data API and a bad one is almost entirely in how it answers when there is nothing to return:
an unknown symbol, an empty window, and a reversed range are three different situations and
a client can only act on them if they arrive as three different responses.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncIterator
from decimal import Decimal

import httpx2 as httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.api.app import create_app
from tradewatch.api.dependencies import get_session
from tradewatch.api.routes.health import clear_readiness_checks
from tradewatch.config.settings import Settings
from tradewatch.domain.entities import NewBar, NewTrade
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.database.models import Instrument
from tradewatch.infrastructure.repositories import BarRepository, TradeRepository

pytestmark = pytest.mark.integration

MOMENT = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)


@pytest_asyncio.fixture
async def api(session: AsyncSession, settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    """A client whose requests run inside the test's own transaction and event loop.

    Two things here are deliberate and both were learned the hard way.

    **An async ASGI client, not `TestClient`.** `TestClient` drives the app from a worker
    thread with its own event loop. The asyncpg connection behind `session` belongs to the
    test's loop, and using it from another produces `InterfaceError` and pending-task
    warnings rather than anything that names the real problem.

    **`get_session` is overridden** rather than pointing the app at the test database,
    because the fixtures never commit. The API has to join the *same* transaction to see
    what a test just wrote — and everything is rolled back afterwards.
    """
    clear_readiness_checks()
    app = create_app(settings)

    async def _session_override() -> AsyncSession:
        return session

    app.dependency_overrides[get_session] = _session_override

    # The lifespan is deliberately not run: it would open a second engine against the
    # configured database, which the override makes unnecessary.
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    app.dependency_overrides.clear()
    clear_readiness_checks()


async def seed_bars(
    session: AsyncSession, instrument: Instrument, *, count: int = 10, start: dt.datetime = MOMENT
) -> list[NewBar]:
    bars = [
        NewBar(
            instrument_id=instrument.id,
            interval=BarInterval.ONE_MINUTE,
            bar_start=start + dt.timedelta(minutes=index),
            open=Decimal("100") + index,
            high=Decimal("101") + index,
            low=Decimal("99") + index,
            close=Decimal("100.5") + index,
            volume=Decimal("1000"),
            vwap=Decimal("100.2") + index,
            trade_count=42,
        )
        for index in range(count)
    ]
    await BarRepository(session).upsert_many(bars)
    return bars


class TestInstruments:
    async def test_lists_instruments(
        self, api: httpx.AsyncClient, session: AsyncSession, instrument: Instrument
    ) -> None:
        response = await api.get("/api/v1/instruments")

        assert response.status_code == 200
        body = response.json()
        assert body["count"] >= 1
        assert instrument.symbol in {row["symbol"] for row in body["instruments"]}

    async def test_an_empty_universe_is_not_an_error(self, api: httpx.AsyncClient) -> None:
        """A fresh database has no instruments. That is a state, not a failure."""
        response = await api.get("/api/v1/instruments")

        assert response.status_code == 200
        assert response.json()["count"] == 0

    async def test_money_fields_are_strings(
        self, api: httpx.AsyncClient, instrument: Instrument
    ) -> None:
        """ADR-0009's last hop: a JSON number would be rounded by the browser's parser."""
        body = (await api.get("/api/v1/instruments")).json()

        row = next(r for r in body["instruments"] if r["symbol"] == instrument.symbol)
        assert isinstance(row["tick_size"], str)
        assert isinstance(row["lot_size"], str)

    async def test_looks_up_one_instrument(
        self, api: httpx.AsyncClient, instrument: Instrument
    ) -> None:
        response = await api.get(f"/api/v1/instruments/{instrument.symbol.lower()}")

        assert response.status_code == 200
        assert response.json()["symbol"] == instrument.symbol

    async def test_an_unknown_instrument_is_404(self, api: httpx.AsyncClient) -> None:
        response = await api.get("/api/v1/instruments/NOSUCH")

        assert response.status_code == 404
        assert response.json()["error"]["code"] == "not_found"


class TestBars:
    async def test_returns_stored_bars(
        self, api: httpx.AsyncClient, session: AsyncSession, instrument: Instrument
    ) -> None:
        await seed_bars(session, instrument, count=5)

        response = await api.get(
            f"/api/v1/market/{instrument.symbol}/bars",
            params={
                "interval": "1m",
                "start": MOMENT.isoformat(),
                "end": (MOMENT + dt.timedelta(hours=1)).isoformat(),
            },
        )

        assert response.status_code == 200
        body = response.json()
        assert body["symbol"] == instrument.symbol
        assert body["interval"] == "1m"
        assert body["count"] == 5
        assert len(body["bars"]) == 5

    async def test_bars_come_back_oldest_first(
        self, api: httpx.AsyncClient, session: AsyncSession, instrument: Instrument
    ) -> None:
        """The order a chart draws and a backtest replays."""
        await seed_bars(session, instrument, count=6)

        body = (
            await api.get(
                f"/api/v1/market/{instrument.symbol}/bars",
                params={
                    "start": MOMENT.isoformat(),
                    "end": (MOMENT + dt.timedelta(hours=1)).isoformat(),
                },
            )
        ).json()

        starts = [row["bar_start"] for row in body["bars"]]
        assert starts == sorted(starts)

    async def test_prices_survive_as_exact_strings(
        self, api: httpx.AsyncClient, session: AsyncSession, instrument: Instrument
    ) -> None:
        await seed_bars(session, instrument, count=1)

        body = (
            await api.get(
                f"/api/v1/market/{instrument.symbol}/bars",
                params={
                    "start": MOMENT.isoformat(),
                    "end": (MOMENT + dt.timedelta(hours=1)).isoformat(),
                },
            )
        ).json()

        first = body["bars"][0]
        for field in ("open", "high", "low", "close", "volume"):
            assert isinstance(first[field], str), f"{field} is not a string"
        assert Decimal(first["open"]) == Decimal("100")

    async def test_a_symbol_is_case_insensitive(
        self, api: httpx.AsyncClient, session: AsyncSession, instrument: Instrument
    ) -> None:
        await seed_bars(session, instrument, count=2)

        response = await api.get(
            f"/api/v1/market/{instrument.symbol.lower()}/bars",
            params={
                "start": MOMENT.isoformat(),
                "end": (MOMENT + dt.timedelta(hours=1)).isoformat(),
            },
        )

        assert response.status_code == 200
        assert response.json()["symbol"] == instrument.symbol

    async def test_the_window_is_half_open(
        self, api: httpx.AsyncClient, session: AsyncSession, instrument: Instrument
    ) -> None:
        """Consecutive requests must tile without repeating the boundary bar.

        An off-by-one here draws a doubled candle on the chart.
        """
        await seed_bars(session, instrument, count=10)

        body = (
            await api.get(
                f"/api/v1/market/{instrument.symbol}/bars",
                params={
                    "start": (MOMENT + dt.timedelta(minutes=2)).isoformat(),
                    "end": (MOMENT + dt.timedelta(minutes=5)).isoformat(),
                },
            )
        ).json()

        starts = [row["bar_start"] for row in body["bars"]]
        assert len(starts) == 3
        assert (MOMENT + dt.timedelta(minutes=5)).isoformat().replace("+00:00", "Z") not in starts

    async def test_an_empty_window_is_200_not_404(
        self, api: httpx.AsyncClient, instrument: Instrument
    ) -> None:
        """A known symbol with no data is a quiet market, not a missing resource.

        Conflating the two is the most common mistake in a market data API, and it makes
        every client build a workaround.
        """
        response = await api.get(
            f"/api/v1/market/{instrument.symbol}/bars",
            params={
                "start": "2020-01-01T00:00:00Z",
                "end": "2020-01-02T00:00:00Z",
            },
        )

        assert response.status_code == 200
        assert response.json() == {
            "symbol": instrument.symbol,
            "interval": "1m",
            "start": "2020-01-01T00:00:00Z",
            "end": "2020-01-02T00:00:00Z",
            "count": 0,
            "bars": [],
        }

    async def test_an_unknown_symbol_is_404(self, api: httpx.AsyncClient) -> None:
        response = await api.get("/api/v1/market/NOSUCH/bars")

        assert response.status_code == 404
        assert "NOSUCH" in response.json()["error"]["message"]

    async def test_a_reversed_range_is_422(
        self, api: httpx.AsyncClient, instrument: Instrument
    ) -> None:
        response = await api.get(
            f"/api/v1/market/{instrument.symbol}/bars",
            params={"start": "2026-02-01T00:00:00Z", "end": "2026-01-01T00:00:00Z"},
        )

        assert response.status_code == 422
        assert "must be after" in response.json()["error"]["message"]

    async def test_a_naive_timestamp_is_422(
        self, api: httpx.AsyncClient, instrument: Instrument
    ) -> None:
        """Without an offset the server would have to guess a timezone, and guessing wrong
        silently shifts every bar by hours."""
        response = await api.get(
            f"/api/v1/market/{instrument.symbol}/bars",
            params={"start": "2026-01-01T00:00:00", "end": "2026-01-02T00:00:00"},
        )

        assert response.status_code == 422
        assert "timezone" in response.json()["error"]["message"]

    @pytest.mark.parametrize("limit", [0, -1, 99_999])
    async def test_an_out_of_range_limit_is_422(
        self, api: httpx.AsyncClient, instrument: Instrument, limit: int
    ) -> None:
        response = await api.get(
            f"/api/v1/market/{instrument.symbol}/bars", params={"limit": limit}
        )

        assert response.status_code == 422

    async def test_an_unparseable_date_is_422(
        self, api: httpx.AsyncClient, instrument: Instrument
    ) -> None:
        response = await api.get(
            f"/api/v1/market/{instrument.symbol}/bars", params={"start": "yesterday"}
        )

        assert response.status_code == 422
        assert response.json()["error"]["code"] == "validation_error"

    async def test_an_unknown_interval_is_422(
        self, api: httpx.AsyncClient, instrument: Instrument
    ) -> None:
        response = await api.get(
            f"/api/v1/market/{instrument.symbol}/bars", params={"interval": "7s"}
        )

        assert response.status_code == 422

    async def test_the_limit_caps_the_result(
        self, api: httpx.AsyncClient, session: AsyncSession, instrument: Instrument
    ) -> None:
        await seed_bars(session, instrument, count=20)

        body = (
            await api.get(
                f"/api/v1/market/{instrument.symbol}/bars",
                params={
                    "start": MOMENT.isoformat(),
                    "end": (MOMENT + dt.timedelta(hours=1)).isoformat(),
                    "limit": 5,
                },
            )
        ).json()

        assert body["count"] == 5

    async def test_the_default_window_is_echoed_back(
        self, api: httpx.AsyncClient, instrument: Instrument
    ) -> None:
        """A client that cannot see the resolved window has to guess where to page next."""
        body = (await api.get(f"/api/v1/market/{instrument.symbol}/bars")).json()

        assert body["start"] < body["end"]


class TestLatestPrice:
    async def test_reports_the_most_recent_trade(
        self, api: httpx.AsyncClient, session: AsyncSession, instrument: Instrument
    ) -> None:
        await TradeRepository(session).bulk_insert(
            [
                NewTrade(
                    event_id=new_id(),
                    instrument_id=instrument.id,
                    price=Decimal("100") + index,
                    size=Decimal("10"),
                    occurred_at=MOMENT + dt.timedelta(minutes=index),
                    ingested_at=MOMENT,
                    provider="synthetic",
                )
                for index in range(3)
            ]
        )

        body = (await api.get(f"/api/v1/market/{instrument.symbol}/latest")).json()

        assert body["price"] == "102.00000000"
        assert body["source"] == "synthetic"
        assert body["traded_at"].startswith("2026-03-02T14:32")

    async def test_a_symbol_with_no_data_returns_nulls_not_404(
        self, api: httpx.AsyncClient, instrument: Instrument
    ) -> None:
        """A registered instrument that has not traded is a real answer."""
        response = await api.get(f"/api/v1/market/{instrument.symbol}/latest")

        assert response.status_code == 200
        body = response.json()
        assert body["price"] is None
        assert body["traded_at"] is None

    async def test_an_unknown_symbol_is_404(self, api: httpx.AsyncClient) -> None:
        assert (await api.get("/api/v1/market/NOSUCH/latest")).status_code == 404


class TestApiContract:
    async def test_the_endpoints_are_in_the_openapi_schema(self, api: httpx.AsyncClient) -> None:
        """The schema is what the frontend's types are generated from in Phase 2."""
        paths = (await api.get("/openapi.json")).json()["paths"]

        assert "/api/v1/instruments" in paths
        assert "/api/v1/market/{symbol}/bars" in paths
        assert "/api/v1/market/{symbol}/latest" in paths

    async def test_errors_use_the_shared_envelope(self, api: httpx.AsyncClient) -> None:
        body = (await api.get("/api/v1/market/NOSUCH/bars")).json()

        assert set(body) == {"error"}
        assert {"code", "message", "request_id"} <= set(body["error"])

    async def test_there_is_no_route_that_writes_market_data(self, api: httpx.AsyncClient) -> None:
        """`ARCHITECTURE.md` §3.9: the write path exists only inside the trust boundary."""
        paths = (await api.get("/openapi.json")).json()["paths"]

        for path, operations in paths.items():
            if not path.startswith("/api/v1/market"):
                continue
            assert set(operations) <= {"get"}, f"{path} exposes a write method"
