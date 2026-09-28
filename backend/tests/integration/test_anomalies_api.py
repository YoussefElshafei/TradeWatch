"""`GET /api/v1/anomalies` against a real database.

Global, not owner-scoped (see `api/routes/anomalies.py`'s docstring for why) — there is no
ownership to test here, unlike every owner-scoped resource elsewhere in this API. What matters
is that the feed is queryable cross-symbol and per-symbol, that an unknown symbol is a 404
(not an empty list — the two mean different things), and that a stored detection comes back
with everything ADR-0008 requires for it to be explainable.
"""

from __future__ import annotations

import datetime as dt
import uuid
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
from tradewatch.domain.enums import AlertSeverity
from tradewatch.domain.ids import new_id
from tradewatch.infrastructure.cache import RedisCache
from tradewatch.infrastructure.database.models import AnomalyDetection, Instrument

pytestmark = pytest.mark.integration

_START = dt.datetime(2026, 6, 1, 14, 30, tzinfo=dt.UTC)


@pytest_asyncio.fixture
async def api(session: AsyncSession, settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    """See `test_market_api.py`'s `api` fixture for why each piece here is the way it is."""
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


async def _seed_detection(
    session: AsyncSession, instrument: Instrument, *, minute: int = 0, score: str = "5.0"
) -> AnomalyDetection:
    window_start = _START + dt.timedelta(minutes=minute)
    window_end = window_start + dt.timedelta(minutes=1)
    row = AnomalyDetection(
        id=new_id(),
        event_id=uuid.uuid4(),
        instrument_id=instrument.id,
        detected_at=window_end,
        window_start=window_start,
        window_end=window_end,
        anomaly_score=Decimal(score),
        threshold=Decimal("3.5"),
        severity=AlertSeverity.CRITICAL,
        model_name="baseline-zscore",
        model_version="window=30",
        features={"return": "0.5", "volatility": "0.1"},
    )
    session.add(row)
    await session.flush()
    return row


class TestListAnomalies:
    async def test_the_cross_symbol_feed_returns_a_stored_detection(
        self, api: httpx.AsyncClient, session: AsyncSession, instrument: Instrument
    ) -> None:
        await _seed_detection(session, instrument)

        response = await api.get("/api/v1/anomalies")

        assert response.status_code == 200
        body = response.json()
        assert body["count"] == 1
        detection = body["detections"][0]
        assert detection["symbol"] == instrument.symbol
        assert detection["model_name"] == "baseline-zscore"
        assert detection["severity"] == "critical"
        assert detection["anomaly_score"] == "5.00000000"  # Numeric(18, 8) on the row
        # Explainable: the features are on the response itself, not just the score.
        assert detection["features"] == {"return": "0.5", "volatility": "0.1"}

    async def test_newest_first(
        self, api: httpx.AsyncClient, session: AsyncSession, instrument: Instrument
    ) -> None:
        await _seed_detection(session, instrument, minute=0)
        await _seed_detection(session, instrument, minute=10)

        response = await api.get("/api/v1/anomalies")

        body = response.json()
        assert body["count"] == 2
        timestamps = [row["window_start"] for row in body["detections"]]
        assert timestamps == sorted(timestamps, reverse=True)

    async def test_filtering_by_symbol_excludes_other_symbols(
        self,
        api: httpx.AsyncClient,
        session: AsyncSession,
        instrument: Instrument,
        instrument_factory: object,
    ) -> None:
        other = instrument_factory()  # type: ignore[operator]
        session.add(other)
        await session.flush()
        await _seed_detection(session, instrument)
        await _seed_detection(session, other)

        response = await api.get("/api/v1/anomalies", params={"symbol": instrument.symbol})

        body = response.json()
        assert body["count"] == 1
        assert body["detections"][0]["symbol"] == instrument.symbol

    async def test_an_unknown_symbol_is_a_404_not_an_empty_list(
        self, api: httpx.AsyncClient
    ) -> None:
        response = await api.get("/api/v1/anomalies", params={"symbol": "NOSUCHSYMBOL"})

        assert response.status_code == 404

    async def test_a_symbol_with_no_detections_is_an_empty_list_not_a_404(
        self, api: httpx.AsyncClient, instrument: Instrument
    ) -> None:
        response = await api.get("/api/v1/anomalies", params={"symbol": instrument.symbol})

        assert response.status_code == 200
        assert response.json() == {"count": 0, "detections": []}

    async def test_limit_bounds_the_page_size(
        self, api: httpx.AsyncClient, session: AsyncSession, instrument: Instrument
    ) -> None:
        for minute in range(5):
            await _seed_detection(session, instrument, minute=minute)

        response = await api.get("/api/v1/anomalies", params={"limit": 2})

        assert response.json()["count"] == 2

    async def test_no_authentication_is_required(self, api: httpx.AsyncClient) -> None:
        """Market data, like `market.py`'s own routes — no `Authorization` header at all."""
        response = await api.get("/api/v1/anomalies")

        assert response.status_code == 200
