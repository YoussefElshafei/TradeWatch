"""Alpaca, for real.

Skipped unless TW_PROVIDER_API_KEY and TW_PROVIDER_API_SECRET are set. See README.md in
this directory for why these are separated from everything else.

Deliberately shallow. They check that a connection can be made and that what comes back
normalizes into TradeWatch's own types — not that AAPL traded at any particular price,
because the market decides that and a test that fails on a public holiday is a test people
learn to ignore.
"""

from __future__ import annotations

import datetime as dt
import os
from collections.abc import AsyncIterator
from decimal import Decimal

import pytest

from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import TradeEvent
from tradewatch.infrastructure.providers.alpaca import AlpacaProvider, AlpacaSettings

pytestmark = [pytest.mark.provider, pytest.mark.slow]

API_KEY = os.environ.get("TW_PROVIDER_API_KEY", "")
API_SECRET = os.environ.get("TW_PROVIDER_API_SECRET", "")

requires_credentials = pytest.mark.skipif(
    not (API_KEY and API_SECRET),
    reason="set TW_PROVIDER_API_KEY and TW_PROVIDER_API_SECRET to run live provider tests",
)


@pytest.fixture
async def provider() -> AsyncIterator[AlpacaProvider]:
    settings = AlpacaSettings(api_key=API_KEY, api_secret=API_SECRET, timeout_seconds=15.0)
    adapter = AlpacaProvider(settings)
    yield adapter
    await adapter.aclose()


@requires_credentials
async def test_fetches_historical_bars(provider: AlpacaProvider) -> None:
    """A week back, so the window contains a trading day whatever day this runs."""
    end = dt.datetime.now(dt.UTC) - dt.timedelta(days=1)
    start = end - dt.timedelta(days=7)

    bars = await provider.fetch_bars("AAPL", BarInterval.ONE_DAY, start, end)

    assert bars, "no bars returned for a week of AAPL"
    for bar in bars:
        assert bar.symbol == "AAPL"
        assert isinstance(bar.open, Decimal)
        assert bar.low <= bar.open <= bar.high
        assert bar.low <= bar.close <= bar.high
        assert bar.bar_start.tzinfo is dt.UTC
        assert bar.source == "alpaca"


@requires_credentials
async def test_reports_a_current_price(provider: AlpacaProvider) -> None:
    snapshot = await provider.fetch_latest_price("AAPL")

    # None is legitimate if AAPL has genuinely never traded on the IEX feed, which would
    # itself be worth knowing.
    if snapshot is None:
        pytest.skip("no last trade available on this feed right now")

    assert snapshot.symbol == "AAPL"
    assert snapshot.price > 0
    assert snapshot.as_of.tzinfo is dt.UTC


@requires_credentials
async def test_lists_instruments(provider: AlpacaProvider) -> None:
    instruments = await provider.list_instruments()

    assert len(instruments) > 100, "the US equity universe should be large"
    symbols = {row.symbol for row in instruments}
    assert "AAPL" in symbols
    assert all(row.symbol == row.symbol.upper() for row in instruments)


@requires_credentials
async def test_streams_live_trades(provider: AlpacaProvider) -> None:
    """Needs the market to be open. Skips rather than fails when it is not."""
    received: list[TradeEvent] = []

    try:
        async for event in provider.stream_trades(["AAPL", "MSFT", "SPY"]):
            received.append(event)
            if len(received) >= 3:
                break
    except Exception as error:
        pytest.skip(f"live stream unavailable: {type(error).__name__}: {error}")

    if not received:
        pytest.skip("no trades arrived; the market is probably closed")

    for event in received:
        assert event.price > 0
        assert event.size > 0
        assert event.occurred_at.tzinfo is dt.UTC
        assert event.source == "alpaca"

    # The property the whole deduplication story depends on, checked against real data.
    ids = [event.event_id for event in received]
    assert len(ids) == len(set(ids))
