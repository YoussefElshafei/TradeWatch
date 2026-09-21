"""The Alpaca adapter, driven entirely by fakes.

Not one test here touches the network. The HTTP client is an `httpx.MockTransport` and the
WebSocket is a scripted object, so the whole adapter — normalization, pagination, HTTP
status mapping, authentication handshake, frame decoding — is exercised deterministically
and offline. The tests that *do* need credentials live in `tests/provider/` and are marked
`provider`, so they are impossible to run by accident.

The payloads below are built from Alpaca's documented Market Data API v2 shapes. That is a
real limitation, and it is the one worth stating: these tests prove the adapter handles the
documented format correctly, not that the documented format is what the live service sends.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

import httpx2 as httpx
import pytest

from tradewatch.application.ports.market_data import MarketDataProvider
from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import QuoteEvent, TradeEvent
from tradewatch.infrastructure.providers.alpaca import AlpacaProvider, AlpacaSettings
from tradewatch.infrastructure.providers.errors import (
    ProviderAuthError,
    ProviderConnectionError,
    ProviderDataError,
    ProviderError,
    ProviderRateLimitError,
    ProviderRequestError,
    ProviderTimeoutError,
    ProviderUnavailableError,
)
from tradewatch.infrastructure.providers.normalization import EventFactory
from tradewatch.infrastructure.providers.resilience import RetryPolicy

pytestmark = pytest.mark.unit

MOMENT = dt.datetime(2026, 3, 2, 14, 30, tzinfo=dt.UTC)

# --- Recorded-shape payloads ------------------------------------------------

BAR_PAYLOAD = {
    "bars": {
        "AAPL": [
            {
                "t": "2026-03-02T14:30:00Z",
                "o": 187.1,
                "h": 187.5,
                "l": 186.9,
                "c": 187.3,
                "v": 12345,
                "n": 100,
                "vw": 187.2,
            },
            {
                "t": "2026-03-02T14:31:00Z",
                "o": 187.3,
                "h": 187.8,
                "l": 187.2,
                "c": 187.6,
                "v": 9876,
                "n": 84,
                "vw": 187.5,
            },
        ]
    },
    "next_page_token": None,
}

LATEST_TRADE_PAYLOAD = {
    "symbol": "AAPL",
    "trade": {
        "t": "2026-03-02T14:30:00.123456Z",
        "x": "V",
        "p": 187.42,
        "s": 100,
        "c": ["@"],
        "i": 998877,
        "z": "C",
    },
}

STREAM_TRADE = {
    "T": "t",
    "S": "AAPL",
    "i": 998877,
    "x": "V",
    "p": 187.42,
    "s": 100,
    "c": ["@", "F"],
    "t": "2026-03-02T14:30:00.123456Z",
    "z": "C",
}

SNAPSHOT_PAYLOAD = {
    "symbol": "AAPL",
    "latestTrade": {
        "t": "2026-03-02T14:30:00.123456Z",
        "p": 187.42,
        "s": 100,
        "i": 12345,
        "c": ["@"],
    },
    "latestQuote": {
        "t": "2026-03-02T14:30:00.200000Z",
        "bp": 187.40,
        "bs": 3,
        "ap": 187.44,
        "as": 2,
    },
}

STREAM_QUOTE = {
    "T": "q",
    "S": "AAPL",
    "bx": "V",
    "bp": 187.40,
    "bs": 3,
    "ax": "V",
    "ap": 187.44,
    "as": 2,
    "c": ["R"],
    "t": "2026-03-02T14:30:00.123456Z",
    "z": "C",
}


def make_settings(**overrides: Any) -> AlpacaSettings:
    defaults: dict[str, Any] = {
        "api_key": "test-key-not-real",
        "api_secret": "test-secret-not-real",
        "timeout_seconds": 1.0,
    }
    return AlpacaSettings(**(defaults | overrides))


def make_provider(
    handler: Any = None,
    *,
    connect: Any = None,
    retry_policy: RetryPolicy | None = None,
) -> AlpacaProvider:
    """An adapter wired to a fake transport and a clock that never really sleeps."""
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler)) if handler else None

    async def instant_sleep(_seconds: float) -> None:
        return None

    return AlpacaProvider(
        make_settings(),
        client=client,
        connect=connect,
        factory=EventFactory(source="alpaca", producer="test", clock=lambda: MOMENT),
        retry_policy=retry_policy or RetryPolicy(max_attempts=3, base_delay=0.0001, jitter=False),
        sleep=instant_sleep,
    )


def json_response(payload: Any, status: int = 200, headers: dict[str, str] | None = None) -> Any:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=payload, headers=headers or {})

    return handler


class ConnectionClosedOK(Exception):  # noqa: N818 - mirrors websockets' own class name
    """Stands in for `websockets.exceptions.ConnectionClosedOK`.

    Matched by *name* by the adapter, which is how it tells a clean close from a broken
    one without importing `websockets` purely to catch two classes.
    """


class FakeSocket:
    """A scripted WebSocket, behaving like the real library on every path.

    Frames are served in order from `inbound`; anything sent is captured in `sent`, so the
    authentication and subscription handshake can be asserted on.

    **Exhaustion raises `ConnectionClosedOK`, the same as a real socket closing cleanly.**
    An earlier version raised `ConnectionError` from `recv()` and `StopAsyncIteration` from
    `__anext__`, and the adapter read through `__anext__` — so the two disagreed and nothing
    noticed. That inconsistency is why a much more serious bug went unseen: the adapter
    opened a fresh socket per stream, which on Alpaca's free tier displaces the previous
    connection, and no test that drives one stream at a time can see it. There is now one
    exhaustion behaviour, and `TestMultiplexing` drives both streams together.
    """

    def __init__(self, inbound: Sequence[Any]) -> None:
        self._inbound = list(inbound)
        self.sent: list[dict[str, Any]] = []
        self.closed = False

    async def send(self, payload: str) -> None:
        self.sent.append(json.loads(payload))

    async def recv(self) -> Any:
        if not self._inbound:
            raise ConnectionClosedOK("socket closed")
        frame = self._inbound.pop(0)
        if isinstance(frame, Exception):
            raise frame
        return frame

    async def close(self) -> None:
        self.closed = True


class ControlledSocket(FakeSocket):
    """A socket that delivers frames only when the test says so.

    `FakeSocket` runs out of frames and closes, which is right for most tests and useless
    for the ones about *timing* — a subscriber attaching while another is mid-stream. Here
    `recv()` waits on a queue, so the connection stays open for exactly as long as the test
    wants it to.
    """

    def __init__(self, inbound: Sequence[Any]) -> None:
        super().__init__(inbound)
        self.pending: asyncio.Queue[Any] = asyncio.Queue()

    async def recv(self) -> Any:
        if self._inbound:
            frame = self._inbound.pop(0)
            if isinstance(frame, Exception):
                raise frame
            return frame
        frame = await self.pending.get()
        if isinstance(frame, Exception):
            raise frame
        return frame

    def deliver(self, frame: Any) -> None:
        self.pending.put_nowait(frame)


def controlled_socket_factory(handshake: Sequence[Any]) -> tuple[Any, list[ControlledSocket]]:
    """A connector handing out sockets the test drives frame by frame."""
    created: list[ControlledSocket] = []

    async def connect(_url: str) -> ControlledSocket:
        socket = ControlledSocket(handshake)
        created.append(socket)
        return socket

    return connect, created


def socket_factory(inbound: Sequence[Any]) -> tuple[Any, list[FakeSocket]]:
    """A connector that records every socket it hands out."""
    created: list[FakeSocket] = []

    async def connect(_url: str) -> FakeSocket:
        socket = FakeSocket(inbound)
        created.append(socket)
        return socket

    return connect, created


HANDSHAKE = [
    json.dumps([{"T": "success", "msg": "connected"}]),
    json.dumps([{"T": "success", "msg": "authenticated"}]),
    json.dumps([{"T": "subscription", "trades": ["AAPL"], "quotes": []}]),
]


class TestPortConformance:
    def test_satisfies_the_port(self) -> None:
        assert isinstance(make_provider(), MarketDataProvider)

    def test_declares_its_capabilities(self) -> None:
        capabilities = make_provider().capabilities

        assert capabilities.streaming_trades
        assert capabilities.historical_bars
        assert capabilities.supports_interval(BarInterval.ONE_MINUTE)
        assert capabilities.rate_limit_per_minute == 200

    def test_requires_both_halves_of_the_key_pair(self) -> None:
        with pytest.raises(ProviderAuthError, match="TW_PROVIDER_API_KEY"):
            AlpacaSettings(api_key="only-a-key", api_secret="")


class TestFetchBars:
    async def test_normalizes_a_valid_response(self) -> None:
        provider = make_provider(json_response(BAR_PAYLOAD))

        bars = await provider.fetch_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
        )

        assert len(bars) == 2
        first = bars[0]
        assert first.symbol == "AAPL"
        assert first.open == Decimal("187.1")
        assert first.close == Decimal("187.3")
        assert first.volume == Decimal("12345")
        assert first.trade_count == 100
        assert first.vwap == Decimal("187.2")
        assert first.bar_start == MOMENT
        assert first.source == "alpaca"

    async def test_prices_are_exact_decimals(self) -> None:
        """ADR-0009: JSON floats must not carry their binary error into the ledger."""
        provider = make_provider(json_response(BAR_PAYLOAD))

        bars = await provider.fetch_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
        )

        assert isinstance(bars[0].open, Decimal)
        assert bars[0].open == Decimal("187.1")
        assert bars[0].open != Decimal(187.1)  # noqa: RUF032 — the point of the test

    async def test_follows_pagination(self) -> None:
        pages = [
            {"bars": {"AAPL": [BAR_PAYLOAD["bars"]["AAPL"][0]]}, "next_page_token": "p2"},  # type: ignore[index]
            {"bars": {"AAPL": [BAR_PAYLOAD["bars"]["AAPL"][1]]}, "next_page_token": None},  # type: ignore[index]
        ]
        seen: list[str | None] = []

        def handler(request: httpx.Request) -> httpx.Response:
            token = request.url.params.get("page_token")
            seen.append(token)
            return httpx.Response(200, json=pages[0] if token is None else pages[1])

        provider = make_provider(handler)

        bars = await provider.fetch_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
        )

        assert len(bars) == 2
        assert seen == [None, "p2"]

    async def test_an_empty_range_returns_nothing(self) -> None:
        provider = make_provider(json_response({"bars": {}, "next_page_token": None}))

        bars = await provider.fetch_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
        )

        assert bars == []

    async def test_sends_credentials(self) -> None:
        captured: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured.update(dict(request.headers))
            return httpx.Response(200, json=BAR_PAYLOAD)

        await make_provider(handler).fetch_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
        )

        assert captured["apca-api-key-id"] == "test-key-not-real"
        assert captured["apca-api-secret-key"] == "test-secret-not-real"

    async def test_refuses_an_interval_the_provider_does_not_serve(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Alpaca happens to serve every BarInterval today.

        The rejection path still needs a test, so the timeframe map is narrowed for this
        one test rather than the branch going unexercised until a provider without daily
        bars appears.
        """
        from tradewatch.infrastructure.providers import alpaca

        monkeypatch.setitem(alpaca._TIMEFRAMES, BarInterval.ONE_DAY, None)
        monkeypatch.delitem(alpaca._TIMEFRAMES, BarInterval.ONE_DAY)
        provider = make_provider(json_response(BAR_PAYLOAD))

        with pytest.raises(ValueError, match="does not serve"):
            await provider.fetch_bars(
                "AAPL", BarInterval.ONE_DAY, MOMENT, MOMENT + dt.timedelta(days=5)
            )

    async def test_refuses_a_reversed_range(self) -> None:
        provider = make_provider(json_response(BAR_PAYLOAD))

        with pytest.raises(ProviderRequestError, match="not after"):
            await provider.fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT - dt.timedelta(minutes=1)
            )


class TestMalformedResponses:
    """Every one of these arrives over a healthy connection and must be rejected clearly."""

    async def test_a_body_that_is_not_json(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, text="<html>502 Bad Gateway</html>")

        with pytest.raises(ProviderDataError, match="not JSON"):
            await make_provider(handler).fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

    async def test_a_response_of_the_wrong_shape(self) -> None:
        with pytest.raises(ProviderDataError, match="not an object"):
            await make_provider(json_response(["unexpected"])).fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

    @pytest.mark.parametrize("missing", ["t", "o", "h", "l", "c"])
    async def test_a_bar_missing_a_required_field(self, missing: str) -> None:
        bar = dict(BAR_PAYLOAD["bars"]["AAPL"][0])  # type: ignore[index]
        del bar[missing]
        provider = make_provider(json_response({"bars": {"AAPL": [bar]}}))

        with pytest.raises(ProviderDataError, match=f"'{missing}'"):
            await provider.fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

    async def test_a_null_required_field(self) -> None:
        bar = dict(BAR_PAYLOAD["bars"]["AAPL"][0]) | {"c": None}  # type: ignore[index]
        provider = make_provider(json_response({"bars": {"AAPL": [bar]}}))

        with pytest.raises(ProviderDataError, match="null"):
            await provider.fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

    async def test_a_price_that_is_not_a_number(self) -> None:
        bar = dict(BAR_PAYLOAD["bars"]["AAPL"][0]) | {"o": "not-a-price"}  # type: ignore[index]
        provider = make_provider(json_response({"bars": {"AAPL": [bar]}}))

        with pytest.raises(ProviderDataError, match="not a number"):
            await provider.fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

    async def test_an_unparseable_timestamp(self) -> None:
        bar = dict(BAR_PAYLOAD["bars"]["AAPL"][0]) | {"t": "yesterday"}  # type: ignore[index]
        provider = make_provider(json_response({"bars": {"AAPL": [bar]}}))

        with pytest.raises(ProviderDataError, match="timestamp"):
            await provider.fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

    async def test_an_incoherent_bar_is_rejected(self) -> None:
        """High below low. The domain validator catches what the provider got wrong."""
        bar = dict(BAR_PAYLOAD["bars"]["AAPL"][0]) | {"h": 100.0, "l": 200.0}  # type: ignore[index]
        provider = make_provider(json_response({"bars": {"AAPL": [bar]}}))

        with pytest.raises(Exception, match="below low"):
            await provider.fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

    async def test_malformed_data_is_not_retried(self) -> None:
        """The same bytes would parse the same way; retrying just wastes the quota."""
        calls = {"n": 0}

        def handler(_request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(200, text="not json")

        with pytest.raises(ProviderDataError):
            await make_provider(handler).fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

        assert calls["n"] == 1


class TestHttpFailures:
    async def test_a_timeout_becomes_a_timeout_error(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            raise httpx.TimeoutException("too slow")

        with pytest.raises(ProviderTimeoutError, match="timed out"):
            await make_provider(handler).fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

    async def test_a_network_failure_becomes_a_connection_error(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("name resolution failed")

        with pytest.raises(ProviderConnectionError, match="failed"):
            await make_provider(handler).fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

    async def test_a_transient_failure_is_retried(self) -> None:
        calls = {"n": 0}

        def handler(_request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] < 3:
                raise httpx.ConnectError("reset")
            return httpx.Response(200, json=BAR_PAYLOAD)

        bars = await make_provider(handler).fetch_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
        )

        assert len(bars) == 2
        assert calls["n"] == 3

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (401, ProviderAuthError),
            (403, ProviderAuthError),
            (404, ProviderRequestError),
            (422, ProviderRequestError),
            (429, ProviderRateLimitError),
            (500, ProviderUnavailableError),
            (503, ProviderUnavailableError),
        ],
    )
    async def test_http_status_maps_to_the_right_error(
        self, status: int, expected: type[Exception]
    ) -> None:
        """The mapping is what decides whether the caller retries or gives up."""
        with pytest.raises(expected):
            await make_provider(json_response({"message": "nope"}, status=status)).fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

    async def test_a_bad_key_is_not_retried(self) -> None:
        """Retrying burns the rate-limit budget to learn nothing."""
        calls = {"n": 0}

        def handler(_request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            return httpx.Response(401, json={"message": "forbidden"})

        with pytest.raises(ProviderAuthError):
            await make_provider(handler).fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

        assert calls["n"] == 1

    async def test_a_rate_limit_carries_the_providers_retry_after(self) -> None:
        provider = make_provider(
            json_response({"message": "slow down"}, status=429, headers={"Retry-After": "12"}),
            retry_policy=RetryPolicy(max_attempts=1),
        )

        with pytest.raises(ProviderRateLimitError) as raised:
            await provider.fetch_bars(
                "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
            )

        assert raised.value.retry_after == 12.0

    async def test_a_rate_limit_is_retried(self) -> None:
        calls = {"n": 0}

        def handler(_request: httpx.Request) -> httpx.Response:
            calls["n"] += 1
            if calls["n"] == 1:
                return httpx.Response(429, json={}, headers={"Retry-After": "0"})
            return httpx.Response(200, json=BAR_PAYLOAD)

        bars = await make_provider(handler).fetch_bars(
            "AAPL", BarInterval.ONE_MINUTE, MOMENT, MOMENT + dt.timedelta(minutes=5)
        )

        assert len(bars) == 2


class TestLatestPrice:
    """Reads ``/v2/stocks/{symbol}/snapshot``.

    Not ``/trades/latest``, which returns only ``{"symbol", "trade"}``. The adapter used to
    call that one and then look for a ``quote`` key in the response — a key that endpoint
    never sends — so the book was silently always absent and `has_book` was permanently
    False. These tests now use the snapshot shape, which is the one that actually carries
    both halves.
    """

    async def test_normalizes_the_latest_trade(self) -> None:
        provider = make_provider(json_response(SNAPSHOT_PAYLOAD))

        snapshot = await provider.fetch_latest_price("aapl")

        assert snapshot is not None
        assert snapshot.symbol == "AAPL"
        assert snapshot.price == Decimal("187.42")
        assert snapshot.size == Decimal("100")
        assert snapshot.as_of.tzinfo is dt.UTC
        assert snapshot.as_of.microsecond == 123_456

    async def test_a_symbol_that_has_not_traded_returns_none(self) -> None:
        """A real answer, not an error."""
        provider = make_provider(json_response({"symbol": "AAPL"}))

        assert await provider.fetch_latest_price("AAPL") is None

    async def test_the_book_comes_back_with_the_trade(self) -> None:
        provider = make_provider(json_response(SNAPSHOT_PAYLOAD))

        snapshot = await provider.fetch_latest_price("AAPL")

        assert snapshot is not None
        assert snapshot.has_book
        assert snapshot.bid_price == Decimal("187.4")
        assert snapshot.ask_price == Decimal("187.44")

    async def test_a_snapshot_without_a_quote_still_gives_a_price(self) -> None:
        """Pre-open, or a symbol with no book. A price without a book is not an error."""
        payload = {"symbol": "AAPL", "latestTrade": SNAPSHOT_PAYLOAD["latestTrade"]}
        provider = make_provider(json_response(payload))

        snapshot = await provider.fetch_latest_price("AAPL")

        assert snapshot is not None
        assert snapshot.price == Decimal("187.42")
        assert not snapshot.has_book

    async def test_it_asks_the_snapshot_endpoint(self) -> None:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url.path))
            return httpx.Response(200, json=SNAPSHOT_PAYLOAD)

        await make_provider(handler).fetch_latest_price("AAPL")

        assert seen == ["/v2/stocks/AAPL/snapshot"]


class TestStreamNormalization:
    """The WebSocket message shapes, without a WebSocket."""

    def test_normalizes_a_trade_message(self) -> None:
        provider = make_provider()

        event = provider._normalize_stream_trade(STREAM_TRADE)

        assert isinstance(event, TradeEvent)
        assert event.symbol == "AAPL"
        assert event.price == Decimal("187.42")
        assert event.size == Decimal("100")
        assert event.provider_trade_id == "998877"
        assert event.conditions == ("@", "F")
        assert event.occurred_at.tzinfo is dt.UTC

    def test_normalizes_a_quote_message(self) -> None:
        provider = make_provider()

        event = provider._normalize_stream_quote(STREAM_QUOTE)

        assert isinstance(event, QuoteEvent)
        assert event.bid_price == Decimal("187.4")
        assert event.ask_price == Decimal("187.44")
        assert event.spread == Decimal("0.04")

    def test_a_quote_side_may_be_empty(self) -> None:
        provider = make_provider()

        event = provider._normalize_stream_quote(STREAM_QUOTE | {"bs": 0})

        assert event.bid_size == 0

    def test_the_same_trade_message_normalizes_to_the_same_id(self) -> None:
        """A reconnect replays recent prints; they must deduplicate."""
        provider = make_provider()

        first = provider._normalize_stream_trade(STREAM_TRADE)
        second = provider._normalize_stream_trade(STREAM_TRADE)

        assert first.event_id == second.event_id

    @pytest.mark.parametrize("missing", ["S", "p", "s", "t"])
    def test_a_trade_missing_a_field_is_rejected(self, missing: str) -> None:
        provider = make_provider()
        message = {key: value for key, value in STREAM_TRADE.items() if key != missing}

        with pytest.raises(ProviderDataError, match=f"'{missing}'"):
            provider._normalize_stream_trade(message)


class TestFrameDecoding:
    def test_decodes_a_batched_array(self) -> None:
        """Alpaca batches messages into one frame."""
        frames = AlpacaProvider._decode_frame(json.dumps([STREAM_TRADE, STREAM_QUOTE]))

        assert len(frames) == 2

    def test_decodes_a_single_object(self) -> None:
        assert len(AlpacaProvider._decode_frame(json.dumps(STREAM_TRADE))) == 1

    def test_decodes_bytes(self) -> None:
        raw = json.dumps([STREAM_TRADE]).encode()

        assert len(AlpacaProvider._decode_frame(raw)) == 1

    def test_rejects_a_frame_that_is_not_json(self) -> None:
        with pytest.raises(ProviderDataError, match="not valid JSON"):
            AlpacaProvider._decode_frame("{not json")

    def test_rejects_a_frame_of_the_wrong_type(self) -> None:
        with pytest.raises(ProviderDataError, match="neither"):
            AlpacaProvider._decode_frame(json.dumps("a bare string"))


class TestStreaming:
    async def test_authenticates_subscribes_and_yields(self) -> None:
        connect, sockets = socket_factory([*HANDSHAKE, json.dumps([STREAM_TRADE])])
        provider = make_provider(connect=connect)

        events = [event async for event in provider.stream_trades(["AAPL"])]

        assert len(events) == 1
        assert events[0].price == Decimal("187.42")

        sent = sockets[0].sent
        assert sent[0]["action"] == "auth"
        assert sent[0]["key"] == "test-key-not-real"
        assert sent[1] == {"action": "subscribe", "trades": ["AAPL"]}

    async def test_control_messages_are_not_yielded_as_events(self) -> None:
        connect, _ = socket_factory(
            [
                *HANDSHAKE,
                json.dumps([{"T": "success", "msg": "connected"}]),
                json.dumps([STREAM_TRADE]),
            ]
        )
        provider = make_provider(connect=connect)

        events = [event async for event in provider.stream_trades(["AAPL"])]

        assert len(events) == 1

    async def test_rejected_credentials_raise_an_auth_error(self) -> None:
        connect, _ = socket_factory(
            [
                json.dumps([{"T": "success", "msg": "connected"}]),
                json.dumps([{"T": "error", "code": 402, "msg": "auth failed"}]),
            ]
        )
        provider = make_provider(connect=connect)

        with pytest.raises(ProviderAuthError, match="auth failed"):
            async for _ in provider.stream_trades(["AAPL"]):
                pass

    async def test_a_connection_limit_is_retryable(self) -> None:
        """Distinct from an auth failure: waiting it out can work."""
        connect, _ = socket_factory(
            [
                json.dumps([{"T": "success", "msg": "connected"}]),
                json.dumps([{"T": "error", "code": 406, "msg": "connection limit exceeded"}]),
            ]
        )
        provider = make_provider(connect=connect)

        with pytest.raises(ProviderRateLimitError) as raised:
            async for _ in provider.stream_trades(["AAPL"]):
                pass

        assert raised.value.retryable

    async def test_a_socket_error_becomes_a_connection_error(self) -> None:
        connect, _ = socket_factory([*HANDSHAKE, ConnectionResetError("connection reset by peer")])
        provider = make_provider(connect=connect)

        with pytest.raises(ProviderConnectionError):
            async for _ in provider.stream_trades(["AAPL"]):
                pass

    async def test_the_socket_is_closed_on_the_way_out(self) -> None:
        connect, sockets = socket_factory([*HANDSHAKE, json.dumps([STREAM_TRADE])])
        provider = make_provider(connect=connect)

        async for _ in provider.stream_trades(["AAPL"]):
            pass

        assert sockets[0].closed

    async def test_quote_subscription_asks_for_quotes(self) -> None:
        connect, sockets = socket_factory([*HANDSHAKE, json.dumps([STREAM_QUOTE])])
        provider = make_provider(connect=connect)

        events = [event async for event in provider.stream_quotes(["AAPL"])]

        assert len(events) == 1
        assert sockets[0].sent[1] == {"action": "subscribe", "quotes": ["AAPL"]}

    async def test_refuses_an_empty_subscription(self) -> None:
        provider = make_provider(connect=socket_factory(HANDSHAKE)[0])

        with pytest.raises(ProviderRequestError, match="no symbols"):
            async for _ in provider.stream_trades([]):
                pass


class TestMultiplexing:
    """Trades and quotes share one socket.

    This is the class that would have caught the original defect. Alpaca's free tier permits
    one concurrent connection and silently displaces the older one when a second appears, and
    the ingestion worker runs `stream_trades` and `stream_quotes` as two concurrent tasks —
    so an adapter that opens a socket per call authenticates twice, streams briefly, and then
    delivers only one of the two feeds, with no error anywhere. Every pre-existing streaming
    test drove one stream in isolation, which is the one shape of test that cannot see it.
    """

    @staticmethod
    async def drain(stream: Any, into: list[Any], *, ready: asyncio.Event | None = None) -> None:
        if ready is not None:
            ready.set()
        async for event in stream:
            into.append(event)

    async def test_both_streams_share_a_single_socket(self) -> None:
        connect, sockets = socket_factory(
            [*HANDSHAKE, json.dumps([STREAM_TRADE]), json.dumps([STREAM_QUOTE])]
        )
        provider = make_provider(connect=connect)
        trades: list[Any] = []
        quotes: list[Any] = []

        await asyncio.gather(
            self.drain(provider.stream_trades(["AAPL"]), trades),
            self.drain(provider.stream_quotes(["AAPL"]), quotes),
        )

        assert len(sockets) == 1, "a second socket would displace the first on the free tier"

    async def test_each_subscription_is_sent_on_that_socket(self) -> None:
        connect, sockets = socket_factory([*HANDSHAKE, json.dumps([STREAM_TRADE])])
        provider = make_provider(connect=connect)
        trades: list[Any] = []
        quotes: list[Any] = []

        await asyncio.gather(
            self.drain(provider.stream_trades(["AAPL"]), trades),
            self.drain(provider.stream_quotes(["AAPL"]), quotes),
        )

        actions = [message for message in sockets[0].sent if message.get("action") == "subscribe"]
        assert {"action": "subscribe", "trades": ["AAPL"]} in actions
        assert {"action": "subscribe", "quotes": ["AAPL"]} in actions

    async def test_authentication_happens_once(self) -> None:
        connect, sockets = socket_factory([*HANDSHAKE, json.dumps([STREAM_TRADE])])
        provider = make_provider(connect=connect)
        sink: list[Any] = []

        await asyncio.gather(
            self.drain(provider.stream_trades(["AAPL"]), sink),
            self.drain(provider.stream_quotes(["AAPL"]), sink),
        )

        auths = [message for message in sockets[0].sent if message.get("action") == "auth"]
        assert len(auths) == 1

    async def test_events_are_routed_to_the_subscriber_that_asked_for_them(self) -> None:
        connect, _ = socket_factory(
            [*HANDSHAKE, json.dumps([STREAM_TRADE, STREAM_QUOTE, STREAM_TRADE])]
        )
        provider = make_provider(connect=connect)
        trades: list[Any] = []
        quotes: list[Any] = []

        await asyncio.gather(
            self.drain(provider.stream_trades(["AAPL"]), trades),
            self.drain(provider.stream_quotes(["AAPL"]), quotes),
        )

        assert len(trades) == 2
        assert len(quotes) == 1
        assert all(isinstance(event, TradeEvent) for event in trades)
        assert all(isinstance(event, QuoteEvent) for event in quotes)

    async def test_a_socket_failure_reaches_both_subscribers(self) -> None:
        """Otherwise one stream reconnects and the other waits on an empty queue forever."""
        connect, _ = socket_factory([*HANDSHAKE, ConnectionResetError("reset")])
        provider = make_provider(connect=connect)

        async def collect(stream: Any) -> BaseException | None:
            try:
                async for _ in stream:
                    pass
            except ProviderError as error:
                return error
            return None

        async with asyncio.timeout(5):
            failures = await asyncio.gather(
                collect(provider.stream_trades(["AAPL"])),
                collect(provider.stream_quotes(["AAPL"])),
            )

        assert all(isinstance(failure, ProviderConnectionError) for failure in failures), (
            "a subscriber that attaches after the reader died must not wait forever"
        )

    async def test_a_clean_close_ends_both_streams_without_hanging(self) -> None:
        """Ending rather than raising: `reconnecting` is what decides to reopen."""
        connect, _ = socket_factory([*HANDSHAKE, json.dumps([STREAM_TRADE])])
        provider = make_provider(connect=connect)
        trades: list[Any] = []
        quotes: list[Any] = []

        async with asyncio.timeout(5):
            await asyncio.gather(
                self.drain(provider.stream_trades(["AAPL"]), trades),
                self.drain(provider.stream_quotes(["AAPL"]), quotes),
            )

        assert len(trades) == 1
        assert quotes == []

    async def test_the_socket_survives_one_subscriber_leaving(self) -> None:
        """One stream stopping must not take the other's connection with it."""
        connect, sockets = controlled_socket_factory(HANDSHAKE)
        provider = make_provider(connect=connect)

        quotes: list[Any] = []
        trades: list[Any] = []
        quote_ready = asyncio.Event()
        trade_ready = asyncio.Event()

        quote_task = asyncio.create_task(
            self.drain(provider.stream_quotes(["AAPL"]), quotes, ready=quote_ready)
        )
        await quote_ready.wait()
        await asyncio.sleep(0)

        trade_task = asyncio.create_task(
            self.drain(provider.stream_trades(["AAPL"]), trades, ready=trade_ready)
        )
        await trade_ready.wait()
        await asyncio.sleep(0)

        assert len(sockets) == 1, "the second subscriber attached rather than reconnecting"

        quote_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await quote_task
        await asyncio.sleep(0)

        assert not sockets[0].closed, "the trade stream is still using it"

        # The surviving subscriber still receives events on the same socket.
        sockets[0].deliver(json.dumps([STREAM_TRADE]))
        async with asyncio.timeout(5):
            while not trades:  # noqa: ASYNC110 - waiting on the adapter's reader task, not an event
                await asyncio.sleep(0)

        trade_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await trade_task
        await asyncio.sleep(0)

        assert sockets[0].closed, "closed once the last subscriber left"

    async def test_streaming_the_same_kind_twice_is_refused(self) -> None:
        """One socket means one trade subscription. Two would silently interleave."""
        connect, _ = socket_factory([*HANDSHAKE, *[json.dumps([STREAM_TRADE])] * 5])
        provider = make_provider(connect=connect)
        sink: list[Any] = []
        started = asyncio.Event()

        first = asyncio.create_task(
            self.drain(provider.stream_trades(["AAPL"]), sink, ready=started)
        )
        await started.wait()
        await asyncio.sleep(0)

        with pytest.raises(ProviderRequestError, match="already being streamed"):
            async for _ in provider.stream_trades(["AAPL"]):
                pass

        first.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await first

    async def test_closing_the_provider_tears_the_socket_down(self) -> None:
        connect, sockets = socket_factory([*HANDSHAKE, *[json.dumps([STREAM_TRADE])] * 50])
        provider = make_provider(connect=connect)
        sink: list[Any] = []
        started = asyncio.Event()

        task = asyncio.create_task(
            self.drain(provider.stream_trades(["AAPL"]), sink, ready=started)
        )
        await started.wait()
        await asyncio.sleep(0)

        await provider.aclose()

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert sockets[0].closed
