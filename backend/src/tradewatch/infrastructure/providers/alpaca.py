"""Alpaca market data adapter (ADR-0011).

This is the one place in TradeWatch that knows what Alpaca's JSON looks like. Every field
name below — `bp`, `ax`, `vw`, `T`, `S` — stops here; above this module there are only
:mod:`tradewatch.domain.events`.

**Unverified against the live service.** This adapter is written against Alpaca's documented
Market Data API v2 shapes, and every test drives it with recorded/constructed payloads. It
has never run against the real endpoint, because no credentials exist in this environment.
If the live API differs from the documentation, the normalizers here are what need
changing — which is the point of confining them to one module, but it is not the same as
having been proven correct. Running `pytest -m provider` with real credentials is what
closes that gap, and until someone does, treat this as untested integration code.

Free tier notes, which shape the implementation:

* The IEX feed is a single venue, so prices are IEX's, not the consolidated tape. Fine for
  a simulation; worth knowing before quoting any number from it.
* One concurrent WebSocket connection. A second connection silently displaces the first,
  so the ingestion service must run as a singleton.
* Trades and quotes multiplex over one socket. The ingestion worker runs
  :meth:`AlpacaProvider.stream_trades` and :meth:`AlpacaProvider.stream_quotes` as two
  concurrent tasks, so this adapter **must** fan both out from a single connection —
  see :class:`_StreamMultiplexer`. An earlier version opened a socket per call, which on
  the free tier means the second connection displaces the first and one of the two streams
  silently stops. It was not caught by the unit tests because each drove one stream in
  isolation, which is exactly the shape of test that cannot see this bug.
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from decimal import Decimal
from typing import Any, Final

import httpx2 as httpx

from tradewatch.application.ports.market_data import (
    InstrumentSnapshot,
    PriceSnapshot,
    ProviderCapabilities,
)
from tradewatch.domain.enums import AssetClass, BarInterval
from tradewatch.domain.events import BarEvent, QuoteEvent, TradeEvent
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
from tradewatch.infrastructure.providers.normalization import (
    EventFactory,
    normalize_symbol,
    parse_decimal,
    parse_timestamp,
    require,
)
from tradewatch.infrastructure.providers.resilience import (
    RetryPolicy,
    TokenBucket,
    call_with_retry,
)
from tradewatch.observability.logging import get_logger

__all__ = ["AlpacaProvider", "AlpacaSettings"]

log = get_logger(__name__)

PROVIDER_NAME = "alpaca"

DATA_BASE_URL: Final = "https://data.alpaca.markets"
TRADING_BASE_URL: Final = "https://api.alpaca.markets"
STREAM_URL: Final = "wss://stream.data.alpaca.markets/v2/iex"

#: Alpaca's timeframe vocabulary. Anything not here is rolled up from 1m locally.
_TIMEFRAMES: Final[dict[BarInterval, str]] = {
    BarInterval.ONE_MINUTE: "1Min",
    BarInterval.FIVE_MINUTES: "5Min",
    BarInterval.ONE_HOUR: "1Hour",
    BarInterval.ONE_DAY: "1Day",
}

_CAPABILITIES = ProviderCapabilities(
    streaming_trades=True,
    streaming_quotes=True,
    historical_bars=True,
    latest_price=True,
    instrument_list=True,
    intervals=frozenset(_TIMEFRAMES),
    max_history=None,
    # The documented free-tier ceiling. The client-side limiter stays under it so a 429 is
    # an anomaly rather than the normal way we discover the limit (failure F2).
    rate_limit_per_minute=200,
)

#: Pages to follow before concluding something is wrong. A month of 1m bars for one symbol
#: is roughly 8,000 rows, well inside this.
_MAX_PAGES: Final = 200

_TRADES: Final = "trades"
_QUOTES: Final = "quotes"

#: Pushed to a subscriber when the socket closed cleanly. Distinct from an exception:
#: the generator *ends*, and `reconnecting` turns "the stream finished" into a reopen —
#: which is where that decision has always lived (see the port's docstring).
_END: Final = object()

#: Per-subscriber buffer. Generous, because the only consumer is the ingestion service,
#: which publishes to Kafka and returns. If it ever fills, something downstream has stopped.
_SUBSCRIBER_BUFFER: Final = 10_000

#: Exception names that mean "the peer closed the socket normally". Matched by name so this
#: module does not import `websockets` just to catch two classes, which would undo the lazy
#: import that keeps the synthetic provider free of the dependency.
_CLEAN_CLOSE_NAMES: Final = frozenset(
    {"ConnectionClosedOK", "ConnectionClosed", "StopAsyncIteration"}
)


class _StreamClosedError(Exception):
    """Internal: the socket closed cleanly. Converted to a reconnectable error by the reader."""


async def _close_connection(connection: Any) -> None:
    """Close a socket whether its ``close`` is a coroutine function or not."""
    closer = getattr(connection, "close", None)
    if closer is None:
        return
    try:
        result = closer()
        if asyncio.iscoroutine(result):
            await result
    except Exception as error:
        log.debug("alpaca.close_failed", error=str(error), error_type=type(error).__name__)


class AlpacaSettings:
    """Connection details, kept out of the adapter so tests can point it anywhere."""

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        data_base_url: str = DATA_BASE_URL,
        trading_base_url: str = TRADING_BASE_URL,
        stream_url: str = STREAM_URL,
        feed: str = "iex",
        timeout_seconds: float = 10.0,
        rate_limit_per_minute: int = 200,
    ) -> None:
        if not api_key or not api_secret:
            raise ProviderAuthError(
                "TW_PROVIDER_API_KEY and TW_PROVIDER_API_SECRET are required for alpaca",
                provider=PROVIDER_NAME,
            )
        self.api_key = api_key
        self.api_secret = api_secret
        self.data_base_url = data_base_url.rstrip("/")
        self.trading_base_url = trading_base_url.rstrip("/")
        self.stream_url = stream_url
        self.feed = feed
        self.timeout_seconds = timeout_seconds
        self.rate_limit_per_minute = rate_limit_per_minute

    @property
    def auth_headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
        }


class AlpacaProvider:
    """Alpaca Market Data API v2.

    Implements :class:`~tradewatch.application.ports.market_data.MarketDataProvider`.

    Both the HTTP client and the WebSocket connector are injectable, so every test drives
    this class with a fake and none of them touch the network.
    """

    def __init__(
        self,
        settings: AlpacaSettings,
        *,
        client: httpx.AsyncClient | None = None,
        connect: Callable[[str], Awaitable[Any]] | None = None,
        factory: EventFactory | None = None,
        retry_policy: RetryPolicy | None = None,
        limiter: TokenBucket | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._settings = settings
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=settings.timeout_seconds,
            headers=settings.auth_headers,
        )
        self._connect = connect
        self._factory = factory or EventFactory(source=PROVIDER_NAME, producer="alpaca-provider")
        self._retry = retry_policy or RetryPolicy(max_attempts=4, base_delay=0.5, max_delay=20.0)
        self._limiter = limiter or TokenBucket(rate_per_minute=settings.rate_limit_per_minute)
        self._sleep = sleep or asyncio.sleep
        self._closed = False
        #: One socket, fanned out to the trade and quote subscribers. See the module
        #: docstring: the free tier permits exactly one connection.
        self._multiplexer = _StreamMultiplexer(self)

    # --- port surface ------------------------------------------------------

    @property
    def name(self) -> str:
        return PROVIDER_NAME

    @property
    def capabilities(self) -> ProviderCapabilities:
        return _CAPABILITIES

    async def aclose(self) -> None:
        self._closed = True
        await self._multiplexer.aclose()
        if self._owns_client:
            await self._client.aclose()

    async def list_instruments(self) -> Sequence[InstrumentSnapshot]:
        payload = await self._get(
            f"{self._settings.trading_base_url}/v2/assets",
            params={"status": "active", "asset_class": "us_equity"},
            description="list_instruments",
        )
        if not isinstance(payload, list):
            raise ProviderDataError(
                "assets response is not a list", provider=PROVIDER_NAME, raw=repr(payload)[:200]
            )
        instruments: list[InstrumentSnapshot] = []
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            try:
                instruments.append(self._normalize_asset(entry))
            except ProviderDataError as error:
                # One unparseable asset must not lose the other eight thousand. Counted and
                # logged, never silently dropped.
                log.warning("alpaca.asset_skipped", error=str(error), symbol=entry.get("symbol"))
        return instruments

    async def fetch_bars(
        self,
        symbol: str,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> Sequence[BarEvent]:
        """Historical bars over ``[start, end)``, following pagination to the end."""
        timeframe = _TIMEFRAMES.get(interval)
        if timeframe is None:
            raise ValueError(f"alpaca does not serve {interval} bars")
        if end <= start:
            raise ProviderRequestError(
                f"end {end.isoformat()} is not after start {start.isoformat()}",
                provider=PROVIDER_NAME,
            )

        wanted = normalize_symbol(symbol, provider=PROVIDER_NAME)
        bars: list[BarEvent] = []
        page_token: str | None = None

        for _page in range(_MAX_PAGES):
            params: dict[str, str] = {
                "symbols": wanted,
                "timeframe": timeframe,
                "start": start.astimezone(dt.UTC).isoformat().replace("+00:00", "Z"),
                "end": end.astimezone(dt.UTC).isoformat().replace("+00:00", "Z"),
                "limit": "10000",
                "feed": self._settings.feed,
                "adjustment": "raw",
            }
            if page_token:
                params["page_token"] = page_token

            payload = await self._get(
                f"{self._settings.data_base_url}/v2/stocks/bars",
                params=params,
                description="fetch_bars",
            )
            if not isinstance(payload, dict):
                raise ProviderDataError("bars response is not an object", provider=PROVIDER_NAME)

            by_symbol = payload.get("bars") or {}
            if not isinstance(by_symbol, dict):
                raise ProviderDataError("bars.bars is not an object", provider=PROVIDER_NAME)

            for raw in by_symbol.get(wanted, []) or []:
                if not isinstance(raw, dict):
                    raise ProviderDataError(
                        "bar entry is not an object", provider=PROVIDER_NAME, raw=repr(raw)[:200]
                    )
                bars.append(self._normalize_bar(wanted, interval, raw))

            token = payload.get("next_page_token")
            if not token:
                break
            page_token = str(token)
        else:
            raise ProviderRequestError(
                f"stopped after {_MAX_PAGES} pages; narrow the requested range",
                provider=PROVIDER_NAME,
            )

        # The API returns bars in ascending time; sorting makes that a guarantee rather
        # than an assumption, and it is cheap.
        bars.sort(key=lambda bar: bar.bar_start)
        return bars

    async def fetch_latest_price(self, symbol: str) -> PriceSnapshot | None:
        """The most recent trade, with the book alongside it.

        Uses ``/v2/stocks/{symbol}/snapshot`` rather than ``/trades/latest``. The latter
        returns only ``{"symbol", "trade"}`` — it has no ``quote`` key at all — so the
        book-enrichment branch that used to read one here could never fire, and
        :attr:`PriceSnapshot.has_book` was permanently False for a reason no caller could
        see. The snapshot endpoint returns ``latestTrade`` and ``latestQuote`` together,
        which is what this method has always claimed to return.
        """
        wanted = normalize_symbol(symbol, provider=PROVIDER_NAME)
        payload = await self._get(
            f"{self._settings.data_base_url}/v2/stocks/{wanted}/snapshot",
            params={"feed": self._settings.feed},
            description="fetch_latest_price",
        )
        if not isinstance(payload, dict):
            raise ProviderDataError("snapshot response is not an object", provider=PROVIDER_NAME)

        trade = payload.get("latestTrade")
        if not isinstance(trade, dict):
            # A symbol that has not traded has no last price. A real answer, not an error.
            return None

        price = parse_decimal(
            require(trade, "p", provider=PROVIDER_NAME), field="p", provider=PROVIDER_NAME
        )
        size = parse_decimal(
            trade.get("s", 0),
            field="s",
            provider=PROVIDER_NAME,
            allow_zero=True,
        )
        as_of = parse_timestamp(
            require(trade, "t", provider=PROVIDER_NAME), field="t", provider=PROVIDER_NAME
        )

        bid = ask = None
        quote = payload.get("latestQuote")
        if isinstance(quote, dict):
            bid = parse_decimal(
                quote.get("bp", 0), field="bp", provider=PROVIDER_NAME, allow_zero=True
            )
            ask = parse_decimal(
                quote.get("ap", 0), field="ap", provider=PROVIDER_NAME, allow_zero=True
            )
            bid = bid or None
            ask = ask or None

        return PriceSnapshot(
            symbol=wanted,
            price=price,
            size=size or None,
            as_of=as_of,
            source=PROVIDER_NAME,
            bid_price=bid,
            ask_price=ask,
        )

    async def stream_trades(self, symbols: Sequence[str]) -> AsyncIterator[TradeEvent]:
        async for event in self._multiplexer.stream(symbols, kind=_TRADES):
            if isinstance(event, TradeEvent):
                yield event

    async def stream_quotes(self, symbols: Sequence[str]) -> AsyncIterator[QuoteEvent]:
        async for event in self._multiplexer.stream(symbols, kind=_QUOTES):
            if isinstance(event, QuoteEvent):
                yield event

    # --- HTTP --------------------------------------------------------------

    async def _get(self, url: str, *, params: Mapping[str, str] | None, description: str) -> Any:
        """One GET, with rate limiting, retries, and every failure mapped to a ProviderError."""
        if self._closed:
            raise ProviderConnectionError("provider is closed", provider=PROVIDER_NAME)

        async def attempt() -> Any:
            await self._limiter.acquire(sleep=self._sleep)
            try:
                response = await self._client.get(
                    url, params=dict(params or {}), headers=self._settings.auth_headers
                )
            except httpx.TimeoutException as error:
                raise ProviderTimeoutError(
                    f"{description} timed out after {self._settings.timeout_seconds:g}s",
                    provider=PROVIDER_NAME,
                ) from error
            except httpx.HTTPError as error:
                # Covers DNS failures, refused connections, resets, and protocol errors.
                raise ProviderConnectionError(
                    f"{description} failed: {type(error).__name__}", provider=PROVIDER_NAME
                ) from error

            self._raise_for_status(response, description=description)

            try:
                return response.json()
            except ValueError as error:
                raise ProviderDataError(
                    f"{description} returned a body that is not JSON",
                    provider=PROVIDER_NAME,
                    raw=response.text,
                ) from error

        return await call_with_retry(
            attempt, policy=self._retry, description=description, sleep=self._sleep
        )

    @staticmethod
    def _raise_for_status(response: httpx.Response, *, description: str) -> None:
        """Map HTTP status to the error class that says what the caller should do."""
        status = response.status_code
        if status < 400:
            return

        if status in (401, 403):
            raise ProviderAuthError(
                f"{description} rejected with {status}; check TW_PROVIDER_API_KEY",
                provider=PROVIDER_NAME,
            )
        if status == 404:
            raise ProviderRequestError(f"{description} returned 404", provider=PROVIDER_NAME)
        if status == 429:
            header = response.headers.get("Retry-After")
            retry_after: float | None = None
            if header:
                try:
                    retry_after = float(header)
                except ValueError:
                    retry_after = None
            raise ProviderRateLimitError(
                f"{description} was rate limited", provider=PROVIDER_NAME, retry_after=retry_after
            )
        if status >= 500:
            raise ProviderUnavailableError(
                f"{description} returned {status}", provider=PROVIDER_NAME
            )
        raise ProviderRequestError(
            f"{description} returned {status}",
            provider=PROVIDER_NAME,
            detail={"body": response.text[:200]},
        )

    # --- normalization -----------------------------------------------------

    def _normalize_asset(self, raw: Mapping[str, Any]) -> InstrumentSnapshot:
        symbol = normalize_symbol(
            require(raw, "symbol", provider=PROVIDER_NAME), provider=PROVIDER_NAME
        )
        return InstrumentSnapshot(
            symbol=symbol,
            name=raw.get("name"),
            asset_class=AssetClass.ETF if raw.get("class") == "us_etf" else AssetClass.EQUITY,
            exchange=str(raw.get("exchange") or "UNKNOWN")[:32],
            currency="USD",
            tick_size=Decimal("0.01"),
            lot_size=Decimal("1"),
            tradable=bool(raw.get("tradable", True)),
            provider_metadata={
                "id": raw.get("id"),
                "status": raw.get("status"),
                "fractionable": raw.get("fractionable"),
                "shortable": raw.get("shortable"),
            },
        )

    def _normalize_bar(
        self, symbol: str, interval: BarInterval, raw: Mapping[str, Any]
    ) -> BarEvent:
        """`{t, o, h, l, c, v, n, vw}` → :class:`BarEvent`."""
        return self._factory.bar(
            symbol=symbol,
            interval=interval,
            bar_start=parse_timestamp(
                require(raw, "t", provider=PROVIDER_NAME), field="t", provider=PROVIDER_NAME
            ),
            open=parse_decimal(
                require(raw, "o", provider=PROVIDER_NAME), field="o", provider=PROVIDER_NAME
            ),
            high=parse_decimal(
                require(raw, "h", provider=PROVIDER_NAME), field="h", provider=PROVIDER_NAME
            ),
            low=parse_decimal(
                require(raw, "l", provider=PROVIDER_NAME), field="l", provider=PROVIDER_NAME
            ),
            close=parse_decimal(
                require(raw, "c", provider=PROVIDER_NAME), field="c", provider=PROVIDER_NAME
            ),
            volume=parse_decimal(
                raw.get("v", 0), field="v", provider=PROVIDER_NAME, allow_zero=True
            ),
            vwap=(
                parse_decimal(raw["vw"], field="vw", provider=PROVIDER_NAME)
                if raw.get("vw")
                else None
            ),
            trade_count=int(raw.get("n", 0) or 0),
        )

    def _normalize_stream_trade(self, raw: Mapping[str, Any]) -> TradeEvent:
        """`{T:"t", S, p, s, t, i, c}` → :class:`TradeEvent`."""
        provider_trade_id = raw.get("i")
        return self._factory.trade(
            symbol=normalize_symbol(
                require(raw, "S", provider=PROVIDER_NAME), provider=PROVIDER_NAME
            ),
            price=parse_decimal(
                require(raw, "p", provider=PROVIDER_NAME), field="p", provider=PROVIDER_NAME
            ),
            size=parse_decimal(
                require(raw, "s", provider=PROVIDER_NAME), field="s", provider=PROVIDER_NAME
            ),
            occurred_at=parse_timestamp(
                require(raw, "t", provider=PROVIDER_NAME), field="t", provider=PROVIDER_NAME
            ),
            provider_trade_id=str(provider_trade_id) if provider_trade_id is not None else None,
            conditions=tuple(str(code) for code in (raw.get("c") or [])),
        )

    def _normalize_stream_quote(self, raw: Mapping[str, Any]) -> QuoteEvent:
        """`{T:"q", S, bp, bs, ap, as, t}` → :class:`QuoteEvent`."""
        return self._factory.quote(
            symbol=normalize_symbol(
                require(raw, "S", provider=PROVIDER_NAME), provider=PROVIDER_NAME
            ),
            bid_price=parse_decimal(
                require(raw, "bp", provider=PROVIDER_NAME), field="bp", provider=PROVIDER_NAME
            ),
            # Sizes are in round lots and may legitimately be zero when a side is empty.
            bid_size=parse_decimal(
                raw.get("bs", 0), field="bs", provider=PROVIDER_NAME, allow_zero=True
            ),
            ask_price=parse_decimal(
                require(raw, "ap", provider=PROVIDER_NAME), field="ap", provider=PROVIDER_NAME
            ),
            ask_size=parse_decimal(
                raw.get("as", 0), field="as", provider=PROVIDER_NAME, allow_zero=True
            ),
            occurred_at=parse_timestamp(
                require(raw, "t", provider=PROVIDER_NAME), field="t", provider=PROVIDER_NAME
            ),
        )

    # --- WebSocket ---------------------------------------------------------

    async def _open_session(self, symbols: Sequence[str], *, trades: bool, quotes: bool) -> Any:
        """Open, authenticate and subscribe one WebSocket session.

        Reconnection is the caller's job, through
        :func:`~tradewatch.infrastructure.providers.resilience.reconnecting` — one retry
        policy for the whole system rather than one per adapter.
        """
        if self._closed:
            raise ProviderConnectionError("provider is closed", provider=PROVIDER_NAME)
        if not symbols:
            raise ProviderRequestError("no symbols to subscribe to", provider=PROVIDER_NAME)

        connection = await self._open_socket()
        try:
            await self._authenticate(connection)
            await self._subscribe(connection, symbols, trades=trades, quotes=quotes)
        except BaseException:
            await _close_connection(connection)
            raise
        return connection

    async def _read_frame(self, connection: Any) -> Any:
        """One frame, with every failure mapped to a ProviderError.

        The port promises that only ``ProviderError`` subclasses cross this boundary. A raw
        ``ConnectionResetError`` escaping here would reach a retry policy that cannot
        classify it, and so would not retry a failure that is entirely retryable.

        Reads through ``recv()`` rather than ``anext(aiter(connection))``. The latter built
        a fresh async generator for every single message and abandoned it, which leaks a
        generator per tick and, on a library whose ``__aiter__`` buffers, can lose messages.
        """
        try:
            return await connection.recv()
        except asyncio.CancelledError:
            raise
        except ProviderError:
            raise
        except Exception as error:
            if type(error).__name__ in _CLEAN_CLOSE_NAMES:
                raise _StreamClosedError from error
            raise ProviderConnectionError(
                f"stream read failed: {type(error).__name__}", provider=PROVIDER_NAME
            ) from error

    async def _open_socket(self) -> Any:
        if self._connect is not None:
            return await self._connect(self._settings.stream_url)
        try:
            from websockets.asyncio.client import connect as ws_connect
        except ImportError as error:  # pragma: no cover - dependency is declared
            raise ProviderConnectionError(
                "the websockets package is required for streaming", provider=PROVIDER_NAME
            ) from error
        try:
            return await ws_connect(self._settings.stream_url)
        except Exception as error:
            raise ProviderConnectionError(
                f"could not open {self._settings.stream_url}: {type(error).__name__}",
                provider=PROVIDER_NAME,
            ) from error

    async def _authenticate(self, connection: Any) -> None:
        """Alpaca greets, then expects auth, then confirms. Anything else is fatal."""
        greeting = self._decode_frame(await self._receive(connection))
        for message in greeting:
            self._check_error(message)

        await self._send(
            connection,
            {
                "action": "auth",
                "key": self._settings.api_key,
                "secret": self._settings.api_secret,
            },
        )
        for message in self._decode_frame(await self._receive(connection)):
            self._check_error(message)
            if message.get("T") == "success" and message.get("msg") == "authenticated":
                log.info("alpaca.authenticated", feed=self._settings.feed)
                return
        raise ProviderAuthError("alpaca did not confirm authentication", provider=PROVIDER_NAME)

    async def _send_subscription(
        self, connection: Any, symbols: Sequence[str], *, trades: bool, quotes: bool
    ) -> None:
        """Ask for a subscription without waiting for the confirmation.

        Used when attaching a second subscriber to a socket whose reader task is already
        running. Reading the acknowledgement here would mean two coroutines calling
        ``recv()`` on one connection, which is undefined behaviour and, in practice, a
        frame delivered to whichever happened to be waiting. The reader already discards
        ``subscription`` control messages and already raises on ``error`` ones, so the
        confirmation and any rejection both arrive through the normal path.
        """
        request: dict[str, Any] = {"action": "subscribe"}
        if trades:
            request["trades"] = list(symbols)
        if quotes:
            request["quotes"] = list(symbols)
        await self._send(connection, request)
        log.info("alpaca.subscribed", symbols=len(symbols), trades=trades, quotes=quotes)

    async def _subscribe(
        self, connection: Any, symbols: Sequence[str], *, trades: bool, quotes: bool
    ) -> None:
        """Subscribe and wait for the confirmation.

        Only safe before the reader task exists — during the initial handshake — which is
        the one moment nothing else is reading the socket.
        """
        await self._send_subscription(connection, symbols, trades=trades, quotes=quotes)
        for message in self._decode_frame(await self._receive(connection)):
            self._check_error(message)

    async def _send(self, connection: Any, payload: Mapping[str, Any]) -> None:
        try:
            await connection.send(json.dumps(payload))
        except Exception as error:
            raise ProviderConnectionError(
                f"send failed: {type(error).__name__}", provider=PROVIDER_NAME
            ) from error

    async def _receive(self, connection: Any) -> Any:
        try:
            return await asyncio.wait_for(connection.recv(), timeout=self._settings.timeout_seconds)
        except TimeoutError as error:
            raise ProviderTimeoutError(
                f"no frame within {self._settings.timeout_seconds:g}s", provider=PROVIDER_NAME
            ) from error
        except Exception as error:
            raise ProviderConnectionError(
                f"receive failed: {type(error).__name__}", provider=PROVIDER_NAME
            ) from error

    @staticmethod
    def _decode_frame(frame: Any) -> list[dict[str, Any]]:
        """Alpaca batches messages into a JSON array per frame."""
        if isinstance(frame, bytes | bytearray):
            frame = frame.decode("utf-8", errors="replace")
        if not isinstance(frame, str):
            raise ProviderDataError(
                f"unexpected frame type {type(frame).__name__}", provider=PROVIDER_NAME
            )
        try:
            payload = json.loads(frame)
        except json.JSONDecodeError as error:
            raise ProviderDataError(
                "frame is not valid JSON", provider=PROVIDER_NAME, raw=frame
            ) from error

        if isinstance(payload, dict):
            return [payload]
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        raise ProviderDataError(
            "frame is neither an object nor an array", provider=PROVIDER_NAME, raw=frame
        )

    @staticmethod
    def _check_error(message: Mapping[str, Any]) -> None:
        """Turn an Alpaca control message into the right exception class.

        The distinction matters: 401/402/403/404 are permanent and must not be retried,
        while a connection-limit refusal is worth waiting out.
        """
        if message.get("T") != "error":
            return
        code = int(message.get("code", 0) or 0)
        text = str(message.get("msg", "unknown error"))
        if code in (401, 402, 403, 404):
            raise ProviderAuthError(f"{text} (code {code})", provider=PROVIDER_NAME)
        if code in (405, 406):
            raise ProviderRateLimitError(f"{text} (code {code})", provider=PROVIDER_NAME)
        raise ProviderConnectionError(f"{text} (code {code})", provider=PROVIDER_NAME)

    def _handle_message(self, message: Mapping[str, Any]) -> TradeEvent | QuoteEvent | None:
        """One decoded message → an event, or None for control traffic.

        A malformed *data* message raises `ProviderDataError`, which the ingestion service
        counts and skips (failure F3). It does not kill the stream: one bad tick must not
        end the session.
        """
        self._check_error(message)
        kind = message.get("T")
        if kind == "t":
            return self._normalize_stream_trade(message)
        if kind == "q":
            return self._normalize_stream_quote(message)
        if kind in ("success", "subscription"):
            return None
        log.debug("alpaca.unhandled_message", message_type=kind)
        return None


class _StreamMultiplexer:
    """One Alpaca socket, fanned out to the trade and quote subscribers.

    **Why this exists.** The port has two streaming methods and the ingestion worker calls
    both, concurrently, as separate tasks. Alpaca's free tier permits one concurrent
    connection and silently displaces the older one when a second appears. So a naive
    adapter — open a socket per call — produces a system that authenticates twice, streams
    for a moment, and then delivers only quotes, with no error anywhere. The failure looks
    like "trades stopped arriving", which is a long way from "you opened two sockets".

    So: the first subscriber opens and authenticates the connection; the second attaches to
    it and adds its subscription. A single reader task decodes frames and routes each event
    to the subscribers that asked for that kind. When the last subscriber goes away, the
    socket closes.

    **Failures reach every subscriber.** When the reader dies, the error is handed to all of
    them, so each one's ``reconnecting`` wrapper sees a `ProviderError` and backs off. The
    first to come back re-opens the socket; the rest attach to it. Without this, a dropped
    connection would wake one stream and strand the other on an empty queue forever.

    **Backpressure is shed, not propagated.** Each subscriber has a bounded buffer. Blocking
    the reader would stall the socket and eventually have Alpaca disconnect us, so a full
    buffer drops its oldest event and counts it — the same shedding posture as failure F4,
    and visible in `dropped` rather than silent.
    """

    def __init__(self, owner: AlpacaProvider) -> None:
        self._owner = owner
        self._lock = asyncio.Lock()
        self._connection: Any = None
        self._reader: asyncio.Task[None] | None = None
        self._subscribers: dict[str, asyncio.Queue[Any]] = {}
        self._symbols: list[str] = []
        self.dropped = 0

    @property
    def connected(self) -> bool:
        return self._connection is not None

    async def stream(
        self, symbols: Sequence[str], *, kind: str
    ) -> AsyncIterator[TradeEvent | QuoteEvent]:
        """Yield events of one ``kind`` until the socket fails or the caller stops."""
        queue = await self._attach(symbols, kind=kind)
        try:
            while True:
                item = await queue.get()
                if item is _END:
                    return
                if isinstance(item, BaseException):
                    raise item
                yield item
        finally:
            await self._detach(kind)

    # --- membership --------------------------------------------------------

    async def _attach(self, symbols: Sequence[str], *, kind: str) -> asyncio.Queue[Any]:
        wanted = [normalize_symbol(symbol, provider=PROVIDER_NAME) for symbol in symbols]
        if not wanted:
            raise ProviderRequestError("no symbols to subscribe to", provider=PROVIDER_NAME)

        async with self._lock:
            if kind in self._subscribers:
                raise ProviderRequestError(
                    f"{kind} are already being streamed by this provider", provider=PROVIDER_NAME
                )

            queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=_SUBSCRIBER_BUFFER)

            if self._connection is not None and self._reader is not None and self._reader.done():
                # The reader finished, so the socket is either closed or unusable. The
                # subscribers that were attached already have their error or end marker
                # queued; this one must not be joined to a session nothing is reading, which
                # would leave it waiting on an empty queue forever.
                log.info("alpaca.session_expired", detail="reopening for a new subscriber")
                await self._teardown_locked()

            if self._connection is None:
                self._connection = await self._owner._open_session(
                    wanted, trades=kind == _TRADES, quotes=kind == _QUOTES
                )
                self._symbols = list(wanted)
            else:
                # Attach to the existing session rather than opening a second one. Send
                # only — the reader owns `recv()` from here on.
                await self._owner._send_subscription(
                    self._connection,
                    wanted,
                    trades=kind == _TRADES,
                    quotes=kind == _QUOTES,
                )

            self._subscribers[kind] = queue
            if self._reader is None or self._reader.done():
                self._reader = asyncio.create_task(self._read_loop(), name="alpaca-stream-reader")
            return queue

    async def _detach(self, kind: str) -> None:
        async with self._lock:
            self._subscribers.pop(kind, None)
            if self._subscribers:
                return
            await self._shutdown_locked()

    async def aclose(self) -> None:
        async with self._lock:
            self._subscribers.clear()
            await self._shutdown_locked()

    async def _shutdown_locked(self) -> None:
        await self._teardown_locked()

    async def _teardown_locked(self) -> None:
        """Stop the reader and close the socket, leaving subscriber queues alone.

        Their contents — an error, or the end marker — are what tells each subscriber what
        happened, so they are deliberately not discarded here.
        """
        reader, self._reader = self._reader, None
        if reader is not None and not reader.done():
            reader.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader

        connection, self._connection = self._connection, None
        self._symbols = []
        if connection is not None:
            await _close_connection(connection)

    # --- the reader --------------------------------------------------------

    async def _read_loop(self) -> None:
        """Decode frames and route events, until the socket fails or this task is cancelled."""
        connection = self._connection
        try:
            while True:
                if connection is None:
                    raise ProviderConnectionError("socket went away", provider=PROVIDER_NAME)
                try:
                    frame = await self._owner._read_frame(connection)
                except _StreamClosedError:
                    # A clean close ends every subscriber's generator. `reconnecting` is what
                    # decides that a finished stream should be reopened, and keeping that
                    # decision there means one reconnect policy rather than one per adapter.
                    self._end_subscribers()
                    return

                for message in self._owner._decode_frame(frame):
                    try:
                        event = self._owner._handle_message(message)
                    except ProviderDataError as error:
                        # One bad tick must not end the session (failure F3). The ingestion
                        # service counts these; here it is enough not to die.
                        log.warning("alpaca.malformed_message", error=str(error))
                        continue
                    if event is not None:
                        self._dispatch(event)
        except asyncio.CancelledError:
            raise
        except ProviderError as error:
            self._fail_subscribers(error)
        except Exception as error:
            self._fail_subscribers(
                ProviderConnectionError(
                    f"stream reader failed: {type(error).__name__}", provider=PROVIDER_NAME
                )
            )

    def _dispatch(self, event: TradeEvent | QuoteEvent) -> None:
        kind = _TRADES if isinstance(event, TradeEvent) else _QUOTES
        queue = self._subscribers.get(kind)
        if queue is None:
            # Nobody asked for this kind. Alpaca sends what was subscribed, so this means a
            # subscriber detached while frames were still in flight.
            return
        try:
            queue.put_nowait(event)
        except asyncio.QueueFull:
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
            self.dropped += 1
            log.warning(
                "alpaca.subscriber_buffer_full",
                kind=kind,
                buffer=_SUBSCRIBER_BUFFER,
                total_dropped=self.dropped,
            )
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(event)

    def _end_subscribers(self) -> None:
        """Tell every subscriber the stream finished, without inventing an error."""
        log.info("alpaca.stream_ended", subscribers=len(self._subscribers))
        for queue in self._subscribers.values():
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(_END)

    def _fail_subscribers(self, error: ProviderError) -> None:
        """Hand the failure to every subscriber so each one reconnects."""
        log.warning(
            "alpaca.stream_failed",
            error=str(error),
            error_type=type(error).__name__,
            subscribers=len(self._subscribers),
        )
        for queue in self._subscribers.values():
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(error)
