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
* Trades and quotes multiplex over one socket, which is why both streams share a connection
  and a subscription here.
"""

from __future__ import annotations

import asyncio
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

    # --- port surface ------------------------------------------------------

    @property
    def name(self) -> str:
        return PROVIDER_NAME

    @property
    def capabilities(self) -> ProviderCapabilities:
        return _CAPABILITIES

    async def aclose(self) -> None:
        self._closed = True
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
        """The most recent trade, enriched with the book when the quote is available."""
        wanted = normalize_symbol(symbol, provider=PROVIDER_NAME)
        payload = await self._get(
            f"{self._settings.data_base_url}/v2/stocks/{wanted}/trades/latest",
            params={"feed": self._settings.feed},
            description="fetch_latest_price",
        )
        if not isinstance(payload, dict):
            raise ProviderDataError(
                "latest trade response is not an object", provider=PROVIDER_NAME
            )

        trade = payload.get("trade")
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
        quote = payload.get("quote")
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
        async for event in self._stream(symbols, trades=True, quotes=False):
            if isinstance(event, TradeEvent):
                yield event

    async def stream_quotes(self, symbols: Sequence[str]) -> AsyncIterator[QuoteEvent]:
        async for event in self._stream(symbols, trades=False, quotes=True):
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

    async def _stream(
        self, symbols: Sequence[str], *, trades: bool, quotes: bool
    ) -> AsyncIterator[TradeEvent | QuoteEvent]:
        """One authenticated, subscribed WebSocket session.

        Yields until the socket closes or fails. Reconnection is the caller's job, through
        :func:`~tradewatch.infrastructure.providers.resilience.reconnecting` — one retry
        policy for the whole system rather than one per adapter.
        """
        if self._closed:
            raise ProviderConnectionError("provider is closed", provider=PROVIDER_NAME)

        wanted = [normalize_symbol(symbol, provider=PROVIDER_NAME) for symbol in symbols]
        if not wanted:
            raise ProviderRequestError("no symbols to subscribe to", provider=PROVIDER_NAME)

        connection = await self._open_socket()
        try:
            await self._authenticate(connection)
            await self._subscribe(connection, wanted, trades=trades, quotes=quotes)

            # The read loop is wrapped because the port promises that only ProviderError
            # subclasses cross this boundary. A raw ConnectionResetError escaping here
            # would reach a retry policy that cannot classify it, and so would not retry
            # a failure that is entirely retryable.
            while True:
                try:
                    frame = await anext(aiter(connection))
                except StopAsyncIteration:
                    break
                except asyncio.CancelledError:
                    raise
                except ProviderError:
                    raise
                except Exception as error:
                    raise ProviderConnectionError(
                        f"stream read failed: {type(error).__name__}", provider=PROVIDER_NAME
                    ) from error

                for message in self._decode_frame(frame):
                    event = self._handle_message(message)
                    if event is not None:
                        yield event
        finally:
            closer = getattr(connection, "close", None)
            if closer is not None:
                result = closer()
                if asyncio.iscoroutine(result):
                    await result

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

    async def _subscribe(
        self, connection: Any, symbols: Sequence[str], *, trades: bool, quotes: bool
    ) -> None:
        request: dict[str, Any] = {"action": "subscribe"}
        if trades:
            request["trades"] = list(symbols)
        if quotes:
            request["quotes"] = list(symbols)
        await self._send(connection, request)
        for message in self._decode_frame(await self._receive(connection)):
            self._check_error(message)
        log.info("alpaca.subscribed", symbols=len(symbols), trades=trades, quotes=quotes)

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
