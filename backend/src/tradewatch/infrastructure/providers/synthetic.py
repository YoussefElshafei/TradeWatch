"""A market data provider that invents its own data, deterministically.

ADR-0004 makes this the default (`TW_PROVIDER=synthetic`) and the input for all development,
CI, and load testing. No API key, no network, no quota, no flakiness, and — given the same
`TW_SYNTHETIC_SEED` — the same data every time.

**What it is not.** The distributions here are invented. A strategy that works on this
proves nothing about real markets, and Phase 8's model evaluation uses real historical data
for exactly that reason. What this *does* prove is that the pipeline works, and it can be
asked for things real data will not supply on demand: a volume spike at bar 300, a
connection drop after 50 messages, a malformed frame, a duplicate print.

The price path is a geometric random walk with a per-symbol volatility. That is not a claim
about market microstructure; it is the simplest process that produces positive prices and
plausible-looking bars.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import random
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from tradewatch.application.ports.market_data import (
    InstrumentSnapshot,
    PriceSnapshot,
    ProviderCapabilities,
)
from tradewatch.domain.enums import AssetClass, BarInterval
from tradewatch.domain.events import BarEvent, QuoteEvent, TradeEvent
from tradewatch.infrastructure.providers._walk import bar_level, unit_noise
from tradewatch.infrastructure.providers.errors import (
    ProviderConnectionError,
    ProviderDataError,
    ProviderRequestError,
)
from tradewatch.infrastructure.providers.normalization import EventFactory
from tradewatch.observability.logging import get_logger

__all__ = ["SyntheticProvider", "SyntheticScenario"]

log = get_logger(__name__)

PROVIDER_NAME = "synthetic"

_PRICE_QUANTUM = Decimal("0.01")
_CAPABILITIES = ProviderCapabilities(
    streaming_trades=True,
    streaming_quotes=True,
    historical_bars=True,
    latest_price=True,
    instrument_list=True,
    intervals=frozenset(BarInterval),
    max_history=None,
    rate_limit_per_minute=None,
)


@dataclass(frozen=True, slots=True)
class SyntheticScenario:
    """Failures and market conditions to inject, on demand.

    This is the part real data cannot provide. Failure scenarios F1 and F3 need a stream
    that drops when asked and a frame that is malformed on purpose; waiting for a provider
    to misbehave is not a test strategy.

    Attributes:
        drop_after: Raise a connection error after this many messages, simulating a socket
            that closes mid-session.
        malformed_at: Emit an unparseable message at these 0-based positions.
        duplicate_at: Re-emit the previous message at these positions, so the deduplicator
            has something real to deduplicate.
        volume_spike_at: Multiply size by `volume_spike_factor` at these positions.
        gap_at: Jump the price by `gap_pct` at these positions.
        stall_after: Stop emitting — without erroring — after this many messages. The
            nastiest failure: the connection looks healthy and the data silently stops.
    """

    drop_after: int | None = None
    malformed_at: frozenset[int] = field(default_factory=frozenset)
    duplicate_at: frozenset[int] = field(default_factory=frozenset)
    volume_spike_at: frozenset[int] = field(default_factory=frozenset)
    volume_spike_factor: int = 50
    gap_at: frozenset[int] = field(default_factory=frozenset)
    gap_pct: Decimal = Decimal("0.05")
    stall_after: int | None = None


@dataclass(slots=True)
class _SymbolState:
    """One symbol's walk. Kept separate so symbols are independent and reproducible."""

    price: Decimal
    volatility: float
    #: How far this symbol swings relative to the others, shared by the tape and the bars.
    volatility_scale: float
    rng: random.Random
    #: Draws quote sizes only. Trades and quotes run as two independent, concurrently
    #: scheduled asyncio tasks (workers/ingestion.py) reading the *same* `_SymbolState`.
    #: If quote sizing drew from `rng` too, the interleaving of the two tasks — which
    #: depends on real-time scheduling, not on the seed — would perturb what `rng.gauss()`
    #: returns to the trade walk next, breaking the "same seed, same stream" guarantee
    #: (ADR-0004) the moment both streams actually run together instead of in isolation,
    #: as every existing test does.
    quote_rng: random.Random
    sequence: int = 0


class SyntheticProvider:
    """A deterministic fake market.

    Implements :class:`~tradewatch.application.ports.market_data.MarketDataProvider`.
    """

    def __init__(
        self,
        symbols: Sequence[str],
        *,
        seed: int = 42,
        start_price: Decimal = Decimal("100"),
        volatility: float = 0.0015,
        interval_seconds: float = 0.0,
        scenario: SyntheticScenario | None = None,
        factory: EventFactory | None = None,
    ) -> None:
        if not symbols:
            raise ValueError("SyntheticProvider needs at least one symbol")
        self._symbols = tuple(symbol.strip().upper() for symbol in symbols)
        self._seed = seed
        self._start_price = start_price
        self._volatility = volatility
        #: Delay between emitted messages. 0 in tests, so a hundred ticks take no time.
        self._interval_seconds = interval_seconds
        self._scenario = scenario or SyntheticScenario()
        self._factory = factory or EventFactory(source=PROVIDER_NAME, producer="synthetic-provider")
        self._closed = False
        self._states: dict[str, _SymbolState] = {}
        self._reset_states()

    # --- port surface ------------------------------------------------------

    @property
    def name(self) -> str:
        return PROVIDER_NAME

    @property
    def capabilities(self) -> ProviderCapabilities:
        return _CAPABILITIES

    @property
    def symbols(self) -> tuple[str, ...]:
        return self._symbols

    async def list_instruments(self) -> Sequence[InstrumentSnapshot]:
        return [
            InstrumentSnapshot(
                symbol=symbol,
                name=f"Synthetic {symbol}",
                asset_class=AssetClass.EQUITY,
                exchange="XTST",
                currency="USD",
                tick_size=_PRICE_QUANTUM,
                lot_size=Decimal("1"),
                provider_metadata={"synthetic": True, "seed": self._seed},
            )
            for symbol in self._symbols
        ]

    async def fetch_bars(
        self,
        symbol: str,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> Sequence[BarEvent]:
        """Generate bars over ``[start, end)``.

        Deterministic in the *range*, not just in the seed: asking for the same window twice
        gives identical bars, and asking for an overlapping window gives identical bars on
        the overlap. Without that, a backfill re-run would produce different history for the
        same minutes and the idempotent upsert would have nothing to be idempotent about.
        """
        self._require_open()
        symbol = symbol.strip().upper()
        if symbol not in self._symbols:
            raise ProviderRequestError(f"unknown symbol {symbol!r}", provider=PROVIDER_NAME)
        if not self.capabilities.supports_interval(interval):
            raise ValueError(f"{PROVIDER_NAME} does not serve {interval} bars")
        if end <= start:
            raise ProviderRequestError(
                f"end {end.isoformat()} is not after start {start.isoformat()}",
                provider=PROVIDER_NAME,
            )

        duration = interval.duration
        aligned = self._align(start, duration)
        bars: list[BarEvent] = []
        index = 0
        cursor = aligned
        while cursor < end:
            bars.append(self._bar_for(symbol, interval, cursor))
            cursor += duration
            index += 1
            if index > 250_000:  # a decade of minutes; beyond this someone has a bug
                raise ProviderRequestError("requested range is too large", provider=PROVIDER_NAME)
        return bars

    async def fetch_latest_price(self, symbol: str) -> PriceSnapshot | None:
        self._require_open()
        symbol = symbol.strip().upper()
        if symbol not in self._symbols:
            raise ProviderRequestError(f"unknown symbol {symbol!r}", provider=PROVIDER_NAME)
        state = self._states[symbol]
        spread = self._spread(state.price)
        return PriceSnapshot(
            symbol=symbol,
            price=state.price,
            as_of=self._factory.now(),
            source=PROVIDER_NAME,
            size=Decimal(state.rng.randint(1, 500)),
            bid_price=self._round(state.price - spread / 2),
            ask_price=self._round(state.price + spread / 2),
        )

    async def stream_trades(self, symbols: Sequence[str]) -> AsyncIterator[TradeEvent]:
        """Yield trade prints until cancelled, or until the scenario says otherwise."""
        async for event in self._stream(symbols, kind="trade"):
            assert isinstance(event, TradeEvent)
            yield event

    async def stream_quotes(self, symbols: Sequence[str]) -> AsyncIterator[QuoteEvent]:
        async for event in self._stream(symbols, kind="quote"):
            assert isinstance(event, QuoteEvent)
            yield event

    async def aclose(self) -> None:
        self._closed = True

    def reset(self) -> None:
        """Rewind to the seed state, so a reconnect replays identically."""
        self._reset_states()
        self._closed = False

    # --- internals ---------------------------------------------------------

    def _require_open(self) -> None:
        if self._closed:
            raise ProviderConnectionError("provider is closed", provider=PROVIDER_NAME)

    def _reset_states(self) -> None:
        self._states = {}
        for symbol in self._symbols:
            # Seeded per symbol, so adding a symbol does not change any other symbol's
            # series — otherwise every fixture changes when the watchlist does.
            rng = random.Random(f"{self._seed}:{symbol}")  # noqa: S311
            quote_rng = random.Random(f"{self._seed}:{symbol}:quote")  # noqa: S311
            volatility_scale = rng.uniform(0.6, 1.5)
            # Anchor the tape to wherever the minute-bar series is right now. Trades and
            # bars are independent until the Phase 3 aggregator derives one from the other,
            # and without this anchor the "latest price" and the chart would disagree by
            # whatever the bar series had drifted.
            now_index = int(dt.datetime.now(dt.UTC).timestamp()) // 60
            self._states[symbol] = _SymbolState(
                price=self._round(
                    bar_level(
                        f"{self._seed}:{symbol}:1m",
                        now_index,
                        base=self._start_price,
                        volatility=volatility_scale,
                    )
                ),
                volatility=self._volatility * volatility_scale,
                volatility_scale=volatility_scale,
                rng=rng,
                quote_rng=quote_rng,
            )

    @staticmethod
    def _align(moment: dt.datetime, duration: dt.timedelta) -> dt.datetime:
        seconds = int(duration.total_seconds())
        epoch_seconds = int(moment.timestamp())
        return dt.datetime.fromtimestamp(epoch_seconds - epoch_seconds % seconds, tz=dt.UTC)

    @staticmethod
    def _round(value: Decimal) -> Decimal:
        return value.quantize(_PRICE_QUANTUM)

    @staticmethod
    def _spread(price: Decimal) -> Decimal:
        return max(_PRICE_QUANTUM, (price * Decimal("0.0002")).quantize(_PRICE_QUANTUM))

    def _bar_for(self, symbol: str, interval: BarInterval, bar_start: dt.datetime) -> BarEvent:
        """A bar that depends only on (seed, symbol, interval, bar_start).

        The level comes from :mod:`._walk`, which is continuous in the bar index and
        addressable at any point. Two consequences, both of which the pipeline relies on:

        * one bar's close is the next bar's open, so a chart of the series reads as a price
          path rather than as noise;
        * asking for the same window twice — or for two overlapping windows — returns
          identical bars, which is what makes an overlapping backfill deduplicate instead
          of rewriting history with different numbers.
        """
        state = self._states[symbol]
        key = f"{self._seed}:{symbol}:{interval.value}"
        seconds = int(interval.duration.total_seconds())
        index = int(bar_start.timestamp()) // seconds

        open_price = self._round(
            bar_level(key, index, base=self._start_price, volatility=state.volatility_scale)
        )
        close_price = self._round(
            bar_level(key, index + 1, base=self._start_price, volatility=state.volatility_scale)
        )

        # Intrabar extremes: the wick beyond the body. Derived from the bar's own
        # coordinates so it is reproducible like everything else.
        body_high = max(open_price, close_price)
        body_low = min(open_price, close_price)
        upper = Decimal(str(round(unit_noise(key, index, "high") * 0.004, 8)))
        lower = Decimal(str(round(unit_noise(key, index, "low") * 0.004, 8)))
        high = self._round(body_high * (Decimal(1) + upper))
        low = self._round(body_low * (Decimal(1) - lower))

        # Rounding can collapse a tiny wick back inside the body; clamp so the OHLC
        # invariants hold by construction rather than by luck.
        high = max(high, body_high)
        low = min(low, body_low)

        volume = Decimal(1_000 + int(unit_noise(key, index, "volume") * 49_000))
        trade_count = 10 + int(unit_noise(key, index, "count") * 390)
        vwap = self._round((high + low + close_price) / 3)

        return self._factory.bar(
            symbol=symbol,
            interval=interval,
            bar_start=bar_start,
            open=open_price,
            high=high,
            low=low,
            close=close_price,
            volume=volume,
            vwap=vwap,
            trade_count=trade_count,
        )

    def _step(self, state: _SymbolState, *, gap: bool) -> Decimal:
        move = state.rng.gauss(0, state.volatility)
        if gap:
            direction = 1 if state.rng.random() < 0.5 else -1
            move += direction * float(self._scenario.gap_pct)
        stepped = state.price * Decimal(str(round(1 + move, 8)))
        state.price = max(self._round(stepped), _PRICE_QUANTUM)
        return state.price

    async def _stream(
        self, symbols: Sequence[str], *, kind: str
    ) -> AsyncIterator[TradeEvent | QuoteEvent]:
        """The shared body of both streams, including scenario injection."""
        self._require_open()
        wanted = [symbol.strip().upper() for symbol in symbols]
        unknown = sorted(set(wanted) - set(self._symbols))
        if unknown:
            raise ProviderRequestError(
                f"unknown symbols: {', '.join(unknown)}", provider=PROVIDER_NAME
            )

        emitted = 0
        previous: TradeEvent | QuoteEvent | None = None
        scenario = self._scenario

        while True:
            if scenario.stall_after is not None and emitted >= scenario.stall_after:
                # A silent stall: still connected, no data. The failure a staleness monitor
                # exists to catch, because nothing else notices it.
                log.warning("synthetic.stalled", emitted=emitted, after=scenario.stall_after)
                await asyncio.sleep(3600)
                continue

            if scenario.drop_after is not None and emitted >= scenario.drop_after:
                raise ProviderConnectionError(
                    f"synthetic stream dropped after {emitted} messages", provider=PROVIDER_NAME
                )

            if emitted in scenario.malformed_at:
                emitted += 1
                raise ProviderDataError(
                    "synthetic malformed message",
                    provider=PROVIDER_NAME,
                    raw='{"broken": ',
                )

            if emitted in scenario.duplicate_at and previous is not None:
                emitted += 1
                yield previous
                continue

            symbol = wanted[emitted % len(wanted)]
            state = self._states[symbol]
            if kind == "trade":
                state.sequence += 1
                price = self._step(state, gap=emitted in scenario.gap_at)
                size = Decimal(state.rng.randint(1, 500))
            else:
                # Quotes wrap a spread around the *current* price instead of advancing
                # the walk themselves (see `_SymbolState.quote_rng`) — only a trade print
                # moves "the" price. A quote stream and a trade stream run concurrently at
                # independent, unthrottled rates; if both called `_step`, whichever one
                # iterated faster would silently dominate the shared walk, and "last trade"
                # and "latest quote" would end up sampled from wildly different points of
                # an artificially high-frequency series instead of tracking one number.
                price = state.price
                size = Decimal(state.quote_rng.randint(1, 500))
            if emitted in scenario.volume_spike_at:
                size *= scenario.volume_spike_factor

            occurred_at = self._factory.now()
            if kind == "trade":
                event: TradeEvent | QuoteEvent = self._factory.trade(
                    symbol=symbol,
                    price=price,
                    size=size,
                    occurred_at=occurred_at,
                    # A stable per-symbol sequence, so a replay of the same stream
                    # deduplicates instead of looking like new prints.
                    provider_trade_id=f"{symbol}-{state.sequence}",
                )
            else:
                spread = self._spread(price)
                event = self._factory.quote(
                    symbol=symbol,
                    bid_price=self._round(price - spread / 2),
                    bid_size=size,
                    ask_price=self._round(price + spread / 2),
                    ask_size=size,
                    occurred_at=occurred_at,
                )

            previous = event
            emitted += 1
            yield event

            if self._interval_seconds:
                await asyncio.sleep(self._interval_seconds)
