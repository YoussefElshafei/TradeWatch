"""The market data provider port.

ADR-0004: all market data enters through this interface, and only the ingestion service ever
imports an implementation. Nothing else in TradeWatch knows a provider exists.

The port is deliberately **narrow**. It is the intersection of what providers offer, not the
union: anything one vendor supports and another does not stays out until a concrete need
appears, because a port that grows to match its first implementation is not an abstraction,
it is that implementation with extra steps.

Providers differ in what they can do at all — a free tier may have historical bars but no
streaming — so capability is data rather than a `NotImplementedError` surprise at runtime.
:class:`ProviderCapabilities` is checked *before* the ingestion service tries to use a
feature.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Protocol, runtime_checkable

from tradewatch.domain.enums import AssetClass, BarInterval
from tradewatch.domain.events import BarEvent, QuoteEvent, TradeEvent

__all__ = [
    "InstrumentSnapshot",
    "MarketDataProvider",
    "PriceSnapshot",
    "ProviderCapabilities",
]


@dataclass(frozen=True, slots=True)
class ProviderCapabilities:
    """What a provider can actually do.

    Checked before use so an unsupported feature is a clear startup decision rather than an
    exception in the middle of a trading session.
    """

    #: Live trade prints over a persistent connection.
    streaming_trades: bool = False
    #: Live top-of-book. Common to have trades without quotes on a free tier.
    streaming_quotes: bool = False
    #: Historical OHLCV bars.
    historical_bars: bool = False
    #: A point-in-time price without opening a stream.
    latest_price: bool = False
    #: The reference instrument list.
    instrument_list: bool = False
    #: Intervals the provider serves natively. Anything else is rolled up from 1m locally.
    intervals: frozenset[BarInterval] = field(default_factory=lambda: frozenset())
    #: How far back history goes, if the provider caps it. None means no known limit.
    max_history: dt.timedelta | None = None
    #: Documented request ceiling, used to size the client-side limiter (failure F2).
    rate_limit_per_minute: int | None = None

    def supports_interval(self, interval: BarInterval) -> bool:
        return interval in self.intervals


@dataclass(frozen=True, slots=True)
class InstrumentSnapshot:
    """Reference data as the provider states it.

    Not the `Instrument` domain entity: this has no id, because the provider does not know
    ours. The ingestion service resolves or creates the row and owns the identity.
    """

    symbol: str
    name: str | None
    asset_class: AssetClass
    exchange: str
    currency: str = "USD"
    tick_size: Decimal = Decimal("0.01")
    lot_size: Decimal = Decimal("1")
    tradable: bool = True
    provider_metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PriceSnapshot:
    """The answer to "what is this trading at right now?".

    ``price`` is whatever the provider considers current — usually the last trade, sometimes
    the mid. ``as_of`` says when, so a stale answer is visible as stale rather than being
    mistaken for a live one.
    """

    symbol: str
    price: Decimal
    as_of: dt.datetime
    source: str
    size: Decimal | None = None
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None

    @property
    def has_book(self) -> bool:
        return self.bid_price is not None and self.ask_price is not None


@runtime_checkable
class MarketDataProvider(Protocol):
    """Everything TradeWatch needs from a market data vendor.

    Implementations raise the exceptions in
    :mod:`tradewatch.infrastructure.providers.errors` and nothing else — a vendor SDK's own
    exception type leaking through this boundary would defeat the abstraction as surely as
    its response format would.

    Streaming methods return an async iterator that yields until cancelled. Reconnection is
    the *caller's* responsibility, through
    :func:`~tradewatch.infrastructure.providers.resilience.reconnecting`, so that the retry
    policy is one implementation rather than one per adapter.
    """

    @property
    def name(self) -> str:
        """Short identifier, stored as `source` on every event this provider produces."""
        ...

    @property
    def capabilities(self) -> ProviderCapabilities: ...

    async def list_instruments(self) -> Sequence[InstrumentSnapshot]:
        """The tradable universe.

        Raises:
            ProviderError: On any failure to obtain it.
        """
        ...

    async def fetch_bars(
        self,
        symbol: str,
        interval: BarInterval,
        start: dt.datetime,
        end: dt.datetime,
    ) -> Sequence[BarEvent]:
        """Historical OHLCV bars in ``[start, end)``, oldest first.

        Half-open so consecutive windows tile without double-counting a bar. Providers
        paginate; an implementation follows the pages and returns the whole range.

        Raises:
            ProviderError: On any failure.
            ValueError: If the interval is not in `capabilities.intervals`.
        """
        ...

    async def fetch_latest_price(self, symbol: str) -> PriceSnapshot | None:
        """The current price, or None if the provider has never seen the symbol trade.

        None is a real answer — a symbol that has not traded today has no last price — and
        it is distinct from an error, which raises.
        """
        ...

    def stream_trades(self, symbols: Sequence[str]) -> AsyncIterator[TradeEvent]:
        """Live trade prints for ``symbols``, until the iterator is cancelled or fails."""
        ...

    def stream_quotes(self, symbols: Sequence[str]) -> AsyncIterator[QuoteEvent]:
        """Live top-of-book for ``symbols``."""
        ...

    async def aclose(self) -> None:
        """Release connections. Safe to call more than once, and on a provider never used."""
        ...
