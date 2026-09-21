"""Response schemas for the market data endpoints.

`ARCHITECTURE.md` §8.3: ORM models are never returned directly. Three reasons, and the third
is the one that actually bites:

1. A column rename would silently change the public API.
2. A lazy relationship serialized by accident issues a query per row, after the request's
   transaction has closed.
3. Money would leave as a JSON **number**. Every JavaScript client parses that into an
   IEEE-754 double, so `187.42` arrives as `187.42000000000001` and the exactness the whole
   stack maintains (ADR-0009) is destroyed on the last hop. Serializing as a string is the
   only thing that prevents it, and it has to be deliberate.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer

from tradewatch.domain.enums import AssetClass, BarInterval, InstrumentStatus

__all__ = [
    "BarPoint",
    "BarSeriesResponse",
    "InstrumentListResponse",
    "InstrumentResponse",
    "LatestPriceResponse",
]

Money = Annotated[Decimal, PlainSerializer(str, return_type=str, when_used="json")]
"""A decimal that leaves as a JSON string.

The single most important line in this module. Without it every price in the system is
rounded by the browser's JSON parser.
"""

OptionalMoney = Annotated[
    Decimal | None, PlainSerializer(lambda v: None if v is None else str(v), when_used="json")
]


class ApiModel(BaseModel):
    """Base for every response body."""

    model_config = ConfigDict(from_attributes=True, frozen=True)


class InstrumentResponse(ApiModel):
    """A tradable symbol."""

    symbol: str = Field(description="Upper-case ticker.", examples=["AAPL"])
    name: str | None = Field(default=None, description="Display name, when known.")
    asset_class: AssetClass
    exchange: str
    currency: str = Field(description="ISO 4217 code.", examples=["USD"])
    tick_size: Money = Field(description="Minimum price increment, as a decimal string.")
    lot_size: Money
    status: InstrumentStatus


class InstrumentListResponse(ApiModel):
    """The instrument universe.

    Wrapped in an object rather than returned as a bare array: a top-level JSON array cannot
    grow a `count` or a pagination cursor later without breaking every client.
    """

    count: int
    instruments: list[InstrumentResponse]


class BarPoint(ApiModel):
    """One OHLCV bar.

    Field names are spelled out — `open` rather than `o` — because this is TradeWatch's
    public shape, not a vendor's wire format optimised for bandwidth.
    """

    bar_start: dt.datetime = Field(description="Start of the interval, UTC.")
    open: Money
    high: Money
    low: Money
    close: Money
    volume: Money
    vwap: OptionalMoney = Field(default=None, description="Volume-weighted average price.")
    trade_count: int = Field(ge=0)


class BarSeriesResponse(ApiModel):
    """A bar series, with the query it answers.

    Echoing the resolved window matters: the endpoint fills in defaults for a missing
    `start` or `end`, and a client that cannot see what it actually got would have to guess
    where to put the next request's cursor.
    """

    symbol: str
    interval: BarInterval
    start: dt.datetime = Field(description="Inclusive start of the window that was queried.")
    end: dt.datetime = Field(description="Exclusive end of the window that was queried.")
    count: int = Field(ge=0, description="Bars returned. Zero is a valid answer.")
    bars: list[BarPoint]


class LatestPriceResponse(ApiModel):
    """The most recent price for a symbol, from stored market data.

    Every field that could be stale carries its own timestamp, so a client can tell a live
    price from one that stopped updating an hour ago instead of drawing both identically.
    """

    symbol: str
    price: OptionalMoney = Field(default=None, description="Last trade price, if any.")
    size: OptionalMoney = Field(default=None, description="Last trade size.")
    traded_at: dt.datetime | None = None
    bid_price: OptionalMoney = None
    ask_price: OptionalMoney = None
    quoted_at: dt.datetime | None = None
    source: str | None = Field(default=None, description="Provider the data came from.")

    @property
    def has_data(self) -> bool:
        return self.price is not None or self.bid_price is not None
