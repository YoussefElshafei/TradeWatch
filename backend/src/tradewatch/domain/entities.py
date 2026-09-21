"""Domain entities.

These are Pydantic models, not ORM rows. The separation is not ceremony: `import-linter`
forbids `tradewatch.domain` from importing SQLAlchemy at all (see `pyproject.toml`), so the
rules expressed here — a bar's high is never below its low, a price is positive, a timestamp
is never naive — can be unit-tested with no database in sight, and the strategy and
execution cores in Phase 6 can be written against them without a session.

Repositories map between these and the ORM models in
`tradewatch.infrastructure.database.models`. That mapping is the cost of the separation; the
benefit is that the shape of the database can change without the trading logic noticing, and
that the trading logic is testable in microseconds.

Only entities with a use case are defined. A domain model written for a table nobody reads
yet is speculation, and it goes stale before it is used.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Self
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from tradewatch.domain.enums import (
    AssetClass,
    BarInterval,
    DeploymentStatus,
    InstrumentStatus,
)

__all__ = [
    "Bar",
    "Instrument",
    "NewBar",
    "NewQuote",
    "NewTrade",
    "Portfolio",
    "Position",
    "Quote",
    "Strategy",
    "Trade",
]


def _require_utc(value: datetime) -> datetime:
    """Reject naive datetimes; normalise aware ones to UTC.

    A naive datetime in a market data system is a bug with a delayed fuse: it works all
    year and then produces two 01:30 bars on one October morning. Everything internal is
    UTC (`ARCHITECTURE.md` §4.4), and local time exists only in the browser.
    """
    if value.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware; TradeWatch works in UTC")
    return value.astimezone(UTC)


class DomainModel(BaseModel):
    """Base for every entity: immutable, strict, and UTC-normalising.

    ``frozen=True`` because an entity that can be mutated in place is an entity whose
    history is unverifiable — and reproducibility is the whole claim of ADR-0007.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    @field_validator("*", mode="after")
    @classmethod
    def _normalise_timestamps(cls, value: Any) -> Any:
        if isinstance(value, datetime):
            return _require_utc(value)
        return value


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------


class Instrument(DomainModel):
    """A tradable symbol. The reference row everything else keys off."""

    id: UUID
    symbol: str = Field(min_length=1, max_length=32)
    name: str | None = Field(default=None, max_length=200)
    asset_class: AssetClass
    exchange: str = Field(min_length=1, max_length=32)
    currency: str = Field(min_length=3, max_length=3)
    tick_size: Decimal = Field(gt=0)
    lot_size: Decimal = Field(gt=0)
    status: InstrumentStatus = InstrumentStatus.ACTIVE
    first_seen_at: datetime
    provider_metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("symbol", "exchange", "currency")
    @classmethod
    def _upper(cls, value: str) -> str:
        return value.upper()


# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------


class NewTrade(DomainModel):
    """A trade print on its way in, before the database assigns a surrogate key.

    ``event_id`` is assigned at the ingestion boundary and is the idempotency key end to
    end (`ARCHITECTURE.md` §7.1): the same print delivered twice by Kafka writes one row.

    ``ingested_at`` earlier than ``occurred_at`` is tolerated rather than rejected. Provider
    clocks drift, and dropping real market data because someone else's NTP is off would be
    the worse failure; Phase 10 turns the difference into a lag metric instead.
    """

    event_id: UUID
    instrument_id: UUID
    price: Decimal = Field(gt=0)
    size: Decimal = Field(gt=0)
    occurred_at: datetime
    ingested_at: datetime
    provider: str = Field(min_length=1, max_length=32)
    provider_trade_id: str | None = Field(default=None, max_length=64)


class Trade(NewTrade):
    """A persisted trade print."""

    id: int
    recorded_at: datetime


class NewQuote(DomainModel):
    """A top-of-book snapshot on its way in.

    Note what is *not* validated: that the ask is at or above the bid. Locked and crossed
    markets are real and brief, and a constraint that rejects them would silently drop the
    most interesting ticks of the day.
    """

    event_id: UUID
    instrument_id: UUID
    bid_price: Decimal = Field(gt=0)
    bid_size: Decimal = Field(ge=0)
    ask_price: Decimal = Field(gt=0)
    ask_size: Decimal = Field(ge=0)
    occurred_at: datetime
    ingested_at: datetime
    provider: str = Field(min_length=1, max_length=32)

    @property
    def spread(self) -> Decimal:
        return self.ask_price - self.bid_price

    @property
    def mid_price(self) -> Decimal:
        return (self.ask_price + self.bid_price) / 2


class Quote(NewQuote):
    id: int
    recorded_at: datetime


class NewBar(DomainModel):
    """An OHLCV bar on its way in.

    The invariants below are the ones a bad aggregator breaks, so they are checked here as
    well as in the database. Two layers is not duplication: the database protects the data
    from every writer including a future one, and this protects the aggregator from
    shipping a bad bar in the first place, with a readable error instead of an
    `IntegrityError`.
    """

    instrument_id: UUID
    interval: BarInterval
    bar_start: datetime
    open: Decimal = Field(gt=0)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)
    volume: Decimal = Field(ge=0)
    vwap: Decimal | None = Field(default=None, gt=0)
    trade_count: int = Field(ge=0)
    is_final: bool = True
    revision: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _ohlc_is_coherent(self) -> Self:
        if self.high < self.low:
            raise ValueError(f"high {self.high} is below low {self.low}")
        if not (self.low <= self.open <= self.high):
            raise ValueError(f"open {self.open} is outside the low-high range")
        if not (self.low <= self.close <= self.high):
            raise ValueError(f"close {self.close} is outside the low-high range")
        return self

    @model_validator(mode="after")
    def _bar_start_is_aligned(self) -> Self:
        """A bar must start on a boundary of its own interval.

        A 1m bar starting at 09:30:17 means the aggregator lost track of the clock, and
        every rollup built on it would inherit the drift.
        """
        duration = self.interval.duration
        if duration.total_seconds() < 86400:
            seconds = int(duration.total_seconds())
            offset = int(self.bar_start.timestamp()) % seconds
            if offset or self.bar_start.microsecond:
                raise ValueError(
                    f"bar_start {self.bar_start.isoformat()} is not aligned to {self.interval}"
                )
        return self

    @property
    def end(self) -> datetime:
        return self.bar_start + self.interval.duration

    @property
    def range(self) -> Decimal:
        return self.high - self.low


class Bar(NewBar):
    """A persisted bar. Identified by (instrument, interval, start) — no surrogate key."""

    recorded_at: datetime


# ---------------------------------------------------------------------------
# Trading
# ---------------------------------------------------------------------------


class Strategy(DomainModel):
    """A user-owned, parameterized configuration of a built-in strategy type.

    ``parameters`` is an untyped mapping here and JSONB in the database. The per-type
    schema that validates it arrives with the strategy implementations in Phase 6 (ADR-0006);
    storing it as an opaque document until then is honest, and storing it as columns would
    mean a migration for every new strategy type.
    """

    id: UUID
    user_id: UUID
    name: str = Field(min_length=1, max_length=100)
    strategy_type: str = Field(min_length=1, max_length=50)
    parameters: dict[str, Any] = Field(default_factory=dict)
    symbols: list[str] = Field(min_length=1)
    is_archived: bool = False
    created_at: datetime
    updated_at: datetime

    @field_validator("symbols")
    @classmethod
    def _upper_symbols(cls, value: list[str]) -> list[str]:
        return [symbol.strip().upper() for symbol in value if symbol.strip()]


class Portfolio(DomainModel):
    """A simulated account. No real money passes through it, ever."""

    id: UUID
    user_id: UUID
    name: str = Field(min_length=1, max_length=100)
    base_currency: str = Field(min_length=3, max_length=3)
    initial_cash: Decimal = Field(gt=0)
    cash_balance: Decimal
    created_at: datetime

    @field_validator("base_currency")
    @classmethod
    def _upper(cls, value: str) -> str:
        return value.upper()


class Position(DomainModel):
    """A holding, derived from fills.

    Stored for query speed, but `fills` is the truth: a rebuild job must be able to
    regenerate every position from fills alone (`ARCHITECTURE.md` §6.2), and there is a
    test that it can.

    ``quantity`` may be negative. Short selling is deferred (D16), but a schema that makes
    it impossible would need a migration rather than a feature flag when it arrives.
    """

    portfolio_id: UUID
    instrument_id: UUID
    quantity: Decimal
    avg_entry_price: Decimal = Field(ge=0)
    realized_pnl: Decimal
    updated_at: datetime

    @property
    def is_open(self) -> bool:
        return self.quantity != 0

    @property
    def is_short(self) -> bool:
        return self.quantity < 0

    def market_value(self, price: Decimal) -> Decimal:
        """Value of the holding at ``price``. Negative for a short position."""
        return self.quantity * price

    def unrealized_pnl(self, price: Decimal) -> Decimal:
        """Profit that would be realised by closing the position at ``price``."""
        return (price - self.avg_entry_price) * self.quantity


class Deployment(DomainModel):
    """A strategy bound to a portfolio and made live for paper trading."""

    id: UUID
    strategy_id: UUID
    portfolio_id: UUID
    status: DeploymentStatus
    position_sizing: dict[str, Any] = Field(default_factory=dict)
    risk_limits: dict[str, Any] = Field(default_factory=dict)
    execution_config: dict[str, Any] = Field(default_factory=dict)
    started_at: datetime | None = None
    stopped_at: datetime | None = None
    error_message: str | None = None
    created_at: datetime

    @property
    def is_active(self) -> bool:
        return self.status is DeploymentStatus.RUNNING
