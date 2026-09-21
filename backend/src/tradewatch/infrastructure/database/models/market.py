"""Reference data and market data tables.

These four tables carry the data volume. `instruments` is small and read constantly;
`trades` and `quotes` are append-only and grow fastest; `bars` is the read surface for
every chart and every backtest.

**Idempotency.** `trades` and `quotes` each carry a surrogate `id` *and* a unique
`event_id`. The surrogate key is there because a bigint is eight bytes against a UUID's
sixteen, repeated across every index on the largest tables in the system. The unique
`event_id` is the idempotency guard: Kafka delivers at least once
(`ARCHITECTURE.md` §7.4), so the same print arrives twice after any consumer restart, and
`ON CONFLICT (event_id) DO NOTHING` turns that from a data-corruption problem into a no-op.

**`bars` has no surrogate key.** Its identity *is* (instrument, interval, start) — that is
what uniqueness means for a bar — so the natural composite key is the primary key. It also
happens to be exactly the index the chart and backtest queries want, so the table's main
read path is an index-only scan on the primary key rather than a lookup through a
surrogate.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    Numeric,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tradewatch.domain.enums import AssetClass, BarInterval, InstrumentStatus
from tradewatch.infrastructure.database.base import (
    MONEY_PRECISION,
    MONEY_SCALE,
    Base,
    TimestampedMixin,
    UUIDPrimaryKey,
)
from tradewatch.infrastructure.database.types import (
    asset_class_enum,
    bar_interval_enum,
    instrument_status_enum,
)

if TYPE_CHECKING:
    from tradewatch.infrastructure.database.models.detection import AnomalyDetection


class Instrument(UUIDPrimaryKey, TimestampedMixin, Base):
    """A tradable symbol.

    Deliberately *not* deletable in practice: every foreign key pointing here uses
    ``ON DELETE RESTRICT``. Market data is the system of record (ADR-0002), and a symbol
    that stops trading becomes ``DELISTED`` rather than disappearing along with its price
    history.
    """

    __tablename__ = "instruments"

    symbol: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    name: Mapped[str | None] = mapped_column(String(200))
    asset_class: Mapped[AssetClass] = mapped_column(asset_class_enum, nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    tick_size: Mapped[Decimal] = mapped_column(nullable=False)
    lot_size: Mapped[Decimal] = mapped_column(nullable=False, server_default="1")
    status: Mapped[InstrumentStatus] = mapped_column(
        instrument_status_enum, nullable=False, server_default=InstrumentStatus.ACTIVE.value
    )
    first_seen_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    provider_metadata: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )

    trades: Mapped[list[Trade]] = relationship(back_populates="instrument")
    quotes: Mapped[list[Quote]] = relationship(back_populates="instrument")
    bars: Mapped[list[Bar]] = relationship(back_populates="instrument")
    anomaly_detections: Mapped[list[AnomalyDetection]] = relationship(back_populates="instrument")

    __table_args__ = (
        CheckConstraint("tick_size > 0", name="tick_size_positive"),
        CheckConstraint("lot_size > 0", name="lot_size_positive"),
        CheckConstraint("char_length(currency) = 3", name="currency_is_iso_code"),
        CheckConstraint("symbol = upper(symbol)", name="symbol_is_upper"),
    )


class Trade(Base):
    """An executed trade print.

    Append-only. A trade that was busted by the exchange is corrected by a later event,
    never by mutating this row — the same reasoning that gives bars a revision rather than
    an in-place edit.
    """

    __tablename__ = "trades"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(nullable=False, unique=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("instruments.id", ondelete="RESTRICT"), nullable=False
    )
    price: Mapped[Decimal] = mapped_column(nullable=False)
    size: Mapped[Decimal] = mapped_column(nullable=False)

    occurred_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingested_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    provider: Mapped[str] = mapped_column(String(32), nullable=False)
    provider_trade_id: Mapped[str | None] = mapped_column(String(64))

    instrument: Mapped[Instrument] = relationship(back_populates="trades")

    __table_args__ = (
        CheckConstraint("price > 0", name="price_positive"),
        CheckConstraint("size > 0", name="size_positive"),
        # The query behind every tape and every backfill gap check: one symbol, newest
        # first, over a time range. DESC matches the read direction so the planner can walk
        # the index backwards without a sort.
        Index("ix_trades_instrument_id_occurred_at", "instrument_id", "occurred_at"),
    )


class Quote(Base):
    """A top-of-book snapshot.

    No constraint requires the ask to be at or above the bid. Locked and crossed markets
    are real, brief, and precisely the moments worth keeping — a tidier-looking constraint
    here would silently discard the most interesting ticks of the day.
    """

    __tablename__ = "quotes"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    event_id: Mapped[uuid.UUID] = mapped_column(nullable=False, unique=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("instruments.id", ondelete="RESTRICT"), nullable=False
    )

    bid_price: Mapped[Decimal] = mapped_column(nullable=False)
    bid_size: Mapped[Decimal] = mapped_column(nullable=False)
    ask_price: Mapped[Decimal] = mapped_column(nullable=False)
    ask_size: Mapped[Decimal] = mapped_column(nullable=False)

    occurred_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ingested_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    recorded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    provider: Mapped[str] = mapped_column(String(32), nullable=False)

    instrument: Mapped[Instrument] = relationship(back_populates="quotes")

    __table_args__ = (
        CheckConstraint("bid_price > 0", name="bid_price_positive"),
        CheckConstraint("ask_price > 0", name="ask_price_positive"),
        CheckConstraint("bid_size >= 0", name="bid_size_not_negative"),
        CheckConstraint("ask_size >= 0", name="ask_size_not_negative"),
        Index("ix_quotes_instrument_id_occurred_at", "instrument_id", "occurred_at"),
    )


class Bar(Base):
    """An OHLCV bar over one interval.

    A closed bar is immutable. When a trade arrives after its bar has been published, the
    aggregator emits a correction and ``revision`` increments, so a consumer that already
    acted on revision 0 can tell that something changed rather than finding history
    rewritten beneath it (`ARCHITECTURE.md` F11).
    """

    __tablename__ = "bars"

    instrument_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("instruments.id", ondelete="RESTRICT"), primary_key=True
    )
    interval: Mapped[BarInterval] = mapped_column(bar_interval_enum, primary_key=True)
    bar_start: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), primary_key=True)

    open: Mapped[Decimal] = mapped_column(nullable=False)
    high: Mapped[Decimal] = mapped_column(nullable=False)
    low: Mapped[Decimal] = mapped_column(nullable=False)
    close: Mapped[Decimal] = mapped_column(nullable=False)
    volume: Mapped[Decimal] = mapped_column(nullable=False)
    vwap: Mapped[Decimal | None] = mapped_column(Numeric(MONEY_PRECISION, MONEY_SCALE))
    trade_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    is_final: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    revision: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    recorded_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    instrument: Mapped[Instrument] = relationship(back_populates="bars")

    __table_args__ = (
        # The OHLC relationships. A bar that breaks one of these is not a bar, and a
        # backtest run on it would produce a confident, wrong number — so the database
        # refuses it regardless of which writer produced it.
        CheckConstraint("high >= low", name="high_not_below_low"),
        CheckConstraint("open BETWEEN low AND high", name="open_within_range"),
        CheckConstraint("close BETWEEN low AND high", name="close_within_range"),
        CheckConstraint("open > 0 AND high > 0 AND low > 0 AND close > 0", name="prices_positive"),
        CheckConstraint("volume >= 0", name="volume_not_negative"),
        CheckConstraint("trade_count >= 0", name="trade_count_not_negative"),
        CheckConstraint("revision >= 0", name="revision_not_negative"),
        # Cross-sectional reads: every symbol's bars over one window, which is what the
        # anomaly detector's feature pass and any market-wide view need. The primary key
        # cannot serve this, because its leading column is the instrument.
        Index("ix_bars_interval_bar_start", "interval", "bar_start"),
    )
