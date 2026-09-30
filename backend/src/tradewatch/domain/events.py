"""The normalized internal market data format.

Everything downstream of ingestion — the processor, the aggregator, the strategy engine,
the detector — sees only these types. No vendor's field names, no vendor's timestamp
format, no vendor's idea of what a trade condition code means. That isolation is the whole
point of ADR-0004: a provider swap should change one adapter, not the system.

The envelope follows `ARCHITECTURE.md` §7.1, with one addition. §7.1 has `producer` — which
service emitted the event. These carry `source` as well — which provider the data came
*from*. They are genuinely different questions: "the ingestion service wrote this" and "this
came off Alpaca's IEX feed" answer different debugging problems, and after a provider swap
the distinction is the only way to tell which rows came from where.

**Event identity is deterministic** (ADR-0012). `event_id` is a UUIDv5 over a natural key
built from the provider's own data, so the same trade normalized twice — after a reconnect,
after a backfill overlapping live data, after a consumer replay — produces the same id both
times. That is what makes `UNIQUE (trades.event_id)` able to absorb duplicates that
originate at the *provider*, not just duplicates introduced by Kafka.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal, Self

from pydantic import Field, model_validator

from tradewatch.domain.entities import DomainModel
from tradewatch.domain.enums import AlertSeverity, BarInterval

__all__ = [
    "TRADEWATCH_NAMESPACE",
    "AlertEvent",
    "AnomalyEvent",
    "BarEvent",
    "EventType",
    "MarketEvent",
    "OrderFilledEvent",
    "QuoteEvent",
    "TradeEvent",
    "deterministic_event_id",
]

#: Namespace for UUIDv5 event identifiers. Fixed forever: changing it would make every
#: previously stored event_id unreachable, and deduplication would silently stop working.
TRADEWATCH_NAMESPACE = uuid.UUID("6f1b6a22-3f2a-4b7a-9a0f-2f5a6b1c9d10")

#: Separator for natural-key components. Chosen because it cannot appear in a symbol, an
#: ISO timestamp, or a decimal — so two different keys can never collide by concatenation.
_KEY_SEPARATOR = "|"


class EventType(StrEnum):
    """`ARCHITECTURE.md` §7.3."""

    TRADE = "market.trade"
    QUOTE = "market.quote"
    BAR_CLOSED = "market.bar.closed"
    BAR_CORRECTED = "market.bar.corrected"
    ANOMALY_DETECTED = "anomaly.detected"
    ORDER_FILLED = "order.filled"
    ALERT_RAISED = "alert.raised"


def deterministic_event_id(*parts: object) -> uuid.UUID:
    """Build a stable event id from a natural key.

    The same inputs always produce the same id, on any machine, in any process, forever.
    That is the property the whole deduplication story rests on.

    ``None`` components are rendered as an empty string rather than skipped, so
    ``(symbol, None, price)`` and ``(symbol, price)`` cannot collide.
    """
    key = _KEY_SEPARATOR.join("" if part is None else str(part) for part in parts)
    return uuid.uuid5(TRADEWATCH_NAMESPACE, key)


class MarketEvent(DomainModel):
    """Fields every market event carries.

    Three timestamps, never conflated (`ARCHITECTURE.md` §4.4): ``occurred_at`` is the
    venue's, ``ingested_at`` is ours, and ``recorded_at`` belongs to the database. All are
    timezone-aware and normalised to UTC by :class:`~tradewatch.domain.entities.DomainModel`.
    """

    event_id: uuid.UUID
    event_type: EventType
    schema_version: int = Field(default=1, ge=1)

    symbol: str = Field(min_length=1, max_length=32)
    occurred_at: datetime
    ingested_at: datetime

    #: Which provider the data came from — `synthetic`, `alpaca`, `replay`.
    source: str = Field(min_length=1, max_length=32)
    #: Which process emitted the event, with its version.
    producer: str = Field(min_length=1, max_length=64)
    #: Inherited by every derived event, so a browser alert traces back to a trade print.
    correlation_id: uuid.UUID

    @property
    def partition_key(self) -> str:
        """The Kafka key (Phase 2). Keying by symbol is what guarantees per-symbol order."""
        return self.symbol

    @property
    def lag_seconds(self) -> float:
        """How far behind the venue we were when this arrived.

        Negative when the provider's clock runs ahead of ours, which happens and is
        tolerated — Phase 10 turns this into a metric rather than an error.
        """
        return (self.ingested_at - self.occurred_at).total_seconds()


class TradeEvent(MarketEvent):
    """An executed trade print."""

    event_type: Literal[EventType.TRADE] = EventType.TRADE
    price: Decimal = Field(gt=0)
    size: Decimal = Field(gt=0)
    provider_trade_id: str | None = Field(default=None, max_length=64)
    #: Venue condition codes, kept verbatim. Interpreting them is venue-specific and no
    #: current consumer needs it; discarding them would make that impossible later.
    conditions: tuple[str, ...] = ()

    @property
    def notional(self) -> Decimal:
        return self.price * self.size

    @classmethod
    def natural_key(
        cls,
        *,
        source: str,
        symbol: str,
        occurred_at: datetime,
        price: Decimal,
        size: Decimal,
        provider_trade_id: str | None,
    ) -> tuple[object, ...]:
        """The identity of a trade, as the provider describes it.

        A provider trade id is used alone when there is one, because it is the venue's own
        statement that two prints are the same print. Without one, the fallback is the
        tuple that makes a print observably distinct. Two genuinely separate trades of the
        same size at the same price in the same microsecond would collapse into one event —
        accepted, because nothing in the feed distinguishes them either.
        """
        if provider_trade_id:
            return (source, "trade", symbol, provider_trade_id)
        return (source, "trade", symbol, occurred_at.isoformat(), price, size)


class QuoteEvent(MarketEvent):
    """A top-of-book snapshot.

    No validator requires the ask to be at or above the bid: crossed and locked markets are
    real, brief, and exactly the moments worth keeping.
    """

    event_type: Literal[EventType.QUOTE] = EventType.QUOTE
    bid_price: Decimal = Field(gt=0)
    bid_size: Decimal = Field(ge=0)
    ask_price: Decimal = Field(gt=0)
    ask_size: Decimal = Field(ge=0)

    @property
    def spread(self) -> Decimal:
        return self.ask_price - self.bid_price

    @property
    def mid_price(self) -> Decimal:
        return (self.bid_price + self.ask_price) / 2

    @classmethod
    def natural_key(
        cls,
        *,
        source: str,
        symbol: str,
        occurred_at: datetime,
        bid_price: Decimal,
        bid_size: Decimal,
        ask_price: Decimal,
        ask_size: Decimal,
    ) -> tuple[object, ...]:
        """Quotes rarely carry an id, so the whole book state is the key.

        Including the sizes matters: the same prices with different sizes is a new quote,
        and treating it as a duplicate would hide book activity.
        """
        return (
            source,
            "quote",
            symbol,
            occurred_at.isoformat(),
            bid_price,
            bid_size,
            ask_price,
            ask_size,
        )


class BarEvent(MarketEvent):
    """A closed OHLCV bar, or a correction to one.

    ``occurred_at`` is the bar's *end*, because that is when the bar became a fact. The
    start is kept separately, since that is what identifies the bar.
    """

    event_type: Literal[EventType.BAR_CLOSED, EventType.BAR_CORRECTED] = EventType.BAR_CLOSED
    interval: BarInterval
    bar_start: datetime
    open: Decimal = Field(gt=0)
    high: Decimal = Field(gt=0)
    low: Decimal = Field(gt=0)
    close: Decimal = Field(gt=0)
    volume: Decimal = Field(ge=0)
    vwap: Decimal | None = Field(default=None, gt=0)
    trade_count: int = Field(default=0, ge=0)
    revision: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _ohlc_is_coherent(self) -> Self:
        """Checked here as well as in the database.

        The database protects the data from every writer; this protects the pipeline from
        publishing a bad bar in the first place, with a readable error instead of an
        IntegrityError three services downstream.
        """
        if self.high < self.low:
            raise ValueError(f"high {self.high} is below low {self.low}")
        if not (self.low <= self.open <= self.high):
            raise ValueError(f"open {self.open} is outside the low-high range")
        if not (self.low <= self.close <= self.high):
            raise ValueError(f"close {self.close} is outside the low-high range")
        return self

    @model_validator(mode="after")
    def _correction_has_a_revision(self) -> Self:
        if self.event_type is EventType.BAR_CORRECTED and self.revision == 0:
            raise ValueError("a corrected bar must carry a revision above 0")
        return self

    @property
    def bar_end(self) -> datetime:
        return self.bar_start + self.interval.duration

    @classmethod
    def natural_key(
        cls,
        *,
        source: str,
        symbol: str,
        interval: BarInterval,
        bar_start: datetime,
        revision: int,
    ) -> tuple[object, ...]:
        """A bar is identified by where it sits in time, plus which revision it is.

        The revision is part of the key on purpose: a correction is a *different* event
        about the same bar, and deduplicating it away would mean the correction never
        arrived.
        """
        return (source, "bar", symbol, interval.value, bar_start.isoformat(), revision)


class AnomalyEvent(MarketEvent):
    """A window of one symbol's activity scored as statistically unusual (ADR-0008).

    **Not evidence of wrongdoing.** This event says a symbol's recent behaviour was unlike
    its own recent history — earnings, index rebalances, news, halts, and thin opens all
    produce exactly this signature. That is the only claim this event makes, everywhere it
    appears — code, logs, UI copy, or documentation — and ADR-0008 names the handful of
    stronger words it must never be described with instead.

    ``occurred_at`` is ``window_end``, by the same convention as :class:`BarEvent`: that is
    the moment the window closed and the score became a fact. ``features`` carries exactly
    what produced ``anomaly_score`` — a detection that cannot be reconstructed from its own
    event is not explainable, and ADR-0008 requires every detection to be.
    """

    event_type: Literal[EventType.ANOMALY_DETECTED] = EventType.ANOMALY_DETECTED
    window_start: datetime
    anomaly_score: Decimal
    threshold: Decimal
    severity: AlertSeverity
    model_name: str = Field(min_length=1, max_length=50)
    model_version: str = Field(min_length=1, max_length=32)
    #: The feature values behind the score — e.g. ``{"return": ..., "volume_z": ...}``. The
    #: same names :mod:`application.detection.features` produces, so a stored detection and
    #: the code that scored it always agree on vocabulary.
    features: dict[str, Decimal]

    @model_validator(mode="after")
    def _window_is_ordered(self) -> Self:
        if self.occurred_at <= self.window_start:
            raise ValueError(
                f"window_end (occurred_at={self.occurred_at}) must be after "
                f"window_start ({self.window_start})"
            )
        return self

    @property
    def window_end(self) -> datetime:
        return self.occurred_at

    @classmethod
    def natural_key(
        cls,
        *,
        source: str,
        symbol: str,
        model_name: str,
        model_version: str,
        window_start: datetime,
        window_end: datetime,
    ) -> tuple[object, ...]:
        """One detection per (symbol, model, window). A model retrained to a new version is
        a different detector, so its score for a window already seen is a new detection, not
        a duplicate of the old model's.
        """
        return (
            source,
            "anomaly",
            symbol,
            model_name,
            model_version,
            window_start.isoformat(),
            window_end.isoformat(),
        )


class OrderFilledEvent(MarketEvent):
    """One execution against a paper-trading order (Phase 7/9's meeting point).

    Published so the Alert Dispatcher (Phase 9) has a live trading-side signal to evaluate
    rules against, alongside `AnomalyEvent` — `ARCHITECTURE.md` §3.8 names both as its inputs.
    The `fills` table remains the source of truth for positions (Phase 7's own rebuild job
    proves that); this event is a notification derived from an already-committed fill, never
    the write path itself.
    """

    event_type: Literal[EventType.ORDER_FILLED] = EventType.ORDER_FILLED
    order_id: uuid.UUID
    fill_id: uuid.UUID
    portfolio_id: uuid.UUID
    price: Decimal = Field(gt=0)
    quantity: Decimal = Field(gt=0)
    commission: Decimal = Field(ge=0)
    slippage_bps: Decimal


class AlertEvent(MarketEvent):
    """A notification raised for one user by the Alert Dispatcher (Phase 9).

    `symbol` is the triggering event's symbol — every alert this phase can produce (from an
    anomaly or a fill) has one. `triggering_event_type` names what set the rule off
    (`"anomaly.detected"`, `"order.filled"`) as a plain string rather than reusing this
    envelope's own `event_type` field, which is always `"alert.raised"` here — the two answer
    different questions and collapsing them would make the more useful one unrecoverable.
    """

    event_type: Literal[EventType.ALERT_RAISED] = EventType.ALERT_RAISED
    alert_id: uuid.UUID
    user_id: uuid.UUID
    alert_rule_id: uuid.UUID
    triggering_event_type: str = Field(min_length=1, max_length=50)
    severity: AlertSeverity
    title: str = Field(min_length=1, max_length=200)
    body: str
    payload: dict[str, Any] = Field(default_factory=dict)


def event_summary(event: MarketEvent) -> dict[str, Any]:
    """The fields worth putting in a log line.

    Deliberately not the whole event: logging a full quote book at debug level for every
    tick is how a disk fills up during a volatile open.
    """
    summary: dict[str, Any] = {
        "event_type": event.event_type.value,
        "symbol": event.symbol,
        "source": event.source,
        "event_id": str(event.event_id),
        "occurred_at": event.occurred_at.isoformat(),
    }
    if isinstance(event, TradeEvent):
        summary |= {"price": str(event.price), "size": str(event.size)}
    elif isinstance(event, QuoteEvent):
        summary |= {"bid": str(event.bid_price), "ask": str(event.ask_price)}
    elif isinstance(event, BarEvent):
        summary |= {"interval": event.interval.value, "close": str(event.close)}
    elif isinstance(event, AnomalyEvent):
        summary |= {
            "anomaly_score": str(event.anomaly_score),
            "severity": event.severity.value,
            "model_name": event.model_name,
        }
    elif isinstance(event, OrderFilledEvent):
        summary |= {"price": str(event.price), "quantity": str(event.quantity)}
    elif isinstance(event, AlertEvent):
        summary |= {"user_id": str(event.user_id), "severity": event.severity.value}
    return summary
