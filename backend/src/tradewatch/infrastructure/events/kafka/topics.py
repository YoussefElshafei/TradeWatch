"""Topic names, and which topic an event belongs on.

`ARCHITECTURE.md` §7.2 fixes the naming convention:
``<prefix>.<domain>.<entity>[.<qualifier>].v<major>``. The prefix is configuration
(``TW_KAFKA_TOPIC_PREFIX``) so two environments can share a broker without sharing a
keyspace; everything after it is a contract and is not configurable, because a topic whose
name depends on a setting is a topic no other team can find.

**As built through Phase 9:** the four market/DLQ topics from Phase 2, `tw.anomalies.v1`
(Phase 8), and `tw.fills.v1` / `tw.alerts.v1` (Phase 9 — the phase where the Phase 6/7 trading
track and the Phase 8 detection track meet in one consumer, the Alert Dispatcher). §7.2 also
lists `tw.signals.v1` and `tw.orders.v1` — neither is created here, because nothing consumes
raw signals or order-submission events independently of the fill they eventually produce; a
topic with no real consumer is the decoration this codebase keeps refusing.

**Bars: 1-minute only, for now.** §7.2 specifies `tw.market.bars.1m.v1`, one topic per
interval. Only 1-minute bars travel over Kafka in Phase 2, because that is the only interval
anything produces: the bar aggregator is Phase 3, and the other intervals in
``TW_BAR_INTERVALS`` are computed from stored 1m bars rather than published as events. A bar
event on another interval is therefore a programming error and :func:`Topics.for_event` says
so, instead of inventing a topic name nobody creates or consumes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from tradewatch.domain.enums import BarInterval
from tradewatch.domain.events import (
    AlertEvent,
    AnomalyEvent,
    BarEvent,
    EventType,
    MarketEvent,
    OrderFilledEvent,
    QuoteEvent,
    TradeEvent,
)

__all__ = ["SCHEMA_MAJOR", "Topics", "UnroutableEventError"]

#: The major version in every topic name. A breaking schema change increments this and
#: creates a parallel set of topics (ADR-0005); it never redefines an existing one.
SCHEMA_MAJOR: Final = 1

#: The only bar interval published as events in this phase.
_PUBLISHED_BAR_INTERVAL: Final = BarInterval.ONE_MINUTE


class UnroutableEventError(RuntimeError):
    """No topic carries this event.

    Permanent by nature — the same event routes nowhere every time — and a bug rather than
    bad data: everything the ingestion path produces has a topic. Raised loudly instead of
    being defaulted onto some topic, because a trade quietly landing on the wrong topic is
    far harder to notice than a producer that refuses to start sending.
    """


@dataclass(frozen=True, slots=True)
class Topics:
    """The topic names for one prefix."""

    prefix: str = "tw"

    @property
    def trades(self) -> str:
        return f"{self.prefix}.market.trades.v{SCHEMA_MAJOR}"

    @property
    def quotes(self) -> str:
        return f"{self.prefix}.market.quotes.v{SCHEMA_MAJOR}"

    @property
    def bars_1m(self) -> str:
        return f"{self.prefix}.market.bars.{_PUBLISHED_BAR_INTERVAL.value}.v{SCHEMA_MAJOR}"

    @property
    def anomalies(self) -> str:
        """Phase 8: the Anomaly Detection Service's output (ADR-0008)."""
        return f"{self.prefix}.anomalies.v{SCHEMA_MAJOR}"

    @property
    def fills(self) -> str:
        """Phase 9: the Paper Trading Engine's fill notifications — one of the Alert
        Dispatcher's two inputs, alongside `anomalies` (`ARCHITECTURE.md` §3.8).
        """
        return f"{self.prefix}.fills.v{SCHEMA_MAJOR}"

    @property
    def alerts(self) -> str:
        """Phase 9: the Alert Dispatcher's output, relayed live over `/ws/alerts`."""
        return f"{self.prefix}.alerts.v{SCHEMA_MAJOR}"

    @property
    def dead_letter(self) -> str:
        """One DLQ for every consumer (§7.2), not one per topic.

        A per-topic DLQ multiplies topics for no gain: what an operator wants is a single
        place to look at everything that failed, and the original topic is preserved in the
        message's headers anyway.
        """
        return f"{self.prefix}.dlq.v{SCHEMA_MAJOR}"

    @property
    def market(self) -> tuple[str, ...]:
        """The topics the market data consumer subscribes to."""
        return (self.trades, self.quotes, self.bars_1m)

    @property
    def all(self) -> tuple[str, ...]:
        """Every topic every phase built so far creates."""
        return (*self.market, self.anomalies, self.fills, self.alerts, self.dead_letter)

    def for_event(self, event: MarketEvent) -> str:
        """The topic an event belongs on.

        Raises:
            UnroutableEventError: If nothing in this phase carries the event.
        """
        match event:
            case TradeEvent():
                return self.trades
            case QuoteEvent():
                return self.quotes
            case AnomalyEvent():
                return self.anomalies
            case OrderFilledEvent():
                return self.fills
            case AlertEvent():
                return self.alerts
            case BarEvent() if event.interval is _PUBLISHED_BAR_INTERVAL:
                return self.bars_1m
            case BarEvent():
                raise UnroutableEventError(
                    f"only {_PUBLISHED_BAR_INTERVAL.value} bars are published as events in "
                    f"this phase, not {event.interval.value}; longer intervals are derived "
                    f"from stored 1m bars until the Phase 3 aggregator exists"
                )
            case _:
                raise UnroutableEventError(
                    f"no topic carries {type(event).__name__} "
                    f"({getattr(event, 'event_type', EventType.TRADE)})"
                )
