"""The event bus port.

`DEVELOPMENT_PLAN.md` Phase 1 defines this seam before Kafka exists, and ADR-0001 explains
why: the in-process implementation cannot survive a restart, cannot replay and cannot fan
out to independent consumers, so Phase 2 replaces it with a Kafka adapter behind this same
interface. Nothing above this line changes when that happens — which is the proof the seam
was drawn in the right place.

It is not throwaway scaffolding either. It is what lets every consumer be unit-tested
against an in-memory bus, forever.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from typing import Protocol, runtime_checkable

from tradewatch.domain.events import MarketEvent

__all__ = ["EventConsumer", "EventPublisher"]


@runtime_checkable
class EventPublisher(Protocol):
    """Somewhere to send events."""

    async def publish(self, event: MarketEvent) -> None:
        """Publish one event.

        Raises:
            EventBusError: If the event could not be accepted. The caller decides whether
                that is fatal — for ingestion it is not: failure F4 says drop quotes before
                blocking trades.
        """
        ...

    async def publish_many(self, events: Sequence[MarketEvent]) -> int:
        """Publish a batch, returning how many were accepted.

        A batch method rather than a loop because the Kafka adapter will want to send one
        produce request, and a port that only offers `publish` would force the caller into
        the slow shape.
        """
        ...

    async def flush(self) -> None:
        """Wait for everything already published to be durable."""
        ...


@runtime_checkable
class EventConsumer(Protocol):
    """Somewhere to read events from."""

    def consume(self) -> AsyncIterator[MarketEvent]:
        """Yield events until cancelled."""
        ...

    async def commit(self) -> None:
        """Acknowledge everything consumed so far.

        Called *after* the side effect, never before (`ARCHITECTURE.md` §7.4): that ordering
        is what turns a mid-batch crash into a redelivery instead of a silent gap.
        """
        ...
