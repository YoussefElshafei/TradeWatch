"""The event bus port.

`DEVELOPMENT_PLAN.md` Phase 1 defines this seam before Kafka exists, and ADR-0001 explains
why: the in-process implementation cannot survive a restart, cannot replay and cannot fan
out to independent consumers, so Phase 2 replaces it with a Kafka adapter behind this same
interface. Nothing above this line changes when that happens — which is the proof the seam
was drawn in the right place.

It is not throwaway scaffolding either. It is what lets every consumer be unit-tested
against an in-memory bus, forever.

**What Phase 2 actually changed here**, because the claim above was that nothing would.
Two protocols were added; none was altered, and no existing implementation needed a change.

:class:`EventBatchConsumer` exists because durable messaging acknowledges in *batches*.
:class:`EventConsumer` yields one event at a time and commits "everything so far", which
leaves the acknowledgement boundary implicit — fine for an in-process queue where a commit
is a no-op, and not fine for offsets, where exactly which events a commit covers is the
whole correctness question. A batch is the unit the processor already flushes in and the
unit Kafka already commits in, so making it the unit of the port removes a guess rather
than adding a concept.

:class:`DeadLetterSink` exists because the application is what discovers that an event is
*permanently* unprocessable — the store rejected it and always will. Quarantining it is a
transport decision, so it belongs behind a port; without one, the consumer's only options
would be to import Kafka, or to drop the event and count it, which is the silent data loss
this codebase refuses everywhere else.

Both additions are deliberately small, and neither mentions Kafka. Messages that never
become domain events at all — bytes that are not JSON, an unknown event type — are handled
entirely inside the adapter, because the application has nothing useful to say about bytes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from tradewatch.domain.events import MarketEvent

__all__ = [
    "DeadLetterSink",
    "EventBatchConsumer",
    "EventConsumer",
    "EventPublisher",
    "PositionedBatchConsumer",
    "StreamPosition",
]


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


@runtime_checkable
class SkipsToLatest(Protocol):
    """A consumer that can abandon its backlog and continue from the newest events.

    Only for a consumer whose job is *live* fan-out. A consumer that owes completeness —
    persistence, aggregation, paper trading, detection — must never do this: skipping is
    exactly the data loss their at-least-once contract exists to prevent.
    """

    async def skip_to_latest(self) -> None:
        """Move every assigned partition to its log end. Commit separately."""
        ...


@runtime_checkable
class EventBatchConsumer(Protocol):
    """A source of events that acknowledges in batches.

    The contract, which is the whole reason this protocol exists:

    * :meth:`batches` yields lists of events. An empty list is a legitimate result — it
      means the poll timed out with nothing to read — and it exists so a consumer can act
      on a quiet market (check for shutdown, report liveness) instead of blocking forever.
    * :meth:`commit` acknowledges **every batch yielded so far**, and is called only after
      those events have been durably stored. Not calling it is how a failure becomes a
      redelivery.

    That ordering is the at-least-once guarantee (`ARCHITECTURE.md` §7.4) stated as an
    interface: a crash between the side effect and the commit costs a redelivery, which
    idempotent writes absorb, and never costs an event.
    """

    def batches(self) -> AsyncIterator[Sequence[MarketEvent]]:
        """Yield batches of events until cancelled, possibly empty ones."""
        ...

    async def commit(self) -> None:
        """Acknowledge every batch yielded so far.

        Raises:
            Exception: If the acknowledgement could not be recorded. The caller must treat
                a failed commit as "these events may be redelivered", never as success.
        """
        ...


@runtime_checkable
class DeadLetterSink(Protocol):
    """Somewhere to put an event that can never be processed.

    Only ever called for a **permanent** failure. A retryable one — the database is down —
    must not come here: quarantining events during an outage would turn a recoverable
    incident into a manual replay of everything that arrived while it lasted.

    Implementations must not raise. A dead-letter sink that fails is a diagnostic problem,
    and letting it take down the consumer would mean one unstorable event stopped the
    pipeline after all — the exact failure quarantining exists to avoid.
    """

    async def send(
        self,
        event: MarketEvent,
        *,
        reason: str,
        error: str,
        attempts: int = 1,
    ) -> None:
        """Quarantine one event.

        Args:
            event: The event that could not be processed.
            reason: A short, stable classifier — suitable for a metric label.
            error: The human-readable detail. Never a full payload at error level.
            attempts: How many times processing was tried before giving up.
        """
        ...


@dataclass(frozen=True, slots=True, order=True)
class StreamPosition:
    """Where an event sat in its stream.

    ``stream`` names one ordered sequence — a Kafka topic-partition, a Kinesis shard — and
    ``offset`` is monotonic within it. Two positions are only comparable when their streams
    match, which is why ``stream`` sorts first.

    **Why the application needs this at all**, given that ports elsewhere work hard to keep
    transport concepts out: a consumer whose output *accumulates* cannot be made idempotent
    by its writes alone. `UNIQUE (event_id)` makes writing a trade idempotent because that
    write is a replacement; folding a trade into a bar adds to it, and adding the same trade
    twice adds its volume twice. The consumer therefore has to record how far it has applied,
    and "how far" is a position in a stream. It is the smallest transport-shaped idea that
    makes an accumulating consumer correct under at-least-once delivery.
    """

    stream: str
    offset: int


@runtime_checkable
class PositionedBatchConsumer(Protocol):
    """A batch consumer that says where each event came from.

    Separate from :class:`EventBatchConsumer` rather than replacing it, because only a
    consumer that accumulates needs this. The market data processor writes one row per event
    and is idempotent through its unique constraints, so handing it positions would be extra
    surface it never reads.
    """

    def positioned_batches(self) -> AsyncIterator[Sequence[tuple[StreamPosition, MarketEvent]]]:
        """Yield batches of ``(position, event)``, possibly empty ones."""
        ...

    async def commit(self) -> None:
        """Acknowledge every batch yielded so far."""
        ...
