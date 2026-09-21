"""An in-process event bus.

Phase 1's implementation of the publisher and consumer ports (ADR-0001). It is honest about
what it is: a bounded queue in one process. It cannot survive a restart, cannot replay, and
cannot fan out to consumers with independent failure domains — which is precisely the list
of things Phase 3 needs and therefore precisely the argument for Kafka in Phase 2.

It is not scaffolding to be thrown away. It stays forever as the bus every consumer is unit
tested against, which is most of why the port exists at all.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass, field

from tradewatch.domain.events import MarketEvent
from tradewatch.observability.logging import get_logger

__all__ = ["EventBusError", "FailingPublisher", "InMemoryEventBus", "RecordingPublisher"]

log = get_logger(__name__)


class EventBusError(RuntimeError):
    """Publishing failed. The ingestion service counts this and keeps going (failure F4)."""


@dataclass(slots=True)
class RecordingPublisher:
    """Keeps every published event in memory.

    The simplest useful publisher, and what most tests assert against: publish, then look at
    `events`.
    """

    events: list[MarketEvent] = field(default_factory=list)
    flushed: int = 0

    async def publish(self, event: MarketEvent) -> None:
        self.events.append(event)

    async def publish_many(self, events: Sequence[MarketEvent]) -> int:
        self.events.extend(events)
        return len(events)

    async def flush(self) -> None:
        self.flushed += 1

    def by_symbol(self, symbol: str) -> list[MarketEvent]:
        return [event for event in self.events if event.symbol == symbol.upper()]

    def clear(self) -> None:
        self.events.clear()


@dataclass(slots=True)
class FailingPublisher:
    """A publisher that fails on demand.

    Failure F4 needs a bus that refuses, and waiting for the real one to break is not a test
    strategy. `fail_predicate` decides per event, so a test can fail only quotes, only the
    third event, or everything.
    """

    fail_predicate: Callable[[MarketEvent], bool] = lambda _event: True
    error_message: str = "publish rejected"
    attempts: int = 0
    accepted: list[MarketEvent] = field(default_factory=list)

    async def publish(self, event: MarketEvent) -> None:
        self.attempts += 1
        if self.fail_predicate(event):
            raise EventBusError(self.error_message)
        self.accepted.append(event)

    async def publish_many(self, events: Sequence[MarketEvent]) -> int:
        self.attempts += 1
        if any(self.fail_predicate(event) for event in events):
            raise EventBusError(self.error_message)
        self.accepted.extend(events)
        return len(events)

    async def flush(self) -> None:
        return None


class InMemoryEventBus:
    """A bounded queue implementing both the publisher and consumer ports.

    The bound matters. An unbounded queue turns a slow consumer into an out-of-memory kill,
    which is a worse failure than the backpressure it was avoiding. When the queue is full,
    `publish` raises rather than blocking, so the ingestion service can apply its shedding
    order (F4) instead of stalling the whole feed behind one slow reader.
    """

    def __init__(self, *, max_size: int = 10_000) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        self._queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=max_size)
        self._max_size = max_size
        self._published = 0
        self._consumed = 0
        self._rejected = 0

    @property
    def depth(self) -> int:
        return self._queue.qsize()

    @property
    def published(self) -> int:
        return self._published

    @property
    def consumed(self) -> int:
        return self._consumed

    @property
    def rejected(self) -> int:
        """Events refused because the queue was full. The backpressure signal."""
        return self._rejected

    async def publish(self, event: MarketEvent) -> None:
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull as error:
            self._rejected += 1
            raise EventBusError(
                f"event bus is full ({self._max_size} events); a consumer is not keeping up"
            ) from error
        self._published += 1

    async def publish_many(self, events: Sequence[MarketEvent]) -> int:
        accepted = 0
        for event in events:
            try:
                await self.publish(event)
            except EventBusError:
                # Partial acceptance is the honest answer: the caller learns how many got
                # through rather than being told the whole batch failed.
                break
            accepted += 1
        return accepted

    async def flush(self) -> None:
        """In-process delivery is already durable to the extent it can be."""
        return None

    async def consume(self) -> AsyncIterator[MarketEvent]:
        """Yield events as they arrive, until cancelled.

        `task_done` is called as soon as an event is handed over, which is what lets
        :meth:`wait_until_empty` know the queue has been fully delivered. It says nothing
        about whether the consumer has *stored* it — that is the consumer's own flush to
        account for.
        """
        while True:
            event = await self._queue.get()
            self._consumed += 1
            self._queue.task_done()
            yield event

    async def wait_until_empty(self) -> None:
        """Wait until every queued event has been handed to a consumer.

        Used at shutdown so a bounded run does not end with events published by the producer
        and never seen by the consumer. Wrap it in `asyncio.timeout` — if no consumer is
        running, this waits forever, which is the correct behaviour and a bad way to exit.
        """
        await self._queue.join()

    async def drain(self, limit: int | None = None) -> list[MarketEvent]:
        """Take everything currently queued, without waiting for more.

        Tests use this to assert on what was published without having to cancel a consumer.
        """
        drained: list[MarketEvent] = []
        while not self._queue.empty():
            if limit is not None and len(drained) >= limit:
                break
            drained.append(self._queue.get_nowait())
            self._consumed += 1
        return drained

    async def drain_iter(self) -> AsyncIterator[MarketEvent]:
        """Yield everything currently queued, then stop.

        Unlike :meth:`consume`, this does not wait for more. It exists for the bootstrap
        path, which needs to hand a finite batch to a consumer and then move on rather than
        blocking on a queue that will not fill again.
        """
        while not self._queue.empty():
            event = self._queue.get_nowait()
            self._consumed += 1
            self._queue.task_done()
            yield event

    async def commit(self) -> None:
        """No offsets to commit in process. Kafka's adapter is where this matters."""
        return None
