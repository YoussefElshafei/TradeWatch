"""In-flight deduplication.

There are two layers of duplicate protection in TradeWatch and they catch different things:

* The database's `UNIQUE (event_id)` is the **authoritative** one. It is correct across
  restarts, across replicas, and across Kafka redeliveries, because it is a fact about
  stored rows rather than about a process's memory.
* This is the **cheap** one. It stops a duplicate before it is serialized, published, and
  round-tripped through a database only to be discarded — which matters when a provider
  replays a thousand prints after a reconnect, and matters more once Kafka is between the
  two (Phase 2).

So this is an optimisation, not a guarantee, and it is deliberately built that way: a
bounded window that forgets. An unbounded set would be a correctness improvement of exactly
zero — the database already covers what it would add — in exchange for a memory leak that
grows with every tick for as long as the process runs.

The window is bounded twice over, because either bound alone fails somewhere:

* **By age**, so a quiet symbol's entries expire rather than accumulating forever.
* **By count**, so a volatile open cannot exhaust memory before anything ages out.

A duplicate that arrives after eviction is not lost data — it reaches the database and the
unique constraint absorbs it. Eviction costs a wasted round trip, never correctness.
"""

from __future__ import annotations

import uuid
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field

__all__ = ["DeduplicationStats", "EventDeduplicator"]


@dataclass(slots=True)
class DeduplicationStats:
    """What the deduplicator has seen. Reported by the ingestion service."""

    seen: int = 0
    duplicates: int = 0
    evicted_by_age: int = 0
    evicted_by_size: int = 0

    @property
    def unique(self) -> int:
        return self.seen - self.duplicates

    @property
    def duplicate_rate(self) -> float:
        """Share of events that were duplicates, 0.0 when nothing has been seen."""
        return self.duplicates / self.seen if self.seen else 0.0


@dataclass(slots=True)
class EventDeduplicator:
    """A bounded, expiring set of event ids.

    Args:
        max_entries: Hard ceiling on remembered ids. At roughly 40 bytes per entry, the
            default is a few megabytes — small enough to ignore, large enough to cover a
            provider's reconnect replay.
        ttl_seconds: How long an id is remembered. Must comfortably exceed the longest
            plausible reconnect-and-replay window, or duplicates will slip past.
        monotonic: Time source, injected so tests can advance the clock instantly instead
            of sleeping through a TTL.
    """

    max_entries: int = 100_000
    ttl_seconds: float = 900.0
    monotonic: Callable[[], float] = field(default_factory=lambda: _default_clock)
    stats: DeduplicationStats = field(default_factory=DeduplicationStats)
    #: id -> when it was first seen. Ordered, so the oldest is evicted first.
    _seen: OrderedDict[uuid.UUID, float] = field(default_factory=OrderedDict, repr=False)

    def __post_init__(self) -> None:
        if self.max_entries <= 0:
            raise ValueError("max_entries must be positive")
        if self.ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")

    def __len__(self) -> int:
        return len(self._seen)

    def is_duplicate(self, event_id: uuid.UUID) -> bool:
        """Record ``event_id`` and report whether it had been seen.

        Records and answers in one call on purpose. A separate `contains` and `add` invites
        the bug where a caller checks, takes a branch, and forgets to record — which is
        silent, and looks like the deduplicator not working.
        """
        now = self.monotonic()
        self._expire(now)
        self.stats.seen += 1

        if event_id in self._seen:
            self.stats.duplicates += 1
            # Not refreshed: a duplicate must not extend the original's lifetime, or a
            # symbol that duplicates constantly would pin its entry forever.
            return True

        self._seen[event_id] = now
        self._trim()
        return False

    def forget(self, event_id: uuid.UUID) -> None:
        """Drop one id, so a failed publish can be retried rather than swallowed.

        Without this, an event that was marked seen and then failed to publish would be
        treated as a duplicate on retry and silently dropped — data loss caused by the
        deduplicator itself.
        """
        self._seen.pop(event_id, None)

    def clear(self) -> None:
        """Forget everything. Called on reconnect, when a replay is expected anyway."""
        self._seen.clear()

    def _expire(self, now: float) -> None:
        cutoff = now - self.ttl_seconds
        while self._seen:
            oldest_id, first_seen = next(iter(self._seen.items()))
            if first_seen > cutoff:
                break
            self._seen.popitem(last=False)
            self.stats.evicted_by_age += 1
            del oldest_id

    def _trim(self) -> None:
        while len(self._seen) > self.max_entries:
            self._seen.popitem(last=False)
            self.stats.evicted_by_size += 1


def _default_clock() -> float:
    """A monotonic clock that does not depend on an event loop being present."""
    import time

    return time.monotonic()
