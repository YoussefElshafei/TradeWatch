"""The broadcast port (Phase 4).

Fan-out to connected browsers is Redis Pub/Sub's job (`ARCHITECTURE.md` §10, §8.2): the API
process holds the WebSocket connections, but the Kafka consumer that knows about new trades,
quotes and bars runs in a separate `tradewatch-broadcaster` process. Pub/Sub is the seam
between them, and this is that seam expressed without naming Redis, for the same reason
`EventPublisher` never named Kafka.

Pub/Sub is fire-and-forget by design (there is no consumer group, no offset, no replay): a
message published while nobody is subscribed is simply never seen. That is the right
trade-off here because this port only ever carries *derived*, ephemeral notifications — "a
new trade for AAPL just landed" — never the trade itself as a system of record. A browser
that connects a second later gets the *next* update, not a missed one, and the REST
endpoints (kept fresh by cache invalidation, not by this channel) are what it should read on
connect to catch up. Losing a live-update message costs a moment of visible staleness in one
open tab; it never costs data.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["EventBroadcaster"]


@runtime_checkable
class EventBroadcaster(Protocol):
    """Somewhere to publish an already-JSON-safe message for fan-out to live subscribers."""

    async def publish(self, channel: str, message: dict[str, object]) -> None:
        """Publish ``message`` to ``channel``. Never raises (failure F9: degrade, not fail).

        A publish that cannot reach Redis means no connected browser sees this particular
        update live — costing a moment of staleness, resolved by that browser's own next
        REST poll or the cache TTL, whichever it uses. It must never take down the consumer
        reading Kafka, whose job (keeping PostgreSQL correct) is unrelated to this one.
        """
        ...
