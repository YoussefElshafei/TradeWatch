"""The cache port (Phase 4, ADR-0003).

Redis is a justified, narrow-scope dependency (`ARCHITECTURE.md` §10): a cache for a handful
of hot, cheap-to-recompute reads, never the system of record. This port is deliberately just
"get, set, delete" — the whole point of `TW_CACHE_ENABLED=false` leaving the application
"fully correct, only slower" is that nothing above this line may depend on the cache for
correctness, only for speed. Anything a caller cannot survive losing does not belong behind
this port; it belongs in PostgreSQL.

Every method must degrade rather than raise (failure F9): the adapter is the one place that
know how to talk to Redis, and it is the one place that decides "Redis is down" means "answer
as if this key were absent," never "propagate an error the caller has no way to act on."
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["Cache"]


@runtime_checkable
class Cache(Protocol):
    """Somewhere to cache a JSON-serializable value, with a TTL and explicit invalidation.

    Every cached key in this system carries a TTL *and* a documented invalidation trigger
    (`ARCHITECTURE.md` §10) — TTL alone bounds staleness, invalidation removes it as soon as
    fresher data exists. Neither replaces the other.
    """

    async def get_json(self, key: str) -> object | None:
        """Return the cached value for ``key``, or ``None`` on a miss *or* an outage.

        A cache miss and "Redis is unreachable" look identical to the caller on purpose —
        both mean "go read the real answer from PostgreSQL," which is what F9 requires.
        """
        ...

    async def set_json(self, key: str, value: object, *, ttl_seconds: int) -> None:
        """Cache ``value`` under ``key`` for ``ttl_seconds``. Never raises."""
        ...

    async def delete(self, *keys: str) -> None:
        """Invalidate one or more keys immediately. Never raises."""
        ...
