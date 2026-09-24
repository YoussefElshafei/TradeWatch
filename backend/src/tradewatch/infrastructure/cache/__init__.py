"""Redis adapters (Phase 4, ADR-0003): cache and Pub/Sub fan-out, never the system of record."""

from __future__ import annotations

from tradewatch.infrastructure.cache.redis_cache import RedisCache

__all__ = ["RedisCache"]
