"""The Redis adapter: implements both :class:`Cache` and :class:`EventBroadcaster`.

One class, one connection, two ports — the same reasoning as ``SqlMarketDataStore``
implementing more than one storage port off one session: they share a client because they
share a failure mode, and splitting them into two classes would only mean duplicating the
"never raise" wrapper below twice.

**Failure F9, centralized here and nowhere else.** Every public method catches
``redis.RedisError`` (and the connection-level ``OSError``/``TimeoutError`` redis-py can also
raise before it has wrapped anything) and degrades: a cache read that cannot reach Redis
returns ``None`` — indistinguishable from a miss — a write or publish that cannot reach Redis
is logged and dropped. Nothing above this module ever has to ask "is Redis up?" — it only
ever sees "hit" or "miss," which is what lets `TW_CACHE_ENABLED=false` and "Redis is down"
be the same code path from the caller's point of view.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as redis
from redis.exceptions import RedisError

from tradewatch.config.settings import RedisSettings
from tradewatch.observability.logging import get_logger

__all__ = ["RedisCache"]

log = get_logger(__name__)

#: Caught around every Redis call. redis-py raises its own ``RedisError`` family once a
#: connection exists; a connection that never came up (wrong host, refused) can still surface
#: as a bare ``OSError``/``TimeoutError`` from the underlying socket layer.
_REDIS_ERRORS: tuple[type[Exception], ...] = (RedisError, OSError, TimeoutError)


class RedisCache:
    """Owns the Redis client for the lifetime of a process.

    Held on ``app.state`` by the API (mirrors :class:`~tradewatch.infrastructure.database.
    session.Database`) and constructed directly by the broadcaster worker. Lazy connection:
    :meth:`connect` creates the client but opens no socket — the first real command does,
    and if that fails every subsequent call degrades per F9 rather than raising.
    """

    def __init__(self, settings: RedisSettings) -> None:
        self._settings = settings
        self._client: redis.Redis | None = None

    @property
    def client(self) -> redis.Redis:
        if self._client is None:
            raise RuntimeError("RedisCache.connect() has not been called")
        return self._client

    async def connect(self) -> None:
        if self._client is not None:
            return
        self._client = redis.from_url(
            self._settings.redis_url,
            decode_responses=True,
            # The *first* connection through Docker Desktop's Windows/WSL2 port-forwarding
            # proxy measured ~2.1s in development — a cold-start cost of the proxy itself,
            # paid once per process, not a sign anything is slow. A tighter value here was
            # tripping `/ready` on a freshly started API before Redis had ever been asked
            # anything. Every command after the first measured ~6ms, which is what
            # `socket_timeout` actually protects: a slow *command* on an open connection,
            # not the one-time cost of opening it.
            socket_connect_timeout=5.0,
            socket_timeout=2.0,
        )
        log.info("cache.client_created")

    async def disconnect(self) -> None:
        if self._client is None:
            return
        await self._client.aclose()
        self._client = None
        log.info("cache.client_closed")

    async def check(self) -> str | None:
        """Readiness check. Never gates `/ready`'s overall status (F9) — see routes/health.py."""
        if self._client is None:
            return "not connected"
        try:
            await self._client.ping()
        except _REDIS_ERRORS as error:
            log.warning("cache.unreachable", error=str(error))
            return type(error).__name__
        return None

    # --- Cache -----------------------------------------------------------------

    async def get_json(self, key: str) -> Any | None:
        if self._client is None:
            return None
        try:
            raw = await self._client.get(key)
        except _REDIS_ERRORS as error:
            log.warning("cache.get_failed", key=key, error=str(error))
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # A value this process did not write (or a stale shape from a previous
            # version). Treat it as a miss rather than crash the caller over a cache row.
            log.warning("cache.corrupt_value", key=key)
            return None

    async def set_json(self, key: str, value: object, *, ttl_seconds: int) -> None:
        if self._client is None:
            return
        try:
            await self._client.set(key, json.dumps(value), ex=ttl_seconds)
        except _REDIS_ERRORS as error:
            log.warning("cache.set_failed", key=key, error=str(error))

    async def delete(self, *keys: str) -> None:
        if self._client is None or not keys:
            return
        try:
            await self._client.delete(*keys)
        except _REDIS_ERRORS as error:
            log.warning("cache.delete_failed", keys=list(keys), error=str(error))

    # --- Rate limiting (Phase 5, `ARCHITECTURE.md` §9.4/§10) ---------------------

    async def increment_with_expiry(self, key: str, *, ttl_seconds: int) -> int:
        """Atomically increment a fixed-window counter, returns the new count.

        `INCR` then `EXPIRE ... NX` in one pipeline (transactional by default in redis-py):
        the TTL is set only on the increment that creates the key, so a counter already
        mid-window is never granted extra life by a later request. A real first increment
        always returns at least 1 — **0 is reserved to mean "could not increment"** (Redis
        unreachable), so the caller can tell "the count is genuinely zero" apart from "the
        count is unknown" without a separate signal.

        Failure F9: an unreachable Redis makes the rate limiter fail *open*, same as it
        makes the cache fail to a miss. Refusing every request because the counter cannot
        be read would turn a Redis outage into a self-inflicted denial of service, which is
        a worse failure than temporarily not rate-limiting.
        """
        if self._client is None:
            return 0
        try:
            async with self._client.pipeline(transaction=True) as pipe:
                pipe.incr(key)
                pipe.expire(key, ttl_seconds, nx=True)
                results = await pipe.execute()
            return int(results[0])
        except _REDIS_ERRORS as error:
            log.warning("cache.rate_limit_failed", key=key, error=str(error))
            return 0

    # --- EventBroadcaster --------------------------------------------------------

    async def publish(self, channel: str, message: dict[str, object]) -> None:
        if self._client is None:
            return
        try:
            await self._client.publish(channel, json.dumps(message))
        except _REDIS_ERRORS as error:
            log.warning("cache.publish_failed", channel=channel, error=str(error))

    async def subscribe(self, channel: str) -> AsyncIterator[dict[str, Any]]:
        """Yield decoded messages published to ``channel`` until cancelled.

        Reconnects with a short backoff on a dropped connection rather than ending the
        generator — the API process holds this open for its whole lifetime, and one Redis
        blip must not permanently stop live updates until the next restart.
        """
        while True:
            try:
                pubsub = self.client.pubsub()
                await pubsub.subscribe(channel)
                try:
                    async for raw in pubsub.listen():
                        if raw["type"] != "message":
                            continue
                        try:
                            yield json.loads(raw["data"])
                        except json.JSONDecodeError:
                            log.warning("cache.subscribe_corrupt_message", channel=channel)
                finally:
                    with contextlib.suppress(*_REDIS_ERRORS):
                        await pubsub.unsubscribe(channel)
                    with contextlib.suppress(*_REDIS_ERRORS):
                        # redis-py's own stub leaves this one method untyped, unlike the
                        # rest of the client — scoped here rather than a blanket override,
                        # same reasoning as the aiokafka mypy override in pyproject.toml.
                        await pubsub.aclose()  # type: ignore[no-untyped-call]
            except _REDIS_ERRORS as error:
                log.warning("cache.subscribe_disconnected", channel=channel, error=str(error))
                await asyncio.sleep(1.0)
