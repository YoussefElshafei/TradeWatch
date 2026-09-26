"""Redis-backed rate limiting (`ARCHITECTURE.md` §9.4, §10).

A fixed one-minute window per key, `tw:rl:{scope}:{identity}`, atomically incremented by
`RedisCache.increment_with_expiry`. A FastAPI dependency factory rather than middleware,
because the cap differs by route — "tighter buckets on expensive routes" (§9.4) is a
per-route decision, and a dependency is where a route already says what makes it different
from every other route.

Two identities, matching §9.4 exactly: **per-IP** for routes nobody is authenticated on yet
(login, register — the routes an attacker would hit first), **per-user** for everything
behind :func:`~tradewatch.api.dependencies.get_current_user`. Never per-IP for authenticated
routes: it would let every user behind one NAT or corporate proxy exhaust one shared bucket.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from fastapi import HTTPException, Request, status

from tradewatch.api.dependencies import CacheDep, CurrentUserDep, SettingsDep
from tradewatch.config.settings import SecuritySettings
from tradewatch.observability.logging import get_logger

__all__ = ["rate_limit_by_ip", "rate_limit_by_user"]

log = get_logger(__name__)

#: Every limit in `SecuritySettings` is "per minute" (`TW_RATE_LIMIT_ANON_PER_MIN`, etc.),
#: so the window this enforces against is the same unit, not a separate setting to keep in
#: sync with the name of the one it is implementing.
_WINDOW_SECONDS = 60


def _client_ip(request: Request) -> str:
    """The caller's address for the anonymous bucket. No `X-Forwarded-For` trust here —
    that is only safe behind a proxy this deployment controls, which is an operational
    decision, not a default this function should make for every deployment.
    """
    return request.client.host if request.client else "unknown"


async def _enforce(cache: CacheDep, key: str, *, limit: int) -> None:
    count = await cache.increment_with_expiry(key, ttl_seconds=_WINDOW_SECONDS)
    if count == 0:
        # Redis is unreachable (failure F9): fail open rather than let a cache outage
        # become a self-inflicted denial of service.
        return
    if count > limit:
        log.warning("rate_limit.exceeded", key=key, count=count, limit=limit)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many requests. Please slow down.",
            headers={"Retry-After": str(_WINDOW_SECONDS)},
        )


def rate_limit_by_ip(
    scope: str, *, limit: Callable[[SecuritySettings], int] | None = None
) -> Callable[[Request, CacheDep, SettingsDep], Awaitable[None]]:
    """Cap requests per source IP. For routes reached before there is a user to key by.

    Args:
        scope: Distinguishes this route's bucket (`tw:rl:{scope}:{ip}`) from every other's.
        limit: Overrides `TW_RATE_LIMIT_ANON_PER_MIN` — the "tighter bucket" for an
            expensive or abuse-prone route.
    """

    async def dependency(request: Request, cache: CacheDep, settings: SettingsDep) -> None:
        cap = limit(settings.security) if limit else settings.security.rate_limit_anon_per_min
        await _enforce(cache, f"tw:rl:{scope}:{_client_ip(request)}", limit=cap)

    return dependency


def rate_limit_by_user(
    scope: str, *, limit: Callable[[SecuritySettings], int] | None = None
) -> Callable[[CurrentUserDep, CacheDep, SettingsDep], Awaitable[None]]:
    """Cap requests per authenticated user. Depends on `get_current_user` itself, so a route
    using this is also, transitively, an authenticated route — there is no way to attach it
    to an anonymous one by mistake.
    """

    async def dependency(
        current_user: CurrentUserDep, cache: CacheDep, settings: SettingsDep
    ) -> None:
        cap = limit(settings.security) if limit else settings.security.rate_limit_user_per_min
        await _enforce(cache, f"tw:rl:{scope}:{current_user.user_id}", limit=cap)

    return dependency
