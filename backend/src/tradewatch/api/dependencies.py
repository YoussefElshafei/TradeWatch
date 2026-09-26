"""Shared FastAPI dependencies.

The important one is :func:`get_session`. A request gets one session, opened at the start
and closed at the end, and the route never sees the engine — so a handler cannot accidentally
leak a connection or run a query after its transaction has closed.

It is a dependency rather than a module-level object because that is what makes it
overridable. The end-to-end test replaces it with the session its fixtures already hold,
which is how the API can be asserted against data the test wrote but has not committed.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Annotated, cast

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.application.auth import (
    AccessTokenClaims,
    ExpiredAccessTokenError,
    InvalidAccessTokenError,
    decode_access_token,
    revoked_key,
)
from tradewatch.config.settings import Settings
from tradewatch.domain.enums import UserRole
from tradewatch.infrastructure.cache import RedisCache
from tradewatch.infrastructure.database.session import Database

__all__ = [
    "CacheDep",
    "CurrentUserDep",
    "SessionDep",
    "SettingsDep",
    "get_cache",
    "get_current_user",
    "get_database",
    "get_session",
    "get_settings_dep",
    "require_role",
]


def get_database(request: Request) -> Database:
    """The process-wide database, attached to the app during startup."""
    database = getattr(request.app.state, "database", None)
    if database is None:
        raise RuntimeError("the database was not attached during application startup")
    return cast("Database", database)


def get_settings_dep(request: Request) -> Settings:
    """The settings this application was built with, not the process-wide singleton.

    Routes read configuration from here so that a test can build an app with its own
    settings and have the routes honour them.
    """
    settings = getattr(request.app.state, "settings", None)
    if settings is None:
        raise RuntimeError("settings were not attached to the application")
    return cast("Settings", settings)


async def get_session(
    database: Annotated[Database, Depends(get_database)],
) -> AsyncIterator[AsyncSession]:
    """One session per request, committed on success and rolled back on any exception."""
    async with database.session() as session:
        yield session


def get_cache(request: Request) -> RedisCache:
    """The process-wide cache, attached to the app during startup (Phase 4)."""
    cache = getattr(request.app.state, "cache", None)
    if cache is None:
        raise RuntimeError("the cache was not attached during application startup")
    return cast("RedisCache", cache)


SessionDep = Annotated[AsyncSession, Depends(get_session)]
SettingsDep = Annotated[Settings, Depends(get_settings_dep)]
CacheDep = Annotated[RedisCache, Depends(get_cache)]


#: `auto_error=False` so a missing/malformed header produces the same error envelope as
#: every other failure in this module, rather than FastAPI's default plain-text 403.
_bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer_scheme)],
    settings: SettingsDep,
    cache: CacheDep,
) -> AccessTokenClaims:
    """Verify the bearer token and return its claims, or 401.

    Three ways to fail, one response shape: no route can tell "expired" from "forged" from
    "revoked" by the response it gets back, which is what makes token handling here no
    different from `ARCHITECTURE.md` §9.2's uniform-login-response rule applied to requests
    instead of logins.
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required."
        )
    try:
        claims = decode_access_token(
            credentials.credentials, secret=settings.security.jwt_secret.get_secret_value()
        )
    except (ExpiredAccessTokenError, InvalidAccessTokenError) as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Access token is invalid or expired."
        ) from error

    # The emergency denylist (`AuthService.logout`, `ARCHITECTURE.md` §10): a token whose
    # signature and `exp` both check out but was explicitly revoked before it expired.
    if await cache.get_json(revoked_key(claims.jti)) is not None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Access token has been revoked."
        )

    return claims


CurrentUserDep = Annotated[AccessTokenClaims, Depends(get_current_user)]


def require_role(*roles: UserRole) -> Callable[[CurrentUserDep], Awaitable[AccessTokenClaims]]:
    """A dependency that additionally requires one of `roles` (`ARCHITECTURE.md` §9.3).

    Built as a factory rather than one dependency with a parameter, because FastAPI resolves
    dependencies from their identity — a route says ``Depends(require_role(UserRole.ADMIN))``
    and gets exactly that check, cached per request like any other dependency.
    """

    async def dependency(claims: CurrentUserDep) -> AccessTokenClaims:
        if claims.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="This action requires a different role.",
            )
        return claims

    return dependency
