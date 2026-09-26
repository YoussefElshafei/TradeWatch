"""Registration, login, refresh rotation, and logout (ADR-0010).

Everything here is unit-testable against a fake `IdentityStore` and a fake `Cache` — no
database, no Redis — which is what makes rotation and reuse detection, the two pieces of
this phase worth getting exactly right, cheap to exercise against every edge case.

**Why the emergency denylist rides on the ordinary `Cache` port.** `Cache`'s contract says
nothing above it may depend on Redis for correctness (`ARCHITECTURE.md` §10, failure F9) —
a cache that fails open. That looks wrong for a security denylist until ADR-0010's own
"Negative consequences" are read: the denylist only ever *shortens* a compromised access
token's remaining lifetime below its already-short TTL. If Redis is unreachable, a revoked
token is good for at most `TW_ACCESS_TOKEN_TTL_MIN` more minutes — exactly the bound the ADR
accepts with no denylist at all. Reusing the cache port is therefore not a shortcut; it is
the same trade-off the architecture document already made, applied once.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache

from tradewatch.application.auth.passwords import hash_password, needs_rehash, verify_password
from tradewatch.application.auth.tokens import (
    AccessTokenClaims,
    create_access_token,
    generate_refresh_token,
    hash_refresh_token,
)
from tradewatch.application.ports.cache import Cache
from tradewatch.application.ports.identity import (
    EmailAlreadyRegisteredError,
    IdentityStore,
    UserRecord,
)
from tradewatch.config.settings import SecuritySettings
from tradewatch.domain.enums import UserRole
from tradewatch.observability.logging import get_logger

__all__ = [
    "AuthService",
    "EmailAlreadyRegisteredError",
    "InvalidCredentialsError",
    "IssuedTokens",
    "RefreshTokenInvalidError",
    "RefreshTokenReusedError",
    "RegistrationDisabledError",
    "WeakPasswordError",
]

log = get_logger(__name__)


class InvalidCredentialsError(Exception):
    """Wrong password, unknown email, or a non-active account — deliberately one exception.

    `ARCHITECTURE.md` §9.2: "Login responses are uniform whether the account exists or not."
    A caller that branched on "no such user" versus "wrong password" would hand an attacker
    a free account-enumeration oracle; there being only one exception to catch is what makes
    that mistake impossible to make by accident at the route.
    """


class RefreshTokenInvalidError(Exception):
    """Unknown, expired, or malformed — nothing to revoke, just refuse."""


class RefreshTokenReusedError(Exception):
    """An already-rotated token was presented again.

    By the time this is raised the whole family has already been revoked — the caller's job
    is only to tell the client to log in again, never to retry.
    """


class RegistrationDisabledError(Exception):
    """`TW_REGISTRATION_ENABLED=false`."""


class WeakPasswordError(Exception):
    """Shorter than `TW_PASSWORD_MIN_LENGTH`."""

    def __init__(self, minimum_length: int) -> None:
        super().__init__(f"password must be at least {minimum_length} characters")
        self.minimum_length = minimum_length


@dataclass(frozen=True, slots=True)
class IssuedTokens:
    """What a login, registration, or refresh hands back to the route."""

    access_token: str
    access_token_expires_at: dt.datetime
    refresh_token: str
    refresh_token_expires_at: dt.datetime
    user: UserRecord


def revoked_key(jti: uuid.UUID) -> str:
    """The denylist key shape from `ARCHITECTURE.md` §10: ``tw:revoked:{jti}``.

    A free function, not a method, so `api/dependencies.py` can check it on every request
    without constructing an `AuthService` — the whole point of the check being cheap.
    """
    return f"tw:revoked:{jti}"


@lru_cache(maxsize=1)
def _dummy_password_hash() -> str:
    """A real Argon2id hash nothing verifies against, computed once, lazily.

    `authenticate` runs a verify against this when the email does not exist, so a login
    attempt against an unknown address costs the same wall-clock time as one against a real
    account with a wrong password — the timing side-channel version of the uniform-response
    rule in `ARCHITECTURE.md` §9.2.
    """
    return hash_password("not-a-real-password-only-used-for-uniform-timing")


class AuthService:
    def __init__(
        self,
        store: IdentityStore,
        cache: Cache,
        settings: SecuritySettings,
        *,
        clock: Callable[[], dt.datetime] | None = None,
    ) -> None:
        self._store = store
        self._cache = cache
        self._settings = settings
        self._clock = clock or (lambda: dt.datetime.now(dt.UTC))

    async def register(self, *, email: str, password: str) -> UserRecord:
        if not self._settings.registration_enabled:
            raise RegistrationDisabledError
        if len(password) < self._settings.password_min_length:
            raise WeakPasswordError(self._settings.password_min_length)

        cleaned_email = email.strip()
        user = await self._store.create_user(
            email=cleaned_email,
            password_hash=hash_password(password),
            # Every new account starts at the least-privileged role; an admin is promoted,
            # never self-registered (`ARCHITECTURE.md` §9.3).
            role=UserRole.VIEWER,
        )
        await self._store.record_audit_event(
            action="auth.register", resource_type="user", user_id=user.id
        )
        log.info("auth.registered", user_id=str(user.id))
        return user

    async def authenticate(
        self,
        *,
        email: str,
        password: str,
        user_agent: str | None,
        ip: str | None,
    ) -> IssuedTokens:
        user = await self._store.get_user_by_email(email.strip())
        hash_to_check = user.password_hash if user is not None else _dummy_password_hash()
        password_is_correct = verify_password(password, hash_to_check)

        if user is None or not password_is_correct or user.status != "active":
            # `user_id` when an account was actually targeted, `None` only when the email
            # matched nobody. The uniform-response rule (`ARCHITECTURE.md` §9.2) governs
            # what the *caller* sees in the HTTP response — identical either way — not what
            # this internal, non-queryable-by-API record keeps for security monitoring; a
            # string of failed attempts against one real account is exactly what it exists
            # to make visible.
            await self._store.record_audit_event(
                action="auth.login_failed",
                resource_type="user",
                user_id=user.id if user is not None else None,
                ip=ip,
            )
            raise InvalidCredentialsError

        if needs_rehash(user.password_hash):
            # The only moment the plaintext exists to rehash it with. Best effort: an
            # upgrade that fails here is retried on the user's next successful login.
            await self._store.update_password_hash(user.id, hash_password(password))

        now = self._clock()
        await self._store.record_login(user.id, at=now)
        tokens = await self._issue_tokens(user, user_agent=user_agent, ip=ip, now=now)
        await self._store.record_audit_event(
            action="auth.login", resource_type="user", user_id=user.id, ip=ip
        )
        log.info("auth.logged_in", user_id=str(user.id))
        return tokens

    async def refresh(
        self,
        *,
        refresh_token: str,
        user_agent: str | None,
        ip: str | None,
    ) -> IssuedTokens:
        now = self._clock()
        token_hash = hash_refresh_token(refresh_token)
        record = await self._store.get_refresh_token(token_hash)
        if record is None:
            raise RefreshTokenInvalidError

        if record.revoked_at is not None:
            # Reuse of an already-rotated (or already-logged-out) token: stolen or replayed.
            # The whole family goes, including whatever legitimate token replaced this one.
            await self._store.revoke_refresh_token_family(record.family_id, at=now)
            await self._store.record_audit_event(
                action="auth.refresh_reuse_detected",
                resource_type="refresh_token",
                user_id=record.user_id,
                ip=ip,
            )
            log.warning("auth.refresh_reuse_detected", user_id=str(record.user_id))
            raise RefreshTokenReusedError

        if record.expires_at <= now:
            raise RefreshTokenInvalidError

        user = await self._store.get_user_by_id(record.user_id)
        if user is None or user.status != "active":
            raise RefreshTokenInvalidError

        # Rotate: the presented token is spent the instant it is used, whether or not the
        # response that carries its replacement ever reaches the client.
        await self._store.revoke_refresh_token(token_hash, at=now)
        tokens = await self._issue_tokens(
            user, user_agent=user_agent, ip=ip, now=now, family_id=record.family_id
        )
        await self._store.record_audit_event(
            action="auth.refresh", resource_type="refresh_token", user_id=user.id, ip=ip
        )
        return tokens

    async def logout(
        self,
        *,
        access_claims: AccessTokenClaims,
        refresh_token: str | None,
        everywhere: bool,
    ) -> None:
        """End the current session, or every session, immediately.

        Revoking the refresh token stops *future* refreshes; denylisting the access token's
        `jti` stops the *current* one from being honoured for the rest of its own TTL — a
        logout that only did the former would leave the caller "logged out" yet still able
        to call the API with the bearer token already in hand for up to 15 more minutes.
        """
        now = self._clock()
        if refresh_token is not None:
            await self._store.revoke_refresh_token(hash_refresh_token(refresh_token), at=now)
        if everywhere:
            await self._store.revoke_all_refresh_tokens(access_claims.user_id, at=now)

        remaining = access_claims.expires_at - now
        if remaining > dt.timedelta(0):
            await self._cache.set_json(
                revoked_key(access_claims.jti),
                {"revoked_at": now.isoformat()},
                ttl_seconds=max(1, int(remaining.total_seconds())),
            )

        await self._store.record_audit_event(
            action="auth.logout",
            resource_type="user",
            user_id=access_claims.user_id,
            details={"everywhere": everywhere},
        )
        log.info("auth.logged_out", user_id=str(access_claims.user_id), everywhere=everywhere)

    async def _issue_tokens(
        self,
        user: UserRecord,
        *,
        user_agent: str | None,
        ip: str | None,
        now: dt.datetime,
        family_id: uuid.UUID | None = None,
    ) -> IssuedTokens:
        secret = self._settings.jwt_secret.get_secret_value()
        access_token = create_access_token(
            user_id=user.id,
            role=user.role,
            secret=secret,
            ttl_minutes=self._settings.access_token_ttl_min,
            now=now,
        )
        access_expires_at = now + dt.timedelta(minutes=self._settings.access_token_ttl_min)

        refresh_plain = generate_refresh_token()
        refresh_expires_at = now + dt.timedelta(days=self._settings.refresh_token_ttl_days)
        await self._store.create_refresh_token(
            user_id=user.id,
            token_hash=hash_refresh_token(refresh_plain),
            # A fresh family on login; the rotated token stays in the family it came from,
            # which is what lets reuse detection revoke every descendant of one login.
            family_id=family_id or uuid.uuid4(),
            expires_at=refresh_expires_at,
            user_agent=user_agent,
            ip=ip,
        )

        return IssuedTokens(
            access_token=access_token,
            access_token_expires_at=access_expires_at,
            refresh_token=refresh_plain,
            refresh_token_expires_at=refresh_expires_at,
            user=user,
        )
