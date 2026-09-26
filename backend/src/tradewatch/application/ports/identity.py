"""The identity persistence port (ADR-0010).

`AuthService` lives in `application`, so it cannot import a repository — repositories are
adapters, and adapters live in `infrastructure`. This is the interface it depends on
instead, implemented by
:class:`~tradewatch.infrastructure.database.identity_store.SqlIdentityStore`.

Every method here is one round trip against one already-open request-scoped session; unlike
:class:`~tradewatch.application.ports.market_store.MarketDataStore`, there is no batching —
an auth request touches at most a user row and a handful of session rows, so the unit of
work is naturally small.
"""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from tradewatch.domain.enums import UserRole

__all__ = ["EmailAlreadyRegisteredError", "IdentityStore", "RefreshTokenRecord", "UserRecord"]


class EmailAlreadyRegisteredError(Exception):
    """Raised by `IdentityStore.create_user` when the email is already taken.

    A distinct exception rather than a `None`/truthy return, because "the email exists" and
    "the write failed" must never be confused: `AuthService.register` catches exactly this
    one and nothing else.
    """


@dataclass(frozen=True, slots=True)
class UserRecord:
    """The minimum `AuthService` needs to know about an account."""

    id: uuid.UUID
    email: str
    password_hash: str
    role: UserRole
    status: str


@dataclass(frozen=True, slots=True)
class RefreshTokenRecord:
    """One row of a refresh-token family, as `AuthService` needs to reason about it."""

    user_id: uuid.UUID
    family_id: uuid.UUID
    expires_at: dt.datetime
    revoked_at: dt.datetime | None


@runtime_checkable
class IdentityStore(Protocol):
    """Durable storage for accounts, sessions, and the audit trail."""

    async def get_user_by_email(self, email: str) -> UserRecord | None: ...

    async def get_user_by_id(self, user_id: uuid.UUID) -> UserRecord | None: ...

    async def create_user(self, *, email: str, password_hash: str, role: UserRole) -> UserRecord:
        """Raises `EmailAlreadyRegisteredError` if the (case-insensitive) email is taken."""
        ...

    async def record_login(self, user_id: uuid.UUID, *, at: dt.datetime) -> None: ...

    async def update_password_hash(self, user_id: uuid.UUID, password_hash: str) -> None:
        """Rewrite a hash in place — used only for the transparent Argon2 upgrade path."""
        ...

    async def get_refresh_token(self, token_hash: str) -> RefreshTokenRecord | None: ...

    async def create_refresh_token(
        self,
        *,
        user_id: uuid.UUID,
        token_hash: str,
        family_id: uuid.UUID,
        expires_at: dt.datetime,
        user_agent: str | None,
        ip: str | None,
    ) -> None: ...

    async def revoke_refresh_token(self, token_hash: str, *, at: dt.datetime) -> None: ...

    async def revoke_refresh_token_family(
        self, family_id: uuid.UUID, *, at: dt.datetime
    ) -> None: ...

    async def revoke_all_refresh_tokens(self, user_id: uuid.UUID, *, at: dt.datetime) -> None: ...

    async def record_audit_event(
        self,
        *,
        action: str,
        resource_type: str,
        user_id: uuid.UUID | None = None,
        ip: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None: ...
