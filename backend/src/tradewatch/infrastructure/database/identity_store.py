"""PostgreSQL implementation of the identity store port (ADR-0010).

Thin, like `SqlMarketDataStore`: the repositories already know how to write correctly
(`UserRepository.create_or_none`'s `ON CONFLICT`, `RefreshTokenRepository`'s atomic family
revocation), so this adapter's job is only to translate between the domain-shaped `Protocol`
`AuthService` depends on and the ORM rows the repositories return.

One session per instance, matching the request-scoped lifetime `api/dependencies.py` already
gives every route — unlike the market-data store, there is no batching worker on the other
side of this one, so there is no session *factory* to own.
"""

from __future__ import annotations

import datetime as dt
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.application.ports.identity import (
    EmailAlreadyRegisteredError,
    RefreshTokenRecord,
    UserRecord,
)
from tradewatch.domain.enums import UserRole
from tradewatch.infrastructure.database.models import RefreshToken, User
from tradewatch.infrastructure.repositories import (
    AuditLogRepository,
    RefreshTokenRepository,
    UserRepository,
)

__all__ = ["SqlIdentityStore"]


def _user_record(row: User) -> UserRecord:
    return UserRecord(
        id=row.id,
        email=row.email,
        password_hash=row.password_hash,
        role=row.role,
        status=row.status.value if hasattr(row.status, "value") else row.status,
    )


def _token_record(row: RefreshToken) -> RefreshTokenRecord:
    return RefreshTokenRecord(
        user_id=row.user_id,
        family_id=row.family_id,
        expires_at=row.expires_at,
        revoked_at=row.revoked_at,
    )


class SqlIdentityStore:
    """Implements :class:`~tradewatch.application.ports.identity.IdentityStore`."""

    def __init__(self, session: AsyncSession) -> None:
        self._users = UserRepository(session)
        self._tokens = RefreshTokenRepository(session)
        self._audit = AuditLogRepository(session)

    async def get_user_by_email(self, email: str) -> UserRecord | None:
        row = await self._users.get_by_email(email)
        return _user_record(row) if row is not None else None

    async def get_user_by_id(self, user_id: uuid.UUID) -> UserRecord | None:
        row = await self._users.get(user_id)
        return _user_record(row) if row is not None else None

    async def create_user(self, *, email: str, password_hash: str, role: UserRole) -> UserRecord:
        row = await self._users.create_or_none(email=email, password_hash=password_hash, role=role)
        if row is None:
            raise EmailAlreadyRegisteredError(email)
        return _user_record(row)

    async def record_login(self, user_id: uuid.UUID, *, at: dt.datetime) -> None:
        user = await self._users.get(user_id)
        if user is not None:
            await self._users.record_login(user, at=at)

    async def update_password_hash(self, user_id: uuid.UUID, password_hash: str) -> None:
        user = await self._users.get(user_id)
        if user is not None:
            user.password_hash = password_hash
            await self._users.session.flush()

    async def get_refresh_token(self, token_hash: str) -> RefreshTokenRecord | None:
        row = await self._tokens.get_by_hash(token_hash)
        return _token_record(row) if row is not None else None

    async def create_refresh_token(
        self,
        *,
        user_id: uuid.UUID,
        token_hash: str,
        family_id: uuid.UUID,
        expires_at: dt.datetime,
        user_agent: str | None,
        ip: str | None,
    ) -> None:
        await self._tokens.create(
            user_id=user_id,
            token_hash=token_hash,
            family_id=family_id,
            expires_at=expires_at,
            user_agent=user_agent,
            ip=ip,
        )

    async def revoke_refresh_token(self, token_hash: str, *, at: dt.datetime) -> None:
        row = await self._tokens.get_by_hash(token_hash)
        if row is not None:
            await self._tokens.revoke(row, at=at)

    async def revoke_refresh_token_family(self, family_id: uuid.UUID, *, at: dt.datetime) -> None:
        await self._tokens.revoke_family(family_id, at=at)

    async def revoke_all_refresh_tokens(self, user_id: uuid.UUID, *, at: dt.datetime) -> None:
        await self._tokens.revoke_all_for_user(user_id, at=at)

    async def record_audit_event(
        self,
        *,
        action: str,
        resource_type: str,
        user_id: uuid.UUID | None = None,
        ip: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        await self._audit.record(
            action=action,
            resource_type=resource_type,
            user_id=user_id,
            ip=ip,
            details=details,
        )
