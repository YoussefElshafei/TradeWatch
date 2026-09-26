"""Repositories for accounts, sessions, and the audit trail (ADR-0010).

Not owner-scoped: a `User` is not owned by a user, and a `RefreshToken` is looked up by its
hash — the one thing a client actually presents — not fetched by id on behalf of an actor.
`OwnedRepository` exists for rows a *browser session* reaches on behalf of a signed-in user
(strategies, portfolios); these are reached by the auth flow itself, before that actor exists.
"""

from __future__ import annotations

import datetime as dt
import uuid
from collections.abc import Sequence

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from tradewatch.domain.enums import UserRole
from tradewatch.infrastructure.database.models import AuditLogEntry, RefreshToken, User
from tradewatch.infrastructure.repositories.base import Repository, rowcount


class UserRepository(Repository[User]):
    """Accounts."""

    model = User

    async def get_by_email(self, email: str) -> User | None:
        """Case-insensitive lookup, matching `uq_users_email_lower` (see `docs/SCHEMA.md`)."""
        result = await self.session.execute(
            select(User).where(func.lower(User.email) == email.strip().lower())
        )
        return result.scalar_one_or_none()

    async def create_or_none(
        self, *, email: str, password_hash: str, role: UserRole
    ) -> User | None:
        """Insert a new account, or return `None` if the email is already taken.

        `ON CONFLICT DO NOTHING` against `uq_users_email_lower` rather than insert-then-catch
        `IntegrityError`: a caught constraint violation leaves an `AsyncSession` unusable
        until it is rolled back, which would force every caller of `register` to know that —
        a race on registration is common enough (two tabs, a double-click) that the store
        should absorb it, not the route.
        """
        statement = (
            pg_insert(User)
            .values(email=email, password_hash=password_hash, role=role)
            .on_conflict_do_nothing(index_elements=[func.lower(User.email)])
            .returning(User)
        )
        result = await self.session.execute(statement)
        await self.session.flush()
        return result.scalar_one_or_none()

    async def record_login(self, user: User, *, at: dt.datetime) -> None:
        user.last_login_at = at
        await self.session.flush()


class RefreshTokenRepository(Repository[RefreshToken]):
    """Rotating sessions.

    Every row is one *use* of a family: a login creates the family's first token, and every
    refresh revokes the one presented and inserts its replacement in the same family. Reuse
    detection is exactly "the presented hash maps to a row that is already revoked."
    """

    model = RefreshToken

    async def get_by_hash(self, token_hash: str) -> RefreshToken | None:
        result = await self.session.execute(
            select(RefreshToken).where(RefreshToken.token_hash == token_hash)
        )
        return result.scalar_one_or_none()

    async def create(
        self,
        *,
        user_id: uuid.UUID,
        token_hash: str,
        family_id: uuid.UUID,
        expires_at: dt.datetime,
        user_agent: str | None = None,
        ip: str | None = None,
    ) -> RefreshToken:
        return await self.add(
            RefreshToken(
                user_id=user_id,
                token_hash=token_hash,
                family_id=family_id,
                expires_at=expires_at,
                user_agent=user_agent,
                ip=ip,
            )
        )

    async def revoke(self, token: RefreshToken, *, at: dt.datetime) -> None:
        token.revoked_at = at
        await self.session.flush()

    async def revoke_family(self, family_id: uuid.UUID, *, at: dt.datetime) -> int:
        """Revoke every token in a family in one statement.

        Called on reuse detection (a stolen, already-rotated token was presented again) and
        must not miss a row to a lost race with a legitimate concurrent refresh — a single
        `UPDATE ... WHERE family_id = :id AND revoked_at IS NULL` is atomic where a
        select-then-update loop would not be.
        """
        result = await self.session.execute(
            update(RefreshToken)
            .where(RefreshToken.family_id == family_id, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=at)
        )
        await self.session.flush()
        return rowcount(result)

    async def revoke_all_for_user(self, user_id: uuid.UUID, *, at: dt.datetime) -> int:
        """Every active session for a user. The server-side half of "log out everywhere"."""
        result = await self.session.execute(
            update(RefreshToken)
            .where(RefreshToken.user_id == user_id, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=at)
        )
        await self.session.flush()
        return rowcount(result)


class AuditLogRepository(Repository[AuditLogEntry]):
    """Security-relevant events. Append-only — nothing here is ever updated or deleted."""

    model = AuditLogEntry

    async def record(
        self,
        *,
        action: str,
        resource_type: str,
        user_id: uuid.UUID | None = None,
        resource_id: str | None = None,
        request_id: str | None = None,
        ip: str | None = None,
        details: dict[str, object] | None = None,
    ) -> AuditLogEntry:
        return await self.add(
            AuditLogEntry(
                user_id=user_id,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                request_id=request_id,
                ip=ip,
                details=details or {},
            )
        )

    async def list_for_user(
        self, user_id: uuid.UUID, *, limit: int = 100
    ) -> Sequence[AuditLogEntry]:
        """Recent events for one user. Served by `ix_audit_log_user_id_created_at`."""
        result = await self.session.execute(
            select(AuditLogEntry)
            .where(AuditLogEntry.user_id == user_id)
            .order_by(AuditLogEntry.created_at.desc())
            .limit(limit)
        )
        return result.scalars().all()


__all__ = ["AuditLogRepository", "RefreshTokenRepository", "UserRepository"]
