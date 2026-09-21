"""Users, sessions, and the audit trail.

**Why these tables exist now, in the persistence phase, rather than in Phase 5 with the
authentication logic.** `ARCHITECTURE.md` §14 states the constraint: *auth must land before
any multi-user feature stores user data, or every table needs a retrofit migration and every
endpoint an audit.* Creating `users` alongside the tables that reference it satisfies that
constraint at the schema level. What is deliberately **not** here is the authentication
logic — password hashing, token issuance, rotation, reuse detection. That is ADR-0010 and it
belongs to Phase 5. This module gives those columns a home; it does not pretend to fill them
safely.

**Roles are a column, not a table.** `ARCHITECTURE.md` §9.3 defines exactly three roles.
A `roles` table plus a `permissions` table plus a join table would be three joins to resolve
three constant values — machinery with no question to answer. The moment a role becomes
something a user can define, that is a table and a migration worth writing.
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import INET, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tradewatch.domain.enums import UserRole, UserStatus
from tradewatch.infrastructure.database.base import (
    Base,
    TimestampedMixin,
    UUIDPrimaryKey,
)
from tradewatch.infrastructure.database.types import user_role_enum, user_status_enum

if TYPE_CHECKING:
    from tradewatch.infrastructure.database.models.backtesting import BacktestRun
    from tradewatch.infrastructure.database.models.detection import Alert, AlertRule
    from tradewatch.infrastructure.database.models.trading import Portfolio, Strategy


class User(UUIDPrimaryKey, TimestampedMixin, Base):
    """An account.

    Everything a user owns cascades on delete: strategies, portfolios, backtests, alert
    rules, alerts, sessions. That is the right default for a simulation platform — there is
    no real money and no regulatory retention requirement, so "delete my account" should
    actually delete it. Market data does not cascade, because it belongs to the market
    rather than to any user.
    """

    __tablename__ = "users"

    email: Mapped[str] = mapped_column(String(320), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[UserRole] = mapped_column(
        user_role_enum, nullable=False, server_default=UserRole.VIEWER.value
    )
    status: Mapped[UserStatus] = mapped_column(
        user_status_enum, nullable=False, server_default=UserStatus.ACTIVE.value
    )
    last_login_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))

    refresh_tokens: Mapped[list[RefreshToken]] = relationship(
        back_populates="user", cascade="all, delete-orphan", passive_deletes=True
    )
    strategies: Mapped[list[Strategy]] = relationship(
        back_populates="user", cascade="all, delete-orphan", passive_deletes=True
    )
    portfolios: Mapped[list[Portfolio]] = relationship(
        back_populates="user", cascade="all, delete-orphan", passive_deletes=True
    )
    backtest_runs: Mapped[list[BacktestRun]] = relationship(
        back_populates="user", cascade="all, delete-orphan", passive_deletes=True
    )
    alert_rules: Mapped[list[AlertRule]] = relationship(
        back_populates="user", cascade="all, delete-orphan", passive_deletes=True
    )
    alerts: Mapped[list[Alert]] = relationship(
        back_populates="user", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        # Case-insensitive uniqueness without the `citext` extension. A functional unique
        # index on lower(email) gives the same guarantee, works on every managed
        # PostgreSQL whether or not it allows extensions, and keeps the stored value
        # exactly as the user typed it. The cost is that every lookup must also say
        # `lower(email) = lower(:email)` to use the index — which the repository does, in
        # one place.
        Index("uq_users_email_lower", func.lower(email), unique=True),
    )


class RefreshToken(UUIDPrimaryKey, Base):
    """A rotating refresh token (ADR-0010).

    The token itself is never stored — only a hash, so a database leak does not hand over
    live sessions. ``family_id`` groups the rotations descended from one login, which is
    what makes reuse detection possible: presenting an already-rotated token means it was
    stolen, and the whole family is revoked at once.
    """

    __tablename__ = "refresh_tokens"

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    token_hash: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)
    family_id: Mapped[uuid.UUID] = mapped_column(nullable=False)

    expires_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    user_agent: Mapped[str | None] = mapped_column(String(255))
    ip: Mapped[str | None] = mapped_column(INET)

    user: Mapped[User] = relationship(back_populates="refresh_tokens")

    __table_args__ = (
        # PostgreSQL does not index foreign keys automatically. Without this, revoking a
        # user's tokens and the cascade on user delete both become sequential scans.
        Index("ix_refresh_tokens_user_id", "user_id"),
        Index("ix_refresh_tokens_family_id", "family_id"),
    )


class AuditLogEntry(UUIDPrimaryKey, Base):
    """A security-relevant action.

    ``user_id`` is nullable and set to NULL rather than cascading when an account is
    deleted: the record that an action happened outlives the account that performed it,
    which is the entire point of an audit trail.
    """

    __tablename__ = "audit_log"

    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(50), nullable=False)
    resource_id: Mapped[str | None] = mapped_column(String(64))
    request_id: Mapped[str | None] = mapped_column(String(64))
    ip: Mapped[str | None] = mapped_column(INET)
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_audit_log_user_id_created_at", "user_id", "created_at"),
        Index("ix_audit_log_resource_type_resource_id", "resource_type", "resource_id"),
    )
