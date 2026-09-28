"""Anomaly detection results, model versions, and alerting.

**On naming.** Every column, constraint, and index here says *anomaly*, and that is the only
claim this schema makes: a score saying market activity was statistically unlike a symbol's
own recent behaviour. ADR-0008 is a hard requirement here, not a stylistic preference, and
names the handful of words this schema (and everything built on it) never uses instead.

**Detections are explainable by construction.** Every row carries the `features` that
produced the score, the `threshold` it crossed, and the `model_name` / `model_version` that
produced it. A detection nobody can explain is a detection nobody should act on, and storing
only the score would make explanation impossible after the fact.

**Alerts are deduplicated in the schema, not in application code.** `UNIQUE (alert_rule_id,
dedupe_key)` is what keeps one volatile open from producing four hundred notifications
(`ARCHITECTURE.md` F18). Putting that in the database rather than in a service means it
survives a dispatcher restart, a second dispatcher replica, and a redelivered Kafka message.
"""

from __future__ import annotations

import datetime as dt
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from tradewatch.domain.enums import AlertSeverity
from tradewatch.infrastructure.database.base import (
    Base,
    TimestampedMixin,
    UUIDPrimaryKey,
)
from tradewatch.infrastructure.database.types import alert_severity_enum

if TYPE_CHECKING:
    from tradewatch.infrastructure.database.models.identity import User
    from tradewatch.infrastructure.database.models.market import Instrument


class ModelVersion(UUIDPrimaryKey, Base):
    """A trained detector artifact and how it was produced.

    The training window is recorded because a model evaluated on data it was trained on is
    not evaluated at all, and six months later nobody remembers which rows went in.
    """

    __tablename__ = "model_versions"

    model_name: Mapped[str] = mapped_column(String(50), nullable=False)
    version: Mapped[str] = mapped_column(String(32), nullable=False)
    trained_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    training_window_start: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    training_window_end: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    hyperparameters: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )
    offline_metrics: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )
    artifact_uri: Mapped[str] = mapped_column(String(500), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("model_name", "version", name="uq_model_versions_model_name_version"),
        CheckConstraint(
            "training_window_end > training_window_start", name="training_window_ordered"
        ),
        # At most one active version per model, enforced by a partial unique index rather
        # than by whoever writes the activation code remembering to deactivate the old one.
        Index(
            "uq_model_versions_one_active_per_model",
            "model_name",
            unique=True,
            postgresql_where=text("is_active"),
        ),
    )


class AnomalyDetection(UUIDPrimaryKey, Base):
    """A scored window of market activity that was statistically unusual.

    Not evidence of wrongdoing. Earnings, index rebalances, news, halts, and thin opens all
    produce exactly this signature (ADR-0008).
    """

    __tablename__ = "anomaly_detections"

    # ADR-0012: a deterministic UUIDv5 over (symbol, model_name, model_version, window),
    # so a Kafka redelivery of `anomaly.detected` upserts onto the same row instead of
    # duplicating a detection (`UNIQUE (event_id)`, added after the initial schema — see
    # migration 89b564a365bd for why this table needed one and the others already had it).
    event_id: Mapped[uuid.UUID] = mapped_column(nullable=False, unique=True)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("instruments.id", ondelete="RESTRICT"), nullable=False
    )
    detected_at: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_start: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[dt.datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    anomaly_score: Mapped[Decimal] = mapped_column(nullable=False)
    threshold: Mapped[Decimal] = mapped_column(nullable=False)
    severity: Mapped[AlertSeverity] = mapped_column(alert_severity_enum, nullable=False)

    model_name: Mapped[str] = mapped_column(String(50), nullable=False)
    model_version: Mapped[str] = mapped_column(String(32), nullable=False)
    # The feature values behind the score. Without these a detection cannot be explained,
    # and an unexplainable detection is not shown to anyone.
    features: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, server_default="{}")

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    instrument: Mapped[Instrument] = relationship(back_populates="anomaly_detections")

    __table_args__ = (
        CheckConstraint("window_end > window_start", name="window_ordered"),
        # One symbol's detection history — the per-instrument feed.
        Index("ix_anomaly_detections_instrument_id_detected_at", "instrument_id", "detected_at"),
        # The cross-symbol feed: "what was unusual in the market this morning?". The
        # composite index above cannot serve it, because its leading column is the symbol.
        Index("ix_anomaly_detections_detected_at", "detected_at"),
    )


class AlertRule(UUIDPrimaryKey, TimestampedMixin, Base):
    """A user's standing request to be told about something.

    ``symbols`` is nullable, and NULL means every symbol — distinct from an empty array,
    which would mean none. The distinction is worth the nullable column: "alert me on
    anything" is the most common rule.
    """

    __tablename__ = "alert_rules"

    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    event_types: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    symbols: Mapped[list[str] | None] = mapped_column(ARRAY(Text))
    conditions: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, server_default="{}")
    cooldown_seconds: Mapped[int] = mapped_column(Integer, nullable=False, server_default="300")
    channels: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default="{in_app}"
    )
    is_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")

    user: Mapped[User] = relationship(back_populates="alert_rules")
    alerts: Mapped[list[Alert]] = relationship(
        back_populates="rule", cascade="all, delete-orphan", passive_deletes=True
    )

    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_alert_rules_user_id_name"),
        CheckConstraint("cardinality(event_types) > 0", name="event_types_not_empty"),
        CheckConstraint("cardinality(channels) > 0", name="channels_not_empty"),
        CheckConstraint("cooldown_seconds >= 0", name="cooldown_not_negative"),
        CheckConstraint(
            "symbols IS NULL OR cardinality(symbols) > 0", name="symbols_null_or_not_empty"
        ),
        Index("ix_alert_rules_user_id", "user_id"),
        Index("ix_alert_rules_enabled", "user_id", postgresql_where=text("is_enabled")),
    )


class Alert(UUIDPrimaryKey, Base):
    """A fired rule instance.

    ``user_id`` is denormalized from the rule. That is deliberate: the alert feed query is
    "this user's recent alerts, newest first", which would otherwise need a join to
    `alert_rules` on the hottest read path in the notification UI, and the value can never
    diverge because an alert's rule cannot change owner.
    """

    __tablename__ = "alerts"

    alert_rule_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("alert_rules.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    instrument_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("instruments.id", ondelete="SET NULL")
    )

    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    severity: Mapped[AlertSeverity] = mapped_column(alert_severity_enum, nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, server_default="{}")

    # What makes this alert "the same" as another. Typically rule + symbol + time bucket.
    dedupe_key: Mapped[str] = mapped_column(String(200), nullable=False)

    created_at: Mapped[dt.datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    acknowledged_at: Mapped[dt.datetime | None] = mapped_column(DateTime(timezone=True))

    rule: Mapped[AlertRule] = relationship(back_populates="alerts")
    user: Mapped[User] = relationship(back_populates="alerts")
    instrument: Mapped[Instrument | None] = relationship()

    __table_args__ = (
        # The storm guard. An `INSERT ... ON CONFLICT DO NOTHING` against this constraint
        # is the whole deduplication mechanism, and it holds across dispatcher restarts and
        # replicas in a way that in-process state could not.
        UniqueConstraint("alert_rule_id", "dedupe_key", name="uq_alerts_alert_rule_id_dedupe_key"),
        Index("ix_alerts_user_id_created_at", "user_id", "created_at"),
        # The unread badge: unacknowledged alerts only, which is a small slice of a table
        # that grows without bound.
        Index(
            "ix_alerts_unacknowledged",
            "user_id",
            postgresql_where=text("acknowledged_at IS NULL"),
        ),
        Index("ix_alerts_alert_rule_id", "alert_rule_id"),
    )
