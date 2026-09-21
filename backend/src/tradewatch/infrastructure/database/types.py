"""Mapping domain enums onto native PostgreSQL enum types."""

from __future__ import annotations

from enum import StrEnum

from sqlalchemy import Enum as SAEnum

from tradewatch.domain.enums import (
    AlertSeverity,
    AssetClass,
    BacktestStatus,
    BarInterval,
    DeploymentStatus,
    InstrumentStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    SignalDirection,
    UserRole,
    UserStatus,
)


def pg_enum[E: StrEnum](enum_cls: type[E], name: str) -> SAEnum:
    """Build a native PostgreSQL enum type for a domain enum.

    ``values_callable`` is the important part. By default SQLAlchemy persists enum
    *member names*, so ``BarInterval.ONE_MINUTE`` would be stored as ``ONE_MINUTE``. We
    store the value — ``1m`` — which is what the wire format, the configuration file, and
    every log line already say. Getting this wrong is invisible until someone runs a raw
    SQL query and finds values nobody recognises.

    ``create_constraint=False`` because a native enum type already restricts the column;
    an additional CHECK would be the same rule stated twice.

    The type is deliberately **not** attached to the shared metadata. Enum lifecycle is
    managed explicitly in migrations instead (``CREATE TYPE`` / ``DROP TYPE``), which is
    the only way a downgrade leaves the database as it found it — metadata-attached types
    survive ``DROP TABLE`` and then collide on the next upgrade.
    """
    return SAEnum(
        enum_cls,
        name=name,
        native_enum=True,
        create_constraint=False,
        validate_strings=True,
        values_callable=lambda enum: [member.value for member in enum],
    )


user_role_enum = pg_enum(UserRole, "user_role")
user_status_enum = pg_enum(UserStatus, "user_status")
asset_class_enum = pg_enum(AssetClass, "asset_class")
instrument_status_enum = pg_enum(InstrumentStatus, "instrument_status")
bar_interval_enum = pg_enum(BarInterval, "bar_interval")
signal_direction_enum = pg_enum(SignalDirection, "signal_direction")
order_side_enum = pg_enum(OrderSide, "order_side")
order_type_enum = pg_enum(OrderType, "order_type")
order_status_enum = pg_enum(OrderStatus, "order_status")
deployment_status_enum = pg_enum(DeploymentStatus, "deployment_status")
backtest_status_enum = pg_enum(BacktestStatus, "backtest_status")
alert_severity_enum = pg_enum(AlertSeverity, "alert_severity")

__all__ = [
    "alert_severity_enum",
    "asset_class_enum",
    "backtest_status_enum",
    "bar_interval_enum",
    "deployment_status_enum",
    "instrument_status_enum",
    "order_side_enum",
    "order_status_enum",
    "order_type_enum",
    "pg_enum",
    "signal_direction_enum",
    "user_role_enum",
    "user_status_enum",
]
