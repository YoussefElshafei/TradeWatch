"""Closed sets of values shared by the domain and the database.

Each of these becomes a native PostgreSQL enum type. That is a deliberate trade:

* the database rejects a value the application never intended, rather than storing a typo
  that is only discovered when a query returns nothing;
* an enum column is four bytes rather than a repeated string.

The cost is that changing one is a migration. Adding a value is cheap
(`ALTER TYPE ... ADD VALUE`); removing or renaming one is not. So only genuinely closed
sets live here. `bar_interval` qualifies — an interval TradeWatch cannot aggregate is not a
valid interval, whatever the configuration says. Anything open-ended, such as an alert
channel name, stays a plain string.

Members use ``StrEnum`` so a value compares equal to its wire form and serializes without a
custom encoder.
"""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum

__all__ = [
    "AlertSeverity",
    "AssetClass",
    "BacktestStatus",
    "BarInterval",
    "DeploymentStatus",
    "InstrumentStatus",
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "SignalDirection",
    "UserRole",
    "UserStatus",
]


class UserRole(StrEnum):
    """`ARCHITECTURE.md` §9.3.

    Three fixed roles, stored as a column on ``users``. There is deliberately no
    roles table and no permissions table: a join to resolve three constant values would
    be machinery without a question to answer. The moment a role becomes something a user
    can define, this becomes a table and that is a migration worth writing.
    """

    VIEWER = "viewer"
    TRADER = "trader"
    ADMIN = "admin"


class UserStatus(StrEnum):
    ACTIVE = "active"
    SUSPENDED = "suspended"
    DELETED = "deleted"


class AssetClass(StrEnum):
    """Only what Phase 3 will actually ingest.

    Crypto, FX, and options are decision D2 and each brings its own session calendar and
    contract semantics. Adding a member later is one `ALTER TYPE`; pretending to support
    an asset class whose rules are not implemented is worse than not listing it.
    """

    EQUITY = "equity"
    ETF = "etf"
    INDEX = "index"


class InstrumentStatus(StrEnum):
    ACTIVE = "active"
    HALTED = "halted"
    DELISTED = "delisted"


class BarInterval(StrEnum):
    """Supported bar intervals.

    1m is the only interval the aggregator produces from trades; everything else is a
    rollup of it (`ARCHITECTURE.md` §3.3).
    """

    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"

    @property
    def duration(self) -> timedelta:
        """How long one bar of this interval covers."""
        return _INTERVAL_DURATIONS[self]

    @property
    def is_base(self) -> bool:
        """True for the interval built directly from trades rather than rolled up."""
        return self is BarInterval.ONE_MINUTE


_INTERVAL_DURATIONS: dict[BarInterval, timedelta] = {
    BarInterval.ONE_MINUTE: timedelta(minutes=1),
    BarInterval.FIVE_MINUTES: timedelta(minutes=5),
    BarInterval.ONE_HOUR: timedelta(hours=1),
    BarInterval.ONE_DAY: timedelta(days=1),
}


class SignalDirection(StrEnum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


class OrderSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    """Market and limit only.

    Stops, brackets, and the rest are decision D16: long-only market and limit orders
    exercise the entire signal-to-fill path, and every further type adds simulator
    semantics that would need their own tests.
    """

    MARKET = "market"
    LIMIT = "limit"


class OrderStatus(StrEnum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"

    @property
    def is_terminal(self) -> bool:
        """True when no further transition is possible."""
        return self in {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}


class DeploymentStatus(StrEnum):
    """Paper-trading deployment lifecycle.

    ``ERRORED`` exists so that a strategy raising an exception stops itself without
    stopping anyone else's (`ARCHITECTURE.md` F14).
    """

    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERRORED = "errored"


class BacktestStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        return self in {
            BacktestStatus.COMPLETED,
            BacktestStatus.FAILED,
            BacktestStatus.CANCELLED,
        }


class AlertSeverity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
