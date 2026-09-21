"""SQLAlchemy ORM models.

Split by bounded context rather than kept in one file, so that a change to the trading
tables does not produce a diff in the same file as a change to market data. Alembic needs
every model imported before it can see the metadata, which is what this module does.

Grouping, and where each group's design reasoning lives:

* :mod:`~.market` — instruments, trades, quotes, bars
* :mod:`~.identity` — users, refresh tokens, audit log
* :mod:`~.trading` — strategies, portfolios, deployments, orders, fills, positions, equity
* :mod:`~.backtesting` — backtest runs, trades, equity curves
* :mod:`~.detection` — model versions, anomaly detections, alert rules, alerts
* :mod:`~.streaming` — consumer progress, so an accumulating consumer is idempotent
"""

from tradewatch.infrastructure.database.base import Base, metadata
from tradewatch.infrastructure.database.models.backtesting import (
    BacktestEquityPoint,
    BacktestRun,
    BacktestTrade,
)
from tradewatch.infrastructure.database.models.detection import (
    Alert,
    AlertRule,
    AnomalyDetection,
    ModelVersion,
)
from tradewatch.infrastructure.database.models.identity import (
    AuditLogEntry,
    RefreshToken,
    User,
)
from tradewatch.infrastructure.database.models.market import Bar, Instrument, Quote, Trade
from tradewatch.infrastructure.database.models.streaming import ConsumerProgress
from tradewatch.infrastructure.database.models.trading import (
    EquityPoint,
    Fill,
    Order,
    Portfolio,
    Position,
    Strategy,
    StrategyDeployment,
)

__all__ = [
    "Alert",
    "AlertRule",
    "AnomalyDetection",
    "AuditLogEntry",
    "BacktestEquityPoint",
    "BacktestRun",
    "BacktestTrade",
    "Bar",
    "Base",
    "ConsumerProgress",
    "EquityPoint",
    "Fill",
    "Instrument",
    "ModelVersion",
    "Order",
    "Portfolio",
    "Position",
    "Quote",
    "RefreshToken",
    "Strategy",
    "StrategyDeployment",
    "Trade",
    "User",
    "metadata",
]
