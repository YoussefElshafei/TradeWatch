"""Repositories: the only code that writes SQL.

Nothing above this layer builds a query. That is what makes the ownership rule in
:mod:`~.base` enforceable — a scope that can be bypassed is not a scope.
"""

from tradewatch.infrastructure.repositories.backtesting import (
    BacktestEquityPointRepository,
    BacktestTradeRepository,
)
from tradewatch.infrastructure.repositories.base import OwnedRepository, Repository
from tradewatch.infrastructure.repositories.identity import (
    AuditLogRepository,
    RefreshTokenRepository,
    UserRepository,
)
from tradewatch.infrastructure.repositories.market import (
    BarRepository,
    InstrumentRepository,
    QuoteRepository,
    TradeRepository,
)
from tradewatch.infrastructure.repositories.trading import (
    BacktestRunRepository,
    DeploymentRepository,
    EquityPointRepository,
    OrderRepository,
    PortfolioRepository,
    PositionRepository,
    RebuiltPosition,
    StrategyRepository,
)

__all__ = [
    "AuditLogRepository",
    "BacktestEquityPointRepository",
    "BacktestRunRepository",
    "BacktestTradeRepository",
    "BarRepository",
    "DeploymentRepository",
    "EquityPointRepository",
    "InstrumentRepository",
    "OrderRepository",
    "OwnedRepository",
    "PortfolioRepository",
    "PositionRepository",
    "QuoteRepository",
    "RebuiltPosition",
    "RefreshTokenRepository",
    "Repository",
    "StrategyRepository",
    "TradeRepository",
    "UserRepository",
]
