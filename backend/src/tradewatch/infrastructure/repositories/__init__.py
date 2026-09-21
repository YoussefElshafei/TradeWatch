"""Repositories: the only code that writes SQL.

Nothing above this layer builds a query. That is what makes the ownership rule in
:mod:`~.base` enforceable — a scope that can be bypassed is not a scope.
"""

from tradewatch.infrastructure.repositories.base import OwnedRepository, Repository
from tradewatch.infrastructure.repositories.market import (
    BarRepository,
    InstrumentRepository,
    QuoteRepository,
    TradeRepository,
)
from tradewatch.infrastructure.repositories.trading import (
    BacktestRunRepository,
    DeploymentRepository,
    OrderRepository,
    PortfolioRepository,
    PositionRepository,
    StrategyRepository,
)

__all__ = [
    "BacktestRunRepository",
    "BarRepository",
    "DeploymentRepository",
    "InstrumentRepository",
    "OrderRepository",
    "OwnedRepository",
    "PortfolioRepository",
    "PositionRepository",
    "QuoteRepository",
    "Repository",
    "StrategyRepository",
    "TradeRepository",
]
