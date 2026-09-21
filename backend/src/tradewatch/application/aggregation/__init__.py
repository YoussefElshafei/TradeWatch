"""Bar aggregation: trades in, OHLCV bars out (ARCHITECTURE.md 3.3)."""

from tradewatch.application.aggregation.consumer import (
    AggregationConsumerStats,
    BarAggregationService,
)
from tradewatch.application.aggregation.service import (
    BASE_INTERVAL,
    AggregatorStats,
    BarAggregator,
    OpenBar,
    floor_to_interval,
)

__all__ = [
    "BASE_INTERVAL",
    "AggregationConsumerStats",
    "AggregatorStats",
    "BarAggregationService",
    "BarAggregator",
    "OpenBar",
    "floor_to_interval",
]
