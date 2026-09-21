"""Market data processing: consume normalized events, persist them durably."""

from tradewatch.application.processing.consumer import (
    ConsumerStats,
    MarketDataConsumerService,
)
from tradewatch.application.processing.errors import (
    ProcessingError,
    StorageError,
    UnprocessableEventError,
)
from tradewatch.application.processing.service import MarketDataProcessor, ProcessorStats

__all__ = [
    "ConsumerStats",
    "MarketDataConsumerService",
    "MarketDataProcessor",
    "ProcessingError",
    "ProcessorStats",
    "StorageError",
    "UnprocessableEventError",
]
