"""Market data processing: consume normalized events, persist them durably."""

from tradewatch.application.processing.errors import (
    ProcessingError,
    StorageError,
    UnprocessableEventError,
)
from tradewatch.application.processing.service import MarketDataProcessor, ProcessorStats

__all__ = [
    "MarketDataProcessor",
    "ProcessingError",
    "ProcessorStats",
    "StorageError",
    "UnprocessableEventError",
]
