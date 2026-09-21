"""Market data ingestion: provider → deduplicate → publish."""

from tradewatch.application.ingestion.dedupe import DeduplicationStats, EventDeduplicator
from tradewatch.application.ingestion.service import IngestionService, IngestionStats

__all__ = [
    "DeduplicationStats",
    "EventDeduplicator",
    "IngestionService",
    "IngestionStats",
]
