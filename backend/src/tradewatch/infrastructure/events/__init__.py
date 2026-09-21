"""Event bus adapters. In-process for Phase 1; Kafka joins in Phase 2 (ADR-0001)."""

from tradewatch.infrastructure.events.memory import (
    EventBusError,
    FailingPublisher,
    InMemoryEventBus,
    RecordingPublisher,
)

__all__ = [
    "EventBusError",
    "FailingPublisher",
    "InMemoryEventBus",
    "RecordingPublisher",
]
