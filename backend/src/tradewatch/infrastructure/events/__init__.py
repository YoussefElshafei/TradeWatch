"""Event bus adapters.

Two of them, and both are permanent.
:class:`~tradewatch.infrastructure.events.memory.InMemoryEventBus` is the Phase 1 in-process
queue, kept forever because it is what every consumer is unit tested against. The ``kafka``
subpackage is the Phase 2 durable transport (ADR-0001), and is the primary path for
development and production alike.

The Kafka adapter is not imported here. It pulls in ``aiokafka``, and a test that only wants
an in-memory bus should not pay for a Kafka client — import it from
``tradewatch.infrastructure.events.kafka`` directly.
"""

from tradewatch.infrastructure.events.envelope import (
    EventDecodeError,
    EventEncodeError,
    EventEnvelope,
    decode,
    encode,
)
from tradewatch.infrastructure.events.memory import (
    EventBusError,
    FailingPublisher,
    InMemoryEventBus,
    RecordingPublisher,
)

__all__ = [
    "EventBusError",
    "EventDecodeError",
    "EventEncodeError",
    "EventEnvelope",
    "FailingPublisher",
    "InMemoryEventBus",
    "RecordingPublisher",
    "decode",
    "encode",
]
