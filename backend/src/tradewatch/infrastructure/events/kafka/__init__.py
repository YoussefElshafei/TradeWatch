"""The Kafka event backbone (ADR-0001, Phase 2).

Four modules, each behind a port the application already had:

* :mod:`~tradewatch.infrastructure.events.kafka.producer` implements ``EventPublisher``, so
  ingestion publishes to a durable log instead of an in-process queue.
* :mod:`~tradewatch.infrastructure.events.kafka.consumer` implements
  ``EventBatchConsumer``, so the market data processor reads from one.
* :mod:`~tradewatch.infrastructure.events.kafka.dlq` implements ``DeadLetterSink``.
* :mod:`~tradewatch.infrastructure.events.kafka.admin` creates the topics, because the
  broker is configured not to.

Nothing above the ports imports anything from here, and ``import-linter`` enforces it.
"""

from tradewatch.infrastructure.events.kafka.admin import ensure_topics
from tradewatch.infrastructure.events.kafka.consumer import (
    ConsumerClient,
    KafkaConsumerStats,
    KafkaEventConsumer,
)
from tradewatch.infrastructure.events.kafka.dlq import (
    KafkaDeadLetterSink,
    open_dead_letter_sink,
)
from tradewatch.infrastructure.events.kafka.errors import (
    BrokerUnavailableError,
    ConsumerEvictedError,
    KafkaPublishError,
    is_retryable,
)
from tradewatch.infrastructure.events.kafka.producer import (
    KafkaEventPublisher,
    ProducerClient,
    PublisherStats,
)
from tradewatch.infrastructure.events.kafka.topics import Topics, UnroutableEventError

__all__ = [
    "BrokerUnavailableError",
    "ConsumerClient",
    "ConsumerEvictedError",
    "KafkaConsumerStats",
    "KafkaDeadLetterSink",
    "KafkaEventConsumer",
    "KafkaEventPublisher",
    "KafkaPublishError",
    "ProducerClient",
    "PublisherStats",
    "Topics",
    "UnroutableEventError",
    "ensure_topics",
    "is_retryable",
    "open_dead_letter_sink",
]
