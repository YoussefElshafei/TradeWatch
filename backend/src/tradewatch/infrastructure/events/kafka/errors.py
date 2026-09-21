"""Kafka failures, sorted into the two categories the callers actually act on.

The split is the same one the storage adapter makes, for the same reason: a caller that has
to understand ``aiokafka``'s exception hierarchy to decide whether to retry is a caller that
will eventually get it wrong. Everything crossing this boundary answers one question —
``retryable``.

:class:`BrokerUnavailableError` means the *broker* is the problem: no connection, a request
that timed out, no leader for a partition, a coordinator mid-election. Transient by nature,
worth retrying, and never a reason to discard an event.

:class:`KafkaPublishError` means this particular publish will fail the same way every time —
a message larger than the broker accepts, a topic that does not exist and will not be
created, an event no topic carries.

:class:`ConsumerEvictedError` means **this process** is the problem. It stopped polling for
longer than ``max_poll_interval_ms``, the group rebalanced, and its partitions went to
somebody else — so the commit had nowhere to land. Retryable by restarting, like a broker
outage, and worth its own class anyway: the two send an operator to opposite ends of the
system, and reporting an eviction as "broker unavailable" costs whoever is on call the time
it takes to establish that the broker was fine all along.

Both subclass :class:`~tradewatch.infrastructure.events.memory.EventBusError`, which
:class:`~tradewatch.application.ingestion.service.IngestionService` already catches. That is
why swapping the in-memory bus for Kafka needed no change at all in the ingestion path: the
failure type it was written against in Phase 1 is the one the new adapter raises.
``EventBusError`` keeps living in ``memory`` because that is where Phase 1 defined it and
every existing test imports it from there; moving it would be churn for a tidier import.

The name is ``BrokerUnavailableError`` and not ``KafkaUnavailableError`` because
``aiokafka.errors`` already exports a class by the latter name. Two different exceptions
with one name, one of them ours and one of them the library's, is a debugging session
nobody needs.
"""

from __future__ import annotations

from typing import Final

from aiokafka.errors import (
    CoordinatorNotAvailableError,
    InvalidProducerEpoch,
    InvalidProducerIdMapping,
    KafkaConnectionError,
    KafkaError,
    KafkaTimeoutError,
    LeaderNotAvailableError,
    NodeNotReadyError,
    NotLeaderForPartitionError,
    OutOfOrderSequenceNumber,
    RequestTimedOutError,
    UnknownProducerId,
)

from tradewatch.infrastructure.events.memory import EventBusError

__all__ = [
    "BrokerUnavailableError",
    "ConsumerEvictedError",
    "KafkaPublishError",
    "is_fatal_to_producer_session",
    "is_retryable",
]


class BrokerUnavailableError(EventBusError):
    """The broker could not be reached, or could not answer in time.

    Retryable. By the time this is raised the adapter has already exhausted its own bounded
    attempts, so the caller is being told the broker is genuinely unhealthy rather than that
    one request was unlucky.
    """

    retryable = True


class ConsumerEvictedError(EventBusError):
    """This consumer was removed from its group before its commit landed.

    The broker is healthy. This process stopped calling ``poll()`` for longer than
    ``max_poll_interval_ms`` — starved of CPU or memory, or stuck in a write that took too
    long — so the group rebalanced and reassigned its partitions.

    Retryable, in that restarting resolves it and the uncommitted events are redelivered.
    Nothing was acknowledged, so nothing was lost.

    Observed for real: the whole service set was reaped under memory pressure, both
    consumers were evicted four seconds apart, and both correctly refused to commit. The
    behaviour was right and the error they reported sent the reader to look at Kafka.
    """

    retryable = True


class KafkaPublishError(EventBusError):
    """This publish will fail identically however many times it is repeated.

    Not retryable. Counted and logged by the caller; never silently swallowed.
    """

    retryable = False


#: Failures that are transient even though ``aiokafka`` does not mark them ``retriable``.
#:
#: The library's own flag describes broker *response* codes, so it is ``False`` for the
#: client-side failures that matter most here: a connection that was never made and a
#: request that timed out are the two most retryable things that can happen to a producer,
#: and treating either as permanent would discard market data during a broker restart.
_RETRYABLE: Final[tuple[type[BaseException], ...]] = (
    KafkaConnectionError,
    KafkaTimeoutError,
    RequestTimedOutError,
    NodeNotReadyError,
    LeaderNotAvailableError,
    NotLeaderForPartitionError,
    CoordinatorNotAvailableError,
    ConnectionError,
    TimeoutError,
    OSError,
)


def is_retryable(error: BaseException) -> bool:
    """Whether another attempt could plausibly succeed.

    Checks the explicit list first, then defers to ``aiokafka``'s own ``retriable`` flag for
    the broker error codes it does classify correctly.
    """
    if isinstance(error, _RETRYABLE):
        return True
    return bool(getattr(error, "retriable", False)) if isinstance(error, KafkaError) else False


#: Failures that permanently invalidate an **idempotent producer session**, and only the
#: session.
#:
#: Kafka's idempotent producer numbers its records per partition, and the broker tracks
#: that state. When the broker loses it — it restarted, or the producer id expired — the
#: next record arrives with a sequence number the broker cannot place, and every subsequent
#: send from that producer object fails the same way. Forever.
#:
#: `aiokafka` marks these ``retriable = False``, which is correct and incomplete: retrying
#: *this producer* is hopeless, and creating a new one works immediately. Treating them as
#: plainly permanent leaves a producer that connected successfully, reports no outage, and
#: silently stops delivering — which is exactly what happened the first time a broker was
#: restarted under a running producer here.
_SESSION_FATAL: Final[tuple[type[BaseException], ...]] = (
    OutOfOrderSequenceNumber,
    UnknownProducerId,
    InvalidProducerEpoch,
    InvalidProducerIdMapping,
)


def is_fatal_to_producer_session(error: BaseException) -> bool:
    """Whether the producer must be recreated before another send can succeed.

    Recreating it starts a fresh producer id, so records the old session may already have
    delivered can be delivered again. That is at-least-once behaving as documented, and the
    duplicate is absorbed by the consumer's idempotent writes (ADR-0012).
    """
    return isinstance(error, _SESSION_FATAL)
