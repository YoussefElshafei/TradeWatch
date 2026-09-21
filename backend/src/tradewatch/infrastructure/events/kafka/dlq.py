"""The dead-letter topic: where a message goes when it can never succeed.

`ARCHITECTURE.md` §7.4 asks for the original headers, the consumer group, the error and the
failure count. This writes all of them, plus where the message came from, so an operator can
find the original record on the original topic by partition and offset.

**What goes here, exactly.** Only permanent failures. There are two kinds and they arrive by
different routes:

* **Undecodable bytes** — not JSON, not an object, an envelope that fails validation, an
  ``event_type`` this build does not know, a ``schema_version`` from the future, or a
  payload the domain model rejects. These never become domain events, so the adapter that
  holds the bytes quarantines them through :meth:`send_raw`.
* **Decoded but unstorable** — the event is valid and the database refuses it, permanently,
  because it violates a constraint. The application discovers this and quarantines the event
  through :meth:`send`, which is the
  :class:`~tradewatch.application.ports.event_bus.DeadLetterSink` port.

**What never goes here.** A retryable failure. A database that is down produces a
``StorageError``, the offset is not committed, and the events stay on their original topic
where a restart will find them. Quarantining during an outage would convert a recoverable
incident into a manual replay of everything that arrived while it lasted — which is the
single most common way a DLQ makes an outage worse.

**What happens after.** Nothing automatic, and that is deliberate. This phase has no
redrive: a message here is a message a human has to look at, because every reason it is here
is a reason that repeating it unchanged would fail again. An automated replay tool is worth
building once there is evidence about what actually lands here.

**A failure to quarantine is not swallowed.** If the DLQ write fails, this raises, the
consumer does not commit, and the message is redelivered. Acknowledging a message that was
neither stored nor quarantined would be the one code path in this system that loses data
silently.
"""

from __future__ import annotations

import contextlib
import datetime as dt
from typing import Final

from aiokafka import AIOKafkaProducer

from tradewatch.config.settings import KafkaSettings
from tradewatch.domain.events import MarketEvent, event_summary
from tradewatch.infrastructure.events.envelope import encode
from tradewatch.infrastructure.events.kafka.errors import (
    BrokerUnavailableError,
    KafkaPublishError,
    is_retryable,
)
from tradewatch.infrastructure.events.kafka.producer import ProducerClient
from tradewatch.infrastructure.events.kafka.topics import Topics
from tradewatch.observability.logging import get_logger

__all__ = ["DLQ_HEADER_PREFIX", "KafkaDeadLetterSink", "open_dead_letter_sink"]

log = get_logger(__name__)

#: Header namespace, so TradeWatch's diagnostic headers cannot collide with a producer's.
DLQ_HEADER_PREFIX: Final = "tw-dlq-"


class KafkaDeadLetterSink:
    """Publishes unprocessable messages to the dead-letter topic.

    Implements :class:`~tradewatch.application.ports.event_bus.DeadLetterSink`, and adds
    :meth:`send_raw` for the bytes that never became an event.

    It owns its own producer rather than borrowing the ingestion one, because the consumer
    process has no ingestion producer — the two are separate services now, which is the
    whole point of Phase 2.
    """

    def __init__(
        self,
        settings: KafkaSettings,
        *,
        client: ProducerClient,
        topics: Topics | None = None,
        consumer_group: str | None = None,
        clock: object = None,
    ) -> None:
        self._settings = settings
        self._client = client
        self._topics = topics or Topics(settings.kafka_topic_prefix)
        self._consumer_group = consumer_group or settings.kafka_consumer_group
        self._clock = clock
        self._sent = 0

    @property
    def sent(self) -> int:
        """How many messages have been quarantined by this sink."""
        return self._sent

    @property
    def topic(self) -> str:
        return self._topics.dead_letter

    async def send(
        self,
        event: MarketEvent,
        *,
        reason: str,
        error: str,
        attempts: int = 1,
    ) -> None:
        """Quarantine an event that decoded cleanly and can never be stored.

        The body is the event re-encoded. Because :func:`encode` is deterministic, that is
        byte-identical to what was originally published — the DLQ record is the original
        message, not a paraphrase of it.
        """
        await self._publish(
            key=event.partition_key.encode("utf-8"),
            value=encode(event),
            headers=self._headers(
                reason=reason,
                error=error,
                attempts=attempts,
                event_id=str(event.event_id),
                symbol=event.symbol,
                event_type=event.event_type.value,
            ),
            context=event_summary(event),
        )

    async def send_raw(
        self,
        *,
        value: bytes | None,
        key: bytes | None,
        source_topic: str,
        source_partition: int,
        source_offset: int,
        reason: str,
        error: str,
        attempts: int = 1,
    ) -> None:
        """Quarantine bytes that could not be turned into an event.

        The original body is preserved verbatim. Anything else — a "repaired" version, a
        truncated one, a description of it — would destroy the only copy of the thing that
        actually needs diagnosing.
        """
        await self._publish(
            key=key,
            value=value,
            headers=self._headers(
                reason=reason,
                error=error,
                attempts=attempts,
                source_topic=source_topic,
                source_partition=str(source_partition),
                source_offset=str(source_offset),
            ),
            context={
                "source_topic": source_topic,
                "source_partition": source_partition,
                "source_offset": source_offset,
            },
        )

    # --- internals ---------------------------------------------------------

    def _headers(self, **values: str | int | None) -> list[tuple[str, bytes]]:
        """Diagnostic headers, namespaced and UTF-8 encoded.

        Truncated because a Kafka header is not a log aggregator: the error detail is there
        to identify the failure, and the body is there for everything else.
        """
        headers: list[tuple[str, bytes]] = [
            (f"{DLQ_HEADER_PREFIX}consumer-group", self._consumer_group.encode("utf-8")),
            (f"{DLQ_HEADER_PREFIX}failed-at", self._now().encode("utf-8")),
        ]
        for name, value in values.items():
            if value is None:
                continue
            text = str(value)[:512]
            headers.append((f"{DLQ_HEADER_PREFIX}{name.replace('_', '-')}", text.encode("utf-8")))
        return headers

    def _now(self) -> str:
        clock = self._clock
        if callable(clock):
            return str(clock())
        return dt.datetime.now(dt.UTC).isoformat()

    async def _publish(
        self,
        *,
        key: bytes | None,
        value: bytes | None,
        headers: list[tuple[str, bytes]],
        context: dict[str, object],
    ) -> None:
        """Write one record to the dead-letter topic.

        Raises:
            BrokerUnavailableError: If the broker is unwell. The caller must not commit.
            KafkaPublishError: If the record itself is unacceptable to the broker.
        """
        if not self._settings.dlq_enabled:
            # Configured off. The caller logs the drop at error level — this is not a quiet
            # path, it is an explicitly chosen one.
            log.warning("kafka.dlq_disabled", **context)
            return

        try:
            future = await self._client.send(
                self._topics.dead_letter,
                value=value,
                key=key,
                headers=headers,
            )
            await future
        except Exception as error:
            log.error(
                "kafka.dlq_write_failed",
                topic=self._topics.dead_letter,
                error=str(error),
                error_type=type(error).__name__,
                **context,
            )
            if is_retryable(error):
                raise BrokerUnavailableError(
                    f"dead-letter write failed: {type(error).__name__}: {error}"
                ) from error
            raise KafkaPublishError(
                f"dead-letter write failed: {type(error).__name__}: {error}"
            ) from error

        self._sent += 1
        log.warning(
            "kafka.dead_lettered",
            topic=self._topics.dead_letter,
            total_dead_lettered=self._sent,
            **context,
        )


async def open_dead_letter_sink(
    settings: KafkaSettings,
    *,
    topics: Topics | None = None,
    consumer_group: str | None = None,
    client_id: str = "tradewatch-consumer-dlq",
) -> tuple[KafkaDeadLetterSink, AIOKafkaProducer]:
    """Build and connect a dead-letter sink, returning it and the client to close.

    Exists so the consumer worker does not start a bare ``AIOKafkaProducer`` itself. Doing
    that raised ``aiokafka``'s own connection error straight through the worker — the one
    place that is supposed to speak in retryable-or-not terms — and left an unclosed client
    behind when the broker was down, which surfaces later as an unraisable error from a
    finaliser.

    Raises:
        BrokerUnavailableError: If the broker cannot be reached.
    """
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap,
        client_id=client_id,
        # The dead-letter topic is the last copy of a message nobody else kept. It gets the
        # same durability as the market topics, not less.
        acks="all",
        enable_idempotence=True,
    )
    try:
        await producer.start()
    except Exception as error:
        with contextlib.suppress(Exception):
            await producer.stop()
        raise BrokerUnavailableError(
            f"cannot reach Kafka at {settings.kafka_bootstrap} to open the dead-letter "
            f"topic: {type(error).__name__}: {error}"
        ) from error

    sink = KafkaDeadLetterSink(
        settings,
        client=producer,
        topics=topics,
        consumer_group=consumer_group,
    )
    log.info(
        "kafka.dlq_ready",
        topic=sink.topic,
        bootstrap=settings.kafka_bootstrap,
        consumer_group=sink._consumer_group,
    )
    return sink, producer
