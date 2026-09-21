"""The Kafka market data consumer.

Implements :class:`~tradewatch.application.ports.event_bus.EventBatchConsumer`: it polls a
batch of records, turns each one into a domain event, and hands the batch up. The
application decides what to do with the events and when to commit; this adapter decides
nothing about persistence and knows nothing about it.

**Auto-commit is off, permanently.** ``enable_auto_commit=True`` acknowledges on a timer,
which means it acknowledges messages that have been *fetched*, not messages that have been
*stored*. That single setting converts at-least-once into at-most-once, and the data it
loses is the data that was in flight when a consumer died — exactly the data an outage makes
most valuable. Offsets move only when the application calls :meth:`commit`.

**A malformed message is this module's problem, not the application's.** The bytes never
become a domain event, so there is nothing to hand upwards. It is quarantined here, and the
partition keeps moving. This is failure F3: one bad message must never stop a partition.

**The case that looks harmless and is not:** a poll where *every* record fails to decode.
The batch handed up is empty, so the application has nothing to store and nothing to commit
— and those offsets would be redelivered forever, a poison-pill loop that no counter would
obviously explain. So when a poll produced records but yielded no events, this adapter
commits them itself, after their dead-letter writes have been acknowledged. They are fully
handled: quarantined and durable. Anything earlier was already committed by the application,
because it commits after every non-empty batch.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import CommitFailedError

from tradewatch.application.ports.event_bus import StreamPosition
from tradewatch.config.settings import KafkaSettings
from tradewatch.domain.events import MarketEvent
from tradewatch.infrastructure.events.envelope import EventDecodeError, decode
from tradewatch.infrastructure.events.kafka.dlq import KafkaDeadLetterSink
from tradewatch.infrastructure.events.kafka.errors import (
    BrokerUnavailableError,
    ConsumerEvictedError,
    is_retryable,
)
from tradewatch.infrastructure.events.kafka.topics import Topics
from tradewatch.observability.logging import get_logger

__all__ = ["ConsumerClient", "KafkaConsumerStats", "KafkaEventConsumer"]

log = get_logger(__name__)


class ConsumerClient(Protocol):
    """The slice of ``AIOKafkaConsumer`` this adapter uses.

    Declared so the failure tests can drive a consumer that raises on connect, returns
    malformed records, or fails to commit — without a broker, and without patching the
    library.
    """

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def getmany(self, *, timeout_ms: int, max_records: int) -> dict[Any, list[Any]]: ...

    async def commit(self) -> None: ...


@dataclass(slots=True)
class KafkaConsumerStats:
    """Counters for the transport half of consumption.

    What happened to the *rows* is
    :class:`~tradewatch.application.processing.service.ProcessorStats`; what happened to the
    *offsets* is :class:`~tradewatch.application.processing.consumer.ConsumerStats`. These
    are about the messages themselves: how many arrived and how many could be read.
    """

    polls: int = 0
    records: int = 0
    decoded: int = 0
    #: Messages that could not be turned into an event. Always permanent.
    malformed: int = 0
    #: Malformed messages successfully written to the dead-letter topic.
    dead_lettered: int = 0
    #: Malformed messages dropped because the DLQ is switched off.
    dropped: int = 0
    commits: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "polls": self.polls,
            "records": self.records,
            "decoded": self.decoded,
            "malformed": self.malformed,
            "dead_lettered": self.dead_lettered,
            "dropped": self.dropped,
            "commits": self.commits,
        }


class KafkaEventConsumer:
    """Reads market data events from Kafka, one batch per poll."""

    def __init__(
        self,
        settings: KafkaSettings,
        *,
        client: ConsumerClient | None = None,
        client_factory: Callable[[], ConsumerClient] | None = None,
        topics: Topics | None = None,
        dead_letters: KafkaDeadLetterSink | None = None,
        group_id: str | None = None,
        subscribe_to: Sequence[str] | None = None,
    ) -> None:
        self._settings = settings
        self._topics = topics or Topics(settings.kafka_topic_prefix)
        self._group_id = group_id or settings.kafka_consumer_group
        #: Which topics this consumer reads. Defaults to every market topic, which is what
        #: the persistence consumer wants. The bar aggregator passes the trades topic alone:
        #: subscribing to quotes and bars as well would hand it events it must skip, and
        #: "ignored" is a counter that should stay at zero rather than one that is expected
        #: to be large.
        self._subscribed = tuple(subscribe_to) if subscribe_to else self._topics.market
        self._client = client
        self._client_factory = client_factory
        self._dead_letters = dead_letters
        self._started = False
        self._stats = KafkaConsumerStats()

    @property
    def stats(self) -> KafkaConsumerStats:
        return self._stats

    @property
    def topics(self) -> Topics:
        return self._topics

    @property
    def subscribed(self) -> tuple[str, ...]:
        return self._subscribed

    @property
    def group_id(self) -> str:
        return self._group_id

    @property
    def started(self) -> bool:
        return self._started

    # --- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        """Join the consumer group and subscribe.

        Raises:
            BrokerUnavailableError: If the broker cannot be reached. Failing here rather
                than on the first poll means a wrong bootstrap address is a startup error,
                not a consumer that runs forever and reads nothing.
        """
        if self._started:
            return

        if self._client is None:
            self._client = (self._client_factory or self._build_client)()

        try:
            await self._client.start()
        except Exception as error:
            # See the producer: a client that failed to connect still has sockets and
            # background tasks, and leaving it for the finaliser turns a clear startup
            # error into an unraisable exception somewhere else entirely.
            failed, self._client = self._client, None
            with contextlib.suppress(Exception):
                await failed.stop()
            raise BrokerUnavailableError(
                f"cannot reach Kafka at {self._settings.kafka_bootstrap}: "
                f"{type(error).__name__}: {error}"
            ) from error

        self._started = True
        log.info(
            "kafka.consumer_started",
            bootstrap=self._settings.kafka_bootstrap,
            group_id=self._group_id,
            topics=list(self._subscribed),
            auto_offset_reset=self._settings.kafka_auto_offset_reset,
            batch_size=self._settings.consumer_batch_size,
        )

    async def stop(self) -> None:
        """Leave the group and disconnect.

        No commit on the way out. Anything consumed and not yet committed was, by
        definition, not finished — letting it be redelivered is the correct answer, and
        committing here to make shutdown look tidy would be the lie that loses it.
        """
        if not self._started or self._client is None:
            return
        await self._client.stop()
        self._started = False
        log.info("kafka.consumer_stopped", group_id=self._group_id, **self._stats.as_dict())

    def _build_client(self) -> ConsumerClient:
        consumer: ConsumerClient = AIOKafkaConsumer(
            *self._subscribed,
            bootstrap_servers=self._settings.kafka_bootstrap,
            group_id=self._group_id,
            client_id=f"tradewatch-consumer-{self._group_id}",
            # The setting that keeps at-least-once honest. See the module docstring.
            enable_auto_commit=False,
            auto_offset_reset=self._settings.kafka_auto_offset_reset,
            session_timeout_ms=self._settings.kafka_consumer_session_timeout_ms,
            heartbeat_interval_ms=self._settings.kafka_consumer_heartbeat_ms,
            max_poll_interval_ms=self._settings.consumer_max_poll_interval_ms,
        )
        return consumer

    # --- consuming ---------------------------------------------------------

    async def positioned_batches(
        self,
    ) -> AsyncIterator[Sequence[tuple[StreamPosition, MarketEvent]]]:
        """Like :meth:`batches`, but each event carries where it sat in its partition.

        Used by the bar aggregator, whose output accumulates and therefore cannot be made
        idempotent by its writes alone — see
        :class:`~tradewatch.application.ports.event_bus.StreamPosition`.

        The two iteration methods share :meth:`_poll_once`, so the decoding, quarantining and
        poison-poll handling cannot drift between them.
        """
        while True:
            positioned = await self._poll_once()
            yield positioned

    async def batches(self) -> AsyncIterator[Sequence[MarketEvent]]:
        """Yield one batch of decoded events per poll, until cancelled.

        An empty batch means the poll timed out with nothing to read. It is yielded rather
        than swallowed so the caller gets a regular turn to notice a shutdown request
        instead of being blocked inside this loop.
        """
        while True:
            positioned = await self._poll_once()
            yield [event for _position, event in positioned]

    async def _poll_once(self) -> list[tuple[StreamPosition, MarketEvent]]:
        """One poll: fetch, decode, quarantine what cannot be read, return the survivors."""
        client = self._require_client()
        timeout = self._settings.kafka_consumer_poll_timeout_ms
        max_records = self._settings.consumer_batch_size

        try:
            fetched = await client.getmany(timeout_ms=timeout, max_records=max_records)
        except Exception as error:
            if is_retryable(error):
                raise BrokerUnavailableError(
                    f"poll failed: {type(error).__name__}: {error}"
                ) from error
            raise

        self._stats.polls += 1
        positioned: list[tuple[StreamPosition, MarketEvent]] = []
        records = 0

        for partition, messages in fetched.items():
            for message in messages:
                records += 1
                event = await self._decode(message, partition)
                if event is not None:
                    positioned.append((self._position_of(message, partition), event))

        self._stats.records += records
        self._stats.decoded += len(positioned)

        if records and not positioned:
            # Every record in this poll was quarantined. Their dead-letter writes are
            # acknowledged, so they are finished — but the caller is about to see an empty
            # batch and will not commit. Without this, the same poison records would be
            # redelivered on every poll, forever.
            await self.commit()
            log.warning("kafka.poison_poll_committed", records=records)

        return positioned

    @staticmethod
    def _position_of(message: Any, partition: Any) -> StreamPosition:
        """Name the partition the way the progress table stores it."""
        topic = getattr(message, "topic", None) or getattr(partition, "topic", "unknown")
        raw_index = getattr(message, "partition", None)
        if raw_index is None:
            raw_index = getattr(partition, "partition", None)
        index = int(raw_index) if raw_index is not None else 0
        return StreamPosition(
            stream=f"{topic}/{index}", offset=int(getattr(message, "offset", 0) or 0)
        )

    async def commit(self) -> None:
        """Commit the offsets of everything consumed so far.

        Raises:
            ConsumerEvictedError: If this consumer was removed from its group before the
                commit landed — a local stall, not a broker fault.
            BrokerUnavailableError: If the commit could not be recorded for any other
                reason. Either way the caller must treat it as "these events may be
                redelivered", which is safe because the writes behind them are idempotent.
        """
        client = self._require_client()
        try:
            await client.commit()
        except CommitFailedError as error:
            # Distinguished from a broker problem on purpose. The broker is fine; this
            # process stopped polling long enough to be thrown out of its group, and the
            # fix is upstream of Kafka entirely.
            log.error(
                "kafka.consumer_evicted",
                group_id=self._group_id,
                error=str(error),
                detail=(
                    "this consumer stopped polling for longer than max_poll_interval_ms "
                    "and its partitions were reassigned; the broker is not the problem"
                ),
            )
            raise ConsumerEvictedError(
                f"consumer was evicted from group {self._group_id!r} before the commit "
                f"landed: {type(error).__name__}"
            ) from error
        except Exception as error:
            log.error(
                "kafka.commit_failed",
                group_id=self._group_id,
                error=str(error),
                error_type=type(error).__name__,
            )
            raise BrokerUnavailableError(
                f"commit failed: {type(error).__name__}: {error}"
            ) from error
        self._stats.commits += 1

    # --- internals ---------------------------------------------------------

    async def _decode(self, message: Any, partition: Any) -> MarketEvent | None:
        """Turn one record into an event, or quarantine it and return None."""
        try:
            return decode(message.value)
        except EventDecodeError as error:
            self._stats.malformed += 1
            await self._quarantine(message, partition, error)
            return None

    async def _quarantine(self, message: Any, partition: Any, error: EventDecodeError) -> None:
        """Send one undecodable record to the dead-letter topic.

        A failure to write it propagates. The offset is then not committed and the record
        is redelivered — which is right, because the alternative is acknowledging a message
        that was neither stored nor quarantined.
        """
        source_topic = getattr(message, "topic", None) or getattr(partition, "topic", "unknown")
        # A record carries its own topic and partition; a TopicPartition key carries them
        # too. Either can be absent on a hand-built test record, so neither is assumed.
        raw_partition = getattr(message, "partition", None)
        if raw_partition is None:
            raw_partition = getattr(partition, "partition", None)
        source_partition = int(raw_partition) if raw_partition is not None else -1
        source_offset = int(getattr(message, "offset", None) or -1)

        context = {
            "source_topic": source_topic,
            "source_partition": source_partition,
            "source_offset": source_offset,
            "reason": error.reason,
            "event_id": str(error.event_id) if error.event_id else None,
        }

        if self._dead_letters is None or not self._settings.dlq_enabled:
            self._stats.dropped += 1
            log.error(
                "kafka.malformed_dropped",
                error=error.message,
                total_dropped=self._stats.dropped,
                **context,
            )
            return

        await self._dead_letters.send_raw(
            value=getattr(message, "value", None),
            key=getattr(message, "key", None),
            source_topic=str(source_topic),
            source_partition=source_partition,
            source_offset=source_offset,
            reason=error.reason,
            error=error.message,
        )
        self._stats.dead_lettered += 1
        log.warning(
            "kafka.malformed_quarantined",
            error=error.message,
            total_malformed=self._stats.malformed,
            **context,
        )

    def _require_client(self) -> ConsumerClient:
        if self._client is None or not self._started:
            raise BrokerUnavailableError("consumer is not started; call start() first")
        return self._client
