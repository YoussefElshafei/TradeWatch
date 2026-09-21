"""The Kafka event publisher.

Implements :class:`~tradewatch.application.ports.event_bus.EventPublisher`, so
:class:`~tradewatch.application.ingestion.service.IngestionService` publishes to Kafka
without knowing Kafka exists. That was the Phase 1 claim about the port, and this adapter is
where it gets tested.

**Delivery, stated exactly** (`ARCHITECTURE.md` §7.4):

* ``acks=all`` — the broker answers only once every in-sync replica has the record.
* ``enable_idempotence=True`` — the broker deduplicates the producer's *own* retries by
  sequence number, so a retried batch does not become two records.
* :meth:`publish` and :meth:`publish_many` wait for those acknowledgements before returning.
  A caller that gets no exception knows the broker has the data.

That is **at-least-once**, not exactly-once. Idempotence covers retries within one producer
session; it does not cover a producer that crashes after the broker wrote the record but
before the acknowledgement arrived, and the process that restarts and republishes is a
different session. This is precisely why event ids are deterministic (ADR-0012) and why the
consumer's writes are idempotent — the duplicate is expected and absorbed downstream, not
prevented here.

**Keying.** Every record is keyed by ``event.partition_key``, which is the symbol. Kafka
guarantees order within a partition, so this guarantees order per symbol — the property the
bar aggregator and every strategy depend on. It guarantees nothing across symbols, and this
module claims nothing more.

**Nothing is dropped quietly.** Every failure path increments a counter on
:class:`PublisherStats` and logs with the event's identity. A serialization failure is
counted rather than raised out of a batch, because one unencodable bar must not abandon the
other 239 — but it is logged at error, and the returned count is lower than what was offered,
which is how the caller finds out.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from aiokafka import AIOKafkaProducer

from tradewatch.config.settings import KafkaSettings
from tradewatch.domain.events import MarketEvent, event_summary
from tradewatch.infrastructure.events.envelope import EventEncodeError, encode
from tradewatch.infrastructure.events.kafka.errors import (
    BrokerUnavailableError,
    KafkaPublishError,
    is_fatal_to_producer_session,
    is_retryable,
)
from tradewatch.infrastructure.events.kafka.topics import Topics, UnroutableEventError
from tradewatch.observability.logging import get_logger

__all__ = ["KafkaEventPublisher", "ProducerClient", "PublisherStats"]

log = get_logger(__name__)

#: How long to wait for a poisoned producer to close before abandoning it. See
#: :meth:`KafkaEventPublisher._close_quietly`.
_CLOSE_TIMEOUT_S = 10.0


class ProducerClient(Protocol):
    """The slice of ``AIOKafkaProducer`` this adapter uses.

    Declared so the failure tests can supply a client that refuses to connect, or times out
    on the third send, without a broker and without patching the library. Every producer
    failure path in the test suite is exercised through this protocol.
    """

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def send(
        self,
        topic: str,
        value: bytes | None = ...,
        key: bytes | None = ...,
        partition: int | None = ...,
        timestamp_ms: int | None = ...,
        headers: Sequence[tuple[str, bytes]] | None = ...,
    ) -> Awaitable[Any]: ...

    async def flush(self) -> None: ...


@dataclass(slots=True)
class PublisherStats:
    """Counters for one producer's lifetime.

    Every one of these sits where an event could otherwise have disappeared without trace.
    They become Prometheus metrics in Phase 10.
    """

    published: int = 0
    #: Events that could not be serialized. Permanent; the event never reached a topic.
    encode_failures: int = 0
    #: Events with no topic. A routing bug, not bad data.
    unroutable: int = 0
    #: Sends that failed after every attempt was used up.
    publish_failures: int = 0
    #: Individual attempts that failed and were retried.
    retries: int = 0
    #: Times the producer session was recreated after the broker lost its sequence state.
    session_resets: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "published": self.published,
            "encode_failures": self.encode_failures,
            "unroutable": self.unroutable,
            "publish_failures": self.publish_failures,
            "retries": self.retries,
            "session_resets": self.session_resets,
        }


class KafkaEventPublisher:
    """Publishes normalized market events to Kafka."""

    def __init__(
        self,
        settings: KafkaSettings,
        *,
        client: ProducerClient | None = None,
        client_factory: Callable[[], ProducerClient] | None = None,
        topics: Topics | None = None,
        client_id: str = "tradewatch-producer",
        sleep: Callable[[float], Awaitable[None]] | None = None,
        close_timeout_s: float = _CLOSE_TIMEOUT_S,
    ) -> None:
        self._settings = settings
        self._topics = topics or Topics(settings.kafka_topic_prefix)
        self._client_id = client_id
        self._client = client
        self._client_factory = client_factory
        #: Whether this publisher may replace its own client. False when one was injected:
        #: a caller that handed us a client owns its lifetime, and silently discarding it
        #: would be a surprising thing to do to a test double.
        self._owns_client = client is None
        self._sleep = sleep or asyncio.sleep
        self._close_timeout_s = close_timeout_s
        #: Close tasks that overran their bound. Held so the loop does not collect a
        #: pending task and warn about it from somewhere unrelated.
        self._abandoned: set[asyncio.Task[None]] = set()
        self._started = False
        #: Incremented every time a session is established. A caller records it before a
        #: send and passes it back on failure, so a session that has *already* been rebuilt
        #: by somebody else is not rebuilt again. Ingestion runs a trade stream and a quote
        #: stream concurrently against one publisher, so without this the two of them race:
        #: both see the same poisoned session, both rebuild, and the second one throws away
        #: the working producer the first just created.
        self._generation = 0
        self._reset_lock = asyncio.Lock()
        self._stats = PublisherStats()

    @property
    def stats(self) -> PublisherStats:
        return self._stats

    @property
    def topics(self) -> Topics:
        return self._topics

    @property
    def started(self) -> bool:
        return self._started

    # --- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        """Connect to the broker.

        Raises:
            BrokerUnavailableError: If the broker cannot be reached. Raised rather than
                deferred, so a misconfigured bootstrap address fails at startup instead of
                at the first tick — the same rule the provider settings follow.
        """
        if self._started:
            return

        if self._client is None:
            self._client = (self._client_factory or self._build_client)()

        try:
            await self._client.start()
        except Exception as error:
            # Close the half-open client before reporting. A producer that failed to
            # connect still holds a client object with sockets and background tasks, and
            # leaving it to the garbage collector produces an unraisable error from a
            # finaliser — at a random later moment, in unrelated code.
            failed, self._client = self._client, None
            with contextlib.suppress(Exception):
                await failed.stop()
            raise BrokerUnavailableError(
                f"cannot reach Kafka at {self._settings.kafka_bootstrap}: "
                f"{type(error).__name__}: {error}"
            ) from error

        self._started = True
        self._generation += 1
        log.info(
            "kafka.producer_connected",
            bootstrap=self._settings.kafka_bootstrap,
            acks=self._settings.kafka_producer_acks,
            idempotence=self._settings.kafka_producer_idempotence,
            client_id=self._client_id,
        )

    async def stop(self) -> None:
        """Flush and disconnect.

        ``stop()`` on an ``AIOKafkaProducer`` already flushes, but flushing explicitly first
        means a failure to deliver the tail is reported as a publish failure rather than
        disappearing into a close path nobody checks.
        """
        if not self._started or self._client is None:
            return
        try:
            await self.flush()
        finally:
            await self._client.stop()
            self._started = False
            log.info("kafka.producer_closed", **self._stats.as_dict())

    def _build_client(self) -> ProducerClient:
        """The real client, configured from settings.

        ``acks`` is sent as an int for "0" and "1" because that is what the wire protocol
        wants; only "all" is a string.
        """
        acks: int | str = self._settings.kafka_producer_acks
        if acks in {"0", "1"}:
            acks = int(acks)

        compression = self._settings.kafka_producer_compression
        producer: ProducerClient = AIOKafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap,
            client_id=self._client_id,
            acks=acks,
            enable_idempotence=self._settings.kafka_producer_idempotence,
            linger_ms=self._settings.kafka_producer_linger_ms,
            compression_type=None if compression == "none" else compression,
            request_timeout_ms=self._settings.kafka_producer_request_timeout_ms,
        )
        return producer

    # --- publishing --------------------------------------------------------

    async def publish(self, event: MarketEvent) -> None:
        """Publish one event, waiting for the broker to acknowledge it.

        Raises:
            KafkaPublishError: If the event cannot be encoded or routed. Permanent.
            BrokerUnavailableError: If the broker did not accept it after every attempt.
        """
        record = self._prepare(event)
        if record is None:
            raise KafkaPublishError(f"event {event.event_id} could not be prepared for publish")
        topic, key, payload = record
        await self._send(topic, key, payload, event)
        self._stats.published += 1

    async def publish_many(self, events: Sequence[MarketEvent]) -> int:
        """Publish a batch, returning how many the broker acknowledged.

        Sends everything first and waits for the acknowledgements afterwards, so one
        round-trip covers the batch instead of one per event. A 240-bar backfill is the
        reason this method exists on the port at all.

        An event that cannot be encoded or routed is counted, logged at error, and skipped —
        the returned count is what actually landed, and it is the caller's signal that
        something did not.

        Raises:
            BrokerUnavailableError: If the broker is unreachable. A batch that failed
                because the broker is down is not partial success, and reporting it as
                "0 accepted" would let ingestion carry on as though the events were merely
                unlucky.
        """
        if not events:
            return 0

        prepared: list[tuple[str, bytes, bytes, MarketEvent]] = []
        for event in events:
            record = self._prepare(event)
            if record is not None:
                topic, key, payload = record
                prepared.append((topic, key, payload, event))

        if not prepared:
            return 0

        client = self._require_client()
        generation = self._generation
        futures = []
        for topic, key, payload, _event in prepared:
            futures.append(await client.send(topic, value=payload, key=key))

        results = await asyncio.gather(*futures, return_exceptions=True)

        accepted = 0
        failures: list[BaseException] = []
        for (topic, _key, _payload, event), outcome in zip(prepared, results, strict=True):
            if isinstance(outcome, BaseException):
                failures.append(outcome)
                self._stats.publish_failures += 1
                log.error(
                    "kafka.publish_failed",
                    topic=topic,
                    error=str(outcome),
                    error_type=type(outcome).__name__,
                    **event_summary(event),
                )
                continue
            accepted += 1
            self._stats.published += 1

        if failures:
            session_fatal = next(
                (failure for failure in failures if is_fatal_to_producer_session(failure)),
                None,
            )
            if session_fatal is not None:
                # Every subsequent send would fail identically until the producer is
                # replaced, so replace it now and tell the caller the batch did not land.
                await self._reset_session(session_fatal, generation)
                raise BrokerUnavailableError(
                    f"{len(failures)} of {len(prepared)} records were rejected and the "
                    f"producer session was reset: {type(session_fatal).__name__}"
                ) from session_fatal

            if any(is_retryable(failure) for failure in failures):
                raise BrokerUnavailableError(
                    f"{len(failures)} of {len(prepared)} records were not acknowledged: "
                    f"{type(failures[0]).__name__}: {failures[0]}"
                ) from failures[0]

        return accepted

    async def flush(self) -> None:
        """Wait for every already-published record to be acknowledged."""
        if self._client is None:
            return
        try:
            await self._client.flush()
        except Exception as error:
            if is_retryable(error):
                raise BrokerUnavailableError(
                    f"flush failed: {type(error).__name__}: {error}"
                ) from error
            raise KafkaPublishError(f"flush failed: {type(error).__name__}: {error}") from error

    # --- internals ---------------------------------------------------------

    def _prepare(self, event: MarketEvent) -> tuple[str, bytes, bytes] | None:
        """Route and encode one event, or count why it could not be.

        Returns ``(topic, key, payload)``, or ``None`` when the event is permanently
        unpublishable — already counted and already logged.
        """
        try:
            topic = self._topics.for_event(event)
        except UnroutableEventError as error:
            self._stats.unroutable += 1
            log.error("kafka.event_unroutable", error=str(error), **event_summary(event))
            return None

        try:
            payload = encode(event)
        except EventEncodeError as error:
            self._stats.encode_failures += 1
            log.error("kafka.encode_failed", error=str(error), **event_summary(event))
            return None

        return topic, event.partition_key.encode("utf-8"), payload

    async def _send(self, topic: str, key: bytes, payload: bytes, event: MarketEvent) -> None:
        """One record, with bounded retries for a broker that is merely unwell.

        A permanent failure is not retried: repeating a message the broker considers too
        large only delays the error. Retrying is reserved for the failures where another
        attempt could plausibly land.
        """
        client = self._require_client()
        attempts = self._settings.kafka_producer_max_attempts
        last_error: BaseException | None = None

        for attempt in range(1, attempts + 1):
            generation = self._generation
            try:
                future = await client.send(topic, value=payload, key=key)
                await future
            except Exception as error:
                last_error = error

                # Not retryable with this producer, and entirely recoverable with a new
                # one. Rebuild, and spend the remaining attempts on the new session.
                if (
                    is_fatal_to_producer_session(error)
                    and attempt < attempts
                    and await self._reset_session(error, generation)
                ):
                    client = self._require_client()
                    self._stats.retries += 1
                    continue

                if not is_retryable(error):
                    self._stats.publish_failures += 1
                    log.error(
                        "kafka.publish_failed",
                        topic=topic,
                        attempt=attempt,
                        error=str(error),
                        error_type=type(error).__name__,
                        **event_summary(event),
                    )
                    raise KafkaPublishError(
                        f"publishing to {topic} failed: {type(error).__name__}: {error}"
                    ) from error

                if attempt >= attempts:
                    break

                self._stats.retries += 1
                delay = min(2.0 ** (attempt - 1) * 0.1, 5.0)
                log.warning(
                    "kafka.publish_retry",
                    topic=topic,
                    attempt=attempt,
                    delay_seconds=round(delay, 3),
                    error=str(error),
                    error_type=type(error).__name__,
                )
                await self._sleep(delay)
                continue
            else:
                log.debug("kafka.published", topic=topic, **event_summary(event))
                return

        self._stats.publish_failures += 1
        log.error(
            "kafka.publish_exhausted",
            topic=topic,
            attempts=attempts,
            error=str(last_error),
            error_type=type(last_error).__name__,
            **event_summary(event),
        )
        raise BrokerUnavailableError(
            f"publishing to {topic} failed after {attempts} attempts: "
            f"{type(last_error).__name__}: {last_error}"
        ) from last_error

    async def _close_quietly(self, client: ProducerClient) -> None:
        """Close a producer that may never be able to finish what it is holding.

        ``AIOKafkaProducer.stop()`` flushes before it closes. That is right for a healthy
        producer and a trap for this one: the batches it is holding are exactly the batches
        the broker is refusing, so the flush waits for a delivery that can never happen.

        **It is waited on in a separate task, not awaited directly**, and that detail is the
        whole point. ``stop()`` blocks in an ``asyncio.wait`` over its own internal tasks and
        does not surface a cancellation from outside, so wrapping the ``await`` in
        ``asyncio.timeout`` looks correct, does nothing, and hangs the process — which is
        exactly what it did here: the reset logged that it was closing the old producer and
        then never logged again. Waiting on a task instead means the bound is enforced by
        this code rather than requested of somebody else's.

        An abandoned task is kept referenced so the event loop does not garbage-collect it
        mid-flight and complain about it later.
        """
        task = asyncio.create_task(client.stop())
        done, pending = await asyncio.wait({task}, timeout=self._close_timeout_s)

        if pending:
            self._abandoned.add(task)
            task.add_done_callback(self._abandoned.discard)
            task.cancel()
            log.warning(
                "kafka.producer_close_timed_out",
                seconds=self._close_timeout_s,
                detail="abandoning the old producer; its undelivered records are lost",
            )
            return

        error = next(iter(done)).exception()
        if error is not None:
            log.warning(
                "kafka.producer_close_failed",
                error=str(error),
                error_type=type(error).__name__,
            )

    async def _reset_session(self, error: BaseException, seen_generation: int) -> bool:
        """Replace the producer after the broker lost its idempotent sequence state.

        Returns whether a fresh session is now available.

        This is the recovery for a failure that has no other one. An idempotent producer
        whose sequence state the broker has forgotten — because the broker restarted, or the
        producer id expired — fails every subsequent send with the same error. The producer
        object is dead; the configuration is fine. Only a new producer helps.

        The new session has a new producer id, so a record the old session may already have
        delivered can be delivered again. That is at-least-once working as documented, and
        the consumer's idempotent writes absorb it (ADR-0012).
        """
        if not self._owns_client:
            return False

        async with self._reset_lock:
            if self._generation != seen_generation and self._started:
                # Another caller rebuilt the session while this one was waiting. Use it.
                return True

            log.warning(
                "kafka.producer_session_reset",
                reason=type(error).__name__,
                error=str(error),
                detail="the broker lost this producer's sequence state; starting a new session",
                total_resets=self._stats.session_resets + 1,
            )

            # Build and connect the replacement **before** letting go of the old one. The
            # obvious order — drop the old client, then start a new one — leaves
            # `self._client` empty across two awaits, and a concurrent publisher that
            # arrives in that window is told the producer was never started. Ingestion has
            # two streams publishing through one producer, so that window is not theoretical.
            old_client = self._client
            try:
                new_client = (self._client_factory or self._build_client)()
                await new_client.start()
            except Exception as connect_error:
                log.error(
                    "kafka.producer_session_reset_failed",
                    error=str(connect_error),
                    error_type=type(connect_error).__name__,
                    detail="the broker is unreachable; keeping the old producer for now",
                )
                return False

            self._client = new_client
            self._started = True
            self._generation += 1
            self._stats.session_resets += 1

            # Only now is the old producer redundant. Closing it can block forever, so it
            # is bounded and, if need be, abandoned.
            if old_client is not None:
                await self._close_quietly(old_client)

            log.info(
                "kafka.producer_session_replaced",
                generation=self._generation,
                total_resets=self._stats.session_resets,
            )
            return True

    def _require_client(self) -> ProducerClient:
        if self._client is None or not self._started:
            raise KafkaPublishError("producer is not started; call start() first")
        return self._client
