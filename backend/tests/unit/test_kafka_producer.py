"""The Kafka publisher, against a fake broker client.

No broker. ``FakeProducerClient`` implements the ``ProducerClient`` protocol the adapter
declares, so every failure a real broker can produce — refusing a connection, timing out,
rejecting an oversized record — is reachable deterministically and in milliseconds.

The point of most of these tests is the same one: **an event is never lost quietly**. Every
path that does not publish increments a counter, logs, and tells the caller. The integration
suite proves the happy path against a real broker; this proves the unhappy ones, which a
real broker will not produce on demand.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from aiokafka.errors import (
    InvalidProducerEpoch,
    InvalidProducerIdMapping,
    KafkaConnectionError,
    KafkaTimeoutError,
    MessageSizeTooLargeError,
    OutOfOrderSequenceNumber,
    RequestTimedOutError,
    UnknownProducerId,
)

from tests.kafka_support import FakeProducerClient, bar, kafka_settings, quote, trade
from tradewatch.domain.enums import BarInterval
from tradewatch.infrastructure.events.envelope import decode
from tradewatch.infrastructure.events.kafka import (
    BrokerUnavailableError,
    KafkaEventPublisher,
    KafkaPublishError,
)

pytestmark = pytest.mark.unit


async def _noop_sleep(_seconds: float) -> None:
    """Retry backoff, without the wait."""
    return None


def publisher(client: FakeProducerClient, **overrides: Any) -> KafkaEventPublisher:
    return KafkaEventPublisher(
        kafka_settings(**overrides),
        client=client,
        sleep=_noop_sleep,
    )


class TestLifecycle:
    async def test_start_connects(self) -> None:
        client = FakeProducerClient()
        subject = publisher(client)

        await subject.start()

        assert client.started is True
        assert subject.started is True

    async def test_start_failure_is_reported_as_broker_unavailable(self) -> None:
        """A wrong bootstrap address fails at startup, not at the first tick."""
        client = FakeProducerClient(start_error=KafkaConnectionError("no route to broker"))
        subject = publisher(client)

        with pytest.raises(BrokerUnavailableError) as caught:
            await subject.start()

        assert "kafka.invalid:9092" in str(caught.value)
        assert subject.started is False

    async def test_starting_twice_is_harmless(self) -> None:
        client = FakeProducerClient()
        subject = publisher(client)

        await subject.start()
        await subject.start()

        assert subject.started is True

    async def test_stop_flushes_before_closing(self) -> None:
        """The tail of a session must be acknowledged, not lost in a close nobody checks."""
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()

        await subject.stop()

        assert client.flushes == 1
        assert client.stopped is True
        assert subject.started is False

    async def test_stop_closes_the_client_even_when_the_flush_fails(self) -> None:
        client = FakeProducerClient(flush_error=KafkaTimeoutError("broker gone"))
        subject = publisher(client)
        await subject.start()

        with pytest.raises(BrokerUnavailableError):
            await subject.stop()

        assert client.stopped is True

    async def test_stop_on_a_producer_that_never_started_is_a_no_op(self) -> None:
        client = FakeProducerClient()

        await publisher(client).stop()

        assert client.stopped is False

    async def test_publishing_before_start_is_refused(self) -> None:
        with pytest.raises(KafkaPublishError, match="not started"):
            await publisher(FakeProducerClient()).publish(trade())


class TestRouting:
    async def test_each_event_type_goes_to_its_own_topic(self) -> None:
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()

        await subject.publish(trade())
        await subject.publish(quote())
        await subject.publish(bar())

        assert client.topics() == [
            "tw.market.trades.v1",
            "tw.market.quotes.v1",
            "tw.market.bars.1m.v1",
        ]

    async def test_the_configured_prefix_is_used(self) -> None:
        client = FakeProducerClient()
        subject = publisher(client, kafka_topic_prefix="staging")
        await subject.start()

        await subject.publish(trade())

        assert client.topics() == ["staging.market.trades.v1"]

    async def test_an_unroutable_event_is_counted_and_refused(self) -> None:
        """Not silently dropped, and not sent to some default topic either."""
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()

        with pytest.raises(KafkaPublishError):
            await subject.publish(bar(interval=BarInterval.ONE_HOUR))

        assert subject.stats.unroutable == 1
        assert client.sent == []


class TestKeyAndPayload:
    async def test_the_record_is_keyed_by_symbol(self) -> None:
        """This is what guarantees per-symbol ordering. It is a correctness property."""
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()

        await subject.publish(trade(symbol="MSFT"))

        assert client.keys() == [b"MSFT"]

    async def test_two_symbols_get_two_keys(self) -> None:
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()

        await subject.publish(trade(symbol="AAPL", trade_id="a"))
        await subject.publish(trade(symbol="MSFT", trade_id="b"))

        assert client.keys() == [b"AAPL", b"MSFT"]

    async def test_events_of_different_types_for_one_symbol_share_a_key(self) -> None:
        """So they land on one partition and stay in order relative to each other."""
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()

        await subject.publish(trade(symbol="NVDA"))
        await subject.publish(bar(symbol="NVDA"))

        assert client.keys() == [b"NVDA", b"NVDA"]

    async def test_the_body_is_the_encoded_envelope(self) -> None:
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()
        original = trade()

        await subject.publish(original)

        body = client.values()[0]
        assert body is not None
        assert decode(body) == original

    async def test_money_reaches_the_wire_as_a_string(self) -> None:
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()

        await subject.publish(trade(price="42.42"))

        body = client.values()[0]
        assert body is not None
        assert json.loads(body)["payload"]["price"] == "42.42"


class TestBatchPublishing:
    async def test_a_batch_is_sent_and_counted(self) -> None:
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()
        events = [trade(trade_id=f"t-{index}") for index in range(5)]

        accepted = await subject.publish_many(events)

        assert accepted == 5
        assert len(client.sent) == 5
        assert subject.stats.published == 5

    async def test_an_empty_batch_does_nothing(self) -> None:
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()

        assert await subject.publish_many([]) == 0
        assert client.sent == []

    async def test_an_unroutable_event_does_not_abandon_the_rest_of_the_batch(self) -> None:
        """One bad bar must not cost a 240-bar backfill."""
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()
        events = [bar(minute=0), bar(interval=BarInterval.ONE_HOUR), bar(minute=2)]

        accepted = await subject.publish_many(events)

        assert accepted == 2
        assert subject.stats.unroutable == 1

    async def test_a_batch_that_the_broker_refuses_raises_rather_than_reporting_zero(
        self,
    ) -> None:
        """Reporting "0 accepted" would let ingestion carry on as if the events were unlucky."""
        client = FakeProducerClient(send_errors=[KafkaConnectionError("down")] * 3)
        subject = publisher(client)
        await subject.start()

        with pytest.raises(BrokerUnavailableError):
            await subject.publish_many([trade(trade_id=f"t-{index}") for index in range(3)])

        assert subject.stats.publish_failures == 3


class TestBrokerFailures:
    async def test_a_retryable_failure_is_retried_and_then_succeeds(self) -> None:
        client = FakeProducerClient(send_errors=[KafkaTimeoutError("slow"), None])
        subject = publisher(client)
        await subject.start()

        await subject.publish(trade())

        assert subject.stats.retries == 1
        assert subject.stats.published == 1
        assert len(client.sent) == 1

    async def test_retries_are_bounded(self) -> None:
        """An unbounded retry is a producer that never reports the outage."""
        client = FakeProducerClient(send_errors=[RequestTimedOutError()] * 10)
        subject = publisher(client, kafka_producer_max_attempts=3)
        await subject.start()

        with pytest.raises(BrokerUnavailableError, match="after 3 attempts"):
            await subject.publish(trade())

        assert subject.stats.retries == 2
        assert subject.stats.publish_failures == 1

    async def test_the_attempt_budget_is_configurable(self) -> None:
        client = FakeProducerClient(send_errors=[KafkaConnectionError("down")] * 10)
        subject = publisher(client, kafka_producer_max_attempts=5)
        await subject.start()

        with pytest.raises(BrokerUnavailableError, match="after 5 attempts"):
            await subject.publish(trade())

        assert subject.stats.retries == 4

    async def test_a_permanent_failure_is_not_retried(self) -> None:
        """Repeating a message the broker considers too large only delays the error."""
        client = FakeProducerClient(send_errors=[MessageSizeTooLargeError(), None])
        subject = publisher(client)
        await subject.start()

        with pytest.raises(KafkaPublishError):
            await subject.publish(trade())

        assert subject.stats.retries == 0
        assert subject.stats.publish_failures == 1

    async def test_a_failed_publish_is_not_counted_as_published(self) -> None:
        client = FakeProducerClient(send_errors=[MessageSizeTooLargeError()])
        subject = publisher(client)
        await subject.start()

        with pytest.raises(KafkaPublishError):
            await subject.publish(trade())

        assert subject.stats.published == 0


class TestFlush:
    async def test_flush_waits_on_the_client(self) -> None:
        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()

        await subject.flush()

        assert client.flushes == 1

    async def test_a_retryable_flush_failure_says_the_broker_is_unavailable(self) -> None:
        client = FakeProducerClient(flush_error=KafkaConnectionError("gone"))
        subject = publisher(client)
        await subject.start()

        with pytest.raises(BrokerUnavailableError):
            await subject.flush()

    async def test_flushing_a_producer_with_no_client_is_a_no_op(self) -> None:
        subject = KafkaEventPublisher(kafka_settings())

        await subject.flush()


class TestIngestionIntegration:
    async def test_the_ingestion_service_needs_no_change_to_use_kafka(self) -> None:
        """The Phase 1 claim about the port, tested.

        `IngestionService` was written against `EventPublisher` and a failure type called
        `EventBusError`. This publisher is a different class, in a different module, talking
        to a broker — and the service does not know.
        """
        from tradewatch.application.ingestion import IngestionService
        from tradewatch.application.ports.event_bus import EventPublisher

        client = FakeProducerClient()
        subject = publisher(client)
        await subject.start()

        assert isinstance(subject, EventPublisher)

        class _Provider:
            name = "test"

        service = IngestionService(_Provider(), subject)  # type: ignore[arg-type]
        assert await service._publish_batch([trade(), quote()]) == 2
        assert service.stats.published == 2

    async def test_a_broker_outage_is_counted_by_ingestion_not_raised(self) -> None:
        """Failure F4: one unroutable event must not end a session."""
        from tradewatch.application.ingestion import IngestionService

        client = FakeProducerClient(send_errors=[KafkaConnectionError("down")] * 5)
        subject = publisher(client)
        await subject.start()

        class _Provider:
            name = "test"

        service = IngestionService(_Provider(), subject)  # type: ignore[arg-type]
        published = await service._publish_one(trade())

        assert published is False
        assert service.stats.publish_failures == 1


class TestProducerSessionRecovery:
    """The failure that a broker restart produces, and that nothing else recovers from.

    Kafka's idempotent producer numbers its records per partition and the broker tracks that
    state. When the broker loses it — it restarted, or the producer id expired — every send
    from that producer object fails with `OutOfOrderSequenceNumber`, forever. `aiokafka`
    marks it non-retryable, which is true of *that producer* and misleading about the system:
    a new producer works immediately.

    Found by restarting the broker under a running producer, which is exactly the scenario
    `DEVELOPMENT_PLAN.md` makes a Phase 2 exit criterion. Before this, the producer stayed
    connected, reported no outage, and silently stopped delivering.
    """

    def rebuildable(
        self, clients: list[FakeProducerClient], **overrides: Any
    ) -> KafkaEventPublisher:
        """A publisher that builds a new client from `clients` each time it starts."""
        remaining = list(clients)

        return KafkaEventPublisher(
            kafka_settings(**overrides),
            client_factory=lambda: remaining.pop(0),
            sleep=_noop_sleep,
        )

    async def test_the_session_is_rebuilt_and_the_send_succeeds(self) -> None:
        broken = FakeProducerClient(send_errors=[OutOfOrderSequenceNumber()])
        fresh = FakeProducerClient()
        subject = self.rebuildable([broken, fresh])
        await subject.start()

        await subject.publish(trade())

        assert subject.stats.session_resets == 1
        assert subject.stats.published == 1
        assert fresh.sent, "the record went out on the new session"
        assert broken.stopped is True, "the dead producer was closed, not leaked"

    async def test_every_session_fatal_error_triggers_a_rebuild(self) -> None:
        for error in (
            OutOfOrderSequenceNumber(),
            UnknownProducerId(),
            InvalidProducerEpoch(),
            InvalidProducerIdMapping(),
        ):
            subject = self.rebuildable(
                [FakeProducerClient(send_errors=[error]), FakeProducerClient()]
            )
            await subject.start()

            await subject.publish(trade())

            assert subject.stats.session_resets == 1, type(error).__name__

    async def test_a_reset_is_not_an_endless_loop(self) -> None:
        """The attempt budget still applies, so a permanently broken broker is reported."""
        clients = [FakeProducerClient(send_errors=[OutOfOrderSequenceNumber()]) for _ in range(6)]
        subject = self.rebuildable(clients, kafka_producer_max_attempts=3)
        await subject.start()

        with pytest.raises(KafkaPublishError):
            await subject.publish(trade())

        assert subject.stats.session_resets == 2, "one per remaining attempt, then it stops"

    async def test_a_batch_resets_the_session_so_the_next_one_works(self) -> None:
        broken = FakeProducerClient(send_errors=[OutOfOrderSequenceNumber()] * 3)
        fresh = FakeProducerClient()
        subject = self.rebuildable([broken, fresh])
        await subject.start()

        with pytest.raises(BrokerUnavailableError, match="session was reset"):
            await subject.publish_many([trade(trade_id=f"t-{index}") for index in range(3)])

        assert subject.stats.session_resets == 1

        # The next batch goes out on the rebuilt session.
        assert await subject.publish_many([trade(trade_id="after")]) == 1
        assert len(fresh.sent) == 1

    async def test_an_injected_client_is_never_replaced(self) -> None:
        """A caller that handed us a client owns its lifetime — usually a test double."""
        client = FakeProducerClient(send_errors=[OutOfOrderSequenceNumber()])
        subject = publisher(client)
        await subject.start()

        with pytest.raises(KafkaPublishError):
            await subject.publish(trade())

        assert subject.stats.session_resets == 0
        assert client.stopped is False

    async def test_a_reset_that_cannot_reconnect_reports_the_original_failure(self) -> None:
        """The broker is gone entirely. Say so rather than looping on a rebuild."""
        broken = FakeProducerClient(send_errors=[OutOfOrderSequenceNumber()])
        unreachable = FakeProducerClient(start_error=KafkaConnectionError("still down"))
        subject = self.rebuildable([broken, unreachable])
        await subject.start()

        with pytest.raises(KafkaPublishError):
            await subject.publish(trade())

        assert subject.stats.session_resets == 0, "no session was established"

    async def test_an_ordinary_retryable_failure_does_not_rebuild(self) -> None:
        """Rebuilding on a timeout would throw away a perfectly good producer."""
        client = FakeProducerClient(send_errors=[KafkaTimeoutError("slow"), None])
        subject = publisher(client)
        await subject.start()

        await subject.publish(trade())

        assert subject.stats.session_resets == 0
        assert subject.stats.retries == 1

    async def test_a_producer_that_will_not_close_is_abandoned_not_waited_on(self) -> None:
        """The hang this introduced before it was bounded.

        `AIOKafkaProducer.stop()` flushes before closing, and the batches a poisoned
        producer is holding are exactly the ones the broker refuses — so the flush waits
        for a delivery that cannot happen. The process stops dead, with a live producer
        object and no further log line, which is a genuinely unpleasant thing to diagnose.
        """

        class NeverCloses(FakeProducerClient):
            async def stop(self) -> None:
                await asyncio.Event().wait()  # never set

        broken = NeverCloses(send_errors=[OutOfOrderSequenceNumber()])
        fresh = FakeProducerClient()
        remaining = [broken, fresh]
        subject = KafkaEventPublisher(
            kafka_settings(),
            client_factory=lambda: remaining.pop(0),
            sleep=_noop_sleep,
            # Short, so the test asserts the bound exists rather than sitting through it.
            close_timeout_s=0.05,
        )
        await subject.start()

        async with asyncio.timeout(5):
            await subject.publish(trade())

        assert subject.stats.session_resets == 1
        assert fresh.sent, "the record went out on the new session"

    async def test_two_concurrent_publishers_rebuild_the_session_once(self) -> None:
        """Ingestion runs a trade stream and a quote stream against one publisher.

        Without the generation check both see the same poisoned session, both rebuild, and
        the second one throws away the working producer the first just made.
        """
        broken = FakeProducerClient(send_errors=[OutOfOrderSequenceNumber()] * 2)
        fresh = FakeProducerClient()
        spare = FakeProducerClient()
        remaining = [broken, fresh, spare]
        subject = KafkaEventPublisher(
            kafka_settings(),
            client_factory=lambda: remaining.pop(0),
            sleep=_noop_sleep,
        )
        await subject.start()

        await asyncio.gather(
            subject.publish(trade(trade_id="a")),
            subject.publish(trade(trade_id="b")),
        )

        assert subject.stats.session_resets == 1, "one rebuild, not two"
        assert len(fresh.sent) == 2, "both records went out on the one new session"
        assert spare.sent == [], "the spare client was never needed"

    async def test_a_publish_arriving_mid_reset_is_not_told_the_producer_is_stopped(
        self,
    ) -> None:
        """The window that dropping the client first opened.

        Rebuilding as "let go of the old client, then start a new one" leaves the publisher
        with no client across two awaits. A second stream publishing in that window got
        `producer is not started`, which is both wrong and alarming. The replacement is
        built before the old one is released, so there is no such window.
        """

        class SlowToStart(FakeProducerClient):
            async def start(self) -> None:
                await asyncio.sleep(0.05)
                await super().start()

        broken = FakeProducerClient(send_errors=[OutOfOrderSequenceNumber()])
        fresh = SlowToStart()
        remaining: list[FakeProducerClient] = [broken, fresh]
        subject = KafkaEventPublisher(
            kafka_settings(),
            client_factory=lambda: remaining.pop(0),
            sleep=_noop_sleep,
            close_timeout_s=0.05,
        )
        await subject.start()

        async def publish_during_the_reset() -> None:
            await asyncio.sleep(0.01)  # land inside the rebuild
            await subject.publish(trade(trade_id="concurrent"))

        await asyncio.gather(subject.publish(trade(trade_id="first")), publish_during_the_reset())

        assert subject.stats.published == 2
        assert subject.stats.session_resets == 1
