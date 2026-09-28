"""Topic creation.

Broker-side auto-creation is off (see the Compose file), so these topics exist because
something created them deliberately — with the partition count the ordering guarantee needs
and the retention `ARCHITECTURE.md` §7.2 specifies. A topic created implicitly would get one
partition and infinite retention, and both would be discovered much later.
"""

from __future__ import annotations

from typing import Any

import pytest
from aiokafka.admin import NewTopic
from aiokafka.errors import KafkaConnectionError, TopicAlreadyExistsError

from tests.kafka_support import kafka_settings
from tradewatch.infrastructure.events.kafka import BrokerUnavailableError, Topics
from tradewatch.infrastructure.events.kafka.admin import RETENTION, ensure_topics

pytestmark = pytest.mark.unit


class FakeAdmin:
    """An ``AdminClient`` that remembers what it was asked to create."""

    def __init__(
        self,
        existing: list[str] | None = None,
        *,
        start_error: Exception | None = None,
        list_error: Exception | None = None,
        create_error: Exception | None = None,
    ) -> None:
        self.existing = existing or []
        self.start_error = start_error
        self.list_error = list_error
        self.create_error = create_error
        self.created: list[NewTopic] = []
        self.started = False
        self.closed = False

    async def start(self) -> None:
        if self.start_error is not None:
            raise self.start_error
        self.started = True

    async def close(self) -> None:
        self.closed = True

    async def list_topics(self) -> list[str]:
        if self.list_error is not None:
            raise self.list_error
        return list(self.existing)

    async def create_topics(self, new_topics: list[NewTopic]) -> Any:
        if self.create_error is not None:
            raise self.create_error
        self.created.extend(new_topics)
        return None

    def created_names(self) -> list[str]:
        return [topic.name for topic in self.created]

    def config_of(self, name: str) -> dict[str, str]:
        for topic in self.created:
            if topic.name == name:
                return dict(topic.topic_configs or {})
        raise AssertionError(f"{name} was not created")


class TestCreation:
    async def test_every_topic_this_phase_needs_is_created(self) -> None:
        admin = FakeAdmin()

        created = await ensure_topics(kafka_settings(), admin=admin)

        assert set(created) == set(Topics().all)
        assert set(admin.created_names()) == {
            "tw.market.trades.v1",
            "tw.market.quotes.v1",
            "tw.market.bars.1m.v1",
            "tw.anomalies.v1",
            "tw.dlq.v1",
        }

    async def test_nothing_beyond_this_phase_is_created(self) -> None:
        """No signals, orders, fills, or alerts topics. Nothing produces them yet."""
        admin = FakeAdmin()

        await ensure_topics(kafka_settings(), admin=admin)

        assert len(admin.created_names()) == 5

    async def test_existing_topics_are_left_alone(self) -> None:
        """Idempotent, so every worker can call it at startup."""
        admin = FakeAdmin(existing=list(Topics().all))

        created = await ensure_topics(kafka_settings(), admin=admin)

        assert created == []
        assert admin.created == []

    async def test_only_the_missing_ones_are_created(self) -> None:
        admin = FakeAdmin(existing=["tw.market.trades.v1", "tw.dlq.v1"])

        created = await ensure_topics(kafka_settings(), admin=admin)

        assert set(created) == {"tw.market.quotes.v1", "tw.market.bars.1m.v1", "tw.anomalies.v1"}

    async def test_a_concurrent_creator_is_not_an_error(self) -> None:
        """Two workers racing to create a topic: one loses, and losing is success."""
        admin = FakeAdmin(create_error=TopicAlreadyExistsError())

        assert await ensure_topics(kafka_settings(), admin=admin) == []

    async def test_the_admin_client_is_always_closed(self) -> None:
        admin = FakeAdmin(list_error=KafkaConnectionError("down"))

        with pytest.raises(BrokerUnavailableError):
            await ensure_topics(kafka_settings(), admin=admin)

        assert admin.closed is True

    async def test_the_prefix_is_honoured(self) -> None:
        admin = FakeAdmin()

        await ensure_topics(kafka_settings(kafka_topic_prefix="staging"), admin=admin)

        assert all(name.startswith("staging.") for name in admin.created_names())


class TestTopicShape:
    async def test_partitions_and_replication_come_from_settings(self) -> None:
        admin = FakeAdmin()

        await ensure_topics(
            kafka_settings(kafka_topic_partitions=12, kafka_topic_replication=3), admin=admin
        )

        for topic in admin.created:
            assert topic.num_partitions == 12
            assert topic.replication_factor == 3

    async def test_more_than_one_partition_by_default(self) -> None:
        """One partition is one consumer forever, and nothing would say so out loud."""
        admin = FakeAdmin()

        await ensure_topics(kafka_settings(), admin=admin)

        assert all(topic.num_partitions > 1 for topic in admin.created)

    async def test_retention_matches_the_architecture(self) -> None:
        """§7.2: trades 7d, quotes 24h, bars 30d, dead letters 30d."""
        admin = FakeAdmin()

        await ensure_topics(kafka_settings(), admin=admin)

        assert admin.config_of("tw.market.trades.v1")["retention.ms"] == RETENTION["market.trades"]
        assert admin.config_of("tw.market.quotes.v1")["retention.ms"] == RETENTION["market.quotes"]
        assert admin.config_of("tw.market.bars.1m.v1")["retention.ms"] == RETENTION["market.bars"]
        assert admin.config_of("tw.dlq.v1")["retention.ms"] == RETENTION["dlq"]

    async def test_quotes_are_kept_for_the_shortest_time(self) -> None:
        """Highest volume, least individually valuable."""
        assert int(RETENTION["market.quotes"]) < int(RETENTION["market.trades"])
        assert int(RETENTION["market.trades"]) < int(RETENTION["market.bars"])

    async def test_every_topic_has_finite_retention(self) -> None:
        """Kafka is a transport here, never the system of record (ADR-0002)."""
        admin = FakeAdmin()

        await ensure_topics(kafka_settings(), admin=admin)

        for topic in admin.created:
            assert int((topic.topic_configs or {})["retention.ms"]) > 0


class TestBrokerUnavailable:
    async def test_a_refused_connection_is_reported_clearly(self) -> None:
        admin = FakeAdmin(start_error=KafkaConnectionError("refused"))

        with pytest.raises(BrokerUnavailableError) as caught:
            await ensure_topics(kafka_settings(), admin=admin)

        assert "kafka.invalid:9092" in str(caught.value)

    async def test_a_failed_creation_is_reported(self) -> None:
        admin = FakeAdmin(create_error=KafkaConnectionError("lost mid-create"))

        with pytest.raises(BrokerUnavailableError):
            await ensure_topics(kafka_settings(), admin=admin)
