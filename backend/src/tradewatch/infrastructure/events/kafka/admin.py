"""Creating the topics, with the retention `ARCHITECTURE.md` §7.2 specifies.

Broker-side topic auto-creation is switched off in the Compose file, deliberately. A
topic created implicitly by whoever connected first gets the broker's default partition
count — one — and one partition means one consumer and no parallelism, discovered much later
under load. Worse, it would be created with infinite retention, and Kafka is a transport with
finite retention here, never the system of record (ADR-0002).

So the topics are created explicitly, and :func:`ensure_topics` is idempotent: it creates
what is missing and leaves what exists alone. It does **not** alter an existing topic. A
topic whose partition count or retention has drifted from this file is an operational
decision someone made, and silently reverting it during a worker restart would be a worse
surprise than the drift.

Retention is per topic and comes straight from §7.2: trades for 7 days, quotes for 24 hours,
bars for 30 days, dead letters for 30 days. Quotes are the shortest because they are the
highest volume and the least individually valuable; dead letters are long because the whole
point is that somebody gets to look at them on Monday.
"""

from __future__ import annotations

import datetime as dt
from typing import Final, Protocol

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError

from tradewatch.config.settings import KafkaSettings
from tradewatch.infrastructure.events.kafka.errors import BrokerUnavailableError
from tradewatch.infrastructure.events.kafka.topics import Topics
from tradewatch.observability.logging import get_logger

__all__ = ["RETENTION", "AdminClient", "ensure_topics"]

log = get_logger(__name__)


def _ms(**kwargs: float) -> str:
    return str(int(dt.timedelta(**kwargs).total_seconds() * 1000))


#: Retention per topic suffix, as `ARCHITECTURE.md` §7.2 specifies it.
RETENTION: Final[dict[str, str]] = {
    "market.trades": _ms(days=7),
    "market.quotes": _ms(hours=24),
    "market.bars": _ms(days=30),
    "anomalies": _ms(days=30),
    "fills": _ms(days=30),
    "alerts": _ms(days=7),
    "dlq": _ms(days=30),
}


class AdminClient(Protocol):
    """The slice of ``AIOKafkaAdminClient`` used here, so tests need no broker."""

    async def start(self) -> None: ...

    async def close(self) -> None: ...

    async def list_topics(self) -> list[str]: ...

    async def create_topics(self, new_topics: list[NewTopic]) -> object: ...


def _retention_for(topic: str) -> str:
    """Match a topic name to its retention, longest prefix first.

    `market.bars.1m.v1` must match `market.bars` and not fall through to a default, which
    is why this is a prefix match rather than a dictionary lookup.
    """
    for key in sorted(RETENTION, key=len, reverse=True):
        if f".{key}." in f".{topic}." or f".{key}" in topic:
            return RETENTION[key]
    return _ms(days=7)


async def ensure_topics(
    settings: KafkaSettings,
    *,
    topics: Topics | None = None,
    admin: AdminClient | None = None,
) -> list[str]:
    """Create any of this phase's topics that do not exist. Returns the ones created.

    Safe to call from every worker at startup, and safe to call concurrently: two workers
    racing to create the same topic means one of them gets ``TopicAlreadyExistsError``,
    which is success spelled differently.

    Raises:
        BrokerUnavailableError: If the broker cannot be reached.
    """
    wanted = topics or Topics(settings.kafka_topic_prefix)
    client = admin or AIOKafkaAdminClient(
        bootstrap_servers=settings.kafka_bootstrap,
        client_id="tradewatch-admin",
    )

    try:
        await client.start()
    except Exception as error:
        raise BrokerUnavailableError(
            f"cannot reach Kafka at {settings.kafka_bootstrap}: {type(error).__name__}: {error}"
        ) from error

    try:
        try:
            existing = set(await client.list_topics())
        except Exception as error:
            raise BrokerUnavailableError(
                f"cannot list topics: {type(error).__name__}: {error}"
            ) from error

        missing = [name for name in wanted.all if name not in existing]
        if not missing:
            log.debug("kafka.topics_present", topics=list(wanted.all))
            return []

        new_topics = [
            NewTopic(
                name=name,
                num_partitions=settings.kafka_topic_partitions,
                replication_factor=settings.kafka_topic_replication,
                topic_configs={"retention.ms": _retention_for(name)},
            )
            for name in missing
        ]

        try:
            await client.create_topics(new_topics)
        except TopicAlreadyExistsError:
            # Another worker won the race. That is the outcome we wanted.
            log.debug("kafka.topics_created_concurrently", topics=missing)
            return []
        except Exception as error:
            raise BrokerUnavailableError(
                f"cannot create topics {missing}: {type(error).__name__}: {error}"
            ) from error

        log.info(
            "kafka.topics_created",
            topics=missing,
            partitions=settings.kafka_topic_partitions,
            replication=settings.kafka_topic_replication,
        )
        return missing
    finally:
        await client.close()
