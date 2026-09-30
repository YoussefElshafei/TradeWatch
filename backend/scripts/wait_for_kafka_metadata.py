"""Warm up a freshly-started broker's topic metadata before the integration suite runs.

Observed directly against a brand-new single-node KRaft broker (`apache/kafka:4.0.0`, the
same image CI and `docker/docker-compose.yml` both use): `create_topics()` returning success
does not mean the broker's `list_topics()` reflects it yet. A long-lived broker exercised for
hours needs none of this — the same probe against one showed 0s of lag, every time — but a
broker that has just started, exactly what CI's `services:` block always is, measured 1s of
lag as *not enough* and 2s as enough. `tests/integration/test_kafka_pipeline.py`'s own
idempotency check (`ensure_topics()` called twice back to back) has no delay between the two
calls, and would race this every single CI run without something settling the metadata path
first.

This is CI plumbing, not a fix to `ensure_topics()` itself: production code calls it once per
worker at startup, never twice within milliseconds, so the race is a test artifact of a
freshly-started broker — worth absorbing here rather than teaching the function to retry
around a codepath production never actually exercises this way.

Usage: ``uv run python scripts/wait_for_kafka_metadata.py``
"""

from __future__ import annotations

import asyncio
import sys
import time
import uuid

from aiokafka.admin import AIOKafkaAdminClient

from tradewatch.config.settings import KafkaSettings
from tradewatch.infrastructure.events.kafka.admin import ensure_topics
from tradewatch.infrastructure.events.kafka.topics import Topics
from tradewatch.observability.logging import get_logger

log = get_logger(__name__)

#: Bounded, not a fixed sleep — polling until the broker actually agrees with itself,
#: the same "wait for a real condition, not a guessed delay" rule
#: `test_kafka_pipeline.py::TestBrokerOutage._wait_for_broker` follows.
_TIMEOUT_S = 30.0
_POLL_INTERVAL_S = 0.5


async def _wait_until_visible(bootstrap: str, wanted: frozenset[str]) -> None:
    deadline = time.monotonic() + _TIMEOUT_S
    while True:
        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap, client_id="warmup-probe")
        await admin.start()
        try:
            existing = set(await admin.list_topics())
        finally:
            await admin.close()
        if wanted <= existing:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(f"Kafka metadata never caught up: still missing {wanted - existing}")
        await asyncio.sleep(_POLL_INTERVAL_S)


async def main() -> None:
    # No `get_settings()` here on purpose: this runs as its own CI step, before the three
    # settings groups with no default (`TW_DATABASE_URL`, `TW_REDIS_URL`, `TW_JWT_SECRET`)
    # are ever going to be set, and this script needs none of them — only Kafka's own
    # default (`localhost:9092`), which already matches the service block's published port.
    prefix = f"warmup{uuid.uuid4().hex[:8]}"
    config = KafkaSettings(kafka_topic_prefix=prefix)
    topics = Topics(prefix)

    started = time.monotonic()
    await ensure_topics(config, topics=topics)
    await _wait_until_visible(config.kafka_bootstrap, frozenset(topics.all))
    elapsed = time.monotonic() - started

    admin = AIOKafkaAdminClient(bootstrap_servers=config.kafka_bootstrap)
    await admin.start()
    try:
        await admin.delete_topics(list(topics.all))
    finally:
        await admin.close()

    log.info("kafka_warmup.ready", elapsed_s=round(elapsed, 2))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except TimeoutError as error:
        print(f"kafka_warmup.failed: {error}", file=sys.stderr)
        sys.exit(1)
