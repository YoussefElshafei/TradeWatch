"""Ingest throughput: events/sec sustained from Kafka to a committed PostgreSQL write.

**Method.** N trade events are published to a fresh set of topics (a unique prefix, so no
other process reads or writes them), then the real persistence consumer — `run_consumer`,
the code `tradewatch-consumer` runs — is started against an empty, freshly migrated
database. A second connection counts committed rows every 250 ms. Only committed rows are
visible to it, so the count is exactly "events durably written", which is the claim.

The figure reported as *sustained* is measured from the first committed row to the last,
so consumer-group join time (a fixed few seconds, unrelated to throughput) is excluded;
the time including it is reported alongside. The publish rate is reported too, but it is
not the ingest figure: the producer is not the stage this measures.

**Why a backlog, not a live feed.** Draining a backlog runs the consumer flat out, which
is what "how many events/sec can the write path sustain" asks. A live feed at a fixed rate
would measure the rate chosen, not the system.

    cd backend && uv run python ../benchmarks/ingest_throughput.py [--events 100000]

Needs the Compose stack's PostgreSQL and Kafka. Leaves nothing behind: the bench database
is dropped and the bench topics deleted.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import itertools
import os
import random
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import asyncpg
from _common import write_result
from _isolated import _plain_dsn, bench_database
from aiokafka.admin import AIOKafkaAdminClient

from tradewatch.config.env import load_env_file
from tradewatch.config.settings import get_settings
from tradewatch.infrastructure.events.kafka import KafkaEventPublisher, Topics, ensure_topics
from tradewatch.infrastructure.providers.normalization import EventFactory
from tradewatch.observability.logging import configure_logging
from tradewatch.workers.market_data_consumer import run_consumer


def _events(count: int, symbols: int) -> list:  # type: ignore[type-arg]
    factory = EventFactory(source="bench", producer="ingest-benchmark@1")
    start = dt.datetime.now(dt.UTC) - dt.timedelta(hours=1)
    prices = dict.fromkeys((f"BNCH{i}" for i in range(symbols)), Decimal("100.00"))
    rng = random.Random(7)  # noqa: S311 - a benchmark workload, not security
    names = list(prices)
    events = []
    for index in range(count):
        symbol = names[index % symbols]
        prices[symbol] = max(prices[symbol] + Decimal(rng.randint(-5, 5)) / 100, Decimal("1"))
        events.append(
            factory.trade(
                symbol=symbol,
                price=prices[symbol],
                size=Decimal(rng.randint(1, 500)),
                occurred_at=start + dt.timedelta(milliseconds=index),
                provider_trade_id=f"bench-{index}",
            )
        )
    return events


async def _publish(settings, events: list) -> float:  # type: ignore[no-untyped-def,type-arg]
    publisher = KafkaEventPublisher(
        settings.kafka, topics=Topics(settings.kafka.kafka_topic_prefix)
    )
    await publisher.start()
    started = time.perf_counter()
    for offset in range(0, len(events), 1000):
        accepted = await publisher.publish_many(events[offset : offset + 1000])
        assert accepted == len(events[offset : offset + 1000]), "the broker refused events"
    elapsed = time.perf_counter() - started
    await publisher.stop()
    return elapsed


async def _drain(settings, bench_url: str, total: int) -> dict:  # type: ignore[no-untyped-def,type-arg]
    counter = await asyncpg.connect(_plain_dsn(bench_url))
    stop = asyncio.Event()
    started = time.perf_counter()
    task = asyncio.create_task(run_consumer(settings, stop=stop))
    samples: list[tuple[float, int]] = []
    try:
        while True:
            committed = await counter.fetchval("SELECT count(*) FROM trades")
            samples.append((time.perf_counter() - started, committed))
            if committed >= total:
                break
            if task.done():
                task.result()  # surfaces the consumer's own failure
                raise RuntimeError("consumer stopped before the backlog was drained")
            await asyncio.sleep(0.25)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=60)
        await counter.close()

    first_row_at = next(t for t, n in samples if n > 0)
    first_count = next(n for _t, n in samples if n > 0)
    done_at = samples[-1][0]
    per_second = [
        (n2 - n1) / (t2 - t1)
        for (t1, n1), (t2, n2) in itertools.pairwise(samples)
        if n1 > 0 and t2 > t1
    ]
    return {
        "events_committed": samples[-1][1],
        "seconds_including_group_join": round(done_at, 3),
        "seconds_first_to_last_row": round(done_at - first_row_at, 3),
        "sustained_events_per_second": round((total - first_count) / (done_at - first_row_at), 1),
        "events_per_second_including_join": round(total / done_at, 1),
        "interval_rates_events_per_second": {
            "min": round(min(per_second), 1),
            "max": round(max(per_second), 1),
        },
        "samples_every_seconds": 0.25,
    }


async def _delete_topics(settings, topics: Topics) -> None:  # type: ignore[no-untyped-def]
    admin = AIOKafkaAdminClient(bootstrap_servers=settings.kafka.kafka_bootstrap)
    await admin.start()
    try:
        existing = set(await admin.list_topics())
        await admin.delete_topics(
            [t for t in (*topics.market, topics.dead_letter) if t in existing]
        )
    finally:
        await admin.close()


async def main(events: int, symbols: int) -> None:
    load_env_file()
    configure_logging(level="WARNING")
    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d%H%M%S")

    async with bench_database(os.environ["TW_DATABASE_URL"]) as bench_url:
        os.environ |= {
            "TW_DATABASE_URL": bench_url,
            "TW_KAFKA_TOPIC_PREFIX": f"bench{stamp}",
            "TW_KAFKA_CONSUMER_GROUP": f"bench-persist-{stamp}",
            "TW_KAFKA_AUTO_OFFSET_RESET": "earliest",
            "TW_METRICS_ENABLED": "false",
        }
        get_settings.cache_clear()
        settings = get_settings()
        topics = Topics(settings.kafka.kafka_topic_prefix)
        await ensure_topics(settings.kafka, topics=topics)
        try:
            workload = _events(events, symbols)
            publish_seconds = await _publish(settings, workload)
            drained = await _drain(settings, bench_url, events)
        finally:
            await _delete_topics(settings, topics)

    write_result(
        "ingest_throughput",
        description="Trade events/sec from a Kafka backlog to committed PostgreSQL rows.",
        command=(
            "cd backend && uv run python ../benchmarks/ingest_throughput.py "
            f"--events {events} --symbols {symbols}"
        ),
        workload={
            "events": events,
            "event_type": "market.trade",
            "symbols": symbols,
            "mode": "backlog drain: all events published before the consumer starts",
        },
        configuration={
            "consumer_batch_size": settings.kafka.consumer_batch_size,
            "topic_partitions": settings.kafka.kafka_topic_partitions,
            "consumer_instances": 1,
            "database_pool_size": settings.database.database_pool_size,
            "log_level": "WARNING (INFO in normal running; the consumer logs per batch at DEBUG)",
            "kafka": "single broker, KRaft, Docker",
        },
        results={
            **drained,
            "publish_seconds": round(publish_seconds, 3),
            "publish_events_per_second": round(events / publish_seconds, 1),
        },
        caveats=[
            "One consumer instance on one machine; it says nothing about how the rate scales "
            "with more consumers or partitions.",
            "Trades only. Quotes and bars take different write paths and were not measured.",
            "A backlog drain measures capacity, not end-to-end latency under a live feed — "
            "see the e2e_latency benchmark for that.",
            "PostgreSQL, Kafka and the consumer shared one laptop's CPU and disk; a separate "
            "database host would change the figure in either direction.",
        ],
    )
    print(f"sustained: {drained['sustained_events_per_second']} events/sec")  # noqa: T201


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--events", type=int, default=100_000)
    parser.add_argument("--symbols", type=int, default=8)
    options = parser.parse_args()
    asyncio.run(main(options.events, options.symbols))
