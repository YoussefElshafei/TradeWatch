"""Recovery time after a 60 s broker outage: lag drain per consumer group, and data lost.

**Method.** With the whole stack running under `scripts/dev.ps1` / `dev.sh` (which restarts
any worker that exits — the workers deliberately exit when the broker evicts them):

1. Sample each consumer group's lag (log-end offset minus committed offset, summed over its
   partitions — the same figure `kafka-exporter` reports) about once a second for a
   baseline window, over connections kept open between samples.
2. `docker stop` the Kafka container, wait `--outage` seconds, `docker start` it.
3. Once the broker answers again, keep sampling. A group has *recovered* at the first sample
   at or below its baseline maximum (plus a small slack), provided it then stays there for
   five consecutive samples. Report, per group: peak lag and seconds to recovered, from the
   broker answering and from the outage starting.
4. Read the trades topic's end offsets before, at, and after the outage: the trade rate the
   broker acknowledged on either side. Lag only measures what reached the broker; the
   producer does not buffer while it is down, so "how much never arrived" is half of the
   answer.

Lag is read straight from the broker rather than from Prometheus, so the result does not
depend on scrape timing — and a consumer that never comes back shows as never recovering,
not as a missing series.

    cd backend && uv run python ../benchmarks/recovery_time.py [--outage 60]
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime as dt
import itertools
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import write_result
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient

from tradewatch.config.env import load_env_file

CONTAINER = "tradewatch-kafka"
SLACK = 50  # messages: a baseline that was briefly zero should not make "recovered" unreachable
STABLE_SAMPLES = 5


class _LagReader:
    """Total lag per group, over connections kept open between samples.

    The first version opened an admin client and a consumer per sample; that took several
    seconds, spaced samples ~6 s apart, and made the recovery figure mostly sampling delay.
    The clients are now reused, and rebuilt only after a failure (the broker going away).
    """

    def __init__(self, bootstrap: str, groups: list[str]) -> None:
        self._bootstrap = bootstrap
        self._groups = groups
        self._admin: AIOKafkaAdminClient | None = None
        self._reader: AIOKafkaConsumer | None = None

    async def sample(self) -> dict[str, int] | None:
        """None when the broker cannot answer right now."""
        try:
            if self._admin is None or self._reader is None:
                await self._connect()
            assert self._admin is not None
            assert self._reader is not None
            lags: dict[str, int] = {}
            for group in self._groups:
                committed = await asyncio.wait_for(
                    self._admin.list_consumer_group_offsets(group), timeout=3
                )
                if not committed:
                    lags[group] = 0
                    continue
                ends = await asyncio.wait_for(self._reader.end_offsets(list(committed)), timeout=3)
                lags[group] = sum(
                    max(ends[tp] - meta.offset, 0)
                    for tp, meta in committed.items()
                    if meta.offset >= 0
                )
            return lags
        except Exception:  # broker down, or still electing a coordinator
            await self.close()
            return None

    async def _connect(self) -> None:
        self._admin = AIOKafkaAdminClient(
            bootstrap_servers=self._bootstrap, request_timeout_ms=3000
        )
        self._reader = AIOKafkaConsumer(bootstrap_servers=self._bootstrap, request_timeout_ms=3000)
        await asyncio.wait_for(self._admin.start(), timeout=5)
        await asyncio.wait_for(self._reader.start(), timeout=5)

    async def close(self) -> None:
        if self._reader is not None:
            with contextlib.suppress(Exception):
                await self._reader.stop()
        if self._admin is not None:
            with contextlib.suppress(Exception):
                await self._admin.close()
        self._admin = None
        self._reader = None


class _TradeOffsets:
    """Trades the broker has acknowledged: the summed end offset of the trades topic.

    Counted at the broker, not in PostgreSQL, because the database count is not usable on a
    development stack: the synthetic provider restarts its trade ids at every producer start,
    so after the first run its trades collide with stored ones and are dropped as duplicates
    (a finding of this phase — see PERFORMANCE.md). End offsets only ever move forward with
    acknowledged publishes, which is exactly "what reached the broker".
    """

    def __init__(self, bootstrap: str, topic: str) -> None:
        self._bootstrap = bootstrap
        self._topic = topic

    async def total(self) -> int:
        # An unsubscribed consumer has no metadata for the topic, so `partitions_for_topic`
        # answers None (the first version of this summed nothing and reported 0 trades/s).
        admin = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap)
        await admin.start()
        try:
            described = await admin.describe_topics([self._topic])
        finally:
            await admin.close()
        partitions = [
            TopicPartition(self._topic, p["partition"]) for p in described[0]["partitions"]
        ]
        if not partitions:
            raise RuntimeError(f"{self._topic} has no partitions — wrong topic prefix?")
        reader = AIOKafkaConsumer(bootstrap_servers=self._bootstrap)
        await reader.start()
        try:
            ends = await reader.end_offsets(partitions)
        finally:
            await reader.stop()
        return sum(ends.values())


async def _groups(bootstrap: str) -> list[str]:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        listed = await admin.list_consumer_groups()
    finally:
        await admin.close()
    # Leftovers from benchmark and integration-test runs are not part of the system.
    return sorted(g[0] for g in listed if not g[0].startswith(("bench", "test-")))


def _docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True)  # noqa: S603, S607


async def main(outage: int, baseline_seconds: int, give_up: int) -> None:
    load_env_file()
    bootstrap = os.environ.get("TW_KAFKA_BOOTSTRAP", "localhost:9092")
    groups = await _groups(bootstrap)
    if not groups:
        raise SystemExit("no consumer groups found — is the stack running?")

    trades = _TradeOffsets(
        bootstrap, f"{os.environ.get('TW_KAFKA_TOPIC_PREFIX', 'tw')}.market.trades.v1"
    )
    lag = _LagReader(bootstrap, groups)
    baseline: dict[str, int] = dict.fromkeys(groups, 0)
    offsets_baseline_start = await trades.total()
    baseline_started = time.monotonic()
    for _ in range(baseline_seconds):
        sample = await lag.sample()
        if sample is None:
            raise SystemExit("broker not answering before the outage — fix that first")
        baseline = {g: max(baseline[g], sample[g]) for g in groups}
        await asyncio.sleep(1)
    await lag.close()
    offsets_at_outage = await trades.total()
    baseline_span = time.monotonic() - baseline_started

    outage_started = time.monotonic()
    outage_started_utc = dt.datetime.now(dt.UTC)
    _docker("stop", "-t", "10", CONTAINER)
    await asyncio.sleep(max(outage - (time.monotonic() - outage_started), 0))
    _docker("start", CONTAINER)
    restarted_at = time.monotonic()
    restarted_utc = dt.datetime.now(dt.UTC)

    broker_back_at: float | None = None
    offsets_at_broker_back = offsets_at_outage
    peak = dict.fromkeys(groups, 0)
    streak = dict.fromkeys(groups, 0)
    first_ok: dict[str, float] = {}
    recovered: dict[str, float] = {}
    timeline: list[dict[str, object]] = []
    while len(recovered) < len(groups) and time.monotonic() - restarted_at < give_up:
        sample = await lag.sample()
        now = time.monotonic()
        if sample is not None:
            if broker_back_at is None:
                broker_back_at = now
                offsets_at_broker_back = await trades.total()
            timeline.append({"t": round(now - restarted_at, 1), "lag": sample})
            for group in groups:
                if group in recovered:
                    continue
                peak[group] = max(peak[group], sample[group])
                ok = sample[group] <= baseline[group] + SLACK
                streak[group] = streak[group] + 1 if ok else 0
                if streak[group] == 1:
                    first_ok[group] = now
                if streak[group] >= STABLE_SAMPLES:
                    recovered[group] = first_ok[group]
        await asyncio.sleep(1)
    await lag.close()
    offsets_after_restart = await trades.total()
    # From the broker's first answer, not from `docker start`: the boot in between can
    # publish nothing, and counting it would understate the rate and hide a catch-up burst.
    after_span = time.monotonic() - (broker_back_at or restarted_at)
    acknowledged = {
        "trades_per_second_before_outage": round(
            (offsets_at_outage - offsets_baseline_start) / baseline_span, 1
        ),
        "trades_acknowledged_during_outage": 0,  # the broker was stopped
        "trades_per_second_after_restart": round(
            (offsets_after_restart - offsets_at_broker_back) / after_span, 1
        ),
        "trades_never_published_estimate": round(
            (offsets_at_outage - offsets_baseline_start) / baseline_span * outage
        ),
    }

    def since(moment: float | None, group: str) -> float | None:
        at = recovered.get(group)
        return round(at - moment, 1) if at is not None and moment is not None else None

    per_group = {
        group: {
            "baseline_max_lag": baseline[group],
            "peak_lag": peak[group],
            "seconds_broker_answering_to_recovered": since(broker_back_at, group),
            "seconds_outage_start_to_recovered": since(outage_started, group),
            "recovered": group in recovered,
        }
        for group in groups
    }
    intervals = [b["t"] - a["t"] for a, b in itertools.pairwise(timeline)]
    write_result(
        "recovery_time",
        description="Lag drain, and trades lost, after the broker is stopped for a fixed outage.",
        command=f"cd backend && uv run python ../benchmarks/recovery_time.py --outage {outage}",
        workload={
            "outage_seconds": outage,
            "baseline_seconds": baseline_seconds,
            "feed": "synthetic provider at its configured live rate",
            "consumer_groups": groups,
        },
        configuration={
            "recovered_when": (
                f"first sample at or below baseline max + {SLACK} messages that then holds for "
                f"{STABLE_SAMPLES} consecutive samples"
            ),
            "supervisor": "scripts/dev.ps1 restarts any worker that exits, polling every 5s",
            "broker": "single KRaft broker in Docker, `docker stop -t 10` / `docker start`",
            "give_up_after_seconds": give_up,
        },
        results={
            "seconds_docker_start_to_broker_answering": (
                round(broker_back_at - restarted_at, 1) if broker_back_at else None
            ),
            "outage_window_utc": [outage_started_utc.isoformat(), restarted_utc.isoformat()],
            "trades_at_the_broker": acknowledged,
            "groups": per_group,
            "all_recovered": len(recovered) == len(groups),
            "mean_seconds_between_samples": (
                round(sum(intervals) / len(intervals), 2) if intervals else None
            ),
            "timeline": timeline,
        },
        caveats=[
            "Low peak lag means little reached the broker to drain, not that nothing is "
            "missing: compare the trade rate after the restart with the rate before. A feed "
            "that caught up would show a burst; `trades_never_published_estimate` is the "
            "pre-outage rate times the outage length, an estimate of the gap, not a count of "
            "failed publishes (the producer's own log has those).",
            "A clean `docker stop`/`start` of one broker. A network partition, a crashed "
            "broker with log recovery, or a multi-broker failover would behave differently.",
            "Includes the supervisor's restart delay for any worker that exited on eviction.",
        ],
    )
    print(acknowledged)  # noqa: T201
    for group, data in per_group.items():
        print(group, data)  # noqa: T201


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--outage", type=int, default=60)
    parser.add_argument("--baseline", type=int, default=15)
    parser.add_argument("--give-up", type=int, default=600)
    options = parser.parse_args()
    asyncio.run(main(options.outage, options.baseline, options.give_up))
