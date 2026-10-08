"""Recovery time: how long every consumer group takes to drain its lag after a 60s outage.

**Method.** With the whole stack running under `scripts/dev.ps1` / `dev.sh` (which restarts
any worker that exits — the workers deliberately exit when the broker evicts them):

1. Sample each consumer group's lag (log-end offset minus committed offset, summed over its
   partitions — the same figure `kafka-exporter` reports) for a baseline window.
2. `docker stop` the Kafka container, wait `--outage` seconds, `docker start` it.
3. Once the broker answers again, keep sampling every second. A group has *recovered* when
   its lag is back at or below its baseline maximum (plus a small slack) for five
   consecutive samples. Report, per group: peak lag, seconds from broker-back to recovered,
   and seconds from outage start to recovered.

Lag is read straight from the broker rather than from Prometheus, so the result does not
depend on scrape timing — and so a consumer that never comes back shows up as never
recovering, not as a missing series.

    cd backend && uv run python ../benchmarks/recovery_time.py [--outage 60]
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import write_result
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient

from tradewatch.config.env import load_env_file

CONTAINER = "tradewatch-kafka"
SLACK = 50  # messages: a baseline that was briefly zero should not make "recovered" unreachable
STABLE_SAMPLES = 5


async def _lag(bootstrap: str, groups: list[str]) -> dict[str, int] | None:
    """Total lag per group, or None when the broker cannot answer."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=3000)
    reader = AIOKafkaConsumer(bootstrap_servers=bootstrap, request_timeout_ms=3000)
    try:
        await asyncio.wait_for(admin.start(), timeout=5)
        await asyncio.wait_for(reader.start(), timeout=5)
        lags: dict[str, int] = {}
        for group in groups:
            committed = await admin.list_consumer_group_offsets(group)
            if not committed:
                lags[group] = 0
                continue
            ends = await reader.end_offsets(list(committed))
            lags[group] = sum(
                max(ends[tp] - meta.offset, 0) for tp, meta in committed.items() if meta.offset >= 0
            )
        return lags
    except Exception:  # broker down, or still electing a coordinator
        return None
    finally:
        with contextlib.suppress(Exception):
            await reader.stop()
        with contextlib.suppress(Exception):
            await admin.close()


async def _groups(bootstrap: str) -> list[str]:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        listed = await admin.list_consumer_groups()
    finally:
        await admin.close()
    return sorted(g[0] for g in listed if not g[0].startswith("bench"))


def _docker(*args: str) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True)  # noqa: S603, S607


async def main(outage: int, baseline_seconds: int, give_up: int) -> None:
    load_env_file()
    bootstrap = os.environ.get("TW_KAFKA_BOOTSTRAP", "localhost:9092")
    groups = await _groups(bootstrap)
    if not groups:
        raise SystemExit("no consumer groups found — is the stack running?")

    baseline: dict[str, int] = dict.fromkeys(groups, 0)
    for _ in range(baseline_seconds):
        sample = await _lag(bootstrap, groups)
        if sample is None:
            raise SystemExit("broker not answering before the outage — fix that first")
        baseline = {g: max(baseline[g], sample[g]) for g in groups}
        await asyncio.sleep(1)

    outage_started = time.monotonic()
    _docker("stop", "-t", "10", CONTAINER)
    await asyncio.sleep(max(outage - (time.monotonic() - outage_started), 0))
    _docker("start", CONTAINER)
    restarted_at = time.monotonic()

    broker_back_at: float | None = None
    peak = dict.fromkeys(groups, 0)
    streak = dict.fromkeys(groups, 0)
    recovered: dict[str, float] = {}
    timeline: list[dict[str, object]] = []
    while len(recovered) < len(groups) and time.monotonic() - restarted_at < give_up:
        sample = await _lag(bootstrap, groups)
        now = time.monotonic()
        if sample is not None:
            broker_back_at = broker_back_at or now
            timeline.append({"t": round(now - restarted_at, 1), "lag": sample})
            for group in groups:
                if group in recovered:
                    continue
                peak[group] = max(peak[group], sample[group])
                streak[group] = streak[group] + 1 if sample[group] <= baseline[group] + SLACK else 0
                if streak[group] >= STABLE_SAMPLES:
                    recovered[group] = now - (STABLE_SAMPLES - 1)
        await asyncio.sleep(1)

    def since(moment: float | None, group: str) -> float | None:
        at = recovered.get(group)
        return round(at - moment, 1) if at is not None and moment is not None else None

    per_group = {
        group: {
            "baseline_max_lag": baseline[group],
            "peak_lag": peak[group],
            "seconds_broker_back_to_recovered": since(broker_back_at, group),
            "seconds_outage_start_to_recovered": since(outage_started, group),
            "recovered": group in recovered,
        }
        for group in groups
    }
    write_result(
        "recovery_time",
        description="Consumer-lag drain after the Kafka broker is stopped for a fixed outage.",
        command=f"cd backend && uv run python ../benchmarks/recovery_time.py --outage {outage}",
        workload={
            "outage_seconds": outage,
            "baseline_seconds": baseline_seconds,
            "feed": "synthetic provider at its configured live rate",
            "consumer_groups": groups,
        },
        configuration={
            "recovered_when": (
                f"lag <= baseline max + {SLACK} for {STABLE_SAMPLES} consecutive 1s samples"
            ),
            "supervisor": "scripts/dev.ps1 restarts any worker that exits, polling every 5s",
            "broker": "single KRaft broker in Docker, `docker stop -t 10` / `docker start`",
            "give_up_after_seconds": give_up,
        },
        results={
            "seconds_docker_start_to_broker_answering": (
                round(broker_back_at - restarted_at, 1) if broker_back_at else None
            ),
            "groups": per_group,
            "all_recovered": len(recovered) == len(groups),
            "timeline": timeline,
        },
        caveats=[
            "Events the synthetic provider generated while the broker was down are not "
            "buffered by the producer; recovery here is the consumers catching up on what "
            "reached the broker, not proof that nothing was lost during the outage.",
            "A clean `docker stop`/`start` of one broker. A network partition, a crashed "
            "broker with log recovery, or a multi-broker failover would behave differently.",
            "Includes the supervisor's restart delay for workers that exited on eviction.",
        ],
    )
    for group, data in per_group.items():
        print(group, data)  # noqa: T201


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--outage", type=int, default=60)
    parser.add_argument("--baseline", type=int, default=15)
    parser.add_argument("--give-up", type=int, default=600)
    options = parser.parse_args()
    asyncio.run(main(options.outage, options.baseline, options.give_up))
