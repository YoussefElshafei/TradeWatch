"""End-to-end latency: provider timestamp -> a real browser, p50/p95/p99.

**Path measured.** The live stack, unmodified: the synthetic provider stamps `occurred_at`
as it emits each event -> producer -> Kafka -> broadcaster -> Redis Pub/Sub -> the API's
`/ws/market` -> a WebSocket in a headless Chrome page (`e2e_latency_browser.mjs`). Three
intervals are reported per event:

* `provider_to_browser_ms` — `occurred_at` to the page's `message` event. The headline.
* `ingest_to_browser_ms` — `ingested_at` to the same moment: everything after normalising.
* `provider_to_next_frame_ms` — `occurred_at` to the start of the next animation frame, the
  earliest a chart could paint it. React's own render work is *not* included.

All timestamps come from one machine's clock (the producer and the browser run on the same
host), so no clock-skew correction is needed or applied.

**Preconditions.** The whole stack running (`scripts/dev.ps1` or `dev.sh`) and Chrome
installed. The script registers `e2e-benchmark@example.com` in the development database on
first use and reuses it after — the one row it leaves behind.

    cd backend && uv run python ../benchmarks/e2e_latency.py [--seconds 120]
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx2 as httpx
from _common import percentiles, write_result
from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient

from tradewatch.config.env import load_env_file

HERE = Path(__file__).resolve().parent
SYMBOLS = "AAPL,MSFT,NVDA,SPY"
BROADCASTER_GROUP = "broadcaster"
MAX_BROADCASTER_LAG = 1_000
ACCOUNT = {"email": "e2e-benchmark@example.com", "password": "e2e-benchmark-password"}


def _epoch_ms(timestamp: str) -> float:
    return dt.datetime.fromisoformat(timestamp).timestamp() * 1000


def _token(api: str) -> str:
    with httpx.Client(base_url=api, timeout=10) as client:
        ready = client.get("/ready").json()
        if ready.get("status") != "ready":
            raise SystemExit(f"the stack is not ready: {ready}")
        client.post("/api/v1/auth/register", json=ACCOUNT)  # 409 after the first run
        response = client.post("/api/v1/auth/login", json=ACCOUNT)
        response.raise_for_status()
        token: str = response.json()["access_token"]
        return token


def _broadcaster_lag(bootstrap: str) -> int:
    """Messages the broadcaster's group has not yet relayed.

    Checked first because the broadcaster resumes from its committed offset: after any
    downtime it replays the backlog, and this benchmark would then report the backlog's age
    as "latency". See PERFORMANCE.md's findings for how to reset the group.
    """

    async def lag() -> int:
        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
        reader = AIOKafkaConsumer(bootstrap_servers=bootstrap)
        await admin.start()
        await reader.start()
        try:
            committed = await admin.list_consumer_group_offsets(BROADCASTER_GROUP)
            ends = await reader.end_offsets(list(committed))
            return sum(max(ends[tp] - meta.offset, 0) for tp, meta in committed.items())
        finally:
            await reader.stop()
            await admin.close()

    return asyncio.run(lag())


def main(api: str, seconds: int) -> None:
    load_env_file()
    behind = _broadcaster_lag(os.environ.get("TW_KAFKA_BOOTSTRAP", "localhost:9092"))
    if behind > MAX_BROADCASTER_LAG:
        raise SystemExit(
            f"the broadcaster is {behind} messages behind; latency now would measure the "
            "backlog's age. Let it drain, or reset the group (PERFORMANCE.md, findings)."
        )
    token = _token(api)
    ws = api.replace("http", "ws", 1) + f"/ws/market?token={token}&symbols={SYMBOLS}"
    completed = subprocess.run(  # noqa: S603 - our own script
        ["node", str(HERE / "e2e_latency_browser.mjs"), ws, str(seconds)],  # noqa: S607
        capture_output=True,
        text=True,
        check=True,
        timeout=seconds + 120,
    )
    payload = json.loads(completed.stdout)
    samples = payload["samples"]
    if len(samples) < 100:
        raise SystemExit(f"only {len(samples)} events arrived — is the producer running?")

    def series(key: str, start: str) -> list[float]:
        return [s[key] - _epoch_ms(s[start]) for s in samples if s[key] is not None]

    def rounded(values: list[float]) -> dict[str, float]:
        return {k: round(v, 2) for k, v in percentiles(values).items()}

    by_type: dict[str, int] = {}
    for sample in samples:
        by_type[sample["type"]] = by_type.get(sample["type"], 0) + 1

    write_result(
        "e2e_latency",
        description="Provider timestamp to a WebSocket message in a real browser page.",
        command=f"cd backend && uv run python ../benchmarks/e2e_latency.py --seconds {seconds}",
        workload={
            "seconds": seconds,
            "symbols": SYMBOLS.split(","),
            "events_received": len(samples),
            "events_by_type": by_type,
            "feed": "synthetic provider at its configured live rate (TW_PROVIDER=synthetic)",
        },
        configuration={
            "path": "producer -> Kafka -> broadcaster -> Redis Pub/Sub -> API /ws/market -> Chrome",
            "browser": payload["userAgent"],
            "processes": "one of each, started by scripts/dev.ps1",
        },
        results={
            "provider_to_browser_ms": rounded(series("received_ms", "occurred_at")),
            "ingest_to_browser_ms": rounded(series("received_ms", "ingested_at")),
            "provider_to_next_frame_ms": rounded(series("frame_ms", "occurred_at")),
        },
        caveats=[
            "Local loopback networking only: no internet, no load balancer, no TLS. Any real "
            "deployment adds those hops.",
            "One browser client. Fan-out cost with many connected clients is not measured.",
            "The synthetic provider's rate is modest; this is latency at light load, not "
            "latency under the ingest benchmark's saturation rate.",
            "`next_frame` is the earliest paint opportunity, not a measured paint: React's "
            "render of the chart is not included.",
        ],
    )
    print(json.dumps(rounded(series("received_ms", "occurred_at"))))  # noqa: T201


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--api", default="http://localhost:8010")
    parser.add_argument("--seconds", type=int, default=120)
    options = parser.parse_args()
    main(options.api, options.seconds)
