"""What every benchmark result records, so a number never travels without its conditions.

`benchmarks/README.md` lists the rules: the commit, the configuration, the hardware and OS,
the workload, and raw percentiles rather than a summary. Each benchmark builds its own
`workload` and `results` dicts and hands them to :func:`write_result`, which adds the rest
and writes `benchmarks/results/<name>/<UTC timestamp>.json`.

Run from `backend/` so the project's environment is the one measured::

    cd backend && uv run python ../benchmarks/<benchmark>.py
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
import platform
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

RESULTS_DIR = Path(__file__).resolve().parent / "results"
REPO_ROOT = Path(__file__).resolve().parent.parent


def percentiles(samples: Sequence[float]) -> dict[str, float]:
    """Nearest-rank percentiles plus the extremes, in the samples' own unit.

    Nearest-rank rather than interpolated: every value reported is one that was actually
    observed, which is the property a reader assumes a "p99" has.
    """
    if not samples:
        return {}
    ordered = sorted(samples)

    def rank(p: float) -> float:
        return ordered[max(math.ceil(p / 100 * len(ordered)) - 1, 0)]

    return {
        "count": len(ordered),
        "min": ordered[0],
        "p50": rank(50),
        "p90": rank(90),
        "p95": rank(95),
        "p99": rank(99),
        "max": ordered[-1],
        "mean": sum(ordered) / len(ordered),
    }


def _run(*command: str) -> str:
    if shutil.which(command[0]) is None:
        return "unavailable"
    try:
        completed = subprocess.run(  # noqa: S603 - fixed commands, no user input
            command, capture_output=True, text=True, timeout=20, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        return "unavailable"
    return completed.stdout.strip() or "unavailable"


def _windows_hardware() -> dict[str, Any]:
    query = (
        "$c=Get-CimInstance Win32_Processor | Select-Object -First 1;"
        "$m=(Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory;"
        "Write-Output $c.Name; Write-Output $c.NumberOfCores;"
        "Write-Output $c.NumberOfLogicalProcessors; Write-Output $m"
    )
    lines = _run("powershell", "-NoProfile", "-Command", query).splitlines()
    if len(lines) < 4:
        return {}
    return {
        "cpu": lines[0].strip(),
        "physical_cores": int(lines[1]),
        "logical_cpus": int(lines[2]),
        "memory_gb": round(int(lines[3]) / 1024**3, 1),
    }


def _posix_hardware() -> dict[str, Any]:
    info: dict[str, Any] = {"logical_cpus": os.cpu_count()}
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                info["cpu"] = line.split(":", 1)[1].strip()
                break
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal"):
                info["memory_gb"] = round(int(line.split()[1]) / 1024**2, 1)
    else:  # macOS
        info["cpu"] = _run("sysctl", "-n", "machdep.cpu.brand_string")
        memory = _run("sysctl", "-n", "hw.memsize")
        if memory.isdigit():
            info["memory_gb"] = round(int(memory) / 1024**3, 1)
    return info


def environment() -> dict[str, Any]:
    """The machine, the software, and the exact code a result was produced on."""
    hardware = _windows_hardware() if sys.platform == "win32" else _posix_hardware()
    dirty = _run("git", "-C", str(REPO_ROOT), "status", "--porcelain", "--untracked-files=no")
    return {
        "commit": _run("git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"),
        "working_tree_clean": dirty in ("", "unavailable"),
        "hardware": hardware,
        "os": platform.platform(),
        "python": platform.python_version(),
        "docker": _run("docker", "version", "--format", "{{.Server.Version}}"),
        "note": (
            "Single developer machine; PostgreSQL, Redis and a one-broker Kafka in Docker "
            "Desktop on the same host as the code under test, sharing its CPU and disk."
        ),
    }


def write_result(
    name: str,
    *,
    description: str,
    command: str,
    workload: dict[str, Any],
    configuration: dict[str, Any],
    results: dict[str, Any],
    caveats: Sequence[str],
) -> Path:
    """Write one dated result file and print where it went."""
    stamp = dt.datetime.now(dt.UTC)
    document = {
        "benchmark": name,
        "description": description,
        "measured_at": stamp.isoformat(),
        "command": command,
        "environment": environment(),
        "workload": workload,
        "configuration": configuration,
        "results": results,
        "does_not_prove": list(caveats),
    }
    target = RESULTS_DIR / name / f"{stamp:%Y-%m-%dT%H%M%SZ}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(document, indent=2, default=str) + "\n", encoding="utf-8")
    print(f"wrote {target.relative_to(REPO_ROOT)}")  # noqa: T201 - a CLI's output
    return target
