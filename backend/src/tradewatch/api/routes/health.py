"""Operational endpoints.

`/health` answers "is this process alive?" and must never depend on anything external —
a liveness probe that fails when the database is down causes a restart loop that makes the
outage worse.

`/ready` answers "can this process serve traffic?" and reports every dependency by name —
`database` (critical), `redis` and `kafka` (reported, never fatal: the API serves reads
from PostgreSQL without either) — so an outage names itself instead of hiding behind one
aggregate status.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Literal

from fastapi import APIRouter, Response
from pydantic import BaseModel, Field

from tradewatch import __version__

router = APIRouter(tags=["ops"])

#: name -> check. A check returns None when healthy, or a short reason when not.
ReadinessCheck = Callable[[], Awaitable[str | None]]
_CHECKS: dict[str, tuple[ReadinessCheck, bool]] = {}

#: Per check. Above `BrokerHealth`'s own connect timeout, so its more specific answer wins.
_CHECK_TIMEOUT_SECONDS = 4.0


def register_readiness_check(name: str, check: ReadinessCheck, *, critical: bool = True) -> None:
    """Add a dependency check to `/ready`.

    Args:
        critical: Whether a failure here should turn `/ready` unhealthy (503) and stop a
            load balancer sending traffic. PostgreSQL is critical — the API cannot serve a
            correct response without it. Redis is not (failure F9: "degrade, never fail") —
            a cache outage must never look like an outage to anything watching `/ready`,
            which is exactly the failure mode F9 exists to rule out. A non-critical check
            still appears in the response body, just never flips ``status``.

    Raises:
        ValueError: If a check is already registered under this name — silently replacing
            one would hide a dependency.
    """
    if name in _CHECKS:
        raise ValueError(f"readiness check {name!r} is already registered")
    _CHECKS[name] = (check, critical)


def clear_readiness_checks() -> None:
    """Remove every registered check. For tests and application teardown."""
    _CHECKS.clear()


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    version: str = Field(description="Application version.")


class ReadinessResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    checks: dict[str, str] = Field(
        default_factory=dict,
        description="Dependency name to 'ok' or a short failure reason.",
    )


async def _run_check(check: ReadinessCheck) -> str | None:
    """One check, bounded in time, with an exception reported as its own failure reason.

    The checks run concurrently and each has its own deadline, so one hung dependency makes
    `/ready` report *that* dependency as `timeout` instead of making the whole probe hang —
    the "exactly which one is down" this endpoint exists to answer.
    """
    task = asyncio.ensure_future(check())
    # `asyncio.wait`, not `wait_for`: only *our* deadline is "timeout". A check that itself
    # raises `TimeoutError` (a driver's own) is reported under that name, like any failure.
    done, _pending = await asyncio.wait({task}, timeout=_CHECK_TIMEOUT_SECONDS)
    if not done:
        task.cancel()
        return "timeout"
    error = task.exception()
    if error is not None:  # a check that raises is a check that failed
        return type(error).__name__
    return task.result()


@router.get("/health", response_model=HealthResponse, summary="Liveness probe")
async def health() -> HealthResponse:
    """Report that the process is running. Checks nothing external, on purpose."""
    return HealthResponse(version=__version__)


@router.get("/ready", response_model=ReadinessResponse, summary="Readiness probe")
async def ready(response: Response) -> ReadinessResponse:
    """Report whether every registered dependency is usable.

    Returns 503 when a critical check fails, so a load balancer stops sending traffic here
    without the process being killed.
    """
    names = list(_CHECKS)
    outcomes = await asyncio.gather(*(_run_check(_CHECKS[name][0]) for name in names))
    results = {
        name: "ok" if failure is None else failure
        for name, failure in zip(names, outcomes, strict=True)
    }
    healthy = all(
        failure is None or not _CHECKS[name][1]
        for name, failure in zip(names, outcomes, strict=True)
    )

    if not healthy:
        response.status_code = 503
    return ReadinessResponse(status="ready" if healthy else "not_ready", checks=results)
