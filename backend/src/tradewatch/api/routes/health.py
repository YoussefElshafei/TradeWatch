"""Operational endpoints.

`/health` answers "is this process alive?" and must never depend on anything external —
a liveness probe that fails when the database is down causes a restart loop that makes the
outage worse.

`/ready` answers "can this process serve traffic?" and checks dependencies. Phase 0 has no
dependencies wired, so it reports an empty check set; Phase 1 registers PostgreSQL,
Phase 2 Kafka, Phase 4 Redis. The registry exists now so those phases add a line rather
than a design.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Literal

from fastapi import APIRouter, Response
from pydantic import BaseModel, Field

from tradewatch import __version__

router = APIRouter(tags=["ops"])

#: name -> check. A check returns None when healthy, or a short reason when not.
ReadinessCheck = Callable[[], Awaitable[str | None]]
_CHECKS: dict[str, ReadinessCheck] = {}


def register_readiness_check(name: str, check: ReadinessCheck) -> None:
    """Add a dependency check to `/ready`.

    Raises:
        ValueError: If a check is already registered under this name — silently replacing
            one would hide a dependency.
    """
    if name in _CHECKS:
        raise ValueError(f"readiness check {name!r} is already registered")
    _CHECKS[name] = check


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


@router.get("/health", response_model=HealthResponse, summary="Liveness probe")
async def health() -> HealthResponse:
    """Report that the process is running. Checks nothing external, on purpose."""
    return HealthResponse(version=__version__)


@router.get("/ready", response_model=ReadinessResponse, summary="Readiness probe")
async def ready(response: Response) -> ReadinessResponse:
    """Report whether every registered dependency is usable.

    Returns 503 when any check fails, so a load balancer stops sending traffic here
    without the process being killed.
    """
    results: dict[str, str] = {}
    healthy = True
    for name, check in _CHECKS.items():
        try:
            failure = await check()
        except Exception as exc:  # a check that raises is a check that failed
            failure = type(exc).__name__
        results[name] = "ok" if failure is None else failure
        healthy = healthy and failure is None

    if not healthy:
        response.status_code = 503
    return ReadinessResponse(status="ready" if healthy else "not_ready", checks=results)
