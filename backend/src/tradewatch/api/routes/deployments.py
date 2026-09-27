"""Strategy deployment lifecycle: create / start / pause / resume / stop (Phase 7).

Ownership is enforced through the portfolio a deployment belongs to — `strategy_deployments`
itself carries no `user_id` (`ARCHITECTURE.md`'s chain is `strategy → deployment → ... →
position`, and a deployment's owner is exactly whoever owns its portfolio). Every route below
fetches the deployment, then checks the caller owns its `portfolio_id` through
`PortfolioRepository`, before doing anything else — a deployment whose portfolio the caller
does not own is a 404, the same rule every other owner-scoped resource follows.

**Status transitions**, deliberately narrow (each any other starting status rejects with 409):

``created --start--> running --pause--> paused --resume--> running``, and any of
``created``/``running``/``paused`` ``--stop--> stopped`` (terminal). ``errored`` has no
route here that leaves it — recovering one is an operator action, not a Phase 7 concern.
"""

from __future__ import annotations

import datetime as dt
import uuid

from fastapi import APIRouter, HTTPException, status

from tradewatch.api.dependencies import CurrentUserDep, SessionDep, SettingsDep
from tradewatch.api.schemas.deployments import (
    DeploymentCreateRequest,
    DeploymentListResponse,
    DeploymentResponse,
)
from tradewatch.application.execution import ExecutionConfig
from tradewatch.application.trading import PositionSizingConfig, RiskLimitsConfig
from tradewatch.domain.enums import DeploymentStatus
from tradewatch.infrastructure.database.models import StrategyDeployment
from tradewatch.infrastructure.repositories import (
    DeploymentRepository,
    PortfolioRepository,
    StrategyRepository,
)

router = APIRouter(prefix="/api/v1/deployments", tags=["deployments"])


async def _require_owned_deployment(
    session: SessionDep, claims: CurrentUserDep, deployment_id: uuid.UUID
) -> StrategyDeployment:
    deployment = await DeploymentRepository(session).get(deployment_id)
    if deployment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found.")
    owned = await PortfolioRepository(session).get_for(deployment.portfolio_id, claims.user_id)
    if owned is None:
        # Exists, but not owned by the caller — 404, not 403, the IDOR defence every other
        # owner-scoped resource uses.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Deployment not found.")
    return deployment


def _conflict(deployment: StrategyDeployment, action: str, *, required: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=(
            f"Cannot {action} a deployment that is {deployment.status.value!r}; "
            f"it must be {required!r}."
        ),
    )


@router.post("", response_model=DeploymentResponse, status_code=status.HTTP_201_CREATED)
async def create_deployment(
    session: SessionDep,
    claims: CurrentUserDep,
    settings: SettingsDep,
    request: DeploymentCreateRequest,
) -> StrategyDeployment:
    strategy = await StrategyRepository(session).get_for(request.strategy_id, claims.user_id)
    if strategy is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found.")
    portfolio = await PortfolioRepository(session).get_for(request.portfolio_id, claims.user_id)
    if portfolio is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found.")

    position_sizing = request.position_sizing or PositionSizingConfig.from_settings(
        settings.execution
    )
    risk_limits = request.risk_limits or RiskLimitsConfig.from_settings(settings.execution)
    execution_config = (
        ExecutionConfig.model_validate(request.execution_config)
        if request.execution_config is not None
        else ExecutionConfig.from_settings(settings.execution)
    )

    deployment = StrategyDeployment(
        strategy_id=strategy.id,
        portfolio_id=portfolio.id,
        status=DeploymentStatus.CREATED,
        position_sizing=position_sizing.as_json(),
        risk_limits=risk_limits.as_json(),
        execution_config=execution_config.as_json(),
    )
    await DeploymentRepository(session).add(deployment)
    await session.commit()
    return deployment


@router.get("", response_model=DeploymentListResponse)
async def list_deployments(
    session: SessionDep, claims: CurrentUserDep, portfolio_id: uuid.UUID
) -> DeploymentListResponse:
    portfolio = await PortfolioRepository(session).get_for(portfolio_id, claims.user_id)
    if portfolio is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found.")
    deployments = await DeploymentRepository(session).list_for_portfolio(portfolio_id)
    return DeploymentListResponse(
        count=len(deployments),
        deployments=[DeploymentResponse.model_validate(row) for row in deployments],
    )


@router.get(
    "/{deployment_id}",
    response_model=DeploymentResponse,
    responses={404: {"description": "Not found"}},
)
async def get_deployment(
    session: SessionDep, claims: CurrentUserDep, deployment_id: uuid.UUID
) -> StrategyDeployment:
    return await _require_owned_deployment(session, claims, deployment_id)


@router.post(
    "/{deployment_id}/start",
    response_model=DeploymentResponse,
    responses={404: {"description": "Not found"}, 409: {"description": "Not startable"}},
)
async def start_deployment(
    session: SessionDep, claims: CurrentUserDep, deployment_id: uuid.UUID
) -> StrategyDeployment:
    deployment = await _require_owned_deployment(session, claims, deployment_id)
    if deployment.status is not DeploymentStatus.CREATED:
        raise _conflict(deployment, "start", required=DeploymentStatus.CREATED.value)
    deployment.status = DeploymentStatus.RUNNING
    deployment.started_at = dt.datetime.now(dt.UTC)
    await session.commit()
    # `updated_at` is server-computed (`onupdate=func.now()`) and an UPDATE, unlike an
    # INSERT, does not report it back without an explicit refresh — `expire_on_commit=False`
    # keeps every *other* attribute available, but this one column would otherwise still
    # read as whatever it was before this request, or raise MissingGreenlet if the session
    # tries to lazy-load it after FastAPI's response serialization runs outside this
    # coroutine. Every other route in this file that mutates and returns a row needs this.
    await session.refresh(deployment)
    return deployment


@router.post(
    "/{deployment_id}/pause",
    response_model=DeploymentResponse,
    responses={404: {"description": "Not found"}, 409: {"description": "Not pausable"}},
)
async def pause_deployment(
    session: SessionDep, claims: CurrentUserDep, deployment_id: uuid.UUID
) -> StrategyDeployment:
    deployment = await _require_owned_deployment(session, claims, deployment_id)
    if deployment.status is not DeploymentStatus.RUNNING:
        raise _conflict(deployment, "pause", required=DeploymentStatus.RUNNING.value)
    deployment.status = DeploymentStatus.PAUSED
    await session.commit()
    await session.refresh(deployment)
    return deployment


@router.post(
    "/{deployment_id}/resume",
    response_model=DeploymentResponse,
    responses={404: {"description": "Not found"}, 409: {"description": "Not resumable"}},
)
async def resume_deployment(
    session: SessionDep, claims: CurrentUserDep, deployment_id: uuid.UUID
) -> StrategyDeployment:
    deployment = await _require_owned_deployment(session, claims, deployment_id)
    if deployment.status is not DeploymentStatus.PAUSED:
        raise _conflict(deployment, "resume", required=DeploymentStatus.PAUSED.value)
    # `started_at` is untouched on purpose: it anchors the live driver's state-warm-up replay
    # window (application/trading/live_service.py), which must not shrink just because the
    # deployment was paused and resumed.
    deployment.status = DeploymentStatus.RUNNING
    await session.commit()
    await session.refresh(deployment)
    return deployment


@router.post(
    "/{deployment_id}/stop",
    response_model=DeploymentResponse,
    responses={404: {"description": "Not found"}, 409: {"description": "Not stoppable"}},
)
async def stop_deployment(
    session: SessionDep, claims: CurrentUserDep, deployment_id: uuid.UUID
) -> StrategyDeployment:
    deployment = await _require_owned_deployment(session, claims, deployment_id)
    if deployment.status not in {
        DeploymentStatus.CREATED,
        DeploymentStatus.RUNNING,
        DeploymentStatus.PAUSED,
    }:
        raise _conflict(deployment, "stop", required="created, running, or paused")
    deployment.status = DeploymentStatus.STOPPED
    deployment.stopped_at = dt.datetime.now(dt.UTC)
    await session.commit()
    await session.refresh(deployment)
    return deployment
