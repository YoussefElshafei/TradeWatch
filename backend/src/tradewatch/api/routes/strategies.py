"""Strategy CRUD (ADR-0006): owner-scoped configuration, not code.

Every route requires a bearer token (`CurrentUserDep`) and every repository call goes through
`OwnedRepository`, so a strategy that is not the caller's own is a 404 — never a 403 — the
same rule `infrastructure/repositories/base.py` establishes for every owner-scoped resource.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError

from tradewatch.api.dependencies import CurrentUserDep, SessionDep
from tradewatch.api.schemas.strategies import (
    StrategyCreateRequest,
    StrategyListResponse,
    StrategyResponse,
)
from tradewatch.application.strategy import (
    InvalidStrategyParametersError,
    UnknownStrategyTypeError,
    compile_strategy,
)
from tradewatch.infrastructure.database.models import Strategy
from tradewatch.infrastructure.repositories import StrategyRepository

router = APIRouter(prefix="/api/v1/strategies", tags=["strategies"])


def _validate_parameters(strategy_type: str, parameters: dict[str, object]) -> None:
    """Raise 422 for a strategy type or parameter set the strategy core would reject.

    Checked here, at creation, and again by the Backtest Runner at run time (ADR-0006:
    "validated on write and again on load") — a row can be changed by a migration or by hand
    between the two.
    """
    try:
        compile_strategy(strategy_type, parameters)
    except (UnknownStrategyTypeError, InvalidStrategyParametersError) as error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)
        ) from error


@router.post(
    "",
    response_model=StrategyResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        409: {"description": "A strategy with this name already exists"},
        422: {"description": "Unknown strategy_type or parameters that fail its schema"},
    },
)
async def create_strategy(
    session: SessionDep, claims: CurrentUserDep, request: StrategyCreateRequest
) -> Strategy:
    _validate_parameters(request.strategy_type, request.parameters)

    strategy = Strategy(
        user_id=claims.user_id,
        name=request.name,
        strategy_type=request.strategy_type,
        parameters=request.parameters,
        symbols=request.symbols,
    )
    try:
        await StrategyRepository(session).add(strategy)
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"A strategy named {request.name!r} already exists.",
        ) from error
    return strategy


@router.get("", response_model=StrategyListResponse)
async def list_strategies(
    session: SessionDep,
    claims: CurrentUserDep,
    include_archived: Annotated[bool, Query(description="Include archived strategies.")] = False,
) -> StrategyListResponse:
    repository = StrategyRepository(session)
    strategies = (
        await repository.list_for(claims.user_id)
        if include_archived
        else await repository.list_active(claims.user_id)
    )
    return StrategyListResponse(
        count=len(strategies),
        strategies=[StrategyResponse.model_validate(row) for row in strategies],
    )


@router.get(
    "/{strategy_id}",
    response_model=StrategyResponse,
    responses={404: {"description": "Not found"}},
)
async def get_strategy(
    session: SessionDep, claims: CurrentUserDep, strategy_id: uuid.UUID
) -> Strategy:
    strategy = await StrategyRepository(session).get_for(strategy_id, claims.user_id)
    if strategy is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found.")
    return strategy


@router.delete(
    "/{strategy_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={404: {"description": "Not found"}},
)
async def archive_strategy(
    session: SessionDep, claims: CurrentUserDep, strategy_id: uuid.UUID
) -> None:
    archived = await StrategyRepository(session).archive(strategy_id, claims.user_id)
    if not archived:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found.")
    await session.commit()
