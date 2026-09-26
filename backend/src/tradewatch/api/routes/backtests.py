"""Backtest submit/list/get/trades/equity (`ARCHITECTURE.md` §4.3).

Submitting a backtest never runs it — it inserts a `queued` row and returns immediately
(§3.6). The Backtest Runner worker picks it up; a client polls `GET /{id}` or lists
`/{id}/trades` and `/{id}/equity` once `status` is `completed`.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status

from tradewatch.api.dependencies import CurrentUserDep, SessionDep, SettingsDep
from tradewatch.api.schemas.backtests import (
    BacktestEquityListResponse,
    BacktestEquityPointResponse,
    BacktestListResponse,
    BacktestResponse,
    BacktestSubmitRequest,
    BacktestTradeListResponse,
    BacktestTradeResponse,
)
from tradewatch.application.execution import ExecutionConfig
from tradewatch.application.strategy import (
    InvalidStrategyParametersError,
    UnknownStrategyTypeError,
    compile_strategy,
)
from tradewatch.infrastructure.database.models import BacktestRun
from tradewatch.infrastructure.repositories import (
    BacktestEquityPointRepository,
    BacktestRunRepository,
    BacktestTradeRepository,
    InstrumentRepository,
    StrategyRepository,
)

router = APIRouter(prefix="/api/v1/backtests", tags=["backtests"])


async def _require_run(
    session: SessionDep, claims: CurrentUserDep, backtest_id: uuid.UUID
) -> BacktestRun:
    run = await BacktestRunRepository(session).get_for(backtest_id, claims.user_id)
    if run is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backtest not found.")
    return run


@router.post(
    "",
    response_model=BacktestResponse,
    status_code=status.HTTP_201_CREATED,
    responses={
        404: {"description": "Unknown strategy"},
        409: {"description": "Too many concurrent backtests for this user"},
        422: {"description": "Invalid parameters or date range"},
    },
)
async def submit_backtest(
    session: SessionDep,
    settings: SettingsDep,
    claims: CurrentUserDep,
    request: BacktestSubmitRequest,
) -> BacktestRun:
    strategy = await StrategyRepository(session).get_for(request.strategy_id, claims.user_id)
    if strategy is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Strategy not found.")

    # ADR-0006: validated on write and again on load — a strategy row can be edited by hand
    # between the two, and a bad backtest is cheaper to refuse here than to fail mid-run.
    try:
        compile_strategy(strategy.strategy_type, strategy.parameters)
    except (UnknownStrategyTypeError, InvalidStrategyParametersError) as error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)
        ) from error

    active = await BacktestRunRepository(session).count_active_for(claims.user_id)
    if active >= settings.security.max_backtests_per_user_concurrent:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"You already have {active} queued or running backtests, "
                f"the maximum allowed ({settings.security.max_backtests_per_user_concurrent})."
            ),
        )

    execution_config = ExecutionConfig.from_settings(settings.execution)
    if request.execution_config is not None:
        overrides = request.execution_config.model_dump(exclude_none=True)
        execution_config = execution_config.model_copy(update=overrides)

    run = BacktestRun(
        user_id=claims.user_id,
        strategy_id=strategy.id,
        symbols=request.symbols or list(strategy.symbols),
        start_date=request.start_date,
        end_date=request.end_date,
        initial_cash=request.initial_cash or settings.execution.initial_cash,
        execution_config=execution_config.as_json(),
    )
    await BacktestRunRepository(session).add(run)
    await session.commit()
    return run


@router.get("", response_model=BacktestListResponse)
async def list_backtests(session: SessionDep, claims: CurrentUserDep) -> BacktestListResponse:
    runs = await BacktestRunRepository(session).list_for(claims.user_id)
    return BacktestListResponse(
        count=len(runs), backtests=[BacktestResponse.model_validate(run) for run in runs]
    )


@router.get(
    "/{backtest_id}",
    response_model=BacktestResponse,
    responses={404: {"description": "Not found"}},
)
async def get_backtest(
    session: SessionDep, claims: CurrentUserDep, backtest_id: uuid.UUID
) -> BacktestRun:
    return await _require_run(session, claims, backtest_id)


@router.get(
    "/{backtest_id}/trades",
    response_model=BacktestTradeListResponse,
    responses={404: {"description": "Not found"}},
)
async def list_backtest_trades(
    session: SessionDep, claims: CurrentUserDep, backtest_id: uuid.UUID
) -> BacktestTradeListResponse:
    await _require_run(session, claims, backtest_id)  # the 404-not-403 ownership check
    trades = await BacktestTradeRepository(session).list_for_run(backtest_id)

    # One lookup per trade rather than a batched reverse-symbol query: a backtest's trade
    # count is small (dozens, not thousands), and InstrumentRepository has no "many by id"
    # method today — adding one for a query this cheap would be speculative machinery.
    instrument_repository = InstrumentRepository(session)
    responses = []
    for trade in trades:
        instrument = await instrument_repository.get(trade.instrument_id)
        symbol = instrument.symbol if instrument is not None else str(trade.instrument_id)
        responses.append(
            BacktestTradeResponse(
                id=trade.id,
                symbol=symbol,
                side=trade.side,
                quantity=trade.quantity,
                entry_price=trade.entry_price,
                entry_at=trade.entry_at,
                exit_price=trade.exit_price,
                exit_at=trade.exit_at,
                pnl=trade.pnl,
                return_pct=trade.return_pct,
            )
        )
    return BacktestTradeListResponse(count=len(responses), trades=responses)


@router.get(
    "/{backtest_id}/equity",
    response_model=BacktestEquityListResponse,
    responses={404: {"description": "Not found"}},
)
async def get_backtest_equity(
    session: SessionDep, claims: CurrentUserDep, backtest_id: uuid.UUID
) -> BacktestEquityListResponse:
    await _require_run(session, claims, backtest_id)  # the 404-not-403 ownership check
    points = await BacktestEquityPointRepository(session).list_for_run(backtest_id)
    return BacktestEquityListResponse(
        count=len(points),
        equity_curve=[BacktestEquityPointResponse.model_validate(point) for point in points],
    )
