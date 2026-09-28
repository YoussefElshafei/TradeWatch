"""Portfolio CRUD: the simulated accounts strategy deployments trade in.

Owner-scoped exactly like `strategies.py` — every repository call goes through
`OwnedRepository`, so a portfolio that is not the caller's own is a 404, never a 403.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, status

from tradewatch.api.dependencies import CurrentUserDep, SessionDep
from tradewatch.api.schemas.portfolios import (
    PortfolioCreateRequest,
    PortfolioDetailResponse,
    PortfolioListResponse,
    PortfolioResponse,
    PositionResponse,
)
from tradewatch.infrastructure.database.models import Portfolio
from tradewatch.infrastructure.repositories import PortfolioRepository, PositionRepository

router = APIRouter(prefix="/api/v1/portfolios", tags=["portfolios"])


@router.post("", response_model=PortfolioResponse, status_code=status.HTTP_201_CREATED)
async def create_portfolio(
    session: SessionDep, claims: CurrentUserDep, request: PortfolioCreateRequest
) -> Portfolio:
    portfolio = Portfolio(
        user_id=claims.user_id,
        name=request.name,
        base_currency=request.base_currency.upper(),
        initial_cash=request.initial_cash,
        cash_balance=request.initial_cash,
    )
    await PortfolioRepository(session).add(portfolio)
    await session.commit()
    return portfolio


@router.get("", response_model=PortfolioListResponse)
async def list_portfolios(session: SessionDep, claims: CurrentUserDep) -> PortfolioListResponse:
    portfolios = await PortfolioRepository(session).list_for(claims.user_id)
    return PortfolioListResponse(
        count=len(portfolios),
        portfolios=[PortfolioResponse.model_validate(row) for row in portfolios],
    )


@router.get(
    "/{portfolio_id}",
    response_model=PortfolioDetailResponse,
    responses={404: {"description": "Not found"}},
)
async def get_portfolio(
    session: SessionDep, claims: CurrentUserDep, portfolio_id: uuid.UUID
) -> PortfolioDetailResponse:
    repository = PortfolioRepository(session)
    portfolio = await repository.get_for(portfolio_id, claims.user_id)
    if portfolio is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Portfolio not found.")

    positions = await PositionRepository(session).list_open(portfolio_id)
    total_equity = await repository.total_equity(portfolio_id, claims.user_id)
    assert total_equity is not None  # just confirmed ownership above
    return PortfolioDetailResponse(
        **PortfolioResponse.model_validate(portfolio).model_dump(),
        positions=[
            PositionResponse(
                instrument_id=position.instrument_id,
                symbol=position.instrument.symbol,
                quantity=position.quantity,
                avg_entry_price=position.avg_entry_price,
                realized_pnl=position.realized_pnl,
                updated_at=position.updated_at,
            )
            for position in positions
        ],
        total_equity=total_equity,
    )
