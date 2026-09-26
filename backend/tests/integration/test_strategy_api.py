"""Strategy CRUD (ADR-0006), against a real database — the `api` fixture is shared with
`test_backtest_api.py` from `conftest.py`.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable

import httpx2 as httpx
import pytest

from tradewatch.infrastructure.database.models import Strategy, User

pytestmark = pytest.mark.integration

AuthHeadersFor = Callable[[User], dict[str, str]]


class TestCreateStrategy:
    async def test_creates_and_returns_the_strategy(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.post(
            "/api/v1/strategies",
            json={
                "name": "My SMA strategy",
                "strategy_type": "sma_crossover",
                "parameters": {"fast_period": 5, "slow_period": 20},
                "symbols": ["aapl", "msft"],
            },
            headers=auth_headers_for(user),
        )
        assert response.status_code == 201
        body = response.json()
        assert body["name"] == "My SMA strategy"
        assert body["symbols"] == ["AAPL", "MSFT"]
        assert body["is_archived"] is False
        assert body["user_id"] == str(user.id)

    async def test_unknown_strategy_type_is_422(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.post(
            "/api/v1/strategies",
            json={
                "name": "Nonsense",
                "strategy_type": "not_a_real_strategy",
                "parameters": {},
                "symbols": ["AAPL"],
            },
            headers=auth_headers_for(user),
        )
        assert response.status_code == 422

    async def test_bad_parameters_are_422(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.post(
            "/api/v1/strategies",
            json={
                "name": "Backwards",
                "strategy_type": "sma_crossover",
                "parameters": {"fast_period": 30, "slow_period": 5},  # fast >= slow: invalid
                "symbols": ["AAPL"],
            },
            headers=auth_headers_for(user),
        )
        assert response.status_code == 422

    async def test_duplicate_name_for_the_same_user_is_409(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        payload = {
            "name": "Duplicate",
            "strategy_type": "sma_crossover",
            "parameters": {"fast_period": 5, "slow_period": 20},
            "symbols": ["AAPL"],
        }
        headers = auth_headers_for(user)
        first = await api.post("/api/v1/strategies", json=payload, headers=headers)
        assert first.status_code == 201
        second = await api.post("/api/v1/strategies", json=payload, headers=headers)
        assert second.status_code == 409

    async def test_requires_authentication(self, api: httpx.AsyncClient) -> None:
        response = await api.post(
            "/api/v1/strategies",
            json={
                "name": "No auth",
                "strategy_type": "sma_crossover",
                "parameters": {"fast_period": 5, "slow_period": 20},
                "symbols": ["AAPL"],
            },
        )
        assert response.status_code == 401


class TestListStrategies:
    async def test_lists_only_the_caller_own_strategies(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        response = await api.get("/api/v1/strategies", headers=auth_headers_for(user))
        assert response.status_code == 200
        body = response.json()
        assert body["count"] == 1
        assert body["strategies"][0]["id"] == str(strategy.id)

    async def test_another_users_strategies_are_invisible(
        self,
        api: httpx.AsyncClient,
        other_user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        response = await api.get("/api/v1/strategies", headers=auth_headers_for(other_user))
        assert response.status_code == 200
        assert response.json()["count"] == 0


class TestGetStrategy:
    async def test_owner_gets_it(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        response = await api.get(
            f"/api/v1/strategies/{strategy.id}", headers=auth_headers_for(user)
        )
        assert response.status_code == 200
        assert response.json()["id"] == str(strategy.id)

    async def test_another_user_gets_404_not_403(
        self,
        api: httpx.AsyncClient,
        other_user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        response = await api.get(
            f"/api/v1/strategies/{strategy.id}", headers=auth_headers_for(other_user)
        )
        assert response.status_code == 404

    async def test_a_missing_id_is_also_404(
        self, api: httpx.AsyncClient, user: User, auth_headers_for: AuthHeadersFor
    ) -> None:
        response = await api.get(
            f"/api/v1/strategies/{uuid.uuid4()}", headers=auth_headers_for(user)
        )
        assert response.status_code == 404


class TestArchiveStrategy:
    async def test_owner_can_archive(
        self,
        api: httpx.AsyncClient,
        user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        response = await api.delete(
            f"/api/v1/strategies/{strategy.id}", headers=auth_headers_for(user)
        )
        assert response.status_code == 204
        listing = await api.get("/api/v1/strategies", headers=auth_headers_for(user))
        assert listing.json()["count"] == 0

    async def test_another_user_gets_404_not_403(
        self,
        api: httpx.AsyncClient,
        other_user: User,
        strategy: Strategy,
        auth_headers_for: AuthHeadersFor,
    ) -> None:
        response = await api.delete(
            f"/api/v1/strategies/{strategy.id}", headers=auth_headers_for(other_user)
        )
        assert response.status_code == 404
