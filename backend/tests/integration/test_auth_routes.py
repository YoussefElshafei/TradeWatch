"""The auth endpoints, against a real database and a real Redis (ADR-0010).

Real Redis, unlike `test_market_api.py`'s deliberately-unconnected cache: rate limiting and
the revocation denylist are exactly the behaviour under test here, and an unconnected cache
degrades to "always allow, never revoked" (failure F9), which would make every negative test
in this file pass for the wrong reason. Redis state does not roll back the way the test's own
Postgres transaction does, so the fixture sweeps the keys this file's scopes can write before
each test hands out a client.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx2 as httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

from tradewatch.api.app import create_app
from tradewatch.api.dependencies import get_session
from tradewatch.api.routes.health import clear_readiness_checks
from tradewatch.config.settings import Settings
from tradewatch.infrastructure.cache import RedisCache

pytestmark = pytest.mark.integration

EMAIL = "auth-route-test@example.com"
PASSWORD = "correct horse battery staple"


def _refresh_cookie_header(value: str) -> dict[str, str]:
    """A `Cookie` header for one specific refresh-token value.

    Not the client's per-request `cookies=` kwarg: httpx deprecated it (ambiguous
    persistence semantics), and this file needs to send a specific — often stale —
    cookie value that differs from whatever the shared `api` client is currently
    carrying, which `client.cookies.set(...)` would overwrite for every later call too.
    """
    return {"Cookie": f"tw_refresh={value}"}


async def _sweep(cache: RedisCache) -> None:
    async for key in cache.client.scan_iter(match="tw:rl:*"):
        await cache.client.delete(key)
    async for key in cache.client.scan_iter(match="tw:revoked:*"):
        await cache.client.delete(key)


@pytest_asyncio.fixture
async def api(session: AsyncSession, settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    """Same shape as `test_market_api.py`'s `api` fixture: the app reads through the test's
    own transaction, so a `register` and a later `login` in the same test see each other's
    writes without either being committed to the real database.
    """
    clear_readiness_checks()
    app = create_app(settings)

    async def _session_override() -> AsyncSession:
        return session

    app.dependency_overrides[get_session] = _session_override

    cache = RedisCache(settings.redis)
    await cache.connect()
    await _sweep(cache)
    app.state.cache = cache

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client

    await _sweep(cache)
    await cache.disconnect()
    app.dependency_overrides.clear()
    clear_readiness_checks()


class TestRegister:
    async def test_creates_an_account(self, api: httpx.AsyncClient) -> None:
        response = await api.post(
            "/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD}
        )
        assert response.status_code == 201
        body = response.json()
        assert body["email"] == EMAIL
        assert body["role"] == "viewer"
        assert "password" not in body
        assert "password_hash" not in body

    async def test_duplicate_email_is_a_conflict(self, api: httpx.AsyncClient) -> None:
        await api.post("/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD})
        response = await api.post(
            "/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD}
        )
        assert response.status_code == 409

    async def test_duplicate_email_is_case_insensitive(self, api: httpx.AsyncClient) -> None:
        await api.post("/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD})
        response = await api.post(
            "/api/v1/auth/register", json={"email": EMAIL.upper(), "password": PASSWORD}
        )
        assert response.status_code == 409

    async def test_too_short_a_password_is_rejected(self, api: httpx.AsyncClient) -> None:
        response = await api.post(
            "/api/v1/auth/register", json={"email": EMAIL, "password": "short"}
        )
        assert response.status_code == 422

    async def test_malformed_email_is_rejected(self, api: httpx.AsyncClient) -> None:
        response = await api.post(
            "/api/v1/auth/register", json={"email": "not-an-email", "password": PASSWORD}
        )
        assert response.status_code == 422


class TestLogin:
    async def test_wrong_password_is_unauthorized(self, api: httpx.AsyncClient) -> None:
        await api.post("/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD})
        response = await api.post("/api/v1/auth/login", json={"email": EMAIL, "password": "nope"})
        assert response.status_code == 401

    async def test_unknown_email_gets_the_same_status_and_shape_as_wrong_password(
        self, api: httpx.AsyncClient
    ) -> None:
        """`ARCHITECTURE.md` §9.2: login responses are uniform whether the account exists
        or not — the one place in this API where matching status codes *is* the test.
        """
        await api.post("/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD})
        wrong_password = await api.post(
            "/api/v1/auth/login", json={"email": EMAIL, "password": "nope"}
        )
        unknown_email = await api.post(
            "/api/v1/auth/login", json={"email": "nobody@example.com", "password": "nope"}
        )
        assert wrong_password.status_code == unknown_email.status_code == 401
        assert wrong_password.json().keys() == unknown_email.json().keys()

    async def test_correct_credentials_issue_a_token_and_a_refresh_cookie(
        self, api: httpx.AsyncClient
    ) -> None:
        await api.post("/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD})
        response = await api.post("/api/v1/auth/login", json={"email": EMAIL, "password": PASSWORD})
        assert response.status_code == 200
        body = response.json()
        assert body["token_type"] == "bearer"
        assert body["user"]["email"] == EMAIL
        assert "refresh_token" not in body, "the refresh token must never appear in the JSON body"
        assert "tw_refresh" in response.cookies
        assert response.cookies["tw_refresh"] not in response.text


class TestMe:
    async def test_returns_the_signed_in_users_own_profile(self, api: httpx.AsyncClient) -> None:
        await api.post("/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD})
        login = await api.post("/api/v1/auth/login", json={"email": EMAIL, "password": PASSWORD})
        token = login.json()["access_token"]

        response = await api.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 200
        assert response.json()["email"] == EMAIL

    async def test_no_token_is_unauthorized(self, api: httpx.AsyncClient) -> None:
        response = await api.get("/api/v1/auth/me")
        assert response.status_code == 401

    async def test_garbage_token_is_unauthorized(self, api: httpx.AsyncClient) -> None:
        response = await api.get(
            "/api/v1/auth/me", headers={"Authorization": "Bearer not-a-real-token"}
        )
        assert response.status_code == 401


class TestRefreshRotation:
    async def test_refresh_returns_a_new_access_token(self, api: httpx.AsyncClient) -> None:
        await api.post("/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD})
        login = await api.post("/api/v1/auth/login", json={"email": EMAIL, "password": PASSWORD})

        refreshed = await api.post("/api/v1/auth/refresh")
        assert refreshed.status_code == 200
        assert refreshed.json()["access_token"] != login.json()["access_token"]

    async def test_no_cookie_is_unauthorized(self, api: httpx.AsyncClient) -> None:
        response = await api.post("/api/v1/auth/refresh")
        assert response.status_code == 401

    async def test_reusing_an_already_rotated_token_is_rejected(
        self, api: httpx.AsyncClient
    ) -> None:
        """The reuse-detection half of ADR-0010: presenting a spent refresh token must fail,
        not silently succeed a second time.
        """
        await api.post("/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD})
        await api.post("/api/v1/auth/login", json={"email": EMAIL, "password": PASSWORD})
        original_cookie = api.cookies["tw_refresh"]

        first_refresh = await api.post("/api/v1/auth/refresh")
        assert first_refresh.status_code == 200
        rotated_cookie = api.cookies["tw_refresh"]
        assert rotated_cookie != original_cookie

        # Present the now-spent original token again (a stolen-and-replayed scenario).
        replay = await api.post(
            "/api/v1/auth/refresh", headers=_refresh_cookie_header(original_cookie)
        )
        assert replay.status_code == 401

    async def test_reuse_also_revokes_the_legitimate_rotated_token(
        self, api: httpx.AsyncClient
    ) -> None:
        """Reuse detection revokes the *whole family* — including the token that replaced
        the stolen one — so the legitimate client is forced to log in again too, which is
        the correct outcome once a family is known to be compromised.
        """
        await api.post("/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD})
        await api.post("/api/v1/auth/login", json={"email": EMAIL, "password": PASSWORD})
        original_cookie = api.cookies["tw_refresh"]

        await api.post("/api/v1/auth/refresh")
        rotated_cookie = api.cookies["tw_refresh"]

        await api.post("/api/v1/auth/refresh", headers=_refresh_cookie_header(original_cookie))

        still_using_rotated = await api.post(
            "/api/v1/auth/refresh", headers=_refresh_cookie_header(rotated_cookie)
        )
        assert still_using_rotated.status_code == 401


class TestLogout:
    async def test_denylists_the_current_access_token(self, api: httpx.AsyncClient) -> None:
        await api.post("/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD})
        login = await api.post("/api/v1/auth/login", json={"email": EMAIL, "password": PASSWORD})
        token = login.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        before = await api.get("/api/v1/auth/me", headers=headers)
        assert before.status_code == 200

        logout = await api.post("/api/v1/auth/logout", json={}, headers=headers)
        assert logout.status_code == 204

        after = await api.get("/api/v1/auth/me", headers=headers)
        assert after.status_code == 401

    async def test_revokes_the_refresh_token(self, api: httpx.AsyncClient) -> None:
        await api.post("/api/v1/auth/register", json={"email": EMAIL, "password": PASSWORD})
        login = await api.post("/api/v1/auth/login", json={"email": EMAIL, "password": PASSWORD})
        token = login.json()["access_token"]

        await api.post("/api/v1/auth/logout", json={}, headers={"Authorization": f"Bearer {token}"})

        refreshed = await api.post("/api/v1/auth/refresh")
        assert refreshed.status_code == 401


class TestRateLimiting:
    async def test_exceeding_the_per_ip_limit_returns_429_with_retry_after(
        self, session: AsyncSession, settings: Settings
    ) -> None:
        """`ARCHITECTURE.md` §9.4: rate limiting per-IP on unauthenticated routes. A fresh
        app with a deliberately tiny cap, so the test does not need sixty real requests.
        """
        tight = settings.model_copy(
            update={"security": settings.security.model_copy(update={"rate_limit_anon_per_min": 2})}
        )
        clear_readiness_checks()
        app = create_app(tight)

        async def _session_override() -> AsyncSession:
            return session

        app.dependency_overrides[get_session] = _session_override
        cache = RedisCache(tight.redis)
        await cache.connect()
        await _sweep(cache)
        app.state.cache = cache

        transport = httpx.ASGITransport(app=app)
        try:
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as api:
                for _ in range(2):
                    response = await api.post(
                        "/api/v1/auth/login", json={"email": EMAIL, "password": "nope"}
                    )
                    assert response.status_code == 401

                limited = await api.post(
                    "/api/v1/auth/login", json={"email": EMAIL, "password": "nope"}
                )
                assert limited.status_code == 429
                assert "Retry-After" in limited.headers
        finally:
            await _sweep(cache)
            await cache.disconnect()
            app.dependency_overrides.clear()
            clear_readiness_checks()
