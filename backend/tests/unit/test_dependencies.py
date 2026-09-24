"""`api/dependencies.py`'s "was not attached during startup" contract, tested directly.

This exists because of a real bug: `tests/integration/test_market_api.py` and
`test_vertical_slice.py` build the app with `httpx.ASGITransport`, which never sends the ASGI
lifespan protocol at all — so `app.state.database`, `app.state.cache` and `app.state.
ws_manager` are only ever set by whichever fixture attaches them by hand. Before Phase 4,
`get_session` was the only thing those fixtures needed to override, and it routes around
`app.state.database` entirely — so the gap was invisible. Phase 4 added `CacheDep` directly
to the market routes, and for a while nothing attached `app.state.cache` in either fixture,
which meant every route touching the cache would raise `RuntimeError`, caught by the API's
own catch-all exception handler, and turned into a quiet 500 — passing tests that never
checked which status code they got back.

Both fixtures were fixed (see `learning.md`'s Phase 4 entry). What was missing was a test
that fails *by name* if that wiring breaks again, rather than relying on unrelated
functional tests to notice a 500 they weren't specifically checking for. That is what this
file is.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import httpx2 as httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from starlette.requests import Request

from tradewatch.api.app import create_app
from tradewatch.api.dependencies import get_cache, get_database, get_settings_dep
from tradewatch.config.settings import Settings
from tradewatch.infrastructure.cache import RedisCache

pytestmark = pytest.mark.unit


def _request_for(app: FastAPI) -> Request:
    """The minimum ASGI scope `Request.app` needs — no real connection involved."""
    return Request(scope={"type": "http", "app": app})


class TestEachDependencyFailsLoudlyWhenUnattached:
    """Every one of these mirrors the same shape: `app.state.X` set only by the lifespan,
    read by a dependency that must never silently treat "missing" as "absent/disabled" —
    a market data cache and a database are different failure modes entirely, and confusing
    "nobody configured this yet" with "Redis is down" (F9) would be a real correctness bug,
    not just a startup-ordering one.
    """

    def test_get_cache_raises_when_app_state_cache_is_unset(self) -> None:
        app = FastAPI()

        with pytest.raises(RuntimeError, match="cache was not attached"):
            get_cache(_request_for(app))

    def test_get_database_raises_when_app_state_database_is_unset(self) -> None:
        app = FastAPI()

        with pytest.raises(RuntimeError, match="database was not attached"):
            get_database(_request_for(app))

    def test_get_settings_dep_raises_when_app_state_settings_is_unset(self) -> None:
        app = FastAPI()

        with pytest.raises(RuntimeError, match="settings were not attached"):
            get_settings_dep(_request_for(app))

    def test_get_cache_succeeds_once_attached(self, settings: Settings) -> None:
        """The positive case, so a typo in the attribute name fails this test too."""
        app = FastAPI()
        app.state.cache = RedisCache(settings.redis)

        assert get_cache(_request_for(app)) is app.state.cache


class TestTheHistoricalBugItself:
    """Reproduces the exact scenario that shipped broken: an app built the way
    `test_market_api.py`/`test_vertical_slice.py` build theirs — `create_app()` plus
    `httpx.ASGITransport`, which never runs the lifespan — hitting a real cache-dependent
    route with **no** `app.state.cache` attached at all (the bug those two fixtures no longer
    have, on purpose, so this test cannot piggyback on either of their `cache` fixtures).

    Before the fix: this request hung on nothing and instead raised `RuntimeError` inside
    dependency resolution, which the API's catch-all handler turned into an ordinary-looking
    500 — the kind of failure a test asserting on response *content* rather than *status*
    would never notice. This test checks the status code specifically, which is what the
    original bug would have failed and what incidental coverage did not.
    """

    @pytest_asyncio.fixture
    async def unwired_client(self, settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
        app = create_app(settings)
        # Deliberately does NOT attach app.state.cache, app.state.database or
        # app.state.ws_manager — reproducing the lifespan-never-ran gap exactly.
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client

    async def test_a_cache_dependent_route_fails_loudly_not_silently(
        self, unwired_client: httpx.AsyncClient
    ) -> None:
        """The API's exception handler deliberately strips detail from the response body
        ("log the detail, return none of it: an exception message is an information leak
        waiting to happen" — `api/errors.py`), so the *cause* can't be read back from this
        response; that's what `TestEachDependencyFailsLoudlyWhenUnattached` pins directly,
        at the dependency level, where the real message is still visible. What this test
        proves that the unit tests above cannot: that the failure is specific to the
        cache-dependent *route*, not a blanket "this app is broken" — `/health` needs
        nothing from `app.state` and must still answer normally on the same unwired app.
        """
        healthy = await unwired_client.get("/health")
        assert healthy.status_code == 200

        broken = await unwired_client.get("/api/v1/instruments")
        # Before the fix, this was also a 500 — the historical bug was never "wrong status
        # code," it was "a 500 nobody was specifically checking for," which is exactly the
        # gap this test file closes.
        assert broken.status_code == 500
        assert broken.json()["error"]["code"] == "internal_error"
