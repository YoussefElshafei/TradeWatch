"""`create_app`'s router wiring, tested directly — not incidentally through route behaviour.

This exists because of a real bug report: the auth endpoints returned a genuine 404 against a
running server. Reading `api/app.py` and `api/__main__.py` showed the router was correctly
included and the actual cause was an unrelated stale process squatting on the API's port — but
answering "does the test suite build the real app, or a hand-rolled one that could silently
diverge from what `bootstrap()` runs?" was worth making impossible to wonder about again.

`api/__main__.py`'s entry point calls `uvicorn.run("tradewatch.api.app:bootstrap", factory=True,
...)`; `bootstrap()` does nothing but load configuration and call `create_app(settings)`. This
file asserts against `create_app(settings)` directly — the one function both the production
entry point and every fixture in this suite call (`tests/conftest.py`'s `api` fixture,
`tests/integration/test_auth_routes.py`'s `api` fixture, ...) — rather than against `bootstrap()`
itself, because `bootstrap()` also reads real environment variables and configures process-wide
logging, neither of which this test should depend on or have side effects from. If a future
change ever calls a *different* app-builder from `bootstrap()`, that divergence is exactly what
`test_dependencies.py`'s docstring calls "a real bug ... invisible" until something asserts on
it by name.

`app.openapi()` rather than walking `app.routes` by hand: FastAPI 0.141 stores an included
router as an internal `_IncludedRouter` wrapper, not flattened `APIRoute` objects, until
something asks for the flattened view — `app.openapi()` is the public, stable way to ask for
exactly that (it is also what actually renders at `/openapi.json`, so this checks precisely
what a real client sees).
"""

from __future__ import annotations

import pytest

from tradewatch.api.app import create_app
from tradewatch.api.routes import websocket
from tradewatch.config.settings import Settings

pytestmark = pytest.mark.unit

#: One path per router in `api/app.py`'s `include_router` calls. Not every path each router
#: defines — enough that a router dropped from `create_app` entirely fails this test, without
#: this file needing an edit every time a route is added to a router it already covers.
_EXPECTED_HTTP_PATHS = {
    "/health",  # health.router
    "/ready",  # health.router
    "/api/v1/auth/register",  # auth.router — the literal regression this file is for
    "/api/v1/auth/login",  # auth.router
    "/api/v1/market/{symbol}/bars",  # market.router
    "/api/v1/strategies",  # strategies.router
    "/api/v1/backtests",  # backtests.router
}


def test_every_router_is_included(settings: Settings) -> None:
    app = create_app(settings)
    paths = set(app.openapi()["paths"])
    missing = _EXPECTED_HTTP_PATHS - paths
    assert not missing, f"create_app never registered: {missing}"


def test_websocket_router_is_included(settings: Settings) -> None:
    """OpenAPI doesn't describe websockets, so there is no `openapi()["paths"]` to check.

    Actually connecting would need a live database and Redis (the real lifespan does, and a
    unit test must not depend on either) — so this checks structurally instead, via FastAPI's
    own record of *which* `APIRouter` a mount came from. `_IncludedRouter` is a private class
    (`fastapi.routing`), but it is the only way `create_app`'s call to
    `app.include_router(websocket.router)` is represented on `app.routes` at all in this
    FastAPI version, which stores an included router as this wrapper rather than flattening it
    eagerly — see this file's module docstring.
    """
    app = create_app(settings)
    included_routers = [getattr(route, "original_router", None) for route in app.routes]
    assert websocket.router in included_routers
