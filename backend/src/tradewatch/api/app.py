"""FastAPI application factory.

Phase 0 wires the things every later route will rely on — settings, logging, request ids,
CORS, and the error envelope — and exposes only the operational endpoints. There is no
business surface yet, deliberately: `DEVELOPMENT_PLAN.md` Phase 0 exists to make the next
phase boring.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from tradewatch import __version__
from tradewatch.api.errors import error_response, install_exception_handlers
from tradewatch.api.routes import auth, health, market, websocket
from tradewatch.api.routes.health import clear_readiness_checks, register_readiness_check
from tradewatch.api.routes.websocket import ConnectionManager
from tradewatch.config.env import load_env_file
from tradewatch.config.settings import Settings, get_settings
from tradewatch.infrastructure.cache import RedisCache
from tradewatch.infrastructure.database.session import Database
from tradewatch.observability.logging import (
    bind_request_context,
    clear_request_context,
    configure_logging,
    get_logger,
    new_request_id,
)

REQUEST_ID_HEADER = "X-Request-ID"

log = get_logger(__name__)

#: The three FastAPI's own docs UI needs a CDN for (`ARCHITECTURE.md` §9.4 names CSP as one
#: of the headers "set at the edge", but the edge also serves `/docs` and `/redoc`, which
#: load Swagger/ReDoc's JS and CSS from jsDelivr — a CSP tight enough to be worth having on
#: the JSON API would break them). Every other response gets the strict policy below instead.
_DOCS_PATHS = frozenset({"/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json"})


def _security_headers(request: Request) -> dict[str, str]:
    """`ARCHITECTURE.md` §9.4: HSTS, CSP, `X-Content-Type-Options`, frame-deny.

    A JSON API serves no scripts, styles, or frames of its own, so `default-src 'none'` is
    not a compromise here the way it would be for a page-serving app — it is simply correct.
    `Strict-Transport-Security` sent over plain HTTP is inert (browsers only honour it on a
    response already received over TLS), so there is no local-dev-vs-prod branch to get
    wrong by forgetting it.
    """
    headers = {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "Strict-Transport-Security": "max-age=63072000; includeSubDomains",
        "Referrer-Policy": "no-referrer",
    }
    if request.url.path not in _DOCS_PATHS:
        headers["Content-Security-Policy"] = "default-src 'none'; frame-ancestors 'none'"
    return headers


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the application.

    Args:
        settings: Configuration to use. Defaults to the process-wide settings; tests pass
            their own so they never depend on the developer's environment.
    """
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        log.info(
            "api.starting",
            version=__version__,
            environment=settings.app.env.value,
            provider=settings.provider.provider.value,
            cache_enabled=settings.redis.cache_enabled,
        )
        database = Database(settings.database)
        await database.connect()
        app.state.database = database
        # `/ready` now actually checks something. Note that `/health` deliberately does
        # not: a liveness probe that fails during a database outage turns it into a
        # restart loop (see routes/health.py).
        register_readiness_check("database", database.check)

        cache = RedisCache(settings.redis)
        await cache.connect()
        app.state.cache = cache
        # critical=False: a Redis outage must degrade (cache misses fall through to
        # PostgreSQL, live updates pause), never flip `/ready` to 503 — that would turn a
        # non-critical dependency into an outage, exactly what failure F9 forbids.
        register_readiness_check("redis", cache.check, critical=False)

        ws_manager = ConnectionManager(cache)
        ws_manager.start()
        app.state.ws_manager = ws_manager

        try:
            yield
        finally:
            clear_readiness_checks()
            await ws_manager.stop()
            await cache.disconnect()
            await database.disconnect()
            log.info("api.stopped")

    app = FastAPI(
        title="TradeWatch API",
        version=__version__,
        summary="Market intelligence and paper-trading platform",
        description=(
            "Simulated trading only — no brokerage connectivity and no real money. "
            "The anomaly endpoints report statistically unusual market activity, not "
            "market manipulation or financial crime (ADR-0008)."
        ),
        docs_url="/docs",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )
    app.state.settings = settings

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.security.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", REQUEST_ID_HEADER],
        expose_headers=[REQUEST_ID_HEADER],
    )

    @app.middleware("http")
    async def request_context(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        """Give every request an id, log its outcome, and never leak an internal error."""
        request_id = request.headers.get(REQUEST_ID_HEADER) or new_request_id()
        bind_request_context(request_id=request_id)
        request.state.request_id = request_id
        try:
            try:
                response = await call_next(request)
            except Exception:
                # The handlers installed below cover exceptions raised inside routing;
                # this catches anything escaping middleware further down the stack.
                log.exception("request.failed", method=request.method, path=request.url.path)
                response = JSONResponse(
                    status_code=500,
                    content=error_response(
                        code="internal_error",
                        message="An unexpected error occurred.",
                        request_id=request_id,
                    ),
                )
            response.headers[REQUEST_ID_HEADER] = request_id
            for name, value in _security_headers(request).items():
                response.headers[name] = value
            return response
        finally:
            clear_request_context()

    install_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(auth.router)
    app.include_router(market.router)
    app.include_router(websocket.router)
    return app


def bootstrap() -> FastAPI:
    """Entry point used by uvicorn: configure logging, then build the app.

    Configuration failures surface here, at startup, with the message
    :func:`tradewatch.config.load_settings` produces — not later, as an obscure error in a
    request.
    """
    load_env_file()
    settings = get_settings()
    configure_logging(level=settings.app.log_level, log_format=settings.app.log_format)
    return create_app(settings)
