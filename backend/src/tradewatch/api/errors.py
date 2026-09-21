"""One error shape for the whole API.

`ARCHITECTURE.md` §8.3 specifies a single envelope:
``{"error": {"code", "message", "details", "request_id"}}``. Establishing it in Phase 0
means no route ever invents its own, and the frontend has one thing to parse.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException as StarletteHTTPException

from tradewatch.observability.logging import get_logger

log = get_logger(__name__)


class ErrorDetail(BaseModel):
    code: str = Field(description="Stable, machine-readable identifier for the failure.")
    message: str = Field(description="Human-readable explanation, safe to show a user.")
    details: list[dict[str, Any]] | None = Field(
        default=None, description="Field-level problems, when the failure is a validation error."
    )
    request_id: str | None = Field(
        default=None, description="Correlates this response with server logs."
    )


class ErrorEnvelope(BaseModel):
    """The response body of every failed request."""

    error: ErrorDetail


def error_response(
    *,
    code: str,
    message: str,
    request_id: str | None = None,
    details: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Build the envelope as a plain dict, for use inside middleware."""
    envelope = ErrorEnvelope(
        error=ErrorDetail(code=code, message=message, details=details, request_id=request_id)
    )
    return envelope.model_dump(mode="json", exclude_none=True)


def _request_id(request: Request) -> str | None:
    value = getattr(request.state, "request_id", None)
    return value if isinstance(value, str) else None


_STATUS_CODES: dict[int, str] = {
    400: "bad_request",
    401: "unauthenticated",
    403: "forbidden",
    404: "not_found",
    409: "conflict",
    422: "validation_error",
    429: "rate_limited",
    503: "unavailable",
}


def install_exception_handlers(app: FastAPI) -> None:
    """Register handlers so every failure leaves through the same door."""

    @app.exception_handler(StarletteHTTPException)
    async def _http_exception(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        code = _STATUS_CODES.get(exc.status_code, "error")
        detail = exc.detail if isinstance(exc.detail, str) else code.replace("_", " ")
        return JSONResponse(
            status_code=exc.status_code,
            content=error_response(code=code, message=detail, request_id=_request_id(request)),
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=error_response(
                code="validation_error",
                message="The request did not match the expected schema.",
                request_id=_request_id(request),
                details=[
                    {
                        "location": ".".join(str(part) for part in error["loc"]),
                        "message": error["msg"],
                        "type": error["type"],
                    }
                    for error in exc.errors()
                ],
            ),
        )

    @app.exception_handler(SQLAlchemyError)
    async def _database_error(request: Request, exc: SQLAlchemyError) -> JSONResponse:
        """A database failure is 503, not 500 (failure F8).

        The distinction is not cosmetic: 503 tells a client the request was valid and worth
        retrying, while 500 says the server is broken and retrying is pointless. Reporting
        an outage as 500 makes every client give up on a database that came back a second
        later.

        The exception detail never reaches the response — it carries the statement and its
        bound parameters.
        """
        log.exception("request.database_error", method=request.method, path=request.url.path)
        return JSONResponse(
            status_code=503,
            content=error_response(
                code="unavailable",
                message="The database is currently unavailable. Please retry.",
                request_id=_request_id(request),
            ),
        )

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        # Log the detail, return none of it: an exception message is an information leak
        # waiting to happen.
        log.exception("request.unhandled_error", method=request.method, path=request.url.path)
        return JSONResponse(
            status_code=500,
            content=error_response(
                code="internal_error",
                message="An unexpected error occurred.",
                request_id=_request_id(request),
            ),
        )


__all__ = [
    "ErrorDetail",
    "ErrorEnvelope",
    "HTTPException",
    "error_response",
    "install_exception_handlers",
]
