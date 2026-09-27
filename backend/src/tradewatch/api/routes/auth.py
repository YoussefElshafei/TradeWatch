"""Auth endpoints: register, login, refresh, logout, me (ADR-0010).

The refresh token never appears in a JSON body. It travels only as an httpOnly cookie —
unreadable to JavaScript, so a token-stealing XSS payload that can read every response this
API sends still cannot get the one credential that survives a page reload. The access token
is the opposite: short-lived on purpose, so it is fine for the frontend to hold it in memory
and attach it as a normal `Authorization: Bearer` header.
"""

from __future__ import annotations

import ipaddress
from typing import Annotated

from fastapi import APIRouter, Cookie, Depends, HTTPException, Request, Response, status

from tradewatch.api.dependencies import CacheDep, CurrentUserDep, SessionDep, SettingsDep
from tradewatch.api.rate_limit import rate_limit_by_ip, rate_limit_by_user
from tradewatch.api.schemas.auth import (
    LoginRequest,
    LogoutRequest,
    RegisterRequest,
    TokenResponse,
    UserResponse,
)
from tradewatch.application.auth import (
    AuthService,
    EmailAlreadyRegisteredError,
    InvalidCredentialsError,
    IssuedTokens,
    RefreshTokenInvalidError,
    RefreshTokenReusedError,
    RegistrationDisabledError,
    WeakPasswordError,
)
from tradewatch.config.settings import Environment, Settings
from tradewatch.infrastructure.database.identity_store import SqlIdentityStore
from tradewatch.infrastructure.repositories import UserRepository
from tradewatch.observability.logging import get_logger

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

log = get_logger(__name__)

#: Sent only to the routes that ever need to read it — narrower than the whole API's cookie
#: jar, and it means a cookie inspector on any other endpoint's response never sees it either.
REFRESH_COOKIE_NAME = "tw_refresh"
REFRESH_COOKIE_PATH = "/api/v1/auth"


def get_auth_service(session: SessionDep, cache: CacheDep, settings: SettingsDep) -> AuthService:
    return AuthService(SqlIdentityStore(session), cache, settings.security)


AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]


def _client_ip(request: Request) -> str | None:
    """`request.client.host` as a real IP, or `None`.

    `refresh_tokens.ip` and `audit_log.ip` are both `INET` columns, and not every value
    Starlette hands back here is one: a Unix-socket peer, a test client (`'testclient'`),
    or some other transport-specific placeholder. Storing an unparseable string would fail
    the whole request over an audit column — a worse outcome than simply not recording an
    IP for the handful of connections that do not have one.
    """
    if request.client is None:
        return None
    try:
        ipaddress.ip_address(request.client.host)
    except ValueError:
        return None
    return request.client.host


def _set_refresh_cookie(response: Response, settings: Settings, tokens: IssuedTokens) -> None:
    response.set_cookie(
        key=REFRESH_COOKIE_NAME,
        value=tokens.refresh_token,
        max_age=settings.security.refresh_token_ttl_days * 24 * 60 * 60,
        path=REFRESH_COOKIE_PATH,
        httponly=True,
        # A dev server over plain http://localhost cannot send a `Secure` cookie back at
        # all — the browser silently drops it — so this only turns on in prod, where the
        # deployment is https by construction (`ARCHITECTURE.md` §13).
        secure=settings.app.env is Environment.PROD,
        # `localhost:5173` and `localhost:8010` are different *origins* but the same *site*
        # (SameSite is defined by registrable domain, not port), so `Lax` already covers
        # local dev. A production split across real domains would need `SameSite=None` plus
        # `Secure` — revisit this the moment the frontend and API stop sharing a domain
        # (`ARCHITECTURE.md` §13's Phase 12 CDN split).
        samesite="lax",
    )


def _clear_refresh_cookie(response: Response) -> None:
    response.delete_cookie(key=REFRESH_COOKIE_NAME, path=REFRESH_COOKIE_PATH)


def _token_response(tokens: IssuedTokens, settings: Settings) -> TokenResponse:
    return TokenResponse(
        access_token=tokens.access_token,
        # The TTL itself, not a delta against "now" — `IssuedTokens` carries an absolute
        # `expires_at` for the cookie's own `Max-Age` math, but what a client wants here is
        # simply "how long is this good for", which is the setting `AuthService` used to
        # mint it, unaffected by clock skew between this process and whoever reads the field.
        expires_in=settings.security.access_token_ttl_min * 60,
        user=UserResponse(
            id=tokens.user.id,
            email=tokens.user.email,
            role=tokens.user.role,
            status=tokens.user.status,
        ),
    )


@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create an account",
    responses={
        403: {"description": "Registration is disabled"},
        409: {"description": "Email already registered"},
        422: {"description": "Password too short"},
    },
    dependencies=[Depends(rate_limit_by_ip("auth-register"))],
)
async def register(body: RegisterRequest, auth: AuthServiceDep) -> UserResponse:
    try:
        user = await auth.register(email=body.email, password=body.password)
    except RegistrationDisabledError as error:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Registration is currently disabled."
        ) from error
    except WeakPasswordError as error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)
        ) from error
    except EmailAlreadyRegisteredError as error:
        # Deliberately *not* uniform with login's failure response: registration is telling
        # the caller about an address, not authenticating a session, so there is no
        # equivalent enumeration risk to defend against here — the caller supplied the
        # email themselves and 409 is the honest answer to "can I use this address".
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail="That email is already registered."
        ) from error
    return UserResponse(id=user.id, email=user.email, role=user.role, status=user.status)


@router.post(
    "/login",
    response_model=TokenResponse,
    summary="Exchange credentials for an access token",
    responses={401: {"description": "Invalid email or password"}},
    dependencies=[Depends(rate_limit_by_ip("auth-login"))],
)
async def login(
    body: LoginRequest,
    request: Request,
    response: Response,
    auth: AuthServiceDep,
    settings: SettingsDep,
) -> TokenResponse:
    try:
        tokens = await auth.authenticate(
            email=body.email,
            password=body.password,
            user_agent=request.headers.get("User-Agent"),
            ip=_client_ip(request),
        )
    except InvalidCredentialsError as error:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password."
        ) from error
    _set_refresh_cookie(response, settings, tokens)
    return _token_response(tokens, settings)


@router.post(
    "/refresh",
    response_model=TokenResponse,
    summary="Rotate the refresh token and issue a new access token",
    responses={401: {"description": "Missing, invalid, expired, or reused refresh token"}},
)
async def refresh(
    request: Request,
    response: Response,
    auth: AuthServiceDep,
    settings: SettingsDep,
    refresh_token: Annotated[str | None, Cookie(alias=REFRESH_COOKIE_NAME)] = None,
) -> TokenResponse:
    if refresh_token is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No refresh token.")
    try:
        tokens = await auth.refresh(
            refresh_token=refresh_token,
            user_agent=request.headers.get("User-Agent"),
            ip=_client_ip(request),
        )
    except RefreshTokenReusedError as error:
        # The family is already revoked inside `AuthService.refresh`; the client's only
        # correct move is to log in again, so the response gives it nothing more specific
        # than "invalid" to act on — and, unlike login, there is no cookie to clear here,
        # because the browser will simply stop being able to use it.
        _clear_refresh_cookie(response)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token is invalid."
        ) from error
    except RefreshTokenInvalidError as error:
        _clear_refresh_cookie(response)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Refresh token is invalid."
        ) from error
    _set_refresh_cookie(response, settings, tokens)
    return _token_response(tokens, settings)


@router.post(
    "/logout",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="End the current session, or every session",
)
async def logout(
    body: LogoutRequest,
    response: Response,
    current_user: CurrentUserDep,
    auth: AuthServiceDep,
    _rate_limited: Annotated[None, Depends(rate_limit_by_user("auth-logout"))],
    refresh_token: Annotated[str | None, Cookie(alias=REFRESH_COOKIE_NAME)] = None,
) -> None:
    await auth.logout(
        access_claims=current_user, refresh_token=refresh_token, everywhere=body.everywhere
    )
    _clear_refresh_cookie(response)


@router.get(
    "/me",
    response_model=UserResponse,
    summary="The signed-in user's own profile",
)
async def me(
    current_user: CurrentUserDep,
    session: SessionDep,
    _rate_limited: Annotated[None, Depends(rate_limit_by_user("auth-me"))],
) -> UserResponse:
    user = await UserRepository(session).get(current_user.user_id)
    if user is None:
        # The token verified, but the account behind it is gone — deleted between the token
        # being issued and this request. Same status a forged token gets: nothing here
        # distinguishes "your account was deleted" from "your token was never valid".
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Account not found.")
    return UserResponse(id=user.id, email=user.email, role=user.role, status=user.status)


__all__ = ["router"]
