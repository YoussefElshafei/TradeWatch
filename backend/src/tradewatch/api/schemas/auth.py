"""Request and response schemas for the auth endpoints (ADR-0010)."""

from __future__ import annotations

import uuid

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from tradewatch.domain.enums import UserRole

__all__ = [
    "LoginRequest",
    "LogoutRequest",
    "RegisterRequest",
    "TokenResponse",
    "UserResponse",
]


class RegisterRequest(BaseModel):
    """`EmailStr` rejects a malformed address before it ever reaches `AuthService` — the
    length and strength of `password` are checked there instead, because the minimum length
    is `TW_PASSWORD_MIN_LENGTH`, a runtime setting a static schema field cannot see.
    """

    email: EmailStr
    password: str = Field(min_length=1, max_length=256)


class LoginRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=1, max_length=256)


class LogoutRequest(BaseModel):
    #: Log out of every session, not just this one. Defaults to false: a route that logged
    #: every device out on a plain `POST /logout` with no body would surprise every client
    #: that calls it expecting the current tab to end.
    everywhere: bool = False


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    email: str
    role: UserRole
    status: str


class TokenResponse(BaseModel):
    """The refresh token is never in here — it leaves only as an httpOnly cookie
    (`api/routes/auth.py`), so a token-stealing XSS payload running in the page can read
    this response and still not get the one credential that outlives a page reload.
    """

    access_token: str
    token_type: str = "bearer"  # noqa: S105 — the OAuth2 token type, not a credential
    expires_in: int = Field(description="Seconds until `access_token` expires.")
    user: UserResponse
