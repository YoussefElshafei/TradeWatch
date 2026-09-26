"""Phase 5: registration, login, refresh rotation, logout, and password hashing (ADR-0010)."""

from tradewatch.application.auth.passwords import hash_password, needs_rehash, verify_password
from tradewatch.application.auth.service import (
    AuthService,
    EmailAlreadyRegisteredError,
    InvalidCredentialsError,
    IssuedTokens,
    RefreshTokenInvalidError,
    RefreshTokenReusedError,
    RegistrationDisabledError,
    WeakPasswordError,
    revoked_key,
)
from tradewatch.application.auth.tokens import (
    AccessTokenClaims,
    ExpiredAccessTokenError,
    InvalidAccessTokenError,
    create_access_token,
    decode_access_token,
    generate_refresh_token,
    hash_refresh_token,
)

__all__ = [
    "AccessTokenClaims",
    "AuthService",
    "EmailAlreadyRegisteredError",
    "ExpiredAccessTokenError",
    "InvalidAccessTokenError",
    "InvalidCredentialsError",
    "IssuedTokens",
    "RefreshTokenInvalidError",
    "RefreshTokenReusedError",
    "RegistrationDisabledError",
    "WeakPasswordError",
    "create_access_token",
    "decode_access_token",
    "generate_refresh_token",
    "hash_password",
    "hash_refresh_token",
    "needs_rehash",
    "revoked_key",
    "verify_password",
]
