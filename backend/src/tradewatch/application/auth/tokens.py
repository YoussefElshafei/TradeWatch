"""Access and refresh tokens (ADR-0010).

Two different token shapes for two different jobs, and neither is the other's job:

**Access token.** A JWT, verified statelessly on every request — no database lookup, no
Redis round trip on the common path. `sub`/`role`/`jti`/`iat`/`exp` only: a JWT is not a
place to put anything an attacker would benefit from reading, since it is a signature over
plaintext, not encryption.

**Refresh token.** An opaque, high-entropy random value. It is never decoded, only compared
against the SHA-256 hash stored in `refresh_tokens` — hashing it with Argon2 would apply a
deliberately-slow KDF meant for low-entropy human passwords to a value that already has 256
bits of entropy, which buys nothing and costs a login-refresh round trip real latency.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import secrets
import uuid
from dataclasses import dataclass

import jwt

from tradewatch.domain.enums import UserRole

__all__ = [
    "AccessTokenClaims",
    "ExpiredAccessTokenError",
    "InvalidAccessTokenError",
    "create_access_token",
    "decode_access_token",
    "generate_refresh_token",
    "hash_refresh_token",
]

_ALGORITHM = "HS256"

#: Bytes of entropy in a refresh token before URL-safe base64 encoding. 256 bits, matching
#: what the access token's HS256 signature assumes an attacker cannot brute-force.
_REFRESH_TOKEN_BYTES = 32


class InvalidAccessTokenError(Exception):
    """The token's signature, shape, or claims do not check out."""


class ExpiredAccessTokenError(InvalidAccessTokenError):
    """The signature is valid but `exp` has passed.

    Split from :class:`InvalidAccessTokenError` because a caller (the WebSocket handshake, in
    particular) treats "expired" and "never valid" differently in its log message, even
    though both end a connection or a request the same way.
    """


@dataclass(frozen=True, slots=True)
class AccessTokenClaims:
    """The decoded, verified contents of an access token."""

    user_id: uuid.UUID
    role: UserRole
    jti: uuid.UUID
    issued_at: dt.datetime
    expires_at: dt.datetime


def create_access_token(
    *,
    user_id: uuid.UUID,
    role: UserRole,
    secret: str,
    ttl_minutes: int,
    jti: uuid.UUID | None = None,
    now: dt.datetime | None = None,
) -> str:
    """Issue a signed access token.

    Args:
        jti: Overridable so a caller can correlate the token it just issued with the
            revocation denylist entry it might write later, without decoding the token it
            just built to recover the id.
        now: Overridable so tests get an exact, reproducible `exp` rather than asserting
            against a moving clock.
    """
    issued_at = now or dt.datetime.now(dt.UTC)
    expires_at = issued_at + dt.timedelta(minutes=ttl_minutes)
    token_id = jti or uuid.uuid4()
    payload = {
        "sub": str(user_id),
        "role": role.value,
        "jti": str(token_id),
        "iat": int(issued_at.timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    return jwt.encode(payload, secret, algorithm=_ALGORITHM)


def decode_access_token(token: str, *, secret: str) -> AccessTokenClaims:
    """Verify signature and expiry, and parse the claims a route needs to make a decision.

    Raises:
        ExpiredAccessTokenError: The signature checks out but `exp` has passed.
        InvalidAccessTokenError: Any other reason the token cannot be trusted — bad signature,
            wrong algorithm, missing or malformed claims.
    """
    try:
        payload = jwt.decode(token, secret, algorithms=[_ALGORITHM])
    except jwt.ExpiredSignatureError as error:
        raise ExpiredAccessTokenError("access token has expired") from error
    except jwt.InvalidTokenError as error:
        raise InvalidAccessTokenError(f"access token is invalid: {error}") from error

    try:
        return AccessTokenClaims(
            user_id=uuid.UUID(payload["sub"]),
            role=UserRole(payload["role"]),
            jti=uuid.UUID(payload["jti"]),
            issued_at=dt.datetime.fromtimestamp(payload["iat"], tz=dt.UTC),
            expires_at=dt.datetime.fromtimestamp(payload["exp"], tz=dt.UTC),
        )
    except (KeyError, ValueError) as error:
        # A signature that checks out but wraps claims this version of the app does not
        # recognise — a token signed by a future or foreign issuer with the same secret.
        raise InvalidAccessTokenError(f"access token claims are malformed: {error}") from error


def generate_refresh_token() -> str:
    """A fresh, unguessable opaque token. Return this to the client; store only its hash."""
    return secrets.token_urlsafe(_REFRESH_TOKEN_BYTES)


def hash_refresh_token(token: str) -> str:
    """The value actually stored in `refresh_tokens.token_hash`.

    Plain SHA-256, not a per-token salt: the token itself already has 256 bits of entropy,
    so a salt defends against a threat (rainbow tables over a small input space) that does
    not apply here, and a lookup by hash needs the hash to be a pure function of the token.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()
