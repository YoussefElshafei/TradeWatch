"""Password hashing (ADR-0010).

Argon2id, via the reference C implementation (`argon2-cffi`), not bcrypt and not a
hand-rolled PBKDF2 call. A password hash is the one place in this codebase where "a
well-known library" is not a suggestion — inventing a KDF, or getting an existing one's
parameters wrong, is how password databases get cracked in bulk.

A hash carries its own parameters (`argon2id$v=19$m=...,t=...,p=...$salt$hash`), so raising
the cost later never invalidates hashes written under the old one — it only requires
rehashing on the next successful login, which is what :func:`needs_rehash` is for.
"""

from __future__ import annotations

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError

__all__ = ["hash_password", "needs_rehash", "verify_password"]

#: One hasher, process-wide. `argon2-cffi`'s defaults already follow the OWASP-recommended
#: parameters (19 MiB, 2 iterations, 1 lane) — overriding them without a measured reason
#: would be tuning a number nobody has a threat model for.
_hasher = PasswordHasher()


def hash_password(password: str) -> str:
    """Hash a plaintext password. Never store, log, or return the input to this function."""
    return _hasher.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    """Check a plaintext password against a stored hash.

    Returns ``False`` for a wrong password *and* for a malformed or foreign hash — a
    corrupt stored value must fail closed, not raise past the login route and turn into a
    500 that tells an attacker the hash shape was unrecognised.
    """
    try:
        return _hasher.verify(password_hash, password)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


def needs_rehash(password_hash: str) -> bool:
    """Whether this hash was made with weaker parameters than the hasher now uses.

    Checked on every successful login (the only moment the plaintext is available) so that
    raising `PasswordHasher`'s parameters in the future upgrades the whole user base
    gradually, one login at a time, with no migration and no forced reset.
    """
    return _hasher.check_needs_rehash(password_hash)
