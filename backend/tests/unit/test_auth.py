"""Password hashing, tokens, and `AuthService` (ADR-0010) — no database, no Redis.

`AuthService` is exercised against `FakeIdentityStore` and `FakeCache`, in-memory
reimplementations of the two ports it depends on. That is what makes rotation and reuse
detection — the two pieces of this phase worth getting exactly right — cheap to test against
every edge case, the same reasoning `test_ingestion.py` uses for the market data pipeline.
"""

from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import replace

import pytest
from pydantic import SecretStr

from tradewatch.application.auth import (
    AuthService,
    EmailAlreadyRegisteredError,
    ExpiredAccessTokenError,
    InvalidAccessTokenError,
    InvalidCredentialsError,
    RefreshTokenInvalidError,
    RefreshTokenReusedError,
    RegistrationDisabledError,
    WeakPasswordError,
    create_access_token,
    decode_access_token,
    generate_refresh_token,
    hash_password,
    hash_refresh_token,
    needs_rehash,
    revoked_key,
    verify_password,
)
from tradewatch.application.ports.identity import RefreshTokenRecord, UserRecord
from tradewatch.config.settings import SecuritySettings
from tradewatch.domain.enums import UserRole

pytestmark = pytest.mark.unit

SECRET = "unit-test-secret-not-used-anywhere-real-0123456789"

#: Real current time, not a fixed historical literal. Every access token minted with this
#: as its clock still has to survive `decode_access_token`, which checks `exp` against the
#: *actual* wall clock — PyJWT has no injectable-clock hook — so a frozen moment from the
#: past would make every token in this file look expired the instant it was decoded.
#: Truncated to whole seconds: `iat`/`exp` are integer Unix timestamps in the token itself,
#: so a `MOMENT` with microseconds would never round-trip back out equal to what went in.
MOMENT = dt.datetime.now(dt.UTC).replace(microsecond=0)


class TestPasswords:
    def test_a_correct_password_verifies(self) -> None:
        hashed = hash_password("correct horse battery staple")
        assert verify_password("correct horse battery staple", hashed)

    def test_a_wrong_password_does_not_verify(self) -> None:
        hashed = hash_password("correct horse battery staple")
        assert not verify_password("wrong", hashed)

    def test_a_malformed_hash_fails_closed_rather_than_raising(self) -> None:
        assert not verify_password("anything", "not-an-argon2-hash")

    def test_two_hashes_of_the_same_password_differ(self) -> None:
        """A random salt per hash — otherwise identical passwords would be identical rows,
        which is exactly what a salt exists to prevent.
        """
        assert hash_password("same password") != hash_password("same password")

    def test_a_freshly_hashed_password_does_not_need_rehashing(self) -> None:
        assert not needs_rehash(hash_password("correct horse battery staple"))


class TestAccessTokens:
    def test_round_trips_the_claims_it_was_given(self) -> None:
        user_id = uuid.uuid4()
        token = create_access_token(
            user_id=user_id, role=UserRole.TRADER, secret=SECRET, ttl_minutes=15, now=MOMENT
        )
        claims = decode_access_token(token, secret=SECRET)
        assert claims.user_id == user_id
        assert claims.role is UserRole.TRADER
        assert claims.issued_at == MOMENT
        assert claims.expires_at == MOMENT + dt.timedelta(minutes=15)

    def test_an_expired_token_is_rejected(self) -> None:
        token = create_access_token(
            user_id=uuid.uuid4(),
            role=UserRole.VIEWER,
            secret=SECRET,
            ttl_minutes=15,
            now=MOMENT - dt.timedelta(minutes=16),
        )
        with pytest.raises(ExpiredAccessTokenError):
            decode_access_token(token, secret=SECRET)

    def test_a_token_signed_with_a_different_secret_is_rejected(self) -> None:
        token = create_access_token(
            user_id=uuid.uuid4(), role=UserRole.VIEWER, secret=SECRET, ttl_minutes=15
        )
        with pytest.raises(InvalidAccessTokenError):
            decode_access_token(token, secret="a-completely-different-secret-0123456789")

    def test_garbage_is_rejected(self) -> None:
        with pytest.raises(InvalidAccessTokenError):
            decode_access_token("not.a.jwt", secret=SECRET)

    def test_each_token_gets_a_distinct_jti(self) -> None:
        """`jti` is what the emergency denylist keys on — two tokens sharing one would let
        revoking one revoke the other.
        """
        first = create_access_token(
            user_id=uuid.uuid4(), role=UserRole.VIEWER, secret=SECRET, ttl_minutes=15
        )
        second = create_access_token(
            user_id=uuid.uuid4(), role=UserRole.VIEWER, secret=SECRET, ttl_minutes=15
        )
        first_jti = decode_access_token(first, secret=SECRET).jti
        second_jti = decode_access_token(second, secret=SECRET).jti
        assert first_jti != second_jti


class TestRefreshTokens:
    def test_two_generated_tokens_differ(self) -> None:
        assert generate_refresh_token() != generate_refresh_token()

    def test_hashing_is_deterministic(self) -> None:
        token = generate_refresh_token()
        assert hash_refresh_token(token) == hash_refresh_token(token)

    def test_different_tokens_hash_differently(self) -> None:
        assert hash_refresh_token(generate_refresh_token()) != hash_refresh_token(
            generate_refresh_token()
        )


# --- AuthService, against fakes ---------------------------------------------------------


class FakeCache:
    """Enough of the `Cache` port for `AuthService`'s denylist — a plain dict."""

    def __init__(self) -> None:
        self.store: dict[str, object] = {}

    async def get_json(self, key: str) -> object | None:
        return self.store.get(key)

    async def set_json(self, key: str, value: object, *, ttl_seconds: int) -> None:
        self.store[key] = value

    async def delete(self, *keys: str) -> None:
        for key in keys:
            self.store.pop(key, None)


class FakeIdentityStore:
    """Enough of the `IdentityStore` port to exercise `AuthService` end to end."""

    def __init__(self) -> None:
        self.users: dict[uuid.UUID, UserRecord] = {}
        self.tokens: dict[str, RefreshTokenRecord] = {}
        self.audit: list[tuple[str, str, uuid.UUID | None]] = []

    async def get_user_by_email(self, email: str) -> UserRecord | None:
        return next((u for u in self.users.values() if u.email.lower() == email.lower()), None)

    async def get_user_by_id(self, user_id: uuid.UUID) -> UserRecord | None:
        return self.users.get(user_id)

    async def create_user(self, *, email: str, password_hash: str, role: UserRole) -> UserRecord:
        if await self.get_user_by_email(email) is not None:
            raise EmailAlreadyRegisteredError(email)
        user = UserRecord(
            id=uuid.uuid4(), email=email, password_hash=password_hash, role=role, status="active"
        )
        self.users[user.id] = user
        return user

    async def record_login(self, user_id: uuid.UUID, *, at: dt.datetime) -> None:
        return None

    async def update_password_hash(self, user_id: uuid.UUID, password_hash: str) -> None:
        self.users[user_id] = replace(self.users[user_id], password_hash=password_hash)

    async def get_refresh_token(self, token_hash: str) -> RefreshTokenRecord | None:
        return self.tokens.get(token_hash)

    async def create_refresh_token(
        self, *, user_id, token_hash, family_id, expires_at, user_agent, ip
    ) -> None:
        self.tokens[token_hash] = RefreshTokenRecord(
            user_id=user_id, family_id=family_id, expires_at=expires_at, revoked_at=None
        )

    async def revoke_refresh_token(self, token_hash: str, *, at: dt.datetime) -> None:
        record = self.tokens.get(token_hash)
        if record is not None:
            self.tokens[token_hash] = replace(record, revoked_at=at)

    async def revoke_refresh_token_family(self, family_id: uuid.UUID, *, at: dt.datetime) -> None:
        for token_hash, record in list(self.tokens.items()):
            if record.family_id == family_id and record.revoked_at is None:
                self.tokens[token_hash] = replace(record, revoked_at=at)

    async def revoke_all_refresh_tokens(self, user_id: uuid.UUID, *, at: dt.datetime) -> None:
        for token_hash, record in list(self.tokens.items()):
            if record.user_id == user_id and record.revoked_at is None:
                self.tokens[token_hash] = replace(record, revoked_at=at)

    async def record_audit_event(
        self, *, action, resource_type, user_id=None, ip=None, details=None
    ) -> None:
        self.audit.append((action, resource_type, user_id))


def _security_settings(**overrides: object) -> SecuritySettings:
    defaults: dict[str, object] = {
        "jwt_secret": SecretStr(SECRET),
        "password_min_length": 12,
        "registration_enabled": True,
    }
    return SecuritySettings(**(defaults | overrides))  # type: ignore[arg-type]


@pytest.fixture
def store() -> FakeIdentityStore:
    return FakeIdentityStore()


@pytest.fixture
def cache() -> FakeCache:
    return FakeCache()


@pytest.fixture
def service(store: FakeIdentityStore, cache: FakeCache) -> AuthService:
    return AuthService(store, cache, _security_settings(), clock=lambda: MOMENT)


class TestRegister:
    async def test_creates_a_viewer_by_default(self, service: AuthService) -> None:
        """Every new account starts least-privileged; an admin is promoted, never
        self-registered (`ARCHITECTURE.md` §9.3).
        """
        user = await service.register(email="new@example.com", password="a very good password")
        assert user.role is UserRole.VIEWER

    async def test_rejects_a_duplicate_email(self, service: AuthService) -> None:
        await service.register(email="dup@example.com", password="a very good password")
        with pytest.raises(EmailAlreadyRegisteredError):
            await service.register(email="dup@example.com", password="another good password")

    async def test_rejects_a_password_below_the_configured_minimum(
        self, store: FakeIdentityStore, cache: FakeCache
    ) -> None:
        service = AuthService(
            store, cache, _security_settings(password_min_length=20), clock=lambda: MOMENT
        )
        with pytest.raises(WeakPasswordError):
            await service.register(email="short@example.com", password="too short")

    async def test_refuses_when_registration_is_disabled(
        self, store: FakeIdentityStore, cache: FakeCache
    ) -> None:
        service = AuthService(
            store, cache, _security_settings(registration_enabled=False), clock=lambda: MOMENT
        )
        with pytest.raises(RegistrationDisabledError):
            await service.register(email="new@example.com", password="a very good password")


class TestAuthenticate:
    async def test_correct_credentials_issue_tokens(self, service: AuthService) -> None:
        await service.register(email="user@example.com", password="a very good password")
        tokens = await service.authenticate(
            email="user@example.com", password="a very good password", user_agent=None, ip=None
        )
        assert tokens.user.email == "user@example.com"
        assert decode_access_token(tokens.access_token, secret=SECRET).user_id == tokens.user.id

    async def test_wrong_password_is_rejected(self, service: AuthService) -> None:
        await service.register(email="user@example.com", password="a very good password")
        with pytest.raises(InvalidCredentialsError):
            await service.authenticate(
                email="user@example.com", password="wrong", user_agent=None, ip=None
            )

    async def test_unknown_email_is_rejected_the_same_way(self, service: AuthService) -> None:
        with pytest.raises(InvalidCredentialsError):
            await service.authenticate(
                email="nobody@example.com", password="whatever", user_agent=None, ip=None
            )

    async def test_a_suspended_account_cannot_log_in(
        self, service: AuthService, store: FakeIdentityStore
    ) -> None:
        user = await service.register(
            email="suspended@example.com", password="a very good password"
        )
        store.users[user.id] = replace(store.users[user.id], status="suspended")
        with pytest.raises(InvalidCredentialsError):
            await service.authenticate(
                email="suspended@example.com",
                password="a very good password",
                user_agent=None,
                ip=None,
            )


class TestRefreshRotation:
    async def test_rotation_issues_a_different_refresh_token(self, service: AuthService) -> None:
        await service.register(email="user@example.com", password="a very good password")
        first = await service.authenticate(
            email="user@example.com", password="a very good password", user_agent=None, ip=None
        )
        second = await service.refresh(refresh_token=first.refresh_token, user_agent=None, ip=None)
        assert second.refresh_token != first.refresh_token
        assert second.access_token != first.access_token

    async def test_an_unknown_token_is_rejected(self, service: AuthService) -> None:
        with pytest.raises(RefreshTokenInvalidError):
            await service.refresh(refresh_token="never-issued", user_agent=None, ip=None)

    async def test_reusing_a_spent_token_is_detected_and_the_family_dies(
        self, service: AuthService
    ) -> None:
        await service.register(email="user@example.com", password="a very good password")
        first = await service.authenticate(
            email="user@example.com", password="a very good password", user_agent=None, ip=None
        )
        second = await service.refresh(refresh_token=first.refresh_token, user_agent=None, ip=None)

        with pytest.raises(RefreshTokenReusedError):
            await service.refresh(refresh_token=first.refresh_token, user_agent=None, ip=None)

        # The legitimate replacement is a casualty of revoking the whole compromised family.
        with pytest.raises(RefreshTokenReusedError):
            await service.refresh(refresh_token=second.refresh_token, user_agent=None, ip=None)

    async def test_an_expired_refresh_token_is_rejected(
        self, store: FakeIdentityStore, cache: FakeCache
    ) -> None:
        settings = _security_settings()
        early = AuthService(store, cache, settings, clock=lambda: MOMENT - dt.timedelta(days=31))
        late = AuthService(store, cache, settings, clock=lambda: MOMENT)
        await early.register(email="user@example.com", password="a very good password")
        tokens = await early.authenticate(
            email="user@example.com", password="a very good password", user_agent=None, ip=None
        )
        with pytest.raises(RefreshTokenInvalidError):
            await late.refresh(refresh_token=tokens.refresh_token, user_agent=None, ip=None)


class TestLogout:
    async def test_denylists_the_access_token(self, service: AuthService, cache: FakeCache) -> None:
        await service.register(email="user@example.com", password="a very good password")
        tokens = await service.authenticate(
            email="user@example.com", password="a very good password", user_agent=None, ip=None
        )
        claims = decode_access_token(tokens.access_token, secret=SECRET)

        await service.logout(access_claims=claims, refresh_token=None, everywhere=False)

        assert await cache.get_json(revoked_key(claims.jti)) is not None

    async def test_revokes_the_presented_refresh_token(self, service: AuthService) -> None:
        await service.register(email="user@example.com", password="a very good password")
        tokens = await service.authenticate(
            email="user@example.com", password="a very good password", user_agent=None, ip=None
        )
        claims = decode_access_token(tokens.access_token, secret=SECRET)

        await service.logout(
            access_claims=claims, refresh_token=tokens.refresh_token, everywhere=False
        )

        # A revoked token is indistinguishable, at the next presentation, from a stolen one
        # being replayed — both are "a token whose record says `revoked_at is not None`" —
        # so this correctly comes back as reuse, not a plain "invalid", and re-triggers a
        # (harmless, already-revoked) family sweep.
        with pytest.raises(RefreshTokenReusedError):
            await service.refresh(refresh_token=tokens.refresh_token, user_agent=None, ip=None)

    async def test_everywhere_revokes_a_session_from_a_different_login(
        self, service: AuthService
    ) -> None:
        await service.register(email="user@example.com", password="a very good password")
        session_a = await service.authenticate(
            email="user@example.com", password="a very good password", user_agent=None, ip=None
        )
        session_b = await service.authenticate(
            email="user@example.com", password="a very good password", user_agent=None, ip=None
        )
        claims_a = decode_access_token(session_a.access_token, secret=SECRET)

        await service.logout(access_claims=claims_a, refresh_token=None, everywhere=True)

        # Same reasoning as above: session B's token is now a revoked row too (different
        # family, same user, swept by `revoke_all_refresh_tokens`), so presenting it reports
        # reuse rather than a plain "invalid".
        with pytest.raises(RefreshTokenReusedError):
            await service.refresh(refresh_token=session_b.refresh_token, user_agent=None, ip=None)
