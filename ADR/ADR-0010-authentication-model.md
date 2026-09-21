# ADR-0010: JWT access tokens with rotating refresh tokens

- **Status:** Accepted
- **Date:** 2026-09-18
- **Phase:** 5

## Context

From Phase 6 onward, TradeWatch stores per-user data: strategies, portfolios, orders, fills,
and alerts. That data must be isolated between users. The system also needs to authenticate
long-lived WebSocket connections, not only HTTP requests, and the API is intended to run as
more than one replica.

The classic trade-off applies. Stateless tokens scale and avoid a lookup per request, but
cannot be revoked before they expire. Server-side sessions revoke instantly but require
shared state on every request.

Separately, the most likely real vulnerability in an application of this shape is not a
broken token scheme — it is IDOR: an endpoint that fetches a resource by id and forgets to
check who owns it. Any auth decision here has to address that, or it addresses the wrong
risk.

## Decision

**Short-lived JWT access tokens plus rotating, server-side refresh tokens.**

- **Access token:** JWT, `TW_ACCESS_TOKEN_TTL_MIN` default 15 minutes, HS256 initially
  (asymmetric later if a second verifier ever exists). Carries `sub`, `role`, `jti`, `exp`,
  `iat`. Verified statelessly on every request.
- **Refresh token:** opaque random value, stored **hashed** in `refresh_tokens`, default TTL
  30 days. Every use **rotates** it: the old one is revoked and a new one issued.
- **Reuse detection:** presenting an already-used refresh token means it was stolen or
  replayed, so the entire token family is revoked and the user must log in again.
- **Emergency revocation:** a short Redis denylist keyed by `jti`, with a TTL equal to the
  remaining access-token lifetime. This bounds the revocation gap to the access-token TTL
  without a database lookup per request.
- **Passwords:** Argon2id (bcrypt an acceptable fallback). Never logged, never returned,
  never reversible. Login responses are uniform whether the account exists or not.
- **WebSocket auth:** the token is validated during the handshake, and the connection is
  **closed when the token expires**. A socket must not outlive its credential — this is the
  detail most implementations get wrong, because the check happens only at connect.
- **Authorization:**
  - Roles `viewer` / `trader` / `admin`, checked by a FastAPI dependency.
  - **Ownership is enforced in the repository layer, not in route handlers.** Every query for
    a user-owned resource takes an actor and filters by `user_id`. A handler that forgets to
    check cannot leak, because the repository will not return another user's rows. Route-level
    checks are defence in depth, not the primary mechanism.
  - A cross-user access test (user B gets 404, not 403, for user A's resource) exists for
    every owner-scoped endpoint and is a hard CI gate.
- Token storage in the browser, per-user resource limits, and rate limiting (per-IP
  anonymous, per-user authenticated) are part of this boundary; see `ARCHITECTURE.md` §9.

## Alternatives considered

**Server-side sessions (opaque token, session table or Redis).** Instant revocation, no
token-expiry subtleties, and simpler to reason about — genuinely the safer default for a
monolith. Rejected as the primary mechanism because it requires a shared-state lookup on
every request including every WebSocket message path, and revocation immediacy is not worth
that here. Note that the refresh half of this design *is* server-side sessions; the hybrid
takes the revocability where it matters and the statelessness where it is cheap.

**Long-lived JWTs with no refresh token.** Simplest possible. Rejected: a stolen token is
valid for its full lifetime with no way to revoke it, and lengthening the lifetime to avoid
constant re-login makes that worse. This is the standard JWT mistake.

**OAuth2 / OIDC via a managed provider (Cognito, Auth0, Clerk).** Removes an entire class of
security responsibility, gives MFA and social login for free, and is what a production
system should usually do. Deferred, not rejected (D9): implementing this correctly
demonstrates the security engineering that is part of this project's point, and the provider
can be adopted later behind the same dependency. If SSO or MFA becomes a requirement, this
becomes the right answer immediately.

**Session cookies with CSRF tokens.** Well-trodden, and browser-native. Rejected: awkward for
the WebSocket path, and it constrains the frontend deployment to a same-site arrangement that
the CDN-hosted static frontend in Phase 12 would complicate.

## Consequences

**Positive.** Request verification is a signature check, so the API scales horizontally
without shared session state. Refresh rotation with reuse detection turns a stolen refresh
token into a detectable, contained event. Repository-layer ownership makes the most likely
vulnerability class structurally hard rather than a matter of remembering.

**Negative.** A compromised access token remains valid for up to its TTL — bounded to 15
minutes, and the Redis denylist covers the emergency case. Refresh rotation must handle
concurrent requests from the same client carefully, or a race between two tabs will revoke a
legitimate family; that needs a short grace window and a test. The signing key becomes a
critical secret whose rotation procedure must be documented.

**Neutral.** `refresh_tokens` and `audit_log` add operational tables that need a cleanup job
for expired rows.

## Revisit when

- SSO, MFA, or social login is required — that is the trigger for D9.
- A second service needs to verify tokens independently, which is the trigger for moving
  from HS256 to an asymmetric algorithm with a published JWKS.
- The application is exposed to real users on the public internet, which warrants a full
  threat model review rather than an ADR amendment.
