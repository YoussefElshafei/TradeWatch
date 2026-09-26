/**
 * A stubbed, successful `/api/v1/auth/refresh` response (Phase 5).
 *
 * `AuthProvider` calls this endpoint on mount, unconditionally — any test that renders a
 * component reading `useAuth()` (directly, or through `MarketView`/`ProtectedRoute`) needs
 * `fetch` to answer it with something shaped like a real `TokenResponse`, or the session
 * resolves to `'unauthenticated'` and every route behind `ProtectedRoute` redirects away.
 */
export const TOKEN_RESPONSE = {
  access_token: 'test-access-token',
  token_type: 'bearer',
  expires_in: 900,
  user: {
    id: '00000000-0000-7000-8000-000000000000',
    email: 'test-user@example.com',
    role: 'trader',
    status: 'active',
  },
};
