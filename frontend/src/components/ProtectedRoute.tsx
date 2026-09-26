/**
 * Gates a route behind a signed-in session (Phase 5, ADR-0010).
 *
 * `useAuth`'s `status` starts as `'loading'` while the initial `/api/v1/auth/refresh` call
 * (using the httpOnly cookie) is still in flight — rendering nothing conclusive during that
 * window is what stops a page reload from flashing the login form before redirecting straight
 * back to where the person already was.
 */

import { Navigate, Outlet, useLocation } from 'react-router-dom';

import { useAuth } from '../hooks/useAuth';

export function ProtectedRoute() {
  const { status } = useAuth();
  const location = useLocation();

  if (status === 'loading') {
    return null;
  }

  if (status === 'unauthenticated') {
    // `state` carries where the person was headed, so `LoginPage` can send them back there
    // instead of always landing on the dashboard root after signing in.
    return <Navigate to="/login" replace state={{ from: location }} />;
  }

  return <Outlet />;
}
