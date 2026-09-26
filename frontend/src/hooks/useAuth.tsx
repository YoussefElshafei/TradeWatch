/**
 * Session state (Phase 5, ADR-0010): the current user, an in-memory access token, and
 * login/register/logout — one `AuthProvider` wrapping the app, read anywhere via `useAuth`.
 *
 * **No state library.** Same reasoning as `useAsync.ts` (decision D13): this holds one
 * small object and refetches nothing on a schedule a library would manage better than a
 * `setTimeout`, so a library would be answering a question nobody has asked yet.
 *
 * **The access token lives only in this component's state — never `localStorage`.** On
 * mount, this calls `/api/v1/auth/refresh`, which succeeds or fails based on the httpOnly
 * refresh cookie the browser already holds; that is what makes a page reload feel like a
 * persisted session without the access token itself ever touching persistent storage.
 *
 * **Proactive refresh, not reactive.** A timer fires shortly before `expires_in` elapses and
 * quietly rotates the token in the background. The alternative — wait for a 401, then retry
 * — would work too, but there is exactly one endpoint in this phase that needs a valid token
 * (`/ws/market`, and `/api/v1/auth/me`), and a socket does not get a second chance to retry a
 * rejected handshake the way an HTTP request does; refreshing ahead of time avoids ever
 * presenting an about-to-expire token to either.
 */

import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';

import {
  login as apiLogin,
  logout as apiLogout,
  register as apiRegister,
  refreshSession,
  type User,
} from '../api/auth';

export type AuthStatus = 'loading' | 'authenticated' | 'unauthenticated';

export interface AuthState {
  status: AuthStatus;
  user: User | null;
  accessToken: string | null;
}

export interface AuthContextValue extends AuthState {
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
}

const AuthContext = createContext<AuthContextValue | null>(null);

//: Refresh a bit before the access token actually expires, so a request already in flight
//: at the boundary never races an expiry the server has already started enforcing.
const REFRESH_MARGIN_SECONDS = 30;
//: A floor under the computed delay — a very short `TW_ACCESS_TOKEN_TTL_MIN` (tests, a
//: misconfigured deployment) must not turn this into a refresh-on-every-render loop.
const MIN_REFRESH_DELAY_SECONDS = 5;

const UNAUTHENTICATED: AuthState = { status: 'unauthenticated', user: null, accessToken: null };

export function AuthProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<AuthState>({ status: 'loading', user: null, accessToken: null });
  const refreshTimer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  // A ref to `scheduleRefresh` itself, reassigned every render. The scheduled callback below
  // reads through this rather than closing over `scheduleRefresh` directly — a function
  // cannot reference its own `const` binding from inside itself before that binding exists,
  // so a *directly* self-recursive `useCallback` is a genuine reference-before-declaration
  // bug, not just a lint nag. By the time the `setTimeout` actually fires, this ref has long
  // since been assigned, so the recursion resolves through it instead.
  const scheduleRefreshRef = useRef<(expiresInSeconds: number) => void>(undefined);

  const scheduleRefresh = useCallback((expiresInSeconds: number) => {
    clearTimeout(refreshTimer.current);
    const delaySeconds = Math.max(expiresInSeconds - REFRESH_MARGIN_SECONDS, MIN_REFRESH_DELAY_SECONDS);
    refreshTimer.current = setTimeout(() => {
      refreshSession()
        .then((tokens) => {
          setState({ status: 'authenticated', user: tokens.user, accessToken: tokens.access_token });
          scheduleRefreshRef.current?.(tokens.expires_in);
        })
        .catch(() => {
          setState(UNAUTHENTICATED);
        });
    }, delaySeconds * 1000);
  }, []);
  // Refs are not to be written during render (React 19's `react-hooks/refs` rule) — this
  // keeps `scheduleRefreshRef` current after every commit instead.
  useEffect(() => {
    scheduleRefreshRef.current = scheduleRefresh;
  }, [scheduleRefresh]);

  useEffect(() => {
    let cancelled = false;
    refreshSession()
      .then((tokens) => {
        if (cancelled) return;
        setState({ status: 'authenticated', user: tokens.user, accessToken: tokens.access_token });
        scheduleRefresh(tokens.expires_in);
      })
      .catch(() => {
        if (!cancelled) setState(UNAUTHENTICATED);
      });
    return () => {
      cancelled = true;
      clearTimeout(refreshTimer.current);
    };
  }, [scheduleRefresh]);

  const login = useCallback(
    async (email: string, password: string) => {
      const tokens = await apiLogin(email, password);
      setState({ status: 'authenticated', user: tokens.user, accessToken: tokens.access_token });
      scheduleRefresh(tokens.expires_in);
    },
    [scheduleRefresh],
  );

  const register = useCallback(
    async (email: string, password: string) => {
      await apiRegister(email, password);
      // Registration issues no tokens of its own (`api/routes/auth.py` keeps the two
      // concerns separate). Logging in right after is what turns "account created" into
      // "signed in", which is what filling in a registration form actually implies.
      await login(email, password);
    },
    [login],
  );

  const logout = useCallback(async () => {
    if (state.accessToken) {
      try {
        await apiLogout(state.accessToken);
      } catch {
        // Already signed out server-side, or the network dropped — either way, the local
        // state cleared below is what actually signs this tab out. A failed server call
        // must not trap someone in a "logging out forever" state.
      }
    }
    clearTimeout(refreshTimer.current);
    setState(UNAUTHENTICATED);
  }, [state.accessToken]);

  const value = useMemo<AuthContextValue>(
    () => ({ ...state, login, register, logout }),
    [state, login, register, logout],
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

// Context, Provider and hook belong together here — splitting them across files only to
// satisfy the fast-refresh heuristic (which wants a component-only module) is not worth it
// for three pieces this tightly coupled.
// eslint-disable-next-line react-refresh/only-export-components
export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
