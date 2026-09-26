/**
 * The auth API client (Phase 5, ADR-0010).
 *
 * Same shape as `market.ts` and `health.ts`: a small, self-contained module with its own
 * `ApiError`, replaced wholesale once generated types arrive from the committed OpenAPI
 * schema (`ARCHITECTURE.md` §8.3) rather than sharing a base client with the others.
 *
 * **`credentials: 'include'` on every call.** The refresh token never appears in a JSON
 * body — it travels only as an httpOnly cookie the browser attaches automatically — so
 * every request here must opt in to sending and receiving cookies across the frontend's and
 * the API's different ports. The backend's CORS config (`allow_credentials=True`,
 * `api/app.py`) is the other half of that agreement.
 *
 * **The access token is never written to `localStorage` or any other persistent browser
 * storage.** It lives only in memory (`useAuth.tsx`), for as long as the tab is open — a
 * token an XSS payload could read from storage after the fact is a token that outlives the
 * page load that leaked it. Losing it on reload is the intended trade-off: `useAuth.tsx`
 * calls `refresh()` on mount, which uses the httpOnly cookie to get a new one silently.
 */

import { apiUrl } from '../config';

export type UserRole = 'viewer' | 'trader' | 'admin';

export interface User {
  id: string;
  email: string;
  role: UserRole;
  status: string;
}

export interface TokenResponse {
  access_token: string;
  token_type: string;
  /** Seconds until `access_token` expires. */
  expires_in: number;
  user: User;
}

export class ApiError extends Error {
  readonly status?: number | undefined;

  constructor(message: string, options?: { status?: number; cause?: unknown }) {
    super(message, { cause: options?.cause });
    this.name = 'ApiError';
    this.status = options?.status;
  }
}

interface ErrorEnvelope {
  error?: { code?: string; message?: string };
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const url = apiUrl(path);

  let response: Response;
  try {
    response = await fetch(url, { ...init, credentials: 'include' });
  } catch (cause) {
    throw new ApiError(`Could not reach the API at ${url}`, { cause });
  }

  if (!response.ok) {
    let message = `Request failed with status ${response.status}`;
    try {
      const body = (await response.json()) as ErrorEnvelope;
      if (body.error?.message) message = body.error.message;
    } catch {
      // A non-JSON error body is not itself an error worth surfacing; the status is.
    }
    throw new ApiError(message, { status: response.status });
  }

  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

function authHeader(accessToken: string): Record<string, string> {
  return { Authorization: `Bearer ${accessToken}` };
}

const JSON_HEADERS = { 'Content-Type': 'application/json' };

export function register(email: string, password: string): Promise<User> {
  return request<User>('/api/v1/auth/register', {
    method: 'POST',
    headers: JSON_HEADERS,
    body: JSON.stringify({ email, password }),
  });
}

export function login(email: string, password: string): Promise<TokenResponse> {
  return request<TokenResponse>('/api/v1/auth/login', {
    method: 'POST',
    headers: JSON_HEADERS,
    body: JSON.stringify({ email, password }),
  });
}

/** Rotates the refresh cookie via the browser's own cookie jar; nothing to pass in. */
export function refreshSession(): Promise<TokenResponse> {
  return request<TokenResponse>('/api/v1/auth/refresh', { method: 'POST' });
}

export async function logout(accessToken: string, everywhere = false): Promise<void> {
  await request<undefined>('/api/v1/auth/logout', {
    method: 'POST',
    headers: { ...JSON_HEADERS, ...authHeader(accessToken) },
    body: JSON.stringify({ everywhere }),
  });
}

export function fetchMe(accessToken: string): Promise<User> {
  return request<User>('/api/v1/auth/me', { headers: authHeader(accessToken) });
}
