/**
 * Frontend configuration.
 *
 * Vite only exposes variables prefixed with `VITE_` to the browser bundle, which is a
 * useful accident: it makes it hard to leak a server-side secret into client code. The
 * backend's `TW_`-prefixed settings are deliberately not readable here.
 *
 * Anything configurable is read once, here, so there is one place to look.
 */

interface AppConfig {
  /** Base URL of the TradeWatch API, without a trailing slash. */
  readonly apiBaseUrl: string;
  /** Environment name, used for display and for enabling development-only affordances. */
  readonly environment: string;
}

function readString(value: string | undefined, fallback: string): string {
  const trimmed = value?.trim();
  return trimmed ? trimmed.replace(/\/+$/, '') : fallback;
}

export const config: AppConfig = {
  apiBaseUrl: readString(import.meta.env.VITE_API_BASE_URL, 'http://localhost:8010'),
  environment: readString(import.meta.env.VITE_ENV, 'local'),
};

/** Join the API base URL with a path, without producing a double slash. */
export function apiUrl(path: string): string {
  return `${config.apiBaseUrl}${path.startsWith('/') ? path : `/${path}`}`;
}

/**
 * The same host as the REST API, over `ws`/`wss` instead of `http`/`https` (Phase 4).
 *
 * There is no separate `VITE_WS_BASE_URL`: `/ws/market` is served by the same FastAPI
 * process as `/api/v1/*`, so deriving it from `apiBaseUrl` is one fewer thing to configure
 * and one fewer way for the two to drift apart in an environment file.
 */
export function wsUrl(path: string): string {
  const url = new URL(apiUrl(path));
  url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';
  return url.toString();
}
