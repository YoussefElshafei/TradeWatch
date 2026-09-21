/**
 * The market data API client.
 *
 * Types are hand-written for now and mirror `backend/src/tradewatch/api/schemas/market.py`.
 * That duplication is deliberate and temporary: `ARCHITECTURE.md` §8.3 calls for generating
 * them from the committed OpenAPI schema so a backend contract change breaks this build
 * rather than production. Generating them needs a codegen step in CI, which is one more
 * piece of infrastructure than closing this slice requires.
 *
 * Money arrives as **strings** and stays that way. The backend serialises decimals as
 * strings precisely so JavaScript's JSON parser cannot round them (ADR-0009); calling
 * `Number()` on one here would undo that on the last hop. Values are converted to numbers
 * only where a chart library demands it, at the point of drawing, and never fed back.
 */

import { apiUrl } from '../config';

export interface Instrument {
  symbol: string;
  name: string | null;
  asset_class: string;
  exchange: string;
  currency: string;
  tick_size: string;
  lot_size: string;
  status: string;
}

export interface InstrumentList {
  count: number;
  instruments: Instrument[];
}

export interface Bar {
  bar_start: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
  vwap: string | null;
  trade_count: number;
}

export interface BarSeries {
  symbol: string;
  interval: string;
  start: string;
  end: string;
  count: number;
  bars: Bar[];
}

export interface LatestPrice {
  symbol: string;
  price: string | null;
  size: string | null;
  traded_at: string | null;
  bid_price: string | null;
  ask_price: string | null;
  quoted_at: string | null;
  source: string | null;
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

/**
 * Fetch and parse, turning every failure into an `ApiError` with something a user can read.
 *
 * The backend answers failures with a single envelope (`ARCHITECTURE.md` §8.3), so the
 * message it supplies — "Unknown symbol 'NOPE'" — is better than anything this layer could
 * invent, and it is used when present.
 */
async function request<T>(path: string, signal?: AbortSignal): Promise<T> {
  const url = apiUrl(path);

  let response: Response;
  try {
    response = await fetch(url, { signal: signal ?? null });
  } catch (cause) {
    if (cause instanceof DOMException && cause.name === 'AbortError') throw cause;
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

  return (await response.json()) as T;
}

export function fetchInstruments(signal?: AbortSignal): Promise<InstrumentList> {
  return request<InstrumentList>('/api/v1/instruments', signal);
}

export interface BarQuery {
  symbol: string;
  interval?: string;
  limit?: number;
}

export function fetchBars(
  { symbol, interval = '1m', limit = 240 }: BarQuery,
  signal?: AbortSignal,
): Promise<BarSeries> {
  const params = new URLSearchParams({ interval, limit: String(limit) });
  return request<BarSeries>(
    `/api/v1/market/${encodeURIComponent(symbol)}/bars?${params.toString()}`,
    signal,
  );
}

export function fetchLatestPrice(
  symbol: string,
  signal?: AbortSignal,
): Promise<LatestPrice> {
  return request<LatestPrice>(
    `/api/v1/market/${encodeURIComponent(symbol)}/latest`,
    signal,
  );
}
