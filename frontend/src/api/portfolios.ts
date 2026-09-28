/**
 * The portfolio API client (Phase 7).
 *
 * Same shape as `backtests.ts`/`strategies.ts` — its own `request`/`ApiError`, not shared,
 * per this codebase's convention of small self-contained per-domain modules.
 */

import { apiUrl } from '../config';

export interface PortfolioResponse {
  id: string;
  user_id: string;
  name: string;
  base_currency: string;
  initial_cash: string;
  cash_balance: string;
  created_at: string;
  updated_at: string;
}

export interface PositionResponse {
  instrument_id: string;
  symbol: string;
  quantity: string;
  avg_entry_price: string;
  realized_pnl: string;
  updated_at: string;
}

export interface PortfolioDetailResponse extends PortfolioResponse {
  positions: PositionResponse[];
  /** Cash plus cost basis of open positions — not mark-to-market. */
  total_equity: string;
}

export interface PortfolioList {
  count: number;
  portfolios: PortfolioResponse[];
}

export interface CreatePortfolioInput {
  name: string;
  base_currency?: string;
  initial_cash: string;
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

async function request<T>(
  path: string,
  accessToken: string,
  init?: RequestInit,
): Promise<T> {
  const url = apiUrl(path);

  // `Headers` merges whichever of the three `HeadersInit` shapes `init.headers` turns out to
  // be; spreading it into a plain object would misread an array-of-tuples form as numeric
  // indices instead of names.
  const headers = new Headers(init?.headers);
  headers.set('Authorization', `Bearer ${accessToken}`);

  let response: Response;
  try {
    response = await fetch(url, { ...init, headers });
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

  return (await response.json()) as T;
}

const JSON_HEADERS = { 'Content-Type': 'application/json' };

export function createPortfolio(
  input: CreatePortfolioInput,
  accessToken: string,
): Promise<PortfolioResponse> {
  return request<PortfolioResponse>('/api/v1/portfolios', accessToken, {
    method: 'POST',
    headers: JSON_HEADERS,
    body: JSON.stringify(input),
  });
}

export function fetchPortfolios(
  accessToken: string,
  signal?: AbortSignal,
): Promise<PortfolioList> {
  return request<PortfolioList>('/api/v1/portfolios', accessToken, {
    signal: signal ?? null,
  });
}

export function fetchPortfolio(
  id: string,
  accessToken: string,
  signal?: AbortSignal,
): Promise<PortfolioDetailResponse> {
  return request<PortfolioDetailResponse>(
    `/api/v1/portfolios/${encodeURIComponent(id)}`,
    accessToken,
    { signal: signal ?? null },
  );
}
