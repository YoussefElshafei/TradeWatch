/**
 * The backtest API client (Phase 6, ADR-0007, ARCHITECTURE.md §4.3).
 *
 * Same shape as `strategies.ts` — its own `request`/`ApiError`, not shared, per this
 * codebase's convention of small self-contained per-domain modules until generated types
 * arrive.
 *
 * **No WebSocket here.** `ARCHITECTURE.md` §4.3 is explicit that a backtest is a batch job
 * behind Kafka, not a stream: "the client polls ... GET /api/v1/backtests/{id}". `useBacktestPolling`
 * below is that poll, not a workaround for a missing push channel.
 */

import { apiUrl } from '../config';

export type FillModel = 'next_bar_open' | 'current_close' | 'vwap';

export interface ExecutionConfig {
  slippage_bps: string;
  commission_per_share: string;
  commission_pct: string;
  fill_model: FillModel;
  allow_partial_fills: boolean;
}

export type BacktestStatus = 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';

/** A run is still going while it has not reached one of these. */
export const TERMINAL_STATUSES: readonly BacktestStatus[] = [
  'completed',
  'failed',
  'cancelled',
];

export interface BacktestMetrics {
  total_return_pct: string;
  cagr_pct: string | null;
  max_drawdown_pct: string;
  sharpe_ratio: string | null;
  volatility_pct: string | null;
  win_rate_pct: string | null;
  trade_count: number;
  exposure_pct: string;
}

export interface BacktestRun {
  id: string;
  user_id: string;
  strategy_id: string;
  symbols: string[];
  start_date: string;
  end_date: string;
  initial_cash: string;
  execution_config: ExecutionConfig;
  status: BacktestStatus;
  error_message: string | null;
  metrics: BacktestMetrics | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
}

export interface BacktestRunList {
  count: number;
  runs: BacktestRun[];
}

export interface BacktestTrade {
  id: string;
  symbol: string;
  side: 'buy' | 'sell';
  quantity: string;
  entry_price: string;
  entry_at: string;
  exit_price: string | null;
  exit_at: string | null;
  pnl: string | null;
  return_pct: string | null;
}

export interface BacktestTradeList {
  count: number;
  trades: BacktestTrade[];
}

export interface BacktestEquityPoint {
  as_of: string;
  total_equity: string;
  drawdown_pct: string | null;
}

export interface BacktestEquityList {
  count: number;
  points: BacktestEquityPoint[];
}

export interface CreateBacktestInput {
  strategy_id: string;
  symbols?: string[];
  start_date: string;
  end_date: string;
  initial_cash?: string;
  execution_config?: Partial<ExecutionConfig>;
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

export function submitBacktest(
  input: CreateBacktestInput,
  accessToken: string,
): Promise<BacktestRun> {
  return request<BacktestRun>('/api/v1/backtests', accessToken, {
    method: 'POST',
    headers: JSON_HEADERS,
    body: JSON.stringify(input),
  });
}

export function fetchBacktests(
  accessToken: string,
  signal?: AbortSignal,
): Promise<BacktestRunList> {
  return request<BacktestRunList>('/api/v1/backtests', accessToken, {
    signal: signal ?? null,
  });
}

export function fetchBacktest(
  id: string,
  accessToken: string,
  signal?: AbortSignal,
): Promise<BacktestRun> {
  return request<BacktestRun>(`/api/v1/backtests/${encodeURIComponent(id)}`, accessToken, {
    signal: signal ?? null,
  });
}

export function fetchBacktestTrades(
  id: string,
  accessToken: string,
  signal?: AbortSignal,
): Promise<BacktestTradeList> {
  return request<BacktestTradeList>(
    `/api/v1/backtests/${encodeURIComponent(id)}/trades`,
    accessToken,
    { signal: signal ?? null },
  );
}

export function fetchBacktestEquity(
  id: string,
  accessToken: string,
  signal?: AbortSignal,
): Promise<BacktestEquityList> {
  return request<BacktestEquityList>(
    `/api/v1/backtests/${encodeURIComponent(id)}/equity`,
    accessToken,
    { signal: signal ?? null },
  );
}
