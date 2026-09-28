/**
 * The strategy-deployment API client (Phase 7).
 *
 * Same shape as `backtests.ts`/`strategies.ts` — its own `request`/`ApiError`, not shared,
 * per this codebase's convention of small self-contained per-domain modules.
 */

import { apiUrl } from '../config';

export type DeploymentStatus = 'created' | 'running' | 'paused' | 'stopped' | 'errored';

export interface PositionSizing {
  target_equity_pct: string;
}

export interface RiskLimits {
  max_position_pct: string;
  max_gross_exposure_pct: string;
  max_orders_per_minute: number;
}

export interface DeploymentResponse {
  id: string;
  strategy_id: string;
  portfolio_id: string;
  status: DeploymentStatus;
  position_sizing: PositionSizing;
  risk_limits: RiskLimits;
  execution_config: Record<string, unknown>;
  started_at: string | null;
  stopped_at: string | null;
  error_message: string | null;
  created_at: string;
  updated_at: string;
}

export interface DeploymentList {
  count: number;
  deployments: DeploymentResponse[];
}

export interface CreateDeploymentInput {
  strategy_id: string;
  portfolio_id: string;
  position_sizing?: PositionSizing;
  risk_limits?: RiskLimits;
  execution_config?: Record<string, unknown>;
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

export function createDeployment(
  input: CreateDeploymentInput,
  accessToken: string,
): Promise<DeploymentResponse> {
  return request<DeploymentResponse>('/api/v1/deployments', accessToken, {
    method: 'POST',
    headers: JSON_HEADERS,
    body: JSON.stringify(input),
  });
}

export function fetchDeployments(
  portfolioId: string,
  accessToken: string,
  signal?: AbortSignal,
): Promise<DeploymentList> {
  const params = new URLSearchParams({ portfolio_id: portfolioId });
  return request<DeploymentList>(`/api/v1/deployments?${params.toString()}`, accessToken, {
    signal: signal ?? null,
  });
}

export function fetchDeployment(
  id: string,
  accessToken: string,
  signal?: AbortSignal,
): Promise<DeploymentResponse> {
  return request<DeploymentResponse>(
    `/api/v1/deployments/${encodeURIComponent(id)}`,
    accessToken,
    {
      signal: signal ?? null,
    },
  );
}

function transition(
  id: string,
  action: string,
  accessToken: string,
): Promise<DeploymentResponse> {
  return request<DeploymentResponse>(
    `/api/v1/deployments/${encodeURIComponent(id)}/${action}`,
    accessToken,
    { method: 'POST' },
  );
}

export function startDeployment(
  id: string,
  accessToken: string,
): Promise<DeploymentResponse> {
  return transition(id, 'start', accessToken);
}

export function pauseDeployment(
  id: string,
  accessToken: string,
): Promise<DeploymentResponse> {
  return transition(id, 'pause', accessToken);
}

export function resumeDeployment(
  id: string,
  accessToken: string,
): Promise<DeploymentResponse> {
  return transition(id, 'resume', accessToken);
}

export function stopDeployment(
  id: string,
  accessToken: string,
): Promise<DeploymentResponse> {
  return transition(id, 'stop', accessToken);
}
