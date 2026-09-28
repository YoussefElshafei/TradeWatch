/**
 * The strategy API client (Phase 6, ADR-0006).
 *
 * Same shape as `auth.ts`: a small, self-contained module with its own `ApiError`, replaced
 * wholesale once generated types arrive from the committed OpenAPI schema
 * (`ARCHITECTURE.md` §8.3). Every route here is owner-scoped, so every call needs the bearer
 * token `auth.ts`'s login/refresh flow already produces.
 */

import { apiUrl } from '../config';

export interface Strategy {
  id: string;
  user_id: string;
  name: string;
  strategy_type: string;
  parameters: Record<string, unknown>;
  symbols: string[];
  is_archived: boolean;
  created_at: string;
  updated_at: string;
}

export interface StrategyList {
  count: number;
  strategies: Strategy[];
}

export interface CreateStrategyInput {
  name: string;
  strategy_type: string;
  parameters: Record<string, unknown>;
  symbols: string[];
}

export interface FieldError {
  location: string;
  message: string;
  type: string;
}

export class ApiError extends Error {
  readonly status?: number | undefined;
  readonly details?: FieldError[] | undefined;

  constructor(
    message: string,
    options?: { status?: number; details?: FieldError[]; cause?: unknown },
  ) {
    super(message, { cause: options?.cause });
    this.name = 'ApiError';
    this.status = options?.status;
    this.details = options?.details;
  }
}

interface ErrorEnvelope {
  error?: { code?: string; message?: string; details?: FieldError[] };
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
    let details: FieldError[] | undefined;
    try {
      const body = (await response.json()) as ErrorEnvelope;
      if (body.error?.message) message = body.error.message;
      details = body.error?.details;
    } catch {
      // A non-JSON error body is not itself an error worth surfacing; the status is.
    }
    throw new ApiError(message, { status: response.status, ...(details && { details }) });
  }

  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

const JSON_HEADERS = { 'Content-Type': 'application/json' };

export function createStrategy(
  input: CreateStrategyInput,
  accessToken: string,
): Promise<Strategy> {
  return request<Strategy>('/api/v1/strategies', accessToken, {
    method: 'POST',
    headers: JSON_HEADERS,
    body: JSON.stringify(input),
  });
}

export function fetchStrategies(
  accessToken: string,
  includeArchived = false,
  signal?: AbortSignal,
): Promise<StrategyList> {
  const params = new URLSearchParams({ include_archived: String(includeArchived) });
  return request<StrategyList>(`/api/v1/strategies?${params.toString()}`, accessToken, {
    signal: signal ?? null,
  });
}

export function fetchStrategy(
  id: string,
  accessToken: string,
  signal?: AbortSignal,
): Promise<Strategy> {
  return request<Strategy>(`/api/v1/strategies/${encodeURIComponent(id)}`, accessToken, {
    signal: signal ?? null,
  });
}

export function archiveStrategy(id: string, accessToken: string): Promise<void> {
  return request<undefined>(`/api/v1/strategies/${encodeURIComponent(id)}`, accessToken, {
    method: 'DELETE',
  });
}

/**
 * The built-in strategy types and their parameter bounds (ADR-0006).
 *
 * There is no schema-introspection endpoint in this phase, so the editor's form fields are
 * hardcoded against the two types the backend registers
 * (`backend/src/tradewatch/application/strategy/types/`). Adding a third type later is one
 * more entry here, not a rewrite of `StrategyEditor`.
 */
export interface StrategyParamField {
  name: string;
  label: string;
  min: number;
  max: number;
  defaultValue: number;
}

export interface StrategyTypeDef {
  value: string;
  label: string;
  fields: StrategyParamField[];
  /** Client-side cross-field check, mirroring the backend's own `model_validator`. */
  validate: (values: Record<string, number>) => string | null;
}

export const STRATEGY_TYPES: StrategyTypeDef[] = [
  {
    value: 'sma_crossover',
    label: 'SMA crossover',
    fields: [
      {
        name: 'fast_period',
        label: 'Fast period (bars)',
        min: 2,
        max: 200,
        defaultValue: 10,
      },
      {
        name: 'slow_period',
        label: 'Slow period (bars)',
        min: 2,
        max: 400,
        defaultValue: 30,
      },
    ],
    validate: (values) =>
      (values.fast_period ?? 0) >= (values.slow_period ?? 0)
        ? 'Fast period must be less than slow period.'
        : null,
  },
  {
    value: 'rsi_reversion',
    label: 'RSI reversion',
    fields: [
      {
        name: 'rsi_period',
        label: 'RSI period (bars)',
        min: 2,
        max: 100,
        defaultValue: 14,
      },
      { name: 'oversold', label: 'Oversold threshold', min: 1, max: 99, defaultValue: 30 },
      {
        name: 'overbought',
        label: 'Overbought threshold',
        min: 1,
        max: 99,
        defaultValue: 70,
      },
    ],
    validate: (values) =>
      (values.oversold ?? 0) >= (values.overbought ?? 0)
        ? 'Oversold threshold must be less than overbought threshold.'
        : null,
  },
];
