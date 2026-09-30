/**
 * The alert-rule and alert-feed API client (Phase 9, `ARCHITECTURE.md` §3.8).
 *
 * Same shape as `strategies.ts`: a small, self-contained module with its own `ApiError`,
 * replaced wholesale once generated types arrive from the committed OpenAPI schema
 * (`ARCHITECTURE.md` §8.3). Every route here is owner-scoped, so every call needs the bearer
 * token `auth.ts`'s login/refresh flow already produces.
 */

import { apiUrl } from '../config';

export interface AlertRule {
  id: string;
  user_id: string;
  name: string;
  event_types: string[];
  symbols: string[] | null;
  min_anomaly_score: string | null;
  cooldown_seconds: number;
  channels: string[];
  is_enabled: boolean;
  created_at: string;
  updated_at: string;
}

export interface AlertRuleList {
  count: number;
  rules: AlertRule[];
}

export interface CreateAlertRuleInput {
  name: string;
  event_types: string[];
  symbols?: string[] | null;
  min_anomaly_score?: string | null;
  cooldown_seconds?: number;
  channels?: string[];
  is_enabled?: boolean;
}

export type UpdateAlertRuleInput = Partial<CreateAlertRuleInput>;

export interface Alert {
  id: string;
  alert_rule_id: string;
  symbol: string | null;
  event_type: string;
  severity: 'info' | 'warning' | 'critical';
  title: string;
  body: string;
  payload: Record<string, unknown>;
  created_at: string;
  acknowledged_at: string | null;
}

export interface AlertList {
  count: number;
  alerts: Alert[];
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

export function createAlertRule(
  input: CreateAlertRuleInput,
  accessToken: string,
): Promise<AlertRule> {
  return request<AlertRule>('/api/v1/alert-rules', accessToken, {
    method: 'POST',
    headers: JSON_HEADERS,
    body: JSON.stringify(input),
  });
}

export function fetchAlertRules(
  accessToken: string,
  signal?: AbortSignal,
): Promise<AlertRuleList> {
  return request<AlertRuleList>('/api/v1/alert-rules', accessToken, {
    signal: signal ?? null,
  });
}

export function updateAlertRule(
  id: string,
  input: UpdateAlertRuleInput,
  accessToken: string,
): Promise<AlertRule> {
  return request<AlertRule>(`/api/v1/alert-rules/${encodeURIComponent(id)}`, accessToken, {
    method: 'PATCH',
    headers: JSON_HEADERS,
    body: JSON.stringify(input),
  });
}

export function deleteAlertRule(id: string, accessToken: string): Promise<void> {
  return request<undefined>(`/api/v1/alert-rules/${encodeURIComponent(id)}`, accessToken, {
    method: 'DELETE',
  });
}

export function fetchAlerts(
  accessToken: string,
  unacknowledgedOnly = false,
  signal?: AbortSignal,
): Promise<AlertList> {
  const params = new URLSearchParams({ unacknowledged_only: String(unacknowledgedOnly) });
  return request<AlertList>(`/api/v1/alerts?${params.toString()}`, accessToken, {
    signal: signal ?? null,
  });
}

export function acknowledgeAlert(id: string, accessToken: string): Promise<void> {
  return request<undefined>(`/api/v1/alerts/${encodeURIComponent(id)}/ack`, accessToken, {
    method: 'POST',
  });
}

/** The only two event types a rule can listen for — see `api/schemas/alerts.py`'s docstring
 * for why the backend rejects anything else with a 422. */
export const ALERT_EVENT_TYPES: { value: string; label: string }[] = [
  { value: 'anomaly.detected', label: 'Unusual activity' },
  { value: 'order.filled', label: 'Order filled' },
];
