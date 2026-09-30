/**
 * The anomaly feed API client (Phase 8, ADR-0008).
 *
 * Same shape as `market.ts`: `GET /api/v1/anomalies` is unauthenticated, like the rest of the
 * market-data routes (`api/routes/anomalies.py`'s own docstring — a detection describes the
 * market's own behaviour, not anything a user owns), so this client needs no access token.
 */

import { apiUrl } from '../config';

export interface AnomalyDetection {
  id: string;
  symbol: string;
  detected_at: string;
  window_start: string;
  window_end: string;
  anomaly_score: string;
  threshold: string;
  severity: 'info' | 'warning' | 'critical';
  model_name: string;
  model_version: string;
  features: Record<string, string>;
}

export interface AnomalyDetectionList {
  count: number;
  detections: AnomalyDetection[];
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

export function fetchAnomalies(
  options: { symbol?: string; limit?: number; offset?: number } = {},
  signal?: AbortSignal,
): Promise<AnomalyDetectionList> {
  const params = new URLSearchParams();
  if (options.symbol) params.set('symbol', options.symbol);
  params.set('limit', String(options.limit ?? 100));
  params.set('offset', String(options.offset ?? 0));

  return request<AnomalyDetectionList>(`/api/v1/anomalies?${params.toString()}`, signal);
}

async function request<T>(path: string, signal?: AbortSignal): Promise<T> {
  const url = apiUrl(path);

  let response: Response;
  try {
    response = await fetch(url, { signal: signal ?? null });
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
