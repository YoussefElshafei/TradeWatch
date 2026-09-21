/**
 * The one API call the Phase 0 shell makes.
 *
 * It exists to answer a question every new developer asks in their first ten minutes —
 * "is the frontend actually talking to the backend?" — without anyone having to open a
 * terminal. Generated API types arrive in Phase 1 from the committed OpenAPI schema; this
 * module is replaced by them rather than kept alongside them.
 */

import { apiUrl } from '../config';

export interface HealthResponse {
  status: 'ok';
  version: string;
}

export class ApiError extends Error {
  readonly status?: number | undefined;

  constructor(message: string, options?: { status?: number; cause?: unknown }) {
    super(message, { cause: options?.cause });
    this.name = 'ApiError';
    this.status = options?.status;
  }
}

/**
 * Fetch the backend's liveness endpoint.
 *
 * @throws ApiError when the backend is unreachable or answers with a non-2xx status.
 */
export async function fetchHealth(signal?: AbortSignal): Promise<HealthResponse> {
  const url = apiUrl('/health');

  let response: Response;
  try {
    response = await fetch(url, { signal: signal ?? null });
  } catch (cause) {
    throw new ApiError(`Could not reach the API at ${url}`, { cause });
  }

  if (!response.ok) {
    throw new ApiError(`API returned ${response.status}`, { status: response.status });
  }

  return (await response.json()) as HealthResponse;
}
