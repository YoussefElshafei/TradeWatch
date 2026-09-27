import { describe, expect, it, vi } from 'vitest';

import { ApiError, fetchHealth } from './health';

describe('fetchHealth', () => {
  it('returns the parsed body on success', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() =>
        Promise.resolve(new Response(JSON.stringify({ status: 'ok', version: '0.1.0' }))),
      ),
    );

    await expect(fetchHealth()).resolves.toEqual({ status: 'ok', version: '0.1.0' });
  });

  it('raises ApiError with the URL when the backend is unreachable', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.reject(new TypeError('Failed to fetch'))),
    );

    await expect(fetchHealth()).rejects.toThrow(ApiError);
    await expect(fetchHealth()).rejects.toThrow('http://localhost:8010/health');
  });

  it('raises ApiError carrying the status on a non-2xx response', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(() => Promise.resolve(new Response('nope', { status: 503 }))),
    );

    await expect(fetchHealth()).rejects.toMatchObject({ status: 503 });
  });

  it('passes the abort signal through, so an unmount cancels the request', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response('{}')));
    vi.stubGlobal('fetch', fetchMock);
    const controller = new AbortController();

    await fetchHealth(controller.signal);

    expect(fetchMock).toHaveBeenCalledWith(expect.any(String), {
      signal: controller.signal,
    });
  });
});
