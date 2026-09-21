import { describe, expect, it } from 'vitest';

import { apiUrl, config } from './config';

describe('config', () => {
  it('falls back to the local API when VITE_API_BASE_URL is unset', () => {
    // The test environment sets no VITE_ variables, which is the case a new developer
    // hits on their first `npm run dev`.
    expect(config.apiBaseUrl).toBe('http://localhost:8000');
  });

  it('defaults the environment name to local', () => {
    expect(config.environment).toBe('local');
  });
});

describe('apiUrl', () => {
  it('joins a path that starts with a slash', () => {
    expect(apiUrl('/health')).toBe('http://localhost:8000/health');
  });

  it('joins a path that does not', () => {
    expect(apiUrl('health')).toBe('http://localhost:8000/health');
  });
});
