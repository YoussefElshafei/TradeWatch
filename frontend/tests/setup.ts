/**
 * Global test setup.
 *
 * `fetch` is stubbed by default so that no test accidentally depends on a running backend.
 * A test that wants a particular response says so explicitly.
 */

import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { afterEach, beforeEach, vi } from 'vitest';

beforeEach(() => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => Promise.reject(new Error('fetch is not stubbed in this test'))),
  );
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});
