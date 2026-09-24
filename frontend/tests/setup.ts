/**
 * Global test setup.
 *
 * `fetch` is stubbed by default so that no test accidentally depends on a running backend.
 * A test that wants a particular response says so explicitly.
 */

import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { afterEach, beforeEach, vi } from 'vitest';

import { FakeWebSocket } from '../src/test-support/fakeWebSocket';

beforeEach(() => {
  vi.stubGlobal(
    'fetch',
    vi.fn(() => Promise.reject(new Error('fetch is not stubbed in this test'))),
  );
  // jsdom has no WebSocket at all (Phase 4). A silent fake that never fires an event is the
  // right default for every test that isn't specifically about the live feed — see
  // `src/test-support/fakeWebSocket.ts`.
  FakeWebSocket.reset();
  vi.stubGlobal('WebSocket', FakeWebSocket);
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});
