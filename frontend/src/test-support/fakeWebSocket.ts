/**
 * A controllable stand-in for `WebSocket`, for tests.
 *
 * jsdom (vitest's test DOM) has no `WebSocket` implementation at all, so every test that
 * mounts `MarketView` needs *some* global or `useMarketSocket`'s `new WebSocket(...)` throws
 * a `ReferenceError` before anything else in the component runs — the same reason `fetch` is
 * stubbed globally in `tests/setup.ts`. Most tests never call `open()`/`message()` on it and
 * never see a socket event at all, which is the point: they are testing the REST path, and a
 * live feed that never says anything is the correct default.
 *
 * Tests that *do* care about live behaviour (`useMarketSocket.test.ts`,
 * `MarketView.live.test.tsx`) grab the most recently constructed instance from
 * `FakeWebSocket.instances` and drive it directly.
 */

export class FakeWebSocket {
  static instances: FakeWebSocket[] = [];

  readonly url: string;
  onopen: (() => void) | null = null;
  onmessage: ((event: { data: string }) => void) | null = null;
  onclose: (() => void) | null = null;
  onerror: (() => void) | null = null;
  closed = false;

  constructor(url: string) {
    this.url = url;
    FakeWebSocket.instances.push(this);
  }

  open(): void {
    this.onopen?.();
  }

  message(payload: unknown): void {
    this.onmessage?.({ data: JSON.stringify(payload) });
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.onclose?.();
  }

  static reset(): void {
    FakeWebSocket.instances = [];
  }

  /** The instance the component most recently created — usually the only one a test needs. */
  static latest(): FakeWebSocket {
    const instance = FakeWebSocket.instances.at(-1);
    if (!instance) throw new Error('no FakeWebSocket has been constructed yet');
    return instance;
  }
}
