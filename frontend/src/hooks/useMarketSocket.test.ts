/**
 * `useMarketSocket` in isolation, against the `FakeWebSocket` every test gets by default
 * (`tests/setup.ts`) — driven directly here instead of through `MarketView`, so a failure
 * points at the socket logic rather than at the view that happens to use it.
 */

import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { FakeWebSocket } from '../test-support/fakeWebSocket';

import { useMarketSocket } from './useMarketSocket';

describe('useMarketSocket', () => {
  it('does nothing until a symbol is given', () => {
    const { result } = renderHook(() => useMarketSocket(''));

    expect(result.current.status).toBe('closed');
    expect(FakeWebSocket.instances).toHaveLength(0);
  });

  it('connects to /ws/market with the symbol in the query string', () => {
    renderHook(() => useMarketSocket('AAPL'));

    expect(FakeWebSocket.latest().url).toContain('/ws/market?symbols=AAPL');
  });

  it('reports "open" once the socket connects', async () => {
    const { result } = renderHook(() => useMarketSocket('AAPL'));

    act(() => {
      FakeWebSocket.latest().open();
    });

    await waitFor(() => {
      expect(result.current.status).toBe('open');
    });
  });

  it('records a live trade as both the latest trade and the head of the tape', async () => {
    const { result } = renderHook(() => useMarketSocket('AAPL'));
    act(() => {
      FakeWebSocket.latest().open();
    });

    act(() => {
      FakeWebSocket.latest().message({
        event_type: 'market.trade',
        event_id: 'trade-1',
        symbol: 'AAPL',
        price: '187.50',
        size: '10',
        occurred_at: '2026-03-02T15:30:00Z',
      });
    });

    await waitFor(() => {
      expect(result.current.latestTrade?.price).toBe('187.50');
      expect(result.current.trades).toHaveLength(1);
    });
  });

  it('does not duplicate a redelivered trade on the tape (broadcaster restart, F9/at-least-once)', async () => {
    const { result } = renderHook(() => useMarketSocket('AAPL'));
    act(() => {
      FakeWebSocket.latest().open();
    });
    const trade = {
      event_type: 'market.trade',
      event_id: 'redelivered-trade',
      symbol: 'AAPL',
      price: '187.50',
      size: '10',
      occurred_at: '2026-03-02T15:30:00Z',
    };

    act(() => {
      FakeWebSocket.latest().message(trade);
    });
    await waitFor(() => {
      expect(result.current.trades).toHaveLength(1);
    });

    // The broadcaster has no consumer_progress tracking (it doesn't need one — see the
    // module docstring), so a restart redelivers the identical event, `event_id` and all.
    act(() => {
      FakeWebSocket.latest().message(trade);
    });

    await waitFor(() => {
      expect(result.current.latestTrade?.event_id).toBe('redelivered-trade');
    });
    // The tape must not grow: this is the same print shown a second time, not a new one.
    expect(result.current.trades).toHaveLength(1);
  });

  it('does not dedupe two different trades that happen to share nothing but a symbol', async () => {
    const { result } = renderHook(() => useMarketSocket('AAPL'));
    act(() => {
      FakeWebSocket.latest().open();
    });

    act(() => {
      FakeWebSocket.latest().message({
        event_type: 'market.trade',
        event_id: 'trade-a',
        symbol: 'AAPL',
        price: '100',
        size: '1',
        occurred_at: '2026-03-02T15:30:00Z',
      });
    });
    act(() => {
      FakeWebSocket.latest().message({
        event_type: 'market.trade',
        event_id: 'trade-b',
        symbol: 'AAPL',
        price: '101',
        size: '1',
        occurred_at: '2026-03-02T15:30:01Z',
      });
    });

    await waitFor(() => {
      expect(result.current.trades).toHaveLength(2);
    });
  });

  it('updates the latest quote independently of trades', async () => {
    const { result } = renderHook(() => useMarketSocket('AAPL'));
    act(() => {
      FakeWebSocket.latest().open();
    });

    act(() => {
      FakeWebSocket.latest().message({
        event_type: 'market.quote',
        symbol: 'AAPL',
        bid_price: '187.40',
        ask_price: '187.44',
        occurred_at: '2026-03-02T15:30:00Z',
      });
    });

    await waitFor(() => {
      expect(result.current.latestQuote?.bid_price).toBe('187.40');
      expect(result.current.latestTrade).toBeNull();
    });
  });

  it('bumps resyncedAt on a resync marker, without touching the trade tape', async () => {
    const { result } = renderHook(() => useMarketSocket('AAPL'));
    act(() => {
      FakeWebSocket.latest().open();
    });
    act(() => {
      FakeWebSocket.latest().message({
        event_type: 'market.trade',
        event_id: 'trade-resync',
        symbol: 'AAPL',
        price: '100',
        size: '1',
        occurred_at: '2026-03-02T15:30:00Z',
      });
    });
    await waitFor(() => {
      expect(result.current.trades).toHaveLength(1);
    });

    act(() => {
      FakeWebSocket.latest().message({ event_type: 'market.resync' });
    });

    await waitFor(() => {
      expect(result.current.resyncedAt).toBeGreaterThan(0);
    });
    expect(result.current.trades).toHaveLength(1);
  });

  it('reconnects after the socket closes', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      const { result } = renderHook(() => useMarketSocket('AAPL'));
      act(() => {
        FakeWebSocket.latest().open();
      });
      await waitFor(() => {
        expect(result.current.status).toBe('open');
      });

      const first = FakeWebSocket.latest();
      act(() => {
        first.close();
      });
      await waitFor(() => {
        expect(result.current.status).toBe('reconnecting');
      });

      await act(async () => {
        await vi.advanceTimersByTimeAsync(1000);
      });

      expect(FakeWebSocket.instances.length).toBeGreaterThan(1);
      expect(FakeWebSocket.instances.length).toBe(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('resets trade history when the symbol changes', async () => {
    const { result, rerender } = renderHook(({ symbol }) => useMarketSocket(symbol), {
      initialProps: { symbol: 'AAPL' },
    });
    act(() => {
      FakeWebSocket.latest().open();
    });
    act(() => {
      FakeWebSocket.latest().message({
        event_type: 'market.trade',
        event_id: 'trade-before-switch',
        symbol: 'AAPL',
        price: '100',
        size: '1',
        occurred_at: '2026-03-02T15:30:00Z',
      });
    });
    await waitFor(() => {
      expect(result.current.trades).toHaveLength(1);
    });

    rerender({ symbol: 'MSFT' });

    expect(result.current.trades).toHaveLength(0);
    expect(result.current.latestTrade).toBeNull();
    expect(FakeWebSocket.latest().url).toContain('symbols=MSFT');
  });
});
