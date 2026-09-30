/**
 * `useAlertSocket` in isolation, against the `FakeWebSocket` every test gets by default
 * (`tests/setup.ts`) — same approach as `useMarketSocket.test.ts`.
 */

import { act, renderHook, waitFor } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { FakeWebSocket } from '../test-support/fakeWebSocket';

import { useAlertSocket } from './useAlertSocket';

const TOKEN = 'test-access-token';

describe('useAlertSocket', () => {
  it('does nothing until a token is available', () => {
    const { result } = renderHook(() => useAlertSocket(null));

    expect(result.current.status).toBe('closed');
    expect(FakeWebSocket.instances).toHaveLength(0);
  });

  it('connects to /ws/alerts with the token in the query string, and no symbols parameter', () => {
    renderHook(() => useAlertSocket(TOKEN));

    const url = FakeWebSocket.latest().url;
    expect(url).toContain('/ws/alerts?');
    expect(url).toContain(`token=${TOKEN}`);
    expect(url).not.toContain('symbols=');
  });

  it('reports "open" once the socket connects', async () => {
    const { result } = renderHook(() => useAlertSocket(TOKEN));

    act(() => {
      FakeWebSocket.latest().open();
    });

    await waitFor(() => {
      expect(result.current.status).toBe('open');
    });
  });

  it('maps an alert.raised message onto the REST Alert shape', async () => {
    const { result } = renderHook(() => useAlertSocket(TOKEN));
    act(() => {
      FakeWebSocket.latest().open();
    });

    act(() => {
      FakeWebSocket.latest().message({
        event_type: 'alert.raised',
        alert_id: 'alert-1',
        alert_rule_id: 'rule-1',
        symbol: 'AAPL',
        triggering_event_type: 'anomaly.detected',
        severity: 'warning',
        title: 'Unusual activity in AAPL',
        body: 'AAPL scored 6.0 against a threshold of 5.0.',
        payload: { anomaly_score: '6.0' },
        occurred_at: '2026-03-02T15:30:00Z',
      });
    });

    await waitFor(() => {
      expect(result.current.liveAlerts).toHaveLength(1);
    });
    const [alert] = result.current.liveAlerts;
    expect(alert?.id).toBe('alert-1');
    expect(alert?.event_type).toBe('anomaly.detected');
    expect(alert?.acknowledged_at).toBeNull();
  });

  it('ignores a message that is not an alert.raised event', () => {
    const { result } = renderHook(() => useAlertSocket(TOKEN));
    act(() => {
      FakeWebSocket.latest().open();
    });

    act(() => {
      FakeWebSocket.latest().message({ event_type: 'market.trade', symbol: 'AAPL' });
    });

    expect(result.current.liveAlerts).toHaveLength(0);
  });

  it('deduplicates a redelivered alert by id', async () => {
    const { result } = renderHook(() => useAlertSocket(TOKEN));
    act(() => {
      FakeWebSocket.latest().open();
    });

    const message = {
      event_type: 'alert.raised',
      alert_id: 'alert-1',
      alert_rule_id: 'rule-1',
      symbol: 'AAPL',
      triggering_event_type: 'anomaly.detected',
      severity: 'warning',
      title: 'Unusual activity in AAPL',
      body: 'AAPL scored 6.0 against a threshold of 5.0.',
      payload: {},
      occurred_at: '2026-03-02T15:30:00Z',
    };
    act(() => {
      FakeWebSocket.latest().message(message);
      FakeWebSocket.latest().message(message);
    });

    await waitFor(() => {
      expect(result.current.liveAlerts).toHaveLength(1);
    });
  });
});
