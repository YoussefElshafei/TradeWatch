/**
 * Live alert delivery for the signed-in user, over `/ws/alerts` (Phase 9).
 *
 * Simpler than `useMarketSocket`: there is no `symbols`-style subscribe protocol to drive —
 * `/ws/alerts` auto-subscribes the connection to the authenticated user's own id
 * (`api/routes/websocket.py`'s `alert_stream` docstring) — and no resync concept, because an
 * alert is a durable row the REST feed (`api/alerts.ts::fetchAlerts`) can always re-fetch;
 * there is nothing a dropped backlog would need to "resync" beyond a normal reload of that
 * feed. This hook only ever *adds* to what the feed already loaded, never replaces it.
 *
 * Reconnection follows `useMarketSocket`'s exact backoff (1s, 2s, 4s, ... capped at 15s) —
 * the same failure mode (a network blip, an API redeploy) deserves the same behaviour.
 */

import { useEffect, useReducer, useRef } from 'react';

import { wsUrl } from '../config';
import type { Alert } from '../api/alerts';

export type ConnectionStatus = 'connecting' | 'open' | 'reconnecting' | 'closed';

/**
 * The wire shape `domain/events.py::AlertEvent` actually serializes — distinct from the REST
 * `Alert` shape (`api/alerts.ts`): `event_type` here is always `"alert.raised"`
 * (`triggering_event_type` is what names the anomaly/fill that set the rule off), the id
 * field is `alert_id`, and there is no `acknowledged_at` — a freshly published alert has
 * never been acknowledged by definition.
 */
interface AlertRaisedMessage {
  event_type: string;
  alert_id: string;
  alert_rule_id: string;
  symbol: string;
  triggering_event_type: string;
  severity: Alert['severity'];
  title: string;
  body: string;
  payload: Record<string, unknown>;
  occurred_at: string;
}

function toAlert(message: AlertRaisedMessage): Alert {
  return {
    id: message.alert_id,
    alert_rule_id: message.alert_rule_id,
    symbol: message.symbol,
    event_type: message.triggering_event_type,
    severity: message.severity,
    title: message.title,
    body: message.body,
    payload: message.payload,
    created_at: message.occurred_at,
    acknowledged_at: null,
  };
}

const INITIAL_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 15000;

//: How many live alerts the hook keeps beyond what the initial REST fetch loaded. A history
//: beyond this is what `GET /api/v1/alerts` is for.
const LIVE_ALERT_LIMIT = 25;

export interface AlertSocketState {
  status: ConnectionStatus;
  /** Newest first. Only alerts that arrived over this socket, not the REST feed's history. */
  liveAlerts: Alert[];
}

type Action =
  { type: 'status'; status: ConnectionStatus } | { type: 'alert'; alert: Alert };

const initialState: AlertSocketState = { status: 'connecting', liveAlerts: [] };

function reducer(state: AlertSocketState, action: Action): AlertSocketState {
  switch (action.type) {
    case 'status':
      return { ...state, status: action.status };
    case 'alert':
      // The broadcaster has no `consumer_progress` tracking (`application/broadcasting/service.py`),
      // so a broadcaster restart can redeliver the identical alert — dedupe by `id`, the same
      // idempotency principle `useMarketSocket`'s trade tape applies to `event_id`.
      if (state.liveAlerts.some((existing) => existing.id === action.alert.id)) {
        return state;
      }
      return {
        ...state,
        liveAlerts: [action.alert, ...state.liveAlerts].slice(0, LIVE_ALERT_LIMIT),
      };
  }
}

/**
 * @param token A current access token (Phase 5, ADR-0010), or `null` while the session is
 *   still resolving — this hook simply does not connect yet, the same way `useMarketSocket`
 *   waits for both its arguments.
 */
export function useAlertSocket(token: string | null): AlertSocketState {
  const [state, dispatch] = useReducer(reducer, initialState);
  const attemptRef = useRef(0);

  useEffect(() => {
    attemptRef.current = 0;

    if (!token) {
      dispatch({ type: 'status', status: 'closed' });
      return undefined;
    }

    let cancelled = false;
    let socket: WebSocket | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

    function connect(): void {
      dispatch({
        type: 'status',
        status: attemptRef.current === 0 ? 'connecting' : 'reconnecting',
      });
      const params = new URLSearchParams({ token: token ?? '' });
      socket = new WebSocket(`${wsUrl('/ws/alerts')}?${params.toString()}`);

      socket.onopen = () => {
        attemptRef.current = 0;
        dispatch({ type: 'status', status: 'open' });
      };

      socket.onmessage = (event: MessageEvent<string>) => {
        let message: AlertRaisedMessage;
        try {
          message = JSON.parse(event.data) as AlertRaisedMessage;
        } catch {
          return;
        }
        if (message.event_type !== 'alert.raised') return;
        dispatch({ type: 'alert', alert: toAlert(message) });
      };

      socket.onclose = () => {
        if (cancelled) return;
        dispatch({ type: 'status', status: 'reconnecting' });
        attemptRef.current += 1;
        const delay = Math.min(
          INITIAL_RECONNECT_DELAY_MS * 2 ** (attemptRef.current - 1),
          MAX_RECONNECT_DELAY_MS,
        );
        reconnectTimer = setTimeout(connect, delay);
      };

      socket.onerror = () => socket?.close();
    }

    connect();

    return () => {
      cancelled = true;
      clearTimeout(reconnectTimer);
      dispatch({ type: 'status', status: 'closed' });
      socket?.close();
    };
  }, [token]);

  return state;
}
