/**
 * Live trades, quotes and closed bars for one symbol, over `/ws/market` (Phase 4).
 *
 * This is additive to the REST path, never a replacement for it: `MarketView` still fetches
 * bars and the latest price over HTTP on mount, exactly as it did before Phase 4. This hook
 * only pushes *updates* on top of that baseline — the four-state pattern (loading / error /
 * empty / success) documented in `MarketView.tsx` still governs the initial render, and this
 * hook adds a fifth, independent axis: whether the live feed is currently connected.
 *
 * **Reconnection.** A closed socket reconnects with exponential backoff (1s, 2s, 4s, ... capped
 * at 15s) rather than giving up — a network blip or an API redeploy must not permanently
 * silence live updates until the user reloads the page.
 *
 * **Resync.** The server drops a slow client's backlog and sends one `market.resync` message
 * instead (failure F19, see `api/routes/websocket.py`). This hook does not try to reconstruct
 * what was missed — it cannot, the messages are gone — it surfaces `resyncedAt`, and
 * `MarketView` treats that exactly like "reconnected": re-fetch the REST endpoints, which are
 * the source of truth this feed only ever supplements.
 *
 * **One `useReducer`, not five `useState`s.** Every socket event (a new symbol, a status
 * change, a trade) touches more than one field of the returned state at once — a trade is
 * both the new "latest trade" and a new head of the tape. Five independent `useState` calls
 * would mean firing several state updates per event, which is also exactly what pulled a
 * fresh, evolving `react-hooks/set-state-in-effect` lint rule: calling `setState` more than
 * once synchronously inside an effect body invites the cascading-render problem that rule
 * exists to catch. One `dispatch` per event sidesteps the question rather than suppressing
 * the rule.
 *
 * **The trade tape dedupes by `event_id`.** `/ws/market` carries Kafka's at-least-once
 * guarantee all the way to the browser: the broadcaster has no `consumer_progress` tracking
 * of its own (see `application/broadcasting/service.py` — publishing is naturally
 * idempotent from the *system's* point of view, because nothing durable accumulates from
 * it), so a broadcaster restart that redelivers a trade republishes the identical message,
 * `event_id` and all. That is harmless for `latestTrade` (the same value overwriting
 * itself), but the trade tape is a *list* — appending a redelivered trade without checking
 * would show the same print twice, which a person watching the tape absolutely would
 * notice. `event_id` is TradeWatch's deterministic, natural-key-derived identity for an
 * event (ADR-0012) — the same one that makes `UNIQUE(event_id)` work in PostgreSQL — so
 * checking it here is the same idempotency principle applied at the very last hop, not a
 * new one invented for the frontend.
 */

import { useEffect, useReducer, useRef } from 'react';

import { wsUrl } from '../config';

export type ConnectionStatus = 'connecting' | 'open' | 'reconnecting' | 'closed';

interface MarketMessageBase {
  event_type: string;
  event_id?: string;
  symbol?: string;
  occurred_at?: string;
}

export interface TradeMessage extends MarketMessageBase {
  event_type: 'market.trade';
  event_id: string;
  symbol: string;
  price: string;
  size: string;
  occurred_at: string;
}

export interface QuoteMessage extends MarketMessageBase {
  event_type: 'market.quote';
  symbol: string;
  bid_price: string;
  ask_price: string;
  occurred_at: string;
}

export interface BarMessage extends MarketMessageBase {
  event_type: 'market.bar.closed' | 'market.bar.corrected';
  symbol: string;
  interval: string;
  close: string;
}

export type MarketMessage = TradeMessage | QuoteMessage | BarMessage | MarketMessageBase;

/**
 * A trade as the tape displays it: no `eslint-plugin-react` in this project means no
 * `no-array-index-key` rule to lean on, but the real reason not to key by index is that a
 * prepend-and-truncate list reassigns every index on every tick anyway. `occurred_at` alone
 * is not unique enough (two trades can share a second), so each trade is tagged with a
 * client-side sequence number the moment it arrives.
 */
export interface TapeEntry extends TradeMessage {
  tapeKey: number;
}

//: How many recent trades the tape keeps. A trade tape is a glance at recent activity, not
//: an archive — REST/the database is where history actually lives.
const TRADE_TAPE_LIMIT = 25;

const INITIAL_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 15000;

export interface MarketSocketState {
  status: ConnectionStatus;
  latestTrade: TradeMessage | null;
  latestQuote: QuoteMessage | null;
  latestBar: BarMessage | null;
  trades: TapeEntry[];
  /** Bumps every time the server signals a dropped backlog. Treat like "just reconnected." */
  resyncedAt: number;
}

type Action =
  | { type: 'symbol_changed' }
  | { type: 'status'; status: ConnectionStatus }
  | { type: 'message'; message: MarketMessage };

interface InternalState extends MarketSocketState {
  /** A pure, ever-increasing counter — the source of each `TapeEntry.tapeKey`. */
  nextTapeKey: number;
}

const initialState: InternalState = {
  status: 'connecting',
  latestTrade: null,
  latestQuote: null,
  latestBar: null,
  trades: [],
  resyncedAt: 0,
  nextTapeKey: 0,
};

function isTrade(message: MarketMessage): message is TradeMessage {
  return message.event_type === 'market.trade';
}

function isQuote(message: MarketMessage): message is QuoteMessage {
  return message.event_type === 'market.quote';
}

function isBar(message: MarketMessage): message is BarMessage {
  return (
    message.event_type === 'market.bar.closed' ||
    message.event_type === 'market.bar.corrected'
  );
}

function reducer(state: InternalState, action: Action): InternalState {
  switch (action.type) {
    case 'symbol_changed':
      // The previous connection's data no longer describes what is on screen. `status` is
      // set separately, right after this, by the `connect()` call that always follows.
      return { ...initialState, status: state.status };
    case 'status':
      return { ...state, status: action.status };
    case 'message': {
      const { message } = action;
      if (message.event_type === 'market.resync') {
        return { ...state, resyncedAt: Date.now() };
      }
      if (isTrade(message)) {
        // Redelivered after a broadcaster restart (no `consumer_progress` — see the module
        // docstring): already on the tape once, so `latestTrade` still takes the new
        // message (identical value, harmless), but the tape itself does not grow.
        const alreadyShown = state.trades.some(
          (entry) => entry.event_id === message.event_id,
        );
        if (alreadyShown) {
          return { ...state, latestTrade: message };
        }
        const entry: TapeEntry = { ...message, tapeKey: state.nextTapeKey };
        return {
          ...state,
          latestTrade: message,
          trades: [entry, ...state.trades].slice(0, TRADE_TAPE_LIMIT),
          nextTapeKey: state.nextTapeKey + 1,
        };
      }
      if (isQuote(message)) {
        return { ...state, latestQuote: message };
      }
      if (isBar(message)) {
        return { ...state, latestBar: message };
      }
      return state;
    }
  }
}

/**
 * @param symbol The symbol to subscribe to.
 * @param token A current access token (Phase 5, ADR-0010). `/ws/market` verifies it during
 *   the handshake and closes the connection the instant it expires — a browser `WebSocket`
 *   cannot set an `Authorization` header, so it travels as a query parameter instead
 *   (`api/routes/websocket.py`). `null` while `useAuth` is still resolving the session means
 *   this hook simply does not connect yet, the same way it already waits for `symbol`.
 */
export function useMarketSocket(symbol: string, token: string | null): MarketSocketState {
  const [state, dispatch] = useReducer(reducer, initialState);

  // A ref, not state: attempt count drives reconnect timing and must not itself trigger a
  // render — only `status` should.
  const attemptRef = useRef(0);

  useEffect(() => {
    dispatch({ type: 'symbol_changed' });
    attemptRef.current = 0;

    // No symbol resolved yet, or no token yet (the session is still loading, or refreshing
    // after an expiry) — nothing to subscribe with. `MarketView` calls this hook
    // unconditionally (rules of hooks), before either is necessarily known, so the guard
    // belongs here rather than at the call site.
    if (!symbol || !token) {
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
      const params = new URLSearchParams({ symbols: symbol, token: token ?? '' });
      socket = new WebSocket(`${wsUrl('/ws/market')}?${params.toString()}`);

      socket.onopen = () => {
        attemptRef.current = 0;
        dispatch({ type: 'status', status: 'open' });
      };

      socket.onmessage = (event: MessageEvent<string>) => {
        let message: MarketMessage;
        try {
          message = JSON.parse(event.data) as MarketMessage;
        } catch {
          return;
        }
        dispatch({ type: 'message', message });
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

      // A socket error is always followed by `close` in browsers; closing explicitly here
      // just means the reconnect logic lives in exactly one place instead of two.
      socket.onerror = () => socket?.close();
    }

    connect();

    return () => {
      cancelled = true;
      clearTimeout(reconnectTimer);
      dispatch({ type: 'status', status: 'closed' });
      socket?.close();
    };
    // Reconnecting on every `token` rotation (roughly every 14.5 minutes — see `useAuth`'s
    // refresh margin) rather than only at actual expiry is the simpler of two correct
    // options: the alternative is a token kept in a ref so only a *stale* connection reads
    // the new value, which buys smoother reconnects at the cost of a second moving part.
    // Worth it if the periodic "connecting" flicker this causes is ever reported as
    // noticeable; skipped here as the simplest thing that is still correct.
  }, [symbol, token]);

  return state;
}
