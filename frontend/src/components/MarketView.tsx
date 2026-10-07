/**
 * The market data view.
 *
 * The visible end of the vertical slice: a symbol selector, a price chart, and the latest
 * stored price. Everything it shows travelled the whole pipeline — synthetic provider →
 * normalization → event bus → processor → PostgreSQL → API → here.
 *
 * It is deliberately not the TradeWatch dashboard. No layout system, no theming beyond what
 * the stylesheet already had, no watchlists. The point is to prove the data path, and every
 * extra surface here is a surface to rewrite when the real dashboard is designed.
 *
 * What it does take seriously is the four states a real view has to handle, because getting
 * them wrong is what makes a data UI feel broken:
 *
 * * **loading** — say so, and keep the layout stable so the page does not jump;
 * * **error** — say what failed and offer a retry, rather than rendering nothing;
 * * **empty** — a known symbol with no bars yet is a normal Sunday, not a failure, and it
 *   must not look like one;
 * * **success** — the chart.
 */

import { useCallback, useEffect, useState } from 'react';

import {
  type BarSeries,
  type Instrument,
  type InstrumentList,
  type LatestPrice,
  fetchBars,
  fetchInstruments,
  fetchLatestPrice,
} from '../api/market';
import { useAsync } from '../hooks/useAsync';
import { useAuth } from '../hooks/useAuth';
import {
  type ConnectionStatus,
  type MarketSocketState,
  useMarketSocket,
} from '../hooks/useMarketSocket';
import { formatMoney, formatTimestamp } from '../lib/format';
import { PriceChart } from './PriceChart';

const BAR_LIMIT = 240;

const STATUS_LABEL: Record<ConnectionStatus, string> = {
  connecting: 'Connecting…',
  open: 'Live',
  reconnecting: 'Reconnecting…',
  closed: 'Offline',
};

function LiveBadge({ status }: { status: ConnectionStatus }) {
  return (
    <span className={`live-badge live-badge-${status}`} role="status">
      <span aria-hidden="true" className="live-dot" />
      {STATUS_LABEL[status]}
    </span>
  );
}

export function Message({
  tone,
  title,
  detail,
  action,
}: {
  tone: 'info' | 'error';
  title: string;
  detail?: string;
  action?: { label: string; onClick: () => void };
}) {
  return (
    <div className={`panel panel-${tone}`} role={tone === 'error' ? 'alert' : 'status'}>
      <p className="panel-title">{title}</p>
      {detail && <p className="panel-detail">{detail}</p>}
      {action && (
        <button type="button" onClick={action.onClick}>
          {action.label}
        </button>
      )}
    </div>
  );
}

function LatestPricePanel({
  symbol,
  socket,
}: {
  symbol: string;
  socket: MarketSocketState;
}) {
  const { state, reload } = useAsync<LatestPrice>(
    useCallback((signal) => fetchLatestPrice(symbol, signal), [symbol]),
    [symbol],
  );

  // The socket told us we may have missed messages (F19) — the REST endpoint is the source
  // of truth this feed only ever supplements, so re-fetch it rather than trying to guess
  // what was lost.
  useEffect(() => {
    if (socket.resyncedAt) reload();
    // `reload` is re-created every render; only a *new* resync should trigger this effect.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [socket.resyncedAt]);

  if (state.status !== 'success') return null;
  const latest = state.data;

  // A live trade/quote always wins over the REST snapshot once one has arrived: it is
  // strictly newer than whatever `fetchLatestPrice` returned, by construction.
  const price = socket.latestTrade?.price ?? latest.price;
  const bid = socket.latestQuote?.bid_price ?? latest.bid_price;
  const ask = socket.latestQuote?.ask_price ?? latest.ask_price;

  return (
    <dl className="quote" aria-label={`${symbol} latest price`}>
      <div>
        <dt>Last</dt>
        <dd data-testid="latest-price">
          {/* Keying on the price value remounts this span on every tick, which replays the
              CSS flash animation — a cheap, JS-free way to make a live price feel alive. */}
          <span key={price ?? 'none'} className="price-flash">
            {price ? formatMoney(price) : 'no trades yet'}
          </span>
        </dd>
      </div>
      <div>
        <dt>Bid / Ask</dt>
        <dd>
          {bid && ask ? `${formatMoney(bid)} / ${formatMoney(ask)}` : 'no quotes yet'}
        </dd>
      </div>
      <div>
        <dt>Source</dt>
        <dd>{latest.source ?? 'unknown'}</dd>
      </div>
    </dl>
  );
}

function TradeTape({
  symbol,
  trades,
}: {
  symbol: string;
  trades: MarketSocketState['trades'];
}) {
  if (trades.length === 0) return null;

  return (
    <div className="panel trade-tape" aria-label={`${symbol} recent trades`}>
      <p className="panel-title">Trade tape</p>
      <ul>
        {trades.map((trade) => (
          <li key={trade.tapeKey}>
            <span className="trade-tape-time">
              {formatTimestamp(trade.occurred_at, 'time')}
            </span>
            <span className="trade-tape-price">{formatMoney(trade.price)}</span>
            <span className="trade-tape-size">×{formatMoney(trade.size)}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function SymbolSelector({
  instruments,
  selected,
  onSelect,
}: {
  instruments: Instrument[];
  selected: string;
  onSelect: (symbol: string) => void;
}) {
  return (
    <label className="symbol-selector">
      <span>Symbol</span>
      <select
        value={selected}
        onChange={(event) => {
          onSelect(event.target.value);
        }}
      >
        {instruments.map((instrument) => (
          <option key={instrument.symbol} value={instrument.symbol}>
            {instrument.symbol}
            {instrument.name ? ` — ${instrument.name}` : ''}
          </option>
        ))}
      </select>
    </label>
  );
}

function BarsPanel({ symbol, socket }: { symbol: string; socket: MarketSocketState }) {
  const { state, reload } = useAsync<BarSeries>(
    useCallback((signal) => fetchBars({ symbol, limit: BAR_LIMIT }, signal), [symbol]),
    [symbol],
  );

  // A closed bar just landed, or the socket dropped a backlog (F19): either way, the
  // cached-then-invalidated REST series (see `application/broadcasting/service.py`) is
  // fresher than what is on screen. `latestBar` fires often at live market rates, so this
  // refetches the *chart*, not on every trade — the price panel above updates every tick on
  // its own, from the socket directly, with no REST round trip.
  useEffect(() => {
    if (socket.latestBar || socket.resyncedAt) reload();
    // See LatestPricePanel's identical note: `reload` is intentionally not a dependency.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [socket.latestBar, socket.resyncedAt]);

  if (state.status === 'loading') {
    return <Message tone="info" title={`Loading ${symbol}…`} />;
  }

  if (state.status === 'error') {
    return (
      <Message
        tone="error"
        title="Could not load market data"
        detail={state.error.message}
        action={{ label: 'Try again', onClick: reload }}
      />
    );
  }

  if (state.data.count === 0) {
    return (
      <Message
        tone="info"
        title={`No bars for ${symbol} in this window`}
        detail="The symbol is known but has no stored data yet. Run the ingestion pipeline to populate it."
        action={{ label: 'Refresh', onClick: reload }}
      />
    );
  }

  return <PriceChart bars={state.data.bars} symbol={symbol} />;
}

export function MarketView() {
  const [selected, setSelected] = useState<string>('');
  const { state, reload } = useAsync<InstrumentList>(
    useCallback((signal) => fetchInstruments(signal), []),
    [],
  );

  // Falling back to the first instrument rather than storing it in state keeps the source
  // of truth in one place: a symbol that disappears from the list cannot leave the view
  // pointing at nothing.
  const instruments = state.status === 'success' ? state.data.instruments : [];
  // `selected` starts as `''`, which is deliberately falsy-but-not-nullish here, so this
  // stays an explicit emptiness check rather than `??` (which would not treat `''` as
  // "nothing chosen yet" and never fall back to the first instrument).
  const symbol = selected !== '' ? selected : (instruments[0]?.symbol ?? '');

  // Called unconditionally, before the early returns below, because hooks must run in the
  // same order every render — `useMarketSocket` itself no-ops until `symbol` and
  // `accessToken` both resolve (Phase 5: `/ws/market` now requires one).
  const { accessToken } = useAuth();
  const socket = useMarketSocket(symbol, accessToken);

  if (state.status === 'loading') {
    return <Message tone="info" title="Loading instruments…" />;
  }

  if (state.status === 'error') {
    return (
      <Message
        tone="error"
        title="Could not reach the TradeWatch API"
        detail={state.error.message}
        action={{ label: 'Try again', onClick: reload }}
      />
    );
  }

  if (!symbol) {
    return (
      <Message
        tone="info"
        title="No instruments yet"
        detail="The database has no instruments. Start the ingestion pipeline to register some."
        action={{ label: 'Refresh', onClick: reload }}
      />
    );
  }

  return (
    <section className="market" aria-labelledby="market-heading">
      <div className="market-header">
        <h2 id="market-heading">{symbol}</h2>
        <div className="market-header-controls">
          <LiveBadge status={socket.status} />
          <SymbolSelector
            instruments={instruments}
            selected={symbol}
            onSelect={setSelected}
          />
        </div>
      </div>
      <LatestPricePanel symbol={symbol} socket={socket} />
      <BarsPanel symbol={symbol} socket={socket} />
      <TradeTape symbol={symbol} trades={socket.trades} />
    </section>
  );
}
