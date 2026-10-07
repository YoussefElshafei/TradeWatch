/**
 * The persistent app shell (frontend design pass): sidebar navigation, the signed-in
 * account control, and the page content area, wrapping every route behind `ProtectedRoute`.
 *
 * Before this, each page (`Dashboard`, `AlertCenter`, `StrategiesPage`, ...) rendered its
 * own full-width `<main className="app">` with no shared chrome, so the product read as a
 * set of separately-built screens rather than one tool. This is the fix: one `<nav>`, one
 * account control, one footer, and every route renders inside `<Outlet />` as a plain page
 * body — no page needs to know the shell exists.
 *
 * Collapses to an icon rail below `--breakpoint-laptop` (CSS only, see `App.css`) rather
 * than a full mobile redesign — this is a desktop data tool, not a phone app.
 */

import type { ReactNode } from 'react';
import { NavLink, Outlet } from 'react-router-dom';

import { config } from '../config';
import { useAuth } from '../hooks/useAuth';

interface NavItem {
  to: string;
  label: string;
  icon: ReactNode;
}

/** Minimal inline SVGs, 20x20, `currentColor` stroke — one stylesheet dependency fewer than
 * pulling in an icon-font or icon-component library for eight glyphs. */
const ICONS = {
  dashboard: <path d="M3 13h5V3H3v10Zm0 4h5v-2H3v2Zm7 0h7V9h-7v8Zm0-14v3h7V3h-7Z" />,
  market: <path d="M3 17l4.5-6 3.5 4L17 4M17 4h-4M17 4v4" />,
  strategies: <path d="M4 4h12M4 10h12M4 16h7M15 15l3 3-3 3" />,
  backtests: <path d="M10 3a7 7 0 1 0 7 7h-7V3Z M14 3.5A7 7 0 0 1 17.5 7H14V3.5Z" />,
  portfolios: <path d="M3 7l7-4 7 4-7 4-7-4Zm0 6l7 4 7-4M3 10l7 4 7-4" />,
  alerts: (
    <path d="M10 2a5 5 0 0 0-5 5v3l-2 3h14l-2-3V7a5 5 0 0 0-5-5Zm-2 13a2 2 0 0 0 4 0" />
  ),
  anomalies: <path d="M3 10h3l2-6 4 12 2-6h3" />,
} as const;

function NavIcon({ name }: { name: keyof typeof ICONS }) {
  return (
    <svg
      viewBox="0 0 20 20"
      width="20"
      height="20"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.6"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      className="nav-icon"
    >
      {ICONS[name]}
    </svg>
  );
}

const NAV_ITEMS: NavItem[] = [
  { to: '/', label: 'Dashboard', icon: <NavIcon name="dashboard" /> },
  { to: '/market', label: 'Market', icon: <NavIcon name="market" /> },
  { to: '/strategies', label: 'Strategies', icon: <NavIcon name="strategies" /> },
  { to: '/backtests', label: 'Backtests', icon: <NavIcon name="backtests" /> },
  { to: '/portfolios', label: 'Portfolios', icon: <NavIcon name="portfolios" /> },
  { to: '/alerts', label: 'Alerts', icon: <NavIcon name="alerts" /> },
  { to: '/anomalies', label: 'Anomalies', icon: <NavIcon name="anomalies" /> },
];

export function AppShell() {
  const { user, logout } = useAuth();

  return (
    <div className="shell">
      <nav className="shell-nav" aria-label="Primary">
        <div className="shell-brand">
          <h1>TradeWatch</h1>
          <p className="shell-brand-tagline">Market intelligence &amp; paper trading</p>
        </div>

        <ul className="shell-nav-list">
          {NAV_ITEMS.map((item) => (
            <li key={item.to}>
              <NavLink
                to={item.to}
                end={item.to === '/'}
                className={({ isActive }) =>
                  isActive ? 'shell-nav-link shell-nav-link-active' : 'shell-nav-link'
                }
              >
                {item.icon}
                <span className="shell-nav-label">{item.label}</span>
              </NavLink>
            </li>
          ))}
        </ul>

        {user && (
          <div className="shell-account">
            <span className="shell-account-email" title={user.email}>
              {user.email}
            </span>
            <button type="button" onClick={() => void logout()}>
              Sign out
            </button>
          </div>
        )}
      </nav>

      <div className="shell-content">
        <Outlet />

        <footer className="shell-footer">
          <p>
            Phase 5 — accounts, sessions, and per-user access control sit in front of the
            Phase 1–4 pipeline. API at <code>{config.apiBaseUrl}</code>.
          </p>
        </footer>
      </div>
    </div>
  );
}
