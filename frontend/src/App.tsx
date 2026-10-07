/**
 * Route definitions (Phase 5, ADR-0010).
 *
 * `/login` and `/register` are public; everything else sits behind `ProtectedRoute`, which
 * redirects to `/login` for a session `useAuth` has not (yet) confirmed.
 */

import { Navigate, Route, Routes } from 'react-router-dom';

import { AlertCenter } from './components/AlertCenter';
import { AnomalyFeed } from './components/AnomalyFeed';
import { AppShell } from './components/AppShell';
import { BacktestForm } from './components/BacktestForm';
import { BacktestResultsPage } from './components/BacktestResults';
import { BacktestsPage } from './components/BacktestsPage';
import { Dashboard } from './components/Dashboard';
import { LoginPage } from './components/LoginPage';
import { MarketView } from './components/MarketView';
import { PortfolioDetailPage } from './components/PortfolioDetailPage';
import { PortfoliosPage } from './components/PortfoliosPage';
import { ProtectedRoute } from './components/ProtectedRoute';
import { RegisterPage } from './components/RegisterPage';
import { StrategiesPage } from './components/StrategiesPage';
import { StrategyEditor } from './components/StrategyEditor';
import './App.css';

export function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/register" element={<RegisterPage />} />
      <Route element={<ProtectedRoute />}>
        <Route element={<AppShell />}>
          <Route path="/" element={<Dashboard />} />
          <Route
            path="/market"
            element={
              <main className="page">
                <MarketView />
              </main>
            }
          />
          <Route path="/strategies" element={<StrategiesPage />} />
          <Route path="/strategies/new" element={<StrategyEditor />} />
          <Route path="/backtests" element={<BacktestsPage />} />
          <Route path="/backtests/new" element={<BacktestForm />} />
          <Route path="/backtests/:id" element={<BacktestResultsPage />} />
          <Route path="/portfolios" element={<PortfoliosPage />} />
          <Route path="/portfolios/:id" element={<PortfolioDetailPage />} />
          <Route path="/alerts" element={<AlertCenter />} />
          <Route path="/anomalies" element={<AnomalyFeed />} />
        </Route>
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
