/**
 * Route definitions (Phase 5, ADR-0010).
 *
 * `/login` and `/register` are public; everything else sits behind `ProtectedRoute`, which
 * redirects to `/login` for a session `useAuth` has not (yet) confirmed.
 */

import { Navigate, Route, Routes } from 'react-router-dom';

import { Dashboard } from './components/Dashboard';
import { LoginPage } from './components/LoginPage';
import { ProtectedRoute } from './components/ProtectedRoute';
import { RegisterPage } from './components/RegisterPage';
import './App.css';

export function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/register" element={<RegisterPage />} />
      <Route element={<ProtectedRoute />}>
        <Route path="/" element={<Dashboard />} />
      </Route>
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}
