#!/usr/bin/env bash
# First-run setup: install dependencies and create a .env from the example.
#
#   ./scripts/setup.sh

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

echo "==> Checking prerequisites"
command -v uv  >/dev/null || { echo "uv is required: https://docs.astral.sh/uv/"; exit 1; }
command -v npm >/dev/null || { echo "Node.js 22+ is required: https://nodejs.org/"; exit 1; }

if [[ ! -f .env ]]; then
  echo "==> Creating .env from .env.example"
  cp .env.example .env
  echo "    Review it before running against anything that matters."
else
  echo "==> .env already exists, leaving it alone"
fi

echo "==> Installing backend dependencies"
(cd backend && uv sync --all-groups)

echo "==> Installing frontend dependencies"
(cd frontend && npm install)

cat <<'NEXT'

Setup complete. Next:

  docker compose -f docker/docker-compose.yml up -d   # PostgreSQL + Redis
  cd backend  && uv run tradewatch-api                # API on http://127.0.0.1:8000
  cd frontend && npm run dev                          # UI  on http://127.0.0.1:5173

Run every check with ./scripts/check.sh
NEXT
