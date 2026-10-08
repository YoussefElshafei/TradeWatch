#!/usr/bin/env bash
# Start the whole local stack and keep it running until Ctrl+C.
#
#   ./scripts/dev.sh               # infrastructure, migrations, API, every worker, frontend
#   ./scripts/dev.sh --no-frontend # leave the frontend to your own `npm run dev`
#
# Any process that exits (a Kafka eviction after the machine sleeps, a dropped database
# connection) is restarted within a few seconds. Logs: backend/.run-logs/<name>.log.
# On Windows prefer scripts/dev.ps1: it also stops a stack already running from this checkout.

set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
logs="$repo_root/backend/.run-logs"
frontend=true

for arg in "$@"; do
  case "$arg" in
    --no-frontend) frontend=false ;;
    -h|--help)     sed -n '2,9p' "$0"; exit 0 ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

workers=(api producer consumer aggregator broadcaster backtest-runner
         paper-trading anomaly-detector alert-dispatcher)
declare -A pids
mkdir -p "$logs"

echo "==> Starting PostgreSQL, Redis, Kafka"
docker compose -f "$repo_root/docker/docker-compose.yml" up -d --wait || {
  echo "docker compose failed - is Docker running?" >&2; exit 1; }

echo "==> Applying migrations"
(cd "$repo_root/backend" && uv run alembic upgrade head) || exit 1

# No auto-reload for the API here, matching dev.ps1: the supervisor restarts processes, and
# a reload that never completes is worse than an explicit restart after an edit.
export TW_API_RELOAD=false

start_worker() {
  (cd "$repo_root/backend" && exec uv run "tradewatch-$1" >"$logs/$1.log" 2>&1) &
  pids[$1]=$!
}

stop_all() {
  echo "Stopping..."
  kill $(jobs -p) 2>/dev/null
  wait 2>/dev/null
  exit 0
}
trap stop_all INT TERM

for name in "${workers[@]}"; do start_worker "$name"; done
if $frontend; then
  (cd "$repo_root/frontend" && exec npm run dev >"$logs/frontend.log" 2>&1) &
fi

echo
echo "Running: ${workers[*]}"
echo "Site: http://localhost:5173   API: http://localhost:8010/docs   Logs: backend/.run-logs"
echo "Ctrl+C stops everything."

while true; do
  sleep 5
  for name in "${workers[@]}"; do
    if ! kill -0 "${pids[$name]}" 2>/dev/null; then
      echo "$(date +%H:%M:%S) $name exited - restarting; see backend/.run-logs/$name.log"
      start_worker "$name"
    fi
  done
done
