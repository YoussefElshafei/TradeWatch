#!/usr/bin/env bash
# Run every check CI runs, in the same order, so a green local run means a green pipeline.
#
#   ./scripts/check.sh            # everything
#   ./scripts/check.sh --backend  # backend only
#   ./scripts/check.sh --frontend # frontend only
#   ./scripts/check.sh --fix      # apply formatting and auto-fixable lint first

set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
run_backend=false
run_frontend=false
fix=false

for arg in "$@"; do
  case "$arg" in
    --backend)  run_backend=true ;;
    --frontend) run_frontend=true ;;
    --fix)      fix=true ;;
    -h|--help)  sed -n '2,8p' "$0"; exit 0 ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

# Neither flag given means both.
if [[ "$run_backend" == false && "$run_frontend" == false ]]; then
  run_backend=true
  run_frontend=true
fi

failures=()

step() {
  local name="$1" directory="$2"
  shift 2
  printf '\n\033[36m==> %s\033[0m\n' "$name"
  if ! (cd "$repo_root/$directory" && "$@"); then
    failures+=("$name")
    printf '\033[31m    FAILED\033[0m\n'
  fi
}

if [[ "$run_backend" == true ]]; then
  if [[ "$fix" == true ]]; then
    step 'backend: format'     backend uv run ruff format .
    step 'backend: lint --fix' backend uv run ruff check --fix .
  fi
  step 'backend: lint'         backend uv run ruff check .
  step 'backend: format check' backend uv run ruff format --check .
  step 'backend: types'        backend uv run mypy
  step 'backend: layering'     backend uv run lint-imports
  # `provider` is excluded explicitly: an -m on the command line overrides the one in
  # pyproject's addopts, so without naming it here a developer with credentials exported
  # would have their local check run hit the network.
  step 'backend: tests'        backend uv run pytest -m 'not integration and not e2e and not provider'
  step 'backend: integration'  backend uv run pytest -m integration
fi

if [[ "$run_frontend" == true ]]; then
  if [[ "$fix" == true ]]; then
    step 'frontend: format'     frontend npm run format
    step 'frontend: lint --fix' frontend npm run lint:fix
  fi
  step 'frontend: lint'         frontend npm run lint
  step 'frontend: format check' frontend npm run format:check
  step 'frontend: types'        frontend npm run typecheck
  step 'frontend: tests'        frontend npm run test
fi

echo
if (( ${#failures[@]} > 0 )); then
  printf '\033[31m%d check(s) failed:\033[0m\n' "${#failures[@]}"
  printf '  - %s\n' "${failures[@]}"
  exit 1
fi

printf '\033[32mAll checks passed.\033[0m\n'
