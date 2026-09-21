# First-run setup: install dependencies and create a .env from the example.
#
#   pwsh scripts/setup.ps1

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host '==> Checking prerequisites' -ForegroundColor Cyan
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    throw 'uv is required: https://docs.astral.sh/uv/'
}
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    throw 'Node.js 22+ is required: https://nodejs.org/'
}

if (-not (Test-Path .env)) {
    Write-Host '==> Creating .env from .env.example' -ForegroundColor Cyan
    Copy-Item .env.example .env
    Write-Host '    Review it before running against anything that matters.'
} else {
    Write-Host '==> .env already exists, leaving it alone' -ForegroundColor Cyan
}

Write-Host '==> Installing backend dependencies' -ForegroundColor Cyan
Push-Location backend
uv sync --all-groups
Pop-Location

Write-Host '==> Installing frontend dependencies' -ForegroundColor Cyan
Push-Location frontend
npm install
Pop-Location

Write-Host @'

Setup complete. Next:

  docker compose -f docker/docker-compose.yml up -d   # PostgreSQL + Redis + Kafka
  cd backend  ; uv run tradewatch-api                 # API on http://127.0.0.1:8000
  cd frontend ; npm run dev                           # UI  on http://127.0.0.1:5173

Run every check with pwsh scripts/check.ps1
'@ -ForegroundColor Green
