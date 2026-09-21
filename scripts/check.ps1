# Run every check CI runs, in the same order, so a green local run means a green pipeline.
#
#   pwsh scripts/check.ps1            # everything
#   pwsh scripts/check.ps1 -Backend   # backend only
#   pwsh scripts/check.ps1 -Frontend  # frontend only
#   pwsh scripts/check.ps1 -Fix       # apply formatting and auto-fixable lint first

[CmdletBinding()]
param(
    [switch]$Backend,
    [switch]$Frontend,
    [switch]$Fix
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$failures = @()

function Invoke-Step {
    param([string]$Name, [string]$Directory, [string]$Command)

    Write-Host ''
    Write-Host "==> $Name" -ForegroundColor Cyan
    Push-Location (Join-Path $repoRoot $Directory)
    try {
        Invoke-Expression $Command
        if ($LASTEXITCODE -ne 0) {
            $script:failures += $Name
            Write-Host "    FAILED" -ForegroundColor Red
        }
    } catch {
        $script:failures += $Name
        Write-Host "    FAILED: $_" -ForegroundColor Red
    } finally {
        Pop-Location
    }
}

# Neither switch given means both.
if (-not $Backend -and -not $Frontend) { $Backend = $true; $Frontend = $true }

if ($Backend) {
    if ($Fix) {
        Invoke-Step 'backend: format'      'backend' 'uv run ruff format .'
        Invoke-Step 'backend: lint --fix'  'backend' 'uv run ruff check --fix .'
    }
    Invoke-Step 'backend: lint'         'backend' 'uv run ruff check .'
    Invoke-Step 'backend: format check' 'backend' 'uv run ruff format --check .'
    Invoke-Step 'backend: types'        'backend' 'uv run mypy'
    Invoke-Step 'backend: layering'     'backend' 'uv run lint-imports'
    # 'provider' is excluded explicitly: an -m on the command line overrides the one in
    # pyproject's addopts, so without naming it here a developer with credentials exported
    # would have their local check run hit the network.
    Invoke-Step 'backend: tests'        'backend' 'uv run pytest -m "not integration and not e2e and not provider"'
    Invoke-Step 'backend: integration'  'backend' 'uv run pytest -m integration'
}

if ($Frontend) {
    if ($Fix) {
        Invoke-Step 'frontend: format'     'frontend' 'npm run format'
        Invoke-Step 'frontend: lint --fix' 'frontend' 'npm run lint:fix'
    }
    Invoke-Step 'frontend: lint'         'frontend' 'npm run lint'
    Invoke-Step 'frontend: format check' 'frontend' 'npm run format:check'
    Invoke-Step 'frontend: types'        'frontend' 'npm run typecheck'
    Invoke-Step 'frontend: tests'        'frontend' 'npm run test'
}

Write-Host ''
if ($failures.Count -gt 0) {
    Write-Host "$($failures.Count) check(s) failed:" -ForegroundColor Red
    $failures | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}

Write-Host 'All checks passed.' -ForegroundColor Green
exit 0
