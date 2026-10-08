# Start the whole local stack and keep it running until Ctrl+C.
#
#   powershell -File scripts/dev.ps1             # infrastructure, migrations, API, every worker, frontend
#   powershell -File scripts/dev.ps1 -NoFrontend # leave the frontend to your own `npm run dev`
#
# Any process that exits (a Kafka eviction after the PC sleeps, a dropped database connection)
# is restarted within a few seconds. Logs: backend/.run-logs/<name>.log and <name>.err.log.
# A stack already running from this checkout is stopped first, so there is never a second
# producer writing different prices into the same topics.

[CmdletBinding()]
param([switch]$NoFrontend)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$backend = Join-Path $repoRoot 'backend'
$frontend = Join-Path $repoRoot 'frontend'
$logs = Join-Path $backend '.run-logs'
New-Item -ItemType Directory -Force $logs | Out-Null

$workers = 'api', 'producer', 'consumer', 'aggregator', 'broadcaster', 'backtest-runner',
           'paper-trading', 'anomaly-detector', 'alert-dispatcher'

function Stop-Tree([int]$ProcessId) {
    # `uv run` and uvicorn's reloader each start children; killing only the parent orphans them.
    # Through cmd, so a child that already exited (taskkill complains on stderr) is not fatal
    # under $ErrorActionPreference = 'Stop' in Windows PowerShell 5.1.
    cmd.exe /c "taskkill /T /F /PID $ProcessId >nul 2>&1"
}

Write-Host '==> Stopping any stack already running from this checkout' -ForegroundColor Cyan
Get-CimInstance Win32_Process |
    Where-Object { $_.Name -like 'tradewatch-*.exe' -and $_.ExecutablePath -like "$backend\*" } |
    ForEach-Object { Stop-Tree $_.ProcessId }

Write-Host '==> Starting PostgreSQL, Redis, Kafka' -ForegroundColor Cyan
docker compose -f (Join-Path $repoRoot 'docker/docker-compose.yml') up -d --wait
if ($LASTEXITCODE -ne 0) { throw 'docker compose failed - is Docker Desktop running?' }

Write-Host '==> Applying migrations' -ForegroundColor Cyan
Push-Location $backend
try { uv run alembic upgrade head; if ($LASTEXITCODE -ne 0) { throw 'migrations failed' } }
finally { Pop-Location }

function Start-Worker([string]$Name) {
    # Start-Process truncates its redirect targets, so a restart used to erase the log of the
    # crash that caused it. Keep the previous run's log beside the new one.
    foreach ($suffix in 'log', 'err.log') {
        $current = Join-Path $logs "$Name.$suffix"
        if ((Test-Path $current) -and (Get-Item $current).Length -gt 0) {
            Move-Item -Force $current (Join-Path $logs ($Name + '.' + $suffix.Replace('log', 'prev.log')))
        }
    }
    $process = Start-Process -FilePath uv -ArgumentList 'run', "tradewatch-$Name" `
        -WorkingDirectory $backend -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $logs "$Name.log") `
        -RedirectStandardError (Join-Path $logs "$Name.err.log")
    $null = $process.Handle  # without a held handle, ExitCode reads as empty once it exits
    $process
}

# No auto-reload for the API here, like every other process. On Windows uvicorn's reloader
# stops the old server with a console Ctrl+C event that never reaches a process started in
# its own hidden window, so the first edit hung the API mid-reload. After changing backend
# code, stop the one process (or this script) and it comes back on the new code.
$env:TW_API_RELOAD = 'false'

$running = @{}
foreach ($name in $workers) { $running[$name] = Start-Worker $name }

$vite = $null
if (-not $NoFrontend) {
    if (Get-NetTCPConnection -LocalPort 5173 -State Listen -ErrorAction SilentlyContinue) {
        Write-Host '    port 5173 is already in use - leaving that frontend alone' -ForegroundColor Yellow
    } else {
        $vite = Start-Process -FilePath npm.cmd -ArgumentList 'run', 'dev' -WorkingDirectory $frontend `
            -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput (Join-Path $logs 'frontend.log') `
            -RedirectStandardError (Join-Path $logs 'frontend.err.log')
    }
}

Write-Host ''
Write-Host "Running: $($workers -join ', ')" -ForegroundColor Green
Write-Host 'Site: http://localhost:5173   API: http://localhost:8010/docs   Logs: backend/.run-logs'
Write-Host 'Ctrl+C stops everything.'

try {
    while ($true) {
        Start-Sleep -Seconds 5
        foreach ($name in $workers) {
            if ($running[$name].HasExited) {
                Write-Host "$(Get-Date -Format HH:mm:ss) $name exited (code $($running[$name].ExitCode)) - restarting; see backend/.run-logs/$name.err.log" -ForegroundColor Yellow
                $running[$name] = Start-Worker $name
            }
        }
    }
} finally {
    Write-Host 'Stopping...' -ForegroundColor Cyan
    foreach ($process in $running.Values) { if (-not $process.HasExited) { Stop-Tree $process.Id } }
    if ($vite -and -not $vite.HasExited) { Stop-Tree $vite.Id }
}
