[CmdletBinding()]
param(
    [string]$DataDir = "",
    [string]$LogsDir = "",
    [ValidateSet("safe", "paper")]
    [string]$EngineMode = "safe",
    [string]$DashboardHost = "127.0.0.1",
    [int]$DashboardPort = 8000,
    [int]$BootTimeoutSeconds = 180,
    [switch]$KeepRunning,
    [switch]$SkipBoot,
    [switch]$SkipSmoke,
    [switch]$SkipSelfTest
)

$ErrorActionPreference = "Stop"
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$python = Join-Path $root ".venv\Scripts\python.exe"

if (-not (Test-Path $python)) {
    throw "python_not_found:$python"
}

if ([string]::IsNullOrWhiteSpace($DataDir)) {
    $DataDir = Join-Path $root "data-staging"
}
if ([string]::IsNullOrWhiteSpace($LogsDir)) {
    $LogsDir = Join-Path $root "logs-staging"
}

New-Item -ItemType Directory -Force -Path $DataDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null
Get-ChildItem -Path $LogsDir -File -ErrorAction SilentlyContinue | Remove-Item -Force -ErrorAction SilentlyContinue

$env:TRADING_DATA = (Resolve-Path $DataDir).Path
$env:DATA_DIR = $env:TRADING_DATA
$env:TRADING_LOGS = (Resolve-Path $LogsDir).Path
$env:LOG_DIR = $env:TRADING_LOGS
$env:DB_PATH = Join-Path $env:TRADING_DATA "trading.db"
$env:ENGINE_MODE = $EngineMode
$env:EXECUTION_MODE = $EngineMode
$env:OPERATOR_MODE = $EngineMode
$env:DASHBOARD_HOST = $DashboardHost
$env:DASHBOARD_PORT = [string]$DashboardPort
$env:ENGINE_SUPERVISED = "1"
$env:PYTHONDONTWRITEBYTECODE = "1"
$env:PYTHONUNBUFFERED = "1"
$env:DATA_SOURCE_MASTER_KEY = if ([string]::IsNullOrWhiteSpace($env:DATA_SOURCE_MASTER_KEY)) { "staging-local-master-key" } else { $env:DATA_SOURCE_MASTER_KEY }
$env:TRADING_VALIDATION_MODE = "full"
$env:AUTO_PIPELINE = "0"
$env:AUTO_PIPELINE_INCLUDE_EXECUTION = "0"
$env:AUTO_STARTUP_BOOTSTRAP = "0"
$env:DEFAULT_SYMBOLS = "SPY,QQQ,AAPL,MSFT,GLD"
$env:DEFAULT_SYMBOLS_INCLUDE_SEEDS = "0"
$env:DEFAULT_SYMBOLS_SEC_TOP_N = "0"
$env:LIVE_PRICE_PROVIDER_CHAIN = "yfinance"
$env:YFINANCE_ENABLED = "1"
$env:POLYGON_WS_ENABLED = "0"
$env:POLYGON_REST_ENABLED = "0"
$env:IBKR_ENABLED = "0"
$env:CCXT_ENABLED = "0"
$env:TRADIER_ENABLED = "0"
$env:GDELT_SYMBOL_LIMIT = "0"
$env:RSS_MAX_ITEMS_PER_SOURCE = "5"
$env:PIPELINE_SMOKE_TIMEOUT_S = "300"
$env:PIPELINE_SMOKE_JOBS = "update_universe,label_due_events,compute_drift"
$healthUrl = "http://$DashboardHost`:$DashboardPort/api/health"
$shutdownUrl = "http://$DashboardHost`:$DashboardPort/api/server/shutdown"
$bootStdout = Join-Path $env:TRADING_LOGS "start_system.stdout.log"
$bootStderr = Join-Path $env:TRADING_LOGS "start_system.stderr.log"
$runtimePidPath = Join-Path $env:TRADING_LOGS "runtime.pid"
$ingestionPidPath = Join-Path $env:TRADING_LOGS "ingestion.pid"
$bootProcess = $null

function Get-DotenvValue {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Key
    )

    if (-not (Test-Path $Path)) {
        return ""
    }

    foreach ($line in Get-Content -LiteralPath $Path -ErrorAction SilentlyContinue) {
        if ($line -match "^\s*$Key=(.*)$") {
            return ($Matches[1] | Out-String).Trim()
        }
    }

    return ""
}

function Invoke-Step {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    Write-Host ""
    Write-Host "== $Name =="
    & $python @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "step_failed:$Name:exit_code=$LASTEXITCODE"
    }
}

function Get-HealthSnapshot {
    param(
        [Parameter(Mandatory = $true)][string]$Url
    )

    try {
        return Invoke-RestMethod -Uri $Url -Method Get -TimeoutSec 5
    } catch {
        return $null
    }
}

function Wait-ForHealth {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        $snap = Get-HealthSnapshot -Url $Url
        if ($null -ne $snap) {
            return $snap
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $deadline)

    throw "boot_timeout:health_endpoint_not_ready:$Url"
}

function Stop-StagingRuntime {
    if ($null -eq $bootProcess) {
        return
    }

    if ($bootProcess.HasExited) {
        return
    }

    try {
        Invoke-RestMethod -Uri $shutdownUrl -Method Post -TimeoutSec 10 | Out-Null
    } catch {
    }

    if (-not $bootProcess.WaitForExit(30000)) {
        try {
            Stop-Process -Id $bootProcess.Id -Force
        } catch {
        }
    }
}

function Clear-StagingPidFile {
    param(
        [Parameter(Mandatory = $true)][string]$PidPath
    )

    if (-not (Test-Path $PidPath)) {
        return
    }

    $raw = (Get-Content -LiteralPath $PidPath -ErrorAction SilentlyContinue | Select-Object -First 1 | Out-String).Trim()
    $pidValue = 0
    [void][int]::TryParse($raw, [ref]$pidValue)
    if ($pidValue -gt 0) {
        try {
            $proc = Get-Process -Id $pidValue -ErrorAction Stop
            Stop-Process -Id $proc.Id -Force
            Start-Sleep -Milliseconds 500
        } catch {
        }
    }

    Remove-Item -LiteralPath $PidPath -Force -ErrorAction SilentlyContinue
}

function Stop-RepoPythonProcesses {
    param(
        [Parameter(Mandatory = $true)][string]$RepoRoot
    )

    $script = @'
import os
import sys

repo_root = os.path.abspath(sys.argv[1])

try:
    import psutil
except Exception:
    psutil = None

if psutil is None:
    raise SystemExit(0)

current_pid = os.getpid()
targets = []

for proc in psutil.process_iter(["pid", "name", "exe", "cmdline", "cwd"]):
    try:
        pid = int(proc.info.get("pid") or 0)
        if pid <= 0 or pid == current_pid:
            continue

        name = str(proc.info.get("name") or "").lower()
        exe = str(proc.info.get("exe") or "").lower()
        if "python" not in name and "python" not in exe:
            continue

        cmdline = " ".join(str(part) for part in (proc.info.get("cmdline") or []) if part)
        cwd = str(proc.info.get("cwd") or "")
        if repo_root.lower() not in cmdline.lower() and os.path.abspath(cwd).lower() != repo_root.lower():
            continue

        targets.append(pid)
    except Exception as exc:
        sys.stderr.write(f"[staging_e2e] process_scan_failed pid={pid}: {type(exc).__name__}: {exc}\n")
        continue

for pid in sorted(set(targets)):
    try:
        psutil.Process(pid).kill()
    except Exception as exc:
        sys.stderr.write(f"[staging_e2e] kill_failed pid={pid}: {type(exc).__name__}: {exc}\n")
'@

    $script | & $python - $RepoRoot | Out-Null
    Start-Sleep -Milliseconds 500
}

function Clear-StagingJobLocks {
    param(
        [Parameter(Mandatory = $true)][string]$DbPath
    )

    if (-not (Test-Path $DbPath)) {
        return
    }

    $script = @'
import sqlite3
import sys

db_path = sys.argv[1]
con = sqlite3.connect(db_path)
try:
    for table in ("job_locks", "job_heartbeats"):
        try:
            con.execute(f"DELETE FROM {table}")
        except Exception as exc:
            sys.stderr.write(f"[staging_e2e] clear_table_failed table={table}: {type(exc).__name__}: {exc}\n")
    try:
        con.execute("DELETE FROM price_feed_lock")
    except Exception as exc:
        sys.stderr.write(f"[staging_e2e] clear_table_failed table=price_feed_lock: {type(exc).__name__}: {exc}\n")
    con.commit()
finally:
    con.close()
'@

    $script | & $python - $DbPath | Out-Null
}

Write-Host "Staging E2E configuration"
Write-Host "root      : $root"
Write-Host "python    : $python"
Write-Host "data dir  : $env:TRADING_DATA"
Write-Host "logs dir  : $env:TRADING_LOGS"
Write-Host "db path   : $env:DB_PATH"
Write-Host "mode      : $EngineMode"
Write-Host "health    : $healthUrl"

if ([string]::IsNullOrWhiteSpace($env:DASHBOARD_API_TOKEN)) {
    $env:DASHBOARD_API_TOKEN = Get-DotenvValue -Path (Join-Path $root ".env") -Key "DASHBOARD_API_TOKEN"
}

$defaultDataDir = (Join-Path $root "data-staging")
if ((Test-Path -LiteralPath $defaultDataDir) -and ((Resolve-Path $DataDir).Path -eq (Resolve-Path $defaultDataDir).Path)) {
    foreach ($dbArtifact in @($env:DB_PATH, "$($env:DB_PATH)-wal", "$($env:DB_PATH)-shm")) {
        if (Test-Path -LiteralPath $dbArtifact) {
            Remove-Item -LiteralPath $dbArtifact -Force -ErrorAction SilentlyContinue
        }
    }
}

try {
    Invoke-Step -Name "Syntax Check" -Arguments @("tools\syntax_check_workspace.py")
    Invoke-Step -Name "Runtime Graph Check" -Arguments @("tools\runtime_graph_check.py", "--mode", "full")

    if (-not $SkipBoot) {
        Write-Host ""
        Write-Host "== Boot =="
        Stop-RepoPythonProcesses -RepoRoot $root
        Clear-StagingPidFile -PidPath $runtimePidPath
        Clear-StagingPidFile -PidPath $ingestionPidPath
        Clear-StagingJobLocks -DbPath $env:DB_PATH
        if (Test-Path $bootStdout) {
            Remove-Item -LiteralPath $bootStdout -Force
        }
        if (Test-Path $bootStderr) {
            Remove-Item -LiteralPath $bootStderr -Force
        }
        $bootProcess = Start-Process `
            -FilePath $python `
            -ArgumentList @("start_system.py") `
            -WorkingDirectory $root `
            -RedirectStandardOutput $bootStdout `
            -RedirectStandardError $bootStderr `
            -PassThru

        Write-Host "start_system pid : $($bootProcess.Id)"
        $health = Wait-ForHealth -Url $healthUrl -TimeoutSeconds $BootTimeoutSeconds
        $lifecycle = ($health.body.lifecycle.state | Out-String).Trim()
        Write-Host "health ready     : yes"
        Write-Host "lifecycle state  : $lifecycle"
    }

    if (-not $SkipSmoke) {
        $env:PIPELINE_SMOKE_BASE = "http://$DashboardHost`:$DashboardPort"
        $env:PIPELINE_SMOKE_SKIP_OPERATOR = "1"
        Invoke-Step -Name "Pipeline Smoke Test" -Arguments @("tools\pipeline_smoke_test.py")
    }

    if (-not $SkipSelfTest) {
        Invoke-Step -Name "Production Self-Test" -Arguments @("-m", "engine.runtime.prod_selftest")
    }

    Write-Host ""
    Write-Host "Staging E2E complete."
    Write-Host "stdout log : $bootStdout"
    Write-Host "stderr log : $bootStderr"
}
finally {
    if ((-not $KeepRunning) -and (-not $SkipBoot)) {
        Stop-StagingRuntime
    }
}
