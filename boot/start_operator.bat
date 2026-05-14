@echo off
setlocal enableextensions

REM ------------------------------------------------------------
REM Resolve project root
REM ------------------------------------------------------------
cd /d "%~dp0\.."
set PROJECT_ROOT=%cd%

echo.
echo ==========================================
echo   Trading System Operator Launcher
echo ==========================================
echo Root: %PROJECT_ROOT%
echo.

REM ------------------------------------------------------------
REM Ensure Python exists
REM ------------------------------------------------------------
set PYTHON_EXE=%PROJECT_ROOT%\.venv\Scripts\python.exe
if exist "%PYTHON_EXE%" (
  "%PYTHON_EXE%" --version >nul 2>nul
) else (
  set PYTHON_EXE=python
  where python >nul 2>nul
  if errorlevel 1 (
    echo [startup] ERROR: Python not found in .venv or PATH
    pause
    exit /b 1
  )
)
set OPERATOR_PYTHON=%PYTHON_EXE%

REM ------------------------------------------------------------
REM Ensure Node exists
REM ------------------------------------------------------------
where node >nul 2>nul
if errorlevel 1 (
  echo [startup] ERROR: Node.js not installed or not in PATH
  pause
  exit /b 1
)

REM ------------------------------------------------------------
REM Ensure runtime folders + DB path exist
REM ------------------------------------------------------------
if "%DB_PATH%"=="" (
  set DB_PATH=%PROJECT_ROOT%\data\trading.db
)

if not exist "%PROJECT_ROOT%\data" (
  mkdir "%PROJECT_ROOT%\data"
)

if not exist "%PROJECT_ROOT%\logs" (
  mkdir "%PROJECT_ROOT%\logs"
)

set LAUNCHER_LOCK_DIR=%PROJECT_ROOT%\data\operator_launcher.lock
set LAUNCHER_LOCK_FILE=%LAUNCHER_LOCK_DIR%\owner.txt

echo [startup] DB_PATH=%DB_PATH%

set OPERATOR_URL=http://127.0.0.1:4001/
set OPERATOR_PING_URL=http://127.0.0.1:4001/api/operator/ping
set DASHBOARD_URL=http://127.0.0.1:8000/ui/dashboard.html

REM ------------------------------------------------------------
REM Ensure Node dependencies exist using lockfile when available
REM ------------------------------------------------------------
if not exist "%PROJECT_ROOT%\node_modules\express\package.json" (
  if exist "%PROJECT_ROOT%\package-lock.json" (
    echo [startup] node_modules missing; running npm ci...
    npm ci
  ) else (
    echo [startup] node_modules missing; running npm install...
    npm install
  )
  if errorlevel 1 (
    echo [startup] ERROR: npm dependency install failed
    pause
    exit /b 1
  )
)

REM ------------------------------------------------------------
REM Acquire single-instance launcher lock
REM ------------------------------------------------------------
2>nul mkdir "%LAUNCHER_LOCK_DIR%"
if errorlevel 1 (
  powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$ProgressPreference='SilentlyContinue';" ^
    "try {" ^
    "  $r = Invoke-WebRequest -UseBasicParsing -Uri '%OPERATOR_PING_URL%' -TimeoutSec 2;" ^
    "  if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) { exit 0 } else { exit 1 }" ^
    "} catch { exit 1 }"
  if not errorlevel 1 (
    echo [startup] launcher already active; reusing operator on 127.0.0.1:4001
    start "" "%OPERATOR_URL%"
    exit /b 0
  )
  echo [startup] removing stale launcher lock...
  rmdir /s /q "%LAUNCHER_LOCK_DIR%" >nul 2>nul
  2>nul mkdir "%LAUNCHER_LOCK_DIR%"
  if errorlevel 1 (
    echo [startup] ERROR: could not acquire launcher lock
    pause
    exit /b 1
  )
)
echo %DATE% %TIME% pid=%RANDOM% > "%LAUNCHER_LOCK_FILE%"

REM The batch file is only a convenience launcher. The operator server remains
REM the long-lived control plane once startup is handed off.

REM ------------------------------------------------------------
REM Clear stale operator restart state for clean launcher ownership
REM ------------------------------------------------------------
if exist "%PROJECT_ROOT%\data\operator.state.json" (
  del /f /q "%PROJECT_ROOT%\data\operator.state.json" >nul 2>nul
)

REM ------------------------------------------------------------
REM Reuse existing operator server if already listening
REM ------------------------------------------------------------
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ProgressPreference='SilentlyContinue';" ^
  "try {" ^
  "  $r = Invoke-WebRequest -UseBasicParsing -Uri '%OPERATOR_PING_URL%' -TimeoutSec 2;" ^
  "  if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) { exit 0 } else { exit 1 }" ^
  "} catch { exit 1 }"
if not errorlevel 1 (
  echo [startup] operator server already responding on 127.0.0.1:4001
  start "" "%OPERATOR_URL%"
  exit /b 0
)

REM ------------------------------------------------------------
REM Open Operator UI after HTTP readiness, not before bind
REM ------------------------------------------------------------
start "" powershell -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -Command ^
  "$ProgressPreference='SilentlyContinue';" ^
  "$deadline=(Get-Date).AddSeconds(45);" ^
  "while ((Get-Date) -lt $deadline) {" ^
  "  try {" ^
  "    $r = Invoke-WebRequest -UseBasicParsing -Uri '%OPERATOR_PING_URL%' -TimeoutSec 2;" ^
  "    if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) {" ^
  "      Start-Process '%OPERATOR_URL%';" ^
  "      exit 0" ^
  "    }" ^
  "  } catch {}" ^
  "  Start-Sleep -Milliseconds 500" ^
  "}" ^
  "exit 0"

REM ------------------------------------------------------------
REM Start engine after operator responds, outside the Node event loop
REM ------------------------------------------------------------
set OPERATOR_AUTO_START=0
set OPERATOR_AUTORESTART=false
set TRADING_SKIP_RUNTIME_GRAPH_CHECK=1
set OPERATOR_DISABLE_INTERNAL_ENGINE_START=1

start "" powershell -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -Command ^
  "$ProgressPreference='SilentlyContinue';" ^
  "$deadline=(Get-Date).AddSeconds(60);" ^
  "while ((Get-Date) -lt $deadline) {" ^
  "  try {" ^
  "    $r = Invoke-WebRequest -UseBasicParsing -Uri '%OPERATOR_PING_URL%' -TimeoutSec 2;" ^
  "    if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) {" ^
  "      $runtimeActive = $false;" ^
  "      if (Test-Path 'logs\\runtime.pid') {" ^
  "        try {" ^
  "          $runtimeRecord = Get-Content 'logs\\runtime.pid' | ConvertFrom-Json;" ^
  "          $runtimePid = [int]($runtimeRecord.pid);" ^
  "          if ($runtimePid -gt 0 -and (Get-Process -Id $runtimePid -ErrorAction SilentlyContinue)) { $runtimeActive = $true }" ^
  "        } catch {}" ^
  "      }" ^
  "      if (-not $runtimeActive) {" ^
  "        Start-Process '%PYTHON_EXE%' -ArgumentList 'start_system.py','safe' -WorkingDirectory '%PROJECT_ROOT%' -WindowStyle Hidden;" ^
  "      }" ^
  "      exit 0" ^
  "    }" ^
  "  } catch {}" ^
  "  Start-Sleep -Milliseconds 500" ^
  "}" ^
  "exit 0"

REM Dashboard open is best-effort convenience only; operator/engine startup does
REM not depend on the browser succeeding.

start "" powershell -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -Command ^
  "$ProgressPreference='SilentlyContinue';" ^
  "$deadline=(Get-Date).AddMinutes(3);" ^
  "while ((Get-Date) -lt $deadline) {" ^
  "  try {" ^
  "    $r = Invoke-WebRequest -UseBasicParsing -Uri '%DASHBOARD_URL%' -TimeoutSec 2;" ^
  "    if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) {" ^
  "      Start-Process '%DASHBOARD_URL%';" ^
  "      exit 0" ^
  "    }" ^
  "  } catch {}" ^
  "  Start-Sleep -Seconds 1" ^
  "}" ^
  "exit 0"

echo [startup] starting operator server...
node boot\operator_server.js

if errorlevel 1 (
  rmdir /s /q "%LAUNCHER_LOCK_DIR%" >nul 2>nul
  echo.
  echo [startup] ERROR: operator server crashed
  pause
  exit /b 1
)

rmdir /s /q "%LAUNCHER_LOCK_DIR%" >nul 2>nul
echo.
echo [startup] operator server exited
pause
