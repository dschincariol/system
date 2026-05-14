#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

mkdir -p logs data

if ! command -v python >/dev/null 2>&1; then
  echo "[operator] ERROR: python not found in PATH"
  exit 1
fi

if ! command -v node >/dev/null 2>&1; then
  echo "[operator] ERROR: node not found in PATH"
  exit 1
fi

# Ensure node deps are present using lockfile when available
if [ ! -f "node_modules/express/package.json" ]; then
  if [ -f "package-lock.json" ]; then
    echo "[operator] node_modules missing; running npm ci..."
    npm ci
  else
    echo "[operator] node_modules missing; running npm install..."
    npm install
  fi
fi

export OPERATOR_AUTO_START="${OPERATOR_AUTO_START:-1}"

# Shell launcher is a thin convenience wrapper. The Node operator owns the actual
# process supervision and readiness behavior after startup.

# Open operator UI (best-effort)
if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "http://127.0.0.1:4001/" >/dev/null 2>&1 || true
elif command -v open >/dev/null 2>&1; then
  open "http://127.0.0.1:4001/" >/dev/null 2>&1 || true
fi

node boot/operator_server.js
