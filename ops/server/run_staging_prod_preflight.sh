#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[run-staging-prod-preflight] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

APP_ROOT="${TRADING_APP_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
PYTHON_BIN="${TRADING_PYTHON_BIN:-${APP_ROOT}/.venv/bin/python}"
ENV_FILE="${STAGING_PREFLIGHT_ENV_FILE:-${APP_ROOT}/deploy/env/staging-prod-preflight.env}"
EVIDENCE_DIR="${STAGING_PREFLIGHT_EVIDENCE_DIR:-${APP_ROOT}/var/artifacts/preflight}"
TARGET_ENV="${STAGING_PREFLIGHT_TARGET_ENV:-staging}"
TIMEOUT_S="${PREFLIGHT_SMOKE_TIMEOUT_S:-900}"

if [ ! -x "$PYTHON_BIN" ]; then
  PYTHON_BIN="${PYTHON:-python}"
fi

if [ ! -r "$ENV_FILE" ]; then
  echo "[run-staging-prod-preflight] missing readable env file: $ENV_FILE" >&2
  echo "[run-staging-prod-preflight] copy deploy/env/staging-prod-preflight.env.example and fill staging values outside Git" >&2
  exit 1
fi

cd "$APP_ROOT"
exec "$PYTHON_BIN" -m engine.runtime.staging_prod_preflight \
  --env-file "$ENV_FILE" \
  --target-env "$TARGET_ENV" \
  --evidence-dir "$EVIDENCE_DIR" \
  --timeout-s "$TIMEOUT_S" \
  "$@"
