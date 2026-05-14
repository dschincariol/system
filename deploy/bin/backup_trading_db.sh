#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${TRADING_ENV_FILE:-/etc/trading-system/trading.env}"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

TRADING_ROOT="${TRADING_ROOT:-/opt/trading-system}"
TRADING_REPO="${TRADING_REPO:-$TRADING_ROOT/repo}"
TRADING_DATA="${TRADING_DATA:-$TRADING_ROOT/data}"
TRADING_BACKUPS="${TRADING_BACKUPS:-$TRADING_ROOT/backups}"
DB_PATH="${DB_PATH:-$TRADING_DATA/trading.db}"
PYTHON_VENV="${PYTHON_VENV:-$TRADING_ROOT/venv}"

mkdir -p "$TRADING_BACKUPS"

STAMP="$(date +%Y%m%d_%H%M%S)"
TMP_DB="$TRADING_BACKUPS/trading_${STAMP}.db"
FINAL_GZ="$TRADING_BACKUPS/trading_${STAMP}.db.gz"

if [[ ! -f "$DB_PATH" ]]; then
  echo "missing_db:$DB_PATH" >&2
  exit 1
fi

if command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 "$DB_PATH" ".timeout 60000" ".backup '$TMP_DB'"
else
  echo "sqlite3_cli_missing_for_online_backup" >&2
  exit 1
fi

gzip -f "$TMP_DB"

find "$TRADING_BACKUPS" -type f -name 'trading_*.db.gz' -mtime +14 -delete || true

echo "$FINAL_GZ"
