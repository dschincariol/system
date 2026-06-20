#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="${TRADING_LOGS:-${LOG_DIR:-$REPO_ROOT/var/log}}"
STATE_DIR="${TRADING_LOCAL_LOGROTATE_STATE_DIR:-$REPO_ROOT/var/tmp}"
MAX_SIZE="${TRADING_LOCAL_LOGROTATE_MAX_SIZE:-50M}"
ROTATE_COUNT="${TRADING_LOCAL_LOGROTATE_ROTATE:-5}"
MAX_AGE="${TRADING_LOCAL_LOGROTATE_MAXAGE:-14}"
QUIET=0
FORCE=0
DRY_RUN=0

usage() {
  cat <<'USAGE'
Usage: deploy/bin/rotate_local_logs.sh [--check|--dry-run] [--force] [--quiet]

Rotates local development logs under TRADING_LOGS/LOG_DIR or ./var/log using
the same logrotate mechanics as deploy/logrotate/trading-system.
USAGE
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --check|--dry-run)
      DRY_RUN=1
      ;;
    --force)
      FORCE=1
      ;;
    --quiet)
      QUIET=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown_arg:$1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if ! command -v logrotate >/dev/null 2>&1; then
  [ "$QUIET" -eq 1 ] || echo "logrotate_not_found; skipping local log rotation" >&2
  exit 0
fi

mkdir -p "$LOG_DIR" "$STATE_DIR"
CONFIG_FILE="$STATE_DIR/logrotate-local-trading-system.conf"
STATE_FILE="$STATE_DIR/logrotate-local-trading-system.status"

cat >"$CONFIG_FILE" <<EOF
$LOG_DIR/*.log
$LOG_DIR/*.out
$LOG_DIR/*.err
$LOG_DIR/*.jsonl
$REPO_ROOT/boot/*.log
$REPO_ROOT/data/ai_operator_log.jsonl {
    daily
    maxsize $MAX_SIZE
    rotate $ROTATE_COUNT
    maxage $MAX_AGE
    missingok
    notifempty
    compress
    delaycompress
    copytruncate
}
EOF

args=(-s "$STATE_FILE")
if [ "$DRY_RUN" -eq 1 ]; then
  args=(-d "${args[@]}")
fi
if [ "$FORCE" -eq 1 ]; then
  args=(-f "${args[@]}")
fi

if [ "$QUIET" -eq 1 ]; then
  logrotate "${args[@]}" "$CONFIG_FILE" >/dev/null
else
  logrotate "${args[@]}" "$CONFIG_FILE"
fi
