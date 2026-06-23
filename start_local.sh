#!/usr/bin/env bash
set -euo pipefail

mkdir -p var/log var/db var/tmp var/artifacts var/audit

ENV_FILE="${TRADING_ENV_FILE:-.env}"
if [ -f "${ENV_FILE}" ]; then
  set -a
  # shellcheck disable=SC1090
  . "${ENV_FILE}"
  set +a
fi

export TRADING_LOGS="${TRADING_LOGS:-$(pwd)/var/log}"
export TRADING_DATA="${TRADING_DATA:-$(pwd)/var/db}"
export DB_PATH="${DB_PATH:-$(pwd)/var/db/trading.db}"
export TRADING_LOCAL_LOG_MAX_BYTES="${TRADING_LOCAL_LOG_MAX_BYTES:-52428800}"
export TRADING_LOCAL_LOG_BACKUP_COUNT="${TRADING_LOCAL_LOG_BACKUP_COUNT:-5}"
export TRADING_LOCAL_LOG_MAX_AGE_DAYS="${TRADING_LOCAL_LOG_MAX_AGE_DAYS:-14}"
export TRADING_LOCAL_LOGROTATE_MAX_SIZE="${TRADING_LOCAL_LOGROTATE_MAX_SIZE:-50M}"
export TRADING_LOCAL_LOGROTATE_ROTATE="${TRADING_LOCAL_LOGROTATE_ROTATE:-5}"
export TRADING_LOCAL_LOGROTATE_MAXAGE="${TRADING_LOCAL_LOGROTATE_MAXAGE:-14}"

LOGROTATE_PID=""
cleanup_logrotate_loop() {
  if [ -n "${LOGROTATE_PID:-}" ]; then
    kill "$LOGROTATE_PID" >/dev/null 2>&1 || true
  fi
}

if [ "${TRADING_LOCAL_LOGROTATE_ENABLED:-1}" = "1" ] && [ -x deploy/bin/rotate_local_logs.sh ]; then
  deploy/bin/rotate_local_logs.sh --quiet || true
  if [ "${TRADING_LOCAL_LOGROTATE_INTERVAL_S:-3600}" != "0" ]; then
    (
      while true; do
        sleep "${TRADING_LOCAL_LOGROTATE_INTERVAL_S:-3600}" || exit 0
        deploy/bin/rotate_local_logs.sh --quiet || true
      done
    ) &
    LOGROTATE_PID="$!"
    trap cleanup_logrotate_loop EXIT
    trap 'cleanup_logrotate_loop; exit 130' INT
    trap 'cleanup_logrotate_loop; exit 143' TERM
  fi
fi

PY="${OPERATOR_PYTHON:-}"
if [ -z "${PY}" ]; then
  if command -v python3 >/dev/null 2>&1; then
    PY="python3"
  else
    PY="python"
  fi
fi

MODE="${ENGINE_MODE:-safe}"

# Run full deterministic bootstrap + dashboard
"$PY" start_system.py "$MODE"
