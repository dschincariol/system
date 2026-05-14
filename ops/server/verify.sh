#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TRADING_USER="${TRADING_USER:-trading}"
TRADING_GROUP="${TRADING_GROUP:-trading}"
DATA_ROOT="${TRADING_DATA_ROOT:-/var/lib/trading}"
DB_DIR="${TRADING_DB_DIR:-${DATA_ROOT}/db}"
REDIS_DIR="${TRADING_REDIS_DIR:-${DATA_ROOT}/redis}"
ARTIFACT_DIR="${TRADING_ARTIFACT_DIR:-${DATA_ROOT}/artifacts}"
NLP_MODELS_DIR="${TRADING_NLP_MODELS_DIR:-${DATA_ROOT}/nlp_models}"
APP_LOG_DIR="${TRADING_APP_LOG_DIR:-${DATA_ROOT}/logs}"
BACKUP_ROOT="${TRADING_BACKUP_ROOT:-/var/backups/trading}"
BACKUP_BASE_DIR="${TRADING_BACKUP_BASE_DIR:-${BACKUP_ROOT}/base}"
BACKUP_WAL_DIR="${TRADING_BACKUP_WAL_DIR:-${BACKUP_ROOT}/wal}"
ETC_DIR="${TRADING_ETC_DIR:-/etc/trading}"
CREDSTORE_DIR="${TRADING_CREDSTORE_DIR:-/etc/credstore.encrypted}"
POSTGRES_DB="${TRADING_POSTGRES_DB:-trading}"
POSTGRES_SOCKET_DIR="${TRADING_POSTGRES_SOCKET_DIR:-/var/run/postgresql}"
PGBOUNCER_PORT="${TRADING_PGBOUNCER_PORT:-6432}"
REDIS_SOCKET="${TRADING_REDIS_SOCKET:-/var/run/redis/trading.sock}"
SYSTEMD_DIR="${TRADING_SYSTEMD_DIR:-/etc/systemd/system}"

log() {
  printf '[verify] %s\n' "$*"
}

fail() {
  printf '[verify] ERROR: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "missing command: $1"
}

check_postgres() {
  require_command psql
  log "checking PostgreSQL socket"
  local result
  result="$(runuser -u postgres -- psql -h "$POSTGRES_SOCKET_DIR" -p 5432 -d "$POSTGRES_DB" -Atqc 'SELECT 1')"
  [ "$result" = "1" ] || fail "PostgreSQL SELECT 1 returned ${result}"

  log "checking TimescaleDB extension"
  local extversion
  extversion="$(runuser -u postgres -- psql -h "$POSTGRES_SOCKET_DIR" -p 5432 -d "$POSTGRES_DB" -Atqc "SELECT extversion FROM pg_extension WHERE extname='timescaledb'")"
  [ -n "$extversion" ] || fail "TimescaleDB extension is not installed in ${POSTGRES_DB}"
  log "TimescaleDB ${extversion}"
}

check_redis() {
  require_command redis-cli
  log "checking Redis socket"
  local pong
  pong="$(redis-cli -s "$REDIS_SOCKET" PING)"
  [ "$pong" = "PONG" ] || fail "Redis PING returned ${pong}"
}

check_pgbouncer() {
  require_command psql
  require_command systemd-creds
  log "checking PgBouncer socket"
  [ -r "${CREDSTORE_DIR}/pg_password_app.cred" ] || fail "missing ${CREDSTORE_DIR}/pg_password_app.cred"
  local password result
  password="$(systemd-creds decrypt "${CREDSTORE_DIR}/pg_password_app.cred" -)"
  result="$(PGPASSWORD="$password" psql -h "$POSTGRES_SOCKET_DIR" -p "$PGBOUNCER_PORT" -U ts_app -d "$POSTGRES_DB" -Atqc 'SELECT 1')"
  [ "$result" = "1" ] || fail "PgBouncer SELECT 1 returned ${result}"
}

check_dir() {
  local path="$1"
  [ -d "$path" ] || fail "missing directory ${path}"

  local owner group mode
  owner="$(stat -c '%U' "$path")"
  group="$(stat -c '%G' "$path")"
  mode="$(stat -c '%a' "$path")"

  [ "$owner" = "$TRADING_USER" ] || fail "${path} owner=${owner}, expected ${TRADING_USER}"
  [ "$group" = "$TRADING_GROUP" ] || fail "${path} group=${group}, expected ${TRADING_GROUP}"
  [ "$mode" = "750" ] || fail "${path} mode=${mode}, expected 750"
}

check_filesystem() {
  log "checking filesystem layout"
  local dir
  for dir in \
    "$DATA_ROOT" \
    "$DB_DIR" \
    "$REDIS_DIR" \
    "$ARTIFACT_DIR" \
    "$NLP_MODELS_DIR" \
    "$APP_LOG_DIR" \
    "$BACKUP_ROOT" \
    "$BACKUP_BASE_DIR" \
    "$BACKUP_WAL_DIR" \
    "$ETC_DIR"
  do
    check_dir "$dir"
  done
  [ -d "$CREDSTORE_DIR" ] || fail "missing directory ${CREDSTORE_DIR}"
  [ "$(stat -c '%U' "$CREDSTORE_DIR")" = "root" ] || fail "${CREDSTORE_DIR} owner must be root"
  [ "$(stat -c '%a' "$CREDSTORE_DIR")" = "700" ] || fail "${CREDSTORE_DIR} mode must be 700"
}

check_systemd_units() {
  require_command systemd-analyze
  log "checking systemd unit syntax"
  local source_dir="$SYSTEMD_DIR"
  if [ ! -f "${source_dir}/trading-api.service" ]; then
    source_dir="${SCRIPT_DIR}/systemd"
  fi

  local unit
  for unit in trading-api.service trading-jobs.service trading-stream-prices.service trading-ingest.service trading.target; do
    [ -f "${source_dir}/${unit}" ] || fail "missing systemd unit ${source_dir}/${unit}"
    systemd-analyze verify "${source_dir}/${unit}"
  done
}

main() {
  check_postgres
  check_redis
  check_pgbouncer
  check_filesystem
  check_systemd_units
  log "all checks passed"
}

main "$@"
