#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

pg_bin() {
  local name="$1"
  if command -v "$name" >/dev/null 2>&1; then
    command -v "$name"
    return 0
  fi
  if [ -d /usr/lib/postgresql ]; then
    find /usr/lib/postgresql -path "*/bin/${name}" -type f 2>/dev/null | sort -V | tail -n 1
  fi
  return 0
}

INITDB="$(pg_bin initdb)"
PG_CTL="$(pg_bin pg_ctl)"
PSQL="$(pg_bin psql)"
CREATEDB="$(pg_bin createdb)"
PG_BASEBACKUP="$(pg_bin pg_basebackup)"
PG_VERIFYBACKUP="$(pg_bin pg_verifybackup)"

if [ -z "$INITDB" ] || [ -z "$PG_CTL" ] || [ -z "$PSQL" ] || [ -z "$CREATEDB" ] || [ -z "$PG_BASEBACKUP" ] || [ -z "$PG_VERIFYBACKUP" ]; then
  echo "[test_restore_drill_dry] PostgreSQL server binaries missing; skipping"
  exit 0
fi
if [ "$(id -u)" -eq 0 ] && ! id -u postgres >/dev/null 2>&1; then
  echo "[test_restore_drill_dry] root execution requires postgres user; skipping"
  exit 0
fi

tmp_dir="$(mktemp -d)"
source_dir="${tmp_dir}/source"
socket_dir="${tmp_dir}/socket"
backup_dir="${tmp_dir}/base"
wal_dir="${tmp_dir}/wal"
drill_dir="${tmp_dir}/drills"
work_root="${tmp_dir}/work"
source_port="$((20000 + RANDOM % 10000))"
restore_port="$((30001 + RANDOM % 10000))"
pgbouncer_port="$((40001 + RANDOM % 10000))"
bin_dir="$(dirname "$PSQL")"
mkdir -p "$socket_dir" "$backup_dir" "$wal_dir" "$drill_dir" "$work_root"

run_pg() {
  if [ "$(id -u)" -eq 0 ] && id -u postgres >/dev/null 2>&1; then
    runuser -u postgres -- "$@"
  else
    "$@"
  fi
}

cleanup() {
  run_pg "$PG_CTL" -D "$source_dir" -m fast stop >/dev/null 2>&1 || true
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

if [ "$(id -u)" -eq 0 ] && id -u postgres >/dev/null 2>&1; then
  chown -R postgres:postgres "$tmp_dir"
fi

run_pg "$INITDB" -D "$source_dir" -A trust -U postgres --no-locale --data-checksums >/dev/null
cat >> "${source_dir}/postgresql.conf" <<EOF
listen_addresses = '127.0.0.1'
port = ${source_port}
unix_socket_directories = '${socket_dir}'
EOF

run_pg "$PG_CTL" -D "$source_dir" -l "${tmp_dir}/source.log" -w start >/dev/null
run_pg "$CREATEDB" -h "$socket_dir" -p "$source_port" -U postgres trading
run_pg "$PSQL" -h "$socket_dir" -p "$source_port" -U postgres -d trading -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
CREATE SCHEMA trading;
SET search_path=trading,public;
CREATE TABLE model_registry(id bigserial primary key, created_ts_ms bigint not null);
CREATE TABLE decision_log(id bigserial primary key, ts_ms bigint not null);
CREATE TABLE broker_fills(id bigserial primary key, ts_ms bigint not null);
INSERT INTO model_registry(created_ts_ms) VALUES ((EXTRACT(EPOCH FROM now())::bigint) * 1000);
INSERT INTO decision_log(ts_ms) VALUES ((EXTRACT(EPOCH FROM now())::bigint) * 1000);
INSERT INTO broker_fills(ts_ms) VALUES ((EXTRACT(EPOCH FROM now())::bigint) * 1000);
SQL

run_pg env \
  PATH="${bin_dir}:${PATH}" \
  TS_BACKUP_BASE_DIR="$backup_dir" \
  PGHOST="$socket_dir" \
  PGPORT="$source_port" \
  PGUSER=postgres \
  PGBASEBACKUP_BIN="$PG_BASEBACKUP" \
  PGVERIFYBACKUP_BIN="$PG_VERIFYBACKUP" \
  bash "${REPO_ROOT}/ops/backup/base_backup.sh"

run_pg env \
  PATH="${bin_dir}:${PATH}" \
  TS_RESTORE_BASE_BACKUP_DIR="$backup_dir" \
  TS_BACKUP_WAL_DIR="$wal_dir" \
  TS_RESTORE_DRILL_DIR="$drill_dir" \
  TS_RESTORE_DRILL_WORK_ROOT="$work_root" \
  TS_RESTORE_DRILL_ALLOW_DIRECT=1 \
  TS_RESTORE_PORT="$restore_port" \
  TS_RESTORE_PGBOUNCER_PORT="$pgbouncer_port" \
  TS_RESTORE_DB=trading \
  TS_RESTORE_USER=postgres \
  PGCTL_BIN="$PG_CTL" \
  PGVERIFYBACKUP_BIN="$PG_VERIFYBACKUP" \
  bash "${REPO_ROOT}/ops/backup/restore_drill.sh"

report="$(find "$drill_dir" -maxdepth 1 -name 'restore_drill_*.txt' -type f | sort | tail -n 1)"
[ -n "$report" ] || {
  echo "restore drill report missing" >&2
  exit 1
}
grep -q '^status=pass$' "$report"
grep -q 'restore_sanity_pass' "$report"

echo "[test_restore_drill_dry] ok"
