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
  echo "[test_base_backup_verify] PostgreSQL server binaries missing; skipping"
  exit 0
fi
if [ "$(id -u)" -eq 0 ] && ! id -u postgres >/dev/null 2>&1; then
  echo "[test_base_backup_verify] root execution requires postgres user; skipping"
  exit 0
fi

tmp_dir="$(mktemp -d)"
source_dir="${tmp_dir}/source"
socket_dir="${tmp_dir}/socket"
backup_dir="${tmp_dir}/base"
port="$((20000 + RANDOM % 20000))"
mkdir -p "$socket_dir" "$backup_dir"

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
port = ${port}
unix_socket_directories = '${socket_dir}'
EOF

run_pg "$PG_CTL" -D "$source_dir" -l "${tmp_dir}/source.log" -w start >/dev/null
run_pg "$CREATEDB" -h "$socket_dir" -p "$port" -U postgres trading
run_pg "$PSQL" -h "$socket_dir" -p "$port" -U postgres -d trading -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
CREATE SCHEMA trading;
SET search_path=trading,public;
CREATE TABLE model_registry(id bigserial primary key, created_ts_ms bigint not null default 0);
INSERT INTO model_registry(created_ts_ms) VALUES (0);
SQL

run_pg env \
  TS_BACKUP_BASE_DIR="$backup_dir" \
  PGHOST="$socket_dir" \
  PGPORT="$port" \
  PGUSER=postgres \
  PGBASEBACKUP_BIN="$PG_BASEBACKUP" \
  PGVERIFYBACKUP_BIN="$PG_VERIFYBACKUP" \
  bash "${REPO_ROOT}/ops/backup/base_backup.sh"

latest="$(readlink -f "${backup_dir}/latest")"
[ -d "$latest" ] || {
  echo "latest symlink did not resolve" >&2
  exit 1
}
[ -s "${latest}/pg_verifybackup.out" ] || {
  echo "pg_verifybackup output missing" >&2
  exit 1
}

printf X | dd of="${latest}/base.tar.gz" bs=1 seek=10 count=1 conv=notrunc status=none
if run_pg env PGVERIFYBACKUP_BIN="$PG_VERIFYBACKUP" bash "${REPO_ROOT}/ops/backup/base_backup.sh" --verify-only "$latest"; then
  echo "corrupt base backup unexpectedly verified" >&2
  exit 1
fi

echo "[test_base_backup_verify] ok"
