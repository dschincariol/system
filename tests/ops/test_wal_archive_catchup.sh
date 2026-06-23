#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

pgdata="${tmp_dir}/pgdata"
pg_wal="${pgdata}/pg_wal"
status_dir="${pg_wal}/archive_status"
wal_dir="${tmp_dir}/archive/wal"
mkdir -p "$status_dir" "$wal_dir"

wal_a="0000000100000000000000A1"
wal_b="0000000100000000000000A2"
printf 'segment-a\n' > "${pg_wal}/${wal_a}"
printf 'segment-b\n' > "${pg_wal}/${wal_b}"
touch "${status_dir}/${wal_a}.ready" "${status_dir}/${wal_b}.ready"

TS_BACKUP_WAL_DIR="$wal_dir" \
TS_WAL_ARCHIVE_REQUIRE_MOUNT=0 \
TS_WAL_ARCHIVE_CATCHUP_MIN_FREE_BYTES=0 \
PGDATA="$pgdata" \
TS_WAL_ARCHIVE_SCRIPT="${REPO_ROOT}/ops/backup/wal_archive.sh" \
  bash "${REPO_ROOT}/ops/backup/wal_archive_catchup.sh"

cmp "${pg_wal}/${wal_a}" "${wal_dir}/${wal_a}"
cmp "${pg_wal}/${wal_b}" "${wal_dir}/${wal_b}"
test ! -f "${status_dir}/${wal_a}.ready"
test ! -f "${status_dir}/${wal_b}.ready"
test -f "${status_dir}/${wal_a}.done"
test -f "${status_dir}/${wal_b}.done"

wal_c="0000000100000000000000A3"
printf 'segment-c\n' > "${pg_wal}/${wal_c}"
touch "${status_dir}/${wal_c}.ready"

set +e
output="$(
  TS_BACKUP_WAL_DIR="$wal_dir" \
  TS_WAL_ARCHIVE_REQUIRE_MOUNT=0 \
  TS_WAL_ARCHIVE_CATCHUP_MIN_FREE_BYTES=1000000000000000 \
  PGDATA="$pgdata" \
  TS_WAL_ARCHIVE_SCRIPT="${REPO_ROOT}/ops/backup/wal_archive.sh" \
    bash "${REPO_ROOT}/ops/backup/wal_archive_catchup.sh" 2>&1
)"
status="$?"
set -e

if [ "$status" -eq 0 ]; then
  echo "expected catchup free-space guard to fail" >&2
  exit 1
fi
grep -F "wal_archive_catchup_insufficient_space" <<<"$output"
test -f "${status_dir}/${wal_c}.ready"
test ! -f "${wal_dir}/${wal_c}"

echo "[test_wal_archive_catchup] ok"
