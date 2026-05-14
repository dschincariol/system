#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

wal_name="000000010000000000000001"
src="${tmp_dir}/${wal_name}.src"
wal_dir="${tmp_dir}/wal"
offsite_dir="${tmp_dir}/offsite"
mkdir -p "$wal_dir" "$offsite_dir"

python3 - "$src" <<'PY'
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_bytes((b"wal-test-segment\n" * 1024))
PY

TS_BACKUP_WAL_DIR="$wal_dir" \
TS_WAL_OFFSITE_CMD="cat > ${offsite_dir}/<name>" \
  bash "${REPO_ROOT}/ops/backup/wal_archive.sh" "$src" "$wal_name"

cmp "$src" "${wal_dir}/${wal_name}"
cmp "$src" "${offsite_dir}/${wal_name}"

TS_BACKUP_WAL_DIR="$wal_dir" \
TS_WAL_OFFSITE_CMD="cat > ${offsite_dir}/${wal_name}.retry" \
  bash "${REPO_ROOT}/ops/backup/wal_archive.sh" "$src" "$wal_name"

cmp "$src" "${offsite_dir}/${wal_name}.retry"

echo "[test_wal_archive_script] ok"
