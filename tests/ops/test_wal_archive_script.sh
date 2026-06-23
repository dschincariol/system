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

conflict_src="${tmp_dir}/${wal_name}.conflict"
printf 'different wal payload\n' > "$conflict_src"
if TS_BACKUP_WAL_DIR="$wal_dir" bash "${REPO_ROOT}/ops/backup/wal_archive.sh" "$conflict_src" "$wal_name"; then
  echo "expected archive conflict to fail" >&2
  exit 1
fi
cmp "$src" "${wal_dir}/${wal_name}"

no_python_bin="${tmp_dir}/no-python-bin"
mkdir -p "$no_python_bin"
for cmd in awk bash chmod cmp cp df find id mkdir mv readlink rm sed sort stat sync tr wc; do
  cmd_path="$(command -v "$cmd")"
  ln -s "$cmd_path" "${no_python_bin}/${cmd}"
done
wal_dir_no_python="${tmp_dir}/wal-no-python"
PATH="$no_python_bin" \
TS_BACKUP_WAL_DIR="$wal_dir_no_python" \
  bash "${REPO_ROOT}/ops/backup/wal_archive.sh" "$src" "${wal_name}.nopython"
cmp "$src" "${wal_dir_no_python}/${wal_name}.nopython"

unmounted_root="${tmp_dir}/not-mounted-backup-root"
mkdir -p "$unmounted_root"
if TS_BACKUP_ROOT="$unmounted_root" \
  TS_BACKUP_WAL_DIR="${unmounted_root}/wal" \
  TS_WAL_ARCHIVE_REQUIRE_MOUNT=1 \
  bash "${REPO_ROOT}/ops/backup/wal_archive.sh" "$src" "${wal_name}.mountcheck"; then
  echo "expected archive root mount requirement to fail" >&2
  exit 1
fi

pgdata="${tmp_dir}/pgdata"
mkdir -p "${pgdata}/pg_wal/archive_status"
cp "$src" "${pgdata}/pg_wal/${wal_name}"
touch "${pgdata}/pg_wal/archive_status/${wal_name}.ready"
catchup_wal_dir="${tmp_dir}/catchup-wal"
TS_BACKUP_WAL_DIR="$catchup_wal_dir" \
TS_WAL_ARCHIVE_REQUIRE_MOUNT=0 \
TS_WAL_ARCHIVE_CATCHUP_MIN_FREE_BYTES=0 \
PGDATA="$pgdata" \
TS_WAL_ARCHIVE_SCRIPT="${REPO_ROOT}/ops/backup/wal_archive.sh" \
  bash "${REPO_ROOT}/ops/backup/wal_archive_catchup.sh"
cmp "$src" "${catchup_wal_dir}/${wal_name}"
test ! -f "${pgdata}/pg_wal/archive_status/${wal_name}.ready"
test -f "${pgdata}/pg_wal/archive_status/${wal_name}.done"

PYTHONPATH="$REPO_ROOT" python3 - <<'PY'
from engine.runtime.postgres_tuning import archive_command_uses_audited_script

assert archive_command_uses_audited_script('/opt/trading/ops/backup/wal_archive.sh "%p" "%f"')
assert not archive_command_uses_audited_script("/bin/true")
assert not archive_command_uses_audited_script("mkdir -p /var/backups/trading/wal && cp %p /var/backups/trading/wal/%f")
PY

echo "[test_wal_archive_script] ok"
