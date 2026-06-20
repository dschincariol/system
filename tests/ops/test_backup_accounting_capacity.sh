#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

backup_root="${tmp_dir}/backups"
base_dir="${backup_root}/base"
wal_dir="${backup_root}/wal"
mkdir -p "$base_dir/2026-06-20T000000Z" "$wal_dir"

head -c 1024 </dev/zero > "${wal_dir}/000000010000000000000001"
head -c 2048 </dev/zero > "${wal_dir}/000000010000000000000002"
touch -d '12 hours ago' "${wal_dir}/000000010000000000000001"
touch -d '1 hour ago' "${wal_dir}/000000010000000000000002"

output="$(
  TRADING_BACKUP_ROOT="$backup_root" \
  TS_BACKUP_MAX_BYTES=2K \
  TS_BACKUP_WAL_OBSERVATION_DAYS=1 \
    bash "${REPO_ROOT}/ops/backup/accounting.sh"
)"

grep -q 'event=backup_accounting ' <<<"$output"
grep -q 'budget_bytes=2048' <<<"$output"
grep -q 'over_budget=1' <<<"$output"
grep -q 'observed_wal_bytes_per_day=3072' <<<"$output"
grep -q 'projected_days_to_full=' <<<"$output"
grep -q 'retention_required_free_bytes=' <<<"$output"

echo "[test_backup_accounting_capacity] ok"
