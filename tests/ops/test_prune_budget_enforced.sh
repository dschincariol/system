#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

backup_root="${tmp_dir}/backups"
base_dir="${backup_root}/base"
wal_dir="${backup_root}/wal"
mkdir -p "$base_dir/2026-06-15T000000Z" "$wal_dir"
touch -d '5 days ago' "$base_dir/2026-06-15T000000Z"

old_wal="${wal_dir}/000000010000000000000001"
needed_wal="${wal_dir}/000000010000000000000002"
cushion_wal="${wal_dir}/000000010000000000000003"
head -c 2048 </dev/zero > "$old_wal"
head -c 2048 </dev/zero > "$needed_wal"
head -c 2048 </dev/zero > "$cushion_wal"
touch -d '6 days ago' "$old_wal"
touch -d '4 days ago' "$needed_wal"
touch -d '1 day ago' "$cushion_wal"

before_bytes="$(du -sb "$backup_root" | awk '{print $1}')"
budget_bytes="$((before_bytes - 1024))"

output="$(
  TRADING_BACKUP_ROOT="$backup_root" \
  TS_BACKUP_KEEP_DAILY_DAYS=14 \
  TS_BACKUP_KEEP_WEEKLY_DAYS=365 \
  TS_BACKUP_WAL_CUSHION_DAYS=2 \
  TS_BACKUP_MAX_BYTES="$budget_bytes" \
  TS_BACKUP_ENFORCE_BUDGET=1 \
  TS_BACKUP_CAPACITY_PREFLIGHT=0 \
    bash "${REPO_ROOT}/ops/backup/prune.sh"
)"

grep -q 'event=backup_over_budget ' <<<"$output"
grep -q 'reason=budget' <<<"$output"
grep -q 'event=backup_budget_enforced ' <<<"$output"
[ ! -e "$old_wal" ]
[ -e "$needed_wal" ]
[ -e "$cushion_wal" ]

preflight_root="${tmp_dir}/preflight"
mkdir -p "${preflight_root}/wal"
head -c 1024 </dev/zero > "${preflight_root}/wal/000000010000000000000004"
set +e
preflight_output="$(
  TRADING_BACKUP_ROOT="$preflight_root" \
  TS_BACKUP_KEEP_DAILY_DAYS=14 \
  TS_BACKUP_WAL_CUSHION_DAYS=7 \
  TS_BACKUP_CAPACITY_PREFLIGHT=1 \
  TS_BACKUP_CAPACITY_FREE_BYTES_OVERRIDE=1 \
    bash "${REPO_ROOT}/ops/backup/prune.sh"
)"
preflight_rc=$?
set -e
[ "$preflight_rc" -ne 0 ]
grep -q 'event=backup_capacity_preflight_failed ' <<<"$preflight_output"
grep -q 'required_free_bytes=' <<<"$preflight_output"

echo "[test_prune_budget_enforced] ok"
