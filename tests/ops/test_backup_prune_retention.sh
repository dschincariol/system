#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SCRIPT="${REPO_ROOT}/ops/backup/prune.sh"

bash -n "$SCRIPT"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

base_dir="${tmp_dir}/base"
wal_dir="${tmp_dir}/wal"
mkdir -p "$base_dir" "$wal_dir"

for day in 0 1 2 3; do
  backup="${base_dir}/backup_${day}"
  mkdir -p "$backup"
  touch -d "${day} days ago" "$backup"
done
ln -s backup_0 "${base_dir}/latest"

old_wal="${wal_dir}/000000010000000000000001"
kept_wal="${wal_dir}/000000010000000000000002"
touch -d "20 days ago" "$old_wal"
touch -d "5 days ago" "$kept_wal"

output="$(
  TS_BACKUP_BASE_DIR="$base_dir" \
  TS_BACKUP_WAL_DIR="$wal_dir" \
  bash "$SCRIPT"
)"

[ -d "${base_dir}/backup_0" ]
[ -d "${base_dir}/backup_1" ]
[ ! -d "${base_dir}/backup_2" ]
[ ! -d "${base_dir}/backup_3" ]
[ ! -e "$old_wal" ]
[ -e "$kept_wal" ]
grep -q 'keep_recent_count=2' <<<"$output"
grep -q 'keep_daily_days=0' <<<"$output"
grep -q 'keep_weekly_days=0' <<<"$output"
grep -q 'wal_cushion_days=10' <<<"$output"

echo "[test_backup_prune_retention] ok"
