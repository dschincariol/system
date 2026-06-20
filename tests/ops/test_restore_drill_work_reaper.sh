#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

backup_root="${tmp_dir}/backups"
drill_dir="${backup_root}/drills"
work_root="${drill_dir}/work"
orphan="${work_root}/restore_drill_orphaned"
report="${drill_dir}/restore_drill_2026-06-20T000000Z.txt"
restore_drill_script="${REPO_ROOT}/ops/backup/restore_drill.sh"
mkdir -p "$orphan"
printf 'scratch\n' > "${orphan}/tmp.txt"
printf 'status=pass\n' > "$report"

grep -q 'trap cleanup EXIT' "$restore_drill_script"
grep -q 'trap .*INT' "$restore_drill_script"
grep -q 'trap .*TERM' "$restore_drill_script"

output="$(
  TRADING_BACKUP_ROOT="$backup_root" \
  TS_RESTORE_DRILL_WORK_TTL_DAYS=2 \
  TS_RESTORE_DRILL_ASSUME_NO_LIVE_PROCESS=1 \
    bash "${REPO_ROOT}/ops/backup/prune.sh"
)"

grep -q 'event=restore_drill_work_reaped ' <<<"$output"
grep -q 'reason=orphaned' <<<"$output"
[ ! -e "$orphan" ]
[ -f "$report" ]

echo "[test_restore_drill_work_reaper] ok"
