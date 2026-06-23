#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

assessment_json="${tmp_dir}/assessment.json"
mismatch_assessment_json="${tmp_dir}/mismatch_assessment.json"
fresh_evidence_json="${tmp_dir}/fresh_backup_evidence.json"
stale_evidence_json="${tmp_dir}/stale_backup_evidence.json"

python - "$assessment_json" "$mismatch_assessment_json" "$fresh_evidence_json" "$stale_evidence_json" <<'PY'
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

assessment_path = Path(sys.argv[1])
mismatch_assessment_path = Path(sys.argv[2])
fresh_path = Path(sys.argv[3])
stale_path = Path(sys.argv[4])
now = time.time()
now_iso = datetime.fromtimestamp(now, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
old = now - 10 * 24 * 60 * 60
old_iso = datetime.fromtimestamp(old, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def assessment(path: Path, name: str) -> None:
    path.write_text(
        json.dumps(
            {
                "ok": True,
                "unused_by_linux": True,
                "generated_at_epoch": now,
                "reason": "unused_by_linux",
                "device": {
                    "name": name,
                    "path": f"/dev/{name}",
                    "stable_path": f"/dev/{name}",
                    "stable_paths": [f"/dev/{name}"],
                    "found": True,
                },
                "identifiers": {"by_id_links": [], "device_paths": [f"/dev/{name}"]},
                "contents": {
                    "partition_count": 4,
                    "windows_layout": {"windows_bitlocker_layout_likely": True},
                },
                "classification": {
                    "classification": "go_candidate",
                    "candidate": True,
                    "reason": "idle_windows_bitlocker_disk",
                    "reasons": ["idle_windows_bitlocker_disk"],
                },
                "references": {"active": [], "config": []},
            }
        ),
        encoding="utf-8",
    )

assessment(assessment_path, "nvme0n1")
assessment(mismatch_assessment_path, "nvme0n1")

def evidence(path: Path, stamp: str) -> None:
    path.write_text(
        json.dumps(
            {
                "status": "pass",
                "generated_at": stamp,
                "script_checks": {"status": "pass"},
                "systemd_checks": {"status": "pass"},
                "base_backup": {"status": "pass", "verified_at": stamp},
                "wal_archive": {"status": "pass", "verified_at": stamp},
                "restore_drill": {"status": "pass", "verified_at": stamp, "time_to_recover_s": 12},
            }
        ),
        encoding="utf-8",
    )

evidence(fresh_path, now_iso)
evidence(stale_path, old_iso)
PY

run_reclaim() {
  env \
    PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}" \
    RECLAIM_ASSESSMENT_JSON="$assessment_json" \
    RECLAIM_FS_LABEL="test-trading-fast-nvme-${RANDOM}" \
    RECLAIM_MOUNT_POINT="${tmp_dir}/mount" \
    "$@" \
    bash "${REPO_ROOT}/ops/server/reclaim_idle_nvme.sh"
}

output="$(run_reclaim BACKUP_EVIDENCE_PATH="$fresh_evidence_json")"
grep -q 'event=decision_required_noop' <<<"$output"
if grep -q '\[dry-run\] wipefs' <<<"$output"; then
  echo "branchless invocation unexpectedly reached dry-run provisioning" >&2
  exit 1
fi

output="$(run_reclaim IDLE_NVME_DECISION=RETAIN BACKUP_EVIDENCE_PATH="$fresh_evidence_json")"
grep -q 'event=retain_selected_noop' <<<"$output"
if grep -q '\[dry-run\] wipefs' <<<"$output"; then
  echo "RETAIN branch unexpectedly reached dry-run provisioning" >&2
  exit 1
fi

output="$(run_reclaim IDLE_NVME_DECISION=RECLAIM BACKUP_EVIDENCE_PATH="$fresh_evidence_json")"
grep -q 'event=dry_run_default' <<<"$output"
grep -q '\[dry-run\] wipefs' <<<"$output"
grep -q 'RECLAIM destroys the Windows/BitLocker install' <<<"$output"

output="$(run_reclaim IDLE_NVME_DECISION=RECLAIM RECLAIM_ROLE=zfs-pool RECLAIM_FS_TYPE=none BACKUP_EVIDENCE_PATH="$fresh_evidence_json")"
grep -q 'event=dry_run_default' <<<"$output"
grep -q '\[dry-run\] wipefs --all /dev/nvme0n1' <<<"$output"
grep -q '\[dry-run\] sgdisk --zap-all /dev/nvme0n1' <<<"$output"
grep -q 'event=zfs_pool_reclaim_complete' <<<"$output"
if grep -Eq 'mkfs|--new=1:0:0|append to /etc/fstab' <<<"$output"; then
  echo "zfs-pool reclaim role unexpectedly formatted or configured a filesystem" >&2
  exit 1
fi

set +e
output="$(run_reclaim IDLE_NVME_DECISION=RECLAIM RECLAIM_DRY_RUN=0 BACKUP_EVIDENCE_PATH="$fresh_evidence_json" 2>&1)"
rc=$?
set -e
[ "$rc" -ne 0 ] || {
  echo "apply without CONFIRM_DESTROY unexpectedly succeeded" >&2
  exit 1
}
grep -q 'event=confirm_destroy_required' <<<"$output"
if grep -q 'event=decision_required' <<<"$output"; then
  echo "apply without CONFIRM_DESTROY hit decision absence guard instead of token refusal" >&2
  exit 1
fi

set +e
output="$(
  run_reclaim \
    IDLE_NVME_DECISION=RECLAIM \
    RECLAIM_DRY_RUN=0 \
    TARGET_DISK=nvme9n1 \
    CONFIRM_DESTROY=nvme9n1 \
    RECLAIM_ASSESSMENT_JSON="$mismatch_assessment_json" \
    BACKUP_EVIDENCE_PATH="$fresh_evidence_json" \
    2>&1
)"
rc=$?
set -e
[ "$rc" -ne 0 ] || {
  echo "apply with mismatched assessment target unexpectedly succeeded" >&2
  exit 1
}
grep -q 'assessment_target_mismatch expected=nvme9n1 actual=nvme0n1' <<<"$output"

set +e
output="$(
  run_reclaim \
    IDLE_NVME_DECISION=RECLAIM \
    RECLAIM_DRY_RUN=0 \
    CONFIRM_DESTROY=nvme0n1 \
    RECLAIM_BACKUP_REQUIRED=0 \
    BACKUP_EVIDENCE_PATH="$fresh_evidence_json" \
    2>&1
)"
rc=$?
set -e
[ "$rc" -ne 0 ] || {
  echo "apply with backup evidence disabled unexpectedly succeeded" >&2
  exit 1
}
grep -q 'event=backup_evidence_required' <<<"$output"

set +e
output="$(
  run_reclaim \
    IDLE_NVME_DECISION=RECLAIM \
    RECLAIM_DRY_RUN=0 \
    CONFIRM_DESTROY=nvme0n1 \
    BACKUP_EVIDENCE_PATH="$stale_evidence_json" \
    BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S=60 \
    BACKUP_EVIDENCE_WAL_RPO_S=60 \
    BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S=60 \
    2>&1
)"
rc=$?
set -e
[ "$rc" -ne 0 ] || {
  echo "apply with stale backup evidence unexpectedly succeeded" >&2
  exit 1
}
grep -q 'backup_evidence' <<<"$output"
grep -q 'backup_evidence_base_backup_stale' <<<"$output"

echo "[test_idle_nvme_reclaim_guards] ok"
