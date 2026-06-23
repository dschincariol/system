#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

DEFAULT_TARGET_DISK="${DEFAULT_TARGET_DISK:-nvme0n1}"
TARGET_DISK="${TARGET_DISK:-}"
TARGET_DISK_BY_ID="${TARGET_DISK_BY_ID:-}"
if [ -z "$TARGET_DISK" ] && [ -z "$TARGET_DISK_BY_ID" ]; then
  TARGET_DISK="$DEFAULT_TARGET_DISK"
fi
CONFIRM_DESTROY="${CONFIRM_DESTROY:-}"
IDLE_NVME_DECISION="${IDLE_NVME_DECISION:-}"
RECLAIM_DRY_RUN="${RECLAIM_DRY_RUN:-1}"
RECLAIM_ROLE="${RECLAIM_ROLE:-docker-pgdata}"
RECLAIM_FS_TYPE="${RECLAIM_FS_TYPE:-ext4}"
RECLAIM_FS_LABEL="${RECLAIM_FS_LABEL:-trading-fast-nvme}"
RECLAIM_MOUNT_POINT="${RECLAIM_MOUNT_POINT:-/var/lib/trading-fast}"
RECLAIM_ASSESSMENT_JSON="${RECLAIM_ASSESSMENT_JSON:-}"
RECLAIM_ASSESSMENT_MAX_AGE_S="${RECLAIM_ASSESSMENT_MAX_AGE_S:-600}"
RECLAIM_BACKUP_REQUIRED="${RECLAIM_BACKUP_REQUIRED:-1}"

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=reclaim_idle_nvme target=%s selector=%s %s\n' \
    "$level" "$event" "${TARGET_DISK:-unset}" "${TARGET_DISK_BY_ID:-kernel-name}" "$*"
}

die() {
  log error "$1" "${2:-}"
  exit 1
}

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

is_dry_run() {
  is_truthy "$RECLAIM_DRY_RUN"
}

resolve_target_identity() {
  if [ -n "$TARGET_DISK_BY_ID" ]; then
    case "$TARGET_DISK_BY_ID" in
      /dev/disk/by-id/*) ;;
      *) die target_by_id_required "TARGET_DISK_BY_ID must be an absolute /dev/disk/by-id path" ;;
    esac
    [ -e "$TARGET_DISK_BY_ID" ] || die target_by_id_missing "path=${TARGET_DISK_BY_ID}"
    local resolved resolved_name
    resolved="$(readlink -f "$TARGET_DISK_BY_ID")"
    resolved_name="$(basename "$resolved")"
    [ -n "$resolved_name" ] || die target_by_id_unresolved "path=${TARGET_DISK_BY_ID}"
    if command -v lsblk >/dev/null 2>&1; then
      local resolved_type
      resolved_type="$(lsblk -ndo TYPE "$resolved" 2>/dev/null | head -n 1 || true)"
      [ "$resolved_type" = "disk" ] || die target_by_id_must_resolve_to_disk "path=${TARGET_DISK_BY_ID} resolved=${resolved} type=${resolved_type:-unknown}"
    fi
    if [ -n "$TARGET_DISK" ] && [ "$TARGET_DISK" != "$resolved_name" ]; then
      die target_selector_mismatch "TARGET_DISK=${TARGET_DISK} TARGET_DISK_BY_ID=${TARGET_DISK_BY_ID} resolves_to=${resolved_name}"
    fi
    TARGET_DISK="$resolved_name"
  fi
  [ -n "$TARGET_DISK" ] || die target_required "set TARGET_DISK or TARGET_DISK_BY_ID"
  case "$TARGET_DISK" in
    */*|""|.|..) die target_kernel_name_required "TARGET_DISK=${TARGET_DISK}" ;;
  esac
}

disk_path() {
  printf '/dev/%s\n' "$TARGET_DISK"
}

partition_path() {
  if [[ "$TARGET_DISK" =~ [0-9]$ ]]; then
    printf '/dev/%sp1\n' "$TARGET_DISK"
  else
    printf '/dev/%s1\n' "$TARGET_DISK"
  fi
}

run_cmd() {
  if is_dry_run; then
    printf '[dry-run] %q' "$1"
    shift
    local arg
    for arg in "$@"; do
      printf ' %q' "$arg"
    done
    printf '\n'
    return 0
  fi
  "$@"
}

require_supported_role() {
  case "$RECLAIM_ROLE" in
    docker-pgdata|zfs-pool) ;;
    *) die unsupported_reclaim_role "role=${RECLAIM_ROLE}" ;;
  esac
}

require_supported_fs() {
  if [ "$RECLAIM_ROLE" = "zfs-pool" ]; then
    [ "$RECLAIM_FS_TYPE" = "none" ] || die unsupported_filesystem "role=${RECLAIM_ROLE} fs_type=${RECLAIM_FS_TYPE} expected=none"
    return 0
  fi
  case "$RECLAIM_FS_TYPE" in
    ext4|xfs) ;;
    *) die unsupported_filesystem "fs_type=${RECLAIM_FS_TYPE}" ;;
  esac
}

normalized_idle_nvme_decision() {
  case "$IDLE_NVME_DECISION" in
    RETAIN|retain|Retain) printf 'retain\n' ;;
    RECLAIM|reclaim|Reclaim) printf 'reclaim\n' ;;
    "") printf '\n' ;;
    *) printf 'invalid\n' ;;
  esac
}

require_decision_branch() {
  local decision
  decision="$(normalized_idle_nvme_decision)"
  case "$decision" in
    retain)
      log info retain_selected_noop "IDLE_NVME_DECISION=RETAIN; Windows/BitLocker install is left untouched"
      exit 0
      ;;
    reclaim)
      return 0
      ;;
    "")
      if is_dry_run; then
        log warn decision_required_noop "set IDLE_NVME_DECISION=RETAIN or IDLE_NVME_DECISION=RECLAIM; no assessment or provisioning commands will run"
        exit 0
      fi
      die decision_required "set IDLE_NVME_DECISION=RECLAIM before apply; RETAIN leaves Windows/BitLocker untouched"
      ;;
    *)
      die invalid_decision "IDLE_NVME_DECISION=${IDLE_NVME_DECISION}; expected RETAIN or RECLAIM"
      ;;
  esac
}

require_apply_confirmation() {
  if is_dry_run; then
    log warn dry_run_default "no destructive commands will run; set RECLAIM_DRY_RUN=0 only after selecting RECLAIM"
    return 0
  fi
  if [ "$CONFIRM_DESTROY" != "$TARGET_DISK" ]; then
    die confirm_destroy_required "expected CONFIRM_DESTROY=${TARGET_DISK}; this destroys the Windows/BitLocker install"
  fi
}

assessment_file() {
  if [ -n "$RECLAIM_ASSESSMENT_JSON" ]; then
    printf '%s\n' "$RECLAIM_ASSESSMENT_JSON"
    return 0
  fi
  local tmp
  local assessment_target="$TARGET_DISK"
  if [ -n "$TARGET_DISK_BY_ID" ]; then
    assessment_target="$TARGET_DISK_BY_ID"
  fi
  tmp="$(mktemp)"
  PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}" \
    python "${REPO_ROOT}/tools/idle_nvme_assessment.py" --device "$assessment_target" --json > "$tmp"
  printf '%s\n' "$tmp"
}

require_unused_assessment() {
  local path="$1"
  PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}" python - "$path" "$TARGET_DISK" "$RECLAIM_ASSESSMENT_MAX_AGE_S" "${TARGET_DISK_BY_ID:-}" <<'PY'
import json
import sys
import time
from pathlib import Path

path = Path(sys.argv[1])
target = sys.argv[2]
max_age_s = float(sys.argv[3])
target_by_id = str(sys.argv[4] if len(sys.argv) > 4 else "").strip()
try:
    report = json.loads(path.read_text(encoding="utf-8"))
except Exception as exc:
    print(f"assessment_json_invalid error={type(exc).__name__}: {exc}", file=sys.stderr)
    raise SystemExit(1)

device = dict(report.get("device") or {})
if str(device.get("name") or "") != target:
    print(f"assessment_target_mismatch expected={target} actual={device.get('name')}", file=sys.stderr)
    raise SystemExit(1)
identifiers = dict(report.get("identifiers") or {})
stable_paths = set(str(value) for value in (device.get("stable_paths") or []))
stable_paths.update(str(value) for value in identifiers.get("by_id_links") or [])
if target_by_id and target_by_id not in stable_paths:
    print(
        "assessment_target_by_id_mismatch "
        f"target_by_id={target_by_id} "
        f"stable_paths={sorted(stable_paths)}",
        file=sys.stderr,
    )
    raise SystemExit(1)
age_s = max(0.0, time.time() - float(report.get("generated_at_epoch") or 0.0))
if age_s > max_age_s:
    print(f"assessment_stale age_s={age_s:.0f} max_age_s={max_age_s:.0f}", file=sys.stderr)
    raise SystemExit(1)
if not bool(report.get("ok")) or not bool(report.get("unused_by_linux")):
    refs = report.get("references") or {}
    print(
        "assessment_not_unused "
        f"reason={report.get('reason')} "
        f"active_refs={len(refs.get('active') or [])} "
        f"config_refs={len(refs.get('config') or [])}",
        file=sys.stderr,
    )
    raise SystemExit(1)
windows = dict((report.get("contents") or {}).get("windows_layout") or {})
if not bool(windows.get("windows_bitlocker_layout_likely")):
    print("assessment_windows_bitlocker_layout_not_confirmed", file=sys.stderr)
    raise SystemExit(1)
classification = dict(report.get("classification") or {})
if classification.get("classification") != "go_candidate":
    print(
        "assessment_not_go_candidate "
        f"classification={classification.get('classification')} "
        f"reason={classification.get('reason')}",
        file=sys.stderr,
    )
    raise SystemExit(1)
print(
    "assessment_ok "
    f"age_s={age_s:.0f} "
    f"partition_count={(report.get('contents') or {}).get('partition_count')}"
)
PY
}

check_backup_evidence() {
  if ! is_truthy "$RECLAIM_BACKUP_REQUIRED"; then
    die backup_evidence_required "RECLAIM_BACKUP_REQUIRED cannot disable apply-mode backup/restore evidence"
  fi
  PYTHONPATH="${REPO_ROOT}:${PYTHONPATH:-}" python - <<'PY'
from __future__ import annotations

import json
import sys

from engine.runtime.backup_evidence import backup_restore_evidence_snapshot

snapshot = backup_restore_evidence_snapshot(engine_mode="live", required=True)
print(
    "backup_evidence "
    f"fresh={str(bool(snapshot.get('fresh'))).lower()} "
    f"ok={str(bool(snapshot.get('ok'))).lower()} "
    f"reason={snapshot.get('reason')} "
    f"evidence_path={snapshot.get('evidence_path')}"
)
if not bool(snapshot.get("fresh")) or not bool(snapshot.get("ok")):
    print(json.dumps({"blockers": snapshot.get("blockers"), "warnings": snapshot.get("warnings")}, sort_keys=True), file=sys.stderr)
    raise SystemExit(1)
PY
}

require_root_for_apply() {
  if is_dry_run; then
    return 0
  fi
  if [ "$(id -u)" -ne 0 ]; then
    die root_required "run as root after all gates pass; the agent must not sudo this automatically"
  fi
}

require_block_device_for_apply() {
  if is_dry_run; then
    return 0
  fi
  [ -b "$(disk_path)" ] || die target_not_block_device "path=$(disk_path)"
}

require_apply_commands() {
  if is_dry_run; then
    return 0
  fi
  local command_name
  local required=(blkid findmnt grep install lsblk mount partprobe sgdisk wipefs)
  if [ "$RECLAIM_ROLE" != "zfs-pool" ]; then
    case "$RECLAIM_FS_TYPE" in
      ext4) required+=(mkfs.ext4) ;;
      xfs) required+=(mkfs.xfs) ;;
    esac
  fi
  for command_name in "${required[@]}"; do
    command -v "$command_name" >/dev/null 2>&1 || die required_command_missing "command=${command_name}"
  done
}

root_parent_disk() {
  local root_source parent
  root_source="$(findmnt -n -o SOURCE / 2>/dev/null || true)"
  [ -n "$root_source" ] || return 0
  parent="$(lsblk -no PKNAME "$root_source" 2>/dev/null | head -n 1 || true)"
  if [ -z "$parent" ]; then
    parent="$(basename "$root_source" 2>/dev/null || true)"
  fi
  printf '%s\n' "$parent"
}

require_not_root_disk() {
  local parent
  parent="$(root_parent_disk)"
  if [ -n "$parent" ] && [ "$parent" = "$TARGET_DISK" ]; then
    if is_dry_run; then
      log warn target_is_root_disk_dry_run "root_parent_disk=${parent}; apply mode would fail"
      return 0
    fi
    die target_is_root_disk "root_parent_disk=${parent}"
  fi
}

already_reclaimed_device() {
  blkid -L "$RECLAIM_FS_LABEL" 2>/dev/null || true
}

ensure_fstab_line() {
  local uuid="$1"
  local fsck_pass="2"
  [ "$RECLAIM_FS_TYPE" = "xfs" ] && fsck_pass="0"
  local line="UUID=${uuid} ${RECLAIM_MOUNT_POINT} ${RECLAIM_FS_TYPE} noatime,nodiratime 0 ${fsck_pass}"
  if grep -qsE "[[:space:]]${RECLAIM_MOUNT_POINT//\//\\/}[[:space:]]" /etc/fstab; then
    if grep -qsF "$line" /etc/fstab; then
      log info fstab_already_configured "mount_point=${RECLAIM_MOUNT_POINT}"
      return 0
    fi
    die fstab_mountpoint_conflict "mount_point=${RECLAIM_MOUNT_POINT}"
  fi
  if is_dry_run; then
    printf '[dry-run] append to /etc/fstab: %s\n' "$line"
  else
    printf '%s\n' "$line" >> /etc/fstab
  fi
}

ensure_role_dirs() {
  run_cmd install -d -m 0750 "$RECLAIM_MOUNT_POINT"
  run_cmd install -d -m 0710 "${RECLAIM_MOUNT_POINT}/docker"
  run_cmd install -d -m 0700 "${RECLAIM_MOUNT_POINT}/pgdata"
  run_cmd install -d -m 0750 "${RECLAIM_MOUNT_POINT}/scratch"
  if ! is_dry_run && id -u postgres >/dev/null 2>&1; then
    chown postgres:postgres "${RECLAIM_MOUNT_POINT}/pgdata"
  fi
}

format_partition() {
  local part
  part="$(partition_path)"
  case "$RECLAIM_FS_TYPE" in
    ext4)
      run_cmd mkfs.ext4 -F -L "$RECLAIM_FS_LABEL" -m 1 "$part"
      ;;
    xfs)
      run_cmd mkfs.xfs -f -L "$RECLAIM_FS_LABEL" "$part"
      ;;
  esac
}

provision_device() {
  local disk part uuid
  disk="$(disk_path)"
  part="$(partition_path)"

  run_cmd wipefs --all "$disk"
  run_cmd sgdisk --zap-all "$disk"
  if [ "$RECLAIM_ROLE" = "zfs-pool" ]; then
    run_cmd partprobe "$disk"
    if command -v udevadm >/dev/null 2>&1; then
      run_cmd udevadm settle
    fi
    log info zfs_pool_reclaim_complete "disk=${disk}; ready for guarded zpool create by caller"
    return 0
  fi
  run_cmd sgdisk --clear "--new=1:0:0" "--typecode=1:8300" "--change-name=1:${RECLAIM_FS_LABEL}" "$disk"
  run_cmd partprobe "$disk"
  if command -v udevadm >/dev/null 2>&1; then
    run_cmd udevadm settle
  fi
  format_partition
  run_cmd install -d -m 0750 "$RECLAIM_MOUNT_POINT"
  if is_dry_run; then
    uuid="DRY-RUN-UUID"
  else
    uuid="$(blkid -s UUID -o value "$part")"
    [ -n "$uuid" ] || die partition_uuid_missing "partition=${part}"
  fi
  ensure_fstab_line "$uuid"
  run_cmd mount "$RECLAIM_MOUNT_POINT"
  ensure_role_dirs
}

handle_existing_reclaim() {
  local existing parent
  [ "$RECLAIM_ROLE" != "zfs-pool" ] || return 1
  existing="$(already_reclaimed_device)"
  [ -n "$existing" ] || return 1
  parent="$(lsblk -no PKNAME "$existing" 2>/dev/null | head -n 1 || true)"
  if [ "$parent" != "$TARGET_DISK" ]; then
    die filesystem_label_conflict "label=${RECLAIM_FS_LABEL} existing=${existing} parent=${parent:-unknown}"
  fi
  log info already_reclaimed "device=${existing} mount_point=${RECLAIM_MOUNT_POINT}"
  if ! findmnt -T "$RECLAIM_MOUNT_POINT" >/dev/null 2>&1; then
    require_root_for_apply
    run_cmd install -d -m 0750 "$RECLAIM_MOUNT_POINT"
    run_cmd mount "$RECLAIM_MOUNT_POINT"
  fi
  require_root_for_apply
  ensure_role_dirs
  return 0
}

print_next_steps() {
  if [ "$RECLAIM_ROLE" = "zfs-pool" ]; then
    cat <<EOF
Next steps after apply:
- This script destroyed the Windows/BitLocker install on $(disk_path) only after the RECLAIM gates passed.
- Create the intended ZFS pool on the same stable by-id path; do not create an ext4/xfs filesystem first.
- For auxpool on host bart, continue with ops/server/provision_storage_pools.sh apply --no-dry-run.
EOF
    return 0
  fi
  cat <<EOF
Next steps after apply:
- This script destroys the Windows/BitLocker install on $(disk_path). Do not apply unless RETAIN was rejected.
- Use ${RECLAIM_MOUNT_POINT}/docker as Docker's data-root after stopping Docker and migrating existing /var/lib/docker intentionally.
- Use ${RECLAIM_MOUNT_POINT}/pgdata for host-native PostgreSQL/Timescale PGDATA only after stopping PostgreSQL and restoring from verified backup evidence.
- Re-run backup_restore_evidence.sh after migration before enabling live runtime modes.
EOF
}

main() {
  resolve_target_identity
  require_decision_branch
  require_supported_role
  require_supported_fs
  log warn destructive_warning "RECLAIM destroys the Windows/BitLocker install on $(disk_path); default is dry-run/no-op"
  require_apply_confirmation

  if handle_existing_reclaim; then
    print_next_steps
    exit 0
  fi

  local assessment tmp_assessment=""
  assessment="$(assessment_file)"
  if [ -z "$RECLAIM_ASSESSMENT_JSON" ]; then
    tmp_assessment="$assessment"
  fi
  trap 'if [ -n "${tmp_assessment:-}" ]; then rm -f "$tmp_assessment"; fi' EXIT
  require_unused_assessment "$assessment"

  if is_dry_run; then
    log warn backup_evidence_not_required_for_dry_run "apply still requires fresh backup and restore evidence"
  else
    check_backup_evidence
  fi

  require_root_for_apply
  require_block_device_for_apply
  require_apply_commands
  require_not_root_disk

  provision_device
  print_next_steps
}

main "$@"
