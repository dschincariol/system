#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[storage-provision] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DB_POOL="${TRADING_DB_ZFS_POOL:-dbpool}"
AUX_POOL="${TRADING_AUX_ZFS_POOL:-auxpool}"
BACKUP_POOL="${TRADING_BACKUP_ZFS_POOL:-zpool}"
EXPECTED_ASHIFT="${TRADING_STORAGE_EXPECT_ASHIFT:-12}"
CAPTURE_DIR="${TRADING_STORAGE_CAPTURE_DIR:-/var/tmp/trading-storage-provision}"

SAMSUNG_BY_ID="${TRADING_DB_DISK_BY_ID:-/dev/disk/by-id/nvme-Samsung_SSD_990_EVO_Plus_4TB_S7U8NU0YA01981P}"
SAMSUNG_EXPECTED_KERNEL="${TRADING_DB_DISK_KERNEL:-nvme2n1}"
KINGSTON_BY_ID="${TRADING_AUX_DISK_BY_ID:-/dev/disk/by-id/nvme-KINGSTON_OM8TAP42048K1-A00_50026B73842ACAC7}"
KINGSTON_EXPECTED_KERNEL="${TRADING_AUX_DISK_KERNEL:-nvme0n1}"

DB_DATASET="${TRADING_DB_DATA_DATASET:-${DB_POOL}/data}"
DB_PGDATA_DATASET="${TRADING_DB_PGDATA_DATASET:-${DB_POOL}/trading/timescaledb/data}"
BACKUP_DATASET="${TRADING_BACKUP_DATASET:-${BACKUP_POOL}/trading-backups}"

DB_ROOT_MOUNT="${TRADING_DB_ROOT_MOUNT:-/${DB_POOL}}"
AUX_ROOT_MOUNT="${TRADING_AUX_ROOT_MOUNT:-/${AUX_POOL}}"

PGDATA_RECORDSIZE="${TRADING_ZFS_PGDATA_RECORDSIZE:-16K}"
PGDATA_LOGBIAS="${TRADING_ZFS_PGDATA_LOGBIAS:-throughput}"
PGDATA_COMPRESSION="${TRADING_ZFS_PGDATA_COMPRESSION:-lz4}"
PGDATA_ATIME="${TRADING_ZFS_PGDATA_ATIME:-off}"
PGDATA_PRIMARYCACHE="${TRADING_ZFS_PGDATA_PRIMARYCACHE:-metadata}"

DRY_RUN=1

AUX_DATASETS=(
  "${AUX_POOL}/trading/redis"
  "${AUX_POOL}/trading/minio"
  "${AUX_POOL}/trading/runtime/data"
  "${AUX_POOL}/trading/runtime/logs"
  "${AUX_POOL}/trading/runtime/artifact_mirror"
  "${AUX_POOL}/trading/runtime/training_datasets"
  "${AUX_POOL}/trading/offline/data"
  "${AUX_POOL}/trading/offline/artifact_mirror"
  "${AUX_POOL}/trading/offline/training_datasets"
)

log() {
  printf '[storage-provision] %s\n' "$*"
}

die() {
  printf '[storage-provision] ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  ops/server/provision_storage_pools.sh apply [--dry-run] [--no-dry-run] [--capture-dir DIR]
  ops/server/provision_storage_pools.sh verify [--capture-dir DIR]
  ops/server/provision_storage_pools.sh spec

Defaults are host bart's confirmed three-NVMe layout:
  dbpool  -> /dev/disk/by-id/nvme-Samsung_SSD_990_EVO_Plus_4TB_S7U8NU0YA01981P
  zpool   -> existing Crucial pool for /var/backups/trading
  auxpool -> /dev/disk/by-id/nvme-KINGSTON_OM8TAP42048K1-A00_50026B73842ACAC7

apply defaults to dry-run. Real apply requires --no-dry-run plus explicit
confirmation gates:
  CONFIRM_WIPE_SAMSUNG=nvme2n1 for the Samsung dbpool wipe/create path.
  For Kingston, the script delegates the destructive Windows reclaim gate to
  reclaim_idle_nvme.sh. That apply path still requires IDLE_NVME_DECISION=RECLAIM,
  TARGET_DISK_BY_ID, CONFIRM_DESTROY=nvme0n1, RECLAIM_DRY_RUN=0, fresh idle-NVMe
  assessment, and fresh backup/restore evidence.

The script never destroys the existing zpool. It only sets zpool autotrim=on,
sets zpool atime=off, and verifies zpool/trading-backups keeps compression=zstd.
EOF
}

parse_common_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --dry-run)
        DRY_RUN=1
        ;;
      --no-dry-run|--apply)
        DRY_RUN=0
        ;;
      --capture-dir)
        [ "$#" -ge 2 ] || die "--capture-dir requires a value"
        CAPTURE_DIR="$2"
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        die "unknown argument: $1"
        ;;
    esac
    shift
  done
}

require_commands() {
  local mode="${1:-apply}"
  local cmd
  for cmd in zpool zfs zdb awk sort; do
    command -v "$cmd" >/dev/null 2>&1 || die "missing command: ${cmd}"
  done
  if [ "$mode" = "verify" ]; then
    for cmd in lsblk readlink; do
      command -v "$cmd" >/dev/null 2>&1 || die "missing command: ${cmd}"
    done
  elif [ "$DRY_RUN" -eq 0 ]; then
    for cmd in lsblk readlink sgdisk wipefs partprobe; do
      command -v "$cmd" >/dev/null 2>&1 || die "missing command: ${cmd}"
    done
  fi
}

run_cmd() {
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '[storage-provision] dry-run:'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

capture_state() {
  local phase="$1" stamp out_dir zdb_rc=0
  if [ "$DRY_RUN" -eq 1 ] && ! command -v zpool >/dev/null 2>&1; then
    log "dry-run capture skipped because zpool is unavailable"
    return 0
  fi
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  out_dir="${CAPTURE_DIR%/}/${stamp}.${phase}"
  mkdir -p "$out_dir"
  zpool list -v >"${out_dir}/zpool-list-v.txt" 2>"${out_dir}/zpool-list-v.stderr" || true
  zpool status -P >"${out_dir}/zpool-status-P.txt" 2>"${out_dir}/zpool-status-P.stderr" || true
  zpool get -Hp -o name,property,value,source autotrim "$DB_POOL" "$AUX_POOL" "$BACKUP_POOL" \
    >"${out_dir}/zpool-autotrim.tsv" 2>"${out_dir}/zpool-autotrim.stderr" || true
  zfs get -Hp -r -o name,property,value,source \
    atime,compression,recordsize,logbias,primarycache,mountpoint "$DB_POOL" "$AUX_POOL" "$BACKUP_POOL" \
    >"${out_dir}/zfs-properties.tsv" 2>"${out_dir}/zfs-properties.stderr" || true
  for pool in "$DB_POOL" "$AUX_POOL" "$BACKUP_POOL"; do
    if ! pool_exists "$pool"; then
      printf 'absent\n' >"${out_dir}/zdb-C-${pool}.skip"
      continue
    fi
    zdb -C "$pool" >"${out_dir}/zdb-C-${pool}.txt" 2>"${out_dir}/zdb-C-${pool}.stderr" || zdb_rc=$?
    printf '%s\n' "$zdb_rc" >"${out_dir}/zdb-C-${pool}.exit"
    zdb_rc=0
  done
  log "captured ${phase} state in ${out_dir}"
}

pool_exists() {
  zpool list -H -o name "$1" >/dev/null 2>&1
}

dataset_exists() {
  zfs list -H -o name "$1" >/dev/null 2>&1
}

zpool_prop_value() {
  zpool get -H -o value "$2" "$1"
}

zfs_prop_value() {
  zfs get -H -o value "$2" "$1"
}

ensure_zpool_prop() {
  local pool="$1" prop="$2" expected="$3" current
  if [ "$DRY_RUN" -eq 1 ] && ! pool_exists "$pool"; then
    log "dry-run: would set ${pool} ${prop}=${expected} after pool create"
    return 0
  fi
  current="$(zpool_prop_value "$pool" "$prop")"
  if [ "$current" = "$expected" ]; then
    log "unchanged ${pool} ${prop}=${expected}"
    return 0
  fi
  log "setting ${pool} ${prop}: ${current} -> ${expected}"
  run_cmd zpool set "${prop}=${expected}" "$pool"
}

ensure_zfs_prop() {
  local dataset="$1" prop="$2" expected="$3" current
  if [ "$DRY_RUN" -eq 1 ] && ! dataset_exists "$dataset"; then
    log "dry-run: would set ${dataset} ${prop}=${expected} after create"
    return 0
  fi
  current="$(zfs_prop_value "$dataset" "$prop")"
  if [ "$current" = "$expected" ]; then
    log "unchanged ${dataset} ${prop}=${expected}"
    return 0
  fi
  log "setting ${dataset} ${prop}: ${current} -> ${expected}"
  run_cmd zfs set "${prop}=${expected}" "$dataset"
}

ensure_poolwide_atime_off() {
  local pool="$1" dataset
  while IFS= read -r dataset; do
    [ -n "$dataset" ] || continue
    ensure_zfs_prop "$dataset" atime off
  done < <(zfs list -H -r -o name "$pool")
}

assert_zpool_prop() {
  local pool="$1" prop="$2" expected="$3" current
  current="$(zpool_prop_value "$pool" "$prop")"
  [ "$current" = "$expected" ] || die "${pool} ${prop}=${current}, expected ${expected}"
  log "verified ${pool} ${prop}=${expected}"
}

assert_zfs_prop() {
  local dataset="$1" prop="$2" expected="$3" current
  current="$(zfs_prop_value "$dataset" "$prop")"
  [ "$current" = "$expected" ] || die "${dataset} ${prop}=${current}, expected ${expected}"
  log "verified ${dataset} ${prop}=${expected}"
}

verify_poolwide_atime_off() {
  local pool="$1" dataset current
  while IFS= read -r dataset; do
    [ -n "$dataset" ] || continue
    current="$(zfs_prop_value "$dataset" atime)"
    [ "$current" = "off" ] || die "${dataset} atime=${current}, expected off"
  done < <(zfs list -H -r -o name "$pool")
  log "verified atime=off on all existing ${pool} datasets"
}

assert_pool_online() {
  local pool="$1" health
  pool_exists "$pool" || die "missing pool: ${pool}"
  health="$(zpool list -H -o health "$pool")"
  [ "$health" = "ONLINE" ] || die "${pool} health=${health}, expected ONLINE"
  log "verified ${pool} ONLINE"
}

ashifts_from_zdb() {
  local pool="$1"
  zdb -C "$pool" 2>/dev/null | awk -F: '
    /^[[:space:]]*ashift:/ {
      value=$2
      gsub(/[[:space:],]/, "", value)
      if (value ~ /^[0-9]+$/) print value
    }
  ' | sort -n -u
}

verify_ashift() {
  local pool="$1" ashifts bad=0
  ashifts="$(ashifts_from_zdb "$pool" || true)"
  [ -n "$ashifts" ] || die "could not determine actual on-disk ashift from zdb -C ${pool}"
  while IFS= read -r ashift; do
    [ -n "$ashift" ] || continue
    log "actual ${pool} on-disk ashift=${ashift}"
    [ "$ashift" = "$EXPECTED_ASHIFT" ] || bad=1
  done <<<"$ashifts"
  [ "$bad" -eq 0 ] || die "${pool} actual ashift must be ${EXPECTED_ASHIFT}"
}

# Advisory ashift check for pools this script does NOT create/manage (the
# pre-existing backup pool). ashift is immutable, so a mismatch is surfaced as
# a warning rather than a hard failure — fixing it requires recreating the pool.
verify_ashift_advisory() {
  local pool="$1" ashifts
  ashifts="$(ashifts_from_zdb "$pool" || true)"
  [ -n "$ashifts" ] || { log "WARNING could not determine ${pool} on-disk ashift"; return 0; }
  while IFS= read -r ashift; do
    [ -n "$ashift" ] || continue
    if [ "$ashift" = "$EXPECTED_ASHIFT" ]; then
      log "verified ${pool} on-disk ashift=${ashift}"
    else
      log "WARNING ${pool} on-disk ashift=${ashift} (expected ${EXPECTED_ASHIFT}); pre-existing pool, ashift is immutable — recreate to optimize (low impact for backups)"
    fi
  done <<<"$ashifts"
}

resolve_disk_by_id() {
  local by_id="$1" expected="$2" resolved name type
  case "$by_id" in
    /dev/disk/by-id/*) ;;
    *) die "disk selector must be an absolute /dev/disk/by-id path: ${by_id}" ;;
  esac
  [ -e "$by_id" ] || die "missing disk by-id path: ${by_id}"
  resolved="$(readlink -f "$by_id")"
  name="$(basename "$resolved")"
  [ "$name" = "$expected" ] || die "${by_id} resolves to ${name}, expected ${expected}"
  type="$(lsblk -ndo TYPE "$resolved" 2>/dev/null | head -n 1 || true)"
  [ "$type" = "disk" ] || die "${by_id} resolves to ${resolved} type=${type:-unknown}, expected disk"
  printf '%s\n' "$resolved"
}

disk_has_mounts() {
  local disk="$1"
  lsblk -nr -o MOUNTPOINTS "$disk" 2>/dev/null | awk 'NF {found=1} END {exit found ? 0 : 1}'
}

disk_pool_references() {
  local disk="$1" path
  while IFS= read -r path; do
    [ -n "$path" ] || continue
    zpool status -P 2>/dev/null | grep -F -- "$path" || true
  done < <(lsblk -nrpo NAME "$disk" 2>/dev/null)
}

require_disk_safe_to_wipe() {
  local disk="$1" label="$2" refs
  if disk_has_mounts "$disk"; then
    die "${label} ${disk} has mounted filesystems; refusing to wipe"
  fi
  refs="$(disk_pool_references "$disk")"
  [ -z "$refs" ] || die "${label} ${disk} is referenced by an imported ZFS pool; refusing to wipe: ${refs}"
}

require_samsung_confirmation() {
  if [ "$DRY_RUN" -eq 1 ]; then
    log "dry-run: Samsung wipe/create would require CONFIRM_WIPE_SAMSUNG=${SAMSUNG_EXPECTED_KERNEL}"
    return 0
  fi
  [ "${CONFIRM_WIPE_SAMSUNG:-}" = "$SAMSUNG_EXPECTED_KERNEL" ] || \
    die "set CONFIRM_WIPE_SAMSUNG=${SAMSUNG_EXPECTED_KERNEL} before wiping Samsung for ${DB_POOL}"
}

wipe_disk_for_pool() {
  local by_id="$1" expected="$2" label="$3" disk=""
  if [ "$DRY_RUN" -eq 1 ]; then
    log "dry-run: would refuse if ${by_id} is mounted or referenced by an imported pool"
  else
    disk="$(resolve_disk_by_id "$by_id" "$expected")"
    require_disk_safe_to_wipe "$disk" "$label"
  fi
  run_cmd wipefs --all "$by_id"
  run_cmd sgdisk --zap-all "$by_id"
  run_cmd partprobe "$by_id"
}

ensure_pool_created() {
  local pool="$1" by_id="$2" mountpoint="$3"
  if pool_exists "$pool"; then
    log "unchanged ${pool} exists"
    return 0
  fi
  run_cmd zpool create \
    -f \
    -o ashift="${EXPECTED_ASHIFT}" \
    -o autotrim=on \
    -O atime=off \
    -O compression=lz4 \
    -O mountpoint="$mountpoint" \
    "$pool" "$by_id"
}

ensure_dataset() {
  local dataset="$1"; shift
  if dataset_exists "$dataset"; then
    log "unchanged ${dataset} exists"
  else
    # -p creates any missing intermediate datasets (e.g. dbpool/trading,
    # auxpool/trading/runtime); -o properties still apply to the leaf only.
    run_cmd zfs create -p "$@" "$dataset"
  fi
}

ensure_pgdata_dataset() {
  ensure_dataset "$DB_DATASET"
  ensure_dataset "$DB_PGDATA_DATASET" \
    -o recordsize="$PGDATA_RECORDSIZE" \
    -o logbias="$PGDATA_LOGBIAS" \
    -o compression="$PGDATA_COMPRESSION" \
    -o atime="$PGDATA_ATIME" \
    -o primarycache="$PGDATA_PRIMARYCACHE"
  ensure_zfs_prop "$DB_PGDATA_DATASET" recordsize "$PGDATA_RECORDSIZE"
  ensure_zfs_prop "$DB_PGDATA_DATASET" logbias "$PGDATA_LOGBIAS"
  ensure_zfs_prop "$DB_PGDATA_DATASET" compression "$PGDATA_COMPRESSION"
  ensure_zfs_prop "$DB_PGDATA_DATASET" atime "$PGDATA_ATIME"
  ensure_zfs_prop "$DB_PGDATA_DATASET" primarycache "$PGDATA_PRIMARYCACHE"
}

ensure_aux_datasets() {
  local dataset
  for dataset in "${AUX_DATASETS[@]}"; do
    ensure_dataset "$dataset"
  done
}

ensure_existing_backup_pool_policy() {
  pool_exists "$BACKUP_POOL" || die "existing backup pool ${BACKUP_POOL} is missing; this script will not create or replace it"
  ensure_zpool_prop "$BACKUP_POOL" autotrim on
  ensure_poolwide_atime_off "$BACKUP_POOL"
  dataset_exists "$BACKUP_DATASET" || die "missing backup dataset: ${BACKUP_DATASET}"
  assert_zfs_prop "$BACKUP_DATASET" compression zstd
}

delegate_kingston_reclaim() {
  if pool_exists "$AUX_POOL"; then
    log "unchanged ${AUX_POOL} exists; Kingston reclaim gate not needed"
    return 0
  fi
  if [ "$DRY_RUN" -eq 1 ]; then
    run_cmd env \
      IDLE_NVME_DECISION="${IDLE_NVME_DECISION:-RECLAIM}" \
      TARGET_DISK_BY_ID="$KINGSTON_BY_ID" \
      CONFIRM_DESTROY="${CONFIRM_DESTROY:-${KINGSTON_EXPECTED_KERNEL}}" \
      RECLAIM_DRY_RUN=1 \
      RECLAIM_ROLE=zfs-pool \
      RECLAIM_FS_TYPE=none \
      bash "${SCRIPT_DIR}/reclaim_idle_nvme.sh"
    return 0
  fi
  [ "${RECLAIM_DRY_RUN:-}" = "0" ] || \
    die "set RECLAIM_DRY_RUN=0 for real Kingston Windows reclaim before creating ${AUX_POOL}"
  IDLE_NVME_DECISION="${IDLE_NVME_DECISION:-}" \
  TARGET_DISK_BY_ID="${TARGET_DISK_BY_ID:-$KINGSTON_BY_ID}" \
  RECLAIM_ROLE="${RECLAIM_ROLE:-zfs-pool}" \
  RECLAIM_FS_TYPE="${RECLAIM_FS_TYPE:-none}" \
  bash "${SCRIPT_DIR}/reclaim_idle_nvme.sh"
}

apply_dbpool() {
  if ! pool_exists "$DB_POOL"; then
    require_samsung_confirmation
    wipe_disk_for_pool "$SAMSUNG_BY_ID" "$SAMSUNG_EXPECTED_KERNEL" "Samsung"
  fi
  ensure_pool_created "$DB_POOL" "$SAMSUNG_BY_ID" "$DB_ROOT_MOUNT"
  ensure_zpool_prop "$DB_POOL" autotrim on
  ensure_zfs_prop "$DB_POOL" atime off
  ensure_zfs_prop "$DB_POOL" compression lz4
  ensure_pgdata_dataset
}

apply_auxpool() {
  delegate_kingston_reclaim
  ensure_pool_created "$AUX_POOL" "$KINGSTON_BY_ID" "$AUX_ROOT_MOUNT"
  ensure_zpool_prop "$AUX_POOL" autotrim on
  ensure_zfs_prop "$AUX_POOL" atime off
  ensure_zfs_prop "$AUX_POOL" compression lz4
  ensure_aux_datasets
}

pool_leaf_disks() {
  local pool="$1"
  zpool status -P "$pool" 2>/dev/null | awk '
    $1 ~ "^/dev/" {
      print $1
    }
  ' | while IFS= read -r dev; do
    [ -n "$dev" ] || continue
    if command -v readlink >/dev/null 2>&1; then
      dev="$(readlink -f "$dev" 2>/dev/null || printf "%s\n" "$dev")"
    fi
    parent="$(lsblk -no PKNAME "$dev" 2>/dev/null | head -n 1 || true)"
    if [ -n "$parent" ]; then
      printf '%s\n' "$parent"
    else
      basename "$dev"
    fi
  done | sort -u
}

verify_db_backup_physical_separation() {
  local db_disks backup_disks disk
  db_disks="$(pool_leaf_disks "$DB_POOL")"
  backup_disks="$(pool_leaf_disks "$BACKUP_POOL")"
  [ -n "$db_disks" ] || die "could not determine physical devices for ${DB_POOL}"
  [ -n "$backup_disks" ] || die "could not determine physical devices for ${BACKUP_POOL}"
  while IFS= read -r disk; do
    [ -n "$disk" ] || continue
    if grep -Fxq "$disk" <<<"$backup_disks"; then
      die "${DB_POOL} and ${BACKUP_POOL} share physical device ${disk}; DB and backups must be on separate drives"
    fi
  done <<<"$db_disks"
  log "verified ${DB_POOL} and ${BACKUP_POOL} use separate physical devices"
}

verify_dataset_exists() {
  local dataset="$1"
  dataset_exists "$dataset" || die "missing dataset: ${dataset}"
  log "verified dataset exists: ${dataset}"
}

verify_aux_datasets() {
  local dataset
  for dataset in "${AUX_DATASETS[@]}"; do
    verify_dataset_exists "$dataset"
  done
}

cmd_apply() {
  parse_common_args "$@"
  require_commands apply
  capture_state before
  ensure_existing_backup_pool_policy
  apply_dbpool
  apply_auxpool
  capture_state after
}

cmd_verify() {
  parse_common_args "$@"
  DRY_RUN=0
  require_commands verify
  capture_state verify
  assert_pool_online "$DB_POOL"
  assert_pool_online "$BACKUP_POOL"
  assert_pool_online "$AUX_POOL"
  verify_ashift "$DB_POOL"
  verify_ashift_advisory "$BACKUP_POOL"
  verify_ashift "$AUX_POOL"
  assert_zpool_prop "$DB_POOL" autotrim on
  assert_zpool_prop "$BACKUP_POOL" autotrim on
  assert_zpool_prop "$AUX_POOL" autotrim on
  assert_zfs_prop "$DB_POOL" atime off
  assert_zfs_prop "$DB_POOL" compression lz4
  assert_zfs_prop "$DB_PGDATA_DATASET" recordsize "$PGDATA_RECORDSIZE"
  assert_zfs_prop "$DB_PGDATA_DATASET" logbias "$PGDATA_LOGBIAS"
  assert_zfs_prop "$DB_PGDATA_DATASET" compression "$PGDATA_COMPRESSION"
  assert_zfs_prop "$DB_PGDATA_DATASET" atime "$PGDATA_ATIME"
  assert_zfs_prop "$DB_PGDATA_DATASET" primarycache "$PGDATA_PRIMARYCACHE"
  verify_poolwide_atime_off "$BACKUP_POOL"
  assert_zfs_prop "$BACKUP_DATASET" compression zstd
  assert_zfs_prop "$AUX_POOL" atime off
  assert_zfs_prop "$AUX_POOL" compression lz4
  verify_dataset_exists "$DB_DATASET"
  verify_dataset_exists "$DB_PGDATA_DATASET"
  verify_aux_datasets
  verify_db_backup_physical_separation
  log "storage pools verified"
}

cmd_spec() {
  cat <<EOF
Pools:
  ${DB_POOL}: ${SAMSUNG_BY_ID}
    create: zpool create -f -o ashift=${EXPECTED_ASHIFT} -o autotrim=on -O atime=off -O compression=lz4 -O mountpoint=${DB_ROOT_MOUNT} ${DB_POOL} ${SAMSUNG_BY_ID}
    datasets: ${DB_DATASET}, ${DB_PGDATA_DATASET}
    PGDATA properties: recordsize=${PGDATA_RECORDSIZE} logbias=${PGDATA_LOGBIAS} compression=${PGDATA_COMPRESSION} atime=${PGDATA_ATIME} primarycache=${PGDATA_PRIMARYCACHE}
  ${BACKUP_POOL}: existing Crucial pool; set autotrim=on and atime=off only
    required backup dataset: ${BACKUP_DATASET} compression=zstd
  ${AUX_POOL}: ${KINGSTON_BY_ID}
    create: zpool create -f -o ashift=${EXPECTED_ASHIFT} -o autotrim=on -O atime=off -O compression=lz4 -O mountpoint=${AUX_ROOT_MOUNT} ${AUX_POOL} ${KINGSTON_BY_ID}
    datasets: ${AUX_DATASETS[*]}
EOF
}

main() {
  local command="${1:-}"
  [ "$#" -gt 0 ] && shift || true
  case "$command" in
    apply) cmd_apply "$@" ;;
    verify) cmd_verify "$@" ;;
    spec) cmd_spec "$@" ;;
    -h|--help|help|"") usage ;;
    *) die "unknown command: ${command}" ;;
  esac
}

main "$@"
