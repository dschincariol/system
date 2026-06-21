#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[zfs-tuning] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

POOL="${TRADING_ZFS_POOL:-zpool}"
DATA_DATASET="${TRADING_ZFS_DATA_DATASET:-${POOL}/data}"
PGDATA_DATASET="${TRADING_ZFS_PGDATA_DATASET:-${POOL}/docker/timescaledb-pgdata}"
CAPTURE_DIR="${TRADING_ZFS_CAPTURE_DIR:-/var/tmp/trading-zfs-tuning}"
EXPECTED_ASHIFT="${TRADING_ZFS_EXPECT_ASHIFT:-12}"
PGDATA_REQUIRED="${TRADING_ZFS_PGDATA_REQUIRED:-1}"

DATA_COMPRESSION="${TRADING_ZFS_DATA_COMPRESSION:-lz4}"
PGDATA_RECORDSIZE="${TRADING_ZFS_PGDATA_RECORDSIZE:-16K}"
PGDATA_LOGBIAS="${TRADING_ZFS_PGDATA_LOGBIAS:-throughput}"
PGDATA_COMPRESSION="${TRADING_ZFS_PGDATA_COMPRESSION:-lz4}"
PGDATA_ATIME="${TRADING_ZFS_PGDATA_ATIME:-off}"
PGDATA_PRIMARYCACHE="${TRADING_ZFS_PGDATA_PRIMARYCACHE:-metadata}"

DRY_RUN=0

log() {
  printf '[zfs-tuning] %s\n' "$*"
}

die() {
  printf '[zfs-tuning] ERROR: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  ops/server/zfs_tuning.sh apply [--dry-run] [--capture-dir DIR]
  ops/server/zfs_tuning.sh verify [--capture-dir DIR]
  ops/server/zfs_tuning.sh spec

Defaults:
  TRADING_ZFS_POOL=zpool
  TRADING_ZFS_DATA_DATASET=zpool/data
  TRADING_ZFS_PGDATA_DATASET=zpool/docker/timescaledb-pgdata
  TRADING_ZFS_DATA_COMPRESSION=lz4
  TRADING_ZFS_PGDATA_RECORDSIZE=16K
  TRADING_ZFS_PGDATA_LOGBIAS=throughput
  TRADING_ZFS_PGDATA_COMPRESSION=lz4
  TRADING_ZFS_PGDATA_ATIME=off
  TRADING_ZFS_PGDATA_PRIMARYCACHE=metadata
  TRADING_ZFS_EXPECT_ASHIFT=12

TRADING_ZFS_DATA_COMPRESSION is enforced on every existing dataset under the
pool; the dedicated PGDATA dataset uses TRADING_ZFS_PGDATA_COMPRESSION, which
defaults to the same lz4 policy.

The apply action is idempotent and captures before/after zpool, zfs, and zdb
state under TRADING_ZFS_CAPTURE_DIR. It never destroys or recreates a pool.
EOF
}

parse_common_args() {
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --dry-run)
        DRY_RUN=1
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
  command -v zpool >/dev/null 2>&1 || die "missing command: zpool"
  command -v zfs >/dev/null 2>&1 || die "missing command: zfs"
  command -v zdb >/dev/null 2>&1 || die "missing command: zdb"
}

run_cmd() {
  if [ "$DRY_RUN" -eq 1 ]; then
    printf '[zfs-tuning] dry-run:'
    printf ' %q' "$@"
    printf '\n'
  else
    "$@"
  fi
}

capture_state() {
  local phase="$1" stamp out_dir zdb_rc=0
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  out_dir="${CAPTURE_DIR%/}/${stamp}.${phase}"
  mkdir -p "$out_dir"

  zpool get -Hp -o name,property,value,source autotrim "$POOL" >"${out_dir}/zpool-autotrim.tsv"
  zfs get -Hp -r -o name,property,value,source \
    atime,compression,recordsize,logbias,primarycache "$POOL" \
    >"${out_dir}/zfs-properties.tsv"
  zdb -C "$POOL" >"${out_dir}/zdb-C.txt" 2>"${out_dir}/zdb-C.stderr" || zdb_rc=$?
  printf '%s\n' "$zdb_rc" >"${out_dir}/zdb-C.exit"
  log "captured ${phase} state in ${out_dir}"
}

zpool_prop_value() {
  zpool get -H -o value "$1" "$POOL"
}

zfs_dataset_exists() {
  zfs list -H -o name "$1" >/dev/null 2>&1
}

zfs_prop_value() {
  zfs get -H -o value "$2" "$1"
}

ensure_zpool_prop() {
  local prop="$1" expected="$2" current
  current="$(zpool_prop_value "$prop")"
  if [ "$current" = "$expected" ]; then
    log "unchanged ${POOL} ${prop}=${expected}"
    return 0
  fi
  log "setting ${POOL} ${prop}: ${current} -> ${expected}"
  run_cmd zpool set "${prop}=${expected}" "$POOL"
}

ensure_zfs_prop() {
  local dataset="$1" prop="$2" expected="$3" current
  current="$(zfs_prop_value "$dataset" "$prop")"
  if [ "$current" = "$expected" ]; then
    log "unchanged ${dataset} ${prop}=${expected}"
    return 0
  fi
  log "setting ${dataset} ${prop}: ${current} -> ${expected}"
  run_cmd zfs set "${prop}=${expected}" "$dataset"
}

ensure_poolwide_atime_off() {
  local dataset
  while IFS= read -r dataset; do
    [ -n "$dataset" ] || continue
    ensure_zfs_prop "$dataset" atime off
  done < <(zfs list -H -r -o name "$POOL")
}

expected_compression_for_dataset() {
  local dataset="$1"
  if [ "$dataset" = "$PGDATA_DATASET" ]; then
    printf '%s\n' "$PGDATA_COMPRESSION"
  else
    printf '%s\n' "$DATA_COMPRESSION"
  fi
}

ensure_poolwide_compression() {
  local dataset expected
  while IFS= read -r dataset; do
    [ -n "$dataset" ] || continue
    expected="$(expected_compression_for_dataset "$dataset")"
    ensure_zfs_prop "$dataset" compression "$expected"
  done < <(zfs list -H -r -o name "$POOL")
}

apply_pgdata_spec_if_present() {
  if ! zfs_dataset_exists "$PGDATA_DATASET"; then
    log "PGDATA dataset ${PGDATA_DATASET} is not present yet; T1.3c must create it with:"
    print_pgdata_spec
    return 0
  fi
  ensure_zfs_prop "$PGDATA_DATASET" recordsize "$PGDATA_RECORDSIZE"
  ensure_zfs_prop "$PGDATA_DATASET" logbias "$PGDATA_LOGBIAS"
  ensure_zfs_prop "$PGDATA_DATASET" compression "$PGDATA_COMPRESSION"
  ensure_zfs_prop "$PGDATA_DATASET" atime "$PGDATA_ATIME"
  ensure_zfs_prop "$PGDATA_DATASET" primarycache "$PGDATA_PRIMARYCACHE"
}

cmd_apply() {
  parse_common_args "$@"
  require_commands
  capture_state before

  ensure_zpool_prop autotrim on
  ensure_poolwide_atime_off
  zfs_dataset_exists "$DATA_DATASET" || die "missing data dataset: ${DATA_DATASET}"
  ensure_poolwide_compression
  apply_pgdata_spec_if_present

  capture_state after
}

ashifts_from_zdb() {
  zdb -C "$POOL" 2>/dev/null | awk -F: '
    /^[[:space:]]*ashift:/ {
      value=$2
      gsub(/[[:space:],]/, "", value)
      if (value ~ /^[0-9]+$/) print value
    }
  ' | sort -n -u
}

verify_ashift() {
  local ashifts bad=0
  ashifts="$(ashifts_from_zdb || true)"
  [ -n "$ashifts" ] || die "could not determine actual on-disk ashift from zdb -C ${POOL}; zpool get ashift can report default 0 and is not sufficient"
  while IFS= read -r ashift; do
    [ -n "$ashift" ] || continue
    log "actual ${POOL} on-disk ashift=${ashift}"
    if [ "$ashift" != "$EXPECTED_ASHIFT" ]; then
      bad=1
    fi
  done <<<"$ashifts"
  if [ "$bad" -ne 0 ]; then
    die "actual ashift must be ${EXPECTED_ASHIFT}. ashift is immutable for existing vdevs; remediation requires creating a new pool with ashift=${EXPECTED_ASHIFT}, restoring/migrating Docker data and backups during a maintenance window, and proving restore evidence before retiring the old pool. This script will not destroy or recreate ${POOL}."
  fi
}

assert_zpool_prop() {
  local prop="$1" expected="$2" current
  current="$(zpool_prop_value "$prop")"
  [ "$current" = "$expected" ] || die "${POOL} ${prop}=${current}, expected ${expected}"
  log "verified ${POOL} ${prop}=${expected}"
}

assert_zfs_prop() {
  local dataset="$1" prop="$2" expected="$3" current
  current="$(zfs_prop_value "$dataset" "$prop")"
  [ "$current" = "$expected" ] || die "${dataset} ${prop}=${current}, expected ${expected}"
  log "verified ${dataset} ${prop}=${expected}"
}

verify_poolwide_atime_off() {
  local dataset current
  while IFS= read -r dataset; do
    [ -n "$dataset" ] || continue
    current="$(zfs_prop_value "$dataset" atime)"
    [ "$current" = "off" ] || die "${dataset} atime=${current}, expected off"
  done < <(zfs list -H -r -o name "$POOL")
  log "verified atime=off on all existing ${POOL} datasets"
}

verify_poolwide_compression() {
  local dataset current expected
  while IFS= read -r dataset; do
    [ -n "$dataset" ] || continue
    expected="$(expected_compression_for_dataset "$dataset")"
    current="$(zfs_prop_value "$dataset" compression)"
    [ "$current" = "$expected" ] || die "${dataset} compression=${current}, expected ${expected}"
  done < <(zfs list -H -r -o name "$POOL")
  log "verified compression policy on all existing ${POOL} datasets"
}

verify_pgdata_spec() {
  if ! zfs_dataset_exists "$PGDATA_DATASET"; then
    if [ "$PGDATA_REQUIRED" = "0" ]; then
      log "PGDATA dataset ${PGDATA_DATASET} is absent and TRADING_ZFS_PGDATA_REQUIRED=0"
      return 0
    fi
    die "missing PGDATA dataset ${PGDATA_DATASET}; run T1.3c Docker data-root relocation so it creates the dedicated tuned dataset"
  fi
  assert_zfs_prop "$PGDATA_DATASET" recordsize "$PGDATA_RECORDSIZE"
  assert_zfs_prop "$PGDATA_DATASET" logbias "$PGDATA_LOGBIAS"
  assert_zfs_prop "$PGDATA_DATASET" compression "$PGDATA_COMPRESSION"
  assert_zfs_prop "$PGDATA_DATASET" atime "$PGDATA_ATIME"
  assert_zfs_prop "$PGDATA_DATASET" primarycache "$PGDATA_PRIMARYCACHE"
}

cmd_verify() {
  parse_common_args "$@"
  require_commands
  capture_state verify
  verify_ashift
  assert_zpool_prop autotrim on
  verify_poolwide_atime_off
  zfs_dataset_exists "$DATA_DATASET" || die "missing data dataset: ${DATA_DATASET}"
  verify_poolwide_compression
  verify_pgdata_spec
  log "ZFS tuning verified"
}

print_pgdata_spec() {
  cat <<EOF
PGDATA dataset: ${PGDATA_DATASET}
  recordsize=${PGDATA_RECORDSIZE}
  logbias=${PGDATA_LOGBIAS}
  compression=${PGDATA_COMPRESSION}
  atime=${PGDATA_ATIME}
  primarycache=${PGDATA_PRIMARYCACHE}

primarycache decision: metadata-only caching avoids duplicating Postgres 8K
table/index pages in both ZFS ARC and shared_buffers. Keep Postgres
shared_buffers sized explicitly, and do not count PGDATA ARC data caching when
choosing effective_cache_size under this policy.
EOF
}

cmd_spec() {
  print_pgdata_spec
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
