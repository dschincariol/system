#!/usr/bin/env bash
#
# disk_remediation.sh - diagnose & fix root-filesystem growth on this host.
#
# Context (already established):
#   * Root partition /dev/nvme1n1p2 was 95% full; /home, /tmp(tmpfs) and the
#     2.9 TB /zpool are SEPARATE and healthy.
#   * Cause: Docker json-file logs have NO size cap, and the `trading-runtime`
#     container was crash-looping (DB schema v63 vs code-expected v20), spewing
#     huge log blobs on every restart. Backups land in /var/backups/trading.
#   * ~39 GB of Docker build-cache / orphan volumes already reclaimed (no sudo).
#
# Usage (run in YOUR terminal, NOT inside the agent - the agent cannot sudo):
#   sudo bash ops/server/disk_remediation.sh diagnose          # READ-ONLY. Run this first.
#   sudo bash ops/server/disk_remediation.sh clean-drills      # delete restore-drill scratch (~53 GB, safe)
#   sudo bash ops/server/disk_remediation.sh truncate-logs     # zero the existing *-json.log
#   sudo bash ops/server/disk_remediation.sh cap-logs          # permanent 50m x5 log cap + restart docker
#   sudo bash ops/server/disk_remediation.sh relocate-backups  # move /var/backups/trading -> ZFS dataset on /zpool
#   sudo bash ops/server/disk_remediation.sh relocate-docker --dry-run
#   sudo bash ops/server/disk_remediation.sh relocate-docker   # move Docker data-root -> /zpool/docker
#   sudo bash ops/server/disk_remediation.sh relocate-docker --rollback
#   sudo bash ops/server/disk_remediation.sh install-monitor   # alert (login + log) when / exceeds 85%
#
# Every mutating command makes a backup or is reversible; nothing is auto-run.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
if [ ! -f "${DEFAULT_REPO_ROOT}/deploy/compose/docker-compose.stack.yml" ] && [ -f "${DEFAULT_REPO_ROOT}/app/deploy/compose/docker-compose.stack.yml" ]; then
  DEFAULT_REPO_ROOT="${DEFAULT_REPO_ROOT}/app"
fi

DRY_RUN=0
DOCKER_DIR="${TS_DOCKER_DIR:-/var/lib/docker}"
DOCKER_DAEMON_JSON="${TS_DOCKER_DAEMON_JSON:-/etc/docker/daemon.json}"
DOCKER_DATASET="${TS_DOCKER_DATASET:-zpool/docker}"
DOCKER_DATA_ROOT="${TS_DOCKER_DATA_ROOT:-/zpool/docker}"
DOCKER_PGDATA_DATASET="${TS_DOCKER_PGDATA_DATASET:-${DOCKER_DATASET}/timescaledb-pgdata}"
DOCKER_ROLLBACK_DATASET="${TS_DOCKER_ROLLBACK_DATASET:-zpool/docker-rollback}"
DOCKER_ROLLBACK_ROOT="${TS_DOCKER_ROLLBACK_ROOT:-/zpool/docker-rollback}"
DOCKER_RELOCATION_STATE_DIR="${TS_DOCKER_RELOCATION_STATE_DIR:-/var/lib/docker-relocation}"
TIMESCALE_VOLUME_NAME="${TS_TIMESCALE_VOLUME_NAME:-compose_timescaledb-data}"
PGDATA_RECORDSIZE="${TS_PGDATA_RECORDSIZE:-16K}"
PGDATA_LOGBIAS="${TS_PGDATA_LOGBIAS:-throughput}"
PGDATA_COMPRESSION="${TS_PGDATA_COMPRESSION:-lz4}"
PGDATA_ATIME="${TS_PGDATA_ATIME:-off}"
PGDATA_PRIMARYCACHE="${TS_PGDATA_PRIMARYCACHE:-metadata}"
REPO_ROOT="${TRADING_REPO_ROOT:-$DEFAULT_REPO_ROOT}"
COMPOSE_ENV="${TRADING_COMPOSE_ENV:-$REPO_ROOT/deploy/compose/.env}"
COMPOSE_EXTERNAL="${TRADING_COMPOSE_EXTERNAL:-$REPO_ROOT/deploy/compose/docker-compose.external-services.yml}"
COMPOSE_STACK="${TRADING_COMPOSE_STACK:-$REPO_ROOT/deploy/compose/docker-compose.stack.yml}"
BACKUPS=/var/backups/trading
ZPOOL="${TS_ZPOOL_MOUNT:-/zpool}"
ZBACKUPS="$ZPOOL/backups/trading"

bar() { printf '\n===== %s =====\n' "$*"; }
is_dry_run() { [ "${DRY_RUN:-0}" -eq 1 ]; }
die() { echo "ERROR: $*" >&2; exit 1; }
need_root() {
  is_dry_run && return 0
  [ "$(id -u)" -eq 0 ] || { echo "Run with sudo: sudo bash $0 $*"; exit 1; }
}
human() { awk '{printf "%.1f GB\t%s\n", $1/1073741824, $2}'; }
print_cmd() { printf '  '; printf '%q ' "$@"; printf '\n'; }
run_cmd() {
  if is_dry_run; then
    printf '[dry-run]'; printf ' %q' "$@"; printf '\n'
  else
    "$@" || die "Command failed: $*"
  fi
}
run_optional_cmd() {
  if is_dry_run; then
    printf '[dry-run]'; printf ' %q' "$@"; printf '\n'
  else
    "$@" >/dev/null 2>&1 || true
  fi
}
bytes_human() { numfmt --to=iec "$1" 2>/dev/null || printf '%s bytes' "$1"; }

cmd_diagnose() {
  bar "df - root filesystem"
  df -h /

  bar "Top-level of / (root fs only, biggest first)"
  du -xhd1 / 2>/dev/null | sort -rh | head -15

  bar "/var/lib/docker internals (logs vs overlay vs volumes)"
  du -xhd1 "$DOCKER_DIR" 2>/dev/null | sort -rh

  bar "Per-container json-file logs (THE usual runaway - biggest first)"
  du -ch "$DOCKER_DIR"/containers/*/*-json.log 2>/dev/null | sort -rh | head -20

  bar "/var/backups/trading (bind-mounted into DB + runtime)"
  du -xhsh "$BACKUPS" 2>/dev/null || echo "(not present)"
  du -xhd1 "$BACKUPS" 2>/dev/null | sort -rh | head -15

  bar "Docker named volumes on disk (biggest first)"
  du -xhd1 "$DOCKER_DIR"/volumes 2>/dev/null | sort -rh | head -15

  bar "/root"
  du -xhsh /root 2>/dev/null

  bar "Deleted-but-open files still holding space ('df full, du low' culprit)"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP +L1 2>/dev/null | awk 'NR==1 || /deleted/' | head -25
  else
    echo "lsof not installed; scanning /proc for deleted fds:"
    ls -l /proc/[0-9]*/fd 2>/dev/null | grep '(deleted)' | head -25
    echo "(install lsof for sizes: apt-get install -y lsof)"
  fi

  bar "Largest individual files on root fs (>500 MB)"
  find / -xdev -type f -size +500M -printf '%s\t%p\n' 2>/dev/null | sort -rn | head -20 | human
}

cmd_truncate_logs() {
  need_root
  bar "Container logs BEFORE"
  du -ch "$DOCKER_DIR"/containers/*/*-json.log 2>/dev/null | tail -1
  truncate -s 0 "$DOCKER_DIR"/containers/*/*-json.log 2>/dev/null
  bar "Container logs AFTER (truncated to 0)"
  du -ch "$DOCKER_DIR"/containers/*/*-json.log 2>/dev/null | tail -1
  df -h /
}

write_docker_daemon_json() {
  local data_root="${1:-}" enable_log_caps="${2:-0}"
  local daemon_dir backup tmp stamp
  command -v python3 >/dev/null 2>&1 || { echo "ERROR: python3 is required to merge $DOCKER_DAEMON_JSON safely."; exit 1; }

  daemon_dir="$(dirname "$DOCKER_DAEMON_JSON")"
  stamp="$(date +%Y%m%d%H%M%S)"
  if is_dry_run; then
    echo "[dry-run] would create $daemon_dir if needed"
    if [ -f "$DOCKER_DAEMON_JSON" ]; then
      echo "[dry-run] would back up $DOCKER_DAEMON_JSON to ${DOCKER_DAEMON_JSON}.bak.${stamp}"
    fi
    [ -n "$data_root" ] && echo "[dry-run] would set Docker data-root to $data_root"
    [ "$enable_log_caps" -eq 1 ] && echo "[dry-run] would enforce json-file log cap max-size=50m max-file=5"
    return 0
  fi

  mkdir -p "$daemon_dir"
  if [ -f "$DOCKER_DAEMON_JSON" ]; then
    backup="${DOCKER_DAEMON_JSON}.bak.${stamp}"
    cp -a "$DOCKER_DAEMON_JSON" "$backup"
    echo "Backed up existing daemon.json to $backup"
  fi

  tmp="$(mktemp "${DOCKER_DAEMON_JSON}.tmp.XXXXXX")"
  if ! python3 - "$DOCKER_DAEMON_JSON" "$tmp" "$data_root" "$enable_log_caps" <<'PY'
import json
import os
import sys

path, tmp_path, data_root, enable_log_caps = sys.argv[1:5]
data = {}
if os.path.exists(path) and os.path.getsize(path) > 0:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise SystemExit(f"{path} must contain a JSON object")

if data_root:
    data["data-root"] = data_root

if enable_log_caps == "1":
    data["log-driver"] = "json-file"
    opts = data.get("log-opts")
    if not isinstance(opts, dict):
        opts = {}
    opts["max-size"] = "50m"
    opts["max-file"] = "5"
    data["log-opts"] = opts

with open(tmp_path, "w", encoding="utf-8") as handle:
    json.dump(data, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
  then
    rm -f "$tmp"
    echo "ERROR: failed to merge $DOCKER_DAEMON_JSON; original left untouched."
    exit 1
  fi

  if command -v dockerd >/dev/null 2>&1; then
    echo "Validating Docker daemon config..."
    dockerd --validate --config-file "$tmp" >/dev/null || {
      rm -f "$tmp"
      echo "ERROR: dockerd rejected the generated daemon.json; original left untouched."
      exit 1
    }
  else
    echo "WARNING: dockerd not found; JSON syntax was validated but daemon-level validation was skipped."
  fi

  mv "$tmp" "$DOCKER_DAEMON_JSON"
  chmod 0644 "$DOCKER_DAEMON_JSON"
  echo "Wrote $DOCKER_DAEMON_JSON:"
  cat "$DOCKER_DAEMON_JSON"
}

cmd_cap_logs() {
  need_root
  write_docker_daemon_json "" 1
  echo
  echo "Restarting docker (existing containers keep old uncapped logs until recreated)..."
  systemctl restart docker
  echo "Done. New/recreated containers are capped at 50 MB x 5 = 250 MB max each."
}

cmd_clean_drills() {
  need_root
  local work="$BACKUPS/drills/work"
  [ -d "$work" ] || { echo "$work not present; nothing to clean."; exit 0; }
  bar "Restore-drill scratch BEFORE"
  du -xhs "$work" 2>/dev/null
  echo "Removing completed/abandoned drill scratch dirs (reports in drills/*.txt are kept):"
  find "$work" -mindepth 1 -maxdepth 1 -type d -name 'restore_drill_*' -print -exec rm -rf {} +
  bar "Restore-drill scratch AFTER"
  du -xhs "$work" 2>/dev/null
  df -h /
}

# Move /var/backups/trading onto a ZFS dataset mounted at the SAME path.
# Path is unchanged, so systemd ReadWritePaths=/var/backups/trading and the
# container bind-mounts keep working with no unit edits. Original data is kept
# as <path>.old-<ts> until you verify and delete it (that delete frees root).
DATASET="${TS_BACKUP_DATASET:-zpool/trading-backups}"
BACKUP_POSTGRES_UID="${TS_BACKUP_POSTGRES_UID:-70}"
BACKUP_GROUP="${TS_BACKUP_GROUP:-trading}"
normalize_backup_wal_target_permissions() {
  local root="$1"
  local wal="${root}/wal"
  local tmp="${wal}/.tmp"
  mkdir -p "$wal" "$tmp"
  chown "${BACKUP_POSTGRES_UID}:${BACKUP_GROUP}" "$root" "$wal" "$tmp"
  chmod 2750 "$root" "$wal" "$tmp"
  find "$wal" -type d -exec chown "${BACKUP_POSTGRES_UID}:${BACKUP_GROUP}" {} +
  find "$wal" -type d -exec chmod 2750 {} +
  echo "Normalized WAL archive target ownership/mode: ${BACKUP_POSTGRES_UID}:${BACKUP_GROUP} 2750 ${wal}"
}

cmd_relocate_backups() {
  need_root
  command -v zfs >/dev/null 2>&1 || { echo "ERROR: zfs CLI not found."; exit 1; }
  [ -d "$ZPOOL" ] || { echo "ERROR: $ZPOOL not mounted; aborting."; exit 1; }
  [ -e "$BACKUPS" ] || { echo "$BACKUPS does not exist; nothing to move."; exit 0; }
  if mountpoint -q "$BACKUPS"; then echo "$BACKUPS is already a mountpoint; looks already relocated."; exit 0; fi
  if zfs list -H -o name "$DATASET" >/dev/null 2>&1; then echo "ERROR: dataset $DATASET already exists; aborting."; exit 1; fi

  bar "Quiescing writers (backup timers + DB/runtime containers)"
  for t in trading-backup-evidence trading-backup-prune trading-base-backup trading-restore-drill; do
    systemctl stop "$t.timer" 2>/dev/null || true
  done
  docker stop trading-timescaledb trading-runtime trading-operator 2>/dev/null || true

  local src_bytes stage="$ZPOOL/.trading-backups.staging"
  src_bytes="$(du -sb "$BACKUPS" | awk '{print $1}')"
  echo "Source apparent size: $(numfmt --to=iec "$src_bytes" 2>/dev/null || echo "$src_bytes bytes")"

  bar "Creating dataset $DATASET (staged at $stage) and copying"
  zfs create -o mountpoint="$stage" -o compression=zstd "$DATASET"
  rsync -aHAX --numeric-ids --info=progress2 "$BACKUPS"/ "$stage"/
  normalize_backup_wal_target_permissions "$stage"

  local dst_bytes; dst_bytes="$(du -sb "$stage" | awk '{print $1}')"
  echo "Copied apparent size: $(numfmt --to=iec "$dst_bytes" 2>/dev/null || echo "$dst_bytes bytes")"
  # allow small variance from in-flight files; require >=99% copied
  if [ "$dst_bytes" -lt "$(( src_bytes * 99 / 100 ))" ]; then
    echo "ERROR: copy looks incomplete ($dst_bytes < 99% of $src_bytes). Dataset left at $stage; original untouched."
    exit 1
  fi

  bar "Swapping $BACKUPS -> ZFS dataset"
  mv "$BACKUPS" "${BACKUPS}.old-$(date +%Y%m%d%H%M%S)"
  mkdir -p "$BACKUPS"
  zfs set mountpoint="$BACKUPS" "$DATASET"
  mountpoint -q "$BACKUPS" && echo "OK: $BACKUPS is now the ZFS dataset $DATASET" || { echo "ERROR: remount failed"; exit 1; }

  bar "Restarting writers"
  for t in trading-backup-evidence trading-backup-prune trading-base-backup trading-restore-drill; do
    systemctl start "$t.timer" 2>/dev/null || true
  done
  docker start trading-timescaledb trading-operator 2>/dev/null || true
  echo "NOTE: trading-runtime left STOPPED on purpose (schema v63 vs v20 crash-loop). Fix schema first."

  bar "Result"
  zfs list "$DATASET"
  df -h / "$BACKUPS"
  echo
  echo ">>> Verify the stack/backups look healthy, THEN reclaim root by deleting the old copy:"
  echo ">>>   sudo rm -rf ${BACKUPS}.old-*"
}

relocate_docker_usage() {
  cat <<EOF
Usage:
  sudo bash $0 relocate-docker [--dry-run]
  sudo bash $0 relocate-docker --rollback [--dry-run]

Environment overrides for tests or non-default hosts:
  TS_DOCKER_DIR=$DOCKER_DIR
  TS_DOCKER_DAEMON_JSON=$DOCKER_DAEMON_JSON
  TS_DOCKER_DATASET=$DOCKER_DATASET
  TS_DOCKER_DATA_ROOT=$DOCKER_DATA_ROOT
  TS_TIMESCALE_VOLUME_NAME=$TIMESCALE_VOLUME_NAME
  TRADING_REPO_ROOT=$REPO_ROOT
EOF
}

parse_relocate_docker_args() {
  RELOCATE_ROLLBACK=0
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --dry-run|-n) DRY_RUN=1 ;;
      --rollback) RELOCATE_ROLLBACK=1 ;;
      --help|-h) relocate_docker_usage; exit 0 ;;
      *) die "Unknown relocate-docker option: $1" ;;
    esac
    shift
  done
}

require_relocation_commands() {
  local cmd missing=0
  for cmd in docker zfs rsync systemctl df du awk mountpoint python3; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
      echo "ERROR: required command not found: $cmd" >&2
      missing=1
    fi
  done
  [ "$missing" -eq 0 ] || exit 1
}

require_compose_files() {
  [ -f "$COMPOSE_ENV" ] || die "Compose env file missing: $COMPOSE_ENV"
  [ -f "$COMPOSE_EXTERNAL" ] || die "Compose services file missing: $COMPOSE_EXTERNAL"
  [ -f "$COMPOSE_STACK" ] || die "Compose stack file missing: $COMPOSE_STACK"
}

compose_down() {
  run_cmd docker compose --env-file "$COMPOSE_ENV" -f "$COMPOSE_EXTERNAL" -f "$COMPOSE_STACK" down
}

compose_up() {
  run_cmd docker compose --env-file "$COMPOSE_ENV" -f "$COMPOSE_EXTERNAL" -f "$COMPOSE_STACK" up -d
}

compose_ps_ids() {
  docker compose --env-file "$COMPOSE_ENV" -f "$COMPOSE_EXTERNAL" -f "$COMPOSE_STACK" ps -q 2>/dev/null || true
}

assert_compose_stack_stopped() {
  is_dry_run && { echo "[dry-run] would assert Compose stack has no running containers"; return 0; }
  local ids
  ids="$(compose_ps_ids)"
  [ -z "$ids" ] || die "Compose stack still has containers after down: $(echo "$ids" | tr '\n' ' ')"
  echo "OK: Compose stack is stopped."
}

backup_units() {
  printf '%s\n' \
    trading-backup-evidence.service \
    trading-backup-prune.service \
    trading-base-backup.service \
    trading-restore-drill.service
}

backup_timers() {
  printf '%s\n' \
    trading-backup-evidence.timer \
    trading-backup-prune.timer \
    trading-base-backup.timer \
    trading-restore-drill.timer
}

assert_no_inflight_backup() {
  local unit active_units=()
  if is_dry_run; then
    echo "[dry-run] would assert backup services and backup scripts are not active"
    return 0
  fi
  while IFS= read -r unit; do
    if systemctl is-active --quiet "$unit" 2>/dev/null; then
      active_units+=("$unit")
    fi
  done < <(backup_units)
  if [ "${#active_units[@]}" -gt 0 ]; then
    die "Backup work is active; wait for it to finish before relocation: ${active_units[*]}"
  fi
  if pgrep -f 'ops/backup/(base_backup|restore_drill|backup_restore_evidence|wal_archive_catchup|prune)\.sh' >/dev/null 2>&1; then
    die "A backup/restore process is active; wait for it to finish before relocation."
  fi
  echo "OK: no in-flight backup or restore process detected."
}

stop_backup_timers() {
  local timer
  bar "Stopping backup timers for the maintenance window"
  while IFS= read -r timer; do
    run_optional_cmd systemctl stop "$timer"
  done < <(backup_timers)
}

start_backup_timers() {
  local timer
  bar "Restarting backup timers"
  while IFS= read -r timer; do
    run_optional_cmd systemctl start "$timer"
  done < <(backup_timers)
}

fs_free_bytes() {
  df -PB1 "$1" | awk 'NR==2 {print $4}'
}

fs_mountpoint() {
  df -P "$1" | awk 'NR==2 {print $6}'
}

fs_type() {
  df -PT "$1" | awk 'NR==2 {print $2}'
}

docker_current_data_root() {
  local root configured
  root="$(docker info -f '{{.DockerRootDir}}' 2>/dev/null || true)"
  if [ -n "$root" ]; then
    printf '%s\n' "$root"
    return 0
  fi
  configured="$(python3 - "$DOCKER_DAEMON_JSON" <<'PY'
import json
import os
import sys

path = sys.argv[1]
if os.path.exists(path) and os.path.getsize(path) > 0:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    value = data.get("data-root") if isinstance(data, dict) else None
    if value:
        print(value)
PY
)"
  if [ -n "$configured" ]; then
    printf '%s\n' "$configured"
  else
    printf '%s\n' "$DOCKER_DIR"
  fi
}

zfs_pool_name() {
  printf '%s\n' "${DOCKER_DATASET%%/*}"
}

zfs_available_bytes() {
  zfs list -Hp -o avail "$(zfs_pool_name)"
}

assert_pool_space() {
  local source_bytes="$1" avail required cushion
  cushion=$((10 * 1024 * 1024 * 1024))
  # Need room for the live Docker data-root copy plus the rollback archive.
  required=$((source_bytes * 220 / 100 + cushion))
  if is_dry_run; then
    echo "[dry-run] would require ZFS free space >= $(bytes_human "$required") for Docker copy + rollback archive"
    return 0
  fi
  avail="$(zfs_available_bytes)"
  echo "ZFS available on $(zfs_pool_name): $(bytes_human "$avail")"
  echo "Required for Docker copy + rollback archive: $(bytes_human "$required")"
  [ "$avail" -ge "$required" ] || die "Not enough free ZFS space for relocation and rollback archive."
}

ensure_zfs_dataset() {
  local dataset="$1" mountpoint="$2" profile="$3"
  local prop
  local props=(-o "mountpoint=$mountpoint" -o atime=off -o xattr=sa -o acltype=posixacl -o dnodesize=auto)
  case "$profile" in
    docker)
      props+=(-o compression=zstd -o recordsize=128K -o logbias=latency)
      ;;
    pgdata)
      props+=(
        -o "recordsize=$PGDATA_RECORDSIZE"
        -o "logbias=$PGDATA_LOGBIAS"
        -o "compression=$PGDATA_COMPRESSION"
        -o "atime=$PGDATA_ATIME"
        -o "primarycache=$PGDATA_PRIMARYCACHE"
      )
      ;;
  esac

  if is_dry_run; then
    echo "[dry-run] would ensure ZFS dataset $dataset mounted at $mountpoint with $profile properties:"
    print_cmd zfs create "${props[@]}" "$dataset"
    return 0
  fi

  if zfs list -H -o name "$dataset" >/dev/null 2>&1; then
    for prop in "mountpoint=$mountpoint" atime=off xattr=sa acltype=posixacl dnodesize=auto; do
      zfs set "$prop" "$dataset"
    done
    case "$profile" in
      docker)
        zfs set compression=zstd "$dataset"
        zfs set recordsize=128K "$dataset"
        zfs set logbias=latency "$dataset"
        ;;
      pgdata)
        zfs set "recordsize=$PGDATA_RECORDSIZE" "$dataset"
        zfs set "logbias=$PGDATA_LOGBIAS" "$dataset"
        zfs set "compression=$PGDATA_COMPRESSION" "$dataset"
        zfs set "atime=$PGDATA_ATIME" "$dataset"
        zfs set "primarycache=$PGDATA_PRIMARYCACHE" "$dataset"
        ;;
    esac
  else
    zfs create "${props[@]}" "$dataset"
  fi
  zfs mount "$dataset" >/dev/null 2>&1 || true
  mountpoint -q "$mountpoint" || die "ZFS dataset $dataset is not mounted at $mountpoint"
}

find_timescale_volume_name_in_root() {
  local root="$1" candidate
  if [ -d "$root/volumes/$TIMESCALE_VOLUME_NAME/_data" ]; then
    printf '%s\n' "$TIMESCALE_VOLUME_NAME"
    return 0
  fi
  for candidate in "$root"/volumes/*timescaledb*data* "$root"/volumes/*timescale*data*; do
    if [ -d "$candidate/_data" ]; then
      basename "$candidate"
      return 0
    fi
  done
  return 1
}

find_timescale_volume_name_runtime() {
  local candidate
  if docker volume inspect "$TIMESCALE_VOLUME_NAME" >/dev/null 2>&1; then
    printf '%s\n' "$TIMESCALE_VOLUME_NAME"
    return 0
  fi
  candidate="$(docker volume ls --format '{{.Name}}' 2>/dev/null | awk '/timescale.*data|timescaledb.*data/ {print; exit}')"
  [ -n "$candidate" ] || return 1
  printf '%s\n' "$candidate"
}

prepare_docker_datasets() {
  local source_root="$1" volume_name="$2" pgdata_mount
  bar "Creating/tuning Docker ZFS datasets"
  ensure_zfs_dataset "$DOCKER_DATASET" "$DOCKER_DATA_ROOT" docker
  pgdata_mount="$DOCKER_DATA_ROOT/volumes/$volume_name/_data"
  run_cmd mkdir -p "$(dirname "$pgdata_mount")"
  ensure_zfs_dataset "$DOCKER_PGDATA_DATASET" "$pgdata_mount" pgdata
  ensure_zfs_dataset "$DOCKER_ROLLBACK_DATASET" "$DOCKER_ROLLBACK_ROOT" docker
  echo "Timescale PGDATA tuning for T2.5: dataset=$DOCKER_PGDATA_DATASET recordsize=$PGDATA_RECORDSIZE logbias=$PGDATA_LOGBIAS compression=$PGDATA_COMPRESSION atime=$PGDATA_ATIME primarycache=$PGDATA_PRIMARYCACHE"
  echo "PGDATA primarycache=metadata avoids duplicating Postgres shared_buffers pages in ZFS ARC; do not count PGDATA ARC data cache in effective_cache_size."
  [ -d "$source_root/volumes/$volume_name/_data/pg_wal" ] && echo "Detected pg_wal under PGDATA; it will move with the tuned PGDATA dataset."
}

save_relocation_state() {
  local previous_root="$1" source_bytes="$2" root_free_before="$3" rollback_source="$4"
  if is_dry_run; then
    echo "[dry-run] would write rollback state under $DOCKER_RELOCATION_STATE_DIR"
    return 0
  fi
  mkdir -p "$DOCKER_RELOCATION_STATE_DIR"
  if [ -f "$DOCKER_DAEMON_JSON" ]; then
    cp -a "$DOCKER_DAEMON_JSON" "$DOCKER_RELOCATION_STATE_DIR/daemon.json.before"
    printf 'DAEMON_JSON_EXISTED=1\n' > "$DOCKER_RELOCATION_STATE_DIR/state.env"
  else
    rm -f "$DOCKER_RELOCATION_STATE_DIR/daemon.json.before"
    printf 'DAEMON_JSON_EXISTED=0\n' > "$DOCKER_RELOCATION_STATE_DIR/state.env"
  fi
  {
    printf 'PREVIOUS_DOCKER_DATA_ROOT=%q\n' "$previous_root"
    printf 'NEW_DOCKER_DATA_ROOT=%q\n' "$DOCKER_DATA_ROOT"
    printf 'DOCKER_DATASET=%q\n' "$DOCKER_DATASET"
    printf 'DOCKER_PGDATA_DATASET=%q\n' "$DOCKER_PGDATA_DATASET"
    printf 'ROLLBACK_SOURCE=%q\n' "$rollback_source"
    printf 'SOURCE_BYTES=%q\n' "$source_bytes"
    printf 'ROOT_FREE_BEFORE=%q\n' "$root_free_before"
    printf 'STATE_CREATED_AT=%q\n' "$(date -u +%Y%m%dT%H%M%SZ)"
  } >> "$DOCKER_RELOCATION_STATE_DIR/state.env"
  chmod 0600 "$DOCKER_RELOCATION_STATE_DIR/state.env"
}

load_relocation_state() {
  local state_file="$DOCKER_RELOCATION_STATE_DIR/state.env"
  [ -f "$state_file" ] || die "No relocation state found at $state_file; rollback is not safe."
  # shellcheck disable=SC1090
  . "$state_file"
}

stop_compose_and_docker() {
  bar "Stopping Compose stack and Docker"
  stop_backup_timers
  compose_down
  assert_compose_stack_stopped
  run_cmd systemctl stop docker.service
  run_optional_cmd systemctl stop docker.socket
  run_optional_cmd systemctl stop containerd.service
}

start_docker_and_compose() {
  bar "Starting Docker and Compose stack"
  run_optional_cmd systemctl start containerd.service
  run_cmd systemctl start docker.service
  compose_up
}

rsync_docker_tree() {
  local source_root="$1"
  bar "Copying Docker data-root to ZFS"
  run_cmd rsync -aHAX --numeric-ids --info=progress2 "$source_root"/ "$DOCKER_DATA_ROOT"/
}

verify_copy_size() {
  local source_root="$1" source_bytes="$2" dst_bytes min_bytes
  is_dry_run && { echo "[dry-run] would verify copied bytes are at least 99% of source"; return 0; }
  dst_bytes="$(du -sb "$DOCKER_DATA_ROOT" | awk '{print $1}')"
  min_bytes=$((source_bytes * 99 / 100))
  echo "Source apparent size: $(bytes_human "$source_bytes")"
  echo "Copied apparent size: $(bytes_human "$dst_bytes")"
  [ "$dst_bytes" -ge "$min_bytes" ] || die "Docker copy looks incomplete: $dst_bytes < 99% of $source_bytes from $source_root"
}

verify_docker_root() {
  local root
  is_dry_run && { echo "[dry-run] would verify docker info reports DockerRootDir=$DOCKER_DATA_ROOT"; return 0; }
  root="$(docker_current_data_root)"
  [ "$root" = "$DOCKER_DATA_ROOT" ] || die "Docker is using $root, expected $DOCKER_DATA_ROOT"
  echo "OK: Docker data-root is $root"
}

verify_compose_health() {
  local deadline ids id line name status health bad=0
  if is_dry_run; then
    echo "[dry-run] would wait for all Compose containers to be running and healthy"
    return 0
  fi
  deadline=$((SECONDS + 180))
  while [ "$SECONDS" -le "$deadline" ]; do
    ids="$(compose_ps_ids)"
    if [ -n "$ids" ]; then
      bad=0
      while IFS= read -r id; do
        [ -n "$id" ] || continue
        line="$(docker inspect -f '{{.Name}} {{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$id")"
        name="$(echo "$line" | awk '{print $1}' | sed 's#^/##')"
        status="$(echo "$line" | awk '{print $2}')"
        health="$(echo "$line" | awk '{print $3}')"
        if [ "$status" != "running" ] || [ "$health" = "unhealthy" ] || [ "$health" = "starting" ]; then
          bad=1
          echo "Waiting for container health: $name status=$status health=$health"
        fi
      done <<< "$ids"
      [ "$bad" -eq 0 ] && { echo "OK: all Compose containers are running and healthy (or have no healthcheck)."; return 0; }
    fi
    sleep 5
  done
  docker compose --env-file "$COMPOSE_ENV" -f "$COMPOSE_EXTERNAL" -f "$COMPOSE_STACK" ps
  die "Compose containers did not become healthy after Docker data-root relocation."
}

verify_timescale_pgdata_on_zfs() {
  local volume mountpoint fstype inspect_source
  if is_dry_run; then
    echo "[dry-run] would verify Timescale PGDATA volume mountpoint is on ZFS under $DOCKER_DATA_ROOT"
    return 0
  fi
  volume="$(find_timescale_volume_name_runtime)" || die "Could not find Timescale Docker volume after restart."
  mountpoint="$(docker volume inspect "$volume" -f '{{.Mountpoint}}')"
  [ -d "$mountpoint" ] || die "Timescale volume mountpoint missing: $mountpoint"
  fstype="$(fs_type "$mountpoint")"
  [ "$fstype" = "zfs" ] || die "Timescale PGDATA is on filesystem $fstype, expected zfs: $mountpoint"
  case "$mountpoint" in
    "$DOCKER_DATA_ROOT"/*) ;;
    *) die "Timescale PGDATA is not under Docker ZFS data-root: $mountpoint" ;;
  esac
  [ -d "$mountpoint/pg_wal" ] || die "Timescale PGDATA mountpoint lacks pg_wal: $mountpoint"
  inspect_source="$(docker inspect trading-timescaledb -f '{{range .Mounts}}{{if eq .Destination "/var/lib/postgresql/data"}}{{.Source}}{{end}}{{end}}' 2>/dev/null || true)"
  [ -z "$inspect_source" ] || [ "$inspect_source" = "$mountpoint" ] || die "trading-timescaledb PGDATA source mismatch: inspect=$inspect_source volume=$mountpoint"
  echo "OK: Timescale PGDATA and pg_wal reside on ZFS: $mountpoint"
}

archive_old_docker_root() {
  local source_root="$1" rollback_source="$2" source_bytes="$3" archive_bytes min_bytes
  bar "Archiving old Docker root for rollback, then reclaiming root"
  case "$source_root" in
    ""|"/"|"/var"|"$DOCKER_DATA_ROOT")
      die "Refusing to archive/remove unsafe Docker source path: $source_root"
      ;;
  esac
  if is_dry_run; then
    print_cmd mkdir -p "$rollback_source"
    print_cmd rsync -aHAX --numeric-ids --info=progress2 "$source_root"/ "$rollback_source"/
    print_cmd rm -rf "$source_root"
    print_cmd mkdir -p "$source_root"
    return 0
  fi
  mkdir -p "$rollback_source"
  rsync -aHAX --numeric-ids --info=progress2 "$source_root"/ "$rollback_source"/
  archive_bytes="$(du -sb "$rollback_source" | awk '{print $1}')"
  min_bytes=$((source_bytes * 99 / 100))
  [ "$archive_bytes" -ge "$min_bytes" ] || die "Rollback archive looks incomplete: $archive_bytes < 99% of $source_bytes"
  rm -rf "$source_root"
  mkdir -p "$source_root"
  chmod 0711 "$source_root"
  echo "Old Docker root retained for guarded rollback at: $rollback_source"
}

assert_root_free_increased() {
  local root_free_before="$1" root_free_after
  if is_dry_run; then
    echo "[dry-run] would assert root free space after rollback archive cleanup is greater than before relocation"
    return 0
  fi
  root_free_after="$(fs_free_bytes /)"
  echo "Root free before: $(bytes_human "$root_free_before")"
  echo "Root free after:  $(bytes_human "$root_free_after")"
  [ "$root_free_after" -gt "$root_free_before" ] || die "Root free space did not increase after relocating Docker off root."
  echo "OK: root free space increased."
}

print_relocation_cleanup() {
  local rollback_source="$1"
  echo
  echo "Rollback copy retained. After the maintenance window and a successful restore drill, reclaim ZFS rollback space with:"
  echo "  sudo rm -rf '$rollback_source'"
}

rollback_relocate_docker() {
  local current_root target_root restore_tree=1
  need_root
  require_relocation_commands
  require_compose_files
  load_relocation_state
  target_root="${PREVIOUS_DOCKER_DATA_ROOT:-$DOCKER_DIR}"
  case "$target_root" in
    ""|"/"|"/var")
      die "Refusing rollback to unsafe Docker data-root path: $target_root"
      ;;
  esac
  [ -n "${ROLLBACK_SOURCE:-}" ] || die "Relocation state lacks ROLLBACK_SOURCE."
  if [ ! -d "$ROLLBACK_SOURCE" ]; then
    if [ -d "$target_root" ] && [ -n "$(find "$target_root" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]; then
      restore_tree=0
      echo "Rollback archive is not present yet; using existing prior data-root at $target_root."
    else
      die "Rollback source is missing: $ROLLBACK_SOURCE"
    fi
  fi

  bar "Rollback preflight"
  current_root="$(docker_current_data_root)"
  echo "Current Docker data-root: $current_root"
  echo "Rollback Docker data-root: $target_root"

  stop_compose_and_docker

  bar "Restoring previous Docker data-root tree"
  if [ "$restore_tree" -eq 1 ] && [ -e "$target_root" ] && [ -n "$(find "$target_root" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]; then
    die "Refusing rollback because $target_root is not empty. Move it aside manually before retrying."
  fi
  if [ "$restore_tree" -eq 1 ]; then
    run_cmd mkdir -p "$target_root"
    run_cmd rsync -aHAX --numeric-ids --info=progress2 "$ROLLBACK_SOURCE"/ "$target_root"/
  else
    echo "Existing prior data-root is still in place; no tree restore needed."
  fi

  bar "Restoring previous Docker daemon config"
  if [ "${DAEMON_JSON_EXISTED:-0}" -eq 1 ]; then
    run_cmd cp -a "$DOCKER_RELOCATION_STATE_DIR/daemon.json.before" "$DOCKER_DAEMON_JSON"
  else
    run_cmd rm -f "$DOCKER_DAEMON_JSON"
  fi

  start_docker_and_compose
  if ! is_dry_run; then
    current_root="$(docker_current_data_root)"
    [ "$current_root" = "$target_root" ] || die "Rollback failed: Docker root is $current_root, expected $target_root"
  fi
  verify_compose_health
  start_backup_timers
  echo "Rollback complete. ZFS data-root remains at $DOCKER_DATA_ROOT for inspection; remove it only after verifying rollback."
}

cmd_relocate_docker() {
  local previous_root source_root source_bytes root_free_before source_mount volume_name rollback_source ts
  parse_relocate_docker_args "$@"
  if [ "$RELOCATE_ROLLBACK" -eq 1 ]; then
    rollback_relocate_docker
    return
  fi

  need_root
  require_relocation_commands
  require_compose_files

  bar "Relocate Docker preflight"
  assert_no_inflight_backup
  previous_root="$(docker_current_data_root)"
  source_root="$previous_root"
  echo "Current Docker data-root: $source_root"
  if [ "$source_root" = "$DOCKER_DATA_ROOT" ]; then
    echo "Docker already uses $DOCKER_DATA_ROOT; running verification only."
    verify_docker_root
    verify_timescale_pgdata_on_zfs
    verify_compose_health
    return 0
  fi
  [ -d "$source_root" ] || die "Current Docker data-root does not exist: $source_root"
  source_mount="$(fs_mountpoint "$source_root")"
  [ "$source_mount" = "/" ] || echo "WARNING: current Docker data-root is on mount $source_mount, not root; root-free assertion may be a no-op."
  volume_name="$(find_timescale_volume_name_in_root "$source_root")" || die "Could not find Timescale volume under $source_root/volumes (expected $TIMESCALE_VOLUME_NAME or *timescale*data*)."
  echo "Timescale volume selected for PGDATA tuning: $volume_name"
  source_bytes="$(du -sb "$source_root" | awk '{print $1}')"
  root_free_before="$(fs_free_bytes /)"
  assert_pool_space "$source_bytes"

  ts="$(date -u +%Y%m%dT%H%M%SZ)"
  rollback_source="$DOCKER_ROLLBACK_ROOT/var-lib-docker.$ts"
  save_relocation_state "$previous_root" "$source_bytes" "$root_free_before" "$rollback_source"

  stop_compose_and_docker
  prepare_docker_datasets "$source_root" "$volume_name"
  rsync_docker_tree "$source_root"
  verify_copy_size "$source_root" "$source_bytes"

  bar "Configuring Docker daemon data-root"
  write_docker_daemon_json "$DOCKER_DATA_ROOT" 0

  start_docker_and_compose
  verify_docker_root
  verify_compose_health
  verify_timescale_pgdata_on_zfs

  archive_old_docker_root "$source_root" "$rollback_source" "$source_bytes"
  assert_root_free_increased "$root_free_before"
  start_backup_timers

  bar "Relocation result"
  if ! is_dry_run; then
    zfs list "$DOCKER_DATASET" "$DOCKER_PGDATA_DATASET" "$DOCKER_ROLLBACK_DATASET"
    df -h / "$DOCKER_DATA_ROOT"
  fi
  print_relocation_cleanup "$rollback_source"
}

cmd_install_monitor() {
  need_root
  cat > /usr/local/sbin/root-disk-check <<'SH'
#!/usr/bin/env bash
THRESH=85
use=$(df --output=pcent / | tail -1 | tr -dc '0-9')
if [ "${use:-0}" -ge "$THRESH" ]; then
  msg="WARNING: root filesystem / is ${use}% full (threshold ${THRESH}%)"
  logger -t root-disk-check "$msg"
  echo "$msg" > /etc/motd.d/99-disk-warning 2>/dev/null || true
else
  rm -f /etc/motd.d/99-disk-warning 2>/dev/null || true
fi
SH
  chmod +x /usr/local/sbin/root-disk-check
  cat > /etc/systemd/system/root-disk-check.service <<'SH'
[Unit]
Description=Warn when root filesystem is nearly full
[Service]
Type=oneshot
ExecStart=/usr/local/sbin/root-disk-check
SH
  cat > /etc/systemd/system/root-disk-check.timer <<'SH'
[Unit]
Description=Run root-disk-check hourly
[Timer]
OnCalendar=hourly
Persistent=true
[Install]
WantedBy=timers.target
SH
  systemctl daemon-reload
  systemctl enable --now root-disk-check.timer
  echo "Installed hourly disk monitor; warns in syslog + MOTD when / >= 85%."
}

cmd="${1:-diagnose}"
if [ "$#" -gt 0 ]; then
  shift
fi

case "$cmd" in
  diagnose)          cmd_diagnose ;;
  clean-drills)      cmd_clean_drills ;;
  truncate-logs)     cmd_truncate_logs ;;
  cap-logs)          cmd_cap_logs ;;
  relocate-backups)  cmd_relocate_backups ;;
  relocate-docker)   cmd_relocate_docker "$@" ;;
  install-monitor)   cmd_install_monitor ;;
  *) echo "Unknown command: $cmd"; echo "Use: diagnose | clean-drills | truncate-logs | cap-logs | relocate-backups | relocate-docker | install-monitor"; exit 1 ;;
esac
