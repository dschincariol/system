#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=artifact_snapshot %s\n' "$level" "$event" "$*"
}

die() {
  log error "$1" "${2:-}"
  exit 1
}

source_dir="${TS_ARTIFACT_SOURCE_DIR:-${TS_ARTIFACTS_ROOT:-/var/lib/trading/artifacts}}"
local_dest="${TS_BACKUP_ARTIFACT_DIR:-/var/backups/trading/artifacts}"
dest="${TS_ARTIFACT_OFFSITE_DEST:-$local_dest}"
stamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

command -v rsync >/dev/null 2>&1 || die missing_rsync "hint=install_rsync"
[ -d "$source_dir" ] || die source_missing "source_dir=${source_dir}"

if [ "$dest" = "$local_dest" ]; then
  mkdir -p "$dest"
fi

log info snapshot_started "source=${source_dir} destination=${dest} offsite=$([ "$dest" != "$local_dest" ] && printf true || printf false) started_at=${stamp}"

rsync -aH --delete --numeric-ids \
  --exclude='temp/' \
  --exclude='.tmp/' \
  --info=stats2 \
  "${source_dir}/" "${dest}/"

log info snapshot_complete "source=${source_dir} destination=${dest} completed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
