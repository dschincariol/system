#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=state_snapshot %s\n' "$level" "$event" "$*"
}

state_dir="${TS_BACKUP_STATE_DIR:-/var/backups/trading/state}"
etc_dir="${TS_ETC_DIR:-/etc/trading}"
artifact_dir="${TS_ARTIFACT_SOURCE_DIR:-${TS_ARTIFACTS_ROOT:-/var/lib/trading/artifacts}}"
redis_dir="${TS_REDIS_DIR:-/var/lib/trading/redis}"
stamp="${TS_BACKUP_STAMP:-$(date -u +%Y-%m-%dT%H%M%SZ)}"
tmp_meta="$(mktemp -d "${TMPDIR:-/tmp}/trading-state.XXXXXX")"
tmp_tar="${state_dir}/state_${stamp}.tar.gz.tmp"
final_tar="${state_dir}/state_${stamp}.tar.gz"

cleanup() {
  rm -rf "$tmp_meta"
  rm -f "$tmp_tar"
}
trap cleanup EXIT

mkdir -p "$state_dir"

{
  printf 'generated_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'artifact_dir=%s\n' "$artifact_dir"
  if [ -d "$artifact_dir" ]; then
    find "$artifact_dir" \
      -path "${artifact_dir}/objects" -prune -o \
      -path "${artifact_dir}/temp" -prune -o \
      -printf '%P\t%y\t%s\t%TY-%Tm-%TdT%TH:%TM:%TS%Tz\n' | sort
  else
    printf 'missing_artifact_dir\n'
  fi
} > "${tmp_meta}/artifact_listing.txt"

tar_args=(-czf "$tmp_tar" -C "$tmp_meta" artifact_listing.txt)

if [ -d "$etc_dir" ]; then
  tar_args+=(-C / "${etc_dir#/}")
fi

if [ -d "$redis_dir" ]; then
  tar_args+=(-C / "${redis_dir#/}")
fi

tar "${tar_args[@]}"
chmod 0640 "$tmp_tar"
mv -f "$tmp_tar" "$final_tar"

bytes="$(wc -c < "$final_tar" | tr -d ' ')"
log info snapshot_complete "snapshot=${final_tar} bytes=${bytes} etc_dir=${etc_dir} redis_dir=${redis_dir} artifact_dir=${artifact_dir}"
