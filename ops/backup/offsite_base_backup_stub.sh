#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=offsite_base_backup_stub %s\n' "$level" "$event" "$*" >&2
}

die() {
  log error "$1" "${2:-}"
  exit 1
}

backup_name="${TS_BASE_BACKUP_NAME:-}"
dest="${TS_OFFSITE_BACKUP_DEST:-}"

[ -n "$backup_name" ] || die backup_name_required "set_by=base_backup.sh"
[ -n "$dest" ] || die destination_required "set TS_OFFSITE_BACKUP_DEST to s3://bucket/prefix or a mounted NAS/local directory"

case "$backup_name" in
  *[!A-Za-z0-9._:-]*|"")
    die invalid_backup_name "backup_name=${backup_name}"
    ;;
esac

case "$dest" in
  s3://*)
    command -v aws >/dev/null 2>&1 || die aws_cli_missing "dest=${dest}"
    aws s3 cp - "${dest%/}/${backup_name}.tar"
    ;;
  /*)
    install -d -m "${TS_OFFSITE_BACKUP_DIR_MODE:-0750}" "$dest"
    tmp="${dest%/}/.${backup_name}.tar.$$"
    final="${dest%/}/${backup_name}.tar"
    cleanup() {
      rm -f "$tmp"
    }
    trap cleanup EXIT
    cat > "$tmp"
    chmod "${TS_OFFSITE_BACKUP_FILE_MODE:-0640}" "$tmp"
    mv -f "$tmp" "$final"
    trap - EXIT
    ;;
  *)
    die unsupported_destination "dest=${dest}; expected s3://... or absolute directory path"
    ;;
esac

log info copied "backup_name=${backup_name} dest=${dest}"
