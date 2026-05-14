#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[credstore-prune-archive] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

CREDSTORE_DIR="${TRADING_CREDSTORE_DIR:-/etc/credstore.encrypted}"
ARCHIVE_DIR="${TRADING_MASTER_KEY_ARCHIVE_DIR:-${CREDSTORE_DIR}/keys/archive}"
ARCHIVE_RETENTION_HOURS="${TRADING_MASTER_KEY_ARCHIVE_RETENTION_HOURS:-0}"

log() {
  printf '[credstore-prune-archive] %s\n' "$*"
}

die() {
  printf '[credstore-prune-archive] ERROR: %s\n' "$*" >&2
  exit 1
}

main() {
  if [ "$(id -u)" -ne 0 ]; then
    die "prune_archive.sh must run as root"
  fi
  case "$ARCHIVE_RETENTION_HOURS" in
    ''|*[!0-9]*) die "TRADING_MASTER_KEY_ARCHIVE_RETENTION_HOURS must be a non-negative integer" ;;
  esac
  if [ "$ARCHIVE_RETENTION_HOURS" -eq 0 ] || [ ! -d "$ARCHIVE_DIR" ]; then
    log "archive pruning skipped"
    return
  fi

  local minutes
  minutes="$((ARCHIVE_RETENTION_HOURS * 60))"
  find "$ARCHIVE_DIR" -type f -name 'master_key.*.cred' -mmin +"$minutes" -delete
  log "pruned archived master keys older than ${ARCHIVE_RETENTION_HOURS}h from ${ARCHIVE_DIR}"
}

main "$@"
