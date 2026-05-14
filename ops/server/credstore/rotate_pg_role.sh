#!/usr/bin/env bash
set -euo pipefail

trap 'rc=$?; echo "[rotate-pg-role] ERROR line ${BASH_LINENO[0]} while running: ${BASH_COMMAND}" >&2; exit "$rc"' ERR

CREDSTORE_DIR="${TRADING_CREDSTORE_DIR:-/etc/credstore.encrypted}"
POSTGRES_SOCKET_DIR="${TRADING_POSTGRES_SOCKET_DIR:-/var/run/postgresql}"
PGBOUNCER_USERLIST="${TRADING_PGBOUNCER_USERLIST:-/etc/pgbouncer/userlist.txt}"

log() {
  printf '[rotate-pg-role] %s\n' "$*"
}

die() {
  printf '[rotate-pg-role] ERROR: %s\n' "$*" >&2
  exit 1
}

role_to_pg_role() {
  case "$1" in
    app|ts_app) printf '%s\n' ts_app ;;
    ingest|ingestion|ts_ingest) printf '%s\n' ts_ingest ;;
    reader|ts_reader) printf '%s\n' ts_reader ;;
    *) die "unknown role '$1'; expected app, ingest, or reader" ;;
  esac
}

role_to_secret() {
  case "$1" in
    ts_app) printf '%s\n' pg_password_app ;;
    ts_ingest) printf '%s\n' pg_password_ingest ;;
    ts_reader) printf '%s\n' pg_password_reader ;;
    *) die "unknown postgres role '$1'" ;;
  esac
}

encrypt_args() {
  if [ -n "${SYSTEMD_CREDS_ENCRYPT_ARGS:-}" ]; then
    # shellcheck disable=SC2086
    printf '%s\n' ${SYSTEMD_CREDS_ENCRYPT_ARGS}
    return
  fi
  if [ -e /sys/class/tpm/tpm0 ]; then
    printf '%s\n' '--tpm2-pcrs=7'
  fi
}

set_role_password() {
  local role="$1" password="$2"
  runuser -u postgres -- psql -v ON_ERROR_STOP=1 -h "$POSTGRES_SOCKET_DIR" -p 5432 -d postgres \
    -v role="$role" -v pwd="$password" <<'SQL'
ALTER ROLE :"role" PASSWORD :'pwd';
SQL
}

install_password_credential() {
  local secret_name="$1" password="$2"
  # shellcheck disable=SC2046
  printf '%s' "$password" | systemd-creds encrypt --name="$secret_name" $(encrypt_args) - "${CREDSTORE_DIR}/${secret_name}.cred"
  chown root:root "${CREDSTORE_DIR}/${secret_name}.cred"
  chmod 0400 "${CREDSTORE_DIR}/${secret_name}.cred"
}

refresh_pgbouncer_userlist() {
  local tmp
  tmp="$(mktemp)"
  runuser -u postgres -- psql -h "$POSTGRES_SOCKET_DIR" -p 5432 -d postgres -AtF $'\t' \
    -c "SELECT '\"' || rolname || '\" \"' || rolpassword || '\"' FROM pg_authid WHERE rolname IN ('ts_app','ts_ingest','ts_reader') ORDER BY rolname" \
    > "$tmp"
  install -m 0640 -o postgres -g postgres "$tmp" "$PGBOUNCER_USERLIST"
  rm -f "$tmp"
}

main() {
  if [ "$(id -u)" -ne 0 ]; then
    die "rotate_pg_role.sh must run as root"
  fi
  [ "$#" -eq 1 ] || die "usage: rotate_pg_role.sh <app|ingest|reader>"
  command -v systemd-creds >/dev/null 2>&1 || die "systemd-creds is required"
  command -v openssl >/dev/null 2>&1 || die "openssl is required"
  install -d -o root -g root -m 0700 "$CREDSTORE_DIR"

  local pg_role secret_name password
  pg_role="$(role_to_pg_role "$1")"
  secret_name="$(role_to_secret "$pg_role")"
  password="${TS_NEW_PG_PASSWORD:-$(openssl rand -base64 24)}"

  set_role_password "$pg_role" "$password"
  install_password_credential "$secret_name" "$password"
  refresh_pgbouncer_userlist
  systemctl reload pgbouncer
  log "rotated ${pg_role}, updated ${secret_name}, and reloaded PgBouncer"
}

main "$@"
