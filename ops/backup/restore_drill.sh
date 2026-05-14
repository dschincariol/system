#!/usr/bin/env bash
set -euo pipefail

log() {
  local level="$1"
  local event="$2"
  shift 2
  printf 'level=%s event=%s script=restore_drill %s\n' "$level" "$event" "$*"
}

die() {
  log error "$1" "${2:-}"
  exit 1
}

pick_port() {
  python3 - <<'PY'
import socket

sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
restore_script="${TS_RESTORE_SCRIPT:-${script_dir}/restore.sh}"
sanity_sql="${TS_RESTORE_SANITY_SQL:-${repo_root}/tools/restore_sanity.sql}"
drill_dir="${TS_RESTORE_DRILL_DIR:-/var/backups/trading/drills}"
work_root="${TS_RESTORE_DRILL_WORK_ROOT:-${drill_dir}/work}"
stamp="${TS_RESTORE_DRILL_STAMP:-$(date -u +%Y-%m-%dT%H%M%SZ)}"
work_dir="${work_root}/restore_drill_${stamp}"
datadir="${work_dir}/pgdata"
report="${drill_dir}/restore_drill_${stamp}.txt"
run_log="${work_dir}/run.log"
sanity_log="${work_dir}/sanity.out"
restore_port="${TS_RESTORE_PORT:-$(pick_port)}"
pgbouncer_port="${TS_RESTORE_PGBOUNCER_PORT:-$(pick_port)}"
restore_db="${TS_RESTORE_DB:-${PGDATABASE:-trading}}"
restore_user="${TS_RESTORE_USER:-postgres}"
keep_data="${TS_RESTORE_DRILL_KEEP_DATA:-0}"
allow_direct="${TS_RESTORE_DRILL_ALLOW_DIRECT:-0}"
start_epoch="$(date +%s)"
pgbouncer_pidfile="${work_dir}/pgbouncer.pid"

mkdir -p "$drill_dir" "$work_dir"

cleanup() {
  if [ -f "$pgbouncer_pidfile" ]; then
    pid="$(cat "$pgbouncer_pidfile" 2>/dev/null || true)"
    if [ -n "${pid:-}" ]; then
      kill "$pid" >/dev/null 2>&1 || true
      rm -f "$pgbouncer_pidfile"
    fi
  fi
  if [ -f "${datadir}/PG_VERSION" ]; then
    "${PGCTL_BIN:-pg_ctl}" -D "$datadir" -m fast stop >/dev/null 2>&1 || true
  fi
  if [ "$keep_data" != "1" ]; then
    rm -rf "$work_dir"
  fi
}

write_report() {
  local rc="$1"
  local elapsed_s="$2"
  {
    printf 'restore_drill_report_version=1\n'
    printf 'generated_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'target_time=latest\n'
    printf 'exit_code=%s\n' "$rc"
    printf 'status=%s\n' "$([ "$rc" -eq 0 ] && printf pass || printf fail)"
    printf 'time_to_recover_s=%s\n' "$elapsed_s"
    printf 'restore_port=%s\n' "$restore_port"
    printf 'pgbouncer_port=%s\n' "$pgbouncer_port"
    printf 'datadir=%s\n' "$datadir"
    if [ -f "${datadir}/restore.env" ]; then
      printf '\n[restore_env]\n'
      cat "${datadir}/restore.env"
    fi
    printf '\n[run_log]\n'
    if [ -f "$run_log" ]; then
      cat "$run_log"
    fi
    printf '\n[sanity_results]\n'
    if [ -f "$sanity_log" ]; then
      cat "$sanity_log"
    fi
    printf '\n[anomalies]\n'
    if [ "$rc" -eq 0 ]; then
      printf 'none\n'
    else
      printf 'restore_drill_failed\n'
    fi
  } > "$report"
}

start_pgbouncer() {
  local cfg="${work_dir}/pgbouncer.ini"
  local userlist="${work_dir}/userlist.txt"
  command -v pgbouncer >/dev/null 2>&1 || return 1
  printf '"%s" ""\n' "$restore_user" > "$userlist"
  cat > "$cfg" <<EOF
[databases]
${restore_db} = host=127.0.0.1 port=${restore_port} dbname=${restore_db}

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = ${pgbouncer_port}
auth_type = trust
auth_file = ${userlist}
admin_users = ${restore_user}
pool_mode = session
ignore_startup_parameters = extra_float_digits
pidfile = ${pgbouncer_pidfile}
logfile = ${work_dir}/pgbouncer.log
EOF
  pgbouncer -d "$cfg"
  for _ in $(seq 1 30); do
    if PGUSER="$restore_user" pg_isready -h 127.0.0.1 -p "$pgbouncer_port" -d "$restore_db" >/dev/null 2>&1; then
      log info pgbouncer_ready "port=${pgbouncer_port}"
      return 0
    fi
    sleep 1
  done
  return 1
}

run_drill() {
  [ -f "$restore_script" ] || die restore_script_missing "path=${restore_script}"
  [ -f "$sanity_sql" ] || die sanity_sql_missing "path=${sanity_sql}"

  log info drill_started "work_dir=${work_dir} datadir=${datadir} report=${report}"
  TS_RESTORE_PORT="$restore_port" TS_RESTORE_DB="$restore_db" TS_RESTORE_USER="$restore_user" \
    bash "$restore_script" --target-time latest --into "$datadir" --allow-trade-paused --force

  if start_pgbouncer; then
    PGUSER="$restore_user" psql -X -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$pgbouncer_port" -d "$restore_db" -f "$sanity_sql" > "$sanity_log" 2>&1
  else
    if [ "$allow_direct" != "1" ]; then
      die pgbouncer_unavailable "hint=install_pgbouncer_or_set_TS_RESTORE_DRILL_ALLOW_DIRECT=1_for_local_dry_runs"
    fi
    log warn pgbouncer_unavailable_direct_allowed "port=${restore_port}"
    source "${datadir}/restore.env"
    PGUSER="$restore_user" psql -X -v ON_ERROR_STOP=1 -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -f "$sanity_sql" > "$sanity_log" 2>&1
  fi
  log info drill_sanity_passed "sanity_log=${sanity_log}"
}

set +e
run_drill > "$run_log" 2>&1
rc=$?
set -e
elapsed_s="$(($(date +%s) - start_epoch))"
write_report "$rc" "$elapsed_s"
log info drill_report_written "report=${report} exit_code=${rc} elapsed_s=${elapsed_s}"
cleanup
exit "$rc"
