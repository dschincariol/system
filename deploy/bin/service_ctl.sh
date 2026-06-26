#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-status}"
TARGET="${2:-all}"
SINCE="${3:-}"
LINES="${4:-400}"
ARG_COUNT="$#"
LOG_DIR="${TRADING_LOGS:-${TRADING_LOG_DIR:-${LOG_DIR:-/var/lib/trading/logs}}}"

map_unit() {
  case "$1" in
    engine) echo "trading-engine.service" ;;
    operator) echo "trading-operator.service" ;;
    backup) echo "trading-backup.service" ;;
    upgrade) echo "trading-upgrade.service" ;;
    all) echo "all" ;;
    *)
      echo "invalid_target:$1" >&2
      exit 2
      ;;
  esac
}

map_logfile() {
  case "$1" in
    engine) echo "$LOG_DIR/engine.log" ;;
    operator) echo "$LOG_DIR/operator.log" ;;
    upgrade) echo "$LOG_DIR/upgrade.log" ;;
    backup|all) echo "" ;;
    *)
      echo "invalid_target:$1" >&2
      exit 2
      ;;
  esac
}

emit_status_json() {
  local unit="$1"
  local active enabled sub load
  active="$(systemctl is-active "$unit" 2>/dev/null || true)"
  enabled="$(systemctl is-enabled "$unit" 2>/dev/null || true)"
  sub="$(systemctl show "$unit" --property SubState --value 2>/dev/null || true)"
  load="$(systemctl show "$unit" --property LoadState --value 2>/dev/null || true)"
  printf '{"ok":true,"unit":"%s","active":"%s","enabled":"%s","substate":"%s","load":"%s"}\n' \
    "$unit" "$active" "$enabled" "$sub" "$load"
}

require_sudo() {
  sudo -n true 2>/dev/null || {
    echo '{"ok":false,"error":"sudo_non_interactive_required"}'
    exit 1
  }
}

if [[ "$ACTION" == "logs" ]]; then
  UNIT="$(map_unit "$TARGET")"
  LOGFILE="$(map_logfile "$TARGET")"
  require_sudo
  if [[ -n "$LOGFILE" && -r "$LOGFILE" ]]; then
    if [[ "$ARG_COUNT" -ge 4 && -n "$SINCE" ]]; then
      echo "# note: file sink has no --since filter; showing last ${LINES} lines" >&2
      exec sudo -n tail -n "$LINES" -- "$LOGFILE"
    fi
    exec sudo -n tail -n "${SINCE:-$LINES}" -- "$LOGFILE"
  fi
  if [[ "$ARG_COUNT" -ge 4 && -n "$SINCE" ]]; then
    exec sudo -n journalctl -u "$UNIT" --since "$SINCE" -n "$LINES" --no-pager
  fi
  exec sudo -n journalctl -u "$UNIT" -n "${SINCE:-$LINES}" --no-pager
fi

if [[ "$ACTION" == "logs_since" ]]; then
  UNIT="$(map_unit "$TARGET")"
  LOGFILE="$(map_logfile "$TARGET")"
  if [[ -z "$SINCE" ]]; then
    echo '{"ok":false,"error":"missing_since"}'
    exit 2
  fi
  require_sudo
  if [[ -n "$LOGFILE" && -r "$LOGFILE" ]]; then
    echo "# note: file sink has no --since filter; showing last ${LINES} lines" >&2
    exec sudo -n tail -n "$LINES" -- "$LOGFILE"
  fi
  exec sudo -n journalctl -u "$UNIT" --since "$SINCE" -n "$LINES" --no-pager
fi

if [[ "$ACTION" == "status" && "$TARGET" == "all" ]]; then
  printf '{'
  first=1
  for name in engine operator backup upgrade; do
    unit="$(map_unit "$name")"
    json="$(emit_status_json "$unit")"
    if [[ $first -eq 0 ]]; then
      printf ','
    fi
    first=0
    printf '"%s":%s' "$name" "$json"
  done
  printf '}\n'
  exit 0
fi

UNIT="$(map_unit "$TARGET")"

case "$ACTION" in
  status)
    emit_status_json "$UNIT"
    ;;
  start|stop|restart|enable|disable)
    require_sudo
    sudo -n systemctl "$ACTION" "$UNIT"
    emit_status_json "$UNIT"
    ;;
  *)
    echo "{\"ok\":false,\"error\":\"invalid_action:$ACTION\"}"
    exit 2
    ;;
esac
