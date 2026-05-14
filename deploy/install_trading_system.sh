#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  exec sudo bash "$0" "$@"
fi

INSTALL_ROOT="${INSTALL_ROOT:-/opt/trading-system}"
REPO_DIR="${REPO_DIR:-$INSTALL_ROOT/repo}"
VENV_DIR="${VENV_DIR:-$INSTALL_ROOT/venv}"
DATA_DIR="${DATA_DIR:-$INSTALL_ROOT/data}"
LOGS_DIR="${LOGS_DIR:-$INSTALL_ROOT/logs}"
BACKUPS_DIR="${BACKUPS_DIR:-$INSTALL_ROOT/backups}"
ETC_DIR="${ETC_DIR:-/etc/trading-system}"
ENV_FILE="${ENV_FILE:-$ETC_DIR/trading.env}"
TRADING_USER="${TRADING_USER:-trading}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_SRC="$(cd "$SCRIPT_DIR/.." && pwd)"

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y \
  curl \
  ca-certificates \
  gnupg \
  lsb-release \
  sudo

ensure_python311_source() {
  local candidate os_id version_id
  candidate="$(apt-cache policy python3.11 | awk '/Candidate:/ {print $2}')"
  if [[ -n "$candidate" && "$candidate" != "(none)" && "$candidate" != *"~rc"* ]]; then
    return 0
  fi

  # shellcheck disable=SC1091
  source /etc/os-release
  os_id="${ID:-}"
  version_id="${VERSION_ID:-}"
  if [[ "$os_id" == "ubuntu" && "$version_id" == "22.04" ]]; then
    apt-get install -y software-properties-common
    add-apt-repository -y ppa:deadsnakes/ppa
    apt-get update
    return 0
  fi

  echo "python3.11_package_unavailable_or_not_final:candidate=${candidate:-none}:os=$os_id:$version_id" >&2
  exit 1
}

install_nodesource_node20() {
  local node_major
  install -d -m 0755 /etc/apt/keyrings
  curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
    | gpg --dearmor --batch --yes -o /etc/apt/keyrings/nodesource.gpg
  chmod 0644 /etc/apt/keyrings/nodesource.gpg
  echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" \
    >/etc/apt/sources.list.d/nodesource.list
  apt-get update
  apt-get install -y nodejs

  node_major="$(node -p "Number(process.versions.node.split('.')[0])")"
  if ! [[ "$node_major" =~ ^[0-9]+$ ]] || (( node_major < 18 )); then
    echo "node_version_too_old:$(node --version 2>/dev/null || echo missing)" >&2
    exit 1
  fi
}

ensure_python311_source
install_nodesource_node20

apt-get install -y \
  python3.11 \
  python3.11-venv \
  python3.11-dev \
  python3-pip \
  build-essential \
  pkg-config \
  libffi-dev \
  libssl-dev \
  openssl \
  sqlite3 \
  git \
  rsync \
  unzip \
  logrotate

id -u "$TRADING_USER" >/dev/null 2>&1 || useradd --system --create-home --shell /bin/bash "$TRADING_USER"

mkdir -p "$INSTALL_ROOT" "$DATA_DIR" "$LOGS_DIR" "$BACKUPS_DIR" "$ETC_DIR"

if [[ -d "$REPO_SRC/boot" && -f "$REPO_SRC/start_system.py" ]]; then
  mkdir -p "$REPO_DIR"
  rsync -a --delete \
    --exclude '.git' \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    "$REPO_SRC/" "$REPO_DIR/"
else
  echo "repo_source_not_found:$REPO_SRC" >&2
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  install -m 0600 "$REPO_DIR/deploy/env/trading.env" "$ENV_FILE"
fi

while IFS= read -r template_line; do
  if [[ "$template_line" =~ ^([A-Z][A-Z0-9_]*)= ]]; then
    template_key="${BASH_REMATCH[1]}"
    if ! grep -qE "^${template_key}=" "$ENV_FILE"; then
      printf '%s\n' "$template_line" >>"$ENV_FILE"
    fi
  fi
done <"$REPO_DIR/deploy/env/trading.env"

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[\/&#]/\\&/g'
}

set_env_value() {
  local key="$1"
  local value="$2"
  local escaped
  escaped="$(escape_sed_replacement "$value")"
  if grep -qE "^${key}=" "$ENV_FILE"; then
    sed -i "s#^${key}=.*#${key}=${escaped}#g" "$ENV_FILE"
  else
    printf '%s=%s\n' "$key" "$value" >>"$ENV_FILE"
  fi
}

get_env_value() {
  local key="$1"
  grep -E "^${key}=" "$ENV_FILE" | tail -n 1 | cut -d= -f2- || true
}

is_placeholder_secret() {
  local value
  value="$(get_env_value "$1")"
  case "$value" in
    ""|"change-me"|"CHANGE_ME"|"__GENERATE_ON_INSTALL__"|"<generate-with-openssl-rand-base64-32>")
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

ensure_generated_secret() {
  local key="$1"
  if is_placeholder_secret "$key"; then
    set_env_value "$key" "$(openssl rand -base64 32)"
  fi
}

set_env_value "TRADING_ROOT" "$INSTALL_ROOT"
set_env_value "TRADING_REPO" "$REPO_DIR"
set_env_value "TRADING_DATA" "$DATA_DIR"
set_env_value "TRADING_LOGS" "$LOGS_DIR"
set_env_value "TRADING_BACKUPS" "$BACKUPS_DIR"
set_env_value "PYTHON_VENV" "$VENV_DIR"
set_env_value "DB_PATH" "$DATA_DIR/trading.db"
set_env_value "HF_HOME" "$DATA_DIR/huggingface"
set_env_value "SENTENCE_TRANSFORMERS_HOME" "$DATA_DIR/huggingface/sentence-transformers"
set_env_value "DATA_SOURCE_MASTER_KEY_FILE" "$DATA_DIR/.data_source_master_key"
set_env_value "DASHBOARD_BASE" "http://127.0.0.1:8000"
ensure_generated_secret "DATA_SOURCE_MASTER_KEY"
ensure_generated_secret "DASHBOARD_API_TOKEN"
chmod 0600 "$ENV_FILE"

rm -f "$REPO_DIR/.env"
ln -s "$ENV_FILE" "$REPO_DIR/.env"

TRADING_REPO="$REPO_DIR" PYTHON_VENV="$VENV_DIR" PYTHON_BIN="python3.11" \
  bash "$REPO_DIR/deploy/bin/install_python_env.sh"

cd "$REPO_DIR"
node_major="$(node -p "Number(process.versions.node.split('.')[0])")"
if ! [[ "$node_major" =~ ^[0-9]+$ ]] || (( node_major < 18 )); then
  echo "node_version_too_old_before_npm_install:$(node --version 2>/dev/null || echo missing)" >&2
  exit 1
fi
npm install

chmod +x "$REPO_DIR"/deploy/install_trading_system.sh
chmod +x "$REPO_DIR"/deploy/bin/*.sh

install -m 0644 "$REPO_DIR/deploy/systemd/trading-operator.service" /etc/systemd/system/trading-operator.service
install -m 0644 "$REPO_DIR/deploy/systemd/trading-engine.service" /etc/systemd/system/trading-engine.service
install -m 0644 "$REPO_DIR/deploy/systemd/trading-backup.service" /etc/systemd/system/trading-backup.service
install -m 0644 "$REPO_DIR/deploy/systemd/trading-backup.timer" /etc/systemd/system/trading-backup.timer
install -m 0644 "$REPO_DIR/deploy/systemd/trading-upgrade.service" /etc/systemd/system/trading-upgrade.service
install -m 0644 "$REPO_DIR/deploy/logrotate/trading-system" /etc/logrotate.d/trading-system

cat >/etc/sudoers.d/trading-system <<'EOF'
trading ALL=(root) NOPASSWD: /bin/systemctl start trading-engine.service
trading ALL=(root) NOPASSWD: /bin/systemctl stop trading-engine.service
trading ALL=(root) NOPASSWD: /bin/systemctl restart trading-engine.service
trading ALL=(root) NOPASSWD: /bin/systemctl status trading-engine.service
trading ALL=(root) NOPASSWD: /bin/systemctl start trading-operator.service
trading ALL=(root) NOPASSWD: /bin/systemctl stop trading-operator.service
trading ALL=(root) NOPASSWD: /bin/systemctl restart trading-operator.service
trading ALL=(root) NOPASSWD: /bin/systemctl status trading-operator.service
trading ALL=(root) NOPASSWD: /bin/systemctl start trading-backup.service
trading ALL=(root) NOPASSWD: /bin/systemctl status trading-backup.service
trading ALL=(root) NOPASSWD: /bin/systemctl start trading-upgrade.service
trading ALL=(root) NOPASSWD: /bin/systemctl status trading-upgrade.service
trading ALL=(root) NOPASSWD: /bin/journalctl -u trading-engine.service *
trading ALL=(root) NOPASSWD: /bin/journalctl -u trading-operator.service *
trading ALL=(root) NOPASSWD: /bin/journalctl -u trading-backup.service *
trading ALL=(root) NOPASSWD: /bin/journalctl -u trading-upgrade.service *
EOF
chmod 0440 /etc/sudoers.d/trading-system

touch "$DATA_DIR/trading.db"
chown -R "$TRADING_USER:$TRADING_USER" "$INSTALL_ROOT" "$ETC_DIR" "$REPO_DIR"

TRADING_HOME="$(getent passwd "$TRADING_USER" | cut -d: -f6)"
sudo -u "$TRADING_USER" env \
  HOME="$TRADING_HOME" \
  HF_HOME="$DATA_DIR/huggingface" \
  SENTENCE_TRANSFORMERS_HOME="$DATA_DIR/huggingface/sentence-transformers" \
  "$VENV_DIR/bin/python" -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

sudo -u "$TRADING_USER" env TRADING_ENV_FILE="$ENV_FILE" PYTHON_VENV="$VENV_DIR" TRADING_REPO="$REPO_DIR" \
  "$VENV_DIR/bin/python" -c "from engine.runtime.db_repair import repair; import json; print(json.dumps(repair(), indent=2))"

systemctl daemon-reload
systemctl enable trading-operator.service
systemctl enable trading-engine.service
systemctl enable trading-backup.timer
systemctl start trading-operator.service
systemctl start trading-backup.timer

echo "install_complete"
echo "operator_url=http://$(hostname -I | awk '{print $1}'):4001"
