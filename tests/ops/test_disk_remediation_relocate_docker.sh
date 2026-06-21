#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REMEDIATION_SCRIPT="$(cd "${REPO_ROOT}/../.." && pwd)/disk-remediation.sh"

bash -n "$REMEDIATION_SCRIPT"

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

fake_bin="${tmp_dir}/bin"
docker_root="${tmp_dir}/var-lib-docker"
compose_root="${tmp_dir}/repo/deploy/compose"
mkdir -p \
  "$fake_bin" \
  "$docker_root/volumes/compose_timescaledb-data/_data/pg_wal" \
  "$compose_root"
touch \
  "$compose_root/.env" \
  "$compose_root/docker-compose.external-services.yml" \
  "$compose_root/docker-compose.stack.yml"
printf '16\n' > "$docker_root/volumes/compose_timescaledb-data/_data/PG_VERSION"
printf 'wal\n' > "$docker_root/volumes/compose_timescaledb-data/_data/pg_wal/000000010000000000000001"

cat > "${fake_bin}/docker" <<'SH'
#!/usr/bin/env bash
set -euo pipefail
if [ "${1:-}" = "info" ]; then
  printf '%s\n' "${TS_DOCKER_DIR:?}"
  exit 0
fi
exit 0
SH
chmod +x "${fake_bin}/docker"

for cmd in zfs rsync systemctl; do
  cat > "${fake_bin}/${cmd}" <<'SH'
#!/usr/bin/env bash
exit 0
SH
  chmod +x "${fake_bin}/${cmd}"
done

output="$(
  PATH="${fake_bin}:$PATH" \
  TS_DOCKER_DIR="$docker_root" \
  TS_DOCKER_DAEMON_JSON="${tmp_dir}/etc/docker/daemon.json" \
  TS_DOCKER_DATASET="zpool/docker" \
  TS_DOCKER_DATA_ROOT="/zpool/docker" \
  TS_DOCKER_PGDATA_DATASET="zpool/docker/timescaledb-pgdata" \
  TS_DOCKER_ROLLBACK_DATASET="zpool/docker-rollback" \
  TS_DOCKER_ROLLBACK_ROOT="/zpool/docker-rollback" \
  TS_DOCKER_RELOCATION_STATE_DIR="${tmp_dir}/state" \
  TRADING_REPO_ROOT="${tmp_dir}/repo" \
  bash "$REMEDIATION_SCRIPT" relocate-docker --dry-run
)"

require_output() {
  local needle="$1"
  if ! grep -Fq "$needle" <<< "$output"; then
    echo "missing expected dry-run output: $needle" >&2
    echo "--- dry-run output ---" >&2
    echo "$output" >&2
    exit 1
  fi
}

require_output "Current Docker data-root: $docker_root"
require_output "Timescale volume selected for PGDATA tuning: compose_timescaledb-data"
require_output "would require ZFS free space"
require_output "zfs create"
require_output "mountpoint=/zpool/docker"
require_output "recordsize=16K"
require_output "logbias=throughput"
require_output "compression=lz4"
require_output "primarycache=metadata"
require_output "rsync -aHAX --numeric-ids --info=progress2"
require_output "would set Docker data-root to /zpool/docker"
require_output "would wait for all Compose containers to be running and healthy"
require_output "sudo rm -rf '/zpool/docker-rollback/var-lib-docker."

grep -Fq 'relocate-docker)   cmd_relocate_docker "$@" ;;' "$REMEDIATION_SCRIPT"
grep -Fq 'write_docker_daemon_json "$DOCKER_DATA_ROOT" 0' "$REMEDIATION_SCRIPT"
grep -Fq 'write_docker_daemon_json "" 1' "$REMEDIATION_SCRIPT"

echo "[test_disk_remediation_relocate_docker] ok"
