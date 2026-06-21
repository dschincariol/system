#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
REMEDIATION_SCRIPT="${REPO_ROOT}/ops/server/disk_remediation.sh"

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
grep -Fq 'run_cmd cp -a "$DOCKER_RELOCATION_STATE_DIR/daemon.json.before" "$DOCKER_DAEMON_JSON"' "$REMEDIATION_SCRIPT"
grep -Fq 'run_cmd rm -f "$DOCKER_DAEMON_JSON"' "$REMEDIATION_SCRIPT"
grep -Fq 'BACKUP_POSTGRES_UID="${TS_BACKUP_POSTGRES_UID:-70}"' "$REMEDIATION_SCRIPT"
grep -Fq 'normalize_backup_wal_target_permissions "$stage"' "$REMEDIATION_SCRIPT"
grep -Fq 'chown "${BACKUP_POSTGRES_UID}:${BACKUP_GROUP}" "$root" "$wal" "$tmp"' "$REMEDIATION_SCRIPT"
grep -Fq 'chmod 2750 "$root" "$wal" "$tmp"' "$REMEDIATION_SCRIPT"

python3 - "$REMEDIATION_SCRIPT" <<'PY'
from pathlib import Path
import sys

text = Path(sys.argv[1]).read_text(encoding="utf-8")
old_path = "/home/david/gitsandbox" + "/disk" + "-remediation.sh"
if old_path in text:
    raise SystemExit("old machine-specific disk remediation path remains in script")

cmd = text[text.index("cmd_relocate_docker()"): text.index("cmd_install_monitor()")]
for needle in (
    'if [ "$source_root" = "$DOCKER_DATA_ROOT" ]; then',
    'echo "Docker already uses $DOCKER_DATA_ROOT; running verification only."',
    "verify_docker_root",
    "verify_timescale_pgdata_on_zfs",
    "verify_compose_health",
    "return 0",
):
    if needle not in cmd:
        raise SystemExit(f"relocate-docker idempotency guard missing {needle!r}")

if cmd.index('assert_pool_space "$source_bytes"') > cmd.index("stop_compose_and_docker"):
    raise SystemExit("space gate must run before stopping Docker")
if cmd.index('verify_copy_size "$source_root" "$source_bytes"') > cmd.index('write_docker_daemon_json "$DOCKER_DATA_ROOT" 0'):
    raise SystemExit("copy-size verification must precede daemon.json switch")
if cmd.index("verify_timescale_pgdata_on_zfs") > cmd.index('archive_old_docker_root "$source_root" "$rollback_source" "$source_bytes"'):
    raise SystemExit("runtime verification must precede old-root archive/removal")

archive = text[text.index("archive_old_docker_root()"): text.index("assert_root_free_increased()")]
real_archive = archive[archive.index('  mkdir -p "$rollback_source"'):]
if real_archive.index('[ "$archive_bytes" -ge "$min_bytes" ]') > real_archive.index('rm -rf "$source_root"'):
    raise SystemExit("rollback archive >=99% assertion must precede rm -rf")

rollback = text[text.index("rollback_relocate_docker()"): text.index("cmd_relocate_docker()")]
for needle in (
    'load_relocation_state',
    'run_cmd cp -a "$DOCKER_RELOCATION_STATE_DIR/daemon.json.before" "$DOCKER_DAEMON_JSON"',
    'run_cmd rm -f "$DOCKER_DAEMON_JSON"',
    "verify_compose_health",
):
    if needle not in rollback:
        raise SystemExit(f"rollback guard missing {needle!r}")
PY

echo "[test_disk_remediation_relocate_docker] ok"
