# OS Migration Runbook: bart 25.10 to LTS

This runbook moves host `bart` off Ubuntu 25.10 "Questing" and onto a supported LTS with minimum trading downtime. The target is Ubuntu 26.04 LTS "Resolute"; Ubuntu 24.04 LTS "Noble" is documented only as a conservative reinstall fallback. Do not run an in-place downgrade from 25.10 to 24.04.

The repo-owned gates are:

- `ops/server/os_migration_preflight.py`: read-only evidence capture before the cutover.
- `ops/server/os_migration_postflight.py`: read-only PASS/FAIL verification after the cutover.

Preflight systemd evidence is collected from unit names shipped in `ops/server/systemd/`. Keep quiesce and bring-up commands aligned with those repo unit files; do not add ad hoc service names to this runbook unless the matching unit file is added to the repo or verified on the host.

The scripts never perform the OS upgrade, stop services, create ZFS snapshots, roll back ZFS datasets, or start containers. The operator runs those commands explicitly from this runbook.

## Go/No-Go Rules

- GO only if the preflight gate exits `0`, writes a JSON report, and every printed check is `PASS`.
- NO-GO if preflight cannot collect package inventory, APT sources, ZFS status, Docker image/container inventory, or systemd unit health without sudo.
- NO-GO if any third-party APT source in the preflight report lacks a reviewed upgrade plan. Docker, AMD/ROCm, PPAs, and vendor repos must be either updated for the target codename, disabled for the upgrade, or replaced by Ubuntu archive packages.
- NO-GO if ZFS pool health is not `ONLINE`, if `zpool status` reports corruption, or if a scrub is active and cannot finish before the maintenance window.
- NO-GO if signed backup/WAL/restore evidence is missing or failing before quiesce.
- GO after the upgrade only if postflight exits `0` and every check is `PASS`.

## Compatibility Notes

Current host:

- Ubuntu 25.10 "Questing" is an interim release with 9 months of support. Canonical's 25.10 release notes say it is supported until July 2026, and the EOL announcement states 25.10 reaches end of life on July 9, 2026 with 25.10 -> 26.04 as the supported upgrade path.
- 25.10 ships Linux 6.17, which is why it was useful for initial Strix Halo enablement but is not acceptable for a production money-handling host near EOL.

Target:

- Ubuntu 26.04 LTS "Resolute" is supported until April 2031.
- 26.04 release notes list upstream Linux kernel 7.0 and ZFS 2.4.1.
- 26.04 release notes list Docker 29. The postflight gate checks Docker daemon access, expected containers, and preflight image IDs because Docker 29 has image-store and CLI output changes.
- 26.04 includes AMD ROCm 7.1.0 in Ubuntu Universe. The same release notes list `gfx1151` as Strix Halo / Ryzen AI MAX 300 Series with CI status `YES`. If T1.1 has enabled ROCm use in this repo, run postflight with `--require-rocm --rocm-gfx gfx1151`.

Conservative fallback:

- Ubuntu 24.04 LTS "Noble" is supported until May 31, 2029 and ships Linux 6.8 in the base LTS release.
- 24.04 is a reinstall fallback, not an in-place downgrade target from 25.10. Use it only if 26.04 has an unresolved blocker and a fresh install can import the ZFS pool without upgrading pool features.
- AMD's current ROCm package-manager docs list Ubuntu 24.04 packages from `repo.radeon.com` for ROCm 7.2.4, but that is a third-party source path rather than the 26.04 in-archive ROCm path. For Strix Halo, 24.04 must be treated as hardware-validation work before production.

Sources:

- Ubuntu 25.10 release notes: https://documentation.ubuntu.com/release-notes/25.10/
- Ubuntu 25.10 EOL/support-path announcement: https://discourse.ubuntu.com/t/ubuntu-25-10-questing-quokka-released/69067
- Ubuntu 26.04 release notes: https://documentation.ubuntu.com/release-notes/26.04/
- Ubuntu 26.04 changes since 25.10: https://documentation.ubuntu.com/release-notes/26.04/changes-since-previous-interim/
- Ubuntu 24.04 release notes: https://documentation.ubuntu.com/release-notes/24.04/
- AMD ROCm Ubuntu package-manager docs: https://rocm.docs.amd.com/projects/install-on-linux/en/latest/install/install-methods/package-manager/package-manager-ubuntu.html
- AMD ROCm compatibility matrix: https://rocm.docs.amd.com/en/latest/compatibility/compatibility-matrix.html

## Phase 0: Prepare The Window

1. Announce a maintenance window that covers preflight, final backup evidence, ZFS snapshots, release upgrade, reboot, postflight, and rollback.
2. Freeze application changes and record the current git SHA:

```bash
git rev-parse HEAD
git status --short --untracked-files=all
```

3. Confirm the operator has console access, recovery media, current credentials, and an out-of-band way to reach the machine if networking changes during the release upgrade.
4. Confirm no live orders should be submitted during the window. Keep live execution disabled or set the global hold before stopping services.

## Phase 1: Preflight Evidence

Run this from the repo root as the normal operator user, not with sudo:

```bash
python ops/server/os_migration_preflight.py --target-lts resolute
```

Expected output:

```text
OS migration preflight gate: PASS
PASS: source_os_supported_for_migration - VERSION_CODENAME=questing; expected questing before cutover or an LTS after cutover
PASS: target_lts_documented - target_lts=resolute
PASS: package_inventory_collected - packages=<count>
PASS: apt_sources_collected - third_party_sources=<count>
PASS: zfs_status_collected - zpool status and zfs list must be readable without sudo
PASS: docker_inventory_collected - containers=<count>; images=<count>
PASS: systemd_health_collected - trading unit health must be readable through systemctl
report: var/os_migration/preflight_<host>_<timestamp>.json
```

The JSON report captures:

- `/etc/os-release`, kernel release, Python/runtime metadata.
- Full `dpkg-query` package inventory and manual package list.
- `/etc/apt/sources.list` and `/etc/apt/sources.list.d/*.{list,sources}`, classified as Ubuntu, local, or third-party.
- `apt-cache policy` for kernel, ZFS, Docker, containerd, ROCm, and release-upgrader packages.
- `zfs version`, `zpool version`, `zpool list`, `zpool status -v`, `zpool import`, mounted datasets, and existing snapshots.
- Docker daemon info, Compose version, all containers, all images, image IDs, repo digests, and container inspect data.
- `systemctl` health for trading services and timers.

Preserve the report with the change ticket. The postflight gate can compare Docker image IDs against it:

```bash
PRE_FLIGHT_REPORT="$(ls -t var/os_migration/preflight_*.json | head -1)"
```

## Phase 2: Final Backup And Quiesce

Run signed backup/WAL/restore evidence before stopping timers:

```bash
sudo bash ops/server/install_backup_evidence_gate.sh --compose --run-evidence
jq '{base_backup,wal_archive,wal_archiver,restore_drill,signature}' \
  /var/backups/trading/evidence/latest_backup_restore_evidence.json
```

Expected: every component status is `pass`, and `signature.status` is `signed`.

Quiesce trading in this order:

1. Keep or set the execution hold. If live capital has ever been armed, cancel broker working orders before stopping the runtime. Use the broker-risk control path documented in `docs/PRODUCTION_CHECKLIST.md`.
2. Stop app containers before data containers:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  stop runtime operator offline-worker
```

3. Stop trading systemd services that may enqueue jobs, ingest, or serve operator traffic:

```bash
sudo systemctl stop \
  trading.target \
  trading-api.service \
  trading-jobs.service \
  trading-stream-prices.service \
  trading-ingest.service \
  trading-prod-preflight.service
```

4. Stop trading timers so no backup, snapshot, prune, or restore-drill job starts mid-upgrade:

```bash
sudo systemctl stop \
  trading-state-snapshot.timer \
  trading-artifact-snapshot.timer \
  trading-base-backup.timer \
  trading-backup-evidence.timer \
  trading-backup-prune.timer \
  trading-restore-drill.timer
```

5. Flush the database and stop data services:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  exec timescaledb sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -X -v ON_ERROR_STOP=1 -c "CHECKPOINT;" -c "SELECT pg_switch_wal();"'

if systemctl list-unit-files pgbouncer.service --no-legend | grep -q '^pgbouncer\.service'; then
  sudo systemctl stop pgbouncer.service
fi

docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  stop timescaledb redis minio
```

6. Confirm nothing trading-owned is still running:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}' | grep '^trading-' || true
systemctl list-units 'trading*' --all --no-pager
systemctl list-timers 'trading*' --all --no-pager
```

## Phase 3: ZFS Snapshot Before Upgrade

Do not run `zpool upgrade` during this migration. Pool feature upgrades can make rollback to the previous OS or rescue media harder.

Create recursive snapshots for the trading datasets:

```bash
SNAP="pre-os-2604-$(date -u +%Y%m%dT%H%M%SZ)"
sudo zfs snapshot -r "zpool/trading@${SNAP}"
sudo zfs list -t snapshot -r zpool/trading | grep "${SNAP}"
sudo zpool status -v zpool
```

If the host has additional ZFS datasets for Docker root, backups, or repo checkout state, snapshot those explicitly:

```bash
sudo zfs snapshot -r "zpool/docker@${SNAP}"
sudo zfs snapshot -r "zpool/backups@${SNAP}"
```

Record `SNAP` in the change ticket and in the shell history notes for rollback.

## Phase 4: OS Upgrade

The repo does not automate this phase.

For 26.04 LTS target:

```bash
sudo apt update
sudo apt full-upgrade
sudo reboot
sudo do-release-upgrade
sudo reboot
```

During release-upgrader prompts:

- Preserve local config only when the diff is understood. Capture any changed files.
- Disable or update third-party APT sources that are not valid for `resolute`.
- Keep Docker data paths and ZFS mountpoints unchanged.
- Do not run `zpool upgrade`.

For 24.04 LTS fallback:

- Do not attempt an in-place downgrade.
- Install 24.04 LTS cleanly to the OS disk.
- Install Docker, ZFS tools, and the repo prerequisites.
- Import the pool without upgrading features.
- Restore repo checkout and compose/systemd config from the preflight report and backups.

## Phase 5: Post-Upgrade Bring-Up

After the final reboot:

```bash
cat /etc/os-release
uname -a
zfs version
zpool status -v zpool
docker info
```

Start data services first:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  up -d timescaledb redis minio
```

Run backup evidence before starting trading runtime:

```bash
sudo systemctl start \
  trading-state-snapshot.timer \
  trading-artifact-snapshot.timer \
  trading-base-backup.timer \
  trading-backup-evidence.timer \
  trading-backup-prune.timer \
  trading-restore-drill.timer

sudo bash ops/server/install_backup_evidence_gate.sh --compose --run-evidence
```

Start runtime and operator:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  up -d runtime operator
```

Keep live trading disabled until postflight and production preflight pass.

## Phase 6: Postflight Gate

Run without ROCm enforcement if T1.1 has not landed:

```bash
python ops/server/os_migration_postflight.py \
  --target-codename resolute \
  --zfs-pool zpool \
  --preflight-report "${PRE_FLIGHT_REPORT}"
```

Run with ROCm enforcement if T1.1 has landed:

```bash
python ops/server/os_migration_postflight.py \
  --target-codename resolute \
  --zfs-pool zpool \
  --preflight-report "${PRE_FLIGHT_REPORT}" \
  --require-rocm \
  --rocm-gfx gfx1151
```

Expected checks:

- `PASS: os_lts_target`
- `PASS: zfs_import_and_health`
- `PASS: docker_data_and_container_health`
- `PASS: backup_timers`
- `PASS: backup_evidence`
- `PASS: rocm_device_access`
- `PASS: runtime_prod_preflight`

`runtime_prod_preflight` is a PASS-by-default reminder unless `--run-runtime-preflight` is supplied. For final production signoff, enforce it:

```bash
python ops/server/os_migration_postflight.py \
  --target-codename resolute \
  --zfs-pool zpool \
  --preflight-report "${PRE_FLIGHT_REPORT}" \
  --run-runtime-preflight
```

Then run the existing production checks:

```bash
python engine/runtime/prod_preflight.py --json
python tools/validate_repo.py --live
```

## Rollback Path

Rollback is valid only if no `zpool upgrade` or irreversible pool feature enablement happened after the snapshot.

If the OS upgrade fails before data services restart:

1. Boot the previous OS entry, rescue media, or a fresh 25.10/24.04 install with compatible ZFS.
2. Import the pool read-only first:

```bash
sudo zpool import -N -o readonly=on zpool
sudo zpool status -v zpool
```

3. If the pool is healthy, export and re-import read/write only when ready to roll back datasets:

```bash
sudo zpool export zpool
sudo zpool import -N zpool
```

4. Roll back trading datasets to the recorded snapshot:

```bash
sudo zfs rollback -r "zpool/trading@${SNAP}"
```

5. Start data services, then runtime/operator, then run postflight against the rollback OS with the matching target codename. For a rollback to 25.10, use postflight only for ZFS/Docker/timer evidence and record NO-GO for LTS completion.

If the upgraded OS boots but trading fails:

1. Stop runtime/operator, then data containers, then timers using the quiesce order above.
2. Preserve failed-state evidence before rollback:

```bash
sudo zfs snapshot -r "zpool/trading@failed-os-2604-$(date -u +%Y%m%dT%H%M%SZ)"
docker ps -a --format '{{json .}}' > /tmp/docker-post-failure.json
journalctl -u 'trading*' --since '2 hours ago' --no-pager > /tmp/trading-post-failure-journal.txt
```

3. Roll back to `${SNAP}` or clone the snapshot to an alternate mount for comparison.
4. Recreate the previous Docker images from the preflight report if local image IDs were pruned.
5. Run `ops/server/os_migration_postflight.py` and `engine/runtime/prod_preflight.py --json` before clearing the execution hold.

Rollback is NO-GO if:

- The ZFS pool was upgraded and the old OS cannot import it.
- The recorded snapshot is missing.
- Docker images or compose files cannot be restored to the preflight versions.
- Backup evidence is stale or unsigned after rollback.
- Broker state cannot be reconciled.

## Final Signoff

Attach these artifacts to the ticket:

- Preflight JSON report.
- ZFS snapshot name and `zpool status -v` output before upgrade.
- Release-upgrader log location and any package/config decisions.
- Postflight JSON report.
- `engine/runtime/prod_preflight.py --json` output.
- Live smoke output, if live validation was run.
- A statement that `zpool upgrade` was not run during the rollback window.

Do not clear the execution hold or enable live trading until all postflight and production preflight gates are PASS.
