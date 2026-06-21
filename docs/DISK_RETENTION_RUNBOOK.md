# Disk Retention Runbook

Use this runbook for the Docker production runtime when the root filesystem or
Docker storage is under pressure.

## Defaults

- Local development logs under `var/log` rotate hourly when launched through
  `start_local.sh`, rotate early at `TRADING_LOCAL_LOGROTATE_MAX_SIZE=50M`,
  keep `TRADING_LOCAL_LOGROTATE_ROTATE=5` compressed rotations, and drop
  rotations older than `TRADING_LOCAL_LOGROTATE_MAXAGE=14` days. Python runtime
  writers also guard append-only local logs with
  `TRADING_LOCAL_LOG_MAX_BYTES=52428800` and
  `TRADING_LOCAL_LOG_BACKUP_COUNT=5` before opening a new append handle.
- Compose service stdout/stderr uses the Docker `local` log driver by default
  with `DOCKER_LOG_MAX_SIZE=50m` and `DOCKER_LOG_MAX_FILE=5`.
- Runtime file logs under `/app/logs`, `/zpool/trading/runtime/logs`,
  `/opt/trading-system/logs`,
  `/opt/trading/app/logs`, boot stderr logs, the diagnostics-only
  operator-AI JSONL log, and the ZFS runtime log bind mount rotate
  daily, rotate early at `maxsize 50M`, keep 10 rotations, delete rotations
  older than 21 days, and compress old logs.
- Production compose must use explicit ZFS bind mounts for high-growth state:
  `TRADING_TIMESCALE_DATA=/zpool/trading/timescaledb/data`,
  `TRADING_REDIS_DATA=/zpool/trading/redis/data`,
  `TRADING_MINIO_DATA=/zpool/trading/minio/data`,
  `TRADING_RUNTIME_DATA=/zpool/trading/runtime/data`,
  `TRADING_RUNTIME_LOGS=/zpool/trading/runtime/logs`, and
  `TRADING_BACKUP_ROOT=/var/backups/trading`.
- TimescaleDB WAL archiving treats `/var/backups/trading` as a required mount,
  not a directory to create opportunistically. If the ZFS dataset is absent or
  unwritable by the container `postgres` UID, `ops/backup/wal_archive.sh` fails
  the archive command loudly instead of writing WAL into Docker/root storage.
- Disk pressure diagnostics warn at `DISK_PRESSURE_WARN_FREE_PCT=15` or
  `DISK_PRESSURE_WARN_FREE_BYTES=21474836480`; they fail critical preflight at
  `DISK_PRESSURE_CRITICAL_FREE_PCT=5` or
  `DISK_PRESSURE_CRITICAL_FREE_BYTES=5368709120`.
- Backup retention defaults are `TS_BACKUP_KEEP_DAILY_DAYS=14`,
  `TS_BACKUP_KEEP_WEEKLY_DAYS=365`, and `TS_BACKUP_WAL_CUSHION_DAYS=7`.

## Local Log Writers Covered

- `engine/runtime/logging.py` writes structured `engine.log` with an internal
  rotating handler.
- `start_system.py` writes supervised ingestion `ingestion.stdout.log` and
  `ingestion.stderr.log`.
- `engine/runtime/ingestion_runtime.py` and `engine/runtime/supervisor.py`
  write child job `*.stdout.log` and `*.stderr.log`.
- `engine/runtime/jobs_manager.py` writes per-job `*.combined.log`; this is the
  source of `ingestion_runtime.combined.log`.
- `start_all.py` writes local operator `operator.stdout.log` and
  `operator.stderr.log`.
- `boot/operator_server.js` writes `runtime.log`, `engine_stderr.log`, and
  `agent_actions.jsonl`; `services/operator_ai/agent.js` writes
  `ai_operator_log.jsonl`.
- `dashboard_server.py` writes `crash_analytics.jsonl`.
- systemd deployments append service stdout/stderr to
  `/opt/trading-system/logs/*.log`; Docker deployments use the Docker `local`
  driver plus `/app/logs` file rotation.

## Inspect First

```bash
cd /home/david/gitsandbox/system/system
du -sh var/log
find var/log -maxdepth 1 -type f -printf '%s %p\n' \
  | sort -nr \
  | numfmt --field=1 --to=iec
./deploy/bin/rotate_local_logs.sh --check

docker system df
docker builder du
sudo /opt/trading/ops/backup/accounting.sh
docker compose --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  exec runtime python engine/runtime/prod_preflight.py --json
```

`ops/backup/accounting.sh` and production preflight report the host backup path,
the container mount destination/source when available, apparent and allocated
backup bytes, subdirectory sizes, current filesystem headroom, and configured
backup retention status.

`prod_preflight.py` also reports `storage_placement` and `disk_pressure`.
Production is not ready if any critical state path resolves to
`/var/lib/docker`, `/var/lib/containerd`, a non-ZFS visible mount, or a host
path outside `/zpool` and `/var/backups/trading`.

## Tune ZFS Pool And PGDATA Dataset

Run T2.5 before or during the T1.3c Docker data-root move. The automation is
idempotent and captures before/after state under
`TRADING_ZFS_CAPTURE_DIR` (default `/var/tmp/trading-zfs-tuning`):

```bash
cd /home/david/gitsandbox/system/system
sudo bash ops/server/zfs_tuning.sh apply --dry-run
sudo bash ops/server/zfs_tuning.sh apply
bash ops/server/zfs_tuning.sh verify
```

The apply action enforces the pool and general dataset policy:

- `zpool set autotrim=on zpool` for the NVMe-backed pool.
- `atime=off` on every existing dataset under `zpool`, with the root dataset
  set so future children inherit the policy unless deliberately overridden.
- `compression=lz4` on `zpool/data`, replacing the old `gzip-4` default with a
  low-CPU compression policy.
- If the dedicated PGDATA dataset already exists, the same PGDATA properties
  listed below are applied in place.

The verifier is read-only and uses `zdb -C zpool` for actual on-disk ashift
because `zpool get ashift` can report default `0`. It fails unless every vdev
reports `ashift=12`; ashift is immutable on existing vdevs, so a mismatch means
a maintenance-window migration to a newly created `ashift=12` pool, followed by
Docker/backups restore evidence, not an in-place repair. The script never
destroys or recreates the pool.

T1.3c consumes the PGDATA dataset spec through `/home/david/gitsandbox/disk-remediation.sh`
when it creates `zpool/docker/timescaledb-pgdata`:

| Property | Value | Reason |
| --- | --- | --- |
| `recordsize` | `16K` | Keeps ZFS records close to Postgres 8K page IO while avoiding very small-record metadata overhead. |
| `logbias` | `throughput` | Favors aggregate write throughput for database/WAL-heavy workloads on NVMe. |
| `compression` | `lz4` | Low-CPU compression for database pages and WAL without the gzip-4 CPU cost. |
| `atime` | `off` | Avoids write amplification from reads. |
| `primarycache` | `metadata` | Avoids duplicating Postgres table/index pages in both `shared_buffers` and ZFS ARC. |

With `primarycache=metadata`, keep `TIMESCALE_SHARED_BUFFERS` explicit
(`8GB` in the committed 123 GiB host profile) and do not count PGDATA ARC data
caching when choosing `TIMESCALE_EFFECTIVE_CACHE_SIZE`. T1.4 caps ARC at
48 GiB for this host class; under the metadata-only PGDATA policy that ARC is
reserved for ZFS metadata and non-PGDATA datasets instead of becoming a second
copy of Postgres shared buffers.

## Move Existing Docker State To ZFS

Use this sequence for a live host that still has named Docker volumes on root
ext4. Do not prune or delete Docker volumes until the restore drill has passed.

### Preferred: relocate Docker data-root

For host `bart`, where the live `compose_timescaledb-data` volume still resides
under `/var/lib/docker` on the root ext4 filesystem, use the sibling remediation
tool. The command moves Docker's whole data-root to the dedicated
`zpool/docker` dataset, creates a tuned child dataset for Timescale PGDATA, and
keeps a rollback copy before root space is reclaimed.

```bash
sudo bash /home/david/gitsandbox/disk-remediation.sh relocate-docker --dry-run
sudo bash /home/david/gitsandbox/disk-remediation.sh relocate-docker
```

The command enforces these gates in production code:

- Refuses to run while backup, prune, restore-drill, or backup-evidence services
  are active.
- Stops backup timers, the Compose stack, Docker, and the Docker socket before
  copying.
- Requires enough ZFS free space for both the new Docker data-root and a
  rollback archive.
- Creates `zpool/docker` with container-safe ZFS properties
  (`compression=zstd`, `atime=off`, `xattr=sa`, `acltype=posixacl`,
  `dnodesize=auto`, `recordsize=128K`, `logbias=latency`).
- Creates `zpool/docker/timescaledb-pgdata` mounted at the Timescale volume's
  `_data` directory with the T2.5 PGDATA tuning:
  `recordsize=16K`, `logbias=throughput`, `compression=lz4`, `atime=off`, and
  `primarycache=metadata`. `pg_wal` remains under that tuned PGDATA dataset
  unless a later change splits it explicitly.
- Merges `"data-root": "/zpool/docker"` into `/etc/docker/daemon.json` while
  preserving existing settings, and backs up the previous file.
- Copies with `rsync -aHAX --numeric-ids --info=progress2`.
- Restarts Docker and Compose, then waits for all Compose containers to be
  running and healthy or to have no healthcheck.
- Verifies Docker reports `/zpool/docker`, verifies the Timescale volume
  mountpoint is on ZFS, and verifies `pg_wal` exists under PGDATA.
- After those checks pass, archives the old root copy to
  `/zpool/docker-rollback/var-lib-docker.<timestamp>`, removes the root copy,
  recreates an empty `/var/lib/docker`, and asserts root free space increased.

Rollback is guarded by the state directory written during relocation:

```bash
sudo bash /home/david/gitsandbox/disk-remediation.sh relocate-docker --rollback
```

Rollback stops the stack and Docker, restores the archived Docker tree to the
previous data-root, restores the prior `daemon.json` or removes it if it did not
exist, restarts Docker and Compose, and reruns container health checks. Do not
delete `/zpool/docker-rollback/var-lib-docker.<timestamp>` until the maintenance
window, postflight preflight, and restore-drill evidence have passed. The
relocation command prints the exact `sudo rm -rf ...` cleanup command for that
rollback archive.

### Manual bind-mount migration fallback

1. Stop writers and take recovery evidence:

```bash
docker compose --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml down

sudo /opt/trading/ops/backup/base_backup.sh
sudo /opt/trading/ops/backup/backup_restore_evidence.sh
sudo cp -a /var/backups/trading/evidence/latest_backup_restore_evidence.json \
  "/var/backups/trading/evidence/pre-zfs-migration.$(date -u +%Y%m%dT%H%M%SZ).json"
```

2. Create ZFS destinations and copy with ownership, modes, xattrs, and hard
links preserved. Confirm exact volume names with `docker volume ls` first:

```bash
sudo install -d -m 0750 /zpool/trading/timescaledb/data /zpool/trading/redis/data /zpool/trading/minio/data
sudo install -d -m 0750 /zpool/trading/runtime/data /zpool/trading/runtime/logs
sudo rsync -aHAX --numeric-ids --info=progress2 /var/lib/docker/volumes/system_timescaledb-data/_data/ /zpool/trading/timescaledb/data/
sudo rsync -aHAX --numeric-ids --info=progress2 /var/lib/docker/volumes/system_redis-data/_data/ /zpool/trading/redis/data/
sudo rsync -aHAX --numeric-ids --info=progress2 /var/lib/docker/volumes/system_minio-data/_data/ /zpool/trading/minio/data/
sudo rsync -aHAX --numeric-ids --info=progress2 /var/lib/docker/volumes/system_trading-data/_data/ /zpool/trading/runtime/data/
sudo rsync -aHAX --numeric-ids --info=progress2 /var/lib/docker/volumes/system_trading-logs/_data/ /zpool/trading/runtime/logs/
sudo rsync -aHAXn --checksum --numeric-ids --delete /var/lib/docker/volumes/system_timescaledb-data/_data/ /zpool/trading/timescaledb/data/
sudo rsync -aHAXn --checksum --numeric-ids --delete /var/lib/docker/volumes/system_redis-data/_data/ /zpool/trading/redis/data/
sudo rsync -aHAXn --checksum --numeric-ids --delete /var/lib/docker/volumes/system_minio-data/_data/ /zpool/trading/minio/data/
sudo rsync -aHAXn --checksum --numeric-ids --delete /var/lib/docker/volumes/system_trading-data/_data/ /zpool/trading/runtime/data/
sudo rsync -aHAXn --checksum --numeric-ids --delete /var/lib/docker/volumes/system_trading-logs/_data/ /zpool/trading/runtime/logs/
```

3. Verify before switching mounts:

```bash
sudo find /zpool/trading -xdev -type f -printf '%P\0' | sort -z | sudo xargs -0 sha256sum > /tmp/zpool-trading.sha256
sudo find /zpool/trading/timescaledb/data -maxdepth 2 -printf '%u:%g %m %p\n' | head -80
sudo du -sh /zpool/trading/timescaledb/data /zpool/trading/redis/data /zpool/trading/minio/data /zpool/trading/runtime/data /zpool/trading/runtime/logs
```

4. Update `deploy/compose/.env` to the ZFS paths from `.env.example`, then
start and verify:

```bash
docker compose --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml up -d
docker compose --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml exec runtime python engine/runtime/prod_preflight.py --json
sudo /opt/trading/ops/backup/restore_drill.sh
sudo /opt/trading/ops/backup/backup_restore_evidence.sh
sudo docker exec -u postgres trading-timescaledb /opt/trading/ops/backup/wal_archive_catchup.sh
```

Expected result: `storage_placement.ok=true`, disk pressure is not critical for
`root`, `zfs_pool`, `backup_wal`, or Docker volume roots, and signed backup
evidence plus restore-drill evidence is fresh. Only then may old Docker named
volumes be archived or removed under a separate change ticket.

## Safe Cleanup

For local logs, prefer rotation or archive-and-truncate. These commands do not
delete the active log content without first preserving an archive:

```bash
cd /home/david/gitsandbox/system/system

# Rotate all local logs now using the repository logrotate policy.
./deploy/bin/rotate_local_logs.sh --force

# Archive one oversized active log, then truncate it in place for active writers.
mkdir -p var/log/archive
gzip -c var/log/ingestion_runtime.combined.log \
  > "var/log/archive/ingestion_runtime.combined.log.$(date -u +%Y%m%dT%H%M%SZ).gz"
truncate -s 0 var/log/ingestion_runtime.combined.log

# Inspect rotated archives before removing anything.
find var/log -maxdepth 1 -type f \( -name '*.gz' -o -name '*.log.[0-9]*' \) \
  -printf '%TY-%Tm-%Td %TH:%TM %s %p\n' | sort
```

If a service is actively writing and exact preservation matters, stop the local
runtime before the archive/truncate step:

```bash
pkill -f '/home/david/gitsandbox/system/system/start_system.py' || true
pkill -f '/home/david/gitsandbox/system/system/boot/operator_server.js' || true
```

These commands remove rebuildable Docker cache or stopped containers only:

```bash
docker builder prune --filter until=168h
docker image prune -a --filter until=168h
docker container prune --filter until=168h
```

Use `docker system prune` only without `--volumes`, and only after reviewing
`docker system df`.

## Do Not Delete

Do not run these commands on a live production host:

```bash
docker volume prune
docker system prune --volumes
rm -rf /var/lib/docker/volumes/*timescaledb*
rm -rf /var/lib/docker/volumes/*redis*
rm -rf /var/lib/docker/volumes/*minio*
rm -rf /var/lib/docker/volumes/*trading-data*
rm -rf /var/lib/docker/volumes/*trading-logs*
rm -rf /var/backups/trading
```

The Timescale, Redis, MinIO, app data, and backup volumes contain live state or
recovery evidence. If backup storage must be reduced, use:

```bash
sudo /opt/trading/ops/backup/prune.sh
sudo /opt/trading/ops/backup/accounting.sh
```

## Evidence Preservation

Before any cleanup, preserve the latest backup evidence JSON and text report,
the latest base backup, WAL files required by the configured retention policy,
and the latest restore-drill report. Do not remove the backup evidence signing
key until newly signed evidence has been generated and production preflight
passes with the replacement key.
