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
- Runtime file logs under `/app/logs`, `/auxpool/trading/runtime/logs`,
  `/var/lib/trading/logs`,
  `/opt/trading/app/logs`, boot stderr logs, the diagnostics-only
  operator-AI JSONL log, and the ZFS runtime log bind mount rotate
  daily, rotate early at `maxsize 50M`, keep 10 rotations, delete rotations
  older than 21 days, and compress old logs.
- Production compose must use explicit ZFS bind mounts for high-growth state:
  `TRADING_TIMESCALE_DATA=/dbpool/trading/timescaledb/data`,
  `TRADING_REDIS_DATA=/auxpool/trading/redis`,
  `TRADING_MINIO_DATA=/auxpool/trading/minio`,
  `TRADING_RUNTIME_DATA=/auxpool/trading/runtime/data`,
  `TRADING_RUNTIME_LOGS=/auxpool/trading/runtime/logs`, and
  `TRADING_BACKUP_ROOT=/var/backups/trading`.
- `docker-compose.external-services.yml` runs `storage-placement-preflight`
  before TimescaleDB. That gate uses `engine.runtime.storage_placement` and
  exits non-zero if PGDATA, `pg_wal`, Redis, MinIO, runtime state, or backups
  are under `/var/lib/docker`/`/var/lib/containerd`, off the approved prefixes,
  root-backed, or visible on a non-ZFS mount.
- TimescaleDB WAL archiving treats `/var/backups/trading` as a required mount,
  not a directory to create opportunistically. If the ZFS dataset is absent or
  unwritable by the container `postgres` UID, `ops/backup/wal_archive.sh` fails
  the archive command loudly instead of writing WAL into Docker/root storage.
  The recurring backup-evidence gate reasserts the archive target directories to
  the configured container `postgres` UID, `trading` group, and `2750` mode and
  records a signed `wal_archive_target` diagnosis artifact when it repairs a
  wrong-owner or wrong-mode condition. That diagnosis includes the pre-repair
  `wal_archive.sh` probe status, failure event/exit code when the drift blocks
  archiving, `pg_stat_archiver` failure fields, and the applied ownership/mode
  fix.
- Disk pressure diagnostics warn at `DISK_PRESSURE_WARN_FREE_PCT=15` or
  `DISK_PRESSURE_WARN_FREE_BYTES=21474836480`; they fail critical preflight at
  `DISK_PRESSURE_CRITICAL_FREE_PCT=5` or
  `DISK_PRESSURE_CRITICAL_FREE_BYTES=5368709120`.
- Backup retention defaults are local-only PITR retention for a database that
  may exceed 1.5 TB: `TS_BACKUP_KEEP_RECENT_COUNT=2`,
  `TS_BACKUP_KEEP_DAILY_DAYS=0`, `TS_BACKUP_KEEP_WEEKLY_DAYS=0`, and
  `TS_BACKUP_WAL_CUSHION_DAYS=10`. A year of local weekly full backups does
  not fit on the 2.9 TiB Crucial backup pool; archival base backups must be
  pushed off host through `TS_BASE_BACKUP_OFFSITE_CMD`.

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
  `/var/lib/trading/logs/*.log`; Docker deployments use the Docker `local`
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
path outside `/zpool`, `/dbpool`, `/auxpool`, and `/var/backups/trading`.

## Provision 3-NVMe Storage Pools

Host-run only. The repo-tracked entry point is
`ops/server/provision_storage_pools.sh`. It is idempotent, defaults to dry-run,
uses the confirmed `/dev/disk/by-id/...` selectors, and captures state under
`TRADING_STORAGE_CAPTURE_DIR` (default
`/var/tmp/trading-storage-provision`).

The target layout is:

| Pool | Device | Role |
| --- | --- | --- |
| `dbpool` | Samsung 990 EVO Plus 4TB, `/dev/disk/by-id/nvme-Samsung_SSD_990_EVO_Plus_4TB_S7U8NU0YA01981P` | Timescale PGDATA and `pg_wal` at `/dbpool/trading/timescaledb/data` |
| `zpool` | Existing Crucial pool on the boot drive | Backups at `/var/backups/trading`; keep `zpool/trading-backups compression=zstd` |
| `auxpool` | Kingston OM8TAP4 2TB, `/dev/disk/by-id/nvme-KINGSTON_OM8TAP42048K1-A00_50026B73842ACAC7` | Redis, MinIO, runtime data/logs, artifact caches, training/offline scratch |

First inspect the dry-run:

```bash
cd /home/david/gitsandbox/system/system
bash ops/server/provision_storage_pools.sh spec
sudo bash ops/server/provision_storage_pools.sh apply --dry-run
```

Apply mode has explicit destructive gates. The Samsung wipe requires
`CONFIRM_WIPE_SAMSUNG=nvme2n1`. The Kingston Windows/BitLocker reclaim is
delegated to `reclaim_idle_nvme.sh`; it requires `IDLE_NVME_DECISION=RECLAIM`,
`TARGET_DISK_BY_ID`, `CONFIRM_DESTROY=nvme0n1`, `RECLAIM_DRY_RUN=0`, fresh
idle-NVMe assessment evidence, and fresh backup/restore evidence. Do not set
those variables unless Windows reclaim is intentional.

```bash
sudo CONFIRM_WIPE_SAMSUNG=nvme2n1 \
  IDLE_NVME_DECISION=RECLAIM \
  TARGET_DISK_BY_ID=/dev/disk/by-id/nvme-KINGSTON_OM8TAP42048K1-A00_50026B73842ACAC7 \
  CONFIRM_DESTROY=nvme0n1 \
  RECLAIM_DRY_RUN=0 \
  bash ops/server/provision_storage_pools.sh apply --no-dry-run
```

The provisioner never destroys `zpool`. It only sets `zpool autotrim=on`,
sets `atime=off` on existing `zpool` datasets, and refuses to proceed if
`zpool/trading-backups` is not still `compression=zstd`.

Verify after apply:

```bash
bash ops/server/provision_storage_pools.sh verify
TRADING_ZFS_POOL=dbpool \
  TRADING_ZFS_DATA_DATASET=dbpool/data \
  TRADING_ZFS_PGDATA_DATASET=dbpool/trading/timescaledb/data \
  bash ops/server/zfs_tuning.sh verify
```

Both verifiers are read-only. They use `zdb -C` for actual on-disk `ashift`;
`zpool get ashift` can report default `0` and is not sufficient.

The PGDATA dataset spec is:

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

## Move Backup Root To ZFS

Use the installed remediation tool when `/var/backups/trading` itself still
lives on root storage:

```bash
sudo bash /opt/trading/ops/server/disk_remediation.sh relocate-backups
```

The command stages a `zpool/trading-backups` dataset, copies with
`rsync -aHAX --numeric-ids --info=progress2`, then normalizes the relocated WAL
archive target before the dataset is mounted back at `/var/backups/trading`.
The normalized target is `TS_BACKUP_POSTGRES_UID` (default `70`) and
`TS_BACKUP_GROUP` (default `trading`) with `2750` on `/var/backups/trading/wal`
and `.tmp`. This step is required because `rsync --numeric-ids` intentionally
preserves any pre-existing wrong owner or mode from the source tree.

After relocation, run the evidence gate and inspect the signed target artifact:

```bash
sudo /opt/trading/ops/backup/backup_restore_evidence.sh
jq '{status,wal_archive_target,wal_archiver}' \
  /var/backups/trading/evidence/latest_backup_restore_evidence.json
```

Run `wal_archive_catchup.sh` only after the target and dataset are healthy and
the operator has confirmed there is enough backup-dataset headroom for the
`.ready` backlog:

```bash
sudo docker exec -u postgres trading-timescaledb \
  /opt/trading/ops/backup/wal_archive_catchup.sh
```

## Migrate PGDATA To dbpool

Host-run only. Do not migrate PGDATA while TimescaleDB is running. The compose
target remains an explicit bind mount, not a Docker named volume:
`TRADING_TIMESCALE_DATA=/dbpool/trading/timescaledb/data`.

### Fresh initdb path

Use this when there is no production data to preserve:

```bash
docker compose --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml down

sudo install -d -m 0700 /dbpool/trading/timescaledb/data
sudo chown -R 70:70 /dbpool/trading/timescaledb/data

docker compose --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml up -d timescaledb
```

After the first container boot, verify that `pg_wal` exists under
`/dbpool/trading/timescaledb/data` and run production preflight.

### Existing data: backup/restore path

Use this when the database already contains state:

```bash
docker compose --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml down

sudo /opt/trading/ops/backup/base_backup.sh
sudo /opt/trading/ops/backup/backup_restore_evidence.sh
sudo cp -a /var/backups/trading/evidence/latest_backup_restore_evidence.json \
  "/var/backups/trading/evidence/pre-zfs-migration.$(date -u +%Y%m%dT%H%M%SZ).json"
```

Restore the latest verified base backup into the dbpool dataset:

```bash
sudo /opt/trading/ops/backup/restore.sh \
  --target-time latest \
  --into /dbpool/trading/timescaledb/data \
  --force
sudo chown -R 70:70 /dbpool/trading/timescaledb/data
sudo chmod 0700 /dbpool/trading/timescaledb/data
```

If the current source is a cleanly stopped PGDATA directory and backup/restore
has already been proven, a direct copy is also acceptable:

```bash
sudo rsync -aHX --numeric-ids --info=progress2 \
  /path/to/stopped/old-pgdata/ \
  /dbpool/trading/timescaledb/data/
sudo rsync -aHXn --checksum --numeric-ids --delete \
  /path/to/stopped/old-pgdata/ \
  /dbpool/trading/timescaledb/data/
sudo chown -R 70:70 /dbpool/trading/timescaledb/data
sudo chmod 0700 /dbpool/trading/timescaledb/data
```

Then start and verify:

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
`root`, `zfs_pool`, `backup_wal`, or Docker volume roots, `pg_wal` exists under
dbpool PGDATA, and signed backup evidence plus restore-drill evidence is fresh.
Only then may old Docker named volumes be archived or removed under a separate
change ticket.

## Migrate Aux State To auxpool

Redis, MinIO, runtime data/logs, artifact mirrors, and training/offline caches
are rebuildable or less critical than PGDATA, but they must still leave
`/var/lib/docker` before production bring-up. If old Docker named volumes
exist, stop the stack and copy with ownership, modes, xattrs, and hard links
preserved. Confirm exact volume names with `docker volume ls` first.

```bash
sudo rsync -aHAX --numeric-ids --info=progress2 /var/lib/docker/volumes/system_redis-data/_data/ /auxpool/trading/redis/
sudo rsync -aHAX --numeric-ids --info=progress2 /var/lib/docker/volumes/system_minio-data/_data/ /auxpool/trading/minio/
sudo rsync -aHAX --numeric-ids --info=progress2 /var/lib/docker/volumes/system_trading-data/_data/ /auxpool/trading/runtime/data/
sudo rsync -aHAX --numeric-ids --info=progress2 /var/lib/docker/volumes/system_trading-logs/_data/ /auxpool/trading/runtime/logs/
sudo rsync -aHAXn --checksum --numeric-ids --delete /var/lib/docker/volumes/system_redis-data/_data/ /auxpool/trading/redis/
sudo rsync -aHAXn --checksum --numeric-ids --delete /var/lib/docker/volumes/system_minio-data/_data/ /auxpool/trading/minio/
sudo rsync -aHAXn --checksum --numeric-ids --delete /var/lib/docker/volumes/system_trading-data/_data/ /auxpool/trading/runtime/data/
sudo rsync -aHAXn --checksum --numeric-ids --delete /var/lib/docker/volumes/system_trading-logs/_data/ /auxpool/trading/runtime/logs/
```

Use the compose `.env` paths already committed for `/auxpool/trading/...`.
Production preflight must show verified ZFS mounts, not prefix-only evidence.

## Offsite Base Backup Requirement

Local retention is intentionally short: two base backups plus the WAL cushion.
Configure an off-host archival copy before relying on local pruning:

```bash
TS_OFFSITE_BACKUP_DEST=/mnt/backup-nas/trading-base \
TS_BASE_BACKUP_OFFSITE_CMD='bash /opt/trading/ops/backup/offsite_base_backup_stub.sh' \
sudo -E /opt/trading/ops/backup/base_backup.sh
```

For S3-compatible destinations, install/configure the AWS CLI for the service
user and set `TS_OFFSITE_BACKUP_DEST=s3://bucket/prefix`. WAL archiving remains
fail-closed through `archive_mode=on`, `TS_WAL_ARCHIVE_REQUIRE_MOUNT=1`, and
`ops/backup/wal_archive.sh`.

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
