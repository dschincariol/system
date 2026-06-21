# Compose Stack

Use these compose assets when you want a containerized staging or production-like deployment path for the current repo architecture.

## Files

- `docker-compose.external-services.yml`
  Brings up Timescale/Postgres, Redis, and MinIO-style object storage.
- `docker-compose.stack.yml`
  Brings up the Python runtime and the Node operator sidecar on top of the external dependency network.
- `.env.example`
  Seed env file for both compose files.
- `Dockerfile.runtime`
  Runtime image for `start_system.py`.
- `Dockerfile.operator`
  Operator image for `boot/operator_server.js`.

## Bring Up

1. Copy `.env.example` to `.env` in this directory.
2. Set approved image tags, non-secret runtime values, and secret file paths. `deploy/compose/.env` must point to files such as `TIMESCALE_PASSWORD_FILE`, `REDIS_PASSWORD_FILE`, `MINIO_ROOT_USER_FILE`, `MINIO_ROOT_PASSWORD_FILE`, `DASHBOARD_API_TOKEN_FILE`, `OPERATOR_API_TOKEN_FILE`, `DATA_SOURCE_MASTER_KEY_FILE`, `BACKUP_EVIDENCE_HMAC_KEY_FILE`, and optional provider/broker files such as `POLYGON_API_KEY_FILE`, `TRADIER_API_TOKEN_FILE`, `ALPACA_KEY_ID_FILE`, and `ALPACA_SECRET_KEY_FILE`. Do not paste live secret values into the compose `.env`. Keep `TRADING_DATA_ROOT=/app/data`; it is the container path, while `TRADING_RUNTIME_DATA` is the ZFS host source mounted there. Dashboard and operator token files must contain generated secrets, not placeholders. The operator sidecar is internal-only by default and does not publish port 4001. Keep host publish binds on loopback: `TIMESCALE_DANGEROUS_PUBLIC_BIND_HOST=127.0.0.1`, `REDIS_DANGEROUS_PUBLIC_BIND_HOST=127.0.0.1`, `MINIO_DANGEROUS_PUBLIC_BIND_HOST=127.0.0.1`, `MINIO_CONSOLE_DANGEROUS_PUBLIC_BIND_HOST=127.0.0.1`, and `DASHBOARD_DANGEROUS_PUBLIC_BIND_HOST=127.0.0.1`. Keep `DOCKER_LOG_DRIVER=local`, `DOCKER_LOG_MAX_SIZE=50m`, and `DOCKER_LOG_MAX_FILE=5` unless the target host has a reviewed reason to change them; these cap Docker stdout/stderr while file logs under `/app/logs` are handled by host logrotate.
   Create the backup evidence HMAC key before `docker compose up` because the runtime mounts it as a Compose secret:

```bash
sudo groupadd --system trading 2>/dev/null || true
sudo install -d -o root -g trading -m 0750 /etc/trading
openssl rand -hex 32 | sudo tee /etc/trading/backup_evidence.hmac.key >/dev/null
sudo chown root:trading /etc/trading/backup_evidence.hmac.key
sudo chmod 0640 /etc/trading/backup_evidence.hmac.key
```
3. Create the secret files referenced by `.env`. Keep these files outside the repository checkout and use owner `root:trading` or the service user with mode `0600`:

```bash
sudo install -d -o root -g trading -m 0750 /etc/trading/secrets
openssl rand -base64 32 | sudo tee /etc/trading/secrets/timescale_password >/dev/null
openssl rand -base64 32 | sudo tee /etc/trading/secrets/redis_password >/dev/null
openssl rand -hex 24 | sudo tee /etc/trading/secrets/dashboard_api_token >/dev/null
openssl rand -hex 24 | sudo tee /etc/trading/secrets/operator_api_token >/dev/null
sudo chown root:trading /etc/trading/secrets/*
sudo chmod 0600 /etc/trading/secrets/*
```

Create MinIO, provider, and broker files the same way, using values generated or issued by the backing service/provider. Rotate any value that was previously stored in a repo-local `.env`.

4. Create the data-source master-key file referenced by `DATA_SOURCE_MASTER_KEY_FILE`:

```bash
install -m 0600 /dev/null ../../data/.data_source_master_key
openssl rand -base64 32 > ../../data/.data_source_master_key
```

Production/live preflight rejects raw text, placeholders, short or low-entropy values, malformed base64, and empty key files.
5. Create the ZFS-backed host directories before first start. The production compose path uses explicit bind mounts rather than Docker named volumes:

```bash
sudo install -d -m 0750 /zpool/trading/timescaledb/data
sudo install -d -m 0750 /zpool/trading/redis/data
sudo install -d -m 0750 /zpool/trading/minio/data
sudo install -d -o "$(id -u)" -g "$(id -g)" -m 0750 /zpool/trading/runtime/data /zpool/trading/runtime/logs
sudo install -d -o "$(id -u)" -g "$(id -g)" -m 0750 /zpool/trading/runtime/artifact_mirror /zpool/trading/runtime/training_datasets
sudo install -d -m 0750 /var/backups/trading/wal /var/backups/trading/evidence
```

Confirm `.env` keeps `PREFLIGHT_REQUIRE_ZFS_STORAGE=1`, `PREFLIGHT_STORAGE_REQUIRE_VISIBLE_HOST_PATHS=1`, `TRADING_ALLOWED_STORAGE_FS_TYPES=zfs`, and every `TRADING_*_DATA`/`TRADING_*_LOGS` source under `/zpool` or `/var/backups/trading`. Production preflight must report verified ZFS mounts, not `approved_prefix_unverified` prefix-only evidence.
6. Build and start the stack:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  up -d --build
```

Default host exposure should look like this:

```bash
docker ps --format 'table {{.Names}}\t{{.Ports}}' | grep '^trading-'
```

```text
trading-timescaledb   127.0.0.1:5432->5432/tcp
trading-redis         127.0.0.1:6379->6379/tcp
trading-minio         127.0.0.1:9000->9000/tcp, 127.0.0.1:9001->9001/tcp
trading-runtime       127.0.0.1:8000->8000/tcp
trading-operator      4001/tcp
```

If any service shows `0.0.0.0:` or `:::`, stop and fix `.env` before live mode. A reviewed LAN exposure must use VPN or a TLS/authenticated reverse proxy for dashboard access, firewall rules that restrict data services to named management hosts, the service-specific `*_ALLOW_DANGEROUS_PUBLIC_BIND=1` flag, and `TRADING_PUBLIC_NETWORK_EXPOSURE_ACK=I_UNDERSTAND_THIS_EXPOSES_TRADING_SERVICES` with non-placeholder owner and reason fields. Production preflight fails without that acknowledgement.

The external TimescaleDB service archives WAL to
`${TRADING_BACKUP_WAL_DIR:-/var/backups/trading/wal}` by invoking the audited
`ops/backup/wal_archive.sh` inside the container:
`/opt/trading/ops/backup/wal_archive.sh "%p" "%f"`. The script writes through
`${TRADING_WAL_ARCHIVE_SCRIPT:-../../ops/backup/wal_archive.sh}` as a read-only
bind mount and keeps the WAL archive outside Docker's root-backed volume. The
Timescale container also mounts
`${TRADING_WAL_ARCHIVE_CATCHUP_SCRIPT:-../../ops/backup/wal_archive_catchup.sh}`
read-only and sets `TS_WAL_ARCHIVE_REQUIRE_MOUNT=1`, so a missing
`/var/backups/trading` bind mount or an unwritable `70:trading` backup dataset
fails loudly instead of creating WAL files in the container root filesystem.
The archive script does not require `python3`; it uses `sync -f`/`sync` when
the Timescale image lacks Python.

On a production host, install the backup evidence gate after the stack env is
populated:

```bash
sudo bash ops/server/install_backup_evidence_gate.sh --compose --restart-postgres --run-evidence
```

That command creates the host backup layout, installs the backup and evidence
systemd timers, restarts/recreates the TimescaleDB service to apply the WAL
archive script bind mount/settings, runs an in-container archive self-test,
runs a one-shot catch-up for any `.ready` WAL backlog without staging data on
root, and writes the first timestamped backup/WAL/restore evidence report. It
reuses the HMAC key at
`BACKUP_EVIDENCE_HMAC_KEY_FILE`, sets `BACKUP_EVIDENCE_REQUIRE_SIGNATURE=1`,
and the runtime verifies `latest_backup_restore_evidence.json` with
`/run/secrets/backup_evidence_hmac_key`.

Verify the running archiver configuration without mutating the live database:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  exec timescaledb sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
    -X -v ON_ERROR_STOP=1 \
    -c "SHOW archive_mode;" \
    -c "SHOW archive_command;" \
    -c "SELECT archived_count,last_archived_wal,last_archived_time,failed_count,last_failed_wal,last_failed_time FROM pg_stat_archiver;"'
```

Expected result: `archive_mode` is `on`, `archive_command` names
`wal_archive.sh "%p" "%f"`, `last_archived_time` is within
`BACKUP_EVIDENCE_WAL_RPO_S`, and `last_failed_time` is empty or older than the
last archived segment.

Generate signed restore proof after every install, key rotation, or backup
policy change:

```bash
sudo bash ops/server/install_backup_evidence_gate.sh --compose --restart-postgres --run-evidence
jq '{status,timeouts,base_backup,wal_catchup,wal_archive,wal_archiver,restore_drill,signature:{status,key_id,signed_at,payload_sha256}}' \
  /var/backups/trading/evidence/latest_backup_restore_evidence.json
```

Expected result: all component statuses are `pass` and the signature status is
`signed`. Inspect these fields before live promotion: `base_backup.backup_dir`,
`base_backup.verify_log`, `wal_archive.wal_file`,
`wal_archiver.source=pg_stat_archiver`, `wal_archiver.archive_mode=on`, the
audited `wal_archiver.archive_command`, `wal_archiver.archived_count`,
`wal_archiver.last_archived_wal`, `restore_drill.report`,
`restore_drill.time_to_recover_s`, `signature.signed_at`, and
`signature.payload_sha256`. No `wal_archiver.last_failed_at` may be newer than
`wal_archiver.last_archived_at`. The `timeouts` object must contain bounded
values for `base_backup_s`, `wal_switch_s`, `wal_archiver_stats_s`,
`wal_catchup_s`, `restore_drill_s`, `signature_s`, and `publish_s`; the
installer writes the matching `TS_BACKUP_EVIDENCE_*_TIMEOUT_S` defaults to
`/etc/trading/trading.env`.
In compose mode the 60-second evidence timer also runs
`wal_archive_catchup.sh` before forcing `pg_switch_wal()`, so a future
archiver stall is retried and then surfaced through failed signed evidence if
the backlog cannot be archived.

7. Run the production preflight against the runtime container:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  exec runtime python engine/runtime/prod_preflight.py --json
```

To rotate the evidence key, create a replacement file with the same owner/mode,
update `BACKUP_EVIDENCE_HMAC_KEY_FILE` in `deploy/compose/.env`, restart the
runtime so the secret is remounted, then run
`sudo bash ops/server/install_backup_evidence_gate.sh --compose --run-evidence`
and rerun the production preflight. Do not remove the old key until a new signed
evidence JSON has been generated and preflight passes.

8. Run the live smoke against the exposed stack from the repo root:

```bash
python tools/validate_repo.py --live
```

For a local compose stack, live smoke reaches the operator through the dashboard bridge. Export the same auth and base URLs before running live smoke:

```bash
export PIPELINE_SMOKE_BASE="http://127.0.0.1:${DASHBOARD_PUBLIC_PORT:-8000}"
export PIPELINE_SMOKE_OPERATOR_BASE="${PIPELINE_SMOKE_BASE}/operator"
export DASHBOARD_API_TOKEN="$(tr -d '\r\n' < "${DASHBOARD_API_TOKEN_FILE}")"
export OPERATOR_API_TOKEN="$(tr -d '\r\n' < "${OPERATOR_API_TOKEN_FILE}")"
python tools/validate_repo.py --live
```

The optional soak probes (`tools/safe_mode_soak.py`, `tools/runtime_stability_probe.py`, and `tools/market_session_soak.py`) use the same `PIPELINE_SMOKE_BASE`, `PIPELINE_SMOKE_OPERATOR_BASE`, and token variables by default, so compose checks continue through the dashboard bridge unless you explicitly pass a direct sidecar URL for a local diagnostic.

## ZFS Storage Layout

This deployment intentionally uses explicit bind mounts instead of Docker named
volumes for high-growth state. The selected production layout is:

| State | Container path | Host source |
| --- | --- | --- |
| Timescale PGDATA and `pg_wal` | `/var/lib/postgresql/data` | `TRADING_TIMESCALE_DATA=/zpool/trading/timescaledb/data` |
| Redis appendonly/RDB data | `/data` | `TRADING_REDIS_DATA=/zpool/trading/redis/data` |
| MinIO object data | `/data` | `TRADING_MINIO_DATA=/zpool/trading/minio/data` |
| Runtime data | `/app/data` | `TRADING_RUNTIME_DATA=/zpool/trading/runtime/data` |
| Runtime/operator logs | `/app/logs` | `TRADING_RUNTIME_LOGS=/zpool/trading/runtime/logs` |
| Artifact mirror/cache | `/app/artifact_mirror` | `TRADING_ARTIFACT_MIRROR=/zpool/trading/runtime/artifact_mirror` |
| Training dataset cache | `/app/training_datasets` | `TRADING_TRAINING_DATASETS=/zpool/trading/runtime/training_datasets` |
| Backups, WAL archive, evidence | `/var/backups/trading` | `TRADING_BACKUP_ROOT=/var/backups/trading` |

`engine/runtime/prod_preflight.py` enforces this through
`engine.runtime.storage_placement`: production-like runs require explicit host
paths, reject `/var/lib/docker` and `/var/lib/containerd`, and require visible
non-root mounts to be on `zfs` and under the approved storage prefixes. Keep
`PREFLIGHT_STORAGE_REQUIRE_VISIBLE_HOST_PATHS=1` in compose so a target cannot
pass from path-prefix-only evidence. Disk-pressure preflight also covers `/`,
`/zpool`, backup WAL, the explicit state paths, and Docker data/volume roots;
`PREFLIGHT_REQUIRE_PG_WAL_RISK=1` adds database-backed checks for `pg_wal`
bytes and `.ready` archive backlog before smoke jobs run.

For an existing named-volume deployment, migrate while services are stopped.
Canonical step-by-step migration: see [../../docs/DISK_RETENTION_RUNBOOK.md](../../docs/DISK_RETENTION_RUNBOOK.md)
("Move Existing Docker State To ZFS"), which covers the preferred Docker
data-root relocation on host `bart` and the manual bind-mount fallback (stop
writers, take recovery evidence, `rsync -aHAX --numeric-ids` copy, checksum and
ownership verification, switch `.env` to the ZFS paths, then run production
preflight and a restore drill before pruning any old Docker volumes).

Deploy-specific note: the `.env` ZFS paths to switch to are the
`TRADING_*_DATA`/`TRADING_*_LOGS` sources in the ZFS Storage Layout table above,
and confirm exact volume names with `docker volume ls` on the target host before
copying. Do not delete or prune old Docker named volumes until the restore drill
and signed backup evidence pass.

## Resource Isolation

The compose files bound every production service with Docker CPU and memory
limits. The values are configurable through `deploy/compose/.env`; keep
`PREFLIGHT_CHECK_RESOURCE_LIMITS=1` so `prod_preflight.py` reports unbounded
services or inconsistent memory/thread settings before the stack is considered
production-ready.

Recommended starting values for a 16-core / 32-thread / 123 GiB host:

| Service | CPU limit | Memory limit | Other bound |
| --- | ---: | ---: | --- |
| runtime | `RUNTIME_CPUS=12` | `RUNTIME_MEM_LIMIT=48g` | `RUNTIME_SHM_SIZE=8g` |
| Timescale | `TIMESCALE_CPUS=8` | `TIMESCALE_MEM_LIMIT=32g` | `TIMESCALE_SHM_SIZE=2g` |
| Redis | `REDIS_CPUS=2` | `REDIS_MEM_LIMIT=8g` | `REDIS_MAXMEMORY=6gb` |
| MinIO | `MINIO_CPUS=2` | `MINIO_MEM_LIMIT=6g` | object store process limit |
| operator | `OPERATOR_CPUS=1` | `OPERATOR_MEM_LIMIT=2g` | internal sidecar only |

Those defaults cap containers at 25 logical CPUs and 96 GiB RAM, leaving about
7 logical CPUs and 27 GiB RAM for the OS, Docker, diagnostics, tests, IDEs, and
emergency shell work. The preflight minimums are
`TRADING_RESOURCE_MIN_HEADROOM_CPUS=6` and
`TRADING_RESOURCE_MIN_HEADROOM_MEMORY=24g`.

The Timescale container is also the source of truth for Postgres tuning. Keep
`PREFLIGHT_REQUIRE_DOCKER_POSTGRES_TUNING=1`; production preflight reads the
same `TIMESCALE_*` values passed to `postgres -c`, validates them against
`TIMESCALE_MEM_LIMIT`, `TRADING_RESOURCE_HOST_MEMORY`, and
`TRADING_RESOURCE_MIN_HEADROOM_MEMORY`, and compares reachable `pg_settings`
values to catch drift after the container starts.

Recommended Timescale/Postgres settings for this 123 GiB host profile:

| Setting family | Values |
| --- | --- |
| Memory | `TIMESCALE_MEM_LIMIT=32g`, `TIMESCALE_SHARED_BUFFERS=8GB`, `TIMESCALE_EFFECTIVE_CACHE_SIZE=22GB`, `TIMESCALE_WORK_MEM=48MB`, `TIMESCALE_MAINTENANCE_WORK_MEM=2GB`, `TIMESCALE_AUTOVACUUM_WORK_MEM=512MB`, `TIMESCALE_MAX_CONNECTIONS=100` |
| Parallelism | `TIMESCALE_MAX_WORKER_PROCESSES=16`, `TIMESCALE_MAX_PARALLEL_WORKERS=8`, `TIMESCALE_MAX_PARALLEL_WORKERS_PER_GATHER=4`, `TIMESCALE_MAX_PARALLEL_MAINTENANCE_WORKERS=4`, `TIMESCALE_TIMESCALEDB_MAX_BACKGROUND_WORKERS=8` |
| Autovacuum | `TIMESCALE_AUTOVACUUM=on`, `TIMESCALE_AUTOVACUUM_MAX_WORKERS=4`, `TIMESCALE_AUTOVACUUM_NAPTIME=10s`, `TIMESCALE_AUTOVACUUM_VACUUM_COST_LIMIT=4000`, `TIMESCALE_AUTOVACUUM_VACUUM_COST_DELAY=2ms` |
| WAL/checkpoint | `TIMESCALE_WAL_BUFFERS=64MB`, `TIMESCALE_MIN_WAL_SIZE=4GB`, `TIMESCALE_MAX_WAL_SIZE=16GB`, `TIMESCALE_WAL_KEEP_SIZE=1GB`, `TIMESCALE_MAX_SLOT_WAL_KEEP_SIZE=8GB`, `TIMESCALE_WAL_DISK_BUDGET=40g`, `TIMESCALE_CHECKPOINT_TIMEOUT=15min`, `TIMESCALE_CHECKPOINT_COMPLETION_TARGET=0.9`, `TIMESCALE_ARCHIVE_TIMEOUT=60s` |
| IO cost | `TIMESCALE_RANDOM_PAGE_COST=1.1`, `TIMESCALE_EFFECTIVE_IO_CONCURRENCY=200`, `TIMESCALE_MAINTENANCE_IO_CONCURRENCY=200` |

That profile estimates about 25 GiB of bounded Postgres memory pressure inside
the 32 GiB service limit and caps configured retained WAL at 25 GiB under the
40 GiB WAL budget. The WAL budget is not a substitute for backup evidence:
archive failures can still force `pg_wal` growth, so keep
`PREFLIGHT_REQUIRE_BACKUP_EVIDENCE=1` and the WAL RPO checks enabled.
On ZFS hosts using the T2.5 PGDATA dataset spec, `primarycache=metadata` means
Postgres `shared_buffers` owns table/index page caching. Do not increase
`TIMESCALE_EFFECTIVE_CACHE_SIZE` to count PGDATA data blocks in ARC unless the
dataset primarycache policy is deliberately changed and reverified.

Smaller fallback hosts should lower the Timescale service limit first, then
scale Postgres settings with that limit:

| Host profile | Timescale limit | Recommended DB settings |
| --- | ---: | --- |
| 32 GiB host | `TIMESCALE_MEM_LIMIT=12g` | `shared_buffers=3GB`, `effective_cache_size=9GB`, `work_mem=8MB`, `maintenance_work_mem=512MB`, `autovacuum_work_mem=256MB`, `max_wal_size=8GB`, `wal_disk_budget=24g`, `TIMESCALE_CPUS=4` |
| 64 GiB host | `TIMESCALE_MEM_LIMIT=20g` | `shared_buffers=5GB`, `effective_cache_size=15GB`, `work_mem=16MB`, `maintenance_work_mem=1GB`, `autovacuum_work_mem=384MB`, `max_wal_size=8GB`, `wal_disk_budget=32g`, `TIMESCALE_CPUS=6` |
| 123 GiB host | `TIMESCALE_MEM_LIMIT=32g` | use the committed `.env.example` values above |

Keep Redis `REDIS_MAXMEMORY` below the Redis container limit, and keep runtime
worker/thread caps (`RESOURCE_SCHEDULER_*`, `MODEL_TRAIN_*`,
`LGBM_*_N_JOBS`, `XGB_N_JOBS`, `META_LABEL_N_JOBS`, `TSFRESH_*_N_JOBS`,
`TSFRESH_SNAPSHOT_*`, `TUNE_N_TRIALS`, `TUNE_MAX_N_TRIALS`,
`RUNTIME_OMP_NUM_THREADS`, `RUNTIME_MKL_NUM_THREADS`,
`RUNTIME_OPENBLAS_NUM_THREADS`, `RUNTIME_NUMEXPR_NUM_THREADS`,
`TORCH_CPU_THREADS`, and `TORCH_INTEROP_THREADS`) at or below
`RUNTIME_CPUS`.

The committed `.env.example` also enables the bounded ingestion host profile:
`INGESTION_TUNING_PROFILE=host_32t_123g`, parent Timescale/price pools of 8,
async price batches of 512 with a 1024-envelope queue, and child feed-process
pools capped at PG 3 / Timescale 4 / price-storage 4. `prod_preflight.py` and
`start_ingestion.py` reject out-of-bound overrides, while `/api/health` exposes
writer queue depth, dropped rows, retry counts, flush latency, and DB write
duration so operators can distinguish real throughput from backlog growth.

## Offline Training Profile

The default `runtime` service runs with `RUNTIME_WORKLOAD_PROFILE=live`,
`ALLOW_TRAINING=0`, serial model-family workers, TSFresh multiprocessing off
(`TSFRESH_N_JOBS=0`), low TSFresh symbol/batch caps
(`TSFRESH_SNAPSHOT_SYMBOL_LIMIT=100`, `TSFRESH_SNAPSHOT_BATCH_SIZE=25`),
low tuning trial caps (`TUNE_N_TRIALS=10`), and low scheduler concurrency.
Production preflight and job launch both reject offline/training jobs in that profile unless the
operator sets `OFFLINE_TRAINING_LIVE_PROFILE_ACK` to
`I_UNDERSTAND_OFFLINE_TRAINING_IN_LIVE_PROFILE` with a non-placeholder owner
and reason. That acknowledgement is for exceptional maintenance only, not the
normal training path.

Use the `offline-worker` compose profile for research, backtests, feature
discovery, TSFresh snapshot materialization, and model training. It uses
`RUNTIME_WORKLOAD_PROFILE=offline`, `ALLOW_TRAINING=1`, higher CPU/memory
limits, larger bounded worker counts, larger TSFresh symbol/batch caps, and a
larger tuning trial budget. It also requires `OFFLINE_TS_PG_DSN` so offline
work targets a restored clone or otherwise isolated datastore instead of
silently sharing the live Timescale service.

Example offline run from the repo root:

```bash
export OFFLINE_TS_PG_DSN="host=offline-timescale port=5432 user=trading dbname=trading_offline password=..."
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.stack.yml \
  --profile offline \
  run --rm offline-worker python -m engine.strategy.jobs.pipeline_train_and_eval
```

Keep the live `runtime`, Redis, and Timescale resource limits unchanged while
offline jobs run. If the offline datastore is a clone on the same host, reserve
host headroom first and lower `OFFLINE_CPUS`/`OFFLINE_MEM_LIMIT` rather than
borrowing from the live services.

Run offline jobs outside market hours or during an approved research window.
Before starting, verify the live service remains bounded:

```bash
python -m engine.runtime.prod_preflight --json
docker compose --env-file deploy/compose/.env -f deploy/compose/docker-compose.stack.yml ps
docker stats --no-stream runtime timescaledb redis offline-worker
```

Expected signals: production preflight reports `workload_profile=live` and
`allow_training=0` for the runtime service; `docker stats` shows live runtime,
Timescale, and Redis inside their configured CPU/memory limits; offline jobs
show `RUNTIME_WORKLOAD_PROFILE=offline` in their environment and use the
`OFFLINE_*` resource settings.

## Disk Retention And Backup Accounting

The compose stack has two retention layers. Docker service stdout/stderr is
bounded by `DOCKER_LOG_DRIVER`, `DOCKER_LOG_MAX_SIZE`, and
`DOCKER_LOG_MAX_FILE`. Runtime file logs written through the
`TRADING_RUNTIME_LOGS` ZFS bind mount are rotated by
`deploy/logrotate/trading-system`: daily, `maxsize 50M`,
10 compressed rotations, `maxage 21`, and `copytruncate`.

Inspect backup accounting and disk pressure before cleanup:

```bash
sudo /opt/trading/ops/backup/accounting.sh
docker system df
docker builder du
```

Production preflight includes `disk_pressure` and, when backup evidence is
fresh or non-required, backup `accounting` with host path, container mount view,
container mount source, apparent bytes, allocated bytes, subdirectory sizes, and
retention status/settings.

Safe Docker cleanup is limited to rebuildable cache, old images, and stopped
containers:

```bash
docker builder prune --filter until=168h
docker image prune -a --filter until=168h
docker container prune --filter until=168h
```

Do not run `docker volume prune`, `docker system prune --volumes`, or manual
deletion of Timescale, Redis, MinIO, app data, or `/var/backups/trading` state
on a live host. Use `ops/backup/prune.sh` for backup retention so base backups,
WAL, restore-drill evidence, and signed evidence remain consistent.

## Provider Credentials

The compose runtime passes provider and broker bootstrap settings from `deploy/compose/.env` into the Python runtime container. Leave providers disabled for the first dependency-only bring-up, then enable only the sources you have validated.

- Keep `PROD_LOCK=1`, `ALLOW_TRAINING=0`, `ENGINE_MODE=safe`, `EXECUTION_MODE=safe`, `DISABLE_LIVE_EXECUTION=1`, and the initial kill-switch hold for the first bring-up. Keep `LIVE_BROKER`, `BROKER`, `BROKER_NAME`, and `BROKER_FAILOVER` pinned to the same intended live broker (`ibkr` in the template).
- Keep `LIVE_TRADING_CONFIRM=0` in the committed example and during dependency-only bring-up. Set `LIVE_TRADING_CONFIRM=I_UNDERSTAND_LIVE_TRADING` only in the target host's operator-controlled compose `.env` when live execution is intentionally being enabled.
- Keep `TRADING_IMPORT_SMOKE_IMPORT_JOBS=0` for runtime startup. Startup compiles registered job files and imports only bootstrap-critical modules so heavy model/data imports cannot block dashboard binding. Use `TRADING_IMPORT_SMOKE_IMPORT_JOBS=1` only for explicit diagnostic runs.
- Keep `TRADING_DEPENDENCY_PROFILE=cpu`, `RUNTIME_HARDWARE_PROFILE=cpu`, `TORCH_DEVICE=cpu`, `EMBED_DEVICE=cpu`, `NLP_DEVICE=cpu`, `FINBERT_DEVICE=cpu`, and `TS_FOUNDATION_DEVICE=cpu` for the default AMD Ryzen AI Max+ 395 deployment. NVIDIA telemetry (`pynvml`/`nvidia-smi`), pinned-memory prefetch, GPU throttling, TF32, and cuDNN benchmark flags are all off by default through explicit `=0` env values. AMD/ROCm is now a reviewed opt-in profile: add `-f deploy/compose/docker-compose.amd-rocm.yml` only after `python tools/validate_rocm_acceleration.py --json` passes on the target host. The overlay builds the runtime with `TRADING_DEPENDENCY_PROFILE=amd-rocm`, maps `/dev/dri` and `/dev/kfd`, sets `RUNTIME_HARDWARE_PROFILE=amd-rocm`, `TRADING_ACCELERATION_PROFILE=amd-rocm`, and `TORCH_DEVICE=auto`, and leaves the operator container CPU-only. See [../../docs/DEPENDENCY_PROFILES.md](../../docs/DEPENDENCY_PROFILES.md) and [../../docs/ROCM_ACCELERATION.md](../../docs/ROCM_ACCELERATION.md) for profile selection and rollback commands.
- Polygon/Massive: create or retrieve an API key in the provider dashboard, confirm your plan includes the market-data entitlements you intend to use, store it in the file named by `POLYGON_API_KEY_FILE`, then set `POLYGON_REST_ENABLED=1` and/or `POLYGON_WS_ENABLED=1` only when that source should run. Official setup: [Polygon quickstart](https://polygon.io/docs/rest/quickstart).
- Tradier: use the API Access page in your Tradier profile to get a sandbox or live token, store it in the file named by `TRADIER_API_TOKEN_FILE`, then set `TRADIER_ENABLED=1`. Use a sandbox token for staging validation unless you are deliberately validating production data access. Official setup: [Tradier API getting started](https://support.tradier.com/kb/guide/en/getting-started-with-tradier-api-JYIaTkOdD1/Steps/4577201).
- Alpaca paper validation belongs in a non-live staging run. Store Alpaca credentials in `ALPACA_KEY_ID_FILE` and `ALPACA_SECRET_KEY_FILE`. Set `ENGINE_MODE=paper`/`EXECUTION_MODE=paper` and `ALPACA_BASE_URL=https://paper-api.alpaca.markets` only for that paper validation; live mode rejects the paper endpoint. Official setup: [Alpaca paper trading](https://docs.alpaca.markets/docs/paper-trading).

Do not commit populated `.env` files, and do not store live secret values in repo-local `.env` files. If provider secret files are absent or providers are disabled, infrastructure and preflight dependency checks can still run, but enabling Polygon, Tradier, Alpaca, or OpenAI makes the matching secret file/provider source mandatory. Production/live preflight rejects missing, empty, unreadable, or placeholder paths such as `/dev/null`, including Docker `/run/secrets/*` mounts.

Dashboard mutation endpoints require the token loaded from `DASHBOARD_API_TOKEN_FILE` or `DASHBOARD_API_TOKEN_SECRET` in production/live mode, including loopback binds and same-origin `/operator/api/*` operator-bridge mutations. The operator bridge also requires dashboard auth for protected sidecar reads before forwarding the server-side sidecar token. The runtime rejects missing, placeholder, too-short, or inline repo-local dashboard tokens before serving mutations or proxying to the operator sidecar; the localhost no-token fallback is only for explicit safe local development and should not be enabled in compose production-like stacks.

## Operator Mode In Containers

The operator container runs with `OPERATOR_DISABLE_INTERNAL_ENGINE_START=1`.

That is intentional. In the compose deployment path, the operator is a UI/proxy sidecar and not the lifecycle owner of the Python runtime process. Runtime lifecycle belongs to the container orchestrator, not to a sibling process trying to spawn `start_system.py` across containers.

Operator sidecar endpoints require the token loaded from `OPERATOR_API_TOKEN_FILE` or `OPERATOR_API_TOKEN_SECRET` for protected GET, HEAD, POST, and WebSocket access. Only `/api/operator/ping` is intentionally unauthenticated as a liveness probe. Loopback alone is not authorization. In compose, the sidecar is reachable on the internal Docker network as `operator:4001`; the runtime dashboard bridge forwards `X-Operator-Token` from server-side configuration after dashboard auth passes. If you intentionally publish the sidecar for a controlled local diagnostic, pass `X-Operator-Token` on every request and do not carry that override into production.

## Production Threshold

Do not treat this compose stack alone as proof that the full target-state plan is complete.

The current production threshold for this repo remains:

- compose stack starts cleanly
- runtime preflight passes
- `python tools/validate_repo.py --live` passes against the running stack
- `docs/handoff/codex_migration/FULL_SCOPE_VALIDATION.md` no longer has blocking `Partial in repo` or `Not implemented in repo` rows for your intended deployment claim

## 2026-06-15 Server Bring-Up Mode

Chosen deployment mode: compose.

Use compose for this production-functional server bring-up because the repository already defines the external dependency services, the Python runtime, and the Node operator sidecar in one deployment contract. The compose path keeps runtime lifecycle ownership with Docker, leaves the operator as a proxy/control sidecar, and starts the first dependency-only bring-up in `ENGINE_MODE=safe`, `EXECUTION_MODE=safe`, `PROD_LOCK=1`, `ALLOW_TRAINING=0`, `MODEL_SCORING_ENABLED=0`, `LIVE_BROKER=ibkr`, `BROKER_NAME=ibkr`, `BROKER=ibkr`, `BROKER_FAILOVER=ibkr`, `AUTO_BOOT_DAEMONS=0`, and `START_INGESTION_WITH_SERVER=0`.

Ingestion is intentionally not auto-started in this safe bring-up. Provider credentials are disabled by default, and explicit ingestion startup should be validated separately before enabling provider polling.

For the next production-function validation step, keep the broker identity pinned to `ibkr`, `EXECUTION_MODE=safe`, `DISABLE_LIVE_EXECUTION=1`, and `AUTO_PIPELINE_INCLUDE_EXECUTION=0`, then enable `AUTO_BOOT_DAEMONS=1`, `START_INGESTION_WITH_SERVER=1`, `MODEL_SCORING_ENABLED=1`, and `AUTO_PIPELINE=1`. This proves the data/scoring health path without enabling live broker order routing.

The exact bring-up command is:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  up -d --build
```

The exact preflight command is:

```bash
docker compose \
  --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  exec runtime python engine/runtime/prod_preflight.py --json
```

The exact endpoint checks are:

```bash
DASHBOARD_API_TOKEN_FILE="$(awk -F= '$1=="DASHBOARD_API_TOKEN_FILE"{print substr($0,index($0,"=")+1)}' deploy/compose/.env)"
OPERATOR_API_TOKEN_FILE="$(awk -F= '$1=="OPERATOR_API_TOKEN_FILE"{print substr($0,index($0,"=")+1)}' deploy/compose/.env)"
DASHBOARD_API_TOKEN="$(tr -d '\r\n' < "$DASHBOARD_API_TOKEN_FILE")"
OPERATOR_API_TOKEN="$(tr -d '\r\n' < "$OPERATOR_API_TOKEN_FILE")"

curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/api/readiness
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/api/operator/support_snapshot?mode=quick
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/api/execution/barrier
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/api/operator/provider_telemetry
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/api/operator/service_status
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/operator/api/operator/status
curl -fsS -H "X-API-Token: ${DASHBOARD_API_TOKEN}" http://127.0.0.1:8000/operator/api/operator/readiness
```

Do not enable real trading during this bring-up.
