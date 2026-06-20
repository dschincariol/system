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
- Runtime file logs under `/app/logs`, `/opt/trading-system/logs`,
  `/opt/trading/app/logs`, boot stderr logs, the diagnostics-only
  operator-AI JSONL log, and the compose `trading-logs` volume rotate
  daily, rotate early at `maxsize 50M`, keep 10 rotations, delete rotations
  older than 21 days, and compress old logs.
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
sudo /opt/trading/app/ops/backup/accounting.sh
docker compose --env-file deploy/compose/.env \
  -f deploy/compose/docker-compose.external-services.yml \
  -f deploy/compose/docker-compose.stack.yml \
  exec runtime python engine/runtime/prod_preflight.py --json
```

`ops/backup/accounting.sh` and production preflight report the host backup path,
the container mount destination/source when available, apparent and allocated
backup bytes, subdirectory sizes, current filesystem headroom, and configured
backup retention status.

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
rm -rf /var/backups/trading
```

The Timescale, Redis, MinIO, app data, and backup volumes contain live state or
recovery evidence. If backup storage must be reduced, use:

```bash
sudo /opt/trading/app/ops/backup/prune.sh
sudo /opt/trading/app/ops/backup/accounting.sh
```

## Evidence Preservation

Before any cleanup, preserve the latest backup evidence JSON and text report,
the latest base backup, WAL files required by the configured retention policy,
and the latest restore-drill report. Do not remove the backup evidence signing
key until newly signed evidence has been generated and production preflight
passes with the replacement key.
