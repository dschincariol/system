# Disk Retention Runbook

This runbook covers host disk controls added after the 2026-06-20 incident where `/var/backups/trading` grew to 339 GB on the root filesystem and pushed `/` to 95%.

## Backup Volume

Production backups must live on a dedicated filesystem, not the host root filesystem.

- Dataset: `zpool/trading-backups`
- Mountpoint: `/var/backups/trading`
- Compose mount: `${TRADING_BACKUP_ROOT:-/var/backups/trading}:/var/backups/trading`
- Runtime mount: read-only at `/var/backups/trading` for backup evidence checks

Verify the mount before enabling WAL archiving:

```bash
findmnt -T /var/backups/trading
df -h /var/backups/trading
```

## Backup Budget

`ops/backup/accounting.sh` reports backup `apparent_bytes`, allocated bytes, optional budget headroom, `observed_wal_bytes_per_day`, and `projected_days_to_full`:

```bash
TRADING_BACKUP_ROOT=/var/backups/trading \
TS_BACKUP_MAX_BYTES=900G \
bash ops/backup/accounting.sh
```

`TS_BACKUP_MAX_BYTES` is optional and accepts raw bytes or `K`, `M`, `G`, `T`, `P` suffixes. When set, `ops/backup/prune.sh` emits:

```text
level=error event=backup_over_budget ...
```

By default this is report-only. Set `TS_BACKUP_ENFORCE_BUDGET=1` to let `prune.sh` delete the oldest WAL that is older than both:

- the newest retained base backup, so the newest base remains restorable
- the configured `TS_BACKUP_WAL_CUSHION_DAYS` window

This budget enforcement intentionally does not delete base backups.

## Capacity Preflight

When `TS_BACKUP_MAX_BYTES` is set, or when `TS_BACKUP_CAPACITY_PREFLIGHT=1`, `prune.sh` checks whether backup filesystem free space can hold the observed WAL rate for:

```text
TS_BACKUP_KEEP_DAILY_DAYS + TS_BACKUP_WAL_CUSHION_DAYS
```

If free space is below that requirement, prune logs `event=backup_capacity_preflight_failed` and exits nonzero. This catches retention windows that cannot fit on the mounted backup filesystem before root disk pressure becomes an incident.

## Restore Drill Scratch

Restore drill reports are kept as `drills/restore_drill_*.txt`. Scratch directories under `drills/work/restore_drill_*` are disposable.

`restore_drill.sh` now traps `EXIT`, `INT`, and `TERM` so normal interruptions remove scratch. `prune.sh` also reaps abandoned scratch older than:

```text
TS_RESTORE_DRILL_WORK_TTL_DAYS=2
```

or scratch with no live drill process. The reaper does not delete the small `drills/*.txt` reports.

## Docker Logs

Compose services use Docker's `local` log driver with bounded files:

```text
DOCKER_LOG_MAX_SIZE=50m
DOCKER_LOG_MAX_FILE=5
```

`engine/runtime/prod_preflight.py` validates running production containers use a capped `local` or `json-file` log configuration when Docker is inspectable.
