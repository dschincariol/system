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

## Idle Windows NVMe Gate

Host `bart` was initially assumed to have an idle second NVMe at `nvme0n1`, but the first read-only smoke check showed that `/dev/nvme0n1` is active Linux storage on this runner (`/boot/efi`, `/`, `/home`, and `zfs_member`). Treat kernel disk names as unstable until discovery proves the actual target.

The default is no-op. Do not wipe, repartition, format, add to ZFS, or move Docker/Postgres onto any NVMe until [ADR 0006](adr/0006-idle-nvme-keep-or-reclaim-gate.md) has an explicit RETAIN or RECLAIM operator decision.

Warning: the RECLAIM branch destroys the Windows/BitLocker install on the selected disk.

Read-only discovery:

```bash
python tools/idle_nvme_assessment.py --discover --json \
  | tee /tmp/bart-idle-nvme-discovery.json
```

Discovery classifies every local disk as:

- `go_candidate` when it is an idle Windows/BitLocker disk with no Linux references
- `retain_candidate` when Windows/BitLocker is present but Linux references prevent reclaim
- `no_go` when it is active Linux/root/home/EFI storage, contains Linux filesystems, contains `zfs_member`, or is not a Windows/BitLocker layout

Use the candidate's stable `/dev/disk/by-id/...` path from discovery. If discovery reports more than one `go_candidate`, choose one explicitly and keep a target-specific assessment.

Target-specific read-only assessment:

```bash
TARGET_DISK_BY_ID=/dev/disk/by-id/<selected-windows-nvme>
python tools/idle_nvme_assessment.py --device "$TARGET_DISK_BY_ID" --json \
  | tee /tmp/bart-selected-idle-nvme-assessment.json
```

The assessment must show:

- `classification.classification=go_candidate`
- `unused_by_linux=true`
- no active mount, swap, md, ZFS, LVM, Docker, or config references
- `windows_bitlocker_layout_likely=true`
- the expected EFI/MSR/BitLocker/recovery partition layout

RETAIN branch:

- Keep the device unchanged.
- Record why Windows dual-boot, firmware tooling, diagnostics, or rollback is still required.
- Do not run `ops/server/reclaim_idle_nvme.sh` with `RECLAIM_DRY_RUN=0`.

RECLAIM branch:

The recommended target use is a dedicated fast local device for Docker data-root plus PostgreSQL/Timescale PGDATA staging. This gives direct value to the current Docker/Timescale-heavy workload without the pool-loss risk of an unmirrored ZFS special vdev. L2ARC is safer but lower value for this write-heavy path; a ZFS special vdev should only be considered if it is redundant and the whole pool design is reviewed.

Dry-run first:

```bash
TARGET_DISK_BY_ID=/dev/disk/by-id/<selected-windows-nvme> \
RECLAIM_ASSESSMENT_JSON=/tmp/bart-selected-idle-nvme-assessment.json \
bash ops/server/reclaim_idle_nvme.sh
```

Apply only after fresh backup/restore evidence exists:

```bash
sudo TARGET_DISK_BY_ID=/dev/disk/by-id/<selected-windows-nvme> \
  CONFIRM_DESTROY=<resolved-kernel-disk> \
  RECLAIM_DRY_RUN=0 \
  RECLAIM_ASSESSMENT_JSON=/tmp/bart-selected-idle-nvme-assessment.json \
  BACKUP_EVIDENCE_PATH=/var/backups/trading/evidence/latest_backup_restore_evidence.json \
  bash ops/server/reclaim_idle_nvme.sh
```

The apply path is guarded in the script itself:

- exact `CONFIRM_DESTROY=<resolved-kernel-disk>` token required
- optional `TARGET_DISK_BY_ID=/dev/disk/by-id/...` must resolve to the same disk named in the assessment
- fresh assessment required, max age `RECLAIM_ASSESSMENT_MAX_AGE_S=600`
- assessment must classify the selected disk as `go_candidate`
- fresh backup, WAL, and restore-drill evidence required
- root disk, Linux filesystem, ZFS, and mounted/reference checks must pass before destructive commands
- dry-run is the default

After a successful apply, stop Docker/Postgres before any data movement. Use `${RECLAIM_MOUNT_POINT:-/var/lib/trading-fast}/docker` as Docker's `data-root` only after intentionally migrating or rebuilding Docker state. Use `${RECLAIM_MOUNT_POINT:-/var/lib/trading-fast}/pgdata` for host-native PGDATA only through a stopped-service restore/migration. Re-run `ops/backup/backup_restore_evidence.sh` before enabling live runtime modes.

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
