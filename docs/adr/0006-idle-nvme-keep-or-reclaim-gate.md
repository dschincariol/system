# ADR 0006: Idle NVMe Keep-or-Reclaim Gate

## Status

Proposed

## Date

2026-06-21

## Context

Host `bart` was initially believed to have a second idle NVMe at `nvme0n1` with approximately 1.9 TB of fast local storage and a Windows install layout: EFI, Microsoft reserved, a large BitLocker partition, and Windows recovery data.

The first read-only smoke assessment invalidated that kernel-name assumption on this runner: `/dev/nvme0n1` is active Linux storage with `/boot/efi`, `/`, `/home`, and a `zfs_member` partition. Kernel disk names are not stable enough to be the source of truth for a destructive workflow.

Any idle Windows/BitLocker capacity is valuable, but reclaiming it is destructive. Wiping or repartitioning the selected disk destroys the Windows/BitLocker install and removes any practical dual-boot recovery path unless the Windows system has been backed up independently.

## Decision

No branch is selected by default. Operators must choose exactly one branch and record the choice before running any destructive command.

### Branch: RETAIN

Retain the discovered Windows/BitLocker disk as-is when Windows dual-boot, Windows-side diagnostics, vendor firmware tooling, or a rollback path is still needed.

Required action:

- Run `python tools/idle_nvme_assessment.py --discover --json` and keep the output with the host notes.
- If retaining a specific Windows disk, record the stable `/dev/disk/by-id/...` path from discovery.
- Record why Windows is still required.
- Do not run `ops/server/reclaim_idle_nvme.sh` in apply mode.

### Branch: RECLAIM

Reclaim a disk only when discovery reports exactly one intended `go_candidate`, Windows is no longer required, and verified backup/restore evidence exists for the trading stack.

The highest-value default use for this workload is a dedicated fast local device for Docker data-root and PostgreSQL/Timescale PGDATA staging:

- Docker named volumes for TimescaleDB, Redis, MinIO, runtime data, and logs become NVMe-backed when Docker data-root is moved there.
- Host-native PostgreSQL/Timescale can use the same device for PGDATA after a stopped-service migration or restore.
- The path is operationally simpler and less pool-critical than adding an unmirrored ZFS special vdev.

ZFS alternatives are explicitly lower priority unless the host storage topology changes:

- L2ARC is non-destructive to the pool but usually lower value when RAM/ARC already absorb hot reads and the workload is write-heavy.
- A ZFS special vdev can speed metadata and small blocks, but an unmirrored special vdev is pool-critical; losing it can lose the whole pool.
- Adding raw capacity to an existing ZFS pool is only appropriate when pool layout, redundancy, and backup posture have been reviewed separately.

Required action:

- Run the read-only discovery and confirm it reports the intended disk as `classification=go_candidate`, `unused_by_linux=true`, and `windows_bitlocker_layout_likely=true`.
- Prefer the candidate's stable `/dev/disk/by-id/...` path over the kernel name when invoking the reclaim script.
- If discovery reports multiple `go_candidate` disks, select one explicitly with `TARGET_DISK_BY_ID=/dev/disk/by-id/...` and keep a target-specific assessment JSON.
- Confirm the backup evidence gate is fresh by running or reusing `ops/backup/backup_restore_evidence.sh`.
- Run `ops/server/reclaim_idle_nvme.sh` first in dry-run mode.
- Apply only with `CONFIRM_DESTROY=<resolved-kernel-disk> RECLAIM_DRY_RUN=0`, optionally paired with `TARGET_DISK_BY_ID=/dev/disk/by-id/...`.

The reclaim script enforces the branch gate in production ops code:

- It defaults to dry-run and no-op behavior.
- Apply mode refuses to run unless `CONFIRM_DESTROY` exactly matches the resolved target kernel disk.
- Apply mode refuses a stale assessment, a mismatched kernel target, or a mismatched `TARGET_DISK_BY_ID` stable path.
- Apply mode refuses stale or missing backup/restore evidence.
- The assessment must be fresh, classify the selected disk as `go_candidate`, confirm a Windows/BitLocker layout, and show no Linux references.
- Disks with critical Linux mountpoints, Linux filesystems, or `zfs_member` partitions are hard `no_go`.
- Destructive commands are reached only after those gates pass.

## Consequences

- The idle NVMe remains untouched until a human chooses RETAIN or RECLAIM.
- RECLAIM makes approximately 1.9 TB of fast NVMe available to the trading stack, but permanently destroys the Windows install.
- The recommended Docker/PGDATA use accelerates the current compose and database-heavy path without coupling the root pool to an unmirrored special vdev.
- If Windows must be preserved, the project accepts the opportunity cost of leaving the NVMe idle for Linux.
- On this runner, `/dev/nvme0n1` is explicitly NO-GO for reclaim because it is active Linux/ZFS storage.
