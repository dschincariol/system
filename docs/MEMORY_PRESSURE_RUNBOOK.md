# Memory Pressure Runbook

This runbook covers the host-level memory hardening for production host `bart`
and other equivalent single-server ZFS deployments. The automation is
repo-tracked under `ops/server/` and must be run by an operator with `sudo`.

## Target State

`bart` has 128 GiB RAM and runs ZFS, Postgres/Timescale, Redis, Docker runtime
workloads, and ML allocations on the same host. The enforced defaults are:

| Setting | Value | Enforcement |
| --- | --- | --- |
| `vm.swappiness` | `10` | `/etc/sysctl.d/zz-trading-memory-pressure.conf` and live `sysctl -w` |
| zram swap | `32 GiB`, priority `100`, `zstd` when available | `trading-zram-swap.service` |
| disk swapfile | `16 GiB` at `/swapfile-trading`, priority `10` | `trading-swapfile.service` |
| ZFS ARC max | `48 GiB` (`51539607552` bytes) | `/etc/modprobe.d/trading-zfs-arc.conf` and live `/sys/module/zfs/parameters/zfs_arc_max` |

The ARC cap is intentionally below half of physical RAM. With ARC capped at
48 GiB, about 80 GiB remains for Postgres shared memory and work memory,
runtime containers, model inference/training allocations, Docker overhead,
tests, diagnostics, and the kernel. The 32 GiB zram device absorbs compressible
bursts without going straight to OOM, while the 16 GiB disk swapfile provides a
real emergency floor when anonymous memory cannot compress enough. zram has a
higher priority than the disk swapfile so the host uses compressed RAM first.

T2.5 keeps the dedicated Timescale PGDATA dataset at
`primarycache=metadata`. That means ARC should cache ZFS metadata and
non-PGDATA working sets, while Postgres `shared_buffers` owns database
table/index page caching. Do not raise `TIMESCALE_EFFECTIVE_CACHE_SIZE` on the
assumption that PGDATA data blocks are also cached in ARC under this policy; see
[DISK_RETENTION_RUNBOOK.md](DISK_RETENTION_RUNBOOK.md) for the ZFS property
verifier.

## Install Or Reapply

From a clean checkout or the installed app tree:

```bash
sudo bash ops/server/memory_pressure_hardening.sh install
```

The installer is idempotent. It writes managed sysctl, modprobe, and systemd
unit files; copies itself to `/usr/local/sbin/trading-memory-pressure`; applies
the active swappiness and ARC values; then enables and starts the zram and
swapfile units. Re-running the command updates changed managed files and leaves
already-correct files unchanged.

Host-specific overrides are environment variables:

```bash
sudo env \
  TRADING_ZFS_ARC_MAX_GIB=48 \
  TRADING_ZRAM_SIZE_GIB=32 \
  TRADING_SWAPFILE_SIZE_GIB=16 \
  bash ops/server/memory_pressure_hardening.sh install
```

## Verify

Run the read-only Python policy check first. It does not require `sudo`; it
reads `/proc/meminfo`, `/proc/sys/vm/swappiness`, `swapon --show`,
`/proc/swaps`, and ZFS ARC sysfs/kstat files when available:

```bash
python -m engine.runtime.memory_pressure --json --required | jq '{status,reason,errors,warnings,memory,swap,vm,zfs}'
```

Expected PASS on `bart`:

- `memory.mem_total_gib` is about `123`
- `memory.swap_total_gib >= 48`
- `swap.zram_total_gib >= 32` with `swap.zram_priority >= 100`
- `swap.managed_swapfile_path=/swapfile-trading`
- `swap.managed_swapfile_gib >= 16` with `swap.managed_swapfile_priority >= 10`
- `vm.swappiness=10`
- `zfs.arc_max_gib=48`

A legacy `SwapTotal` around `0.5 GiB` or an active `/swapfile` without the
managed `/swapfile-trading` is not production-ready and must block live
promotion when `PREFLIGHT_REQUIRE_MEMORY_PRESSURE_POLICY=1`.

Run the privileged active verifier after install, after reboot, and before live
promotion:

```bash
sudo bash ops/server/memory_pressure_hardening.sh verify
```

The verifier fails non-zero unless the persisted files are present and the
active host has:

- `vm.swappiness=10`
- readable `zfs_arc_max=51539607552`
- active zram swap of at least 32 GiB with priority at least 100
- active `/swapfile-trading` of at least 16 GiB with priority at least 10

Useful manual inspection commands:

```bash
awk '/MemTotal|MemAvailable|SwapTotal|SwapFree/ {print}' /proc/meminfo
sysctl -n vm.swappiness
cat /sys/module/zfs/parameters/zfs_arc_max
swapon --show=NAME,TYPE,SIZE,USED,PRIO
systemctl status trading-zram-swap.service trading-swapfile.service
```

## Reverse

To remove the managed policy:

```bash
sudo bash ops/server/memory_pressure_hardening.sh remove
```

The remove action stops and disables the managed swap units, removes the
managed `/swapfile-trading`, deletes the managed sysctl/modprobe/unit files,
and removes `/usr/local/sbin/trading-memory-pressure`. It does not guess or
restore unknown pre-existing swappiness or ARC values; after removal, reboot or
apply the site-approved replacement policy.

## Deleted `/tmp` File Reclaim

When `df -h /tmp` is much larger than `du -sh /tmp`, a process is holding
deleted files open on tmpfs. Detect the holders without modifying them:

```bash
python ops/server/detect_deleted_tmpfs_holders.py --path /tmp
```

The detector runs `lsof -nP +L1 -- /tmp` when `lsof` is installed and always
performs a read-only `/proc/*/fd` scan. It prints PID, user, command, fd,
size, and deleted target path. JSON evidence is available for incident records:

```bash
python ops/server/detect_deleted_tmpfs_holders.py --path /tmp --json
```

Safe reclaim procedure:

1. Identify the largest holder PID from the detector output.
2. Confirm it is a dead test/audit worker or other disposable process:
   `ps -fp <PID>` and, if useful, `pstree -sap <PID>`.
3. Ask the owner to stop it cleanly, or send `TERM` when it is clearly stale:
   `sudo kill -TERM <PID>`.
4. Recheck `python ops/server/detect_deleted_tmpfs_holders.py --path /tmp` and
   `df -h /tmp`.
5. Use `KILL` only after `TERM` fails and the process has no production
   responsibility. Do not truncate `/proc/<PID>/fd/<N>` for database, runtime,
   or broker processes.

## Test Scratch Policy

The pytest suite no longer defaults to `/tmp`. `tests/conftest.py` sets
`TMPDIR`, `TEMP`, `TMP`, and `PYTEST_DEBUG_TEMPROOT` to
`/var/tmp/trading-system-tests-<uid>/pytest` before pytest creates `tmp_path`
directories. `tools/validate_repo.py` sets the same environment for its pytest
collection and execution lanes, with a `validate-repo-<pid>` child directory so
parallel validators do not share the same SQLite/cache scratch files. Override
with `TRADING_TEST_TMPDIR=/disk/path` when a different disk-backed scratch root is
needed.

The default scratch tree should be removed with ordinary local cleanup when it
grows too large:

```bash
rm -rf "/var/tmp/trading-system-tests-$(id -u)"
```
