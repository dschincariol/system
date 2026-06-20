# Codex DB Prompt 07 — Backup, WAL Archive, and Restore Drill

You are working in a Python systematic trading system that has just
been wired to a Postgres + Timescale + Redis stack on a single Linux
server. Before live capital flows, the persistence layer needs a
**defensible recovery story**: continuous WAL archiving, scheduled
base backups, an explicit point-in-time-recovery script, and a
quarterly restore drill that proves backups are real.

The bar is institutional: any second of committed data must be
recoverable, and the recovery procedure must be a single command an
operator can run under stress.

## Linux-only note

This prompt is **Linux-only by design**. Backups are a
staging/production concern and rely on bash scripts, systemd timers,
`pg_basebackup`, and Postgres' `archive_command`. For dev, occasional
`pg_dump` of the dev database is sufficient and is documented in
`docs/codex_prompts/database/CROSS_PLATFORM.md`. Do not produce
platform shell wrappers.

## Goal

1. Continuous WAL archiving from Postgres into
   `/var/backups/trading/wal/` with optional offsite mirror to a
   user-supplied object store.
2. Scheduled `pg_basebackup` runs (daily) with a clear retention
   schedule.
3. Daily snapshot of the artifact store (prompt 05) and a tarball of
   `/etc/trading/` (config) into `/var/backups/trading/state/`.
4. Redis AOF persistence is preserved verbatim; it is part of the
   backup set but is not authoritative (Postgres is — Redis is the
   cache, see prompt 04).
5. A scripted **restore-into-clean-host** procedure that produces a
   running system from the backup set in under 30 minutes.
6. A quarterly drill harness that runs the restore in an isolated
   environment and produces a pass/fail report.

## RPO / RTO

- **RPO ≤ 1 minute** for committed Postgres data, achieved via
  continuous WAL archiving.
- **RTO ≤ 30 minutes** to a running, trade-paused system on a fresh
  host.

## Files to read first (read-only)

- `ops/server/bootstrap.sh` (prompt 01) — current installation flow;
  this prompt adds backup-side configuration to it.
- `ops/server/config/postgres.conf.tmpl` (prompt 01) — `archive_mode`
  and `archive_command` already need values; this prompt finalizes
  them.
- `engine/artifacts/store.py` (prompt 05) — the artifact directory
  layout we are backing up.
- `engine/runtime/storage_pg.py` (prompt 02) — to understand which
  database is the system of record.
- `engine/execution/kill_switch.py` — to understand the trade-pause
  semantics the restore script will trigger as its last step.

## Files to create

- `ops/backup/wal_archive.sh` — script invoked by Postgres'
  `archive_command`. Atomic copy into
  `/var/backups/trading/wal/`; optional second copy to an offsite
  destination supplied via `TS_WAL_OFFSITE_CMD` env var (e.g.
  `aws s3 cp - s3://bucket/wal/<name>` or `rclone rcat
  remote:trading/wal/<name>`). Returns non-zero on any failure so
  Postgres knows to retry.
- `ops/backup/base_backup.sh` — runs `pg_basebackup -D
  /var/backups/trading/base/<ISO_DATE>/ -F tar -z -X stream -P -R`.
  Verifies the resulting tarball with `pg_verifybackup`. Updates a
  `latest` symlink atomically.
- `ops/backup/state_snapshot.sh` — tars `/etc/trading/` and the
  artifact-store directory listing (not the objects themselves —
  those are handled by `artifact_snapshot.sh` because they may be
  large).
- `ops/backup/artifact_snapshot.sh` — incremental rsync of the
  artifact store to `/var/backups/trading/artifacts/` (or to the
  offsite destination). Runs nightly.
- `ops/backup/prune.sh` — retention enforcement: keep base backups
  for 14 days locally, keep weekly base backups for 1 year, keep WAL
  segments needed by the oldest retained base backup plus 7 days
  cushion.
- `ops/backup/restore.sh` — the headline restore script. Arguments:
  `--target-time <ISO8601 | latest>`, `--into <directory>`,
  `--allow-trade-paused`. Steps:
  1. Verify selected base backup with `pg_verifybackup`.
  2. Untar base into `--into`.
  3. Construct `recovery.signal` and `restore_command` pointing at
     `/var/backups/trading/wal/`.
  4. Set `recovery_target_time` if specified.
  5. Start the recovered Postgres on a non-default port.
  6. Wait for `pg_is_in_recovery() = false`.
  7. Run a smoke query against `model_registry`.
  8. **Trip the kill switch** in the recovered DB so any process
     pointed at it cannot trade.
  9. Print recovery summary (checkpoint LSN, recovered to time,
     time elapsed).
- `ops/backup/restore_drill.sh` — orchestrates a full drill:
  - Picks a fresh datadir.
  - Calls `restore.sh` with `--target-time latest`.
  - Connects through PgBouncer-on-test-port.
  - Runs a row-count sanity script (`tools/restore_sanity.sql`)
    against a curated set of tables (registries, recent decisions,
    last day of fills) and produces `restore_drill_<date>.txt` in
    `/var/backups/trading/drills/`.
  - Tears down the recovered instance.
  - Exits non-zero on any sanity failure.
- `ops/server/systemd/trading-base-backup.service` and
  `trading-base-backup.timer` — daily 03:00 local.
- `ops/server/systemd/trading-state-snapshot.service` and `.timer` —
  daily 03:30.
- `ops/server/systemd/trading-artifact-snapshot.service` and
  `.timer` — daily 04:00.
- `ops/server/systemd/trading-backup-prune.service` and `.timer` —
  daily 05:00.
- `ops/server/systemd/trading-restore-drill.service` and `.timer` —
  monthly (calibration; quarterly is the operator's drill).
- `tools/restore_sanity.sql` — read-only sanity queries.
- `tests/ops/test_wal_archive_script.sh` — feeds a fake WAL segment
  through the archive script; verifies destination layout and offsite
  hook invocation.
- `tests/ops/test_base_backup_verify.sh` — runs base backup against
  a tiny test instance; `pg_verifybackup` passes.
- `tests/ops/test_restore_drill_dry.sh` — restore-drill in a
  containerized fresh Postgres; sanity script returns expected row
  counts.

## Files to modify

- `ops/server/config/postgres.conf.tmpl` (prompt 01) — set
  `archive_mode = on`, `archive_command = '/opt/trading/ops/backup/wal_archive.sh "%p" "%f"'`,
  `wal_level = replica`, `archive_timeout = 60s` (force a segment
  every minute even on quiet systems so RPO holds).
- `ops/server/bootstrap.sh` — install the backup scripts; install the
  systemd units / timers.
- `engine/runtime/job_registry.py` — register
  `monthly_restore_drill` job that runs `restore_drill.sh` and emits
  a `runtime_metrics` row with the drill outcome.

## Implementation plan

1. **WAL archive command must be atomic and reliable.** Write to a
   temporary file under `/var/backups/trading/wal/.tmp/`, fsync, then
   `rename` into the final location. On any failure, return non-zero
   so Postgres holds the segment and retries.
2. **Base backups in compressed tar with checksum stream.** Keep
   `pg_verifybackup` output in a sidecar file alongside the tarball;
   refuse to mark a backup `latest` until verification passes.
3. **Retention is enforced separately from creation.** A failed
   prune does not break the next backup; an exception in prune is
   logged and surfaced as an alert.
4. **Restore script is idempotent on the target directory.** If the
   target directory is non-empty, refuse unless `--force` is passed.
5. **Recovered instance starts in a paused state.** The script
   tripping the kill switch in the recovered DB ensures that even if
   an operator points the trading processes at the recovered host
   immediately, no live order can be submitted until they
   intentionally clear it.
6. **Drills produce structured artifacts.** Every drill writes a
   text report including: time-to-recover, target time achieved,
   sanity-query results, anomalies, exit code. These accumulate and
   become the audit trail for "have we tested recovery?".
7. **Offsite is optional but ergonomic.** `TS_WAL_OFFSITE_CMD`
   accepts any shell command that reads the WAL segment from stdin;
   an unset variable disables offsite without breaking the local
   pipeline.

## Performance targets

- WAL archive command runs in **< 50 ms** for a 16 MB segment on
  the canonical host with local-only archiving.
- Base backup of a 100 GB cluster completes in **< 30 minutes**.
- Restore to a fresh host completes in **< 30 minutes** for that
  same 100 GB cluster, including WAL replay of the trailing 24
  hours.

## Acceptance criteria

- [ ] `archive_mode = on` and `archive_command` invokes
      `wal_archive.sh`; a synthetic write produces a segment that
      lands in the archive within `archive_timeout`.
- [ ] `pg_verifybackup` passes on every base backup before it is
      marked `latest`.
- [ ] `restore.sh --target-time latest --into <tmp>` reproduces a
      working Postgres on a non-default port with the kill switch
      tripped.
- [ ] `restore_drill.sh` runs end-to-end in CI in a Debian Docker
      container (Postgres + Timescale installed) and reports pass.
- [ ] Pruning never deletes a WAL segment that any retained base
      backup needs.
- [ ] All scripts use `set -euo pipefail` and emit structured logs
      (key=value or JSON) to journald.
- [ ] No drill report has been older than 32 days
      (`runtime_metrics` row "backup.last_drill_age_days" surfaces
      it on the dashboard).

## Test plan

- `tests/ops/test_wal_archive_script.sh` — fake segment in,
  destination layout correct; offsite-cmd invoked when set.
- `tests/ops/test_base_backup_verify.sh` — base backup → verify →
  latest symlink; corrupt the tarball and verify refuses.
- `tests/ops/test_restore_drill_dry.sh` — full drill in container;
  sanity SQL returns expected counts; report file written.

Run: `bash tests/ops/test_wal_archive_script.sh && bash
tests/ops/test_base_backup_verify.sh && bash
tests/ops/test_restore_drill_dry.sh`

## Out of scope

- Multi-region replication. One server, one offsite copy of WAL,
  one offsite copy of base backups. Geo-redundancy is a future move.
- Hot standby / streaming replication. The single-server model is
  authoritative; replication adds operational complexity that does
  not pay back at this scale. WAL + base backups + a drilled restore
  beats an unmonitored replica.
- Backup encryption beyond what the offsite destination provides
  (e.g. S3 server-side encryption). Local backups inherit
  filesystem encryption from the host (LUKS recommended); managing
  separate backup encryption keys is its own project.
- Backup of Redis as the system of record. Redis is a cache; AOF
  rolls forward; a Redis loss is not a data loss because Postgres
  is authoritative.
