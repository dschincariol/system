# Database Implementation Audit Checklist

This is the **human-judgment** portion of the audit, complementary to
the automated `tools/audit_db_implementation.py` script. Run the
script first; it covers structural, lint, file-existence, and (with
`--run-tests`) functional checks. This document covers what scripts
cannot — design correctness, performance under realistic load, and
Linux-only behavior verified by running.

## How to use

1. **Run the automated audit first.** From the repo root:
   ```
   python tools/audit_db_implementation.py --full
   ```
   `--full` runs the structural checks, the relevant pytest groups,
   and the live-database state queries (using `TS_PG_DSN` from the
   environment). Any failures here block this checklist — fix them
   first.
2. **Then walk this checklist** against the running staging
   environment, ticking each box only after you have verified it
   yourself with eyes on the running system. Do not tick from
   memory; do not tick from "the test passed."
3. **Record the date and operator** at the bottom of each section.
   Past audits become the audit trail of audits.

## Section A — Structural completeness

The automated script gives you the box; this section asks you to
look at what passed and confirm it is **right**, not just present.

- [ ] Open `engine/runtime/storage.py`. The public function names
      match the pre-prompt API (no callers outside `engine/runtime/`
      were forced to change). Spot-check three call sites
      (e.g. `engine/strategy/promotion_audit.py`,
      `engine/execution/trade_attribution_ledger.py`,
      `engine/strategy/predictor.py`) — they import and call the
      same function names they used pre-migration.
- [ ] Open `engine/runtime/schema/migrations/` and confirm:
      `0001_baseline.py` through at least `0009_credential_access_log.py`
      are present, in numerical order, each with a one-line
      `description` constant.
- [ ] Open `docs/Database_Schema.md`. It exists, lists every table,
      its classification, expected write rate, and read patterns.
      No table appears with "TBD" or "?" in any column.
- [ ] `docs/Audit_Chain_Spec.md` exists and is concrete enough that
      a third party with the spec and the row content could
      independently recompute a `row_hash`.
- [ ] `docs/Secrets_Rotation_Runbook.md` exists, contains numbered
      steps for both master-key and Postgres-role rotation, and
      names the operator-facing commands.
- [ ] `docs/codex_prompts/database/CROSS_PLATFORM.md` reflects the
      Linux-only env vars actually used by `engine/runtime/platform.py`. Open
      both side-by-side and reconcile any drift.

Operator: _________________________ Date: _____________

## Section B — Schema design correctness

Hypertables, indexes, retention, and compression are easy to get
syntactically correct and operationally wrong. Walk the live DB.

- [ ] Connect to the production-shaped DB and run:
      ```sql
      SELECT hypertable_name, num_chunks
      FROM timescaledb_information.hypertables
      ORDER BY hypertable_name;
      ```
      Every table classified as a hypertable in
      `engine/runtime/schema/table_classification.py` appears here.
      No surprise misses.
- [ ] Run:
      ```sql
      SELECT hypertable_name, segmentby
      FROM timescaledb_information.compression_settings;
      ```
      Every compressed hypertable has a sensible `segmentby` (usually
      `symbol`) — not empty, not the wrong column.
- [ ] Run:
      ```sql
      SELECT job_id, application_name, schedule_interval, hypertable_name
      FROM timescaledb_information.jobs
      WHERE proc_name IN ('policy_compression', 'policy_retention');
      ```
      Counts match the classification table; no policy missing or
      duplicated.
- [ ] Run:
      ```sql
      SELECT view_name, view_owner, materialization_hypertable_name,
             materialized_only
      FROM timescaledb_information.continuous_aggregates;
      ```
      Continuous aggregates exist (5-minute and 1-hour rollups for
      the dashboard), each with an active refresh policy.
- [ ] BRIN indexes on `ts` exist on every hypertable:
      ```sql
      SELECT tablename, indexname FROM pg_indexes
      WHERE indexdef LIKE '%USING brin%' AND indexname LIKE '%_ts_brin%';
      ```
- [ ] GIN indexes on JSONB columns where queried:
      ```sql
      SELECT tablename, indexname FROM pg_indexes WHERE indexdef LIKE '%USING gin%';
      ```
      Includes `decision_log.payload` and the audit ledgers' payloads.
- [ ] Audit-class tables have the chain columns:
      ```sql
      SELECT table_name FROM information_schema.columns
      WHERE column_name = 'row_hash' GROUP BY table_name;
      ```

Operator: _________________________ Date: _____________

## Section C — Hot-path latency under realistic load

The performance targets in the prompts are testable. Verify them, do
not assume them.

- [ ] **Kill switch read**: with the live decision pipeline running,
      time `read_kill_switch()` over 10 000 calls. p50 should be
      < 0.3 ms, p99 < 1 ms. Use:
      ```
      python -m engine.cache.wrappers.kill_switch --bench 10000
      ```
      (Add this `--bench` flag in the wrapper if it does not exist
      yet — five-minute task, well worth it.)
- [ ] **Latest feature snapshot**: time
      `feature_snapshots.latest("AAPL", "market_features")` over
      1 000 calls warm. p50 < 0.5 ms.
- [ ] **Postgres point read** (no Redis): time a `SELECT * FROM
      kill_switch_state WHERE id = 1` through the storage pool over
      1 000 calls. p50 < 0.5 ms, p99 < 2 ms.
- [ ] **Insert path**: time a `trade_attribution_ledger` insert
      including the audit hash chain over 1 000 calls. p99 < 5 ms.
- [ ] **Streaming write rate**: with the Polygon WS streamer
      pointed at a representative universe, observe Postgres write
      rate on `price_quotes_raw` for 60 seconds. The writer is not
      backpressured (no `WAL writer lag` warnings, no
      `unable to acquire connection` errors in the streamer log).

Record actual numbers below; the targets are not negotiable.

| Metric | Target | Observed |
|---|---|---|
| `read_kill_switch` p50 | < 0.3 ms | _______ |
| `read_kill_switch` p99 | < 1.0 ms | _______ |
| `feature_snapshots.latest` p50 | < 0.5 ms | _______ |
| Postgres point read p99 | < 2.0 ms | _______ |
| `trade_attribution_ledger` insert p99 | < 5.0 ms | _______ |
| Streamer sustained writes / sec | n/a | _______ |

Operator: _________________________ Date: _____________

## Section D — Linux platform contract

The whole point of the platform work is that the same Python code runs
on Linux development, staging, and production hosts. Verify.

- [ ] On the Linux staging server, set `TS_PG_DSN`, `TS_REDIS_URL`,
      `TS_DATA_ROOT` to the platform defaults. Run `pytest -q`. All
      relevant tests pass.
- [ ] On a Linux dev host, set the env vars to your dev DB DSN, dev
      Redis URL, and `/var/lib/trading` or an explicit `TS_DATA_ROOT`.
      Run `pytest -q`. Same tests pass.
- [ ] No `pytest.skip` markers fire because of platform on Linux tests
      that should have run.
- [ ] `engine/runtime/platform.py::default_data_root()` returns
      `/var/lib/trading` unless `TS_DATA_ROOT` overrides it.
- [ ] `engine/runtime/platform.py::default_pg_dsn()` returns the
      Linux Unix-socket DSN.
- [ ] systemd-creds round-trips a sample secret on Linux. The
      plaintext provider refuses to import when `TS_ENV=production`.

Operator: _________________________ Date: _____________

## Section E — Operational: backups, restore, secrets

- [ ] On staging, run `ops/backup/base_backup.sh` end-to-end. Output
      tarball plus sidecar verification file appears in
      `/var/backups/trading/base/<date>/`. `pg_verifybackup` passes.
- [ ] `archive_command` is firing: induce a write, force a WAL
      switch (`SELECT pg_switch_wal();`), and confirm a new file
      appears in `/var/backups/trading/wal/` within 60 s.
- [ ] Run `ops/backup/restore_drill.sh` end-to-end on staging. The
      drill report writes to
      `/var/backups/trading/drills/<date>.txt` with `OK` status.
      Time-to-recover is recorded; under 30 minutes.
- [ ] Master key rotation: run `ops/server/credstore/rotate_master_key.sh`
      against staging. Every `data_sources` row decrypts under the
      new key; old key is removed.
- [ ] Postgres role-password rotation:
      `ops/server/credstore/rotate_pg_role.sh ts_app`. PgBouncer
      reload completes; an application reconnect succeeds with the
      new password without restart.
- [ ] `journalctl -u trading-base-backup.timer` shows the timer is
      armed and last run completed cleanly.

Operator: _________________________ Date: _____________

## Section F — Audit chain integrity

- [ ] Insert a synthetic row into one audit table directly via
      `psql` (bypassing `chain.append_chain_row`). Run
      `python -m engine.audit verify --table <name>`. The verifier
      flags exactly one finding for that row. Delete the row.
- [ ] Re-run the verifier; clean exit, zero findings.
- [ ] Time a verification of a 100 000-row chain. Target: < 60 s.
- [ ] Open the `audit_chain_findings` table; it is empty in the
      steady state.

Operator: _________________________ Date: _____________

## Section G — No regressions

- [ ] The full pre-existing `pytest -q` suite still passes.
      Specifically, the tests under `tests/test_audit_invariants.py`,
      `tests/test_failure_diagnostics.py`, and any
      `test_promotion_*` are green.
- [ ] The dashboard renders. Spot-check three panels: kill-switch
      state, model registry, recent trades.
- [ ] A representative training job (`engine/strategy/jobs/train_temporal_predictor.py`)
      runs end-to-end against the new DB and writes a model to the
      artifact store with a content hash.
- [ ] A representative ingestion job (`engine/data/ingest/rss_ingest.py`)
      runs end-to-end and writes to `news_event_features`.
- [ ] No `WARNING` lines in `journalctl -u trading-*` for the last
      10 minutes that did not also appear pre-migration. (Some are
      expected; new ones are red flags.)

Operator: _________________________ Date: _____________

## Sign-off

When every box above is ticked and every observed number meets its
target, the implementation is **complete**. Record:

- Audit completion date: _____________________________
- Operator: _____________________________
- Staging DB version (`SELECT version();`): _____________________
- Timescale version: _____________________
- Outstanding follow-ups (if any): _____________________________

This document, completed and signed, is the artefact you keep for
the next time someone asks "is the database production-ready?".
