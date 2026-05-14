# Codex DB Prompt 06 — PgBouncer + Observability

You are working in a Python systematic trading system about to run a
fleet of supervised processes (UI, jobs manager, Polygon WS streamer,
RSS / news / GDELT pollers, training jobs) all hitting one Postgres on
the same Linux server. Without connection pooling, each process opens
its own Postgres backends, file-descriptor pressure rises, planner
caches do not warm, and tail latency suffers. This prompt puts
**PgBouncer in transaction-pool mode** between every application
connection and Postgres, and stands up the observability stack
(`pg_stat_statements`, slow-query log, key Grafana-friendly metrics)
the operator needs to keep the system healthy.

## Cross-platform note

PgBouncer is **Linux-only in this deployment**. The user develops on
Windows; on the dev machine, the application talks directly to
Postgres on TCP via `TS_PG_DSN` — at dev workload no pooler is
needed. The Python storage layer (DB-02) handles both
transports identically because the DSN is a single env var.

The observability snapshotter is platform-aware: when
`pg_stat_statements` is not installed (e.g., on a dev Postgres the
developer has not extended) it logs a single startup warning and
becomes a no-op. See
`docs/codex_prompts/database/CROSS_PLATFORM.md`.

## Goal

1. Every application connection routes through PgBouncer at
   `/var/run/postgresql/.s.PGSQL.6432`.
2. Per-process pool sizing tuned for the workload (streamers get more,
   one-shot jobs get fewer).
3. `pg_stat_statements` and the slow-query log give first-class
   visibility into the top contributors to latency and load.
4. A small metrics-export job ships the handful of dashboard-grade
   numbers (active connections, write rates per table, replication
   lag if present, cache hit rate) to Postgres' `runtime_metrics`
   hypertable so the existing dashboard surfaces them.
5. A first-cut Grafana dashboard JSON file the operator can import.

## Files to read first (read-only)

- `engine/runtime/storage_pool.py` (from prompt 02) — pool-size
  configuration that PgBouncer will sit in front of.
- `engine/runtime/storage_pg.py` — DSN handling.
- `engine/runtime/jobs_manager.py` — to know which processes hold
  the longest connections.
- `ops/server/config/pgbouncer.ini.tmpl` (from prompt 01) — the
  baseline PgBouncer config; this prompt extends it with per-database
  / per-role pool sizing.
- `engine/runtime/storage.py` for `runtime_metrics` schema.

## Files to create

- `ops/server/config/pgbouncer.userlist.txt.tmpl` — generated at
  bootstrap; user → SCRAM-SHA-256 hash from the systemd-creds-managed
  passwords (prompt 09).
- `ops/server/config/pgbouncer.ini` — final, post-render
  configuration. `pool_mode = transaction`,
  `default_pool_size = 25`, `reserve_pool_size = 5`,
  `reserve_pool_timeout = 3`, `max_client_conn = 200`,
  `server_idle_timeout = 60`, `query_wait_timeout = 30`,
  `application_name_add_host = 1`. Per-user overrides for
  `ts_ingest` (50), `ts_app` (40), `ts_reader` (15).
- `ops/server/systemd/pgbouncer.service` — drop-in to enforce
  `Restart=on-failure`, `LimitNOFILE=65536`,
  `User=postgres`.
- `engine/runtime/observability/__init__.py`
- `engine/runtime/observability/pg_stats.py` — periodic snapshotter:
  reads `pg_stat_statements` (top 50 by total_time and by calls),
  `pg_stat_user_tables` (per-table writes / reads / dead tuples),
  `pg_stat_database` (cache hit ratio, deadlocks, conflicts), and
  PgBouncer's `SHOW STATS` over its admin socket. Persists to
  `runtime_metrics` with structured `metric_name` strings so the
  dashboard can chart them.
- `engine/runtime/observability/slow_log.py` — tails the Postgres
  log file (`log_min_duration_statement = 250` from prompt 01's conf)
  and emits one `runtime_metrics` row per slow query with the
  normalized statement text.
- `engine/strategy/jobs/observability_snapshot.py` — registered in
  `job_registry` to run every 60 s.
- `tools/grafana/trading-overview.json` — Grafana dashboard JSON.
  Panels: connection state (active vs idle), top-10 queries by
  total_time, top-10 tables by write rate, cache hit ratio, slow
  queries last 5 min, Redis circuit state, ingestion lag per source.
- `tests/test_pgbouncer_routing.py`
- `tests/test_observability_pg_stats.py`
- `tests/test_observability_slow_log.py`
- `tests/test_pgbouncer_userlist_render.py`

## Files to modify

- `engine/runtime/storage_pg.py` (prompt 02) — default DSN points
  at PgBouncer's socket (`host=/var/run/postgresql port=6432`)
  rather than directly at Postgres. The direct-Postgres socket is
  reserved for migrations and admin tooling.
- `engine/runtime/storage_pool.py` (prompt 02) — Pool sizes shrink
  because PgBouncer multiplexes; per-process `max_size` reduces to 4
  for app processes, 8 for streamers, 2 for one-shot jobs.
- `ops/server/bootstrap.sh` (prompt 01) — render the userlist; copy
  the new pgbouncer.ini; reload pgbouncer.
- `engine/runtime/job_registry.py` — register
  `observability_snapshot`.
- `engine/runtime/schema/migrations/0006_observability.py` —
  `runtime_metrics` already exists; add an index on
  `(metric_name, ts DESC)` to support dashboard queries.

## Implementation plan

1. **PgBouncer config rendering.** The bootstrap script renders the
   final `pgbouncer.ini` from the template using values from systemd-
   creds (passwords) and host capacity (pool sizes). Idempotent — no
   diff after the first render unless capacity changes.
2. **Userlist.** SCRAM-SHA-256 hashes only; never plaintext on disk.
   The bootstrap script generates the hashes from the
   systemd-managed plaintexts at install time.
3. **Pool-mode caveats.** Transaction-pool mode forbids
   session-scoped state: `SET LOCAL` is fine, `SET` (session) breaks.
   The storage layer is already transaction-scoped (prompt 02), so
   verify nothing relies on session settings except `search_path`,
   which goes into the PgBouncer `server_reset_query`.
4. **Prepared statements under PgBouncer.** psycopg 3.x supports
   protocol-level prepared statements that work under transaction
   pooling via `prepare_threshold`. Set
   `prepare_threshold=5` so hot statements get prepared after the
   fifth use.
5. **`pg_stat_statements` sampling.** Snapshot the top 50 every
   minute; persist `(query_id, normalized_text, calls, total_time,
   mean_time, rows)`. Store normalized text the first time we see a
   `query_id`; subsequent snapshots store only the deltas.
6. **Slow-log tail.** Use `inotify` (`watchdog` package) to follow
   the Postgres log; parse with the regex documented in PG 16 docs
   (`duration: ... ms statement: ...`); emit a row per slow query.
7. **Dashboard.** The Grafana JSON is concrete: it queries
   `runtime_metrics` so it works against any Grafana instance the
   operator stands up.

## Performance targets

- PgBouncer adds **< 0.2 ms** to a round-trip on the Unix socket.
- 200 concurrent client connections multiplex onto **≤ 50** real
  Postgres backends.
- The observability snapshotter's load (queries against
  `pg_stat_statements`, etc.) is **< 1% CPU** sustained.
- Slow-log tail keeps up with bursts of 100 slow queries / minute
  without dropping events.

## Acceptance criteria

- [ ] No application code opens a Postgres connection on port 5432
      after this prompt; everything routes through 6432 (PgBouncer).
      Migrations and admin tools still use 5432.
- [ ] PgBouncer pool mode is `transaction` and the storage layer
      passes its existing tests under it.
- [ ] `pg_stat_statements` is enabled and snapshots land in
      `runtime_metrics` every minute.
- [ ] Slow-query log tail produces a `runtime_metrics` row for each
      query exceeding the configured threshold.
- [ ] The Grafana dashboard imports cleanly into a Grafana 10
      instance pointed at the Postgres datasource and renders
      non-empty panels within 10 s of import.
- [ ] PgBouncer userlist is generated from systemd-creds; no
      plaintext password appears in the userlist file (only SCRAM
      hashes).
- [ ] Test that proves prepared statements work under PgBouncer's
      transaction-pool mode (regression against a known foot-gun).

## Test plan

- `tests/test_pgbouncer_routing.py` — connect through PgBouncer,
  open / close 100 connections, assert Postgres-side backend count
  ≤ pool size.
- `tests/test_observability_pg_stats.py` — exercise the
  `pg_stat_statements` snapshotter; row appears in
  `runtime_metrics`.
- `tests/test_observability_slow_log.py` — synthetic log line →
  parsed → row.
- `tests/test_pgbouncer_userlist_render.py` — given a fixture
  password, the rendered file contains the expected SCRAM hash and
  no plaintext.
- Manual smoke: import the Grafana JSON, confirm panels render.

Run: `pytest -q tests/test_pgbouncer_routing.py
tests/test_observability_pg_stats.py
tests/test_observability_slow_log.py
tests/test_pgbouncer_userlist_render.py`

## Out of scope

- Prometheus exporter for Postgres (`postgres_exporter`). It works,
  but adds a dependency we do not need on a one-server deployment;
  `runtime_metrics` is the single source of dashboard data.
- Distributed tracing (OpenTelemetry). Add only if a future scaling
  push warrants it.
- Alertmanager / PagerDuty wiring. Alerting belongs in a separate
  prompt; observability here means metrics, not paging.
- Query plan analysis automation. Operator reads `EXPLAIN ANALYZE`
  by hand for now; auto-explain is too noisy.
