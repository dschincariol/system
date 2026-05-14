# Codex DB Prompt 02 — Replace SQLite Backbone with Postgres

You are working in a Python systematic trading system whose persistence
layer today is a single SQLite database in WAL mode covering ~210
tables. The system is **not yet in production**, so we are replacing
SQLite outright — no dual-write, no compatibility shim, no
SQLite-as-fallback. The single Linux server bootstrapped in prompt 01
provides Postgres 16 + TimescaleDB on the local Unix socket; this
prompt reworks `engine/runtime/storage.py` and every caller to use it.

This prompt establishes the **storage abstraction and connection
management**. Schema-as-code (hypertables, indexes, retention) is
prompt 03. Concentrate here on getting the application talking to
Postgres correctly and quickly.

## Cross-platform note

This is **application code** that must run unchanged on the developer's
Windows machine and on the Linux staging / production servers. Every
path uses `pathlib.Path`. Every connection target comes from an
environment variable with a platform-appropriate default. Read
`docs/codex_prompts/database/CROSS_PLATFORM.md` before writing code.

## Goal

1. `engine/runtime/storage.py` exposes the same public function names
   it does today, but the implementation talks to Postgres via
   `psycopg[binary,pool]` 3.x, using a DSN driven by the
   `TS_PG_DSN` environment variable. On Linux the default is the
   PgBouncer Unix socket; on Windows the default is TCP
   `127.0.0.1:5432`. The same Python code runs on either platform.
2. A connection-pool wrapper that gives every caller a fast,
   prepared-statement-friendly connection without managing pool
   lifecycle by hand.
3. A migrations framework: every schema change is a numbered Python
   module under `engine/runtime/schema/migrations/`. The startup
   sequence applies pending migrations in a single transaction per
   migration.
4. Parameter-binding compatibility: existing SQL strings using `?` are
   transparently rewritten to `%s` inside the storage layer so call
   sites do not change.
5. JSON columns become `JSONB`; `BLOB` becomes `BYTEA`; `INTEGER PK
   AUTOINCREMENT` becomes `BIGSERIAL`. The dialect helper from prompt
   01's spec is the place these map.
6. **No SQLite code remains in the runtime path.** A grep for
   `sqlite3` in `engine/` returns nothing.

## Files to read first (read-only)

- `engine/runtime/storage.py` — every public function here defines
  the API surface that must be preserved.
- A grep of `from engine.runtime.storage` and `import storage` —
  every call site must keep working with no changes.
- `engine/runtime/jobs_manager.py` — heaviest user of the connection
  context manager.
- `engine/strategy/promotion_audit.py`,
  `engine/strategy/decision_log.py`,
  `engine/execution/trade_attribution_ledger.py` — three of the
  busiest writers.
- `engine/data/poll_prices.py`,
  `engine/data/ingest/company_news_ingest.py` — high-cardinality
  ingestion writers.
- `engine/jobs/stream_prices_polygon_ws.py` — streaming writer; the
  pool sizing must accommodate it.
- `engine/runtime/locks.py` — to understand whether any logic relies
  on SQLite's specific advisory-lock behavior (Postgres has its own
  `pg_advisory_lock`).
- `tests/` — for the existing storage test patterns.

## Files to create

- `engine/runtime/storage_pg.py` — concrete Postgres implementation.
  All actual psycopg interaction lives here.
- `engine/runtime/storage_pool.py` — `ConnectionPool` wrapper around
  `psycopg_pool.ConnectionPool`, configured for the local Unix socket
  and a per-process pool size driven by env var
  `TS_PG_POOL_SIZE` (default 8 application, 16 ingestion, 4 jobs).
- `engine/runtime/storage_dialect.py` — helpers:
  - `to_pg_params(sql: str) -> str` — replaces `?` with `%s` outside
    of string literals (use `sqlparse` token walk, do not regex).
  - `bigserial()`, `jsonb()`, `bytea()`, `timestamptz()` — emit DDL
    fragments. New schema written through these from day one.
- `engine/runtime/schema/__init__.py`
- `engine/runtime/schema/migrator.py` — applies migrations in order,
  records applied set in `schema_migrations(id INTEGER PK,
  description TEXT, applied_at TIMESTAMPTZ DEFAULT now())`.
- `engine/runtime/schema/migrations/__init__.py`
- `engine/runtime/schema/migrations/0001_baseline.py` — emits the
  full baseline schema. (Prompt 03 fills in hypertable definitions
  and indexes.)
- `engine/runtime/locks_pg.py` — replaces the SQLite advisory-lock
  surface with `pg_advisory_lock` / `pg_advisory_unlock` over a
  64-bit lock-name hash.
- `tests/test_storage_pg_smoke.py` — round-trip insert / select on a
  trivial table; pool returns a connection within the configured
  timeout.
- `tests/test_storage_param_rewrite.py` — `?` → `%s` rewriter handles
  parameters, string literals containing `?`, JSON paths.
- `tests/test_storage_migrator.py` — migrations apply once; second
  run is a no-op; a failed migration rolls back its transaction.
- `tests/test_storage_locks_pg.py` — advisory lock contention; second
  acquirer blocks until the first releases.
- `tests/test_no_sqlite_in_runtime.py` — fails the build if any
  module under `engine/` imports `sqlite3` or references `.db` paths.
- `engine/runtime/platform.py` — single helper module:
  `is_linux()`, `is_windows()`, `default_pg_dsn()`,
  `default_admin_pg_dsn()`, `default_data_root() -> Path`.
  The single source of truth for platform-conditional defaults.
- `tests/test_platform_defaults.py` — defaults match the contract
  in `CROSS_PLATFORM.md` for each platform.
- `tests/test_no_string_paths.py` — AST scan of `engine/` and
  `services/`: no module body contains hardcoded path string
  literals like `/var/lib/`, `/etc/`, or `\\Trading\\`. All paths
  go through `pathlib.Path` and platform helpers.

## Files to modify

- `engine/runtime/storage.py` — becomes the **only** importable
  public facade. Re-exports the same function names. Internal
  implementation delegates to `storage_pg`. **Delete the SQLite
  code path entirely**; do not leave commented-out blocks.
- `engine/runtime/locks.py` — re-exports from `locks_pg`.
- `engine/runtime/storage.py` callers — none should require source
  changes if the API surface is preserved. Verify by grep.

## Implementation plan

1. **Define the public API.** Read every external call to
   `storage.*` and write the Protocol that satisfies them. Do not
   add speculative methods. Likely shape:
   `connection() -> ContextManager[Connection]`,
   `transaction() -> ContextManager[Connection]`,
   `execute(sql, params=None)`,
   `executemany(sql, seq_of_params)`,
   `fetch_one(sql, params=None)`,
   `fetch_all(sql, params=None)`,
   `apply_migrations()`.
2. **Connection pool.** `psycopg_pool.ConnectionPool` with
   `min_size=2, max_size=<env>`. Connection string built from env
   `TS_PG_DSN`. Defaults computed by `engine/runtime/platform.py`:
   on Linux, `host=/var/run/postgresql user=ts_app dbname=trading`;
   on Windows, `host=127.0.0.1 port=5432 user=ts_app dbname=trading`
   (developer supplies password via `TS_DEV_PG_PASSWORD` or via the
   secrets loader from prompt 09). Connections request
   `autocommit=False` and apply `SET search_path = trading, public`
   on check-out.
3. **Parameter rewrite.** `?` → `%s` happens once per SQL string at
   the boundary; cache the rewritten form by `id(sql)` so hot loops
   pay it once. Use `sqlparse` to walk tokens and only rewrite
   placeholders outside string literals.
4. **Migrator.** Each migration module exports `id: int`,
   `description: str`, `up(conn)`. The migrator selects the max
   `id` from `schema_migrations`, runs every higher-numbered
   migration in order, each in its own transaction. Idempotent.
5. **Baseline migration.** `0001_baseline` mirrors the current
   SQLite schema in Postgres-friendly types (TEXT, BIGINT,
   TIMESTAMPTZ, JSONB). The hypertable conversion and index design
   live in prompt 03's migration `0002_hypertables`.
6. **Advisory locks.** `with advisory_lock(name): ...` hashes
   `name` to a 64-bit integer (CRC64) and calls
   `SELECT pg_advisory_xact_lock(:k)` inside the active transaction,
   or `pg_advisory_lock(:k)` for session-scoped. Mirror the surface
   of `engine/runtime/locks.py`.
7. **Delete SQLite cleanly.** Remove `sqlite3` imports and the
   WAL-related shims. The `tests/test_no_sqlite_in_runtime.py`
   guard ensures it stays gone.

## Performance targets

- A point-read from a primary key over the Unix socket from a warm
  pool returns in **< 0.5 ms p50, < 2 ms p99** on the canonical host.
- A 1 000-row `executemany` insert into a regular table completes in
  **< 50 ms p99**.
- Pool acquisition under no contention is **< 0.1 ms**; under
  saturation (all connections in use) it blocks up to
  `TS_PG_POOL_TIMEOUT` (default 5 s) and raises a typed exception.
- `apply_migrations()` on a clean DB runs in **< 30 s** for the full
  baseline schema.

## Acceptance criteria

- [ ] `grep -rn "import sqlite3\|from sqlite3" engine/` returns no
      results.
- [ ] No production code path opens a `.db` file.
- [ ] `psycopg.connect()` is never called from application code; all
      access goes through the pool.
- [ ] Re-running `apply_migrations()` is a no-op.
- [ ] Existing call sites compile and pass tests with **zero source
      changes** outside `engine/runtime/storage*.py` and `locks*.py`.
- [ ] `?`-style placeholders in pre-existing SQL strings still work
      via the rewriter, including SQL containing `?` inside a string
      literal.
- [ ] Advisory-lock contention test demonstrates a second acquirer
      blocks until the first releases, and unblocks within 50 ms of
      release.
- [ ] Full test suite runs to green on both Linux (Ubuntu 22.04 in
      CI) and Windows (windows-latest runner in CI), pointed at a
      Postgres reachable via the platform-default DSN.
- [ ] No hardcoded `/var/`, `/etc/`, or `C:\` path literals in
      `engine/` or `services/` (enforced by
      `tests/test_no_string_paths.py`).
- [ ] All file paths use `pathlib.Path`; no string concatenation
      with `/` or `\\` separators.

## Test plan

- `tests/test_storage_pg_smoke.py` — pool check-out / check-in;
  trivial round-trip; transaction rollback isolation.
- `tests/test_storage_param_rewrite.py` — placeholder rewriting;
  string-literal preservation; JSON-path operator preservation
  (`->`, `->>`).
- `tests/test_storage_migrator.py` — apply / re-apply / failed
  migration rollback; concurrent applier collision uses an advisory
  lock so only one applier wins.
- `tests/test_storage_locks_pg.py` — contention; release; named
  collision behavior.
- `tests/test_no_sqlite_in_runtime.py` — AST-walk every module under
  `engine/` and assert no `sqlite3` import.

Run: `pytest -q tests/test_storage_pg_smoke.py
tests/test_storage_param_rewrite.py tests/test_storage_migrator.py
tests/test_storage_locks_pg.py tests/test_no_sqlite_in_runtime.py`

These tests require a running Postgres on the local Unix socket from
prompt 01, with the `trading` database and the `ts_app` role
present. The test suite skips with a clear message if `TS_PG_DSN`
points to an unreachable instance.

## Out of scope

- Defining hypertables, indexes, retention, compression — that is
  prompt 03.
- Redis caching — that is prompt 04.
- PgBouncer wiring — prompt 06. (For now the pool talks directly to
  Postgres on the Unix socket; switching to PgBouncer is a one-line
  DSN change once prompt 06 lands.)
- Backups — prompt 07.
- Schema for any new feature (FDR audit, ensemble OOS, NLP cache).
  Those land in their own prompts after this lays the foundation.
- Performance tuning beyond the targets above. PostgreSQL config
  itself is owned by prompt 01.
