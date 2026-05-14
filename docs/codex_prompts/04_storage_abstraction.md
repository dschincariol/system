# Codex Prompt 04 — Storage Abstraction Layer + Postgres/Timescale Adapter

> **Status: superseded.** This prompt's deliverables were replaced wholesale by the database track under `docs/codex_prompts/database/` — specifically DB-02 (Postgres storage layer) and DB-03 (schema with hypertables), which moved straight to a Postgres-only implementation rather than a SQLite-Postgres abstraction layer. The acceptance checklist below should not be audited; the live equivalents live at `engine/runtime/storage_pool.py`, `engine/runtime/storage_pg.py`, `engine/runtime/storage.py` (facade), `engine/runtime/schema/migrator.py`, and `engine/runtime/schema/migrations/`.

You are working in a Python systematic trading system whose entire
persistence model is **a single SQLite database in WAL mode**, with
~25 tables ranging from time-series price snapshots to model registry
audits. SQLite has served well at single-machine scale, but it is the
hard ceiling on (a) concurrent ingestion throughput, (b) time-series
query patterns, and (c) horizontal-scale training workers. This prompt
introduces a **storage abstraction layer** so the codebase can target
Postgres + TimescaleDB without rewriting call sites, while keeping
SQLite as the default for development.

This is intentionally a **non-disruptive refactor**: behavior under
SQLite must be identical post-change. The Postgres adapter is feature-
complete but opt-in via a single environment variable.

## Goal

1. A `Storage` protocol that captures the methods currently exposed by
   `engine/runtime/storage.py` (connection, transaction, schema
   migration, common DDL helpers).
2. Two adapters: `SQLiteStorage` (default, behavior-preserving) and
   `PostgresStorage` (Timescale-aware where applicable).
3. A schema-portability layer: `engine/runtime/schema/` containing one
   migration per logical change, expressible against either dialect via
   small dialect-aware DDL emitters.
4. A migration tool that can dump SQLite → Postgres so a developer can
   spin up a Timescale instance and replay history.

## Files to read first (read-only)

- `engine/runtime/storage.py` — the entire current storage surface;
  every public function here defines the protocol you are extracting.
- Every `import storage` or `from engine.runtime.storage` site (grep) —
  the change must not break a single one of them.
- `engine/runtime/jobs_manager.py` — uses the connection extensively.
- `engine/strategy/promotion_audit.py`,
  `engine/strategy/decision_log.py`,
  `engine/execution/trade_attribution_ledger.py` — three of the busiest
  writers; reference for write patterns.
- `engine/data/poll_prices.py` and
  `engine/data/ingest/company_news_ingest.py` — high-cardinality
  ingestion writers; reference for batch insert patterns.
- `tests/` — for the existing storage test conventions.

## Files to create

- `engine/runtime/storage_protocol.py` — `Storage` Protocol class with
  every method currently used externally (connection, transaction,
  execute, executemany, fetch_one, fetch_all, ensure_schema,
  apply_migration).
- `engine/runtime/storage_sqlite.py` — extracted SQLite implementation
  (this is essentially today's `storage.py` body).
- `engine/runtime/storage_postgres.py` — psycopg3-based adapter;
  detects Timescale via `SELECT extversion FROM pg_extension WHERE
  extname='timescaledb'` and converts time-series tables to hypertables.
- `engine/runtime/schema/__init__.py`
- `engine/runtime/schema/migrations.py` — list of `Migration` objects:
  `id`, `description`, `up_sqlite(conn)`, `up_postgres(conn)`. The
  existing schema is migration `0001_initial`.
- `engine/runtime/schema/dialect.py` — small helpers like
  `integer_pk()`, `timestamp_col()`, `jsonb_or_text()` that emit the
  right SQL per dialect.
- `tools/migrate_sqlite_to_postgres.py` — one-shot transfer utility.
  Reads from SQLite, writes to Postgres, validates row counts.
- `tests/test_storage_protocol_compliance.py` — runs the same suite of
  smoke operations against both adapters; the Postgres branch is
  skipped unless `PG_TEST_DSN` is set.
- `tests/test_storage_migrations.py`
- `tests/test_storage_dialect_helpers.py`

## Files to modify

- `engine/runtime/storage.py` — becomes a thin facade that selects
  `SQLiteStorage` (default) or `PostgresStorage` based on env var
  `TS_STORAGE_BACKEND` (`sqlite` | `postgres`). Re-exports the public
  API so no caller changes.
- `engine/runtime/jobs_manager.py` — typed against `Storage` Protocol
  rather than the SQLite class directly. Cosmetic.
- `engine/runtime/job_registry.py` — register the migration job
  `apply_pending_migrations` if not already present.

## Implementation plan

1. **Protocol extraction.** Read every external use of `storage.*`.
   Define the minimum Protocol that satisfies them. Do not add
   speculative methods.
2. **SQLite adapter.** Move the body of today's `storage.py` into
   `storage_sqlite.py`. Public API unchanged.
3. **Postgres adapter.** Mirror every method using psycopg3. For
   parameter-binding, normalize `?` (SQLite) → `%s` (psycopg) inside
   the adapter so call sites do not change.
4. **Schema migrations.** Express the current schema as
   `0001_initial`. Convert hot time-series tables (prices, news,
   decision_log, trade_attribution) to Timescale hypertables on
   Postgres only.
5. **Dialect helpers.** Use these for any new migration so each lands
   in both backends with one definition.
6. **Round-trip tool.** `migrate_sqlite_to_postgres.py` opens both
   sides, applies migrations on Postgres, and copies row-by-row in
   batches of 5 000. Validates counts at the end.
7. **Compliance tests.** Run the same write/read scenarios against
   both adapters. Skip Postgres unless `PG_TEST_DSN` is set in the
   environment.

## Acceptance criteria

- [ ] No call site outside `engine/runtime/storage*.py` changes (grep
      `storage_sqlite|storage_postgres` in callers — must be empty).
- [ ] `TS_STORAGE_BACKEND=sqlite` is the default and produces
      byte-identical behavior to today's system on the existing test
      suite.
- [ ] `TS_STORAGE_BACKEND=postgres` with a valid `PG_DSN` initializes
      the schema, including hypertable conversions, on first run.
- [ ] `tools/migrate_sqlite_to_postgres.py` round-trips a populated
      development database and reports zero row-count diffs.
- [ ] All current tests pass without modification under the default
      backend.
- [ ] Storage compliance tests pass under both backends in CI when
      `PG_TEST_DSN` is supplied.

## Test plan

- `tests/test_storage_protocol_compliance.py` — parametrized over
  available backends; covers connection, transaction commit / rollback,
  ensure_schema idempotency, executemany, JSON column round-trip.
- `tests/test_storage_migrations.py` — applying migrations to an empty
  DB twice is a no-op; mid-failure rolls back; `current_version()`
  reflects applied state.
- `tests/test_storage_dialect_helpers.py` — emitted SQL contains the
  expected dialect-specific keywords (`JSONB` for Postgres,
  `TEXT` for SQLite, etc.).

Run: `pytest -q tests/test_storage_protocol_compliance.py
tests/test_storage_migrations.py tests/test_storage_dialect_helpers.py`
plus the full suite with the default backend.

## Out of scope

- Do not move data live. This prompt produces an *option*; the cutover
  is operational and tracked separately.
- Do not introduce an ORM. Stay raw SQL with parameter binding.
- Do not change the schema's logical content; only its dialect.
- Do not introduce Redis caching or Feast — those are separate prompts
  in the broader roadmap.
