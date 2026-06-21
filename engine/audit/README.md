# Audit Subsystem

The `engine/audit/` package owns the tamper-evident SHA-256 hash chain laid over the system's append-only ledger tables. It provides the canonical row serialization, the row-hash formula, the append API that writers use to chain new rows, and the verifier (plus its CLI) that recomputes every chain and records divergences. Consumers are the ledger writers that call `append_chain_row`, the `python -m engine.audit` operator CLI, and audit read helpers / governance checks that depend on the chain holding.

## Files

- [canonical.py](canonical.py)
  Builds `canonical_row_bytes(row)`: deterministic ASCII-encoded JSON over a row with `prev_hash`/`row_hash` excluded, lexicographically sorted keys, no insignificant whitespace, and value normalization (finite floats via `Decimal`, datetimes to UTC ISO-8601 `Z`, bytes to lowercase hex, sets to sorted arrays).
- [hashing.py](hashing.py)
  Computes `compute_row_hash(prev_hash, row)` as `sha256(prev_hash || canonical_row_bytes(row))`. A `None` prev means the genesis row (nothing prepended); an explicitly empty `b""` prev is replaced by the fixed `_EXPLICIT_EMPTY_PREV_HASH` sentinel (`b"\x00audit-empty-prev-hash\x00"`) so it cannot collide with genesis.
- [chain.py](chain.py)
  Append API. `append_chain_row(table, row, conn)` takes a per-table thread lock plus a Postgres advisory transaction lock, coerces JSON columns, allocates `id`, reads the latest `row_hash` as `prev_hash`, computes the new `row_hash`, and inserts the row. Also exposes the shared `table_columns`, `order_by_clause`, `latest_row_hash`, `coerce_row_for_hash`, and `row_identifier` helpers used by the verifier.
- [verifier.py](verifier.py)
  Recomputes each chain in verifier order and reports divergences. `verify_table` / `verify_all` stream cursor rows, emit `prev_hash_mismatch`, `row_hash_mismatch`, and `window_boundary_missing` findings, and persist them to `audit_chain_findings`. The verifier reports all divergences and never repairs the chain.
- [cli.py](cli.py)
  Argparse CLI exposing exactly two subcommands: `verify` (table/id-window/batch-size options, runs `verify_all` with findings emitted) and `hash-row` (recomputes one row by id and compares against its stored hash). No `benchmark` command exists.
- [__main__.py](__main__.py)
  Entry point for `python -m engine.audit`; delegates to `cli.main`.

## Row Order and Scope

The chain applies to tables classified `audit=True` by `engine.runtime.schema.table_classification.audit_tables()`. Rows are chained per table in verifier order: the first present time column among `ts_ms`, `ts`, `created_ts_ms`, `timestamp`, then `id` when present, then primary-key columns when no `id` exists. Writers serialize per table with a transaction-scoped Postgres advisory lock keyed by the table name (with a thread-lock and SQLite fallback for non-Postgres adapters).

## Key Outputs

- `audit_chain_findings` — one row per divergence, carrying `table_name`, `row_id`, `finding`, `expected_hash`, `actual_hash`, and a bounded JSON `payload_excerpt`.
- Verifier health/metrics — degraded cursor handling records `audit_chain_verifier` component health and the `audit_chain_verifier_degraded` counter.

## Contract

The authoritative serialization and verification contract is [../../docs/Audit_Chain_Spec.md](../../docs/Audit_Chain_Spec.md). That spec defines the `prev_hash`/`row_hash` column semantics, the canonical row bytes rules, the per-table hash formula, the row order, and the operator verification workflow; the modules here are its implementation and must stay byte-exact with it.
