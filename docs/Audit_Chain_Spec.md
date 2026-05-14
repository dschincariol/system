# Audit Hash Chain Specification

## Scope

The audit hash chain applies to tables classified with `audit=True` in
`engine.runtime.schema.table_classification.audit_tables()`. Each row carries:

- `prev_hash BYTEA NULL`: the previous row's `row_hash` in the same table.
- `row_hash BYTEA NOT NULL`: `SHA-256(prev_hash || canonical_row_bytes)`.

`prev_hash` is omitted from the hash input for the first row. Hash columns are
never part of `canonical_row_bytes`.

## Row Order

Rows are chained in verifier order:

1. The first existing time column among `ts_ms`, `ts`, `created_ts_ms`,
   `timestamp`.
2. `id`, when present.
3. Primary-key columns, when no `id` column exists.

Writers serialize per table with a transaction-scoped Postgres advisory lock
keyed by the table name.

## Canonical Row Bytes

Canonical row bytes are UTF-8 encoded deterministic JSON with ASCII escaping:

- Object keys are sorted lexicographically.
- No insignificant whitespace is emitted.
- `prev_hash` and `row_hash` are excluded.
- JSON columns are hashed as structured JSON, not as database-specific JSON
  text.
- `datetime` values are normalized to UTC ISO-8601 with microseconds and `Z`.
- Integers are decimal JSON numbers.
- Floats are finite only and serialized through decimal form so `1.0` and `1`
  both hash as `1`.
- `Decimal` values are normalized without exponent notation.
- `bytes`, `bytearray`, and `memoryview` values are encoded as lowercase hex
  strings.
- Sets are converted into deterministically sorted arrays.

The implementation source of truth is `engine.audit.canonical.canonical_row_bytes`.

## Hash Formula

For each table independently:

```text
row_hash[0] = SHA256(canonical_row_bytes(row[0]))
row_hash[n] = SHA256(row_hash[n-1] || canonical_row_bytes(row[n]))
prev_hash[n] = row_hash[n-1]
```

Hashes are persisted as raw bytes in `BYTEA`. CLI tools print lowercase hex for
manual comparison.

## Verification

Run:

```bash
python -m engine.audit verify
python -m engine.audit verify --table trade_attribution_ledger
python -m engine.audit verify --table trade_attribution_ledger --from-id 1000 --to-id 2000
python -m engine.audit hash-row --table trade_attribution_ledger --id 123
python -m engine.audit benchmark --rows 10000
```

The verifier recomputes every row in order. A mismatch is written to
`audit_chain_findings` with the table, row id, finding type, expected hash,
actual hash, and a small JSON payload excerpt. The verifier reports all observed
divergences and does not repair the chain.

The verifier streams cursor rows instead of loading whole ledgers into memory.
`--from-id` and `--to-id` bound the walk for operational checks; when a bounded
walk starts after the first row, the verifier seeds the previous hash from the
last earlier row so the first row in the window is not falsely reported.

Audit read helpers and `/api/audit/records?table=<audit_table>` return
`prev_hash` and `row_hash` as lowercase hex strings so third parties can
recompute hashes outside the system without needing database-native `BYTEA`
handling.

Repair is intentionally manual. Re-hashing a changed row without investigation
would destroy the evidence that the chain is designed to preserve.
