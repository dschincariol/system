# Codex DB Prompt 08 — Tamper-Evident Audit Hash Chain

You are working in a Python systematic trading system whose audit
ledgers (`trade_attribution_ledger`, `kill_switch_audit`,
`execution_mode_audit`, `execution_policy_audit`,
`position_reconcile_audit`, `promotion_statistical_evidence` from
prompt 01 of the original 1–10 series, `decision_log` once it lands)
collectively constitute the audit trail a regulator, an LP, or even
the operator's own future self will rely on to reconstruct what
happened. Today they are append-only by application convention but
not by enforcement: a row could be silently mutated and no one would
know. This prompt makes the audit ledgers **tamper-evident** via a
simple per-table hash chain, and stands up a verifier that proves the
chain is intact.

This is not encryption and it is not access control; it is **integrity
attestation**. A row whose hash chain validates is one that has not
been altered since it was written.

## Goal

1. Every audit-class table grows two columns: `prev_hash BYTEA NULL`,
   `row_hash BYTEA NOT NULL`.
2. The storage layer computes both at insert time:
   `row_hash = sha256(prev_hash || canonical_serialization(row_minus_hashes))`.
3. A periodic verifier walks each chain and reports any divergence to
   `audit_chain_findings`.
4. A small admin tool reproduces a row's hash from its content for
   manual investigation.
5. Hashes are surfaced in the audit-record API so external auditors
   can verify off-system.

## Files to read first (read-only)

- `engine/strategy/promotion_audit.py` — first audit table built into
  the new system; archetype.
- `engine/execution/trade_attribution_ledger.py` — busiest audit
  table.
- `engine/execution/kill_switch.py` — writes to `kill_switch_audit`.
- `engine/execution/execution_policy_engine.py` — writes to
  `execution_mode_audit`, `execution_policy_audit`.
- `engine/execution/position_reconcile.py` — writes to
  `position_reconcile_audit`.
- `engine/runtime/storage_pg.py` (prompt 02) — to understand the
  insert path that this prompt will hook into.
- `engine/runtime/schema/table_classification.py` — to enumerate
  every table classified as an audit ledger (this prompt only
  applies to those).

## Files to create

- `engine/audit/__init__.py`
- `engine/audit/canonical.py` — canonical row serialization.
  Deterministic JSON: keys sorted, no insignificant whitespace,
  numbers in canonical form (no `1.0` vs `1`), UTC ISO-8601 for
  timestamps. The function is the single point of truth; tests pin
  exemplar rows to exemplar bytes.
- `engine/audit/hashing.py` — `compute_row_hash(prev_hash: bytes |
  None, row: dict) -> bytes`. SHA-256.
- `engine/audit/chain.py` — `append_chain_row(table: str, row: dict,
  conn) -> ChainResult`. Acquires `pg_advisory_xact_lock(hash(table))`
  inside the transaction so concurrent writers cannot interleave;
  reads the previous `row_hash`, computes the new one, inserts.
- `engine/audit/verifier.py` — walks a chain end-to-end (or in
  bounded windows for large tables) and emits findings.
- `engine/audit/cli.py` — `python -m engine.audit verify
  [--table T] [--from-id N] [--to-id N]`. Prints per-table
  pass/fail summary; non-zero exit on any failure.
- `engine/runtime/schema/migrations/0007_audit_chain.py` — adds
  `prev_hash`, `row_hash` columns to every audit table; backfills
  the chain across any historical rows (in greenfield, this is a
  no-op or near-no-op).
- `engine/runtime/schema/migrations/0008_audit_findings.py` —
  `audit_chain_findings(id BIGSERIAL PK, ts TIMESTAMPTZ DEFAULT
  now(), table_name TEXT, row_id BIGINT, finding TEXT, expected_hash
  BYTEA, actual_hash BYTEA, payload_excerpt JSONB)`.
- `engine/strategy/jobs/audit_chain_verify.py` — daily job; runs
  the verifier against each audit table; emits `runtime_metrics`
  rows for "rows verified", "findings", "max chain length".
- `tests/test_audit_canonical.py`
- `tests/test_audit_hashing.py`
- `tests/test_audit_chain_append.py`
- `tests/test_audit_chain_verifier.py`
- `tests/test_audit_chain_tamper_detection.py` — deliberately
  mutates a row and asserts the verifier produces exactly one
  finding at the right position.
- `tests/test_audit_chain_concurrent_writers.py` — N concurrent
  writers; resulting chain is a single linear sequence (no forks).

## Files to modify

- Every audit-table writer (the modules listed under "read first")
  switches its insert call from raw `executemany` to
  `chain.append_chain_row(...)`. The function signature is identical
  to the existing inserts plus an additional connection parameter
  (already present).
- `engine/runtime/schema/table_classification.py` — mark the audit
  tables with an `audit=True` flag so the migration generator and
  the verifier can find them programmatically.

## Implementation plan

1. **Canonical serialization is the schelling point.** Pick it once;
   the tests pin the byte representation of exemplar rows so future
   changes are intentional. RFC 8785 (JCS) is the reference; a
   simplified subset (sorted keys, ASCII, no insignificant
   whitespace) is sufficient and easier to implement.
2. **Hashing.** SHA-256. Bytes, not hex. Persist in `BYTEA`. The
   admin tool prints hex for human use.
3. **Append API.** A single call site per row:
   ```python
   chain.append_chain_row(
       table="trade_attribution_ledger",
       row=row_dict_minus_hashes,
       conn=tx,
   )
   ```
   It opens an advisory lock keyed by table name (so concurrent
   writers serialize per-table), reads the latest `row_hash` for
   that table inside the transaction, computes the new one,
   inserts the row including both hash columns. The lock is
   transaction-scoped; throughput per-audit-table is limited by
   row insert rate, which is fine — audit writes are not on the
   hot path.
4. **Verifier.** Walks ordered by `(ts, id)`. Recomputes each row's
   hash and compares. On mismatch, writes a finding and continues
   (do not stop — we want the full set of bad rows).
5. **Backfill.** In greenfield, audit tables are empty or near-empty;
   the migration computes the chain from row 1. If the migration is
   ever applied to a populated DB, it grandfathers existing rows by
   computing their hashes in order during the migration and refusing
   to mark the migration complete unless every row hashes
   successfully.
6. **External verification.** The hash output is reproducible by any
   third party with the row content and the chain head; document
   the canonical serialization in `docs/Audit_Chain_Spec.md` so an
   auditor can re-implement and verify.

## Performance targets

- Adding a row to `trade_attribution_ledger` via the chain API takes
  **< 5 ms** end-to-end including the advisory lock and the
  previous-hash read.
- Verifying a 1 000 000-row chain takes **< 60 s**.
- The daily verifier job's CPU cost is **< 2%** of one core for a
  typical day's audit volume.

## Acceptance criteria

- [ ] Every audit table has `prev_hash` and `row_hash` columns; the
      latter is `NOT NULL`.
- [ ] No audit-table insert exists outside `chain.append_chain_row(...)`
      (lint-tested by AST scan).
- [ ] Two concurrent writers to the same table produce a single
      linear chain (no forks; the second writer waits on the
      advisory lock).
- [ ] A row deliberately mutated post-insert produces exactly one
      finding at the right position; downstream rows produce
      findings until the chain is repaired (which we do **not**
      automate — repair is a human decision).
- [ ] Canonical serialization is byte-pinned by exemplar tests
      across at least 10 rows of varied shape.
- [ ] The CLI exits non-zero on any unverified chain and zero on a
      clean walk.
- [ ] `docs/Audit_Chain_Spec.md` describes the canonical format
      well enough that a third party could re-implement it.

## Test plan

- `tests/test_audit_canonical.py` — exemplar rows → exemplar bytes.
- `tests/test_audit_hashing.py` — known input → known SHA-256
  output.
- `tests/test_audit_chain_append.py` — append three rows; manually
  recompute; chain matches.
- `tests/test_audit_chain_verifier.py` — clean chain → no
  findings; correct exit.
- `tests/test_audit_chain_tamper_detection.py` — modify row N's
  payload; verifier flags row N specifically.
- `tests/test_audit_chain_concurrent_writers.py` — 8 threads, 100
  rows each, on the same audit table; final chain has 800 linearly
  ordered hashes with no gaps or forks.

Run: `pytest -q tests/test_audit_canonical.py tests/test_audit_hashing.py
tests/test_audit_chain_append.py tests/test_audit_chain_verifier.py
tests/test_audit_chain_tamper_detection.py
tests/test_audit_chain_concurrent_writers.py`

## Out of scope

- Cryptographic signatures (Merkle trees, blockchains, HSM-backed
  signing). Hash chains are sufficient for tamper evidence at this
  scale; signatures answer a different question (who signed) at a
  much higher operational cost.
- Auto-repair of broken chains. A finding is a human decision;
  silently re-hashing a tampered row defeats the entire mechanism.
- Encryption of audit row bodies. That is a different control; this
  prompt does integrity, not confidentiality.
- Applying the chain to non-audit tables. State and registry tables
  are mutable by design; chains there are nonsense.
