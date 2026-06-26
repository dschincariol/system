# Schema / Table-Classification Remediation Deep-Dive Prompts

Two independent, self-contained codex prompts. Each was surfaced by a documentation audit
against HEAD that found the docs had been corrected to describe reality while the underlying
code remained wrong. Run them separately.

---

## SCHEMA-01 — `risk_var_backtest_results` silently materializes as a regular table (declared hypertable never converts) (P1)

ROLE: Storage / Timescale schema engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change.

PROBLEM. The VaR/CVaR exception-evidence table is declared as a hypertable but never becomes one, so its chunking, compression, and retention policy is silently unenforced.

- `engine/runtime/schema/table_classification.py:658` registers `TABLE_CLASS["risk_var_backtest_results"] = _h(chunk="30 days", compress_after="90 days", retain="5 years", segmentby=("confidence_level",), ...)` but **omits `time_column`**. The `_h(...)` helper (`engine/runtime/schema/table_classification.py:91`) defaults `time_column="ts_ms"` (line 97).
- Migration `engine/runtime/schema/migrations/0079_risk_var_backtesting.py:35` creates `risk_var_backtest_results` with `forecast_ts_ms BIGINT NOT NULL` (line 38), `realized_ts_ms` (line 39), and `created_ts_ms` (line 58). There is **no `ts_ms` column**.
- The hypertable application loop in `engine/runtime/schema/migrations/0002_hypertables.py` `up()` (lines ~383-387) iterates `TABLE_CLASS` and calls `_create_hypertable`, which returns early at `engine/runtime/schema/migrations/0002_hypertables.py:293-294` (`if not _column_exists(conn, table_name, time_column): return`) because `ts_ms` does not exist. `_enable_compression` / `_enable_retention` (lines ~389-393) likewise no-op because the table is not a hypertable.
- Net effect: the declared 30-day chunk / 90-day compression / 5-year retention / `segmentby=confidence_level` policy is NOT applied. VaR/CVaR exception evidence accumulates in a plain, unchunked, uncompressed, never-retained Postgres table. `docs/Database_Schema.md` was already corrected to note "declared; materializes as regular," so the docs and code currently disagree by design — close the gap in code.

REQUIRED CHANGE.
1. In `engine/runtime/schema/table_classification.py:658`, add `time_column="forecast_ts_ms"` to the `_h(...)` call for `risk_var_backtest_results` so the declared hypertable matches the actual time column created by migration 0079.
2. Convert ALREADY-DEPLOYED databases. Determine the hypertable application/re-run model: whether `0002_hypertables.up()` is re-applied after later-numbered migrations (e.g. how `garch_vol_forecasts` from 0080 became a real hypertable) or whether conversion is attempted only once. If late-created tables are not re-converted automatically, add a new forward migration (next sequential number after `0083`) that **idempotently** converts `risk_var_backtest_results` to a hypertable on `forecast_ts_ms` and applies the compression/retention/segmentby policy, guarded by table-exists / column-exists / already-hypertable checks (mirror the helpers in `0002_hypertables.py`). Confirm BOTH fresh installs and existing installs end with a correctly-configured hypertable.
3. Add an enforcement guard so this class of bug cannot recur: for every `Hypertable` spec in `TABLE_CLASS`, assert its declared `time_column` actually exists in the materialized schema. Extend `tests/test_schema_hypertable_creation.py` and/or add a check in `tools/validate_repo.py` (or a schema self-check at migration time) that cross-references each hypertable's `time_column` against the columns its `CREATE TABLE` produces. A declared hypertable whose `time_column` is absent must fail loudly rather than downgrade to a regular table.
4. Update `docs/Database_Schema.md` and `docs/README_DATABASE_MAP.md` to state that `risk_var_backtest_results` is a genuine hypertable on `forecast_ts_ms` once the fix lands.

VERIFY. On a Postgres/Timescale-backed instance, `risk_var_backtest_results` appears in `timescaledb_information.hypertables` with time dimension `forecast_ts_ms` and the configured chunk/compression/retention; `tests/test_schema_hypertable_creation.py`, `tests/test_schema_classification.py`, `tests/test_schema_compression_policy.py`, and `tests/test_schema_retention_policy.py` pass; the new guard fails if `time_column` is reverted or mismatched.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.

---

## SCHEMA-02 — Three `learned_alpha_decay_*` tables ship unclassified through a coverage-gate blind spot (P1)

ROLE: Storage / schema-governance engineer. TARGET: `/home/david/gitsandbox/system/system`. Read-then-change.

PROBLEM. Three real runtime tables exist with no entry in the schema source of truth, and the gate that is supposed to forbid this cannot see them.

- `engine/strategy/learned_alpha_decay.py` creates three tables: `learned_alpha_decay_runs` (`RUNS_TABLE`, line 32; `CREATE TABLE IF NOT EXISTS {RUNS_TABLE}` at line 135, `ts_ms INTEGER NOT NULL` at line 137), `learned_alpha_decay_estimates` (`ESTIMATES_TABLE`, line 33; created at line 145), and `learned_alpha_decay_age_edges` (`AGE_EDGES_TABLE`, line 34; created at line 170).
- None are registered in `engine/runtime/schema/table_classification.py` `TABLE_CLASS` (grep returns zero hits). They therefore carry no declared classification, retention, or compression governance, and are absent from the classification source of truth even though they are already documented in `docs/Database_Schema.md:103-105` (docs are ahead of code).
- The classification-coverage gate `tests/test_schema_classification.py:79-81` is supposed to fail on any unclassified `CREATE TABLE` and it does scan `engine/` and `ops/` via `_source_create_table_names()`, but it **skips any create-table name containing `{` or `}`** (`tests/test_schema_classification.py:73`). Because these tables are created with f-string/variable names (`CREATE TABLE IF NOT EXISTS {RUNS_TABLE}`), the gate cannot discover them — the blind spot that let three real tables ship unclassified.

REQUIRED CHANGE.
1. Register all three tables in `TABLE_CLASS` (`engine/runtime/schema/table_classification.py`) as `Regular` via `_r(...)`, matching their documented intent in `docs/Database_Schema.md:103-105` (low write rate; `learned_alpha_decay_runs` → latest-run / training-audit lookup; `learned_alpha_decay_estimates` → latest-cohort lookup from execution/portfolio/champion paths; `learned_alpha_decay_age_edges` → run/cohort drill-down). Confirm `Regular` (not `Hypertable`) is correct: these are bounded operational tables with AUTOINCREMENT / composite primary keys, not append-mostly time series.
2. Close the coverage blind spot so this cannot recur. Choose the optimal enforcement (prefer both for defense in depth):
   a. Extend `_source_create_table_names()` (and any production schema scanner) to resolve simple module-level string constants used in `CREATE TABLE IF NOT EXISTS {VAR}` so variable-named tables are discovered automatically; and/or
   b. Add the three names to `SOURCE_DECLARED_TABLES` (`engine/runtime/schema/table_classification.py:1048`) — the existing escape hatch the gate honors — AND make the scanner FAIL rather than silently skip when it encounters a `CREATE TABLE ... {VAR}` whose resolved name is not covered by a classification, so future variable-named tables cannot slip through.
3. Verify `docs/Database_Schema.md` already lists all three (it does, lines 103-105) so `test_database_schema_doc_lists_every_classified_table` stays green; update `docs/README_DATABASE_MAP.md` if it omits any of them.

VERIFY. `tests/test_schema_classification.py` now discovers and requires all three tables (removing any from `TABLE_CLASS` makes the gate fail); each resolves to `Regular`; the scanner no longer silently skips variable-named `CREATE TABLE` statements; `python tools/validate_docs.py` stays green.

After implementation, audit your own work. Show exact files changed, why each change is required, and how the fix is enforced in production code rather than only tests/docs. Update documentation to reflect changes made. Run targeted tests for the changed behavior, then run `git status --short --untracked-files=all`, `python tools/git_worktree_triage.py`, and the relevant validators. Capture exact exit codes and key output lines. If any requirement is not fully implemented, say NO-GO and explain the remaining work.
