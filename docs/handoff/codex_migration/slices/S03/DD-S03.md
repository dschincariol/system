Slice ID: S03
Goal: Extract the storage-owned live-ingestion schema family out of `engine/runtime/storage.py` without changing runtime behavior or schema contracts.

In scope:
- engine/runtime/storage.py
- engine/runtime/storage_live_ingestion_schema.py
- tests/test_storage_contracts.py
- tests/test_sqlite_contention_relief.py
- tests/test_ingestion_runtime_reliability.py
- tests/test_trade_lifecycle_regressions.py

Out of scope:
- engine/runtime/jobs/repair_schema.py
- engine/runtime/storage_pg_prices.py
- engine/runtime/timescale_client.py
- execution, portfolio, model, and dashboard schema families
- schema version changes

Required reading:
- engine/runtime/storage.py
- engine/runtime/jobs/repair_schema.py
- tests/test_storage_contracts.py
- tests/test_sqlite_contention_relief.py
- tests/test_ingestion_runtime_reliability.py
- tests/test_trade_lifecycle_regressions.py

Required changes:
- No edits.
- Identify the smallest schema family that can be extracted cleanly from `storage.py` in one slice.
- Confirm the exact DDL ownership contract enforced by `tests/test_storage_contracts.py`.
- Determine whether external callers require the existing `_ensure_*` names to remain in `storage.py`.
- Produce the exact `S03` touch set and stop conditions.

Required verification:
- none during DD

Acceptance criteria:
- The DD output names one bounded schema family.
- The DD output names the exact production and test touch set.
- The DD output states whether wrapper compatibility in `storage.py` is required.

Stop and report if:
- The extraction needs schema-version changes.
- The extraction needs `repair_schema.py` changes.
- The extraction expands beyond one schema family.

## DD Findings

- Approved schema family: storage-owned live-ingestion tables only.
  - `prices`
  - `price_quotes`
  - `price_quotes_raw`
  - `price_provider_health`
  - `ingestion_pipeline_health`
  - `price_feed_lock`
  - `options_symbol_ingestion_state`
- Approved production touch set:
  - `engine/runtime/storage.py`
  - `engine/runtime/storage_live_ingestion_schema.py`
- Approved test touch set:
  - `tests/test_storage_contracts.py`
- Wrapper compatibility is required.
  - `engine/runtime/price_router.py` imports `_ensure_price_quotes_schema` and `_ensure_price_quotes_raw_schema` from `storage.py`.
  - `engine/runtime/telemetry_append_buffer.py` also relies on the storage-owned raw-quote schema helper path.
  - `storage.py` must keep the `_ensure_*` function names and delegate to the extracted module instead of deleting those entry points.
- `tests/test_storage_contracts.py` already treats the live-ingestion family as a coherent ownership unit, so the slice can stay bounded by moving DDL ownership to a dedicated module and updating the repo contract accordingly.
- `engine/runtime/jobs/repair_schema.py` stays out of scope.
  - It remains an allowed legacy DDL owner for `prices` in the repo contract, but `S03` does not need to move or refactor that job.
- No schema-version increment is required for this extraction-only slice.
