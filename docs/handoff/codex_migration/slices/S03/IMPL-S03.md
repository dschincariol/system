Slice ID: S03
Goal: Move the storage-owned live-ingestion schema family out of `engine/runtime/storage.py` into a dedicated runtime module without changing runtime behavior.

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
- schema-version changes

Required reading:
- engine/runtime/storage.py
- tests/test_storage_contracts.py
- tests/test_sqlite_contention_relief.py
- tests/test_ingestion_runtime_reliability.py
- tests/test_trade_lifecycle_regressions.py

Required changes:
- Add a dedicated runtime module for the storage-owned live-ingestion schema family.
- Move the live-ingestion DDL and migration logic into that new module.
- Keep the existing `_ensure_*` functions in `storage.py` as compatibility wrappers that delegate to the extracted module.
- Update the storage contract test so repo DDL ownership points at the new module instead of `storage.py` for this schema family.
- Do not change schema versions, add placeholder code, or broaden the extraction beyond the approved tables.

Required verification:
- python -m pytest tests/test_storage_contracts.py -q
- python -m pytest tests/test_sqlite_contention_relief.py -q
- python -m pytest tests/test_ingestion_runtime_reliability.py -q
- python -m pytest tests/test_trade_lifecycle_regressions.py -q

Acceptance criteria:
- Live-ingestion DDL ownership is explicit in a dedicated module.
- `storage.py` no longer owns the extracted DDL bodies directly.
- Existing callers still work through `storage.py` compatibility wrappers.
- Storage contract and ingestion/runtime regressions stay green.
- No unrelated file edits.

Stop and report if:
- The extraction requires `repair_schema.py` changes.
- The extraction requires schema-version changes.
- The approved touch set expands beyond the live-ingestion schema family.
