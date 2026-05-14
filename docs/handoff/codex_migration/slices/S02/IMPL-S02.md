```text
Slice ID: S02
Goal: Introduce one shared `TimeseriesWritePolicy` surface for immediate vs deferred ingest writes and route the current ingest hot-path code through it.

In scope:
- engine/runtime/timeseries_write_policy.py
- engine/runtime/price_router.py
- engine/data/poll_prices.py
- engine/runtime/ingestion_status.py
- tests/test_sqlite_contention_relief.py
- tests/test_ingestion_runtime_reliability.py
- tests/test_timescale_integration_hooks.py

Out of scope:
- engine/runtime/storage.py
- engine/runtime/storage_pg_prices.py
- engine/runtime/timescale_client.py
- services/data_source_manager.py
- dashboard_server.py
- execution, model, and UI files

Required reading:
- engine/runtime/price_router.py
- engine/data/poll_prices.py
- engine/runtime/ingestion_status.py
- tests/test_sqlite_contention_relief.py
- tests/test_ingestion_runtime_reliability.py
- tests/test_timescale_integration_hooks.py

Required changes:
- Add a shared `TimeseriesWritePolicy` helper or interface that centralizes the ingest hot-path write/defer decisions.
- Replace duplicated env parsing and branch logic in `price_router.py` and `poll_prices.py` with that shared policy.
- Route the chosen buffering boundary through the same policy.
- Preserve current `S01` behavior:
  - canonical prices and quotes may remain immediate unless explicit cutover flags disable them
  - raw quote evidence is deferred by default
  - provider-health and ingestion-pipeline-health writes remain deferred on the hot path
- Do not add placeholder code, unrelated refactors, or schema changes.

Required verification:
- python -m pytest tests/test_sqlite_contention_relief.py -q
- python -m pytest tests/test_ingestion_runtime_reliability.py -q
- python -m pytest tests/test_timescale_integration_hooks.py -q

Acceptance criteria:
- One shared policy surface owns the ingest hot-path write/defer decisions.
- `price_router.py` and `poll_prices.py` no longer duplicate that policy logic locally.
- The buffered behavior proven by `S01` remains intact.
- No unrelated file edits.

Stop and report if:
- The approved touch set needs to expand.
- The implementation requires `storage.py` or schema edits.
- A failing verification indicates the slice really belongs in `S03`.
```
