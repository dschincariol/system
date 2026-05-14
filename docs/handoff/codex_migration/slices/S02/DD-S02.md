```text
Slice ID: S02
Goal: Define one shared `TimeseriesWritePolicy` surface for immediate vs deferred ingest writes, replacing duplicated hot-path write-policy decisions in the current runtime.

In scope:
- engine/runtime/price_router.py
- engine/data/poll_prices.py
- engine/runtime/telemetry_append_buffer.py
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
- engine/runtime/telemetry_append_buffer.py
- engine/runtime/ingestion_status.py
- tests/test_sqlite_contention_relief.py
- tests/test_ingestion_runtime_reliability.py
- tests/test_timescale_integration_hooks.py

Required changes:
- No edits.
- Identify every environment flag and branch currently deciding whether the ingest hot path writes immediately or buffers/defer-writes:
  - raw quote evidence
  - canonical price/quote rows
  - provider auxiliary rows
  - provider-health telemetry
  - ingestion-pipeline-health telemetry
- Determine the smallest safe interface for a shared `TimeseriesWritePolicy`.
- Produce the exact `S02` touch set, including whether `telemetry_append_buffer.py` or `ingestion_status.py` must consume the new policy directly.

Required verification:
- none during DD

Acceptance criteria:
- The DD output names the exact production touch set.
- The DD output names the current duplicated policy sources and the target shared policy owner.
- The DD output states whether the slice can stay within one new interface plus the bounded ingest-write files.

Stop and report if:
- The minimal implementation needs `storage.py` changes.
- The implementation needs `services/data_source_manager.py` changes.
- The slice expands beyond one new policy module plus the ingest-write files.
- A schema change appears necessary.

## DD Findings

- Approved production touch set:
  - `engine/runtime/timeseries_write_policy.py`
  - `engine/runtime/price_router.py`
  - `engine/data/poll_prices.py`
  - `engine/runtime/ingestion_status.py`
- `engine/runtime/telemetry_append_buffer.py` does not need direct edits in `S02`. It is the deferred append mechanism, but it is not the caller-side policy owner for this slice.
- `services/data_source_manager.py` still performs separate best-effort source/job status writes on the `poll_prices` path, but that is a control-plane status family rather than the timeseries hot-path family scoped for `S02`.

## Current Duplicated Policy Sources

- `engine/runtime/price_router.py`
  - parses `PRICE_ROUTER_SQLITE_WRITE_ENABLED`
  - parses `PRICE_ROUTER_SQLITE_PRICES_ENABLED`
  - parses `PRICE_ROUTER_SQLITE_QUOTES_ENABLED`
  - parses `PRICE_ROUTER_SQLITE_RAW_ENABLED`
  - parses `PRICE_ROUTER_REQUIRE_ASYNC_DURING_CUTOVER`
  - locally derives `sqlite_write_prices`, `sqlite_write_quotes`, `sqlite_write_raw`, and `async_required`
- `engine/data/poll_prices.py`
  - parses `POLL_PRICES_SYNC_PROVIDER_AUX_SQLITE_WRITE_ENABLED`
  - locally chooses `_persist_provider_auxiliary_rows_sync(...)` vs `_enqueue_provider_auxiliary_rows(...)`
- `engine/runtime/ingestion_status.py`
  - locally decides whether best-effort pipeline-health rows buffer or sync-write via `_should_buffer_pipeline_health_row(...)`

## Policy Surface

- Minimal safe interface:
  - `get_timeseries_write_policy() -> TimeseriesWritePolicy`
  - `TimeseriesWritePolicy.plan_price_router_writes(...)`
  - `TimeseriesWritePolicy.should_buffer_pipeline_health(...)`
  - `TimeseriesWritePolicy.sync_provider_aux_sqlite_write_enabled`
- This keeps `S02` within one new interface plus the bounded ingest-write files, without changing schema ownership or buffer internals.

## Verified Boundaries

- No `storage.py` change is required.
- No schema change is required.
- No `services/data_source_manager.py` change is required for `S02`.
- `S02` can stay inside one new policy module plus the approved ingest-write files.
```
