Slice ID: S04
Goal: Harden the price and telemetry read routers so validated Timescale reads are the primary cutover path and SQLite remains an explicit override or runtime fallback only.

In scope:
- engine/runtime/price_read_router.py
- engine/runtime/telemetry_read_router.py
- tests/test_price_migration_validation.py
- tests/test_telemetry_read_routing.py
- tests/test_timescale_integration_hooks.py

Out of scope:
- engine/runtime/price_migration_validation.py
- engine/runtime/telemetry_migration_validation.py
- engine/api/*
- engine/runtime/storage.py
- engine/runtime/storage_pg_prices.py
- engine/runtime/timescale_client.py
- schema, write-path, or dashboard orchestration changes

Required reading:
- engine/runtime/price_read_router.py
- engine/runtime/telemetry_read_router.py
- engine/runtime/price_migration_validation.py
- engine/runtime/telemetry_migration_validation.py
- engine/api/api_dashboard_reads.py
- engine/api/api_read.py
- engine/api/api_system.py
- engine/runtime/health.py
- tests/test_price_migration_validation.py
- tests/test_telemetry_read_routing.py

Required changes:
- No edits.
- Determine whether the router boundary can switch from SQLite-first to validated-Timescale-first without changing callers.
- Determine whether the existing validation snapshots already provide the gating signal needed for automatic cutover.
- Define the exact touch set, acceptance criteria, and stop conditions for the slice.

Required verification:
- none during DD

Acceptance criteria:
- The DD output names one bounded router policy change.
- The DD output names the exact production and test touch set.
- The DD output states whether caller changes are required.

Stop and report if:
- The slice requires schema or write-path changes.
- The slice requires changes in API, dashboard, or health callers.
- The slice expands beyond the router boundary and focused routing tests.

## DD Findings

- Both read routers still default `_READ_BACKEND` to `sqlite`, so Timescale reads never become the primary path unless an operator explicitly forces `*_READ_BACKEND=timescale`.
- Existing validation snapshots already expose the exact gating signal needed for automatic cutover.
  - `engine/runtime/price_migration_validation.py:get_price_migration_validation_snapshot`
  - `engine/runtime/telemetry_migration_validation.py:get_telemetry_migration_validation_snapshot`
- Caller changes are not required.
  - Price reads already flow through `engine.runtime.price_read_router` from `engine/api/api_dashboard_reads.py` and `engine/api/api_market.py`.
  - Telemetry reads already flow through `engine.runtime.telemetry_read_router` from `engine/api/api_read.py`, `engine/api/api_system.py`, `engine/runtime/health.py`, `engine/runtime/metrics_store.py`, and `services/data_source_manager.py`.
- Approved production touch set:
  - `engine/runtime/price_read_router.py`
  - `engine/runtime/telemetry_read_router.py`
- Approved test touch set:
  - `tests/test_price_migration_validation.py`
  - `tests/test_telemetry_read_routing.py`
- `tests/test_timescale_integration_hooks.py` should remain in verification even though it is not part of the touch set.
- Keep fetch-time fallback behavior unchanged.
  - `*_READ_FALLBACK_TO_SQLITE` still governs runtime fetch failures.
  - `sqlite` remains a valid explicit operator override.
- Normalizing missing or invalid `*_READ_BACKEND` values to `auto` keeps the change bounded to the router layer and avoids caller awareness of cutover state.
