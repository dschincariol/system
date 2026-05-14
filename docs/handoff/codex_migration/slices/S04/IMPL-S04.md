Slice ID: S04
Goal: Make the price and telemetry read routers prefer validated Timescale reads by default while preserving explicit SQLite override and runtime fallback behavior.

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
- schema, writer, or migration changes
- dashboard orchestration or execution changes

Required reading:
- engine/runtime/price_read_router.py
- engine/runtime/telemetry_read_router.py
- tests/test_price_migration_validation.py
- tests/test_telemetry_read_routing.py
- tests/test_timescale_integration_hooks.py

Required changes:
- Change both routers so the default backend mode is `auto` instead of `sqlite`.
- Keep `sqlite` as an explicit operator override.
- Keep validated Timescale gating in the router layer; do not modify the validation snapshot builders.
- Normalize missing or invalid backend values to `auto`.
- Extend focused tests to cover:
  - automatic Timescale preference when validation passes
  - explicit SQLite override remaining primary
  - automatic SQLite fallback when Timescale is unavailable
- Do not add placeholder code, broaden the slice into callers, or change fetch-time fallback semantics.

Required verification:
- python -m pytest tests/test_price_migration_validation.py -q
- python -m pytest tests/test_telemetry_read_routing.py -q
- python -m pytest tests/test_timescale_integration_hooks.py -q

Acceptance criteria:
- Validated Timescale reads are the primary default path in both routers.
- SQLite stays available only as an explicit override or runtime fallback.
- Existing API and health surfaces continue to consume the same router helpers unchanged.
- Focused routing and Timescale hook tests stay green.
- No unrelated file edits.

Stop and report if:
- The change requires edits outside the approved router/test boundary.
- Validation builders need contract changes.
- The slice expands into schema or write-path behavior.
