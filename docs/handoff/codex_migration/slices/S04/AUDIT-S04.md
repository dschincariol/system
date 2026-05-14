Slice ID: S04
Goal: Independently audit the `S04` diff and verify that read-routing hardening made validated Timescale reads primary without leaking into caller, schema, or write-path changes.

In scope:
- engine/runtime/price_read_router.py
- engine/runtime/telemetry_read_router.py
- tests/test_price_migration_validation.py
- tests/test_telemetry_read_routing.py
- the `S04` diff only
- tests/test_timescale_integration_hooks.py

Out of scope:
- validation-builder refactors
- API, dashboard, or health caller changes
- schema, write-path, and migration changes

Required reading:
- the `S04` diff
- engine/runtime/price_read_router.py
- engine/runtime/telemetry_read_router.py
- tests/test_price_migration_validation.py
- tests/test_telemetry_read_routing.py
- tests/test_timescale_integration_hooks.py

Required changes:
- No code changes unless a concrete finding requires a follow-up patch.
- Review the implementation as a code audit.
- Findings must come first.
- Explicitly check for:
  - default backend behavior now preferring validated Timescale reads
  - explicit SQLite override still working
  - unchanged fetch-time fallback behavior
  - no caller or validation-builder edits hidden in the slice

Required verification:
- python -m pytest tests/test_price_migration_validation.py -q
- python -m pytest tests/test_telemetry_read_routing.py -q
- python -m pytest tests/test_timescale_integration_hooks.py -q

Acceptance criteria:
- Findings-first audit output.
- Explicit statement whether `S04` is complete or needs follow-up.
- Verification results included.

Stop and report if:
- The implementation diff touches files outside the approved slice.
- The router change regresses explicit SQLite override or runtime fallback behavior.
- Any required fix expands the slice beyond the read-router boundary.

## Audit Result

- Findings: none.
- `engine/runtime/price_read_router.py` and `engine/runtime/telemetry_read_router.py` now normalize missing backend configuration to `auto`, which prefers validated Timescale reads and only returns SQLite immediately for the explicit `sqlite` override or when Timescale is not ready.
- Validation builders stayed unchanged, so the cutover gate remains isolated to the existing snapshot contracts.
- API, health, and service callers stayed unchanged and continue consuming the same router helpers.
- Verification rerun:
  - `python -m pytest tests/test_price_migration_validation.py -q`
  - `python -m pytest tests/test_telemetry_read_routing.py -q`
  - `python -m pytest tests/test_timescale_integration_hooks.py -q`
