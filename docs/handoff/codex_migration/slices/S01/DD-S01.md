```text
Slice ID: S01
Goal: Keep raw quote evidence, provider health, and ingestion pipeline health fully deferred so the live-stream path only sync-writes canonical prices and quotes.

In scope:
- engine/runtime/price_router.py
- engine/runtime/telemetry_append_buffer.py
- engine/data/poll_prices.py
- tests/test_sqlite_contention_relief.py

Out of scope:
- engine/runtime/storage.py
- engine/runtime/timescale_client.py
- engine/runtime/storage_pg_prices.py
- dashboard_server.py
- model, execution, and UI files

Required reading:
- engine/runtime/price_router.py
- engine/runtime/telemetry_append_buffer.py
- engine/data/poll_prices.py
- tests/test_sqlite_contention_relief.py
- docs/FAILURE_MODES.md

Required changes:
- Identify every synchronous SQLite touch on the live-stream path related to raw quote evidence.
- Confirm whether provider health and ingestion pipeline health already flush through telemetry buffers.
- Produce an exact touch set and the smallest safe implementation path.
- Do not edit files.

Required verification:
- none during DD

Acceptance criteria:
- The DD output names the exact files that need edits.
- The DD output names the precise regression that proves the bug.
- The DD output states the stop conditions clearly.

Stop and report if:
- More than 3 production modules need edits.
- The fix requires schema changes outside the existing quote/telemetry family.
- The regression cannot be explained by the current hot-path code.
```
