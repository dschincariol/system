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

Required changes:
- Edit only the approved touch set.
- Prefer a `price_router.py`-only fix if the failing regression is caused by schema/bootstrap work leaking onto the immediate raw path.
- Keep telemetry buffer ownership unchanged.
- Do not add placeholder code, broad cleanup, or unrelated formatting changes.

Required verification:
- python -m pytest tests/test_sqlite_contention_relief.py -q
- python -m pytest tests/test_timescale_integration_hooks.py -q
- python -m pytest tests/test_trade_lifecycle_regressions.py -q
- python -m pytest tests/test_ingestion_runtime_reliability.py -q

Acceptance criteria:
- `tests/test_sqlite_contention_relief.py` is green.
- Raw quote evidence no longer produces immediate SQLite writes on the live-stream path before explicit telemetry buffer flush.
- Provider health and ingestion pipeline health remain deferred.
- No unrelated file edits.

Stop and report if:
- Fixing the regression requires changes outside the approved touch set.
- A schema change outside the quote/telemetry family appears necessary.
- The verification suite surfaces a new failure that changes the slice scope.
```
