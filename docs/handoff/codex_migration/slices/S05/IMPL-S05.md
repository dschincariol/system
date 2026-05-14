Slice ID: S05
Goal: Replace direct process-local ownership of live market-data price and feature caches with a runtime `LiveCache` boundary that supports optional Redis backing while preserving existing callers.

In scope:
- engine/runtime/live_cache.py
- engine/data/price_cache.py
- engine/data/feature_store.py
- tests/test_live_cache.py
- tests/test_feature_store.py
- tests/test_inference_engine.py
- tests/test_timescale_integration_hooks.py
- tests/test_sqlite_contention_relief.py

Out of scope:
- engine/runtime/storage.py
- engine/runtime/health.py
- engine/runtime/price_cache.py
- engine/strategy/feature_store.py
- schema, write-path, or API changes
- requirements or packaging changes

Required reading:
- engine/data/price_cache.py
- engine/data/feature_store.py
- engine/runtime/price_cache.py
- tests/test_feature_store.py
- tests/test_inference_engine.py
- tests/test_timescale_integration_hooks.py
- tests/test_sqlite_contention_relief.py

Required changes:
- Add a runtime `LiveCache` interface module with:
  - default in-memory backend
  - optional Redis backend selected by environment
  - explicit fallback to memory when Redis is unavailable or unconfigured
- Move `engine.data.price_cache` cache ownership behind the new interface without changing its public APIs.
- Move `engine.data.feature_store` cache ownership behind the new interface without changing its public APIs.
- Surface backend information through the existing cache snapshot helpers.
- Add focused tests for:
  - default backend selection
  - explicit Redis fallback behavior
  - price/feature snapshot helpers surfacing backend selection
- Do not refactor callers, touch the strategy feature-store sidecar, or change schemas.

Required verification:
- python -m pytest tests/test_live_cache.py -q
- python -m pytest tests/test_feature_store.py -q
- python -m pytest tests/test_timescale_integration_hooks.py -q
- python -m pytest tests/test_sqlite_contention_relief.py -q -k "feature_store_can_skip_sqlite_writes_during_cutover"
- python -m pytest tests/test_inference_engine.py -q -k "weighted_ensemble or degrades_to_single_member"

Acceptance criteria:
- Live market-data cache ownership is routed through a runtime interface instead of module-local globals.
- Existing price and feature cache callers stay unchanged.
- Redis support is configurable but safely falls back to memory when unavailable.
- Focused cache, feature-store, inference, and hook regressions stay green.
- No unrelated file edits.

Stop and report if:
- The slice requires storage or health caller changes.
- The slice requires dependency or packaging changes to stay safe.
- The slice expands into the strategy feature-store or broader inference refactors.
