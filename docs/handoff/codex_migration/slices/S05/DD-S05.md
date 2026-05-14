Slice ID: S05
Goal: Introduce a runtime `LiveCache` boundary for live price and feature snapshots so the backing store can move beyond process-local memory, with Redis support gated behind configuration and safe fallback.

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
- inference, regime, or API caller refactors
- schema or write-path changes

Required reading:
- engine/data/price_cache.py
- engine/data/feature_store.py
- engine/runtime/price_cache.py
- engine/runtime/storage.py
- tests/test_feature_store.py
- tests/test_inference_engine.py
- tests/test_timescale_integration_hooks.py
- tests/test_sqlite_contention_relief.py

Required changes:
- No edits.
- Determine the smallest boundary that removes direct ownership of live cache state from `engine.data.price_cache` and `engine.data.feature_store`.
- Identify whether existing runtime callers can stay unchanged.
- Determine whether Redis is already present as a dependency or must remain optional.
- Produce the exact `S05` touch set, acceptance criteria, and stop conditions.

Required verification:
- none during DD

Acceptance criteria:
- The DD output names one bounded cache-boundary change.
- The DD output names the exact production and test touch set.
- The DD output states whether caller or dependency changes are required.

Stop and report if:
- The slice requires schema changes.
- The slice requires caller-wide refactors.
- The slice expands into the strategy feature-store sidecar.

## DD Findings

- Live cache ownership is currently split across two process-local globals:
  - `engine.data.price_cache` owns the live market price snapshot cache.
  - `engine.data.feature_store` owns the live market feature snapshot cache.
- Existing runtime callers already sit behind stable APIs and do not need to change.
  - Price reads flow through `engine.runtime.price_cache`.
  - Feature reads flow through `engine.data.feature_store.get_live_features`, `get_features`, and `get_features_asof`.
- The strategy-side `engine.strategy.feature_store` is a different bounded system and must stay out of scope for `S05`.
- There is no Redis dependency currently present in `requirements.txt`, so Redis support must remain optional in this slice.
  - Explicit Redis mode should degrade to the in-memory backend when the dependency or URL is missing.
  - Default behavior should remain safe for local dev and current tests.
- Approved production touch set:
  - `engine/runtime/live_cache.py`
  - `engine/data/price_cache.py`
  - `engine/data/feature_store.py`
- Approved test touch set:
  - `tests/test_live_cache.py`
  - `tests/test_feature_store.py`
- Verification-only suites:
  - `tests/test_inference_engine.py`
  - `tests/test_timescale_integration_hooks.py`
  - `tests/test_sqlite_contention_relief.py`
- The clean slice is to move cache ownership behind a runtime interface with:
  - default in-memory backend
  - optional Redis backend
  - unchanged public caller APIs
  - cache/backend health surfaced through existing snapshot helpers
