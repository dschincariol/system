Slice ID: S08
Goal: Isolate live inference behind runtime reader interfaces so `engine.inference_engine` consumes online feature snapshots and model-catalog reads through a bounded runtime boundary instead of importing the concrete feature-store and model-cache modules directly.

In scope:
- engine/runtime/inference_runtime.py
- engine/inference_engine.py
- tests/test_inference_runtime.py
- tests/test_inference_engine.py

Out of scope:
- predictor legacy-fallback policy changes
- model registry schema or artifact-store changes
- feature-store write paths or cache-backend changes
- ensemble weighting behavior changes
- training-service extraction

Required reading:
- engine/inference_engine.py
- engine/runtime/model_cache.py
- engine/data/feature_store.py
- tests/test_inference_engine.py
- tests/test_ensemble_model_interfaces.py

Required changes:
- No edits during DD.
- Determine the smallest runtime interface needed for:
  - online feature contract lookup
  - live feature snapshot reads and validation
  - model-catalog reads for live inference resolution
- Determine whether inference can stop importing `engine.data.feature_store` and `engine.runtime.model_cache` directly in this slice.
- Determine whether the default feature-id fallback can come from the runtime feature contract instead of a concrete module constant.
- Keep behavior and persistence unchanged outside the interface cut.

Required verification:
- none during DD

Acceptance criteria:
- The DD output names one bounded interface extraction.
- The DD output names the exact production and test touch set.
- The DD output states whether schema, dependency, or predictor-policy changes are required.

Stop and report if:
- The slice requires model-registry schema changes.
- The slice requires feature-store write-path changes or new dependencies.
- The slice expands into predictor fallback policy or ensemble logic changes.

## DD Findings

- `engine.inference_engine` still imports the concrete feature-store module directly:
  - `FEATURE_NAMES`
  - `get_live_features(...)`
  - `validate_feature_snapshot(...)`
- `engine.inference_engine` also imports the model-cache helpers directly:
  - `load_model_record(...)`
  - `list_model_records(...)`
  - `get_best_model_record(...)`
- The clean bounded `S08` change is:
  - add one runtime reader module that lazily resolves online feature reads, validation, feature contract metadata, and model-catalog reads
  - route `engine.inference_engine` through that runtime reader module
  - keep prediction, persistence, artifact loading, and ensemble behavior unchanged
- No DB schema changes are required for `S08`.
- No new dependencies are required for `S08`.
- Predictor fallback policy stays out of scope.
  - The existing predictor tests already show legacy fallback is opt-in.
  - `S08` only isolates the runtime read boundary used by live inference.
