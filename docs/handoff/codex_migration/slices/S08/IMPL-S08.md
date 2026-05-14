Slice ID: S08
Goal: Isolate live inference behind runtime reader interfaces so `engine.inference_engine` reads online features and model records through one runtime boundary instead of concrete store/cache imports.

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
- Add `engine/runtime/inference_runtime.py` with lazy runtime readers for:
  - online feature contract lookup
  - live feature snapshot reads
  - live feature snapshot validation
  - model-catalog load/list/best-record reads
- Update `engine.inference_engine` to use those runtime readers instead of importing the concrete feature-store and model-cache modules directly.
- Update the default feature-id fallback to use the runtime feature contract.
- Add focused runtime-reader coverage in `tests/test_inference_runtime.py`.
- Update inference-engine tests for the new runtime reader boundary and add coverage for the runtime feature-contract fallback.
- Do not change prediction semantics, persistence behavior, registry schema, or predictor fallback policy.

Required verification:
- python -m pytest tests/test_inference_runtime.py -q
- python -m pytest tests/test_inference_engine.py -q
- python -m pytest tests/test_ensemble_model_interfaces.py -q -k "online_model_uses_feature_store_inputs_and_registers_with_registry"

Acceptance criteria:
- `engine.inference_engine` no longer imports the concrete feature-store or model-cache modules directly.
- Live feature reads and model-catalog reads now flow through one runtime interface module.
- Default feature-id fallback comes from the runtime feature contract.
- Existing inference-engine behavior remains green.
- No schema or dependency changes are introduced.
- No unrelated file edits.

Stop and report if:
- The slice requires schema changes or new dependencies.
- The slice expands into predictor fallback policy changes.
- The slice requires feature-store write-path changes.

## Implementation Result

- Added `engine/runtime/inference_runtime.py` as the bounded runtime reader layer for:
  - online feature contract metadata
  - live feature snapshot reads and validation
  - model-catalog read helpers used by inference resolution
- Updated `engine/inference_engine.py` to consume those runtime readers instead of importing `engine.data.feature_store` and `engine.runtime.model_cache` directly.
- Updated the fallback feature-id path to use the runtime feature contract rather than a concrete module constant.
- Added focused runtime-reader coverage in `tests/test_inference_runtime.py`.
- Updated `tests/test_inference_engine.py` to patch the new runtime feature-reader boundary and added a direct unit test for the runtime feature-contract fallback path.
