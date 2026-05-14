Slice ID: S06
Goal: Add a runtime `ArtifactStore` contract that normalizes immutable artifact manifests in the registry and lets inference plus model scoring consume object-store-backed artifact URIs safely while keeping existing local-path flows working.

In scope:
- engine/runtime/artifact_store.py
- engine/model_registry.py
- engine/inference_engine.py
- engine/model_scoring.py
- tests/test_model_registry_catalog.py
- tests/test_inference_engine.py
- tests/test_model_scoring.py
- tests/test_ensemble_model_interfaces.py

Out of scope:
- engine/strategy/models/base_model.py
- training jobs, upload/orchestration flows, or artifact publishing
- requirements or cloud/object-store SDK dependencies
- schema changes outside persisted model metadata
- broad ensemble timeout or stale-feature test rewrites

Required reading:
- engine/model_registry.py
- engine/inference_engine.py
- engine/model_scoring.py
- tests/test_model_registry_catalog.py
- tests/test_inference_engine.py
- tests/test_model_scoring.py
- tests/test_ensemble_model_interfaces.py

Required changes:
- Add `engine/runtime/artifact_store.py` with helpers to:
  - normalize local-path and object-store artifact references
  - require immutable identity for object-store registrations
  - resolve read paths through a local mirror root for object-store artifacts
  - expose stable artifact cache keys
  - resolve mutable write paths for local artifacts only
- Update `engine.model_registry.register_model` to normalize and persist artifact manifest metadata without adding new DB columns.
- Update `engine.model_registry.load_model` / list-path parsing to expose the normalized manifest on returned records.
- Update `engine.inference_engine._load_model_artifact` to load artifacts through the new runtime helper instead of assuming `artifact_uri` is a local path.
- Update `engine.model_scoring` so online updates still persist local artifacts but skip immutable object-store artifacts safely.
- Add focused tests for:
  - registry normalization and validation of object-store manifests
  - inference loading of mirrored object-store artifacts
  - scoring refusal to mutate immutable object-store artifacts
  - unchanged local-path registration compatibility
- Do not add object-store SDK dependencies, schema migrations, or writer-side upload support.

Required verification:
- python -m pytest tests/test_model_registry_catalog.py -q
- python -m pytest tests/test_inference_engine.py -q -k "scores_registered_model_and_persists_prediction or loads_object_store_artifact_from_local_mirror or degrades_to_single_member_when_an_ensemble_member_is_missing"
- python -m pytest tests/test_model_scoring.py -q -k "updates_online_model_artifact or immutable_object_store_artifact_updates"
- python -m pytest tests/test_ensemble_model_interfaces.py -q -k "online_model_uses_feature_store_inputs_and_registers_with_registry"

Acceptance criteria:
- Registry writes normalize and persist artifact manifest metadata without new columns.
- Object-store artifact URIs require immutable identity when registered.
- Inference can load mirrored object-store artifacts through the runtime helper.
- Model scoring keeps local artifact updates working and skips immutable object-store rewrites safely.
- Existing local-path registration compatibility stays intact.
- No unrelated file edits.

Stop and report if:
- The slice requires writer-side upload support or base-model registration refactors.
- The slice requires cloud/object-store SDK dependencies.
- The slice expands into training-data or orchestration changes.
