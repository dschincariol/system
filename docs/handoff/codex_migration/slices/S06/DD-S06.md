Slice ID: S06
Goal: Introduce an `ArtifactStore` contract that normalizes immutable artifact manifests in the registry and lets inference plus model scoring consume object-store-backed artifact URIs safely without changing training or upload flows.

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
- training jobs or artifact upload/orchestration flows
- requirements or cloud/object-store SDK dependencies
- schema changes outside persisted model metadata
- inference timeout tuning or broader ensemble execution changes

Required reading:
- engine/model_registry.py
- engine/inference_engine.py
- engine/model_scoring.py
- engine/runtime/model_cache.py
- tests/test_model_registry_catalog.py
- tests/test_inference_engine.py
- tests/test_model_scoring.py
- tests/test_ensemble_model_interfaces.py

Required changes:
- No edits.
- Determine the smallest bounded change that introduces an immutable artifact manifest contract without adding object-store SDK dependencies.
- Identify whether `artifact_uri` can stay as the durable field while manifest metadata carries the immutable identity.
- Determine whether inference and scoring can resolve object-store artifacts through a local mirror/cache path instead of direct remote fetches.
- Produce the exact `S06` touch set, acceptance criteria, and stop conditions.

Required verification:
- none during DD

Acceptance criteria:
- The DD output names one bounded artifact-contract change.
- The DD output names the exact production and test touch set.
- The DD output states whether schema, upload, or dependency changes are required.

Stop and report if:
- The slice requires new DB columns or a new schema family.
- The slice requires cloud SDK dependencies to stay safe.
- The slice expands into training/upload orchestration or base-model writer refactors.

## DD Findings

- The current catalog contract stores `artifact_uri` as an opaque string and defers all interpretation to consumers.
  - `engine.model_registry.register_model` persists the raw string.
  - `engine.inference_engine._load_model_artifact` assumes the string is a local filesystem path.
  - `engine.model_scoring._persist_artifact` mutates the same path in place for online updates.
- There is no existing object-store helper, no immutable manifest contract, and no cloud/object-store dependency in the repo.
- The artifact contract cannot stay bounded to registry plus inference only.
  - `engine.model_scoring` is a direct consumer because immutable object-store artifacts must not be rewritten in place.
  - This is the one justified expansion beyond the default three production modules: one new boundary module plus the three direct consumers.
- No new DB columns are required for `S06`.
  - The durable manifest can live inside `models.metadata_json` as `metadata.artifact_manifest`.
  - `artifact_uri` can remain the top-level durable reference field.
- The clean slice is:
  - add a runtime `ArtifactStore` helper that normalizes local and object-store artifact references
  - require immutable identity for object-store registrations via manifest metadata (`version_id`, `sha256`, or `etag`)
  - resolve object-store artifacts through a configured local mirror root instead of remote fetches
  - expose the normalized manifest on loaded model records
  - make model scoring skip in-place persistence for immutable object-store artifacts
- Explicitly out of scope for `S06`:
  - writer-side object-store uploads
  - changing `BaseModel.register` to emit object-store URIs
  - training/orchestration pipeline changes
