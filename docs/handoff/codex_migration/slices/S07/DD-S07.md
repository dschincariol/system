Slice ID: S07
Goal: Introduce a bounded training dataset contract that materializes parquet provenance bundles plus manifests with explicit `feature_schema` and `training_window` metadata for live retraining paths, without moving training into a new service or adding remote object-store dependencies.

In scope:
- engine/runtime/dataset_store.py
- engine/strategy/learning_loop.py
- engine/strategy/model_lifecycle.py
- engine/strategy/jobs/train_hmm_regime.py
- engine/strategy/gbm_regressor.py
- engine/strategy/train_embed_models.py
- engine/strategy/train_temporal_predictor.py
- engine/research/alpha_generator.py
- tests/test_training_dataset_contract.py
- requirements.txt

Out of scope:
- model registry, inference, or artifact-store flows
- remote object-store upload clients or SDKs
- new DB tables, columns, or schema families
- Prefect/orchestration extraction
- full feature-matrix export pipelines for every trainer

Required reading:
- engine/strategy/learning_loop.py
- engine/strategy/model_lifecycle.py
- engine/strategy/jobs/train_hmm_regime.py
- engine/strategy/gbm_regressor.py
- engine/strategy/train_embed_models.py
- engine/strategy/train_temporal_predictor.py
- engine/research/alpha_generator.py
- tests/test_training_dataset_contract.py
- tests/test_model_lifecycle_hmm.py
- tests/test_drift_triggered_retrain.py
- tests/test_gbm_regressor.py
- tests/test_temporal_training_integrity_regressions.py

Required changes:
- No edits during DD.
- Identify the narrowest shared helper that can materialize parquet dataset bundles and JSON manifests from existing `dataset_used` provenance records.
- Determine how `build_dataset_snapshot(...)` should accept explicit `feature_schema` and `training_window` metadata without changing existing callers that do not yet provide them.
- Determine how the dedicated HMM dataset snapshot path should adopt the same contract.
- Determine whether object-store semantics can stay as URI-prefix metadata while writes remain local in this slice.
- Determine whether a parquet dependency is required for the bounded contract.

Required verification:
- none during DD

Acceptance criteria:
- The DD output names one bounded dataset-contract change.
- The DD output names the exact production, dependency, and test touch set.
- The DD output states whether new DB schema or remote object-store dependencies are required.

Stop and report if:
- The slice requires new DB tables or columns.
- The slice requires remote object-store SDKs to remain safe.
- The slice expands into registry/inference changes or full training-service extraction.

## DD Findings

- The current shared provenance seam is `engine.strategy.learning_loop.build_dataset_snapshot(...)`.
  - It records lightweight `dataset_used` metadata and a fingerprint.
  - It does not persist a bundle, manifest, or explicit dataset storage contract.
- The HMM path is separate.
  - `engine.strategy.model_lifecycle._build_hmm_dataset_snapshot(...)` and `engine.strategy.jobs.train_hmm_regime._build_dataset_used(...)` build dataset metadata outside the shared provenance helper.
- The clean bounded `S07` change is:
  - add a runtime `dataset_store` helper that writes local parquet bundles plus JSON manifests
  - normalize `feature_schema` and `training_window` metadata there
  - let callers expose object-style dataset URIs through configuration only, without remote upload logic
  - update the shared provenance seam and direct training callers to populate the contract
- No new DB schema is required for `S07`.
  - The dataset contract can stay on the filesystem/object-style URI boundary.
  - Existing model metadata and lifecycle records can continue to store `dataset_used` as JSON.
- A parquet dependency is required for the bounded contract.
  - `pyarrow` is the lowest-risk fit because pandas is already present and the repo already uses pandas-heavy training paths.
- Full row-level feature-matrix export remains out of scope for this slice.
  - `S07` is limited to materialized training provenance bundles plus explicit schema/window metadata.
