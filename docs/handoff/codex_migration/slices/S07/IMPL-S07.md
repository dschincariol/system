Slice ID: S07
Goal: Add a bounded training dataset contract that materializes parquet provenance bundles plus manifests with explicit `feature_schema` and `training_window` metadata for current retraining paths.

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
- orchestration extraction
- full feature-matrix export pipelines for every trainer

Required reading:
- engine/runtime/dataset_store.py
- engine/strategy/learning_loop.py
- engine/strategy/model_lifecycle.py
- engine/strategy/jobs/train_hmm_regime.py
- tests/test_training_dataset_contract.py
- tests/test_model_lifecycle_hmm.py
- tests/test_drift_triggered_retrain.py
- tests/test_gbm_regressor.py
- tests/test_temporal_training_integrity_regressions.py

Required changes:
- Add `engine/runtime/dataset_store.py` with helpers to:
  - normalize `feature_schema`
  - normalize `training_window`
  - materialize a parquet bundle and JSON manifest under a configurable dataset-store root
  - emit local paths plus optional object-style URI prefixes through environment configuration
- Update `engine.strategy.learning_loop.build_dataset_snapshot(...)` to:
  - accept optional `feature_schema` and `training_window`
  - materialize the shared provenance bundle through the new runtime helper
  - preserve existing lightweight `dataset_used` semantics for callers
- Update the HMM dataset snapshot paths in lifecycle and training code to use the same contract.
- Update the GBM, embed, temporal, and alpha training callers to pass explicit `feature_schema` / `training_window` metadata into the shared provenance helper.
- Add focused dataset-contract coverage in `tests/test_training_dataset_contract.py`.
- Add the parquet dependency needed by the dataset-store helper.
- Do not add registry/inference changes, remote uploads, or DB schema changes.

Required verification:
- python -m pytest tests/test_training_dataset_contract.py -q
- python -m pytest tests/test_model_lifecycle_hmm.py -q
- python -m pytest tests/test_drift_triggered_retrain.py -q
- python -m pytest tests/test_gbm_regressor.py -q
- python -m pytest tests/test_temporal_training_integrity_regressions.py -q

Acceptance criteria:
- Shared `dataset_used` records now include dataset bundle metadata (`dataset_id`, format, URIs, local paths, storage backend, row count).
- JSON manifests persist explicit `feature_schema` and `training_window`.
- HMM and non-HMM training paths use the same materialized provenance contract.
- Object-style dataset URIs are configuration-driven only; no remote upload logic is introduced.
- No new DB schema family is introduced.
- No unrelated file edits.

Stop and report if:
- The slice requires registry/inference changes to stay safe.
- The slice requires new DB schema or a new storage family.
- The slice requires remote object-store SDKs or upload logic.

## Implementation Result

- Added `engine/runtime/dataset_store.py` as the shared training dataset-contract helper.
  - Writes `dataset.parquet` and `manifest.json` bundles.
  - Normalizes `feature_schema` and `training_window`.
  - Supports local paths plus object-style URI prefixes through environment configuration.
- Updated `engine.strategy.learning_loop.build_dataset_snapshot(...)` to materialize the shared provenance bundle and carry explicit schema/window metadata.
- Fixed the label provenance query to keep label counts accurate when `events` exists but a label row does not have a matching event row.
- Updated `engine.strategy.model_lifecycle._build_hmm_dataset_snapshot(...)` and `engine.strategy.jobs.train_hmm_regime._build_dataset_used(...)` to use the same contract.
- Updated the direct training callers to pass explicit schema/window metadata:
  - `engine/strategy/gbm_regressor.py`
  - `engine/strategy/train_embed_models.py`
  - `engine/strategy/train_temporal_predictor.py`
  - `engine/research/alpha_generator.py`
- Added focused regression coverage in `tests/test_training_dataset_contract.py`.
- Added `pyarrow>=18,<20` to `requirements.txt` for the parquet writer.
