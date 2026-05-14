# Strategy Subsystem

The `engine/strategy/` tree contains the decision and learning logic of the system.

It is responsible for:

- feature construction
- event interpretation
- labeling and prediction logic
- model training and validation
- promotion and governance logic
- portfolio and sizing decisions

## How The ML Stack Works

At a high level, the trading ML path is:

1. ingestion and event jobs write prices, events, labels, and embeddings
2. training jobs such as `train_model_v2.py`, `train_embed_models.py`, and `train_temporal_predictor.py` build model artifacts
3. training persists a feature contract and dataset snapshot into registry/lifecycle state
4. [predictor.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\predictor.py) resolves the live model and serves against that same feature contract
5. downstream portfolio and execution layers consume model intent, but they still apply the final safety and execution gates

Important distinction:

- Trading ML lives here and produces predictions, confidence, and model intent.
- Operator AI and execution AI elsewhere in the repo are advisory/diagnostic layers, not autonomous trading controllers.

## How Champion/Challenger Works

The repo uses a supervised competition model rather than blindly serving the latest trained artifact.

1. challengers first accumulate shadow evidence through [challenger_runtime.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\challenger_runtime.py) and [model_marketplace.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\model_marketplace.py)
2. replay validation and self-critic checks decide whether a challenger is even eligible
3. [champion_manager.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\champion_manager.py) compares the current champion and best challenger by score, observation window, replay approval, cooldowns, and degradation rules
4. [engine/model_registry.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\model_registry.py) and champion assignment tables hold the durable promotion state
5. [predictor.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\predictor.py) asks those assignments which model should serve live

## Key Files For New Engineers

- [model_v2.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\model_v2.py)
  Core model logic and regime-aware helpers.
- [predict.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\predict.py)
  Prediction entrypoints.
- [predictor.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\predictor.py)
  Predictor orchestration and related model use.
- [validation.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\validation.py)
  Validation routines and metrics logic.
- [decision_snapshot.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\decision_snapshot.py)
  Captures decision context for explainability and debugging.
- [alpha_lifecycle_engine.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\alpha_lifecycle_engine.py)
  Alpha lifecycle and promotion/demotion logic.
- [allocation_risk_overlay.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\allocation_risk_overlay.py)
  Deterministic post-allocation guardrail layer for crowding, concentration, and execution-capacity limits.
- [model_governance_ext.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\model_governance_ext.py)
  Additive governance snapshots and audit helpers shared by jobs, APIs, and research tooling.
- [model_lifecycle.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\model_lifecycle.py)
  Version planning, retraining cadence, active-version tracking, and retirement logic for deployable models.
- [learning_loop.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\learning_loop.py)
  Read-side learning signal extraction and dataset snapshots used by lifecycle decisions.
- [champion_manager.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\champion_manager.py)
  Champion/challenger coordination helpers that bridge registry state to promotion decisions.
- [portfolio_execution_intents.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\portfolio_execution_intents.py)
  Canonical portfolio-to-execution intent shaping used before broker-side policy takes over.
- [compute_social_regime.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\compute_social_regime.py)
  Social-regime feature computation.
- [ensemble_blender.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\ensemble_blender.py)
  Family-level prediction blending, stacked-weight persistence, and ensemble telemetry.
- [feature_store.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\feature_store.py)
  Versioned feature snapshot sink for live serving and offline analysis.
- [gbm_regressor.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\gbm_regressor.py)
  LightGBM-based model family with artifact persistence and prediction helpers.
- [hmm_regime.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\hmm_regime.py)
  HMM-based latent regime model and regime-aware ensemble weighting helpers.
- [tsfresh_features.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\tsfresh_features.py)
  Deterministic TSFresh feature extraction and snapshot materialization for training and replay.
- [statistical_gates.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\statistical_gates.py)
  Promotion-gate statistics such as bootstrap tests, SPA, and deflated Sharpe checks.
- [cpcv.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\cpcv.py)
  Combinatorial purged cross-validation and probability-of-backtest-overfitting utilities.
- [drift_retrain_controller.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\drift_retrain_controller.py)
  Drift-triggered retraining planner that publishes governance-friendly retraining events.
- [shap_explainer.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\shap_explainer.py)
  Explanation payload builder for live and offline model diagnostics.
- [black_litterman.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\black_litterman.py) and [hrp_allocator.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\hrp_allocator.py)
  Portfolio-construction helpers for blending model views with covariance-aware risk allocation.

## Newer Model Families And Controls

- Ensemble and family orchestration:
  [ensemble_blender.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\ensemble_blender.py),
  [engine/ensemble_engine.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\ensemble_engine.py), and
  [jobs/train_ensemble_meta.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\train_ensemble_meta.py).
- Regime-aware modeling:
  [hmm_regime.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\hmm_regime.py),
  [regime_stack.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\regime_stack.py), and
  [jobs/train_hmm_regime.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\train_hmm_regime.py).
- Feature and validation expansion:
  [feature_store.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\feature_store.py),
  [tsfresh_features.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\tsfresh_features.py),
  [cpcv.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\cpcv.py), and
  [statistical_gates.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\statistical_gates.py).
- Automated retraining controls:
  [drift_retrain_controller.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\drift_retrain_controller.py),
  [optuna_tuner.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\optuna_tuner.py), and
  [jobs/drift_triggered_retrain.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\drift_triggered_retrain.py).

## Jobs

The `jobs/` subdirectory contains long-running or one-shot strategy tasks such as:

- [jobs/train_model_v2.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\train_model_v2.py)
- [jobs/validate_now.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\validate_now.py)
- [jobs/train_embed_models.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\train_embed_models.py)
- [jobs/pipeline_train_and_eval.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\pipeline_train_and_eval.py)
- [jobs/execution_quality_job.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\execution_quality_job.py)
- [jobs/live_stability_guard_job.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\live_stability_guard_job.py)
- [jobs/model_lifecycle_manager.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\model_lifecycle_manager.py)
- [jobs/train_temporal_predictor.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\train_temporal_predictor.py)
- [jobs/promote_temporal_models.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\promote_temporal_models.py)
- [jobs/backtest_cpcv.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\backtest_cpcv.py)
- [jobs/train_gbm_regressor.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\train_gbm_regressor.py)
- [jobs/tune_gbm_regressor_optuna.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\tune_gbm_regressor_optuna.py)
- [jobs/train_hmm_regime.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\train_hmm_regime.py)
- [jobs/drift_triggered_retrain.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\drift_triggered_retrain.py)
- [jobs/alpha_discovery_loop.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\alpha_discovery_loop.py)

## Maintenance Guidance

- Keep feature generation and model serving compatible.
  If a new feature is required at train time, ensure it exists at inference time and is persisted clearly.
- Keep promotion and registry changes auditable.
  Update promotion, validation, and registry surfaces together.
- Avoid hidden coupling to DB schema names.
  Strategy files should prefer shared storage helpers or stable access patterns.
- Document every new model-producing job.
  New people need to know which tables and registry entries the job is expected to mutate.
- Keep research and live-governance boundaries explicit.
  Alpha discovery, CPCV, Optuna tuning, SHAP diagnostics, and drift-triggered retraining should feed auditable governance state rather than bypassing it.

## Extending Strategy Logic

When adding a new model or policy:

1. Add the model or feature implementation under `strategy/`.
2. Add or update the training/validation job under `strategy/jobs/` if required.
3. Update any prediction path that consumes the new artifact.
4. Update registry, promotion, and validation surfaces if the model becomes deployable.
