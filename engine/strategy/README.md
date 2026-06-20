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
4. [predictor.py](predictor.py) resolves the live model and serves against that same feature contract
5. downstream portfolio and execution layers consume model intent, but they still apply the final safety and execution gates

Important distinction:

- Trading ML lives here and produces predictions, confidence, and model intent.
- Operator AI and execution AI elsewhere in the repo are advisory/diagnostic layers, not autonomous trading controllers.

## How Champion/Challenger Works

The repo uses a supervised competition model rather than blindly serving the latest trained artifact.

1. challengers first accumulate shadow evidence through [challenger_runtime.py](challenger_runtime.py) and [model_marketplace.py](model_marketplace.py)
2. replay validation and self-critic checks decide whether a challenger is even eligible
3. [champion_manager.py](champion_manager.py) compares the current champion and best challenger by score, observation window, replay approval, cooldowns, deconfounded signal evidence, and degradation rules. The same promotion eligibility helper gates both symbol/horizon assignments and the aggregate `MODEL_COMPETITION_SCOPE` champion, so a global best model cannot bypass replay freshness, replay approval, or self-critic blocked keys.
4. [engine/model_registry.py](../model_registry.py) and champion assignment tables hold the durable promotion state
5. [predictor.py](predictor.py) asks those assignments which model should serve live

Strategy-stage promotion follows the same governance principle. Portfolio shadow
outperformance records a `strategy_promotion_candidates` row through
[strategy_promotion_governance.py](strategy_promotion_governance.py); it does
not set `strategy_registry.stage='live'`. The governance job may promote a
candidate only after operator approval, positive realized PnL evidence, passing
statistical evidence, fresh approved replay validation, passing OPE evidence,
system promotion-guard/cooldown approval, and promotion audit persistence.

## Key Files For New Engineers

- [model_v2.py](model_v2.py)
  Core model logic and regime-aware helpers.
- [predict.py](predict.py)
  Prediction entrypoints.
- [predictor.py](predictor.py)
  Predictor orchestration and related model use.
- [validation.py](validation.py)
  Validation routines and metrics logic.
- [decision_snapshot.py](decision_snapshot.py)
  Captures decision context for explainability and debugging.
- [alpha_lifecycle_engine.py](alpha_lifecycle_engine.py)
  Alpha lifecycle and promotion/demotion logic.
- [alpha_shrinkage.py](alpha_shrinkage.py)
  Empirical-Bayes per-symbol alpha shrinkage. Portfolio rebalance applies it before expected-return blending, allocation optimization, and risk overlays so thin-history symbols are partially pooled toward sector, liquidity, volatility-regime, model-family, or neutral priors.
- [allocation_risk_overlay.py](allocation_risk_overlay.py)
  Deterministic post-allocation guardrail layer for crowding, concentration, and execution-capacity limits.
- [model_governance_ext.py](model_governance_ext.py)
  Additive governance snapshots and audit helpers shared by jobs, APIs, and research tooling.
- [strategy_promotion_governance.py](strategy_promotion_governance.py)
  Candidate, approval, evidence, and audit checks for shadow strategy promotion into live strategy-registry state.
- [model_lifecycle.py](model_lifecycle.py)
  Version planning, retraining cadence, active-version tracking, and retirement logic for deployable models.
- [learning_loop.py](learning_loop.py)
  Read-side learning signal extraction and dataset snapshots used by lifecycle decisions.
- [champion_manager.py](champion_manager.py)
  Champion/challenger coordination helpers that bridge registry state to promotion decisions.
- [portfolio_execution_intents.py](portfolio_execution_intents.py)
  Canonical portfolio-to-execution intent shaping used before broker-side policy takes over.
- [compute_social_regime.py](compute_social_regime.py)
  Social-regime feature computation.
- [ensemble_blender.py](ensemble_blender.py)
  Family-level prediction blending, stacked-weight persistence, and ensemble telemetry.
- [feature_store.py](feature_store.py)
  Versioned feature snapshot sink for live serving and offline analysis.
- [gbm_regressor.py](gbm_regressor.py)
  LightGBM-based model family with artifact persistence and prediction helpers.
- [hmm_regime.py](hmm_regime.py)
  HMM-based latent regime model and regime-aware ensemble weighting helpers.
- [tsfresh_features.py](tsfresh_features.py)
  Deterministic TSFresh feature extraction and snapshot materialization for training and replay.
- [ts_foundation_encoder.py](ts_foundation_encoder.py)
  Shadow-only frozen time-series foundation encoder. The initial backend is Chronos and it emits `tsfm.chronos_v2.*` feature ids with PIT and artifact provenance metadata.
- [statistical_gates.py](statistical_gates.py)
  Promotion-gate statistics such as bootstrap tests, SPA, and deflated Sharpe checks.
- [deconfounded_promotion.py](deconfounded_promotion.py)
  Residualized incremental signal validation for promotion gates after beta, sector, size, volatility, liquidity, regime, and existing-model exposure controls.
- [cpcv.py](cpcv.py)
  Combinatorial purged cross-validation and probability-of-backtest-overfitting utilities.
- [drift_retrain_controller.py](drift_retrain_controller.py)
  Drift-triggered retraining planner that publishes governance-friendly retraining events.
- [shap_explainer.py](shap_explainer.py)
  Explanation payload builder for live and offline model diagnostics.
- [black_litterman.py](black_litterman.py) and [hrp_allocator.py](hrp_allocator.py)
  Portfolio-construction helpers for blending model views with covariance-aware risk allocation.

## Newer Model Families And Controls

- Ensemble and family orchestration:
  [ensemble_blender.py](ensemble_blender.py),
  [engine/ensemble_engine.py](../ensemble_engine.py), and
  [jobs/train_ensemble_meta.py](jobs/train_ensemble_meta.py).
- Regime-aware modeling:
  [hmm_regime.py](hmm_regime.py),
  [regime_stack.py](regime_stack.py), and
  [jobs/train_hmm_regime.py](jobs/train_hmm_regime.py).
- Feature and validation expansion:
  [feature_store.py](feature_store.py),
  [tsfresh_features.py](tsfresh_features.py),
  [ts_foundation_encoder.py](ts_foundation_encoder.py),
  [deconfounded_promotion.py](deconfounded_promotion.py),
  [cpcv.py](cpcv.py), and
  [statistical_gates.py](statistical_gates.py).
- Automated retraining controls:
  [drift_retrain_controller.py](drift_retrain_controller.py),
  [optuna_tuner.py](optuna_tuner.py), and
  [jobs/drift_triggered_retrain.py](jobs/drift_triggered_retrain.py).

## Jobs

The `jobs/` subdirectory contains long-running or one-shot strategy tasks such as:

- [jobs/train_model_v2.py](jobs/train_model_v2.py)
- [jobs/validate_now.py](jobs/validate_now.py)
- [jobs/train_embed_models.py](jobs/train_embed_models.py)
- [jobs/pipeline_train_and_eval.py](jobs/pipeline_train_and_eval.py)
- [jobs/execution_quality_job.py](jobs/execution_quality_job.py)
- [jobs/live_stability_guard_job.py](jobs/live_stability_guard_job.py)
- [jobs/model_lifecycle_manager.py](jobs/model_lifecycle_manager.py)
- [jobs/train_temporal_predictor.py](jobs/train_temporal_predictor.py)
- [jobs/promote_temporal_models.py](jobs/promote_temporal_models.py)
- [jobs/backtest_cpcv.py](jobs/backtest_cpcv.py)
- [jobs/train_gbm_regressor.py](jobs/train_gbm_regressor.py)
- [jobs/tune_gbm_regressor_optuna.py](jobs/tune_gbm_regressor_optuna.py)
- [jobs/train_hmm_regime.py](jobs/train_hmm_regime.py)
- [jobs/drift_triggered_retrain.py](jobs/drift_triggered_retrain.py)
- [jobs/alpha_discovery_loop.py](jobs/alpha_discovery_loop.py)

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
