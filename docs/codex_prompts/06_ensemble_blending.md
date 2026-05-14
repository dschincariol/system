# Codex Prompt 06 — Stacked Ridge Ensemble (Replace Champion-Takes-All)

You are working in a Python systematic trading system whose predictor
router today selects **one** champion model per symbol per horizon.
Champion-takes-all forfeits the diversification benefit of stacked
generalization; in financial prediction, where signal-to-noise is low,
a ridge meta-learner over multiple heterogeneous models routinely beats
the best single model out-of-sample. This prompt introduces a **stacked
ensemble layer** that runs *on top of* the existing champion mechanism,
so champions remain meaningful but the production prediction is the
ensemble blend.

## Goal

1. A meta-learner `RidgeStackEnsemble` that takes per-model OOS
   predictions as features and emits the final prediction.
2. An OOS-prediction store so the meta-learner is never trained on
   in-sample predictions (a classic stacking pitfall).
3. A predictor-router upgrade so live inference returns the ensemble's
   blend by default, with a config flag to fall back to single-champion
   for ablation.
4. Audit trail: every served prediction logs the per-model component
   predictions and the meta-learner weights used.

## Files to read first (read-only)

- `engine/strategy/predictor.py` — current predictor router.
- `engine/strategy/champion_manager.py` — current champion logic.
- `engine/model_registry.py` — model artifact tracking.
- `engine/strategy/temporal_predictor.py`,
  `engine/strategy/embed_regressor.py`, and the new families from
  prompt 05 — sources of component predictions.
- `engine/strategy/decision_log.py` — current prediction logging
  surface.
- `engine/strategy/promotion_guard.py` — decides which families are
  eligible to feed the ensemble.
- `engine/runtime/storage.py` — schema patterns for the OOS store.
- `tests/test_model_competition_real_pnl.py`.

## Files to create

- `engine/strategy/ensemble/__init__.py`
- `engine/strategy/ensemble/oos_store.py` — per-model OOS predictions
  keyed by `(symbol, horizon, family, ts)`. Backing table:
  `model_oos_predictions(symbol TEXT, horizon INTEGER, family TEXT,
   ts INTEGER, prediction REAL, target REAL NULL,
   PRIMARY KEY(symbol, horizon, family, ts))`.
- `engine/strategy/ensemble/ridge_meta.py` — `RidgeStackEnsemble.fit`
  takes a long-format dataframe of OOS predictions, pivots to wide,
  fits ridge with non-negative weights (`scipy.optimize.nnls`) by
  default and unconstrained ridge as an option. Persists as JSON.
- `engine/strategy/ensemble/blender.py` — at serve time, loads weights,
  pulls per-family component predictions, returns weighted sum.
- `engine/strategy/jobs/train_ensemble.py` — periodic refit job.
- `tests/test_oos_store.py`
- `tests/test_ridge_meta.py`
- `tests/test_ensemble_blender.py`
- `tests/test_ensemble_integration.py`

## Files to modify

- `engine/strategy/predictor.py` — wrap the existing per-family
  prediction call in a blender. New env flag
  `ENSEMBLE_MODE=blend|single_champion` (default `blend`).
- `engine/strategy/decision_log.py` — log the per-family components
  and the weight vector that produced the final blend.
- `engine/runtime/job_registry.py` — register `train_ensemble` job.
- `engine/runtime/storage.py` — add the `model_oos_predictions` and
  `ensemble_weights` tables.

## Implementation plan

1. **OOS store.** Modify each trainer to *always* write its OOS
   validation-fold predictions to `model_oos_predictions` after
   training. Targets are filled in lazily by a tiny job that joins
   actuals once enough time has passed.
2. **Ridge meta.** Default constraint is `weights >= 0`, sum unbounded
   (Breiman-style stacking). Regularization
   strength `alpha` is in the Optuna catalog (prompt 03). Refit
   nightly on the trailing 252 trading days of OOS predictions.
3. **Persistence.** `ensemble_weights(symbol TEXT, horizon INTEGER,
   ts INTEGER, weights_json TEXT, intercept REAL, alpha REAL,
   n_train_obs INTEGER, val_metric REAL,
   PRIMARY KEY(symbol, horizon, ts))`.
4. **Blender at serve.** `predictor.predict(symbol, horizon, ts)`:
   - load latest weights for `(symbol, horizon)`,
   - call each family that has non-zero weight,
   - return `intercept + sum(w_i * pred_i)`.
   - log the components.
5. **Fallback.** If no weights row exists for `(symbol, horizon)` —
   common at first deploy — fall back to single-champion silently.
6. **Eligibility.** Only families currently at stage `champion` or
   `challenger` may receive ensemble weight. Shadow models are not
   blended.

## Acceptance criteria

- [ ] Every trainer writes OOS predictions to `model_oos_predictions`
      after each training run; rows are unique per
      `(symbol, horizon, family, ts)`.
- [ ] `RidgeStackEnsemble.fit` with `nonneg=True` produces all weights
      `>= 0`; with `nonneg=False`, ridge regression is exact (matches
      sklearn `Ridge` to 1e-9 on a fixed seed).
- [ ] The blender fallback to single-champion is exercised in tests
      and produces predictions identical to today's path when no
      weights exist.
- [ ] At least one OOS test asserts: on a synthetic DGP where two
      noisy estimators each have R² ≈ 0.10 but are uncorrelated,
      the ensemble achieves R² > max(individual R²) + 0.03.
- [ ] Decision log records the component vector and weight vector for
      every served prediction when `ENSEMBLE_MODE=blend`.
- [ ] No shadow-stage family appears in the weight vector.

## Test plan

- `tests/test_oos_store.py` — write / read / upsert; primary key is
  enforced.
- `tests/test_ridge_meta.py` — non-negative constraint honored;
  unconstrained matches sklearn; predict on held-out data.
- `tests/test_ensemble_blender.py` — load weights, compute blend,
  fallback path.
- `tests/test_ensemble_integration.py` — full path: trainers write
  OOS, meta refits, blender serves; component logging is correct.

Run: `pytest -q tests/test_oos_store.py tests/test_ridge_meta.py
tests/test_ensemble_blender.py tests/test_ensemble_integration.py
tests/test_model_competition_real_pnl.py`

## Out of scope

- Do not change champion / challenger / shadow lifecycles. The
  ensemble runs on top of them.
- Do not introduce non-linear meta-learners (gradient boosting on top
  of model outputs). Linear stacking is robust under low signal-to-
  noise; non-linear meta-learners overfit.
- Do not feed feature columns into the meta-learner — only model
  predictions. Mixing breaks the independence assumption.
