# Quick Wins — Self-Contained Implementation Prompts

Last verified against code: 2026-06-11

Historical archive. These prompts were originally designed to be handed to a fresh AI coding session to implement one quick win end-to-end. Their "current" context statements describe the pre-implementation state and should not be read as the current system state.

All five quick wins are DONE in current code:

| Prompt | Current status | Implementing modules |
| --- | --- | --- |
| Statistical promotion gates | DONE | `engine/strategy/statistical_gates.py`, `engine/strategy/promotion_guard.py`, `engine/strategy/promotion_audit.py` |
| CPCV / PBO promotion backtests | DONE | `engine/strategy/cpcv.py`, `engine/strategy/gated_backtest.py`, `engine/strategy/jobs/backtest_cpcv.py` |
| tsfresh automated features | DONE | `engine/strategy/tsfresh_features.py`, `engine/data/jobs/compute_tsfresh_snapshots.py`, `engine/strategy/feature_registry.py` |
| LightGBM/XGBoost/GBM/PatchTST model families | DONE | `engine/strategy/models/`, `engine/strategy/jobs/train_lgbm_models.py`, `engine/strategy/jobs/train_xgb_models.py`, `engine/strategy/jobs/train_patchtst_models.py` |
| Ridge meta-ensemble blending | DONE | `engine/strategy/ensemble/ridge_meta.py`, `engine/strategy/jobs/train_ensemble_meta.py`, `engine/strategy/jobs/train_ensemble.py` |

Historical recommended execution order: Prompt 1 -> Prompt 2 -> Prompt 4 -> Prompt 3 -> Prompt 5.

Repo root for this verification pass: `/home/david/gitsandbox/system/system`

---

## Prompt 1 — DONE — Harvey/Liu/Zhu t > 3.0 Promotion Gate

**Historical context before implementation.** This described a full-stack Python supervised trading system (~400+ files, SQLite-era local storage) with a champion/challenger ML loop. At the time, the promotion logic in `engine/strategy/champion_manager.py` promoted challengers when their marketplace score exceeded the champion by a margin and did **not** correct for multiple-hypothesis testing. Per Harvey/Liu/Zhu (2016), ~95% of published factors are false positives at the standard t > 2.0 threshold. Industry-standard is t > 3.0 plus Deflated Sharpe Ratio (Bailey & de Prado 2014) and Benjamini-Hochberg FDR control.

**Current status.** DONE in `engine/strategy/statistical_gates.py`, `engine/strategy/promotion_guard.py`, and `engine/strategy/promotion_audit.py`.

**Goal.** Add a statistical promotion gate that every new champion must pass.

**Files to create.**
- `engine/strategy/statistical_gates.py` — pure library with these functions:
  - `compute_t_statistic(returns: list[float]) -> float` — mean / (std / sqrt(n))
  - `deflated_sharpe_ratio(sharpe: float, n_trials: int, n_obs: int, skew: float, kurt: float) -> float` — Bailey & de Prado 2014 formula
  - `benjamini_hochberg_fdr(p_values: list[float], alpha: float = 0.05) -> list[bool]` — returns accept/reject per hypothesis
  - `harvey_liu_zhu_threshold(n_trials: int) -> float` — dynamic Bonferroni-adjusted t threshold, floor = 3.0
  - `passes_promotion_gate(returns: list[float], n_competing_trials: int) -> tuple[bool, dict]` — returns (pass/fail, diagnostics dict)

**Files to modify.**
- `engine/strategy/champion_manager.py` — in the promotion check, call `passes_promotion_gate(...)` on the challenger's recent returns before allowing replacement. On fail, log reason to `registry_metrics` and skip promotion.
- `engine/strategy/jobs/promotion_guard.py` (if exists; otherwise find equivalent) — add same gate to any independent promotion path.

**Historical storage additions.**
```sql
CREATE TABLE IF NOT EXISTS hypothesis_registry (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_ts INTEGER NOT NULL,
  model_name TEXT NOT NULL,
  candidate_version TEXT NOT NULL,
  n_observations INTEGER NOT NULL,
  t_statistic REAL NOT NULL,
  deflated_sharpe REAL NOT NULL,
  threshold_t REAL NOT NULL,
  n_competing_trials INTEGER NOT NULL,
  passed INTEGER NOT NULL,
  diagnostics_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_hypothesis_model ON hypothesis_registry(model_name, created_ts);
```

**Environment variables.**
- `CHAMPION_PROMOTION_MIN_T_STAT` (default `3.0`)
- `CHAMPION_PROMOTION_MIN_DEFLATED_SHARPE` (default `0.0`)
- `CHAMPION_PROMOTION_MIN_OBSERVATIONS` (default `50`)
- `CHAMPION_PROMOTION_FDR_ALPHA` (default `0.05`)

**Acceptance criteria.**
- Unit tests in `tests/` covering the four math functions with known inputs.
- Integration test: seed `champion_manager` with a challenger whose returns have t=2.5; assert promotion is blocked and `hypothesis_registry` row is written with `passed=0`.
- Integration test: same with t=3.5; assert promotion proceeds.
- Dashboard/API: expose a recent-hypothesis-rows endpoint under `engine/api/api_dashboard_reads.py` for visibility.

**Constraints.**
- Do not bypass or remove existing promotion checks — this gate is additive.
- Must be a no-op if `CHAMPION_PROMOTION_MIN_OBSERVATIONS` is not met (log "insufficient data", don't block).
- Respect the system's existing logging and storage patterns — do not introduce new DB connection logic.

---

## Prompt 2 — DONE — Combinatorial Purged Cross-Validation (de Prado)

**Historical context before implementation.** Same system as Prompt 1. At the time, backtesting leaked across the time axis because it used naive splits. Marcos López de Prado's *Advances in Financial Machine Learning* (2018) introduced Combinatorial Purged Cross-Validation (CPCV) and the Probability of Backtest Overfitting (PBO) metric. CPCV is the gold standard for robust time-series backtesting because it generates many non-overlapping backtest paths, purges label overlap between train/test, and embargoes post-test bars.

**Current status.** DONE in `engine/strategy/cpcv.py`, `engine/strategy/gated_backtest.py`, and `engine/strategy/jobs/backtest_cpcv.py`.

**Goal.** Add CPCV as a first-class backtesting path used by the promotion guard.

**Files to create.**
- `engine/strategy/cpcv.py` — pure library:
  - `make_cpcv_splits(n_samples: int, n_splits: int, n_test_splits: int) -> list[tuple[list[int], list[int]]]` — returns combinations of (train_idx, test_idx)
  - `purge_train_indices(train_idx: list[int], test_idx: list[int], label_horizon: int) -> list[int]` — removes train rows whose labels overlap test rows
  - `embargo_train_indices(train_idx: list[int], test_idx: list[int], embargo_pct: float) -> list[int]` — removes train rows within embargo of test boundary
  - `cpcv_backtest(features, labels, model_factory, n_splits, n_test_splits, embargo_pct, label_horizon) -> dict` — returns per-path returns + aggregate stats
  - `compute_pbo(in_sample_scores: list[float], out_of_sample_scores: list[float]) -> float` — Probability of Backtest Overfitting
- `engine/strategy/jobs/backtest_cpcv.py` — runnable job that pulls a candidate model spec, runs CPCV, writes results.

**Files to modify.**
- `engine/strategy/jobs/promotion_guard.py` — read latest CPCV result for the candidate; require `pbo < 0.5` and `mean_path_sharpe > 0.5` (configurable) before allowing promotion.
- `engine/runtime/job_registry.py` — register `backtest_cpcv` as an allowed job.

**Historical storage shape.**
```sql
CREATE TABLE IF NOT EXISTS backtest_cpcv_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_ts INTEGER NOT NULL,
  model_name TEXT NOT NULL,
  candidate_version TEXT NOT NULL,
  n_splits INTEGER NOT NULL,
  n_test_splits INTEGER NOT NULL,
  embargo_pct REAL NOT NULL,
  n_paths INTEGER NOT NULL,
  mean_sharpe REAL NOT NULL,
  median_sharpe REAL NOT NULL,
  pbo REAL NOT NULL,
  diagnostics_json TEXT
);
CREATE TABLE IF NOT EXISTS backtest_cpcv_paths (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id INTEGER NOT NULL REFERENCES backtest_cpcv_runs(id),
  path_idx INTEGER NOT NULL,
  returns_json TEXT NOT NULL,
  sharpe REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cpcv_runs_model ON backtest_cpcv_runs(model_name, created_ts);
```

**Environment variables.**
- `CPCV_N_SPLITS` (default `6`)
- `CPCV_N_TEST_SPLITS` (default `2`)
- `CPCV_EMBARGO_PCT` (default `0.01`)
- `CPCV_MAX_PBO` (default `0.5`)
- `CPCV_MIN_PATH_SHARPE` (default `0.5`)

**Acceptance criteria.**
- Unit tests: `make_cpcv_splits(10, 5, 2)` returns C(5,2)=10 combinations with disjoint test/train.
- Unit test: `purge_train_indices` removes train rows within `label_horizon` of any test row.
- Unit test: PBO computation on a known dataset returns the expected value.
- Integration: run `backtest_cpcv` job on an existing champion model and write a row to `backtest_cpcv_runs`.
- Promotion integration: fabricate a challenger with high in-sample Sharpe but PBO > 0.5; assert promotion is blocked.

**Constraints.**
- Respect existing runtime storage patterns.
- Use numpy only for array ops; no pandas in the hot path (keeps dependencies light).
- Do not change existing backtest code paths — CPCV is a new, additive check.

---

## Prompt 3 — DONE — tsfresh Automated Feature Extraction

**Historical context before implementation.** Same system. At the time, features were described as hand-coded in `engine/strategy/feature_registry.py` across 10 groups. WorldQuant and quantitative funds generate millions of candidate features automatically. `tsfresh` computes 700+ time-series features (FFTs, entropy, autocorrelations, peak counts, etc.) from a single window of prices. Feeding these into the existing schema-driven feature pipeline gives the champion/challenger loop a much larger hypothesis space to discover alpha.

**Current status.** DONE in `engine/strategy/tsfresh_features.py`, `engine/data/jobs/compute_tsfresh_snapshots.py`, and `engine/strategy/feature_registry.py`.

**Goal.** Register a new `tsfresh.*` feature group in `feature_registry.py`, computed from a rolling window of prices, train/serve-consistent.

**Files to create.**
- `engine/strategy/tsfresh_features.py`:
  - `build_tsfresh_window(symbol: str, end_ts: int, window_s: int) -> pd.DataFrame` — pulls prices from runtime storage
  - `compute_tsfresh_features(window_df) -> dict[str, float]` — calls `tsfresh.extract_features` with `MinimalFCParameters` or a curated subset for speed
  - `get_tsfresh_feature_ids() -> list[str]` — canonical list of feature ids (prefix `tsfresh.`) registered with the feature registry
- `engine/data/jobs/compute_tsfresh_snapshots.py` — job that computes and persists feature snapshots per symbol on a schedule.

**Files to modify.**
- `engine/strategy/feature_registry.py` — add `TSFRESH` group gated by `USE_TSFRESH_FEATURES`, register all feature ids returned by `get_tsfresh_feature_ids()`. Hook into `feature_expansion.py` so they flow through the normal train/serve path.
- `engine/runtime/job_registry.py` — register the new job.

**Historical storage shape.**
```sql
CREATE TABLE IF NOT EXISTS tsfresh_feature_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  ts INTEGER NOT NULL,
  window_s INTEGER NOT NULL,
  features_json TEXT NOT NULL,
  UNIQUE(symbol, ts, window_s)
);
CREATE INDEX IF NOT EXISTS idx_tsfresh_symbol_ts ON tsfresh_feature_snapshots(symbol, ts);
```

**Environment variables.**
- `USE_TSFRESH_FEATURES` (default `0` — opt-in, off by default for safety)
- `TSFRESH_WINDOW_S` (default `3600` — 1 hour rolling window)
- `TSFRESH_FC_PROFILE` (default `minimal` — options: `minimal` / `efficient` / `comprehensive`)
- `TSFRESH_MAX_FEATURES` (default `200` — cap to avoid blowing out schema)

**Acceptance criteria.**
- `pip install tsfresh` added to `requirements.txt`.
- Unit test: `compute_tsfresh_features` on a synthetic sine wave returns non-trivial feature values.
- Integration: run `compute_tsfresh_snapshots` on 1 symbol for 1 day; assert rows land in `tsfresh_feature_snapshots`.
- Train/serve parity: train a schema-aware model with `USE_TSFRESH_FEATURES=1`; assert the model's persisted feature schema includes `tsfresh.*` ids and that live serving uses the same ids.
- Performance: single-symbol computation completes in under 2 seconds on `minimal` profile.

**Constraints.**
- Must be **off by default** (`USE_TSFRESH_FEATURES=0`) — this is a new feature source and should be opt-in.
- Must flow through the existing `feature_registry.py` schema — do not create a parallel feature pipeline.
- When a feature cannot be computed (insufficient window), return NaN and let downstream imputation handle it — do not block.

---

## Prompt 4 — DONE — LightGBM Model Family

**Historical context before implementation.** Same system. At the time, the docs described the current model families as legacy `regime_stats_v2`, `embed_regressor`, and `temporal_predictor` paths using Bayesian stats, Ridge/MLP, and sequence models. LightGBM is an industry-standard gradient boosted decision tree library: fast, non-linear, robust to feature scaling, and strong on tabular problems. Adding it as a new family gave the champion/challenger loop a meaningfully different model to compete.

**Current status.** DONE, and the current stack now includes LightGBM, XGBoost, sklearn GBM, PatchTST, and Ridge ensemble paths.

**Original goal.** Implement `gbm_regressor` as a new schema-aware model family with a train job, serve adapter, champion routing, and registry persistence.

**Files to create.**
- `engine/strategy/gbm_regressor.py`:
  - `_LGB_MAGIC = b"LGB1"` for blob serialization, matching the existing model-blob pattern
  - `train_gbm_model(X, y, feature_ids, hyperparams) -> bytes` — returns a serialized blob (magic + pickled LGBMRegressor + schema)
  - `load_gbm_model(blob: bytes) -> tuple[model, schema]`
  - `predict_with_gbm_model(blob: bytes, feature_snapshot: dict) -> tuple[float, dict]` — returns (point prediction, explain dict with feature importances)
- `engine/strategy/jobs/train_gbm_regressor.py` — training job: pulls labeled dataset, trains, persists blob to `gbm_models` table, writes registry entry with feature schema.

**Files to modify.**
- `engine/strategy/predictor.py` — add a new branch that routes to `gbm_regressor` when the champion assignment points to it. Must respect the same `feature_ids` schema contract as other families.
- `engine/runtime/job_registry.py` — register `train_gbm_regressor`.
- `engine/model_registry.py` — add `gbm_regressor` as an allowed family name.

**Historical storage shape.**
```sql
CREATE TABLE IF NOT EXISTS gbm_models (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  model_name TEXT NOT NULL,
  version TEXT NOT NULL,
  created_ts INTEGER NOT NULL,
  blob BLOB NOT NULL,
  feature_schema_json TEXT NOT NULL,
  training_metrics_json TEXT,
  UNIQUE(model_name, version)
);
CREATE INDEX IF NOT EXISTS idx_gbm_models_name ON gbm_models(model_name, created_ts);
```

**Environment variables.**
- `USE_GBM_REGRESSOR` (default `0`)
- `GBM_MODEL_NAME` (default `gbm_regressor`)
- `GBM_NUM_LEAVES` (default `31`)
- `GBM_LEARNING_RATE` (default `0.05`)
- `GBM_N_ESTIMATORS` (default `200`)
- `GBM_MIN_CHILD_SAMPLES` (default `20`)

**Acceptance criteria.**
- `pip install lightgbm` added to `requirements.txt`.
- Unit test: train on a synthetic dataset, serialize, reload, predict — assert round-trip consistency.
- Integration: train `gbm_regressor` on a labeled dataset used by a schema-aware baseline; assert a row lands in `gbm_models` and registry metrics are updated.
- Integration: set `gbm_regressor` as the active champion for one symbol; call the predictor live and assert a prediction is returned.
- Champion/challenger: run the model marketplace job with both a baseline model and `gbm_regressor` registered; assert both appear in the scoring table.

**Constraints.**
- Must follow the existing schema-persistence and predictor-routing patterns. No novel abstractions.
- Feature schema contract must be identical (same `feature_ids` list format).
- Off by default via `USE_GBM_REGRESSOR=0`.
- Do not modify other model families — this is purely additive.

---

## Prompt 5 — DONE — Stacked Ensemble Blending

**Historical context before implementation.** Same system. At the time, champion/challenger selected a single winning model and used its prediction directly. Best-in-class systems blend many models via stacking: a meta-learner weights the family outputs. This is more robust to regime changes and model-specific failure modes. The existing champion/challenger loop should continue to exist to retire bad models, while live prediction can be a weighted blend.

**Current status.** DONE through the Ridge meta-ensemble and ensemble training jobs in `engine/strategy/ensemble/ridge_meta.py`, `engine/strategy/jobs/train_ensemble_meta.py`, and `engine/strategy/jobs/train_ensemble.py`.

**Goal.** Add an ensemble blender that sits between `predictor.py` and the downstream portfolio logic. It collects predictions from all registered families and returns a blended prediction with multiple blend modes.

**Files to create.**
- `engine/strategy/ensemble_blender.py`:
  - `collect_family_predictions(symbol: str, ts: int) -> dict[str, tuple[float, float]]` — returns `{family_name: (point, variance)}` by calling each adapter
  - `compute_blend_weights(families: list[str], mode: str, regime: str | None = None) -> dict[str, float]` — modes: `equal`, `inverse_variance`, `stacked`, `regime_conditional`
  - `train_stacking_meta_learner(history_rows) -> bytes` — fits a Ridge meta-learner on historical per-family predictions vs. realized labels
  - `blend_predictions(family_preds: dict, weights: dict) -> tuple[float, dict]` — returns (blended point, blend diagnostics including per-family contribution)
  - `prediction_agreement(family_preds: dict) -> float` — returns a [0, 1] disagreement metric (useful for confidence gating)

**Files to modify.**
- `engine/strategy/predictor.py` — when `ENSEMBLE_BLEND_ENABLED=1`, replace single-champion routing with a call to `blend_predictions`. Preserve the same output contract (prediction + explain dict). Log the per-family weights and contributions to the decision log.
- `engine/strategy/jobs/train_ensemble_meta.py` — new job that periodically retrains the stacking meta-learner from historical decision logs.
- `engine/runtime/job_registry.py` — register the new job.

**Historical storage shape.**
```sql
CREATE TABLE IF NOT EXISTS ensemble_blend_weights (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_ts INTEGER NOT NULL,
  mode TEXT NOT NULL,
  regime TEXT,
  weights_json TEXT NOT NULL,
  meta_blob BLOB
);
CREATE TABLE IF NOT EXISTS ensemble_predictions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL,
  ts INTEGER NOT NULL,
  blended_prediction REAL NOT NULL,
  family_preds_json TEXT NOT NULL,
  weights_json TEXT NOT NULL,
  agreement REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS ensemble_family_performance (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  window_start_ts INTEGER NOT NULL,
  window_end_ts INTEGER NOT NULL,
  family TEXT NOT NULL,
  n_predictions INTEGER NOT NULL,
  realized_sharpe REAL,
  hit_rate REAL
);
CREATE INDEX IF NOT EXISTS idx_ensemble_preds_symbol_ts ON ensemble_predictions(symbol, ts);
```

**Environment variables.**
- `ENSEMBLE_BLEND_ENABLED` (default `0` — opt-in for rollback safety)
- `ENSEMBLE_BLEND_MODE` (default `equal` — options: `equal`, `inverse_variance`, `stacked`, `regime_conditional`)
- `ENSEMBLE_MAX_WEIGHT` (default `0.75` — cap per family to prevent any single family dominating)
- `ENSEMBLE_MIN_AGREEMENT` (default `0.0` — if < this, fall back to single champion)
- `ENSEMBLE_META_RETRAIN_S` (default `86400` — retrain stacking meta-learner daily)

**Acceptance criteria.**
- Unit tests: each blend mode on synthetic predictions returns correct weights.
- Unit test: `ENSEMBLE_MAX_WEIGHT` cap is respected (no family exceeds cap even if its `inverse_variance` weight would).
- Integration: with two registered families, set `ENSEMBLE_BLEND_ENABLED=1`; call live predictor; assert a row lands in `ensemble_predictions` and the blended value is a weighted combination of family predictions.
- Integration: train stacking meta-learner on seeded history; assert meta-blob is written to `ensemble_blend_weights`.
- Dashboard: add a panel under the governance view showing current blend weights and per-family contributions.

**Constraints.**
- Off by default (`ENSEMBLE_BLEND_ENABLED=0`) so rollback is a single env-var flip.
- Must respect the same prediction output contract — downstream portfolio logic doesn't change.
- Champion/challenger retirement still runs — families that degrade below promotion gates get dropped from the blend.
- When a family fails to produce a prediction for any reason, re-normalize weights across the remaining families rather than failing the whole call.

---

## Notes for the implementing agent

- The system's train/serve parity contract is sacred. Any new feature or model must round-trip through `engine/strategy/feature_registry.py` with explicit `feature_ids` persisted in the registry metrics.
- Models propose; the runtime gates. Never add a code path that lets a model bypass risk/execution safety.
- Runtime storage is high blast radius. Use existing `engine/runtime/storage.py` patterns for connections, migrations, and writes — do not introduce new DB plumbing.
- The champion/challenger, governance, and promotion loops are the canonical way new models enter production. Work inside them, not around them.
- Off-by-default for any new subsystem. Rollback must be a single env-var flip.
