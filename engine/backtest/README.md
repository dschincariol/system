# Backtest Subsystem

The `engine/backtest/` package owns the reusable, leakage-aware statistical primitives behind the promotion gate: a combinatorial purged cross-validation splitter and the Bailey-de Prado deflated Sharpe diagnostics. These are dependency-light building blocks (numpy + stdlib only, no storage or env reads) consumed by training jobs, model rankers, factor discovery, and the CPCV/gated promotion-backtest workflow that produces gated backtest evidence for champion/challenger promotion.

## Files

- [cpcv.py](cpcv.py)
  `CombinatorialPurgedKFold`, an sklearn-shaped `split(X, y, groups)` CPCV splitter that purges train samples whose label windows overlap a test interval and applies a post-test embargo. Exposes the standalone `purged_train_indices` and `embargo_indices` helpers it is built from.
- [deflated_sharpe.py](deflated_sharpe.py)
  Deflated Sharpe Ratio (DSR) utilities: `deflated_sharpe_ratio` returns a `DeflatedSharpeResult` (raw/deflated Sharpe, expected-max Sharpe, DSR probability and upper-tail p-value), backed by `expected_max_sharpe` for the multiple-trials inflation term.
- [__init__.py](__init__.py)
  Re-exports the CPCV splitter and its purge/embargo helpers as the package surface.

## CombinatorialPurgedKFold contract

The dataclass defaults are `n_splits=6`, `n_test_splits=2`, `embargo=0.0`, `label_horizon=0`. `get_n_splits` returns `C(n_splits, n_test_splits)` — the number of test-group combinations, not a single held-out fold. Construction validates `n_splits >= 2`, `1 <= n_test_splits < n_splits`, and non-negative embargo.

Label windows (the interval over which a sample's target is observed) drive purging and may be supplied three ways: constructor `label_start_times`/`label_end_times`, the sklearn `groups` argument (dict with `label_start`/`label_end` or `start`/`end`, a 2-tuple/list, or a single end-times array), or an integer `label_horizon` fallback applied to the sample index. `embargo` is a fraction of total samples when `0 < embargo < 1`, otherwise an absolute observation count.

## Deflated Sharpe contract

`deflated_sharpe_ratio(trial_sharpes, ...)` takes the cross-trial Sharpe distribution plus an optional `realized_sharpe`, `n_trials`, `n_observations`, `skew`, and `kurtosis` (default `3.0`). `deflated_sharpe` is the realized Sharpe minus the expected maximum under repeated trials (kept as an adjusted Sharpe value for caller compatibility, collapsing to the raw Sharpe at a single trial); `probability` is the DSR probability and `p_value` its upper tail. With `n_observations` supplied it uses the moment-corrected DSR variance term; otherwise it falls back to the trial-Sharpe standard deviation.

## Relationship to engine/strategy/cpcv.py

This package holds the reusable primitives; `engine/strategy/cpcv.py` is the job-level orchestrator that drives the actual promotion backtest. `engine/strategy/cpcv.py` imports `deflated_sharpe_ratio` from here and layers on storage, env configuration, transaction-cost models, retrain-cadence replay, gated backtesting, and PBO (`cpcv_backtest`, `compute_pbo`, `run_backtest_cpcv_job`). Note that the strategy module carries its own array-based split/purge/embargo helpers (`make_cpcv_splits`, `purge_train_indices`, `embargo_train_indices`) rather than the splitter here; the sklearn-shaped `CombinatorialPurgedKFold` in this package is instead consumed by `engine/strategy/jobs/backtest_cpcv.py`, `train_model_v2`, `train_temporal_predictor`, `meta_labeling`, `models/lgbm_ranker.py`, and `discovery/llm_factor_generator.py`.
