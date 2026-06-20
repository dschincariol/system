# Codex Prompt 02 — Backtest Realism: Combinatorial Purged CV + Almgren-Chriss Costs

You are working in a Python systematic trading system. The current
backtester (`engine/strategy/jobs/backtest_walk_forward.py`) is, by its own docstring,
"intentionally simple and honest" — fixed walk-forward folds and
constant per-share commission. That simplicity is fine as a smoke test
but it produces optimistic Sharpes because (a) it does not purge
overlapping labels across the train/test boundary and (b) it ignores
market impact entirely. Both problems compound when many strategies are
compared.

Your task is to add a **Combinatorial Purged Cross-Validation** runner
and an **Almgren-Chriss execution-cost model**, and to make them the
default for any model whose holding horizon exceeds one bar.

## Reference

- de Prado, M. L. *Advances in Financial Machine Learning* (2018),
  ch. 7 — purged & embargoed CV, ch. 12 — combinatorial purged CV.
- Almgren, R. & Chriss, N. *Optimal Execution of Portfolio Transactions*
  (2000); Almgren et al. *Direct Estimation of Equity Market Impact*
  (2005) for the empirically calibrated coefficients.

## Goal

1. A reusable CV splitter `CombinatorialPurgedKFold(n_splits, n_test_splits, embargo)`
   compatible with the sklearn splitter interface.
2. A transaction-cost model `AlmgrenChrissCost` returning expected
   slippage in bps as a function of trade size, ADV, volatility, and
   participation rate.
3. A new entrypoint `engine/strategy/jobs/backtest_cpcv.py` that runs CPCV with the
   cost model wired in, produces a distribution of OOS Sharpes (one
   per CPCV path), and emits a deflated Sharpe ratio per de Prado.
4. Training pipelines that consume CV folds (`engine/strategy/jobs/train_model_v2.py`,
   `engine/strategy/jobs/train_temporal_predictor.py`) switch to the
   new splitter for any model with `holding_horizon_bars > 1`.

## Files to read first (read-only)

- `engine/strategy/jobs/backtest_walk_forward.py` — current honest backtester; preserve
  it as a baseline.
- `engine/strategy/jobs/train_model_v2.py` — main training entrypoint; this is where the
  splitter is selected.
- `engine/strategy/jobs/train_temporal_predictor.py` — multi-horizon
  trainer; this one *needs* purging because labels overlap.
- `engine/strategy/feature_registry.py` — to learn what "label horizon"
  means in this system.
- `engine/execution/execution_slicing_engine.py` — current slicer; the
  cost model must be consistent with the slicing logic in production.
- `engine/execution/broker_sim.py` — simulator path; you will plumb the
  cost model into the simulator's fill function.
- `engine/execution/trade_attribution_ledger.py` — to verify cost
  bookkeeping is consistent with live attribution.
- `tests/` — match the existing test style.

## Files to create

- `engine/backtest/__init__.py`
- `engine/backtest/cpcv.py` — `CombinatorialPurgedKFold` splitter,
  `purged_train_indices(...)` helper, `embargo_indices(...)` helper.
- `engine/backtest/deflated_sharpe.py` — deflated Sharpe per
  Bailey & de Prado (2014), takes the distribution of trial Sharpes.
- `engine/execution/cost_models/__init__.py`
- `engine/execution/cost_models/almgren_chriss.py` — temporary +
  permanent impact, configurable participation, square-root market
  impact.
- `engine/strategy/jobs/backtest_cpcv.py` — end-to-end CPCV runner with cost model
  injected; writes one row per path to a new audit table.
- `tests/test_cpcv_splitter.py`
- `tests/test_cpcv_purging.py` — verifies overlapping labels are
  removed.
- `tests/test_almgren_chriss_cost.py`
- `tests/test_deflated_sharpe.py`
- `tests/test_backtest_cpcv_integration.py`

## Files to modify

- `engine/strategy/jobs/train_model_v2.py` — when `cfg.holding_horizon_bars > 1`, use
  `CombinatorialPurgedKFold` instead of `TimeSeriesSplit`.
- `engine/strategy/jobs/train_temporal_predictor.py` — same.
- `engine/execution/broker_sim.py` — accept an optional `cost_model:
  CostModel` parameter; default to the existing flat-bps cost so live
  behavior is unchanged. Production paths can opt in later.
- `engine/runtime/storage.py` — add table `backtest_cpcv_runs(
   id INTEGER PK, ts INTEGER, model_id TEXT, cfg_json TEXT,
   path_index INTEGER, sharpe REAL, deflated_sharpe REAL, n_trials INTEGER,
   total_return REAL, max_drawdown REAL, payload_json TEXT)`.

## Implementation plan

1. **CPCV splitter.** Implement the algorithm exactly as in de Prado
   ch. 12: choose `n_test_splits` of the `n_splits` groups as test;
   purge any training observation whose label window overlaps a test
   group; embargo `int(embargo_pct * n_samples)` observations after each
   test group. Yield `(train_idx, test_idx)` like sklearn.
2. **Deflated Sharpe.** Given a vector of trial Sharpes, compute the
   expected maximum (per Bailey-de Prado), and the deflated p-value
   for the realized best.
3. **Almgren-Chriss cost.** `cost_bps(notional, adv, sigma_daily,
   participation, half_spread_bps)` returns
   `half_spread_bps + eta * sigma_daily * sqrt(notional/adv) +
    gamma * (notional/adv)`, with `eta` and `gamma` calibrated to the
   defaults in Almgren et al. (2005) Table 4 (eta = 0.142, gamma =
   0.314 for US equities) and overrideable per asset class.
4. **Backtest entrypoint.** `engine/strategy/jobs/backtest_cpcv.py` reads a config,
   loads the model, runs CPCV with the cost model in the loop (every
   simulated fill is shrunk by `cost_bps`), aggregates into one row per
   path, computes deflated Sharpe, writes to the new table.
5. **Wire-in.** Trainers select CPCV when horizon > 1. Default behavior
   for live execution is unchanged unless callers explicitly opt in.

## Acceptance criteria

- [ ] `CombinatorialPurgedKFold(n_splits=6, n_test_splits=2)` yields
      C(6,2) = 15 splits, each with the correct purged train indices
      verified by an explicit test (overlapping label windows removed).
- [ ] Embargo correctly excludes the configured fraction of post-test
      observations (verified with a synthetic time index).
- [ ] On a synthetic momentum DGP with known Sharpe = 1.2, CPCV
      reports a sample-mean Sharpe within ±0.15 of 1.2 across 50 seeds;
      walk-forward over the same data systematically biases higher
      (assert mean walk-forward Sharpe > mean CPCV Sharpe — this is the
      known overstatement and our test asserts it).
- [ ] Almgren-Chriss `cost_bps` is monotonic in notional, monotonic in
      participation, and matches a hand-computed value to 1e-6 bps for
      a fixed set of inputs (recorded in the test).
- [ ] Deflated Sharpe collapses to raw Sharpe when `n_trials=1`.
- [ ] `engine/strategy/jobs/backtest_cpcv.py` runs to completion on
      `engine/strategy/temporal_predictor.py` against the last 18 months
      of cached prices (use whatever the existing walk-forward test
      uses) and writes ≥ 15 rows to `backtest_cpcv_runs`.
- [ ] Old walk-forward backtester still passes its own tests unchanged.

## Test plan

- `tests/test_cpcv_splitter.py` — synthetic indices, count splits, fold
  size, no leakage.
- `tests/test_cpcv_purging.py` — labels with explicit overlap windows;
  assert overlap removed.
- `tests/test_almgren_chriss_cost.py` — monotonicity, hand-computed
  reference value, asset-class override.
- `tests/test_deflated_sharpe.py` — `n_trials=1` ≡ raw Sharpe; large
  `n_trials` shrinks Sharpe.
- `tests/test_backtest_cpcv_integration.py` — end-to-end on the
  smallest possible fixture; assert ≥ 1 row written, sane ranges.

Run: `pytest -q tests/test_cpcv_splitter.py tests/test_cpcv_purging.py
tests/test_almgren_chriss_cost.py tests/test_deflated_sharpe.py
tests/test_backtest_cpcv_integration.py`

## Out of scope

- Do not delete `engine/strategy/jobs/backtest_walk_forward.py`; it remains the simple
  smoke-test backtester.
- Do not change live execution costs. The Almgren-Chriss model is
  available to `broker_sim` but live paths keep their current cost
  accounting until a follow-up prompt promotes the model end-to-end.
- Do not introduce a new ML library. Use numpy / pandas / the project's
  existing dependencies only.
