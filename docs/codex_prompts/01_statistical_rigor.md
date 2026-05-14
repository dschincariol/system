# Codex Prompt 01 — Statistical Rigor in Feature & Model Acceptance

You are working in a Python systematic trading system. Your task is to
introduce **rigorous multiple-hypothesis-testing controls** into the
feature and model acceptance pipeline. Today, new features and new
champion models are accepted on raw RMSE / bias / hit-rate gates without
any control for the false-discovery rate that arises from screening
hundreds of candidates. This is the single largest source of overfit
risk in the system.

## Goal

Add three peer-reviewed acceptance gates to the promotion pipeline:

1. **Benjamini-Hochberg FDR control** for batches of candidate features
   or factors evaluated together (target FDR q = 0.10).
2. **Harvey-Liu-Zhu single-test threshold** for any new factor: require
   `|t-stat| > 3.0` on out-of-sample returns before the factor can be
   added to `feature_registry`.
3. **White's Reality Check (or the Hansen SPA test)** for comparing a
   challenger model's out-of-sample Sharpe to the incumbent champion
   under the null of no superior performance.

The gates must be **non-bypassable** (no env-var escape hatch) and must
write their full statistical evidence (p-values, t-stats, q-values,
bootstrap distributions) into a new audit table so every promotion
decision is reconstructable.

## Files to read first (read-only)

- `engine/strategy/promotion_guard.py` — current acceptance gates.
- `engine/strategy/feature_registry.py` — feature schema and groups; the
  point at which a new feature becomes "live."
- `engine/strategy/promotion_audit.py` — existing audit trail; you will
  extend its schema, not replace it.
- `engine/model_registry.py` — champion / challenger / shadow lifecycle.
- `engine/strategy/champion_manager.py` — actual promotion logic.
- `ops/train_model_v2.py` — training entry point that produces the OOS
  metrics the new gates will consume.
- `engine/runtime/storage.py` — schema versioning conventions; you will
  add a new table.
- `tests/test_audit_invariants.py` and `tests/test_model_competition_real_pnl.py`
  for the testing patterns this project uses.

## Files to create

- `engine/strategy/statistics/__init__.py`
- `engine/strategy/statistics/multiple_testing.py` — BH-FDR, Bonferroni,
  Holm. Pure-numpy; no scipy beyond `scipy.stats`.
- `engine/strategy/statistics/reality_check.py` — White's Reality Check
  via stationary bootstrap (Politis-Romano). 10 000 bootstrap replicates
  by default.
- `engine/strategy/statistics/factor_threshold.py` — Newey-West HAC
  t-statistic with the Harvey-Liu-Zhu 3.0 threshold helper.
- `tests/test_statistics_multiple_testing.py`
- `tests/test_statistics_reality_check.py`
- `tests/test_statistics_factor_threshold.py`
- `tests/test_promotion_guard_fdr.py` — integration test for the gate.

## Files to modify

- `engine/strategy/promotion_guard.py` — call the new gates; refuse
  promotion on failure.
- `engine/strategy/promotion_audit.py` — persist p-values, q-values,
  bootstrap distributions.
- `engine/runtime/storage.py` — add table
  `promotion_statistical_evidence` with columns:
  `id INTEGER PK, ts INTEGER, model_id TEXT, feature_id TEXT NULL,
   test_name TEXT, t_stat REAL, p_value REAL, q_value REAL NULL,
   bootstrap_samples INTEGER NULL, decision TEXT, payload_json TEXT`.
- `ops/train_model_v2.py` — emit the OOS-return series (not just
  aggregate metrics) so Reality Check can bootstrap it.

## Implementation plan

1. Implement the three statistics modules with **deterministic seeding**
   (every function takes a `random_state` argument; default 42).
2. Each module has a "primary" function that returns a structured
   dataclass: `MultipleTestResult`, `RealityCheckResult`,
   `FactorThresholdResult`. No bare tuples.
3. Wire `promotion_guard.assess_challenger(...)` to require:
   - Reality-Check p-value < 0.05 vs current champion, **and**
   - if the challenger introduces new features: BH-FDR q < 0.10 on the
     batch, and every kept feature has Newey-West |t| > 3.0.
4. Persist evidence to `promotion_statistical_evidence` inside the same
   transaction as the promotion decision (use `storage.connection()` and
   the existing transaction pattern; do not invent a new connection
   manager).
5. Update `model_registry.promote()` to read the latest evidence row and
   refuse the state transition if `decision != 'pass'`.

## Acceptance criteria

- [ ] BH-FDR implementation matches the reference table on
      Benjamini & Hochberg (1995), Table 1, within 1e-12.
- [ ] Reality-Check p-values match White's published values on the
      built-in synthetic test in `tests/test_statistics_reality_check.py`.
- [ ] HAC t-stat matches `statsmodels.stats.sandwich_covariance.cov_hac`
      to 1e-6 on a 1 000-point synthetic series (kept as a regression
      anchor; the production code does not depend on statsmodels).
- [ ] No env var, config flag, or CLI option can bypass any of the three
      gates. The only way to promote without them is to delete the gate
      in code, which a grep for `STATS_BYPASS|skip_fdr|disable_reality`
      must not find.
- [ ] Every promotion attempt writes exactly one row per applicable test
      to `promotion_statistical_evidence`, success or failure.
- [ ] All new modules have docstrings citing the relevant paper
      (Benjamini & Hochberg 1995; White 2000; Harvey, Liu & Zhu 2016).

## Test plan

Each new module has a unit test covering:

- **Multiple testing:** known p-value vector → expected q-values; edge
  cases (all p=1, all p=0, single p-value).
- **Reality Check:** synthetic two-strategy comparison with known DGP;
  100 trials, false-positive rate must be ≤ 6% at α = 0.05.
- **Factor threshold:** OLS slope of y = 2x + ε with 1 000 points
  produces |t| ≫ 3; pure noise produces |t| < 3 in ≥ 95% of seeds.
- **Promotion guard integration:** mock a challenger that beats champion
  by lucky chance (high Sharpe, p > 0.10) and verify promotion is
  refused; one with p < 0.05 promotes.

Run: `pytest -q tests/test_statistics_multiple_testing.py
tests/test_statistics_reality_check.py
tests/test_statistics_factor_threshold.py
tests/test_promotion_guard_fdr.py`

## Out of scope

- Do not change the existing RMSE / bias gates; the new tests add to
  them, they do not replace them.
- Do not touch `feature_expansion.py` or auto-feature generation; the
  acceptance gate runs *after* candidates exist, regardless of how they
  were produced.
- Do not introduce a new database (Postgres / Timescale); the new table
  goes into the existing SQLite schema. A separate prompt handles the
  storage layer.
