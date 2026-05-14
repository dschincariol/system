# Codex Prompt 07 — Automated Feature Discovery (tsfresh + PySR)

You are working in a Python systematic trading system whose feature
catalog is **hand-curated**. Every column in `feature_registry` was
written by a human; the system has no automated mechanism to propose,
test, and accept new features at scale. The user's stated north star is
*autonomous alpha discovery* — that begins here. This prompt adds two
complementary discovery engines:

- `tsfresh` for **exhaustive statistical feature extraction** from
  raw time series (~700 candidate features per series, automatic
  relevance filtering).
- `PySR` for **symbolic regression** that proposes interpretable
  algebraic combinations of existing features.

Both feed the **same acceptance gate** added in prompt 01 (BH-FDR + t>3).
Without that gate, this prompt produces noise; **prompt 01 is a hard
prerequisite**.

## Goal

1. A discovery framework that runs as a job, generates candidate
   features, evaluates them on OOS data, applies the BH-FDR + t>3
   gate from prompt 01, and registers survivors.
2. Two engines on day one: `TsfreshDiscoverer` and `PySRDiscoverer`.
3. A persistent record of every candidate ever proposed, accepted, or
   rejected, with the full statistical evidence.
4. **No automatic live deployment**: accepted features land in
   `feature_registry` at stage `shadow` and require a human-confirmed
   promotion to enter the live serving path.

## Files to read first (read-only)

- `engine/strategy/feature_registry.py` — feature schema; the contract
  every accepted feature must satisfy.
- `engine/strategy/feature_expansion.py` — existing manual expansion;
  pattern reference.
- `engine/strategy/statistics/multiple_testing.py` and
  `engine/strategy/statistics/factor_threshold.py` — the gates from
  prompt 01.
- `engine/strategy/promotion_audit.py` — audit trail conventions.
- `engine/runtime/storage.py` — schema patterns.
- `engine/runtime/job_registry.py` — job allowlist.

## Files to create

- `engine/strategy/discovery/__init__.py`
- `engine/strategy/discovery/base.py` — `Discoverer` Protocol:
  `propose(symbol, train_df) -> list[CandidateFeature]`,
  `evaluate(candidate, test_df, target) -> EvaluationResult`.
- `engine/strategy/discovery/tsfresh_discoverer.py` — wraps tsfresh.
  Runs `extract_features` with the comprehensive parameter set on a
  rolling window; relevance-filtered with the project's gate, not
  tsfresh's `select_features` (which uses a different criterion).
- `engine/strategy/discovery/pysr_discoverer.py` — wraps PySR. Search
  budget bounded (default 100 iterations, max complexity 12). Operator
  set restricted to `+ - * /` plus `log abs sqrt` to avoid
  ill-conditioned forms.
- `engine/strategy/discovery/registry.py` — persistence layer for
  candidate features and decisions.
- `engine/strategy/jobs/discover_features.py` — discovery job:
  iterate symbols, run discoverers, evaluate, gate, register.
- `tests/test_tsfresh_discoverer.py`
- `tests/test_pysr_discoverer.py`
- `tests/test_discovery_registry.py`
- `tests/test_discovery_integration.py`

## Files to modify

- `engine/strategy/feature_registry.py` — add a `stage` column
  (`shadow|live`) per feature; new discoverer-accepted features
  default to `shadow`.
- `engine/runtime/storage.py` — add tables:
  - `feature_candidates(id INTEGER PK, ts INTEGER, source TEXT,
    symbol TEXT, expression TEXT, params_json TEXT, hash TEXT UNIQUE)`
  - `feature_evaluation(candidate_id INTEGER, ts INTEGER, t_stat REAL,
    p_value REAL, q_value REAL, oos_ic REAL, decision TEXT,
    PRIMARY KEY(candidate_id, ts))`.
- `engine/runtime/job_registry.py` — register `discover_features` job.

## Implementation plan

1. **Hashing.** Every candidate has a content hash so duplicate
   proposals are not re-evaluated. tsfresh hashes its
   parameter-tuple; PySR hashes the simplified SymPy expression.
2. **tsfresh.** Run the comprehensive parameter set on rolling
   180-bar windows; return one candidate per surviving column.
3. **PySR.** Initialize population from current registry features as
   primitives. Constrain complexity ≤ 12. Return the top-k by PySR's
   internal score; the *acceptance* decision is then ours, not PySR's.
4. **Evaluation.** For each candidate: compute it on OOS data, score
   `Information Coefficient` against forward returns, run the HAC
   t-stat from prompt 01, batch p-values for BH-FDR.
5. **Gate.** Only candidates passing `q < 0.10` AND `|t| > 3.0` are
   inserted into `feature_registry` at stage `shadow`.
6. **Audit.** Every candidate, accepted or rejected, gets an entry in
   `feature_evaluation`. This is the experiment log.
7. **No silent live promotion.** Promotion from `shadow` to `live`
   requires a separate explicit job invocation
   (`engine/strategy/jobs/promote_features.py`) which the human
   triggers; the discovery job never promotes.

## Acceptance criteria

- [ ] tsfresh runs end-to-end on a synthetic price series and proposes
      ≥ 50 candidates.
- [ ] PySR runs end-to-end with a 30-second budget and returns ≥ 5
      candidates of complexity ≤ 12.
- [ ] Every accepted candidate has a row in `feature_candidates` and
      a passing row in `feature_evaluation`.
- [ ] Every rejected candidate has a row in `feature_evaluation` with
      `decision in ('fdr_failed','tstat_failed','degenerate')`.
- [ ] No candidate is registered into `feature_registry` with stage =
      `live` by the discovery job. A test asserts the absence.
- [ ] Re-running the discovery job is a no-op for previously-evaluated
      candidates (hashes match).

## Test plan

- `tests/test_tsfresh_discoverer.py` — synthetic series → ≥ N
  candidates with stable hashes.
- `tests/test_pysr_discoverer.py` — short-budget search returns
  candidates; complexity ceiling honored.
- `tests/test_discovery_registry.py` — insert / dedup / read; hash
  uniqueness enforced.
- `tests/test_discovery_integration.py` — full pipeline on a tiny
  fixture: discover → evaluate → gate → register; confirm only gated
  survivors land in the registry at `shadow`.

Run: `pytest -q tests/test_tsfresh_discoverer.py
tests/test_pysr_discoverer.py tests/test_discovery_registry.py
tests/test_discovery_integration.py`

## Out of scope

- Do not add `gplearn` in this prompt; PySR is a strict superset for
  our purposes and supporting two symbolic engines is gratuitous.
- Do not weave a deep-learning feature discovery (autoencoders, etc.).
- Do not auto-promote shadow features to live. That gate stays
  manual until a follow-up prompt builds the auto-promotion machinery
  with proper champion/challenger semantics for features.
- Do not exceed a 10-minute wall-clock budget per symbol for PySR.

## Hard prerequisite

Prompt 01 (statistical-rigor gates) **must be merged first**. Without
the FDR gate, this prompt produces a fire-hose of false positives.
