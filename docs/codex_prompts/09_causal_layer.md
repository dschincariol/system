# Codex Prompt 09 — Causal Layer (Granger Causality + DoWhy)

You are working in a Python systematic trading system whose entire
modeling pipeline is **correlational**. Models predict returns from
features that are statistically associated with them, with no
machinery to interrogate whether those associations would survive
intervention. This is fine for short-horizon prediction but it is the
weakest leg of the system on regime breaks: a feature that worked
because of a spurious common driver fails the moment the driver
shifts. This prompt introduces a **causal diagnostics layer** that
runs alongside (never inside) the prediction path.

The layer is **diagnostic, not directive** in this iteration: it
records causal evidence per feature, surfaces it in promotion audits,
and supplies a downweighting signal to the ensemble blender. It does
not unilaterally veto features or models. A future prompt can promote
causal scores to gating status once the layer has accumulated track
record.

## Goal

1. A `engine/causal/` subpackage with two analyses:
   - **Granger causality** (multivariate, with HAC variance) for every
     candidate feature → forward-return pair.
   - **Backdoor-adjusted effect estimation** via DoWhy / EconML for a
     curated set of `(treatment, outcome, confounders)` graphs.
2. Persistent storage of causal scores keyed by
   `(feature, target, window)`.
3. Surfacing in `promotion_audit` and the dashboard.
4. A non-binding **causal weight** that the ensemble blender (prompt
   06) consults to softly downweight features with weak causal
   evidence.

## Files to read first (read-only)

- `engine/strategy/feature_registry.py` — feature catalog.
- `engine/strategy/promotion_audit.py` — audit conventions.
- `engine/strategy/promotion_guard.py` — to understand the gate
  surface (this prompt does *not* add a gate; it adds an input).
- `engine/strategy/ensemble/ridge_meta.py` (after prompt 06) — the
  blender; this prompt extends it with an optional prior on weights.
- `engine/runtime/storage.py` — schema patterns.
- `engine/runtime/job_registry.py` — job allowlist.
- The latest validation / training pipeline modules to understand how
  forward returns are constructed and labeled.

## Files to create

- `engine/causal/__init__.py`
- `engine/causal/granger.py` — multivariate Granger via VAR with HAC
  covariance; returns F-stat, p-value, optimal lag (BIC-selected).
- `engine/causal/dag.py` — graph specification dataclass: nodes,
  edges, treatment, outcome, confounder set. JSON-serializable.
- `engine/causal/dowhy_runner.py` — wraps DoWhy. Optional dependency:
  if `dowhy` is missing, the module imports cleanly and records
  `decision='skipped_no_dependency'`.
- `engine/causal/scores.py` — per-feature causal score in [0, 1]
  derived from Granger p-values and DoWhy effect sizes (precise
  formula in implementation plan).
- `engine/strategy/jobs/causal_scoring.py` — job that iterates
  registered features, runs Granger, runs DoWhy where a graph exists,
  writes scores.
- `tests/test_causal_granger.py`
- `tests/test_causal_dag.py`
- `tests/test_causal_scoring_integration.py`

## Files to modify

- `engine/runtime/storage.py` — add tables:
  - `causal_scores(feature TEXT, target TEXT, window TEXT, ts INTEGER,
    granger_p REAL, granger_lag INTEGER, dowhy_effect REAL NULL,
    dowhy_p REAL NULL, score REAL, decision TEXT,
    PRIMARY KEY(feature, target, window, ts))`
  - `causal_dags(name TEXT PK, dag_json TEXT, created_ts INTEGER)`
- `engine/strategy/promotion_audit.py` — include the latest causal
  score per feature in the audit payload.
- `engine/runtime/job_registry.py` — register `causal_scoring` job.
- `engine/strategy/ensemble/ridge_meta.py` (if prompt 06 is merged) —
  optional `prior_weights: dict[str, float]` parameter that biases
  the ridge solution toward features with high causal score (Tikhonov
  with a non-zero center). Default behavior unchanged when no priors.

## Implementation plan

1. **Granger.** Standard F-test on a VAR(p), p chosen by BIC over
   `[1, p_max]` with `p_max = 10`. HAC covariance via Newey-West with
   `lag = floor(4 * (T/100)^(2/9))`. Returns
   `GrangerResult(p_value, lag, F)`.
2. **DoWhy.** Each named DAG in `causal_dags` is loaded; the DoWhy
   pipeline runs with a back-door identification → linear
   regression estimator → bootstrap refutation. Effect estimates and
   refuter p-values are persisted.
3. **Score.** A simple monotone composition:
   `score = 0.5 * sigmoid(-log10(granger_p) - 1.5)
          + 0.5 * sigmoid(abs(dowhy_t) - 2.0)`,
   with `dowhy_t = effect / se(effect)`, defaulting `dowhy_t = 0` when
   no DAG exists. Score ∈ [0, 1]; ≥ 0.5 implies non-trivial causal
   evidence.
4. **Job.** Iterates features × forward-return targets × windows
   (`window in {30d, 90d, 365d}`). Writes one row per combo.
5. **Audit.** When a model is proposed for promotion, the audit
   payload includes the latest causal score for each feature it uses.
   The promotion guard does not veto on causal score; it surfaces it.
6. **Ensemble prior.** When the blender refits, it can be passed a
   prior derived from causal scores. Default behavior is unchanged
   (no prior). When supplied, the ridge solution is regularized
   toward the prior weights with strength `lambda_prior` (Optuna-
   tuned per prompt 03).

## Acceptance criteria

- [ ] Granger module returns deterministic results given a fixed seed
      and matches a hand-computed example to 1e-6.
- [ ] On a synthetic DGP where x Granger-causes y, the test rejects
      H0 in ≥ 95% of seeds at α = 0.05; on a non-causal DGP, the
      false-positive rate is ≤ 6%.
- [ ] DoWhy module degrades cleanly to `decision='skipped_no_dependency'`
      when `dowhy` is uninstalled — no ImportError reaches the caller.
- [ ] Causal score is in [0, 1]; ≥ 0.5 ↔ non-trivial evidence in a
      test fixture.
- [ ] `promotion_audit` payload contains `causal_scores: {feature:
      score}` for every feature in the proposed model.
- [ ] Ensemble blender's behavior with `prior_weights=None` is
      bit-identical to the prompt-06 baseline (regression-tested).
- [ ] No production prediction path imports `dowhy`.

## Test plan

- `tests/test_causal_granger.py` — synthetic causal vs non-causal
  series; rejection rates inside the stated bounds.
- `tests/test_causal_dag.py` — DAG round-trips through JSON; cycles
  rejected.
- `tests/test_causal_scoring_integration.py` — end-to-end on a tiny
  fixture; rows land in `causal_scores`; audit payload includes the
  scores.

Run: `pytest -q tests/test_causal_granger.py tests/test_causal_dag.py
tests/test_causal_scoring_integration.py`

## Out of scope

- Do not auto-veto features on causal score in this iteration.
- Do not introduce structure-discovery algorithms (PC, GES) that
  infer DAGs from data. DAGs are human-curated for now; auto-discovery
  is a future prompt.
- Do not run causal analyses inside prediction or training inner loops.
- Do not make `dowhy` a hard dependency.
