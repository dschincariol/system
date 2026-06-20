# Codex Prompt 03 — Optuna-Driven Hyperparameter Optimization

You are working in a Python systematic trading system that today carries
**200+ hardcoded `os.getenv(...)` thresholds** controlling model
training, feature gating, risk caps, and execution policy. They were
hand-tuned over time and are scattered across the code with no audit
trail. Your task is to introduce **Optuna-based Bayesian optimization**
as the standard mechanism for tuning *training-time* hyperparameters
(not runtime risk caps — those stay explicit), with persistent study
storage so trials accumulate across runs.

## Goal

1. A reusable training-time tuning framework: `engine/strategy/tuning/`.
2. **Persistent Optuna studies** stored in the existing SQLite database
   so studies survive process restarts and accumulate trials across
   training jobs.
3. Convert two model trainers as canonical examples:
   - `engine/strategy/temporal_predictor.py`
   - `engine/strategy/embed_regressor.py`
4. A registry / catalog of "managed" hyperparameters so the project can
   migrate the rest of the env-var bestiary incrementally without
   losing track.

## Files to read first (read-only)

- `engine/strategy/jobs/train_model_v2.py` — main training entrypoint.
- `engine/strategy/temporal_predictor.py` — current temporal model;
  contains hardcoded numeric defaults you will expose as search ranges.
- `engine/strategy/embed_regressor.py` — current embed model; same.
- `engine/strategy/jobs/train_temporal_predictor.py` — job wrapper.
- `engine/strategy/jobs/train_embed_models.py` — job wrapper.
- `engine/runtime/storage.py` — schema patterns; you will add a small
  table for per-symbol best-trial caching.
- `engine/runtime/job_registry.py` — to add the new tuning job.
- `tests/test_model_competition_real_pnl.py` — existing patterns for
  testing trainers.
- A sweep of every `os.getenv(...)` call: produce a full inventory in
  `docs/hyperparameter_inventory.md` as part of this task (read-only
  pass first; document it before changing anything).

## Files to create

- `engine/strategy/tuning/__init__.py`
- `engine/strategy/tuning/study.py` — wrapper that opens / creates an
  Optuna study with `sqlite:///` storage pointing at the project DB
  path; convention: study name is `<model_family>_<symbol_or_global>`.
- `engine/strategy/tuning/objective.py` — an objective protocol; each
  model family supplies a `build_objective(symbol, train, valid) ->
  Callable[[Trial], float]` returning OOS validation Sharpe (or
  negative RMSE for regression-only models).
- `engine/strategy/tuning/catalog.py` — declarative catalog of every
  managed hyperparameter: `name, model_family, dtype, low, high, log,
  default`. The single source of truth for search spaces.
- `engine/strategy/jobs/tune_models.py` — job that picks N candidate
  symbols and runs `optuna.optimize(...)` for `n_trials` (default 50)
  with a wall-clock budget. Persists best trial per symbol.
- `tests/test_tuning_study.py`
- `tests/test_tuning_catalog.py`
- `tests/test_tune_temporal_integration.py` — small end-to-end on a
  synthetic dataset, n_trials = 5, asserts study persists and is
  resumable.
- `docs/hyperparameter_inventory.md` — full enumeration of all
  `os.getenv` constants with: variable name, file:line, current
  default, role (training / runtime / safety), recommended action
  (migrate / keep / deprecate).

## Files to modify

- `engine/strategy/temporal_predictor.py` — accept a `params: dict`
  argument; remove direct `os.getenv` lookups for tuneable knobs.
  Defaults come from `tuning/catalog.py`.
- `engine/strategy/embed_regressor.py` — same pattern.
- `engine/strategy/jobs/train_temporal_predictor.py` — pull best-known
  params from the latest completed Optuna study before training; if
  none exist, use catalog defaults.
- `engine/strategy/jobs/train_embed_models.py` — same.
- `engine/runtime/storage.py` — add table
  `model_best_params(model_family TEXT, symbol TEXT, ts INTEGER,
   study_name TEXT, params_json TEXT, value REAL,
   PRIMARY KEY(model_family, symbol))`.
- `engine/runtime/job_registry.py` — register the new tuning job in the
  allowlist with its own cron-style cadence (default: weekly).

## Implementation plan

1. **Inventory pass.** Grep every `os.getenv` in `engine/` and `ops/`,
   classify each into `training`, `runtime`, or `safety`. Only
   `training` items are migration candidates for this prompt. Write the
   full table into `docs/hyperparameter_inventory.md`.
2. **Catalog.** Express the search space declaratively. A `Hyperparam`
   dataclass with sampling helpers `suggest(trial)` so the objective
   can iterate without hardcoding distributions.
3. **Study persistence.** Use Optuna's RDB backend pointed at the
   project SQLite DB (`sqlite:///<path>`). Studies are created with
   `direction="maximize"` and `pruner=MedianPruner(n_startup_trials=5)`.
4. **Objective.** For temporal_predictor: train on the train slice,
   evaluate on the validation slice, return OOS Sharpe. For
   embed_regressor: return negative validation MSE. The objective must
   be deterministic given a seed.
5. **Tuning job.** Reads `tuning_universe` env var (e.g. top-50
   liquidity-ranked symbols), runs N trials each, persists the best
   params to `model_best_params`. Idempotent: re-running resumes the
   study.
6. **Trainer wiring.** At training time, look up the latest row in
   `model_best_params` for `(model_family, symbol)`; fall back to
   `catalog.defaults()`.

## Acceptance criteria

- [ ] `docs/hyperparameter_inventory.md` exists and lists every
      `os.getenv` in `engine/` and `ops/` with classification.
- [ ] `temporal_predictor` and `embed_regressor` no longer call
      `os.getenv` for any parameter listed in the catalog.
- [ ] An Optuna study, once created, is resumable: rerunning the tuning
      job continues trial numbering rather than starting at zero.
- [ ] Trial seeds are recorded; rerunning a single trial reproduces
      its objective value to 1e-9.
- [ ] If no completed study exists for `(family, symbol)`, the trainer
      uses `catalog.defaults()` and logs a warning with the family /
      symbol pair.
- [ ] `pytest -q` covering the new tests passes; existing trainer
      tests pass unchanged.

## Test plan

- `tests/test_tuning_catalog.py` — catalog round-trips through JSON;
  `Hyperparam.suggest` calls the right Optuna distribution.
- `tests/test_tuning_study.py` — create study; close; reopen; trial
  count is preserved.
- `tests/test_tune_temporal_integration.py` — synthetic data,
  `n_trials=5`, study writes to a tmp DB, best params land in
  `model_best_params`, second run resumes.
- Existing tests in `tests/test_model_competition_real_pnl.py` must
  still pass without modification.

Run: `pytest -q tests/test_tuning_study.py tests/test_tuning_catalog.py
tests/test_tune_temporal_integration.py
tests/test_model_competition_real_pnl.py`

## Out of scope

- Do not migrate runtime risk caps (`MAX_GROSS_EXPOSURE`,
  `KILL_SWITCH_DRAWDOWN`, etc.). Risk thresholds remain explicit human
  decisions and must stay as code-level constants or env vars.
- Do not migrate execution-policy thresholds (TTL, alpha-decay rates) —
  those go through their own adaptive-policy machinery and are out of
  scope here.
- Do not introduce a new optimizer (Hyperopt, Ray Tune). Optuna only.
- Do not change the cadence of training jobs. Tuning is its own job.
