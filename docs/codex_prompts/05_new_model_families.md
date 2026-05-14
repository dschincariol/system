# Codex Prompt 05 — Add LightGBM, XGBoost, and PatchTST Model Families

You are working in a Python systematic trading system whose champion /
challenger competition currently runs across **three** predictive
families: `regime_stats_v2`, `embed_regressor`, and `temporal_predictor`.
That is a thin design space. Two near-free wins on the modeling side
are gradient-boosted trees (LightGBM, XGBoost) for tabular features and
a transformer (PatchTST) for multi-horizon return forecasting. Adding
them as **first-class members of the existing competition** — not as
ad-hoc scripts — is the goal of this prompt.

## Goal

1. Three new model families plugged into the existing model registry,
   trainer router, predictor router, and promotion pipeline:
   - `lgbm_regressor` — LightGBM tabular regressor
   - `xgb_regressor` — XGBoost tabular regressor
   - `patchtst` — PatchTST sequence model
2. They must respect the project's existing **schema-driven train/serve
   parity** convention via `feature_registry`.
3. They must be **shadow-only on first deploy** and earn promotion via
   the same `promotion_guard` gates every other family uses.
4. They must integrate with the new statistical-rigor gates from
   prompt 01 if those are merged.

## Files to read first (read-only)

- `engine/strategy/feature_registry.py` — feature schema & versioning;
  the contract every new family must honor.
- `engine/model_registry.py` — registry; new families register here.
- `engine/strategy/predictor.py` — predictor router; pluggable family
  surface.
- `engine/strategy/champion_manager.py` — champion / challenger logic.
- `engine/strategy/temporal_predictor.py` — reference for sequence
  models; PatchTST should mirror this surface.
- `engine/strategy/embed_regressor.py` — reference for tabular models;
  LightGBM / XGBoost mirror this surface.
- `engine/strategy/promotion_guard.py` — promotion gates.
- `engine/strategy/jobs/train_temporal_predictor.py` and
  `engine/strategy/jobs/train_embed_models.py` — job patterns for
  trainers; new families need analogous jobs.
- `engine/runtime/job_registry.py` — allowlist of jobs.
- `tests/test_model_competition_real_pnl.py` — existing competition
  test pattern.

## Files to create

- `engine/strategy/models/__init__.py`
- `engine/strategy/models/lgbm_regressor.py` — wraps LightGBM with the
  project's `Trainer` / `Predictor` protocol; honors feature schema.
- `engine/strategy/models/xgb_regressor.py` — same for XGBoost.
- `engine/strategy/models/patchtst.py` — PatchTST in PyTorch (Yuqi Nie
  et al. 2023). Patch length 16, stride 8, 3 layers, 4 heads default.
- `engine/strategy/jobs/train_lgbm_models.py`
- `engine/strategy/jobs/train_xgb_models.py`
- `engine/strategy/jobs/train_patchtst_models.py`
- `tests/test_lgbm_regressor.py`
- `tests/test_xgb_regressor.py`
- `tests/test_patchtst_model.py`
- `tests/test_new_families_competition_integration.py` — runs the four
  new families through champion / challenger end-to-end on a small
  synthetic dataset.

## Files to modify

- `engine/model_registry.py` — register the three new families with
  their training and inference entry points.
- `engine/strategy/predictor.py` — route inference to the right family
  based on champion state.
- `engine/runtime/job_registry.py` — add the three new training jobs
  to the allowlist.

## Implementation plan

1. **Conform to the feature-registry contract.** Every new model reads
   its feature columns from `feature_registry.expected_columns(...)`.
   Predictions at serve time use the same call so train / serve parity
   is enforced by construction.
2. **LightGBM family.** Tabular regressor. Hyperparameters governed by
   the Optuna catalog (prompt 03). `fit(X, y, sample_weight=...)`,
   `predict(X)`. Save / load via `joblib`. Persist to the same model
   directory layout as `embed_regressor`.
3. **XGBoost family.** Same surface as LightGBM.
4. **PatchTST family.** PyTorch. Input `(batch, seq_len, n_features)`,
   patches the sequence, applies a transformer encoder, and projects
   to `n_horizons` outputs. Default `seq_len=128`, `n_horizons=6`.
   Train with cosine LR schedule, AdamW, gradient clipping. Save the
   `state_dict` plus a JSON config so the model is reconstructable.
5. **Trainer jobs.** Each job iterates the universe, trains per-symbol
   (or globally for PatchTST when configured that way), writes models
   to disk under `models/<family>/<symbol>/`, registers the artifact
   in `model_registry`.
6. **Predictor routing.** `predictor.predict(symbol, ts)` looks up the
   champion family for the symbol and dispatches.
7. **Shadow-on-first-deploy.** New families register with stage =
   `shadow`. They cannot become champions without going through the
   normal promotion path.

## Acceptance criteria

- [ ] All three families register on import; `pytest -q -k registry`
      shows them in the family list.
- [ ] Each family's predictions at serve time use the exact column
      order returned by `feature_registry.expected_columns(...)`.
      A test asserts this by mutating the registry order and verifying
      predictions change identically (i.e., schema-bound).
- [ ] Each model can be saved, loaded, and produces bit-identical
      predictions across the round-trip on a fixed seed.
- [ ] Champion / challenger competition test
      (`test_new_families_competition_integration.py`) runs all four
      families (3 new + 1 incumbent) on a synthetic dataset and ranks
      them; no family crashes; the test asserts the ranking is stable
      across two repeats with the same seed.
- [ ] On first deploy, all three families are at stage = `shadow`.
- [ ] No family is allowed to bypass `promotion_guard`.

## Test plan

- `tests/test_lgbm_regressor.py` — fit / predict / save / load /
  schema parity.
- `tests/test_xgb_regressor.py` — same.
- `tests/test_patchtst_model.py` — forward shape, loss decreases on a
  trivial DGP, save / load round-trip.
- `tests/test_new_families_competition_integration.py` — multi-family
  competition; deterministic ranking under fixed seed.

Run: `pytest -q tests/test_lgbm_regressor.py tests/test_xgb_regressor.py
tests/test_patchtst_model.py
tests/test_new_families_competition_integration.py
tests/test_model_competition_real_pnl.py`

## Out of scope

- Do not promote any new family to champion in this PR. Promotion
  happens only after live shadow data accumulates.
- Do not add neural sequence models other than PatchTST. iTransformer,
  Informer, TFT etc. are deferred to a separate prompt.
- Do not change `temporal_predictor.py`, `embed_regressor.py`, or
  `regime_stats` family. They remain the incumbents.
- Do not introduce a new GPU dependency. PatchTST must train on CPU
  for the default test fixture; GPU is opt-in via `torch.cuda`.
