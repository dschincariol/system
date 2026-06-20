# Hyperparameter Inventory

This inventory tracks parameters that are managed by the Optuna tuning catalog
in `engine.strategy.tuning.catalog`. Runtime/platform switches remain
environment-controlled; model-training parameters should flow through the
catalog and `model_best_params`.

| Env name | Catalog key | Model family | Type | Default | Range / choices | Status |
|---|---|---|---|---:|---|---|
| `TEMPORAL_SEQ_LEN` | `seq_len` | `temporal_predictor` | int | 6 | 3-32 | managed |
| `TEMPORAL_CONF_K` | `conf_k` | `temporal_predictor` | float | 75.0 | 10-250 log | managed |
| `TEMPORAL_HIDDEN_WIDTH` | `hidden_width` | `temporal_predictor` | int | 128 | 32-512 log | managed |
| `TEMPORAL_LR` | `lr` | `temporal_predictor` | float | 0.003 | 1e-4-1e-2 log | managed |
| `TEMPORAL_EPOCHS` | `epochs` | `temporal_predictor` | int | 120 | 20-240 | managed |
| `EMBED_TRAIN_SPLIT` | `train_split` | `embed_regressor` | float | 0.8 | 0.5-0.95 | managed |
| `EMBED_REGRESSOR_CONF_K` | `conf_k` | `embed_regressor` | float | 75.0 | 10-250 log | managed |
| `EMBED_RIDGE_ALPHA` | `ridge_alpha` | `embed_regressor` | float | 1.0 | 1e-4-100 log | managed |
| `LGBM_NUM_LEAVES` | `num_leaves` | `lgbm_regressor` | int | 31 | 8-256 log | managed |
| `LGBM_LEARNING_RATE` | `learning_rate` | `lgbm_regressor` | float | 0.05 | 1e-3-0.2 log | managed |
| `LGBM_N_ESTIMATORS` | `n_estimators` | `lgbm_regressor` | int | 300 | 50-1200 log | managed |
| `XGB_MAX_DEPTH` | `max_depth` | `xgb_regressor` | int | 4 | 2-10 | managed |
| `XGB_LEARNING_RATE` | `learning_rate` | `xgb_regressor` | float | 0.05 | 1e-3-0.2 log | managed |
| `XGB_N_ESTIMATORS` | `n_estimators` | `xgb_regressor` | int | 300 | 50-1200 log | managed |
| `PATCHTST_SEQ_LEN` | `seq_len` | `patchtst` | int | 128 | 32-256 log | managed |
| `PATCHTST_PATCH_LEN` | `patch_len` | `patchtst` | int | 16 | 4-32 log | managed |
| `PATCHTST_D_MODEL` | `d_model` | `patchtst` | int | 64 | 16-256 log | managed |
| `ITRANSFORMER_SEQ_LEN` | `seq_len` | `itransformer` | int | 128 | 32-256 log | managed |
| `ITRANSFORMER_D_MODEL` | `d_model` | `itransformer` | int | 64 | 16-256 log | managed |
| `ITRANSFORMER_LAYERS` | `n_layers` | `itransformer` | int | 2 | 1-6 | managed |
| `ITRANSFORMER_HEADS` | `n_heads` | `itransformer` | int | 4 | 1-8 | managed |
| `ITRANSFORMER_DROPOUT` | `dropout` | `itransformer` | float | 0.1 | 0.0-0.5 | managed |

Environment variables not listed here are classified as runtime/platform
controls, feature flags, credentials, provider settings, or job scheduling
knobs. They are intentionally not part of model hyperparameter search.
