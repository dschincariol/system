"""Load and validate the runtime environment contract.

This module centralizes the environment variables that materially affect
runtime safety, execution gating, supervised operation, and optional
alternative-data features.
"""

# engine/runtime/config_schema.py
import os
from pathlib import Path
from dataclasses import dataclass


class ConfigError(RuntimeError):
    pass


_ALLOWED_ENVS = {"dev", "prod", "test"}
_ALLOWED_ENGINE_MODES = {"safe", "shadow", "live", "dev", "paper"}


def _req(name: str) -> str:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        raise ConfigError(f"Missing required env: {name}")
    return str(v).strip()


def _opt(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return default if v is None else str(v).strip()


def _opt_int(name: str, default: int) -> int:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return int(default)
    try:
        return int(str(v).strip())
    except Exception as e:
        raise ConfigError(f"Invalid int for {name}: {v}") from e


def _opt_float(name: str, default: float) -> float:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return float(default)
    try:
        return float(str(v).strip())
    except Exception as e:
        raise ConfigError(f"Invalid float for {name}: {v}") from e


def _opt_bool(name: str, default: bool = False) -> bool:
    v = os.environ.get(name)
    if v is None or str(v).strip() == "":
        return bool(default)
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    raise ConfigError(f"Invalid bool for {name}: {v}")


def _normalize_env_name(raw: str) -> str:
    env = str(raw or "").strip().lower()
    if env == "production":
        env = "prod"
    elif env == "development":
        env = "dev"
    if env not in _ALLOWED_ENVS:
        raise ConfigError(f"Invalid ENV: {raw}")
    return env


def _normalize_engine_mode(raw: str) -> str:
    mode = str(raw or "").strip().lower() or "safe"
    if mode == "development":
        mode = "dev"
    if mode not in _ALLOWED_ENGINE_MODES:
        raise ConfigError(f"Invalid ENGINE_MODE: {raw}")
    return mode


def get_runtime_safety_context() -> dict[str, object]:
    env_raw = _opt("ENV", os.environ.get("NODE_ENV", "dev"))
    env = _normalize_env_name(env_raw)
    engine_mode = _normalize_engine_mode(_opt("ENGINE_MODE", "safe"))
    env_explicit = bool(str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "").strip())
    explicit_dev_env = bool(env_explicit and env in {"dev", "test"})
    supervised = _opt_bool("ENGINE_SUPERVISED", default=False)
    live_like_mode = engine_mode in {"live", "shadow", "paper"}
    strict_runtime = bool(supervised or env == "prod" or (live_like_mode and not explicit_dev_env))
    require_explicit_training = bool(env == "prod" or (live_like_mode and not explicit_dev_env))
    return {
        "env": env,
        "engine_mode": engine_mode,
        "supervised": bool(supervised),
        "strict_runtime": strict_runtime,
        "explicit_dev_env": explicit_dev_env,
        "live_like_mode": live_like_mode,
        "require_explicit_training": require_explicit_training,
    }


def _validate_new_subsystem_flags() -> None:
    hmm_num_states = _opt_int("HMM_NUM_STATES", 3)
    if hmm_num_states < 3 or hmm_num_states > 5:
        raise ConfigError("HMM_NUM_STATES must be between 3 and 5")
    if not _opt("HMM_REGIME_MODEL_SYMBOL", "SPY").strip():
        raise ConfigError("HMM_REGIME_MODEL_SYMBOL must be non-empty")
    hmm_model_cache_ttl_s = _opt_float("HMM_REGIME_MODEL_CACHE_TTL_S", 15.0)
    if hmm_model_cache_ttl_s < 0.0:
        raise ConfigError("HMM_REGIME_MODEL_CACHE_TTL_S must be >= 0")
    hmm_train_lookback_rows = _opt_int("HMM_TRAIN_LOOKBACK_ROWS", 640)
    hmm_train_min_rows = _opt_int("HMM_TRAIN_MIN_ROWS", 96)
    hmm_train_max_iter = _opt_int("HMM_TRAIN_MAX_ITER", 200)
    if hmm_train_lookback_rows <= 0:
        raise ConfigError("HMM_TRAIN_LOOKBACK_ROWS must be > 0")
    if hmm_train_min_rows <= 0:
        raise ConfigError("HMM_TRAIN_MIN_ROWS must be > 0")
    if hmm_train_min_rows > hmm_train_lookback_rows:
        raise ConfigError("HMM_TRAIN_MIN_ROWS must be <= HMM_TRAIN_LOOKBACK_ROWS")
    if hmm_train_max_iter < 1:
        raise ConfigError("HMM_TRAIN_MAX_ITER must be >= 1")

    cpcv_n_splits = _opt_int("CPCV_N_SPLITS", 6)
    cpcv_n_test_splits = _opt_int("CPCV_N_TEST_SPLITS", 2)
    cpcv_embargo_pct = _opt_float("CPCV_EMBARGO_PCT", 0.01)
    cpcv_label_horizon = _opt_int("CPCV_LABEL_HORIZON", 0)
    cpcv_max_pbo = _opt_float("CPCV_MAX_PBO", 0.5)
    _opt_float("CPCV_MIN_PATH_SHARPE", 0.5)
    if cpcv_n_splits < 2:
        raise ConfigError("CPCV_N_SPLITS must be >= 2")
    if cpcv_n_test_splits < 1 or cpcv_n_test_splits >= cpcv_n_splits:
        raise ConfigError("CPCV_N_TEST_SPLITS must be >= 1 and < CPCV_N_SPLITS")
    if not (0.0 <= cpcv_embargo_pct < 1.0):
        raise ConfigError("CPCV_EMBARGO_PCT must be between 0 and 1")
    if cpcv_label_horizon < 0:
        raise ConfigError("CPCV_LABEL_HORIZON must be >= 0")
    if not (0.0 <= cpcv_max_pbo <= 1.0):
        raise ConfigError("CPCV_MAX_PBO must be between 0 and 1")

    if not _opt("GBM_MODEL_NAME", "gbm_regressor").strip():
        raise ConfigError("GBM_MODEL_NAME must be non-empty")
    if _opt_int("GBM_LOOKBACK_DAYS", 365) <= 0:
        raise ConfigError("GBM_LOOKBACK_DAYS must be > 0")
    if _opt_int("GBM_MIN_SAMPLES", 50) <= 0:
        raise ConfigError("GBM_MIN_SAMPLES must be > 0")
    if _opt_int("GBM_MIN_NEW_LABELS", 25) < 0:
        raise ConfigError("GBM_MIN_NEW_LABELS must be >= 0")
    if _opt_int("GBM_HORIZON_S", _opt_int("MODEL_HORIZON_MEDIUM_S", 3600)) <= 0:
        raise ConfigError("GBM_HORIZON_S must be > 0")
    if _opt_int("GBM_NUM_LEAVES", 31) < 2:
        raise ConfigError("GBM_NUM_LEAVES must be >= 2")
    if _opt_float("GBM_LEARNING_RATE", 0.05) <= 0.0:
        raise ConfigError("GBM_LEARNING_RATE must be > 0")
    if _opt_int("GBM_N_ESTIMATORS", 200) < 1:
        raise ConfigError("GBM_N_ESTIMATORS must be >= 1")
    if _opt_int("GBM_MIN_CHILD_SAMPLES", 20) < 1:
        raise ConfigError("GBM_MIN_CHILD_SAMPLES must be >= 1")

    tsfresh_fc_profile = _opt("TSFRESH_FC_PROFILE", "minimal").lower() or "minimal"
    if tsfresh_fc_profile not in ("minimal", "efficient", "balanced"):
        raise ConfigError(f"Invalid TSFRESH_FC_PROFILE: {tsfresh_fc_profile}")
    if _opt_int("TSFRESH_WINDOW_S", 3600) < 60:
        raise ConfigError("TSFRESH_WINDOW_S must be >= 60")
    if _opt_int("TSFRESH_MAX_FEATURES", 200) < 1:
        raise ConfigError("TSFRESH_MAX_FEATURES must be >= 1")
    if _opt_int("TSFRESH_SNAPSHOT_BUCKET_SEC", _opt_int("MODEL_FEATURE_SNAPSHOT_BUCKET_SEC", 300)) < 60:
        raise ConfigError("TSFRESH_SNAPSHOT_BUCKET_SEC must be >= 60")
    if _opt_int("TSFRESH_SNAPSHOT_SYMBOL_LIMIT", 1500) < 1:
        raise ConfigError("TSFRESH_SNAPSHOT_SYMBOL_LIMIT must be >= 1")

    if not _opt("FINBERT_MODEL_NAME", "ProsusAI/finbert").strip():
        raise ConfigError("FINBERT_MODEL_NAME must be non-empty")
    if _opt_int("FINBERT_BATCH_SIZE", 16) < 1:
        raise ConfigError("FINBERT_BATCH_SIZE must be >= 1")
    if _opt_int("FINBERT_MAX_TEXT_LEN", 4000) < 64:
        raise ConfigError("FINBERT_MAX_TEXT_LEN must be >= 64")

    if _opt_int("DRIFT_RETRAIN_COOLDOWN_S", 6 * 60 * 60) < 0:
        raise ConfigError("DRIFT_RETRAIN_COOLDOWN_S must be >= 0")
    if _opt_float("DRIFT_RETRAIN_MIN_DEGRADATION", 0.25) < 0.0:
        raise ConfigError("DRIFT_RETRAIN_MIN_DEGRADATION must be >= 0")
    if _opt_int("DRIFT_RETRAIN_MAX_PARALLEL_JOBS", 1) < 1:
        raise ConfigError("DRIFT_RETRAIN_MAX_PARALLEL_JOBS must be >= 1")

    if _opt_int("SHAP_TOP_K", 10) < 1:
        raise ConfigError("SHAP_TOP_K must be >= 1")

    black_litterman_tau = _opt_float("BLACK_LITTERMAN_TAU", 0.05)
    black_litterman_view_confidence = _opt_float("BLACK_LITTERMAN_VIEW_CONFIDENCE", 0.60)
    if black_litterman_tau <= 0.0:
        raise ConfigError("BLACK_LITTERMAN_TAU must be > 0")
    if not (0.0 < black_litterman_view_confidence <= 1.0):
        raise ConfigError("BLACK_LITTERMAN_VIEW_CONFIDENCE must be between 0 and 1")

    champion_min_observations = _opt_int("CHAMPION_PROMOTION_MIN_OBSERVATIONS", 50)
    champion_fdr_alpha = _opt_float("CHAMPION_PROMOTION_FDR_ALPHA", 0.05)
    spa_min_models = _opt_int("SPA_MIN_MODELS", 3)
    spa_bootstrap_samples = _opt_int("SPA_BOOTSTRAP_SAMPLES", 1000)
    spa_alpha = _opt_float("SPA_ALPHA", 0.05)
    _opt_float("CHAMPION_PROMOTION_MIN_T_STAT", 3.0)
    _opt_float("CHAMPION_PROMOTION_MIN_DEFLATED_SHARPE", 0.0)
    if champion_min_observations < 2:
        raise ConfigError("CHAMPION_PROMOTION_MIN_OBSERVATIONS must be >= 2")
    if not (0.0 < champion_fdr_alpha <= 1.0):
        raise ConfigError("CHAMPION_PROMOTION_FDR_ALPHA must be between 0 and 1")
    if spa_min_models < 2:
        raise ConfigError("SPA_MIN_MODELS must be >= 2")
    if spa_bootstrap_samples < 100:
        raise ConfigError("SPA_BOOTSTRAP_SAMPLES must be >= 100")
    if not (0.0 < spa_alpha < 1.0):
        raise ConfigError("SPA_ALPHA must be between 0 and 1")

    _opt_bool("HMM_REGIME_ENABLED", default=False)
    _opt_bool("HMM_REGIME_ENSEMBLE_WEIGHT_ENABLED", default=False)
    _opt_bool("CPCV_ENABLED", default=False)
    _opt_bool("USE_GBM_REGRESSOR", default=False)
    _opt_bool("GBM_USE_TUNED_HYPERPARAMS", default=False)
    _opt_bool("USE_TSFRESH_FEATURES", default=False)
    _opt_bool("TSFRESH_USE_PERSISTED_SNAPSHOTS", default=True)
    _opt_bool("TSFRESH_LIVE_COMPUTE_ENABLED", default=False)
    _opt_bool("USE_FINBERT_SENTIMENT", default=False)
    _opt_bool("FINBERT_USE_PERSISTED_ENRICHMENT", default=True)
    _opt_bool("FINBERT_LIVE_INFERENCE_ENABLED", default=False)
    _opt_bool("DRIFT_RETRAIN_ENABLED", default=False)
    _opt_bool("DRIFT_RETRAIN_REQUIRE_CPCV", default=True)
    _opt_bool("DRIFT_RETRAIN_REQUIRE_STAT_GATE", default=True)
    _opt_bool("SHAP_EXPLANATIONS_ENABLED", default=False)
    _opt_bool("SHAP_LIVE_COMPUTE_ENABLED", default=False)
    _opt_bool("SHAP_PERSIST_EXPLANATIONS", default=True)
    _opt_bool("BLACK_LITTERMAN_ENABLED", default=False)
    _opt_bool("CHAMPION_PROMOTION_USE_STAT_GATE", default=False)
    _opt_bool("SPA_TEST_ENABLED", default=False)


@dataclass(frozen=True)
class RuntimeConfig:
    """Normalized runtime configuration derived from environment variables.

    Attributes
    ----------
    env : str
        Normalized runtime environment name.
    db_path : str
        Absolute path to the active SQLite database.
    prod_lock : bool
        Whether production safety locking is enabled.
    allow_training : bool
        Whether online or background training is allowed in the current mode.
    supervisor_enabled : bool
        Whether the supervisor loop should run.
    supervisor_tick_s : int
        Supervisor polling interval in seconds.
    exec_degrade_block : bool
        Whether degraded execution conditions block order flow.
    exec_degrade_warn_cost_pct : float
        Warning threshold for execution-cost degradation.
    exec_degrade_crit_cost_pct : float
        Critical threshold for execution-cost degradation.
    ensemble_blend_enabled : bool
        Whether predictor ensemble blending is enabled.
    ensemble_blend_mode : str
        Ensemble weighting mode.
    ensemble_max_weight : float
        Maximum per-model blend weight.
    ensemble_min_agreement : float
        Minimum agreement threshold for the blend.
    ensemble_meta_retrain_s : int
        Retraining cadence for ensemble meta state in seconds.
    use_form4_data : bool
        Whether Form 4 data can participate in feature generation.
    use_congressional_trade_data : bool
        Whether congressional-trade data can participate in feature generation.
    form4_backfill_days : int
        Backfill window for Form 4 ingestion.
    congressional_backfill_days : int
        Backfill window for congressional-trade ingestion.
    ingest_form4_enabled : bool
        Whether the Form 4 ingestion job may run.
    ingest_congressional_enabled : bool
        Whether the congressional-trade ingestion job may run.
    use_pit_universe : bool
        Whether PIT universe inputs are enabled.
    pit_universe_backfill_enabled : bool
        Whether PIT universe backfill jobs may run.
    """

    env: str
    db_path: str

    # production lock
    prod_lock: bool
    allow_training: bool

    # supervisor
    supervisor_enabled: bool
    supervisor_tick_s: int

    # execution barrier thresholds
    exec_degrade_block: bool
    exec_degrade_warn_cost_pct: float
    exec_degrade_crit_cost_pct: float

    # opt-in predictor ensemble blending
    ensemble_blend_enabled: bool
    ensemble_blend_mode: str
    ensemble_max_weight: float
    ensemble_min_agreement: float
    ensemble_meta_retrain_s: int

    # optional alternative-data controls
    use_form4_data: bool
    use_congressional_trade_data: bool
    form4_backfill_days: int
    congressional_backfill_days: int
    ingest_form4_enabled: bool
    ingest_congressional_enabled: bool
    use_pit_universe: bool
    pit_universe_backfill_enabled: bool


def load_runtime_config() -> RuntimeConfig:
    """Load and validate the runtime configuration from the current environment.

    Returns
    -------
    RuntimeConfig
        Parsed runtime configuration with normalized environment and path
        values.

    Raises
    ------
    ConfigError
        Raised when a required variable is missing, a value cannot be parsed,
        or a safety invariant such as `PROD_LOCK=1` with `ALLOW_TRAINING=1`
        is violated.

    Notes
    -----
    Supervised runtimes and `prod` mode are validated more strictly than local
    development so ambiguous storage roots and unsafe defaults fail closed.
    """

    safety = get_runtime_safety_context()
    env = str(safety["env"])
    engine_mode = str(safety["engine_mode"])
    supervised = bool(safety["supervised"])
    explicit_dev_env = bool(safety["explicit_dev_env"])
    strict_runtime = bool(safety["strict_runtime"])
    require_explicit_training = bool(safety["require_explicit_training"])

    from engine.runtime.platform import default_data_root

    if engine_mode == "live" and explicit_dev_env and not supervised:
        default_db_path = str(Path.cwd() / "data" / ("trading" + "." + "db"))
    else:
        default_db_path = str(default_data_root())
    db_path_raw = _opt("DB_PATH", "")
    if strict_runtime and not db_path_raw:
        raise ConfigError(
            f"DB_PATH must be explicitly set when env={env} engine_mode={engine_mode} strict_runtime=1"
        )
    if not db_path_raw:
        db_path_raw = default_db_path
    db_path = str(Path(db_path_raw).expanduser().resolve())

    prod_lock = _opt_bool("PROD_LOCK", default=(env == "prod"))
    allow_training_raw = str(os.environ.get("ALLOW_TRAINING") or "").strip()
    if require_explicit_training and not allow_training_raw:
        raise ConfigError(
            f"ALLOW_TRAINING must be explicitly set when env={env} engine_mode={engine_mode}"
        )
    allow_training = _opt_bool("ALLOW_TRAINING", default=(env != "prod" and not require_explicit_training))

    supervisor_enabled = _opt_bool("SUPERVISOR_ENABLED", default=True)
    supervisor_tick_s = _opt_int("SUPERVISOR_TICK_S", 2)

    exec_degrade_block = _opt_bool("EXEC_DEGRADE_BLOCK", default=True)
    exec_degrade_warn_cost_pct = _opt_float("EXEC_DEGRADE_WARN_COST_PCT", 0.25)
    exec_degrade_crit_cost_pct = _opt_float("EXEC_DEGRADE_CRIT_COST_PCT", 0.50)
    ensemble_blend_enabled = _opt_bool("ENSEMBLE_BLEND_ENABLED", default=False)
    ensemble_blend_mode = _opt("ENSEMBLE_BLEND_MODE", "equal").lower() or "equal"
    if ensemble_blend_mode not in ("equal", "inverse_variance", "stacked"):
        raise ConfigError(f"Invalid ENSEMBLE_BLEND_MODE: {ensemble_blend_mode}")
    ensemble_max_weight = _opt_float("ENSEMBLE_MAX_WEIGHT", 0.75)
    ensemble_min_agreement = _opt_float("ENSEMBLE_MIN_AGREEMENT", 0.0)
    ensemble_meta_retrain_s = _opt_int("ENSEMBLE_META_RETRAIN_S", 86400)
    use_form4_data = _opt_bool("USE_FORM4_DATA", default=False)
    use_congressional_trade_data = _opt_bool("USE_CONGRESSIONAL_TRADE_DATA", default=False)
    form4_backfill_days = _opt_int("FORM4_BACKFILL_DAYS", 180)
    congressional_backfill_days = _opt_int("CONGRESSIONAL_BACKFILL_DAYS", 180)
    ingest_form4_enabled = _opt_bool("INGEST_FORM4_ENABLED", default=False)
    ingest_congressional_enabled = _opt_bool("INGEST_CONGRESSIONAL_ENABLED", default=False)
    use_pit_universe = _opt_bool("USE_PIT_UNIVERSE", default=False)
    pit_universe_backfill_enabled = _opt_bool("PIT_UNIVERSE_BACKFILL_ENABLED", default=False)
    _validate_new_subsystem_flags()

    if strict_runtime and not Path(db_path).is_absolute():
        raise ConfigError(f"DB_PATH must resolve to an absolute path in supervised/prod mode: {db_path_raw}")

    # Hard production safety: do not allow training when prod_lock enabled.
    if prod_lock and allow_training:
        raise ConfigError("PROD_LOCK=1 forbids ALLOW_TRAINING=1")

    if not (0.0 <= float(ensemble_max_weight) <= 1.0):
        raise ConfigError("ENSEMBLE_MAX_WEIGHT must be between 0 and 1")
    if not (0.0 <= float(ensemble_min_agreement) <= 1.0):
        raise ConfigError("ENSEMBLE_MIN_AGREEMENT must be between 0 and 1")
    if int(ensemble_meta_retrain_s) <= 0:
        raise ConfigError("ENSEMBLE_META_RETRAIN_S must be > 0")

    return RuntimeConfig(
        env=env,
        db_path=db_path,
        prod_lock=prod_lock,
        allow_training=allow_training,
        supervisor_enabled=supervisor_enabled,
        supervisor_tick_s=supervisor_tick_s,
        exec_degrade_block=exec_degrade_block,
        exec_degrade_warn_cost_pct=exec_degrade_warn_cost_pct,
        exec_degrade_crit_cost_pct=exec_degrade_crit_cost_pct,
        ensemble_blend_enabled=ensemble_blend_enabled,
        ensemble_blend_mode=ensemble_blend_mode,
        ensemble_max_weight=ensemble_max_weight,
        ensemble_min_agreement=ensemble_min_agreement,
        ensemble_meta_retrain_s=ensemble_meta_retrain_s,
        use_form4_data=use_form4_data,
        use_congressional_trade_data=use_congressional_trade_data,
        form4_backfill_days=form4_backfill_days,
        congressional_backfill_days=congressional_backfill_days,
        ingest_form4_enabled=ingest_form4_enabled,
        ingest_congressional_enabled=ingest_congressional_enabled,
        use_pit_universe=use_pit_universe,
        pit_universe_backfill_enabled=pit_universe_backfill_enabled,
    )
