"""Load and validate the runtime environment contract.

This module centralizes the environment variables that materially affect
runtime safety, execution gating, supervised operation, and optional
alternative-data features.
"""

# engine/runtime/config_schema.py
import os
from pathlib import Path
from dataclasses import dataclass
import math

from engine.runtime.workload_profiles import (
    LIVE_PROFILE_OFFLINE_TRAINING_ACK_PHRASE,
    normalize_workload_profile,
    offline_training_ack_snapshot,
    workload_profile_defaults,
    workload_profile_from_env,
)


class ConfigError(RuntimeError):
    pass


_ALLOWED_ENVS = {"dev", "prod", "test"}
_ALLOWED_ENGINE_MODES = {"safe", "shadow", "live", "dev", "paper"}
_PLACEHOLDER_VALUES = {
    "changeme",
    "change-me",
    "default",
    "dummy",
    "example",
    "none",
    "null",
    "placeholder",
    "sample",
    "tbd",
    "test",
    "todo",
    "unset",
}

_LIVE_RISK_REQUIRED_ENABLED_FLAGS = (
    "PORTFOLIO_USE_RISK_ENGINE",
    "PORTFOLIO_RISK_USE_MONTE_CARLO",
    "PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE",
    "MODEL_AWARE_KILL_SWITCH",
)
_LIVE_RISK_REQUIRED_FLOAT_THRESHOLDS = (
    "PORTFOLIO_RISK_MC_VAR_95_BLOCK",
    "PORTFOLIO_RISK_MC_VAR_99_BLOCK",
    "PORTFOLIO_RISK_MC_CVAR_95_BLOCK",
    "PORTFOLIO_RISK_MC_CVAR_99_BLOCK",
    "PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK",
    "PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK",
    "PORTFOLIO_RISK_VOL_HARD_BLOCK",
    "KILL_SWITCH_MODEL_MAX_DRAWDOWN",
)
_LIVE_RISK_REQUIRED_INT_THRESHOLDS = (
    "KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES",
)
_LIVE_RISK_REQUIRED_COST_FILTER_FLAG = "ALERT_USE_EXEC_COST_FILTER"
_LIVE_RISK_REQUIRED_COST_FILTER_THRESHOLD = "ALERT_MIN_NET_ABS_Z"
_LIVE_RISK_COST_FILTER_REQUIRED_IN_LIVE = "EQUITY_EXEC_COST_FILTER_REQUIRED_IN_LIVE"
_LIVE_RISK_ACCEPTANCE_OVERRIDE = "LIVE_RISK_THRESHOLD_ACCEPTANCE_OVERRIDE"
_LIVE_RISK_ACCEPTANCE_AUDIT_FIELDS = (
    "LIVE_RISK_THRESHOLD_ACCEPTANCE_ID",
    "LIVE_RISK_THRESHOLD_ACCEPTANCE_OWNER",
    "LIVE_RISK_THRESHOLD_ACCEPTANCE_REASON",
)
_CPU_FIRST_DEVICE_ENV_KEYS = (
    "TORCH_DEVICE",
    "EMBED_DEVICE",
    "NLP_DEVICE",
    "FINBERT_DEVICE",
    "TS_FOUNDATION_DEVICE",
)
_DEVICE_VALUE_PREFIXES = ("cuda:",)
_DEVICE_VALUE_EXACT = {"cpu", "cuda", "auto"}


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


def _validate_device_value(name: str, default: str = "cpu") -> None:
    value = _opt(name, default).lower()
    if not value:
        raise ConfigError(f"{name} must be non-empty")
    if value in _DEVICE_VALUE_EXACT:
        return
    if any(value.startswith(prefix) for prefix in _DEVICE_VALUE_PREFIXES):
        return
    raise ConfigError(f"Invalid device for {name}: {value}")


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


def _is_placeholder_value(raw: object) -> bool:
    text = str(raw if raw is not None else "").strip()
    if not text:
        return True
    lowered = text.lower().replace("_", "-").replace(" ", "")
    return lowered in _PLACEHOLDER_VALUES


def _parse_bool_value(name: str, raw: object, default: bool) -> bool:
    if raw is None or str(raw).strip() == "":
        return bool(default)
    text = str(raw).strip().lower()
    if text in ("1", "true", "yes", "y", "on"):
        return True
    if text in ("0", "false", "no", "n", "off"):
        return False
    raise ConfigError(f"Invalid bool for {name}: {raw}")


def _live_risk_required(safety: dict[str, object] | None = None) -> bool:
    ctx = safety if safety is not None else get_runtime_safety_context()
    return bool(str(ctx.get("engine_mode") or "").lower() == "live" and bool(ctx.get("strict_runtime")))


def _positive_float_issue(name: str) -> str | None:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return f"{name} unset"
    if _is_placeholder_value(raw):
        return f"{name} placeholder"
    try:
        value = float(str(raw).strip())
    except Exception:
        return f"{name} invalid"
    if not math.isfinite(value) or value <= 0.0:
        return f"{name} must be > 0"
    return None


def _positive_int_issue(name: str) -> str | None:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return f"{name} unset"
    if _is_placeholder_value(raw):
        return f"{name} placeholder"
    try:
        value = int(str(raw).strip())
    except Exception:
        return f"{name} invalid"
    if value <= 0:
        return f"{name} must be > 0"
    return None


def live_risk_threshold_validation_snapshot(
    safety: dict[str, object] | None = None,
) -> dict[str, object]:
    """Return live-capital risk-threshold validation state.

    Live capital must not rely on risk gates whose production defaults disable
    blocking behaviour. A risk-acceptance override is allowed only when it is
    explicitly enabled and carries audit metadata that can be surfaced by
    preflight.
    """

    ctx = safety if safety is not None else get_runtime_safety_context()
    required = _live_risk_required(ctx)
    issues: list[str] = []
    audit: dict[str, str] = {}
    required_enabled_flags = list(_LIVE_RISK_REQUIRED_ENABLED_FLAGS)
    required_thresholds = list(_LIVE_RISK_REQUIRED_FLOAT_THRESHOLDS + _LIVE_RISK_REQUIRED_INT_THRESHOLDS)

    if not required:
        return {
            "required": False,
            "ok": True,
            "override": False,
            "issues": [],
            "audit": {},
        }

    try:
        cost_filter_required = _parse_bool_value(
            _LIVE_RISK_COST_FILTER_REQUIRED_IN_LIVE,
            os.environ.get(_LIVE_RISK_COST_FILTER_REQUIRED_IN_LIVE),
            default=False,
        )
    except ConfigError as exc:
        issues.append(str(exc))
        cost_filter_required = False

    for name in _LIVE_RISK_REQUIRED_ENABLED_FLAGS:
        raw = os.environ.get(name)
        if raw is not None and _is_placeholder_value(raw):
            issues.append(f"{name} placeholder")
            continue
        try:
            enabled = _parse_bool_value(name, raw, default=True)
        except ConfigError as exc:
            issues.append(str(exc))
            continue
        if not enabled:
            issues.append(f"{name} disabled")

    for name in _LIVE_RISK_REQUIRED_FLOAT_THRESHOLDS:
        issue = _positive_float_issue(name)
        if issue:
            issues.append(issue)

    for name in _LIVE_RISK_REQUIRED_INT_THRESHOLDS:
        issue = _positive_int_issue(name)
        if issue:
            issues.append(issue)

    if cost_filter_required:
        required_enabled_flags.append(_LIVE_RISK_REQUIRED_COST_FILTER_FLAG)
        required_thresholds.append(_LIVE_RISK_REQUIRED_COST_FILTER_THRESHOLD)

        raw = os.environ.get(_LIVE_RISK_REQUIRED_COST_FILTER_FLAG)
        if raw is not None and _is_placeholder_value(raw):
            issues.append(f"{_LIVE_RISK_REQUIRED_COST_FILTER_FLAG} placeholder")
        else:
            try:
                enabled = _parse_bool_value(_LIVE_RISK_REQUIRED_COST_FILTER_FLAG, raw, default=False)
            except ConfigError as exc:
                issues.append(str(exc))
            else:
                if not enabled:
                    issues.append(f"{_LIVE_RISK_REQUIRED_COST_FILTER_FLAG} disabled")

        issue = _positive_float_issue(_LIVE_RISK_REQUIRED_COST_FILTER_THRESHOLD)
        if issue:
            issues.append(issue)

    try:
        override = _parse_bool_value(
            _LIVE_RISK_ACCEPTANCE_OVERRIDE,
            os.environ.get(_LIVE_RISK_ACCEPTANCE_OVERRIDE),
            default=False,
        )
    except ConfigError as exc:
        issues.append(str(exc))
        override = False

    if override:
        for field in _LIVE_RISK_ACCEPTANCE_AUDIT_FIELDS:
            raw = os.environ.get(field)
            if _is_placeholder_value(raw):
                issues.append(f"{field} required for {_LIVE_RISK_ACCEPTANCE_OVERRIDE}=1")
            else:
                audit[field] = str(raw).strip()

    ok = not issues or bool(override and len(audit) == len(_LIVE_RISK_ACCEPTANCE_AUDIT_FIELDS))
    return {
        "required": True,
        "ok": bool(ok),
        "override": bool(override and ok),
        "issues": list(issues),
        "audit": audit,
        "required_thresholds": required_thresholds,
        "required_enabled_flags": required_enabled_flags,
        "cost_filter_required": bool(cost_filter_required),
    }


def validate_live_risk_thresholds(safety: dict[str, object] | None = None) -> dict[str, object]:
    snapshot = live_risk_threshold_validation_snapshot(safety)
    if not bool(snapshot.get("ok")):
        issues = "; ".join(str(item) for item in list(snapshot.get("issues") or []))
        raise ConfigError(f"live risk thresholds invalid: {issues}")
    return snapshot


def validate_live_trading_confirmation(safety: dict[str, object] | None = None) -> dict[str, object]:
    ctx = safety if safety is not None else get_runtime_safety_context()
    required = bool(str(ctx.get("engine_mode") or "").lower() == "live" and bool(ctx.get("strict_runtime")))
    if not required:
        return {
            "required": False,
            "ok": True,
            "blockers": [],
        }

    from engine.runtime.live_trading_preflight import (
        DEFAULT_LIVE_CONFIRM_PHRASE,
        live_confirmation_snapshot,
    )

    snapshot = live_confirmation_snapshot(engine_mode="live")
    if not bool(snapshot.get("ok")):
        blockers = "; ".join(str(item) for item in list(snapshot.get("blockers") or []))
        raise ConfigError(
            "live trading confirmation invalid: "
            f"{blockers}; expected LIVE_TRADING_CONFIRM={DEFAULT_LIVE_CONFIRM_PHRASE}"
        )
    return snapshot


def validate_options_instrument_config(safety: dict[str, object] | None = None) -> dict[str, object]:
    """Validate the options-as-instruments rollout mode.

    Options market data/features are available today, but broker adapters do
    not yet implement live options order submission. Live/prod attempts to
    enable options instruments must therefore fail closed until the execution
    gate reports a complete readiness contract.
    """

    from engine.execution.options_readiness import (
        live_options_readiness_snapshot,
        options_instruments_mode,
    )

    mode = options_instruments_mode()
    if mode == "invalid":
        raw = os.environ.get("OPTIONS_INSTRUMENTS_MODE") or os.environ.get("OPTIONS_AS_INSTRUMENTS_MODE") or ""
        raise ConfigError(f"Invalid OPTIONS_INSTRUMENTS_MODE: {raw}")

    ctx = safety if safety is not None else get_runtime_safety_context()
    required_context = bool(str(ctx.get("engine_mode") or "").lower() == "live" and bool(ctx.get("strict_runtime")))
    snapshot = live_options_readiness_snapshot(
        engine_mode=str(ctx.get("engine_mode") or ""),
        execution_mode=os.environ.get("EXECUTION_MODE", ""),
        broker=os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or os.environ.get("LIVE_BROKER") or "",
    )
    if required_context and bool(snapshot.get("required")) and not bool(snapshot.get("ok")):
        blockers = "; ".join(str(item) for item in list(snapshot.get("blockers") or []))
        raise ConfigError(f"live options instruments invalid: {blockers or snapshot.get('reason')}")
    return snapshot


def validate_data_source_master_key_config(safety: dict[str, object] | None = None) -> dict[str, object]:
    ctx = safety if safety is not None else get_runtime_safety_context()
    required = bool(ctx.get("strict_runtime"))
    try:
        from services.credential_encryption import validate_data_source_master_key

        return validate_data_source_master_key(
            production=required,
            require_present=required,
        )
    except Exception as exc:
        raise ConfigError(f"data source master key invalid: {exc}") from exc


def validate_production_secret_sources(safety: dict[str, object] | None = None) -> dict[str, object]:
    """Fail closed when production/live uses inline repo-local secret values."""

    try:
        from engine.runtime.secret_sources import (
            format_secret_source_policy_error,
            secret_source_policy_snapshot,
        )

        ctx = safety if safety is not None else get_runtime_safety_context()
        snapshot = secret_source_policy_snapshot(validate_files=bool(ctx.get("strict_runtime")))
    except Exception as exc:
        raise ConfigError(f"secret source policy validation failed: {type(exc).__name__}: {exc}") from exc

    if bool(ctx.get("strict_runtime")) and not bool(snapshot.get("ok")):
        raise ConfigError(format_secret_source_policy_error(snapshot))
    return snapshot


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
    try:
        workload_profile = workload_profile_from_env()
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc
    env_explicit = bool(str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "").strip())
    explicit_dev_env = bool(env_explicit and env in {"dev", "test"})
    supervised = _opt_bool("ENGINE_SUPERVISED", default=False)
    live_like_mode = engine_mode in {"live", "shadow", "paper"}
    strict_runtime = bool(supervised or env == "prod" or (live_like_mode and not explicit_dev_env))
    require_explicit_training = bool(env == "prod" or (live_like_mode and not explicit_dev_env))
    return {
        "env": env,
        "engine_mode": engine_mode,
        "workload_profile": workload_profile,
        "supervised": bool(supervised),
        "strict_runtime": strict_runtime,
        "explicit_dev_env": explicit_dev_env,
        "live_like_mode": live_like_mode,
        "require_explicit_training": require_explicit_training,
    }


def validate_workload_profile_guardrails(safety: dict[str, object] | None = None) -> dict[str, object]:
    ctx = safety if safety is not None else get_runtime_safety_context()
    profile = normalize_workload_profile(str(ctx.get("workload_profile") or workload_profile_from_env()))
    snapshot = offline_training_ack_snapshot(profile=profile)
    if bool(ctx.get("strict_runtime")) and bool(snapshot.get("required")) and not bool(snapshot.get("ok")):
        blockers = "; ".join(str(item) for item in list(snapshot.get("blockers") or []))
        enabled = ",".join(str(item) for item in list(snapshot.get("enabled_settings") or []))
        raise ConfigError(
            "offline training in live workload profile requires explicit acknowledgement: "
            f"enabled_settings={enabled}; blockers={blockers}; "
            f"expected OFFLINE_TRAINING_LIVE_PROFILE_ACK={LIVE_PROFILE_OFFLINE_TRAINING_ACK_PHRASE}"
        )
    return snapshot


def _validate_new_subsystem_flags() -> None:
    for device_key in _CPU_FIRST_DEVICE_ENV_KEYS:
        _validate_device_value(device_key, "cpu")

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
    profile_defaults = workload_profile_defaults()
    tsfresh_symbol_default = int(profile_defaults.get("tsfresh_snapshot_symbol_limit") or 1)
    tsfresh_batch_default = int(profile_defaults.get("tsfresh_snapshot_batch_size") or 1)
    tune_trials_default = int(profile_defaults.get("tune_n_trials") or 1)
    tune_max_trials_default = int(profile_defaults.get("tune_max_n_trials") or tune_trials_default)
    tsfresh_symbol_limit = _opt_int("TSFRESH_SNAPSHOT_SYMBOL_LIMIT", tsfresh_symbol_default)
    tsfresh_max_symbols = _opt_int("TSFRESH_SNAPSHOT_MAX_SYMBOLS", tsfresh_symbol_default)
    tsfresh_batch_size = _opt_int("TSFRESH_SNAPSHOT_BATCH_SIZE", tsfresh_batch_default)
    tsfresh_max_batch_size = _opt_int("TSFRESH_SNAPSHOT_MAX_BATCH_SIZE", tsfresh_batch_default)
    if tsfresh_symbol_limit < 1:
        raise ConfigError("TSFRESH_SNAPSHOT_SYMBOL_LIMIT must be >= 1")
    if tsfresh_max_symbols < 1:
        raise ConfigError("TSFRESH_SNAPSHOT_MAX_SYMBOLS must be >= 1")
    if tsfresh_symbol_limit > tsfresh_max_symbols:
        raise ConfigError("TSFRESH_SNAPSHOT_SYMBOL_LIMIT must be <= TSFRESH_SNAPSHOT_MAX_SYMBOLS")
    if tsfresh_batch_size < 1:
        raise ConfigError("TSFRESH_SNAPSHOT_BATCH_SIZE must be >= 1")
    if tsfresh_max_batch_size < 1:
        raise ConfigError("TSFRESH_SNAPSHOT_MAX_BATCH_SIZE must be >= 1")
    if tsfresh_batch_size > tsfresh_max_batch_size:
        raise ConfigError("TSFRESH_SNAPSHOT_BATCH_SIZE must be <= TSFRESH_SNAPSHOT_MAX_BATCH_SIZE")
    if _opt_int("TSFRESH_N_JOBS", int(profile_defaults.get("tsfresh_n_jobs") or 0)) < 0:
        raise ConfigError("TSFRESH_N_JOBS must be >= 0")
    if _opt_int("TSFRESH_MAX_N_JOBS", int(profile_defaults.get("tsfresh_max_n_jobs") or 1)) < 1:
        raise ConfigError("TSFRESH_MAX_N_JOBS must be >= 1")
    if _opt_int("MODEL_TRAIN_N_JOBS", int(profile_defaults.get("model_n_jobs") or 1)) < 1:
        raise ConfigError("MODEL_TRAIN_N_JOBS must be >= 1")
    if _opt_int("MODEL_TRAIN_MAX_N_JOBS", int(profile_defaults.get("model_max_n_jobs") or 1)) < 1:
        raise ConfigError("MODEL_TRAIN_MAX_N_JOBS must be >= 1")
    tune_n_trials = _opt_int("TUNE_N_TRIALS", tune_trials_default)
    tune_max_n_trials = _opt_int("TUNE_MAX_N_TRIALS", tune_max_trials_default)
    if tune_n_trials < 1:
        raise ConfigError("TUNE_N_TRIALS must be >= 1")
    if tune_max_n_trials < 1:
        raise ConfigError("TUNE_MAX_N_TRIALS must be >= 1")
    if tune_n_trials > tune_max_n_trials:
        raise ConfigError("TUNE_N_TRIALS must be <= TUNE_MAX_N_TRIALS")
    for n_jobs_key in ("LGBM_N_JOBS", "LGBM_RANKER_N_JOBS", "XGB_N_JOBS", "META_LABEL_N_JOBS"):
        if _opt_int(n_jobs_key, int(profile_defaults.get("model_n_jobs") or 1)) < 1:
            raise ConfigError(f"{n_jobs_key} must be >= 1")

    ts_foundation_backend = _opt("TS_FOUNDATION_BACKEND", "chronos").lower() or "chronos"
    if ts_foundation_backend not in {"chronos"}:
        raise ConfigError(f"Invalid TS_FOUNDATION_BACKEND: {ts_foundation_backend}")
    if not _opt("TS_FOUNDATION_CHRONOS_MODEL_ID", _opt("TS_FOUNDATION_MODEL_ID", "amazon/chronos-2")).strip():
        raise ConfigError("TS_FOUNDATION_CHRONOS_MODEL_ID must be non-empty")
    ts_foundation_dim = _opt_int("TS_FOUNDATION_EMBEDDING_DIM", 16)
    if ts_foundation_dim < 1 or ts_foundation_dim > 512:
        raise ConfigError("TS_FOUNDATION_EMBEDDING_DIM must be between 1 and 512")
    ts_foundation_context_rows = _opt_int("TS_FOUNDATION_CONTEXT_ROWS", 256)
    ts_foundation_min_context_rows = _opt_int("TS_FOUNDATION_MIN_CONTEXT_ROWS", 32)
    if ts_foundation_context_rows < 16:
        raise ConfigError("TS_FOUNDATION_CONTEXT_ROWS must be >= 16")
    if ts_foundation_min_context_rows < 4:
        raise ConfigError("TS_FOUNDATION_MIN_CONTEXT_ROWS must be >= 4")
    if ts_foundation_min_context_rows > ts_foundation_context_rows:
        raise ConfigError("TS_FOUNDATION_MIN_CONTEXT_ROWS must be <= TS_FOUNDATION_CONTEXT_ROWS")
    graph_max_neighbors = _opt_int("GRAPH_RELATIONAL_MAX_NEIGHBORS", 24)
    if graph_max_neighbors < 1 or graph_max_neighbors > 512:
        raise ConfigError("GRAPH_RELATIONAL_MAX_NEIGHBORS must be between 1 and 512")
    graph_corr_lookback_rows = _opt_int("GRAPH_RELATIONAL_CORR_LOOKBACK_ROWS", 96)
    if graph_corr_lookback_rows < 8:
        raise ConfigError("GRAPH_RELATIONAL_CORR_LOOKBACK_ROWS must be >= 8")
    graph_corr_min_abs = _opt_float("GRAPH_RELATIONAL_CORR_MIN_ABS", 0.35)
    if not (0.0 <= graph_corr_min_abs <= 1.0):
        raise ConfigError("GRAPH_RELATIONAL_CORR_MIN_ABS must be between 0 and 1")
    if _opt_int("GRAPH_RELATIONAL_NEWS_LOOKBACK_HOURS", 72) < 1:
        raise ConfigError("GRAPH_RELATIONAL_NEWS_LOOKBACK_HOURS must be >= 1")

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
    _opt_bool("USE_TS_FOUNDATION_FEATURES", default=False)
    _opt_bool("TS_FOUNDATION_LOCAL_FILES_ONLY", default=True)
    _opt_bool("TS_FOUNDATION_REQUIRE_ARTIFACT_PERSISTENCE", default=True)
    _opt_bool("USE_GRAPH_RELATIONAL_FEATURES", default=False)
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
    runtime_workload_profile : str
        Workload profile controlling live/offline defaults and guardrails.
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
    runtime_workload_profile: str

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
    workload_profile = normalize_workload_profile(str(safety["workload_profile"]))
    profile_defaults = workload_profile_defaults(workload_profile)
    strict_runtime = bool(safety["strict_runtime"])
    require_explicit_training = bool(safety["require_explicit_training"])
    validate_production_secret_sources(safety)

    from engine.runtime.platform import default_data_root, default_local_db_path

    if strict_runtime:
        default_db_path = str(default_data_root())
    else:
        default_db_path = str(default_local_db_path())
    db_path_raw = _opt("DB_PATH", "")
    if strict_runtime and not db_path_raw:
        raise ConfigError(
            f"DB_PATH must be explicitly set when env={env} engine_mode={engine_mode} strict_runtime=1"
        )
    if not db_path_raw:
        db_path_raw = default_db_path
    db_path_expanded = Path(db_path_raw).expanduser()
    if strict_runtime and not db_path_expanded.is_absolute():
        raise ConfigError(
            "DB_PATH must be absolute when "
            f"env={env} engine_mode={engine_mode} strict_runtime=1: {db_path_raw}"
        )
    data_root_raw = _opt("TS_DATA_ROOT", "")
    if strict_runtime and data_root_raw and not Path(data_root_raw).expanduser().is_absolute():
        raise ConfigError(
            "TS_DATA_ROOT must be absolute when "
            f"env={env} engine_mode={engine_mode} strict_runtime=1: {data_root_raw}"
        )
    db_path = str(db_path_expanded.resolve())

    prod_lock = _opt_bool("PROD_LOCK", default=(env == "prod"))
    allow_training_raw = str(os.environ.get("ALLOW_TRAINING") or "").strip()
    if require_explicit_training and not allow_training_raw:
        raise ConfigError(
            f"ALLOW_TRAINING must be explicitly set when env={env} engine_mode={engine_mode}"
        )
    allow_training = _opt_bool(
        "ALLOW_TRAINING",
        default=bool(profile_defaults.get("allow_training")) and env != "prod",
    )

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
    validate_workload_profile_guardrails(safety)
    _validate_new_subsystem_flags()
    validate_data_source_master_key_config(safety)
    validate_options_instrument_config(safety)
    validate_live_risk_thresholds(safety)
    validate_live_trading_confirmation(safety)

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
        runtime_workload_profile=workload_profile,
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
