"""Runtime-owned environment configuration exports and schema bridge.

The authoritative schema lives in `engine.runtime.config_schema`, while this
module preserves the repo's env-driven compatibility surface for runtime,
supervisor, and job-manager imports.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from engine.runtime.config_schema import ConfigError, get_runtime_safety_context, load_runtime_config
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger


# ---------------------------------------------------------
# schema bridge (authoritative when available)
# ---------------------------------------------------------

try:
    _SCHEMA_CFG = load_runtime_config()
    _SCHEMA_ERROR = None
except ConfigError as e:
    _SCHEMA_CFG = None
    _SCHEMA_ERROR = str(e)
    os.environ["RUNTIME_CONFIG_ERROR"] = _SCHEMA_ERROR

    try:
        _strict_cfg = bool(get_runtime_safety_context().get("strict_runtime"))
    except ConfigError:
        _strict_cfg = (
            str(os.environ.get("ENGINE_SUPERVISED", "")).strip().lower() in ("1", "true", "yes", "y", "on")
            or str(os.environ.get("ENV", "")).strip().lower() in ("prod", "production")
        )
    if _strict_cfg:
        raise


def _schema_attr(name: str, default):
    if _SCHEMA_CFG is None:
        return default
    return getattr(_SCHEMA_CFG, name, default)


# ---------------------------------------------------------
# helpers
# ---------------------------------------------------------

LOG = get_logger("runtime.config")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="runtime_config_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.config",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _env_bool(key: str, default: bool = False) -> bool:
    # Centralized env parsing keeps legacy callers aligned on the same truthy
    # values instead of each module inventing its own version.
    v = os.environ.get(key)
    if v is None:
        return bool(default)
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, str(default)))
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_CONFIG_ENV_INT_FAILED",
            e,
            once_key=f"runtime_config_env_int:{key}",
            key=str(key),
            default=default,
        )
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, str(default)))
    except Exception as e:
        _warn_nonfatal(
            "RUNTIME_CONFIG_ENV_FLOAT_FAILED",
            e,
            once_key=f"runtime_config_env_float:{key}",
            key=str(key),
            default=default,
        )
        return default


# ---------------------------------------------------------
# canonical schema-backed exports
# ---------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
from engine.runtime.platform import default_data_root

_DEFAULT_DB_PATH = str(default_data_root())

ENV = _schema_attr("env", os.environ.get("ENV", "dev"))
DB_PATH = _schema_attr("db_path", os.environ.get("DB_PATH", _DEFAULT_DB_PATH))
PROD_LOCK = _schema_attr("prod_lock", _env_bool("PROD_LOCK", ENV == "prod"))
ALLOW_TRAINING = _schema_attr("allow_training", _env_bool("ALLOW_TRAINING", ENV != "prod"))
SUPERVISOR_ENABLED = _schema_attr("supervisor_enabled", _env_bool("SUPERVISOR_ENABLED", True))
SUPERVISOR_TICK_S = _schema_attr("supervisor_tick_s", _env_int("SUPERVISOR_TICK_S", 2))
EXEC_DEGRADE_BLOCK = _schema_attr("exec_degrade_block", _env_bool("EXEC_DEGRADE_BLOCK", True))
EXEC_DEGRADE_WARN_COST_PCT = _schema_attr("exec_degrade_warn_cost_pct", _env_float("EXEC_DEGRADE_WARN_COST_PCT", 0.25))
EXEC_DEGRADE_CRIT_COST_PCT = _schema_attr("exec_degrade_crit_cost_pct", _env_float("EXEC_DEGRADE_CRIT_COST_PCT", 0.50))
ENSEMBLE_BLEND_ENABLED = _schema_attr("ensemble_blend_enabled", _env_bool("ENSEMBLE_BLEND_ENABLED", False))
ENSEMBLE_BLEND_MODE = _schema_attr("ensemble_blend_mode", os.environ.get("ENSEMBLE_BLEND_MODE", "equal"))
ENSEMBLE_MAX_WEIGHT = _schema_attr("ensemble_max_weight", _env_float("ENSEMBLE_MAX_WEIGHT", 0.75))
ENSEMBLE_MIN_AGREEMENT = _schema_attr("ensemble_min_agreement", _env_float("ENSEMBLE_MIN_AGREEMENT", 0.0))
ENSEMBLE_META_RETRAIN_S = _schema_attr("ensemble_meta_retrain_s", _env_int("ENSEMBLE_META_RETRAIN_S", 86400))


# ---------------------------------------------------------
# Feature store
# ---------------------------------------------------------

FEATURE_STORE_VERSION = _env_int("FEATURE_STORE_VERSION", 1)
FEATURE_STORE_ENABLED = _env_bool("FEATURE_STORE_ENABLED", False)
FEATURE_STORE_READS_ENABLED = _env_bool("FEATURE_STORE_READS_ENABLED", False)
FEATURE_STORE_INIT_ON_STARTUP = _env_bool("FEATURE_STORE_INIT_ON_STARTUP", True)


# ---------------------------------------------------------
# Optional alternative data
# ---------------------------------------------------------

USE_FORM4_DATA = _schema_attr("use_form4_data", _env_bool("USE_FORM4_DATA", False))
USE_CONGRESSIONAL_TRADE_DATA = _schema_attr(
    "use_congressional_trade_data",
    _env_bool("USE_CONGRESSIONAL_TRADE_DATA", False),
)
FORM4_BACKFILL_DAYS = _schema_attr("form4_backfill_days", _env_int("FORM4_BACKFILL_DAYS", 180))
CONGRESSIONAL_BACKFILL_DAYS = _schema_attr(
    "congressional_backfill_days",
    _env_int("CONGRESSIONAL_BACKFILL_DAYS", 180),
)
INGEST_FORM4_ENABLED = _schema_attr("ingest_form4_enabled", _env_bool("INGEST_FORM4_ENABLED", False))
INGEST_CONGRESSIONAL_ENABLED = _schema_attr(
    "ingest_congressional_enabled",
    _env_bool("INGEST_CONGRESSIONAL_ENABLED", False),
)
USE_PIT_UNIVERSE = _schema_attr("use_pit_universe", _env_bool("USE_PIT_UNIVERSE", False))
PIT_UNIVERSE_BACKFILL_ENABLED = _schema_attr(
    "pit_universe_backfill_enabled",
    _env_bool("PIT_UNIVERSE_BACKFILL_ENABLED", False),
)


# ---------------------------------------------------------
# Auto-restart guards (MUST be enabled by default)
# ---------------------------------------------------------

# Restart/watchdog defaults live here because both JobsManager and the older
# supervisor rely on the same semantics.
AUTO_RESTART_DAEMONS = _env_bool("AUTO_RESTART_DAEMONS", True)

DAEMON_RESTART_BASE_DELAY_MS = _env_int("DAEMON_RESTART_BASE_DELAY_MS", 2000)
DAEMON_RESTART_MAX_DELAY_MS = _env_int("DAEMON_RESTART_MAX_DELAY_MS", 30000)
DAEMON_RESTART_WINDOW_S = _env_int("DAEMON_RESTART_WINDOW_S", 120)
DAEMON_RESTART_MAX_IN_WINDOW = _env_int("DAEMON_RESTART_MAX_IN_WINDOW", 5)
DAEMON_WATCHDOG_PERIOD_S = _env_float("DAEMON_WATCHDOG_PERIOD_S", 1.0)


# ---------------------------------------------------------
# Scheduler knobs
# ---------------------------------------------------------

AUTO_RECALIBRATE = _env_bool("AUTO_RECALIBRATE", True)
AUTO_RECALIBRATE_INTERVAL_S = 86400

AUTO_SIZE_POLICY = _env_bool("AUTO_SIZE_POLICY", False)
AUTO_SIZE_POLICY_INTERVAL_S = _env_float("AUTO_SIZE_POLICY_INTERVAL_S", 86400.0)
AUTO_SIZE_POLICY_START_DELAY_S = _env_float("AUTO_SIZE_POLICY_START_DELAY_S", 20.0)
AUTO_SIZE_POLICY_LOG = _env_bool("AUTO_SIZE_POLICY_LOG", True)

AUTO_PIPELINE = _env_bool("AUTO_PIPELINE", False)
AUTO_PIPELINE_INTERVAL_S = _env_float("AUTO_PIPELINE_INTERVAL_S", 300.0)
AUTO_PIPELINE_START_DELAY_S = _env_float("AUTO_PIPELINE_START_DELAY_S", 2.0)
AUTO_PIPELINE_LOG = _env_bool("AUTO_PIPELINE_LOG", True)

AUTO_CHALLENGER = _env_bool("AUTO_CHALLENGER", False)
AUTO_CHALLENGER_INTERVAL_S = _env_float("AUTO_CHALLENGER_INTERVAL_S", 3600.0)
AUTO_CHALLENGER_START_DELAY_S = _env_float("AUTO_CHALLENGER_START_DELAY_S", 10.0)
AUTO_CHALLENGER_LOG = _env_bool("AUTO_CHALLENGER_LOG", True)

AUTO_CHALLENGER_MIN_DRIFT = _env_float("AUTO_CHALLENGER_MIN_DRIFT", 0.0)
AUTO_PIPELINE_INCLUDE_EXECUTION = _env_bool("AUTO_PIPELINE_INCLUDE_EXECUTION", False)

# In SAFE, execution must be disabled even if other loops are enabled
EXECUTION_DISABLED_IN_SAFE = _env_bool("EXECUTION_DISABLED_IN_SAFE", True)


# ---------------------------------------------------------
# Resource scheduler
# ---------------------------------------------------------

RESOURCE_SCHEDULER_ENABLE = _env_bool("RESOURCE_SCHEDULER_ENABLE", True)
RESOURCE_SCHEDULER_GLOBAL_MAX = _env_int("RESOURCE_SCHEDULER_GLOBAL_MAX", 2)
RESOURCE_SCHEDULER_EXECUTION_MAX = _env_int("RESOURCE_SCHEDULER_EXECUTION_MAX", 1)
RESOURCE_SCHEDULER_INFERENCE_MAX = _env_int("RESOURCE_SCHEDULER_INFERENCE_MAX", 1)
RESOURCE_SCHEDULER_TRAINING_MAX = _env_int("RESOURCE_SCHEDULER_TRAINING_MAX", 1)
RESOURCE_SCHEDULER_REPLAY_MAX = _env_int("RESOURCE_SCHEDULER_REPLAY_MAX", 1)
RESOURCE_SCHEDULER_BACKGROUND_MAX = _env_int("RESOURCE_SCHEDULER_BACKGROUND_MAX", 1)


# ---------------------------------------------------------
# Health thresholds
# ---------------------------------------------------------

HEALTH_PRICES_MAX_AGE_S = _env_float("HEALTH_PRICES_MAX_AGE_S", 120.0)
HEALTH_EVENTS_MAX_AGE_S = _env_float("HEALTH_EVENTS_MAX_AGE_S", 600.0)
HEALTH_PREDICTIONS_MAX_AGE_S = _env_float("HEALTH_PREDICTIONS_MAX_AGE_S", 600.0)
HEALTH_JOBS_MAX_STALE_S = _env_float("HEALTH_JOBS_MAX_STALE_S", 180.0)

HEALTH_MIN_LABELS = _env_int("HEALTH_MIN_LABELS", 10)
HEALTH_MIN_MODEL_SUPPORT = _env_int("HEALTH_MIN_MODEL_SUPPORT", 10)


# ---------------------------------------------------------
# Training
# ---------------------------------------------------------

TRAINING_RESUME_MIN_OK_STREAK = _env_int("TRAINING_RESUME_MIN_OK_STREAK", 5)


# ---------------------------------------------------------
# Preflight
# ---------------------------------------------------------

PREFLIGHT_ENABLE = _env_bool("PREFLIGHT_ENABLE", True)

# CRITICAL:
# Do NOT block daemons during warmup or you deadlock.
PREFLIGHT_BLOCK_JOBS = _env_bool("PREFLIGHT_BLOCK_JOBS", False)

PREFLIGHT_PRICES_MAX_AGE_S = _env_float("PREFLIGHT_PRICES_MAX_AGE_S", 300.0)
