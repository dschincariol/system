"""Runtime workload profile helpers.

The live profile is conservative and treats training/research jobs as offline
work. The offline profile intentionally allows higher local parallelism, but it
must be selected explicitly by the runtime environment.
"""

from __future__ import annotations

import os
from typing import Any, Mapping, Sequence

LIVE_PROFILE = "live"
OFFLINE_PROFILE = "offline"
LIVE_PROFILE_OFFLINE_TRAINING_ACK_PHRASE = "I_UNDERSTAND_OFFLINE_TRAINING_IN_LIVE_PROFILE"

_PROFILE_ALIASES = {
    "runtime": LIVE_PROFILE,
    "production": LIVE_PROFILE,
    "prod": LIVE_PROFILE,
    "research": OFFLINE_PROFILE,
    "training": OFFLINE_PROFILE,
    "train": OFFLINE_PROFILE,
}

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

_LIVE_DEFAULTS: dict[str, int | bool | str] = {
    "profile": LIVE_PROFILE,
    "allow_training": False,
    "model_n_jobs": 1,
    "model_max_n_jobs": 2,
    "tsfresh_n_jobs": 0,
    "tsfresh_max_n_jobs": 1,
    "tsfresh_snapshot_symbol_limit": 100,
    "tsfresh_snapshot_batch_size": 25,
    "tune_n_trials": 10,
    "tune_max_n_trials": 10,
    "resource_scheduler_global_max": 2,
    "resource_scheduler_execution_max": 1,
    "resource_scheduler_inference_max": 1,
    "resource_scheduler_training_max": 1,
    "resource_scheduler_replay_max": 1,
    "resource_scheduler_background_max": 1,
}

_OFFLINE_DEFAULTS: dict[str, int | bool | str] = {
    "profile": OFFLINE_PROFILE,
    "allow_training": True,
    "model_n_jobs": 8,
    "model_max_n_jobs": 16,
    "tsfresh_n_jobs": 4,
    "tsfresh_max_n_jobs": 16,
    "tsfresh_snapshot_symbol_limit": 5000,
    "tsfresh_snapshot_batch_size": 250,
    "tune_n_trials": 200,
    "tune_max_n_trials": 500,
    "resource_scheduler_global_max": 8,
    "resource_scheduler_execution_max": 0,
    "resource_scheduler_inference_max": 2,
    "resource_scheduler_training_max": 4,
    "resource_scheduler_replay_max": 2,
    "resource_scheduler_background_max": 4,
}

_OFFLINE_TRAINING_FLAGS = (
    "ALLOW_TRAINING",
    "AUTO_PIPELINE",
    "AUTO_CHALLENGER",
    "AUTO_SIZE_POLICY",
    "TUNE_MODELS_ENABLED",
    "DRIFT_RETRAIN_ENABLED",
)

_HEAVY_FEATURE_FLAGS = (
    "USE_TSFRESH_FEATURES",
    "TSFRESH_LIVE_COMPUTE_ENABLED",
    "USE_TS_FOUNDATION_FEATURES",
    "USE_GRAPH_RELATIONAL_FEATURES",
    "LLM_FACTOR_DISCOVERY",
    "SHAP_LIVE_COMPUTE_ENABLED",
)


def _clean(value: Any) -> str:
    return str(value if value is not None else "").strip()


def _truthy(value: Any, default: bool = False) -> bool:
    text = _clean(value).lower()
    if not text:
        return bool(default)
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _parse_int(value: Any, default: int) -> int:
    text = _clean(value)
    if not text:
        return int(default)
    try:
        return int(float(text))
    except Exception:
        return int(default)


def _is_placeholder_value(raw: object) -> bool:
    text = _clean(raw)
    if not text:
        return True
    lowered = text.lower().replace("_", "-").replace(" ", "")
    return lowered in _PLACEHOLDER_VALUES


def normalize_workload_profile(raw: Any) -> str:
    profile = _clean(raw).lower() or LIVE_PROFILE
    profile = _PROFILE_ALIASES.get(profile, profile)
    if profile not in {LIVE_PROFILE, OFFLINE_PROFILE}:
        raise ValueError(f"Invalid RUNTIME_WORKLOAD_PROFILE: {raw}")
    return profile


def workload_profile_from_env(env: Mapping[str, str] | None = None) -> str:
    source = env if env is not None else os.environ
    raw = source.get("RUNTIME_WORKLOAD_PROFILE") or source.get("TRADING_WORKLOAD_PROFILE") or LIVE_PROFILE
    return normalize_workload_profile(raw)


def workload_profile_defaults(profile: str | None = None) -> dict[str, int | bool | str]:
    normalized = normalize_workload_profile(profile or workload_profile_from_env())
    defaults = _OFFLINE_DEFAULTS if normalized == OFFLINE_PROFILE else _LIVE_DEFAULTS
    return dict(defaults)


def bounded_n_jobs(
    primary_key: str,
    *,
    fallback_keys: Sequence[str] = (),
    max_key: str = "MODEL_TRAIN_MAX_N_JOBS",
    default_key: str = "model_n_jobs",
    max_default_key: str = "model_max_n_jobs",
    allow_zero: bool = False,
    env: Mapping[str, str] | None = None,
) -> int:
    source = env if env is not None else os.environ
    defaults = workload_profile_defaults(workload_profile_from_env(source))
    default_value = int(defaults.get(default_key, 1) or 1)
    max_default = int(defaults.get(max_default_key, default_value) or default_value)
    raw = ""
    for key in (str(primary_key), *[str(item) for item in fallback_keys]):
        raw = _clean(source.get(key))
        if raw:
            break
    parsed = _parse_int(raw, default_value)
    lower = 0 if bool(allow_zero) else 1
    if parsed < lower:
        parsed = lower
    max_jobs = _parse_int(source.get(max_key), max_default)
    max_jobs = max(lower, max(1, int(max_jobs)))
    if parsed > max_jobs:
        parsed = max_jobs
    return int(parsed)


def tsfresh_n_jobs(env: Mapping[str, str] | None = None) -> int:
    return bounded_n_jobs(
        "TSFRESH_N_JOBS",
        fallback_keys=("TSFRESH_WORKERS",),
        max_key="TSFRESH_MAX_N_JOBS",
        default_key="tsfresh_n_jobs",
        max_default_key="tsfresh_max_n_jobs",
        allow_zero=True,
        env=env,
    )


def model_family_n_jobs(
    primary_key: str,
    *,
    fallback_keys: Sequence[str] = ("MODEL_TRAIN_N_JOBS",),
    env: Mapping[str, str] | None = None,
) -> int:
    return bounded_n_jobs(
        primary_key,
        fallback_keys=fallback_keys,
        max_key="MODEL_TRAIN_MAX_N_JOBS",
        default_key="model_n_jobs",
        max_default_key="model_max_n_jobs",
        allow_zero=False,
        env=env,
    )


def profile_int(
    primary_key: str,
    *,
    default_key: str,
    min_value: int = 1,
    max_key: str | None = None,
    max_default_key: str | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    source = env if env is not None else os.environ
    defaults = workload_profile_defaults(workload_profile_from_env(source))
    lower = int(min_value)
    default_value = int(defaults.get(default_key, lower) or lower)
    parsed = _parse_int(source.get(str(primary_key)), default_value)
    if parsed < lower:
        parsed = lower
    if max_key:
        max_default = int(defaults.get(max_default_key or default_key, parsed) or parsed)
        max_value = _parse_int(source.get(str(max_key)), max_default)
        max_value = max(lower, int(max_value))
        if parsed > max_value:
            parsed = max_value
    return int(parsed)


def tsfresh_snapshot_symbol_limit(env: Mapping[str, str] | None = None) -> int:
    return profile_int(
        "TSFRESH_SNAPSHOT_SYMBOL_LIMIT",
        default_key="tsfresh_snapshot_symbol_limit",
        min_value=1,
        max_key="TSFRESH_SNAPSHOT_MAX_SYMBOLS",
        max_default_key="tsfresh_snapshot_symbol_limit",
        env=env,
    )


def tsfresh_snapshot_batch_size(env: Mapping[str, str] | None = None) -> int:
    return profile_int(
        "TSFRESH_SNAPSHOT_BATCH_SIZE",
        default_key="tsfresh_snapshot_batch_size",
        min_value=1,
        max_key="TSFRESH_SNAPSHOT_MAX_BATCH_SIZE",
        max_default_key="tsfresh_snapshot_batch_size",
        env=env,
    )


def tuning_n_trials(env: Mapping[str, str] | None = None) -> int:
    return profile_int(
        "TUNE_N_TRIALS",
        default_key="tune_n_trials",
        min_value=1,
        max_key="TUNE_MAX_N_TRIALS",
        max_default_key="tune_max_n_trials",
        env=env,
    )


def offline_training_ack_snapshot(
    env: Mapping[str, str] | None = None,
    *,
    profile: str | None = None,
    force_required: bool = False,
) -> dict[str, Any]:
    source = env if env is not None else os.environ
    normalized = normalize_workload_profile(profile or workload_profile_from_env(source))
    defaults = workload_profile_defaults(normalized)
    enabled: list[str] = []

    for name in _OFFLINE_TRAINING_FLAGS:
        default = bool(defaults.get("allow_training")) if name == "ALLOW_TRAINING" else False
        if _truthy(source.get(name), default=default):
            enabled.append(name)

    for name in _HEAVY_FEATURE_FLAGS:
        if _truthy(source.get(name), default=False):
            enabled.append(name)

    if force_required and normalized == LIVE_PROFILE and not enabled:
        enabled.append("OFFLINE_JOB_REQUESTED")

    required = bool(normalized == LIVE_PROFILE and enabled)
    ack = _clean(source.get("OFFLINE_TRAINING_LIVE_PROFILE_ACK"))
    owner = _clean(source.get("OFFLINE_TRAINING_LIVE_PROFILE_OWNER"))
    reason = _clean(source.get("OFFLINE_TRAINING_LIVE_PROFILE_REASON"))
    blockers: list[str] = []
    if required:
        if ack != LIVE_PROFILE_OFFLINE_TRAINING_ACK_PHRASE:
            blockers.append(
                "OFFLINE_TRAINING_LIVE_PROFILE_ACK must equal "
                + LIVE_PROFILE_OFFLINE_TRAINING_ACK_PHRASE
            )
        if _is_placeholder_value(owner):
            blockers.append("OFFLINE_TRAINING_LIVE_PROFILE_OWNER required")
        if _is_placeholder_value(reason):
            blockers.append("OFFLINE_TRAINING_LIVE_PROFILE_REASON required")

    return {
        "required": bool(required),
        "ok": bool(not required or not blockers),
        "profile": normalized,
        "enabled_settings": list(dict.fromkeys(enabled)),
        "acknowledged": bool(required and not blockers),
        "blockers": blockers,
        "expected_ack": LIVE_PROFILE_OFFLINE_TRAINING_ACK_PHRASE,
        "audit": {"owner": owner, "reason": reason} if required and not blockers else {},
    }


def assert_offline_work_allowed(*, job_name: str = "offline_job", env: Mapping[str, str] | None = None) -> dict[str, Any]:
    snapshot = offline_training_ack_snapshot(env, force_required=True)
    if not bool(snapshot.get("ok")):
        blockers = "; ".join(str(item) for item in list(snapshot.get("blockers") or []))
        enabled = ",".join(str(item) for item in list(snapshot.get("enabled_settings") or []))
        raise RuntimeError(
            "offline_training_live_profile_ack_required "
            f"job={job_name} enabled_settings={enabled} blockers={blockers}"
        )
    return snapshot


__all__ = [
    "LIVE_PROFILE",
    "OFFLINE_PROFILE",
    "LIVE_PROFILE_OFFLINE_TRAINING_ACK_PHRASE",
    "assert_offline_work_allowed",
    "bounded_n_jobs",
    "model_family_n_jobs",
    "normalize_workload_profile",
    "offline_training_ack_snapshot",
    "profile_int",
    "tsfresh_snapshot_batch_size",
    "tsfresh_snapshot_symbol_limit",
    "tsfresh_n_jobs",
    "tuning_n_trials",
    "workload_profile_defaults",
    "workload_profile_from_env",
]
