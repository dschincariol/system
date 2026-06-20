from __future__ import annotations

"""Live-mode AI serving safety checks.

The checks in this module are intentionally live-only. Development, paper, and
shadow modes can keep using fallback models and research policies, but live
capital must fail closed when AI serving would silently degrade.
"""

import hashlib
import math
import os
import time
from pathlib import Path
from typing import Any, Mapping, Sequence


LIVE_BROKERS = {"alpaca", "ibkr", "interactive_brokers"}
SAFE_BROKERS = {"", "unknown", "sim", "paper", "sandbox", "test", "mock"}
VALID_UNCERTAINTY_PRODUCTION_POLICIES = {"log_only", "shrink", "strict"}

_TRUTHY_VALUES = {"1", "true", "t", "yes", "y", "on"}
_FALSEY_VALUES = {"0", "false", "f", "no", "n", "off"}
_DEFAULT_PREFLIGHT_SYMBOLS = ("SPY",)
_DEFAULT_PREFLIGHT_HORIZONS = (300,)
_NON_PRODUCTION_MODEL_TOKENS = {
    "rl",
    "reinforcement",
    "llm",
    "gpt",
    "openai",
    "advisor",
    "advisory",
    "operatorai",
    "operator_ai",
}


def _normalize_mode(value: Any, default: str = "safe") -> str:
    text = str(value if value is not None else default).strip().lower() or default
    return text


def live_ai_required(
    *,
    engine_mode: Any = None,
    execution_mode: Any = None,
    broker: Any = None,
) -> bool:
    """Return whether live AI strictness is required for this context."""

    modes = {
        _normalize_mode(engine_mode if engine_mode is not None else os.environ.get("ENGINE_MODE")),
        _normalize_mode(execution_mode if execution_mode is not None else os.environ.get("EXECUTION_MODE")),
    }
    if "live" in modes:
        return True
    broker_name = str(
        broker
        if broker is not None
        else (os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or os.environ.get("LIVE_BROKER") or "")
    ).strip().lower()
    if broker_name in LIVE_BROKERS:
        return True
    return bool(broker_name and broker_name not in SAFE_BROKERS)


def _env_text(name: str) -> str:
    return str(os.environ.get(str(name), "") or "").strip()


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None:
        return bool(default)
    text = str(raw).strip().lower()
    if not text:
        return bool(default)
    if text in _FALSEY_VALUES:
        return False
    return text in _TRUTHY_VALUES


def _safe_float(value: Any, default: float = math.nan) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _dedupe(values: Sequence[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        text = str(raw or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _split_csv(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def _numeric_env_snapshot(
    names: Sequence[str],
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    missing_reason: str,
    invalid_reason: str,
) -> dict[str, Any]:
    configured_name = ""
    configured_raw = ""
    for name in names:
        raw = _env_text(str(name))
        if raw:
            configured_name = str(name)
            configured_raw = raw
            break
    if not configured_name:
        return {
            "ok": False,
            "configured": False,
            "name": "",
            "value": None,
            "reason": str(missing_reason),
            "accepted_names": list(names),
        }

    value = _safe_float(configured_raw, math.nan)
    invalid = not math.isfinite(value)
    if minimum is not None and value <= float(minimum):
        invalid = True
    if maximum is not None and value > float(maximum):
        invalid = True
    return {
        "ok": not invalid,
        "configured": True,
        "name": configured_name,
        "raw": configured_raw,
        "value": (float(value) if math.isfinite(value) else None),
        "reason": "ok" if not invalid else str(invalid_reason),
        "accepted_names": list(names),
    }


def live_decision_gate_snapshot(
    *,
    engine_mode: Any = None,
    execution_mode: Any = None,
    broker: Any = None,
    decision_engine: Any = None,
) -> dict[str, Any]:
    """Validate live decision-gate availability and explicit thresholds."""

    required = live_ai_required(engine_mode=engine_mode, execution_mode=execution_mode, broker=broker)
    blockers: list[str] = []
    engine_state: dict[str, Any] = {}

    try:
        engine = decision_engine
        if engine is None:
            from engine.decision_engine import DecisionEngine

            engine = DecisionEngine()
        enabled = bool(getattr(engine, "enabled", False))
        engine_state = {
            "available": True,
            "enabled": bool(enabled),
            "min_confidence": float(getattr(engine, "min_confidence", 0.0) or 0.0),
            "min_abs_prediction": float(getattr(engine, "min_abs_prediction", 0.0) or 0.0),
            "cache_token": str(engine.cache_token()) if hasattr(engine, "cache_token") else "",
        }
    except Exception as exc:
        engine_state = {
            "available": False,
            "enabled": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
        if required:
            blockers.append("live_decision_gate_unavailable")

    if required and not bool(engine_state.get("enabled")):
        blockers.append("live_decision_gate_disabled")

    confidence_threshold = _numeric_env_snapshot(
        ("DECISION_MIN_CONFIDENCE", "PORTFOLIO_MIN_CONF"),
        minimum=0.0,
        maximum=1.0,
        missing_reason="live_confidence_threshold_missing",
        invalid_reason="live_confidence_threshold_invalid",
    )
    prediction_threshold = _numeric_env_snapshot(
        ("DECISION_MIN_ABS_PREDICTION", "PORTFOLIO_MIN_ABS_Z"),
        minimum=0.0,
        missing_reason="live_prediction_threshold_missing",
        invalid_reason="live_prediction_threshold_invalid",
    )
    if required and not bool(confidence_threshold.get("ok")):
        blockers.append(str(confidence_threshold.get("reason") or "live_confidence_threshold_invalid"))
    if required and not bool(prediction_threshold.get("ok")):
        blockers.append(str(prediction_threshold.get("reason") or "live_prediction_threshold_invalid"))

    blockers = _dedupe(blockers)
    return {
        "ok": not blockers,
        "required": bool(required),
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "decision_engine": engine_state,
        "thresholds": {
            "confidence": confidence_threshold,
            "prediction": prediction_threshold,
        },
    }


def assert_live_decision_gate(
    *,
    engine_mode: Any = None,
    execution_mode: Any = None,
    broker: Any = None,
    decision_engine: Any = None,
) -> dict[str, Any]:
    state = live_decision_gate_snapshot(
        engine_mode=engine_mode,
        execution_mode=execution_mode,
        broker=broker,
        decision_engine=decision_engine,
    )
    if bool(state.get("required")) and not bool(state.get("ok")):
        raise RuntimeError("live_decision_gate_failed:" + ",".join(str(x) for x in state.get("blockers") or []))
    return state


def live_uncertainty_threshold_snapshot(
    *,
    engine_mode: Any = None,
    execution_mode: Any = None,
    broker: Any = None,
) -> dict[str, Any]:
    """Validate explicit live uncertainty and OOD threshold configuration."""

    required = live_ai_required(engine_mode=engine_mode, execution_mode=execution_mode, broker=broker)
    blockers: list[str] = []
    policy = _env_text("UNCERTAINTY_SIZING_PRODUCTION_POLICY").lower()
    thresholds: dict[str, dict[str, Any]] = {
        "uncertainty_high": _numeric_env_snapshot(
            ("UNCERTAINTY_HIGH_THRESHOLD",),
            minimum=0.0,
            missing_reason="live_uncertainty_threshold_missing:UNCERTAINTY_HIGH_THRESHOLD",
            invalid_reason="live_uncertainty_threshold_invalid:UNCERTAINTY_HIGH_THRESHOLD",
        ),
        "uncertainty_hard": _numeric_env_snapshot(
            ("UNCERTAINTY_HARD_THRESHOLD",),
            minimum=0.0,
            missing_reason="live_uncertainty_threshold_missing:UNCERTAINTY_HARD_THRESHOLD",
            invalid_reason="live_uncertainty_threshold_invalid:UNCERTAINTY_HARD_THRESHOLD",
        ),
        "uncertainty_max_age_ms": _numeric_env_snapshot(
            ("UNCERTAINTY_MAX_AGE_MS",),
            minimum=0.0,
            missing_reason="live_uncertainty_threshold_missing:UNCERTAINTY_MAX_AGE_MS",
            invalid_reason="live_uncertainty_threshold_invalid:UNCERTAINTY_MAX_AGE_MS",
        ),
        "ood_suppress": _numeric_env_snapshot(
            ("OOD_SUPPRESS_THRESHOLD",),
            minimum=0.0,
            missing_reason="live_ood_threshold_missing:OOD_SUPPRESS_THRESHOLD",
            invalid_reason="live_ood_threshold_invalid:OOD_SUPPRESS_THRESHOLD",
        ),
        "ood_hard": _numeric_env_snapshot(
            ("OOD_HARD_THRESHOLD",),
            minimum=0.0,
            missing_reason="live_ood_threshold_missing:OOD_HARD_THRESHOLD",
            invalid_reason="live_ood_threshold_invalid:OOD_HARD_THRESHOLD",
        ),
    }

    if required and policy not in VALID_UNCERTAINTY_PRODUCTION_POLICIES:
        blockers.append("live_uncertainty_production_policy_missing")
    if required:
        for state in thresholds.values():
            if not bool(state.get("ok")):
                blockers.append(str(state.get("reason") or "live_uncertainty_threshold_invalid"))

    high = _safe_float(thresholds["uncertainty_high"].get("value"), math.nan)
    hard = _safe_float(thresholds["uncertainty_hard"].get("value"), math.nan)
    if required and math.isfinite(high) and math.isfinite(hard) and hard <= high:
        thresholds["uncertainty_hard"]["ok"] = False
        thresholds["uncertainty_hard"]["reason"] = "live_uncertainty_threshold_order_invalid"
        blockers.append("live_uncertainty_threshold_order_invalid")

    ood_suppress = _safe_float(thresholds["ood_suppress"].get("value"), math.nan)
    ood_hard = _safe_float(thresholds["ood_hard"].get("value"), math.nan)
    if required and math.isfinite(ood_suppress) and math.isfinite(ood_hard) and ood_hard <= ood_suppress:
        thresholds["ood_hard"]["ok"] = False
        thresholds["ood_hard"]["reason"] = "live_ood_threshold_order_invalid"
        blockers.append("live_ood_threshold_order_invalid")

    blockers = _dedupe(blockers)
    return {
        "ok": not blockers,
        "required": bool(required),
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "production_policy": str(policy),
        "valid_production_policies": sorted(VALID_UNCERTAINTY_PRODUCTION_POLICIES),
        "thresholds": thresholds,
    }


def _hash_file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _artifact_location_for_model(model_name: str) -> dict[str, str]:
    spec: dict[str, Any] = {}
    config: dict[str, Any] = {}
    try:
        from engine.model_registry import get_model_spec

        spec = dict(get_model_spec(str(model_name), regime="global") or {})
    except Exception:
        spec = {}
    try:
        from engine.strategy.model_config import get_model_config

        config = dict(get_model_config(str(model_name)) or {})
    except Exception:
        config = {}
    return {
        "alias": str(spec.get("artifact_alias") or config.get("artifact_alias") or config.get("artifact_uri") or ""),
        "sha256": str(spec.get("artifact_sha256") or config.get("artifact_sha256") or ""),
        "path": str(spec.get("artifact_path") or config.get("artifact_path") or ""),
    }


def model_feature_contract_snapshot(model_name: str) -> dict[str, Any]:
    """Validate that a live model feature contract has no shadow-only ids."""

    name = str(model_name or "").strip()
    spec: dict[str, Any] = {}
    config: dict[str, Any] = {}
    try:
        from engine.model_registry import get_model_spec

        spec = dict(get_model_spec(name, regime="global") or {})
    except Exception:
        spec = {}
    try:
        from engine.strategy.model_config import get_model_config

        config = dict(get_model_config(name) or {})
    except Exception:
        config = {}
    try:
        from engine.strategy.feature_registry import feature_set_tag_from_ids, resolve_feature_ids, shadow_feature_ids

        feature_ids = resolve_feature_ids(
            (spec.get("feature_ids") or config.get("feature_ids")),
            model_name=name,
            model_spec=(spec or config),
        )
        shadow_ids = shadow_feature_ids(list(feature_ids))
        return {
            "ok": not bool(shadow_ids),
            "reason": "ok" if not shadow_ids else "live_model_shadow_feature_contract",
            "model_name": name,
            "feature_ids": list(feature_ids),
            "feature_set_tag": str(feature_set_tag_from_ids(list(feature_ids))),
            "shadow_feature_ids": list(shadow_ids),
        }
    except Exception as exc:
        return {
            "ok": False,
            "reason": "live_model_feature_contract_invalid",
            "model_name": name,
            "error": f"{type(exc).__name__}: {exc}",
            "feature_ids": [],
            "shadow_feature_ids": [],
        }


def model_artifact_snapshot(model_name: str, location: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Verify that a live model artifact reference exists and is readable."""

    name = str(model_name or "").strip()
    loc = dict(location or _artifact_location_for_model(name))
    alias = str(loc.get("alias") or "").strip()
    sha256 = str(loc.get("sha256") or "").strip().lower()
    path_text = str(loc.get("path") or "").strip()

    if not any((alias, sha256, path_text)):
        return {
            "ok": False,
            "reason": "live_model_artifact_reference_missing",
            "model_name": name,
            "location": {"alias": alias, "sha256": sha256, "path": path_text},
        }

    if alias:
        try:
            from engine.artifacts.store import LocalArtifactStore

            store = LocalArtifactStore()
            ref = store.resolve(alias)
            if ref is None:
                return {
                    "ok": False,
                    "reason": "live_model_artifact_missing",
                    "model_name": name,
                    "location": {"alias": alias, "sha256": sha256, "path": path_text},
                    "detail": "artifact_alias_not_found",
                }
            if sha256 and str(ref.sha256).lower() != sha256:
                return {
                    "ok": False,
                    "reason": "live_model_artifact_sha_mismatch",
                    "model_name": name,
                    "location": {"alias": alias, "sha256": sha256, "path": path_text},
                    "resolved_sha256": str(ref.sha256),
                }
            payload = store.get_bytes(ref, verify=True)
            return {
                "ok": bool(payload),
                "reason": "ok" if payload else "live_model_artifact_empty",
                "model_name": name,
                "location": {"alias": alias, "sha256": str(ref.sha256), "path": path_text},
                "size_bytes": int(len(payload)),
            }
        except Exception as exc:
            return {
                "ok": False,
                "reason": "live_model_artifact_read_failed",
                "model_name": name,
                "location": {"alias": alias, "sha256": sha256, "path": path_text},
                "error": f"{type(exc).__name__}: {exc}",
            }

    if path_text:
        path = Path(path_text).expanduser()
        try:
            if not path.is_file():
                return {
                    "ok": False,
                    "reason": "live_model_artifact_missing",
                    "model_name": name,
                    "location": {"alias": alias, "sha256": sha256, "path": str(path)},
                    "detail": "artifact_path_not_file",
                }
            size = int(path.stat().st_size)
            if size <= 0:
                return {
                    "ok": False,
                    "reason": "live_model_artifact_empty",
                    "model_name": name,
                    "location": {"alias": alias, "sha256": sha256, "path": str(path)},
                    "size_bytes": size,
                }
            actual_sha = _hash_file_sha256(path)
            if sha256 and actual_sha != sha256:
                return {
                    "ok": False,
                    "reason": "live_model_artifact_sha_mismatch",
                    "model_name": name,
                    "location": {"alias": alias, "sha256": sha256, "path": str(path)},
                    "resolved_sha256": actual_sha,
                }
            return {
                "ok": True,
                "reason": "ok",
                "model_name": name,
                "location": {"alias": alias, "sha256": sha256 or actual_sha, "path": str(path)},
                "size_bytes": size,
            }
        except Exception as exc:
            return {
                "ok": False,
                "reason": "live_model_artifact_read_failed",
                "model_name": name,
                "location": {"alias": alias, "sha256": sha256, "path": str(path)},
                "error": f"{type(exc).__name__}: {exc}",
            }

    if sha256:
        try:
            from engine.artifacts.paths import object_path, validate_sha256

            digest = validate_sha256(sha256)
            path = object_path(digest)
            if not path.is_file():
                return {
                    "ok": False,
                    "reason": "live_model_artifact_missing",
                    "model_name": name,
                    "location": {"alias": alias, "sha256": sha256, "path": str(path)},
                }
            size = int(path.stat().st_size)
            actual_sha = _hash_file_sha256(path)
            if actual_sha != digest:
                return {
                    "ok": False,
                    "reason": "live_model_artifact_sha_mismatch",
                    "model_name": name,
                    "location": {"alias": alias, "sha256": digest, "path": str(path)},
                    "resolved_sha256": actual_sha,
                }
            return {
                "ok": size > 0,
                "reason": "ok" if size > 0 else "live_model_artifact_empty",
                "model_name": name,
                "location": {"alias": alias, "sha256": digest, "path": str(path)},
                "size_bytes": size,
            }
        except Exception as exc:
            return {
                "ok": False,
                "reason": "live_model_artifact_read_failed",
                "model_name": name,
                "location": {"alias": alias, "sha256": sha256, "path": path_text},
                "error": f"{type(exc).__name__}: {exc}",
            }

    return {
        "ok": False,
        "reason": "live_model_artifact_reference_missing",
        "model_name": name,
        "location": {"alias": alias, "sha256": sha256, "path": path_text},
    }


def _probe_symbols() -> list[str]:
    raw = _env_text("LIVE_AI_PREFLIGHT_SYMBOLS") or _env_text("MODEL_PREFLIGHT_SYMBOLS")
    symbols = [str(item).upper().strip() for item in _split_csv(raw)]
    return _dedupe(symbols or list(_DEFAULT_PREFLIGHT_SYMBOLS))


def _probe_horizons() -> list[int]:
    raw = _env_text("LIVE_AI_PREFLIGHT_HORIZONS_S") or _env_text("MODEL_PREFLIGHT_HORIZONS_S")
    values = [_safe_int(item, 0) for item in _split_csv(raw)]
    horizons = [int(value) for value in values if int(value) > 0]
    if horizons:
        return horizons
    try:
        from engine.strategy.model_config import configured_model_horizons

        horizons = [int(value) for value in configured_model_horizons(default=_DEFAULT_PREFLIGHT_HORIZONS) if int(value) > 0]
    except Exception:
        horizons = list(_DEFAULT_PREFLIGHT_HORIZONS)
    return list(dict.fromkeys(horizons or list(_DEFAULT_PREFLIGHT_HORIZONS)))[:5]


def live_model_serving_snapshot(
    *,
    engine_mode: Any = None,
    execution_mode: Any = None,
    broker: Any = None,
    symbols: Sequence[Any] | None = None,
    horizons_s: Sequence[Any] | None = None,
) -> dict[str, Any]:
    """Validate live model resolution and artifact readability."""

    required = live_ai_required(engine_mode=engine_mode, execution_mode=execution_mode, broker=broker)
    blockers: list[str] = []
    probes: list[dict[str, Any]] = []
    if not required:
        return {"ok": True, "required": False, "reason": "not_live", "blockers": [], "probes": []}

    probe_symbols = [str(sym).upper().strip() for sym in (symbols or _probe_symbols()) if str(sym or "").strip()]
    probe_horizons = [_safe_int(h, 0) for h in (horizons_s or _probe_horizons()) if _safe_int(h, 0) > 0]
    if not probe_symbols:
        probe_symbols = list(_DEFAULT_PREFLIGHT_SYMBOLS)
    if not probe_horizons:
        probe_horizons = list(_DEFAULT_PREFLIGHT_HORIZONS)

    try:
        from engine.strategy import predictor
    except Exception as exc:
        return {
            "ok": False,
            "required": True,
            "reason": "live_model_serving_snapshot_unavailable",
            "blockers": ["live_model_serving_snapshot_unavailable"],
            "error": f"{type(exc).__name__}: {exc}",
            "probes": [],
        }

    checked_models: set[str] = set()
    for symbol in probe_symbols:
        for horizon_s in probe_horizons:
            resolution: dict[str, Any]
            try:
                resolution = dict(predictor._live_model_resolution(str(symbol), int(horizon_s)) or {})
            except Exception as exc:
                resolution = {
                    "error": f"{type(exc).__name__}: {exc}",
                    "resolved_model_name": "",
                    "requested_model_name": "",
                    "serve_fallback_active": True,
                    "fallback_reason": "live_model_resolution_failed",
                }
            model_name = str(resolution.get("resolved_model_name") or resolution.get("requested_model_name") or "").strip()
            probe: dict[str, Any] = {
                "symbol": str(symbol),
                "horizon_s": int(horizon_s),
                "resolution": dict(resolution),
            }
            if bool(resolution.get("serve_fallback_active")):
                blockers.append("live_model_resolution_fallback")
                probe["ok"] = False
            if not model_name:
                blockers.append("live_model_resolution_missing")
                probe["ok"] = False
            if model_name and model_name not in checked_models:
                checked_models.add(model_name)
                artifact = model_artifact_snapshot(model_name)
                probe["artifact"] = dict(artifact)
                if not bool(artifact.get("ok")):
                    blockers.append(str(artifact.get("reason") or "live_model_artifact_missing"))
                    probe["ok"] = False
                feature_contract = model_feature_contract_snapshot(model_name)
                probe["feature_contract"] = dict(feature_contract)
                if not bool(feature_contract.get("ok")):
                    blockers.append(str(feature_contract.get("reason") or "live_model_shadow_feature_contract"))
                    probe["ok"] = False
            probe.setdefault("ok", True)
            probes.append(probe)

    blockers = _dedupe(blockers)
    return {
        "ok": not blockers,
        "required": True,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "symbols": list(probe_symbols),
        "horizons_s": list(probe_horizons),
        "probes": probes,
    }


def live_rl_policy_snapshot(
    *,
    engine_mode: Any = None,
    execution_mode: Any = None,
    broker: Any = None,
) -> dict[str, Any]:
    """Validate that live mode is not using training/shadow RL placeholders."""

    required = live_ai_required(engine_mode=engine_mode, execution_mode=execution_mode, broker=broker)
    blockers: list[str] = []
    active_flags = {
        name: _env_text(name)
        for name in (
            "RL_STRATEGY_POLICY_ENABLED",
            "RL_STRATEGY_POLICY_LIVE_ENABLED",
            "RL_POLICY_ENABLED",
            "RL_PORTFOLIO_LIVE_ENABLED",
            "RL_PORTFOLIO_EXECUTION_ENABLED",
        )
    }
    active = any(_env_truthy(name, False) for name in active_flags)
    fallback_agent_allowed = _env_truthy("RL_ALLOW_FALLBACK_AGENT", False)
    model_name = _env_text("MODEL_NAME").lower()
    if any(token in model_name for token in ("rl", "reinforcement")):
        active = True

    if required and fallback_agent_allowed:
        blockers.append("live_rl_fallback_agent_allowed")
    if required and active:
        blockers.append("live_rl_placeholder_policy_active")

    blockers = _dedupe(blockers)
    return {
        "ok": not blockers,
        "required": bool(required),
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "active": bool(active),
        "fallback_agent_allowed": bool(fallback_agent_allowed),
        "env": {
            **active_flags,
            "RL_ALLOW_FALLBACK_AGENT": _env_text("RL_ALLOW_FALLBACK_AGENT"),
            "MODEL_NAME": _env_text("MODEL_NAME"),
        },
    }


def live_ai_safety_snapshot(
    *,
    engine_mode: Any = None,
    execution_mode: Any = None,
    broker: Any = None,
    include_model_checks: bool = True,
) -> dict[str, Any]:
    """Return the aggregate live AI safety state."""

    required = live_ai_required(engine_mode=engine_mode, execution_mode=execution_mode, broker=broker)
    decision = live_decision_gate_snapshot(engine_mode=engine_mode, execution_mode=execution_mode, broker=broker)
    uncertainty = live_uncertainty_threshold_snapshot(engine_mode=engine_mode, execution_mode=execution_mode, broker=broker)
    rl_policy = live_rl_policy_snapshot(engine_mode=engine_mode, execution_mode=execution_mode, broker=broker)
    model_serving = (
        live_model_serving_snapshot(engine_mode=engine_mode, execution_mode=execution_mode, broker=broker)
        if include_model_checks
        else {"ok": True, "required": bool(required), "reason": "not_checked", "blockers": []}
    )
    blockers = _dedupe(
        [
            *list(decision.get("blockers") or []),
            *list(uncertainty.get("blockers") or []),
            *list(rl_policy.get("blockers") or []),
            *list(model_serving.get("blockers") or []),
        ]
    )
    return {
        "ok": not blockers,
        "required": bool(required),
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "decision_gate": dict(decision),
        "uncertainty_thresholds": dict(uncertainty),
        "rl_policy": dict(rl_policy),
        "model_serving": dict(model_serving),
    }


def _nested_candidates(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    obj = dict(payload or {})
    candidates: list[dict[str, Any]] = [obj]
    for key in (
        "explain",
        "reason",
        "model_intent",
        "alpha_intent",
        "signal",
        "decision",
        "model",
        "selector",
        "serve_fallback",
        "ensemble_output",
        "uncertainty",
        "metadata",
    ):
        value = obj.get(key)
        if isinstance(value, Mapping):
            candidates.append(dict(value))
    idx = 0
    while idx < len(candidates):
        candidate = candidates[idx]
        idx += 1
        for key in (
            "explain",
            "model_intent",
            "alpha_intent",
            "signal",
            "decision",
            "model",
            "selector",
            "serve_fallback",
            "ensemble_output",
            "uncertainty",
            "metadata",
        ):
            nested = candidate.get(key)
            if isinstance(nested, Mapping):
                nested_dict = dict(nested)
                if nested_dict not in candidates:
                    candidates.append(nested_dict)
    return candidates[:40]


def _contains_non_production_model_token(value: Any) -> bool:
    import re

    text = str(value or "").strip().lower()
    if not text:
        return False
    normalized = text.replace("-", " ").replace("_", " ")
    tokens = [tok for tok in re.split(r"[^a-z0-9]+", normalized) if tok]
    return any(tok in _NON_PRODUCTION_MODEL_TOKENS for tok in tokens)


def live_ai_order_guard(
    payload: Mapping[str, Any] | None,
    *,
    execution_mode: Any = None,
    broker: Any = None,
    risk_increasing: bool = True,
    decision_engine: Any = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    """Fail closed for live risk-increasing orders with degraded AI evidence."""

    del now_ms
    required = bool(live_ai_required(execution_mode=execution_mode, broker=broker) and risk_increasing)
    if not required:
        return {"ok": True, "required": False, "reason": "not_live_risk_increasing", "blockers": []}

    blockers: list[str] = []
    diagnostics: dict[str, Any] = {}
    decision = live_decision_gate_snapshot(
        execution_mode=execution_mode,
        broker=broker,
        decision_engine=decision_engine,
    )
    uncertainty = live_uncertainty_threshold_snapshot(execution_mode=execution_mode, broker=broker)
    if not bool(decision.get("ok")):
        blockers.extend(str(item) for item in list(decision.get("blockers") or []))
    if not bool(uncertainty.get("ok")):
        blockers.extend(str(item) for item in list(uncertainty.get("blockers") or []))
    diagnostics["decision_gate"] = dict(decision)
    diagnostics["uncertainty_thresholds"] = dict(uncertainty)

    candidates = _nested_candidates(payload)
    fallback_reasons: list[str] = []
    artifact_failures: list[dict[str, Any]] = []
    for candidate in candidates:
        fallback = candidate.get("serve_fallback")
        fallback_reason = str(candidate.get("fallback_reason") or "").strip()
        if bool(candidate.get("serve_fallback_active")):
            blockers.append("live_model_resolution_fallback")
            if fallback_reason:
                fallback_reasons.append(fallback_reason)
        if isinstance(fallback, Mapping) and fallback:
            blockers.append("live_model_resolution_fallback")
            reason = str(fallback.get("reason") or "").strip()
            if reason:
                fallback_reasons.append(reason)
        if fallback_reason and any(
            token in fallback_reason
            for token in (
                "requested_live_model_unavailable",
                "live_model_resolution_fallback",
                "live_model_resolution_failed",
                "resolved_to_",
            )
        ):
            blockers.append("live_model_resolution_fallback")
            fallback_reasons.append(fallback_reason)

        model_name = str(candidate.get("model_name") or candidate.get("model") or "").strip()
        loc = {
            "alias": str(candidate.get("artifact_alias") or ""),
            "sha256": str(candidate.get("artifact_sha256") or ""),
            "path": str(candidate.get("artifact_path") or ""),
        }
        if model_name and any(loc.values()):
            artifact = model_artifact_snapshot(model_name, loc)
            if not bool(artifact.get("ok")):
                blockers.append(str(artifact.get("reason") or "live_model_artifact_missing"))
                artifact_failures.append(dict(artifact))

    online_failures: list[dict[str, Any]] = []
    for candidate in candidates:
        model_text = " ".join(
            str(candidate.get(key) or "")
            for key in ("model_kind", "model_family", "model_name", "model_backend", "model_class")
        ).lower()
        if "online" not in model_text and "sgd" not in model_text:
            continue
        prediction = _safe_float(
            candidate.get(
                "prediction",
                candidate.get("expected_z", candidate.get("predicted_z", candidate.get("score"))),
            ),
            math.nan,
        )
        updates = candidate.get("online_updates", candidate.get("n_updates", candidate.get("model_n")))
        unfitted = candidate.get("model_fitted") is False or candidate.get("fitted") is False
        dummy_flag = bool(candidate.get("dummy_zero_prediction") or candidate.get("unfitted_dummy_prediction"))
        if math.isfinite(prediction) and abs(float(prediction)) <= 1e-12 and (
            dummy_flag or unfitted or _safe_int(updates, 0) <= 0
        ):
            blockers.append("live_online_dummy_zero_prediction")
            online_failures.append(
                {
                    "model": model_text,
                    "prediction": float(prediction),
                    "updates": _safe_int(updates, 0),
                    "dummy_flag": bool(dummy_flag),
                    "unfitted": bool(unfitted),
                }
            )

    rl_sources: list[str] = []
    for candidate in candidates:
        for key in ("model_id", "model_name", "model_kind", "strategy_name", "policy_name", "policy_type"):
            value = candidate.get(key)
            if _contains_non_production_model_token(value):
                blockers.append("live_rl_placeholder_policy_active")
                rl_sources.append(f"{key}:{value}")
        if any(key in candidate for key in ("rl_choice", "rl_score", "rl_policy", "rl_action")):
            blockers.append("live_rl_placeholder_policy_active")
            rl_sources.append("rl_selector_metadata")
        if candidate.get("training_only") is True or candidate.get("live_execution_allowed") is False:
            if _contains_non_production_model_token(candidate.get("model_name") or candidate.get("policy_name") or "rl"):
                blockers.append("live_rl_placeholder_policy_active")
                rl_sources.append("training_only_or_live_forbidden")
        if candidate.get("fallback") is True and _contains_non_production_model_token(
            candidate.get("model_name") or candidate.get("policy_name") or "rl"
        ):
            blockers.append("live_rl_placeholder_policy_active")
            rl_sources.append("fallback_rl_policy")

    diagnostics.update(
        {
            "fallback_reasons": _dedupe(fallback_reasons),
            "artifact_failures": artifact_failures,
            "online_failures": online_failures,
            "rl_sources": _dedupe(rl_sources),
        }
    )
    blockers = _dedupe(blockers)
    return {
        "ok": not blockers,
        "required": True,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "diagnostics": diagnostics,
        "ts_ms": int(time.time() * 1000),
    }


def assert_live_ai_safety(
    *,
    engine_mode: Any = None,
    execution_mode: Any = None,
    broker: Any = None,
    include_model_checks: bool = True,
) -> dict[str, Any]:
    state = live_ai_safety_snapshot(
        engine_mode=engine_mode,
        execution_mode=execution_mode,
        broker=broker,
        include_model_checks=include_model_checks,
    )
    if bool(state.get("required")) and not bool(state.get("ok")):
        raise RuntimeError("live_ai_safety_failed:" + ",".join(str(x) for x in state.get("blockers") or []))
    return state


__all__ = [
    "SAFE_BROKERS",
    "VALID_UNCERTAINTY_PRODUCTION_POLICIES",
    "assert_live_ai_safety",
    "assert_live_decision_gate",
    "live_ai_order_guard",
    "live_ai_required",
    "live_ai_safety_snapshot",
    "live_decision_gate_snapshot",
    "live_model_serving_snapshot",
    "live_rl_policy_snapshot",
    "live_uncertainty_threshold_snapshot",
    "model_artifact_snapshot",
]
