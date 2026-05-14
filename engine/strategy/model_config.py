"""
Config-backed model instance normalization.

This keeps model-family diversity additive: callers still work with existing
model names/interfaces, but those names can now be generated from JSON-backed
instance definitions instead of hardcoded constants.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.strategy.feature_registry import (
    BASE_FEATURE_IDS,
    MACRO_FEATURE_IDS,
    OPTIONS_FEATURE_IDS,
    SOCIAL_FEATURE_IDS,
    SOCIAL_REGIME_FEATURE_IDS,
    STRESS_FEATURE_IDS,
    TECH_FEATURE_IDS,
    WEATHER_FEATURE_IDS,
    default_feature_ids,
    feature_set_tag_from_ids,
)

MODEL_CONFIG_FILE_ENV = "MODEL_CONFIG_FILE"
MODEL_CONFIG_JSON_ENV = "MODEL_CONFIG_JSON"
MODEL_INSTANCE_CONFIG_JSON_ENV = "MODEL_INSTANCE_CONFIG_JSON"
ENABLE_EXPERIMENTAL_MODELS_ENV = "ENABLE_EXPERIMENTAL_MODELS"

DEFAULT_MODEL_CONFIG_PATH = Path(__file__).resolve().parents[2] / "data" / "model_configs.json"
DEFAULT_FAMILY = str(os.environ.get("MODEL_NAME", "embed_regressor") or "embed_regressor").strip() or "embed_regressor"

DEFAULT_HORIZON_MAP = {
    "short": int(os.environ.get("MODEL_HORIZON_SHORT_S", "300") or 300),
    "medium": int(os.environ.get("MODEL_HORIZON_MEDIUM_S", "3600") or 3600),
    "long": int(os.environ.get("MODEL_HORIZON_LONG_S", "86400") or 86400),
}

FEATURE_GROUPS = {
    "base": list(BASE_FEATURE_IDS),
    "tech": list(TECH_FEATURE_IDS),
    "stress": list(STRESS_FEATURE_IDS),
    "macro": list(MACRO_FEATURE_IDS),
    "social": list(SOCIAL_FEATURE_IDS),
    "social_regime": list(SOCIAL_REGIME_FEATURE_IDS),
    "weather": list(WEATHER_FEATURE_IDS),
    "options": list(OPTIONS_FEATURE_IDS),
}
LOG = get_logger("engine.strategy.model_config")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.strategy.model_config",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return int(default)
    if isinstance(value, str) and not value.strip():
        return int(default)
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal("MODEL_CONFIG_SAFE_INT_FAILED", e, once_key="safe_int", value=repr(value), default=int(default))
        return int(default)


def _safe_bool(value: Any, default: bool = True) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return bool(value)
    text = str(value or "").strip().lower()
    if not text:
        return bool(default)
    return text in {"1", "true", "yes", "y", "on"}


def _slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def _safe_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
        except Exception as e:
            _warn_nonfatal("MODEL_CONFIG_JSON_DICT_PARSE_FAILED", e, once_key="safe_json_dict", value=repr(value)[:512])
            return {}
        return dict(obj) if isinstance(obj, dict) else {}
    return {}


def _safe_json_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, dict):
        models = value.get("models")
        return list(models) if isinstance(models, list) else []
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
        except Exception as e:
            _warn_nonfatal("MODEL_CONFIG_JSON_LIST_PARSE_FAILED", e, once_key="safe_json_list", value=repr(value)[:512])
            return []
        if isinstance(obj, list):
            return list(obj)
        if isinstance(obj, dict):
            models = obj.get("models")
            return list(models) if isinstance(models, list) else []
    return []


def _dedupe_strings(values: Iterable[Any], *, upper: bool = False) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        item = str(value or "").strip()
        if upper:
            item = item.upper()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _expand_horizon_value(value: Any) -> List[int]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        out: List[int] = []
        for item in value:
            for horizon_s in _expand_horizon_value(item):
                if horizon_s > 0 and horizon_s not in out:
                    out.append(int(horizon_s))
        return out
    if isinstance(value, str):
        text = str(value).strip().lower()
        if not text:
            return []
        if "," in text:
            return _expand_horizon_value([part for part in text.split(",") if str(part).strip()])
        mapped = DEFAULT_HORIZON_MAP.get(text)
        if mapped:
            return [int(mapped)]
    try:
        horizon_s = int(value)
    except Exception as e:
        _warn_nonfatal("MODEL_CONFIG_HORIZON_PARSE_FAILED", e, once_key="expand_horizon_value", value=repr(value))
        return []
    return [int(horizon_s)] if int(horizon_s) > 0 else []


def _resolve_feature_ids(config: Dict[str, Any]) -> List[str]:
    explicit = config.get("feature_ids")
    if isinstance(explicit, list) and explicit:
        return _dedupe_strings(explicit)

    group_values: List[Any] = []
    for key in ("feature_groups", "feature_group", "feature_set", "feature_subset"):
        raw = config.get(key)
        if raw is None:
            continue
        if isinstance(raw, (list, tuple, set)):
            group_values.extend(list(raw))
        else:
            group_values.append(raw)

    out: List[str] = []
    seen = set()
    for raw_group in group_values:
        group = str(raw_group or "").strip().lower()
        if not group:
            continue
        if group == "default":
            group_feature_ids = list(default_feature_ids())
        elif group in FEATURE_GROUPS:
            group_feature_ids = list(FEATURE_GROUPS[group])
        else:
            group_feature_ids = []
        if group_feature_ids:
            for feature_id in group_feature_ids:
                if feature_id not in seen:
                    seen.add(feature_id)
                    out.append(str(feature_id))
            continue
        # Unknown group names are treated as direct feature ids so configs can
        # mix presets and one-off features without extra schema.
        if group not in seen:
            seen.add(group)
            out.append(group)

    return out or list(default_feature_ids())


def _load_raw_model_config_objects() -> List[Dict[str, Any]]:
    for env_key in (MODEL_INSTANCE_CONFIG_JSON_ENV, MODEL_CONFIG_JSON_ENV):
        raw = str(os.environ.get(env_key, "") or "").strip()
        items = _safe_json_list(raw)
        if not items and env_key == MODEL_INSTANCE_CONFIG_JSON_ENV:
            single = _safe_json_dict(raw)
            if single:
                items = [single]
        if items:
            return [dict(item) for item in items if isinstance(item, dict)]

    path_candidates = []
    env_path = str(os.environ.get(MODEL_CONFIG_FILE_ENV, "") or "").strip()
    if env_path:
        path_candidates.append(Path(env_path).expanduser())
    path_candidates.append(DEFAULT_MODEL_CONFIG_PATH)

    for path in path_candidates:
        try:
            if not path or not path.exists():
                continue
            obj = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            _warn_nonfatal(
                "MODEL_CONFIG_FILE_LOAD_FAILED",
                e,
                once_key=f"model_config_file:{path}",
                path=str(path),
            )
            continue
        items = _safe_json_list(obj)
        if items:
            return [dict(item) for item in items if isinstance(item, dict)]

    return []


def _normalize_model_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    config = dict(raw or {})
    family = str(
        config.get("family")
        or config.get("model_family")
        or config.get("base_model_name")
        or config.get("family_name")
        or DEFAULT_FAMILY
    ).strip() or DEFAULT_FAMILY

    explicit_name = str(config.get("model_name") or "").strip()
    instance_name = str(
        config.get("instance_name")
        or config.get("variant")
        or config.get("name")
        or config.get("id")
        or ""
    ).strip()
    if explicit_name:
        model_name = explicit_name
    elif instance_name and instance_name != family:
        model_name = f"{family}.{_slug(instance_name)}"
    else:
        model_name = str(family)

    model_id = str(config.get("model_id") or model_name).strip() or str(model_name)

    horizons_s = []
    for key in ("horizons_s", "horizons", "horizon_s", "horizon"):
        horizons_s = _expand_horizon_value(config.get(key))
        if horizons_s:
            break
    if not horizons_s:
        horizons_s = _expand_horizon_value(DEFAULT_HORIZON_MAP["medium"])

    feature_ids = _resolve_feature_ids(config)
    feature_set_tag = str(config.get("feature_set_tag") or feature_set_tag_from_ids(feature_ids)).strip()

    training_window_days = _safe_int(
        config.get("training_window_days")
        or config.get("lookback_days")
        or config.get("train_window_days")
        or os.environ.get("EMBED_MODEL_LOOKBACK_DAYS", "365"),
        365,
    )
    symbol_universe = _dedupe_strings(
        config.get("symbol_universe")
        or config.get("symbols")
        or config.get("universe")
        or [],
        upper=True,
    )

    horizon_bucket = "medium"
    first_horizon = int(horizons_s[0]) if horizons_s else 0
    if first_horizon > 0 and first_horizon <= DEFAULT_HORIZON_MAP["short"]:
        horizon_bucket = "short"
    elif first_horizon >= DEFAULT_HORIZON_MAP["long"]:
        horizon_bucket = "long"

    enabled = bool(_safe_bool(config.get("enabled"), True))
    prediction_enabled = bool(_safe_bool(config.get("prediction_enabled"), enabled))
    experimental = bool(_safe_bool(config.get("experimental"), False))

    normalized = {
        **config,
        "enabled": bool(enabled),
        "prediction_enabled": bool(prediction_enabled),
        "experimental": bool(experimental),
        "active": bool(enabled and prediction_enabled and not experimental),
        "family": str(family),
        "instance_name": str(instance_name or model_name),
        "model_name": str(model_name),
        "model_id": str(model_id),
        "model_kind": str(config.get("model_kind") or os.environ.get("EMBED_MODEL_KIND", "ridge")).strip().lower() or "ridge",
        "risk_profile": str(config.get("risk_profile") or "balanced").strip().lower() or "balanced",
        "horizons_s": [int(h) for h in horizons_s if int(h) > 0],
        "horizon_s": int(first_horizon),
        "horizon_bucket": str(config.get("horizon_bucket") or horizon_bucket),
        "feature_ids": list(feature_ids),
        "feature_set_tag": str(feature_set_tag),
        "training_window_days": int(max(1, training_window_days)),
        "symbol_universe": list(symbol_universe),
        "notes": str(config.get("notes") or "").strip(),
    }
    return normalized


def load_model_configs(*, family: Optional[str] = None, include_disabled: bool = False) -> List[Dict[str, Any]]:
    raw_items = _load_raw_model_config_objects()
    if not raw_items:
        raw_items = [{"family": str(family or DEFAULT_FAMILY)}]

    out: List[Dict[str, Any]] = []
    for raw in raw_items:
        cfg = _normalize_model_config(raw)
        if family and str(cfg.get("family") or "").strip() != str(family):
            continue
        if not include_disabled and not bool(cfg.get("enabled")):
            continue
        out.append(cfg)
    return out


def get_model_config(model_name: str, *, family: Optional[str] = None) -> Dict[str, Any]:
    target = str(model_name or "").strip()
    if not target:
        return {}
    for cfg in load_model_configs(family=family, include_disabled=True):
        if str(cfg.get("model_name") or "").strip() == target:
            return dict(cfg)
    return {}


def configured_model_horizons(default: Optional[Iterable[int]] = None, *, family: Optional[str] = None) -> List[int]:
    horizons: List[int] = []
    for cfg in load_model_configs(family=family):
        for horizon_s in list(cfg.get("horizons_s") or []):
            hs = _safe_int(horizon_s, 0)
            if hs > 0 and hs not in horizons:
                horizons.append(hs)
    if not horizons:
        for horizon_s in list(default or []):
            hs = _safe_int(horizon_s, 0)
            if hs > 0 and hs not in horizons:
                horizons.append(hs)
    return horizons


def configured_model_names(
    *,
    symbol: Optional[str] = None,
    horizon_s: Optional[int] = None,
    family: Optional[str] = None,
    include_disabled: bool = False,
) -> List[str]:
    sym = str(symbol or "").upper().strip()
    hs = _safe_int(horizon_s, 0)
    out: List[str] = []
    for cfg in load_model_configs(family=family, include_disabled=include_disabled):
        universe = [str(item).upper().strip() for item in list(cfg.get("symbol_universe") or []) if str(item or "").strip()]
        if sym and universe and "*" not in universe and sym not in universe:
            continue
        if hs > 0 and hs not in list(cfg.get("horizons_s") or []):
            continue
        name = str(cfg.get("model_name") or "").strip()
        if name and name not in out:
            out.append(name)
    return out


def select_default_model_name(
    *,
    symbol: Optional[str] = None,
    horizon_s: Optional[int] = None,
    family: Optional[str] = None,
) -> str:
    names = configured_model_names(symbol=symbol, horizon_s=horizon_s, family=family)
    return str(names[0]) if names else ""


def experimental_models_enabled() -> bool:
    return bool(_safe_bool(os.environ.get(ENABLE_EXPERIMENTAL_MODELS_ENV), False))


def load_active_model_configs(*, family: Optional[str] = None) -> List[Dict[str, Any]]:
    return [
        dict(cfg)
        for cfg in load_model_configs(family=family, include_disabled=True)
        if bool(cfg.get("active"))
    ]


def active_model_names(
    *,
    symbol: Optional[str] = None,
    horizon_s: Optional[int] = None,
    family: Optional[str] = None,
) -> List[str]:
    sym = str(symbol or "").upper().strip()
    hs = _safe_int(horizon_s, 0)
    out: List[str] = []
    for cfg in load_active_model_configs(family=family):
        universe = [
            str(item).upper().strip()
            for item in list(cfg.get("symbol_universe") or [])
            if str(item or "").strip()
        ]
        if sym and universe and "*" not in universe and sym not in universe:
            continue
        if hs > 0 and hs not in list(cfg.get("horizons_s") or []):
            continue
        name = str(cfg.get("model_name") or "").strip()
        if name and name not in out:
            out.append(name)
    return out


def primary_active_model_name(*, family: Optional[str] = None) -> str:
    names = active_model_names(family=family)
    return str(names[0]) if names else ""


def is_active_model_name(model_name: str, *, family: Optional[str] = None) -> bool:
    target = str(model_name or "").strip()
    if not target:
        return False
    for cfg in load_active_model_configs(family=family):
        if str(cfg.get("model_name") or "").strip() == target:
            return True
    return False


def resolve_active_model_name(
    *,
    symbol: Optional[str] = None,
    horizon_s: Optional[int] = None,
    preferred_names: Optional[Iterable[Any]] = None,
    family: Optional[str] = None,
) -> str:
    candidates = active_model_names(symbol=symbol, horizon_s=horizon_s, family=family)
    if preferred_names:
        allowed = set(candidates)
        for raw_name in preferred_names:
            name = str(raw_name or "").strip()
            if name and name in allowed:
                return str(name)
    return str(candidates[0]) if candidates else ""


def build_model_registration_metadata(config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = dict(config or {})
    feature_ids = list(cfg.get("feature_ids") or default_feature_ids())
    feature_set_tag = str(cfg.get("feature_set_tag") or feature_set_tag_from_ids(feature_ids)).strip()
    return {
        "model_id": str(cfg.get("model_id") or cfg.get("model_name") or ""),
        "model_family": str(cfg.get("family") or cfg.get("model_name") or ""),
        "instance_name": str(cfg.get("instance_name") or cfg.get("model_name") or ""),
        "horizon_s": _safe_int(cfg.get("horizon_s"), 0),
        "horizons_s": [int(h) for h in list(cfg.get("horizons_s") or []) if _safe_int(h, 0) > 0],
        "horizon_bucket": str(cfg.get("horizon_bucket") or ""),
        "feature_ids": feature_ids,
        "feature_set_tag": str(feature_set_tag),
        "feature_schema": {
            "feature_ids": feature_ids,
            "feature_set_tag": str(feature_set_tag),
        },
        "symbol_universe": list(cfg.get("symbol_universe") or []),
        "risk_profile": str(cfg.get("risk_profile") or ""),
        "training_window_days": _safe_int(cfg.get("training_window_days"), 0),
        "model_kind": str(cfg.get("model_kind") or ""),
    }
