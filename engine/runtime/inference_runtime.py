"""Runtime reader interfaces for live inference inputs and catalog lookups."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.runtime.inference_runtime")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.inference_runtime",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(str(once_key))


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def get_online_feature_contract() -> Dict[str, Any]:
    try:
        from engine.data import feature_store as feature_store_module

        feature_names = [
            str(name).strip()
            for name in list(getattr(feature_store_module, "FEATURE_NAMES", ()) or ())
            if str(name).strip()
        ]
        return {
            "ok": True,
            "feature_names": list(feature_names),
            "feature_set_tag": str(getattr(feature_store_module, "FEATURE_SET_TAG", "") or ""),
            "schema_version": int(getattr(feature_store_module, "FEATURE_SCHEMA_VERSION", 0) or 0),
            "source": "engine.data.feature_store",
        }
    except Exception as exc:
        _warn_nonfatal(
            "INFERENCE_RUNTIME_FEATURE_CONTRACT_FAILED",
            exc,
            once_key="feature_contract",
        )
        return {
            "ok": False,
            "feature_names": [],
            "feature_set_tag": "",
            "schema_version": 0,
            "source": "unavailable",
        }


def _zero_feature_snapshot(symbol: str, *, contract: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    resolved_contract = dict(contract or {})
    feature_names = [
        str(name).strip()
        for name in list(resolved_contract.get("feature_names") or [])
        if str(name).strip()
    ]
    return {
        "symbol": str(_normalize_symbol(symbol)),
        "ts_ms": 0,
        "schema_version": int(resolved_contract.get("schema_version") or 0),
        "feature_set_tag": str(resolved_contract.get("feature_set_tag") or ""),
        "feature_names": list(feature_names),
        "vector": [0.0 for _ in feature_names],
        "point_count": 0,
        "source_timestamps": {},
        "features": {str(name): 0.0 for name in feature_names},
    }


def read_online_feature_snapshot(symbol: str, *, persist: bool = False) -> Dict[str, Any]:
    symbol_key = _normalize_symbol(symbol)
    contract = get_online_feature_contract()
    if not symbol_key:
        return _zero_feature_snapshot("", contract=contract)
    try:
        from engine.data.feature_store import get_live_features

        snapshot = get_live_features(symbol_key, persist=bool(persist))
        if isinstance(snapshot, Mapping):
            return dict(snapshot)
    except Exception as exc:
        _warn_nonfatal(
            "INFERENCE_RUNTIME_FEATURE_READ_FAILED",
            exc,
            once_key=f"feature_read:{symbol_key}",
            symbol=str(symbol_key),
        )
    return _zero_feature_snapshot(symbol_key, contract=contract)


def validate_online_feature_snapshot(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    snapshot_dict = dict(snapshot or {})
    try:
        from engine.data.feature_store import validate_feature_snapshot

        validation = validate_feature_snapshot(snapshot_dict)
        if isinstance(validation, Mapping):
            return dict(validation)
    except Exception as exc:
        _warn_nonfatal(
            "INFERENCE_RUNTIME_FEATURE_VALIDATE_FAILED",
            exc,
            once_key="feature_validate",
        )
    feature_ts_ms = int(_safe_int(snapshot_dict.get("ts_ms"), 0) or 0)
    detail = "feature_snapshot_validation_unavailable"
    status = "invalid"
    reason_codes = ["feature_snapshot_validation_unavailable"]
    if feature_ts_ms <= 0:
        detail = "feature_store_empty"
        status = "empty"
        reason_codes = ["feature_store_empty"]
    return {
        "ok": False,
        "status": str(status),
        "detail": str(detail),
        "feature_ts_ms": int(feature_ts_ms),
        "feature_set_tag": str(snapshot_dict.get("feature_set_tag") or ""),
        "stale": bool(feature_ts_ms <= 0),
        "missing_required_features": [],
        "reason_codes": list(reason_codes),
    }


def load_online_model_record(
    symbol: str,
    *,
    model_name: Optional[str] = None,
    version: Optional[str] = None,
    active_only: bool = False,
    allow_db_refresh: bool = True,
) -> Optional[Dict[str, Any]]:
    symbol_key = _normalize_symbol(symbol)
    if not symbol_key:
        return None
    try:
        from engine.runtime.model_cache import load_model_record

        record = load_model_record(
            symbol_key,
            model_name=model_name,
            version=version,
            active_only=bool(active_only),
            allow_db_refresh=bool(allow_db_refresh),
        )
        return dict(record) if isinstance(record, Mapping) else None
    except Exception as exc:
        _warn_nonfatal(
            "INFERENCE_RUNTIME_MODEL_LOAD_FAILED",
            exc,
            once_key=f"model_load:{symbol_key}:{model_name}:{version}:{int(bool(active_only))}",
            symbol=str(symbol_key),
            model_name=str(model_name or ""),
            version=str(version or ""),
        )
        return None


def list_online_model_records(
    symbol: Optional[str] = None,
    *,
    model_name: Optional[str] = None,
    active_only: bool = False,
    limit: int = 100,
    allow_db_refresh: bool = True,
) -> List[Dict[str, Any]]:
    try:
        from engine.runtime.model_cache import list_model_records

        rows = list_model_records(
            symbol=symbol,
            model_name=model_name,
            active_only=bool(active_only),
            limit=int(limit),
            allow_db_refresh=bool(allow_db_refresh),
        )
        return [dict(row) for row in (rows or []) if isinstance(row, Mapping)]
    except Exception as exc:
        _warn_nonfatal(
            "INFERENCE_RUNTIME_MODEL_LIST_FAILED",
            exc,
            once_key=f"model_list:{symbol}:{model_name}:{int(bool(active_only))}",
            symbol=str(symbol or ""),
            model_name=str(model_name or ""),
        )
        return []


def get_best_online_model_record(
    symbol: str,
    *,
    model_name: Optional[str] = None,
    metric_name: Optional[str] = None,
    higher_is_better: Optional[bool] = None,
    allow_db_refresh: bool = True,
) -> Optional[Dict[str, Any]]:
    symbol_key = _normalize_symbol(symbol)
    if not symbol_key:
        return None
    try:
        from engine.runtime.model_cache import get_best_model_record

        record = get_best_model_record(
            symbol_key,
            model_name=model_name,
            metric_name=metric_name,
            higher_is_better=higher_is_better,
            allow_db_refresh=bool(allow_db_refresh),
        )
        return dict(record) if isinstance(record, Mapping) else None
    except Exception as exc:
        _warn_nonfatal(
            "INFERENCE_RUNTIME_MODEL_BEST_FAILED",
            exc,
            once_key=f"model_best:{symbol_key}:{model_name}:{metric_name}",
            symbol=str(symbol_key),
            model_name=str(model_name or ""),
            metric_name=str(metric_name or ""),
        )
        return None


__all__ = [
    "get_best_online_model_record",
    "get_online_feature_contract",
    "list_online_model_records",
    "load_online_model_record",
    "read_online_feature_snapshot",
    "validate_online_feature_snapshot",
]
