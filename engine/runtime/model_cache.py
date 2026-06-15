"""In-memory serving cache for model catalog reads."""

from __future__ import annotations

import copy
import logging
import threading
import time
from typing import Any, Dict, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.observability import record_component_health

LOG = get_logger("runtime.model_cache")
_LOCK = threading.RLock()
_MODEL_ROWS: list[dict[str, Any]] = []
_MODEL_ROWS_LOADED = False
_LAST_REFRESH_TS_MS = 0
_LAST_ERROR = ""
_MODEL_DB_PATH = ""


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.model_cache",
        extra=dict(extra or {}) or None,
        persist=False,
    )


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_float(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        _warn_nonfatal("MODEL_CACHE_FLOAT_PARSE_FAILED", exc, value=repr(value)[:120], default=default)
        return default


def _current_db_path() -> str:
    try:
        from engine.runtime.db_guard import resolve_db_path

        return str(resolve_db_path())
    except Exception as exc:
        _warn_nonfatal("MODEL_CACHE_DB_PATH_RESOLVE_FAILED", exc)
        return ""


def _reset_cache_if_db_changed() -> None:
    global _MODEL_ROWS, _MODEL_ROWS_LOADED, _LAST_REFRESH_TS_MS, _LAST_ERROR, _MODEL_DB_PATH
    current_db_path = _current_db_path()
    with _LOCK:
        if str(_MODEL_DB_PATH or "") == str(current_db_path or ""):
            return
        _MODEL_ROWS = []
        _MODEL_ROWS_LOADED = False
        _LAST_REFRESH_TS_MS = 0
        _LAST_ERROR = ""
        _MODEL_DB_PATH = str(current_db_path or "")


def warm_model_catalog(*, force: bool = False, limit: int = 5000, readonly: bool = False) -> dict[str, Any]:
    """Warm the in-memory model catalog from the persisted registry."""
    global _MODEL_ROWS, _MODEL_ROWS_LOADED, _LAST_REFRESH_TS_MS, _LAST_ERROR
    _reset_cache_if_db_changed()
    with _LOCK:
        if _MODEL_ROWS_LOADED and not force:
            return get_snapshot()
    try:
        from engine.model_registry import list_models

        rows = [
            dict(row)
            for row in (
                list_models(limit=max(100, int(limit)), readonly=bool(readonly)) or []
            )
            if isinstance(row, dict)
        ]
        rows.sort(
            key=lambda row: (
                _normalize_symbol(row.get("symbol")),
                str(row.get("model_name") or ""),
                0 if bool(row.get("is_active")) else 1,
                -int(row.get("updated_ts_ms") or 0),
                -int(row.get("created_ts_ms") or 0),
            )
        )
        with _LOCK:
            _MODEL_ROWS = rows
            _MODEL_ROWS_LOADED = True
            _LAST_REFRESH_TS_MS = int(time.time() * 1000)
            _LAST_ERROR = ""
        record_component_health(
            "model_cache",
            ok=True,
            status="ok",
            detail="model_catalog_warmed",
            observed_ts_ms=int(_LAST_REFRESH_TS_MS),
            extra={"rows": int(len(rows))},
        )
        return get_snapshot()
    except Exception as exc:
        with _LOCK:
            _LAST_ERROR = f"{type(exc).__name__}:{exc}"
        _warn_nonfatal("MODEL_CACHE_WARM_FAILED", exc)
        record_component_health(
            "model_cache",
            ok=False,
            status="error",
            detail=str(_LAST_ERROR),
            extra={"rows": int(len(_MODEL_ROWS))},
        )
        return get_snapshot()


def invalidate_model_catalog() -> None:
    """Mark the in-memory model catalog stale so the next read refreshes it."""
    global _MODEL_ROWS_LOADED
    _reset_cache_if_db_changed()
    with _LOCK:
        _MODEL_ROWS_LOADED = False


def upsert_model_record(record: Dict[str, Any]) -> None:
    """Insert or replace one model record inside the in-memory catalog cache."""
    global _MODEL_ROWS, _MODEL_ROWS_LOADED, _LAST_REFRESH_TS_MS, _LAST_ERROR
    _reset_cache_if_db_changed()
    row = dict(record or {})
    symbol = _normalize_symbol(row.get("symbol"))
    model_name = str(row.get("model_name") or "").strip()
    version = str(row.get("version") or "").strip()
    if not symbol or not model_name or not version:
        return
    with _LOCK:
        kept: list[dict[str, Any]] = []
        replaced = False
        for existing in _MODEL_ROWS:
            if (
                _normalize_symbol(existing.get("symbol")) == symbol
                and str(existing.get("model_name") or "").strip() == model_name
                and str(existing.get("version") or "").strip() == version
            ):
                kept.append(dict(row))
                replaced = True
            else:
                kept.append(existing)
        if not replaced:
            kept.append(dict(row))
        _MODEL_ROWS = kept
        _MODEL_ROWS_LOADED = True
        _LAST_REFRESH_TS_MS = int(time.time() * 1000)
        _LAST_ERROR = ""


def _snapshot_rows(*, allow_db_refresh: bool = False) -> list[dict[str, Any]]:
    _reset_cache_if_db_changed()
    with _LOCK:
        loaded = bool(_MODEL_ROWS_LOADED)
        rows = [dict(row) for row in _MODEL_ROWS]
    if loaded or not allow_db_refresh:
        return rows
    warm_model_catalog(force=False)
    with _LOCK:
        return [dict(row) for row in _MODEL_ROWS]


def list_model_records(
    symbol: Optional[str] = None,
    *,
    model_name: Optional[str] = None,
    active_only: bool = False,
    limit: int = 100,
    allow_db_refresh: bool = False,
) -> List[Dict[str, Any]]:
    """List cached model records with optional symbol and activity filters."""
    symbol_u = _normalize_symbol(symbol) if symbol else ""
    name = str(model_name or "").strip()
    out = []
    for row in _snapshot_rows(allow_db_refresh=allow_db_refresh):
        if symbol_u and _normalize_symbol(row.get("symbol")) != symbol_u:
            continue
        if name and str(row.get("model_name") or "").strip() != name:
            continue
        if active_only and not bool(row.get("is_active")):
            continue
        out.append(dict(row))
    out.sort(
        key=lambda row: (
            1 if bool(row.get("is_active")) else 0,
            int(row.get("updated_ts_ms") or 0),
            int(row.get("created_ts_ms") or 0),
        ),
        reverse=True,
    )
    return out[: max(1, int(limit or 100))]


def load_model_record(
    symbol: str,
    *,
    model_name: Optional[str] = None,
    version: Optional[str] = None,
    active_only: bool = False,
    allow_db_refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    """Load one cached model record by symbol, name, and optional version."""
    version_s = str(version or "").strip()
    rows = list_model_records(
        symbol=str(symbol),
        model_name=model_name,
        active_only=active_only,
        limit=1000,
        allow_db_refresh=allow_db_refresh,
    )
    for row in rows:
        if version_s and str(row.get("version") or "").strip() != version_s:
            continue
        return dict(row)
    return None


def get_best_model_record(
    symbol: str,
    *,
    model_name: Optional[str] = None,
    metric_name: Optional[str] = None,
    higher_is_better: Optional[bool] = None,
    allow_db_refresh: bool = False,
) -> Optional[Dict[str, Any]]:
    """Return the best cached model record according to the requested metric."""
    candidates = list_model_records(
        symbol=str(symbol),
        model_name=model_name,
        active_only=False,
        limit=5000,
        allow_db_refresh=allow_db_refresh,
    )
    if not candidates:
        return None

    ranked = []
    for rec in candidates:
        performance_metrics_dict = dict(rec.get("performance_metrics") or {})
        metric_key = str(metric_name or rec.get("selection_metric_name") or "").strip() or None
        metric_value = None
        metric_direction = higher_is_better
        if metric_key:
            metric_value = _safe_float(performance_metrics_dict.get(metric_key), None)
            if metric_value is None and rec.get("selection_metric_name") == metric_key:
                metric_value = _safe_float(rec.get("selection_metric_value"), None)
            if metric_direction is None:
                metric_direction = bool(rec.get("selection_metric_higher_is_better", True))
        else:
            metric_key = str(rec.get("selection_metric_name") or "").strip() or None
            metric_value = _safe_float(rec.get("selection_metric_value"), None)
            if metric_direction is None:
                metric_direction = bool(rec.get("selection_metric_higher_is_better", True))

        if metric_value is None:
            continue
        effective_direction = True if metric_direction is None else bool(metric_direction)
        comparable_score = float(metric_value) if effective_direction else (-1.0 * float(metric_value))
        ranked.append(
            (
                float(comparable_score),
                1 if bool(rec.get("is_active")) else 0,
                int(rec.get("updated_ts_ms") or 0),
                dict(rec),
            )
        )
    if not ranked:
        return None
    ranked.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    return dict(ranked[0][3])


def get_snapshot() -> Dict[str, Any]:
    """Return cache health, freshness, and row-count metadata."""
    _reset_cache_if_db_changed()
    with _LOCK:
        rows = len(_MODEL_ROWS)
        loaded = bool(_MODEL_ROWS_LOADED)
        refreshed = int(_LAST_REFRESH_TS_MS or 0)
        last_error = str(_LAST_ERROR or "")
    age_s = round((time.time() * 1000 - refreshed) / 1000.0, 1) if refreshed > 0 else None
    return {
        "ok": bool(loaded and not last_error),
        "loaded": bool(loaded),
        "rows": int(rows),
        "last_refresh_ts_ms": (int(refreshed) if refreshed > 0 else None),
        "age_s": age_s,
        "last_error": str(last_error),
        "ts_ms": int(time.time() * 1000),
    }


__all__ = [
    "get_best_model_record",
    "get_snapshot",
    "invalidate_model_catalog",
    "list_model_records",
    "load_model_record",
    "upsert_model_record",
    "warm_model_catalog",
]
