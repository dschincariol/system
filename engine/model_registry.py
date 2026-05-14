"""Canonical SQLite-backed registry for trained, challenger, and champion models.

This module is the durable contract between training and serving. Training jobs
append model records with metrics and feature-schema metadata, while governance
and prediction code read the latest stage assignments to decide which model is
shadow-only, which is challenger-only, and which is currently live.
"""

import json
import os
import sys
import time
import logging
import threading
from typing import Optional, Dict, Any, List, Tuple, Union

from engine.prediction_logger import flush_prediction_tracking, submit_model_registry_record
from engine.runtime.artifact_store import get_artifact_manifest, normalize_artifact_registration
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect as _connect
from engine.runtime.storage import connect_ro as _connect_ro
from engine.runtime.storage import init_db as _init_db
from engine.runtime.storage import run_write_txn

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [model_registry] %(message)s",
)
LOG = get_logger("engine.model_registry")
_WARNED_NONFATAL_KEYS: set[str] = set()
_TRACKING_MODEL_CACHE_LOCK = threading.RLock()
_TRACKING_MODEL_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
_MODEL_REGISTRY_READY_LOCK = threading.RLock()
_MODEL_REGISTRY_READY_PATH = ""


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.model_registry",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _json_load_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
        obj = json.loads(raw)
    except Exception:
        return {}
    return dict(obj) if isinstance(obj, dict) else {}


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").upper().strip()


def _infer_model_family_name(model_name: str) -> str:
    name = str(model_name or "").strip().lower()
    if not name:
        return ""
    if name == "lgbm_regressor" or name.startswith("lgbm_regressor"):
        return "lgbm_regressor"
    if name == "xgb_regressor" or name.startswith("xgb_regressor"):
        return "xgb_regressor"
    if name == "patchtst" or name.startswith("patchtst"):
        return "patchtst"
    if name == "gbm_regressor" or name.startswith("gbm_regressor"):
        return "gbm_regressor"
    if name == "temporal_predictor" or name.startswith("temporal_predictor"):
        return "temporal_predictor"
    if name.startswith("regime_stats_") or name == "regime_stats":
        return "regime_stats"
    if name == "embed_regressor" or name.startswith("embed_regressor"):
        return "embed_regressor"
    return str(model_name or "").strip()


def _default_artifact_alias(model_name: str, symbol_or_scope: str) -> str:
    family = _infer_model_family_name(str(model_name or "").strip()) or str(model_name or "").strip() or "model"
    scope = str(symbol_or_scope or "global").upper().strip() or "GLOBAL"
    return f"model:{family}:{scope}:current"


def _safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _normalize_training_data_window(training_data_window: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if training_data_window is None:
        return {}
    if not isinstance(training_data_window, dict):
        raise TypeError("training_data_window must be a dict when provided")
    return dict(training_data_window)


def _extract_training_window_bounds(training_data_window: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    start_ts_ms = None
    end_ts_ms = None
    for key in ("start_ts_ms", "train_start_ts_ms", "window_start_ts_ms", "from_ts_ms", "start"):
        start_ts_ms = _safe_int(training_data_window.get(key))
        if start_ts_ms is not None:
            break
    for key in ("end_ts_ms", "train_end_ts_ms", "window_end_ts_ms", "to_ts_ms", "end"):
        end_ts_ms = _safe_int(training_data_window.get(key))
        if end_ts_ms is not None:
            break
    return start_ts_ms, end_ts_ms


def _can_reuse_existing_model_registry() -> bool:
    con = None
    try:
        con = _connect_ro()
        rows = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('models','model_registry')"
        ).fetchall() or []
        present = {str(row[0] or "").strip().lower() for row in rows if row and row[0]}
        return "models" in present and "model_registry" in present
    except Exception as e:
        _warn_nonfatal(
            "model_registry_existing_schema_read_failed",
            "MODEL_REGISTRY_EXISTING_SCHEMA_READ_FAILED",
            e,
            warn_key="model_registry_existing_schema_read_failed",
        )
        return False
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _model_registry_ready_key() -> str:
    try:
        from engine.runtime.db_guard import resolve_db_path

        return str(resolve_db_path())
    except Exception as e:
        _warn_nonfatal(
            "model_registry_ready_key_resolve_failed",
            "MODEL_REGISTRY_READY_KEY_RESOLVE_FAILED",
            e,
            warn_key="model_registry_ready_key_resolve_failed",
        )
        return str(os.environ.get("DB_PATH", "") or "")


def _model_registry_schema_ready() -> bool:
    global _MODEL_REGISTRY_READY_PATH

    ready_key = _model_registry_ready_key()
    if str(_MODEL_REGISTRY_READY_PATH or "") == str(ready_key or ""):
        return True

    con = None
    try:
        con = _connect_ro()
        models_cols = {
            str(row[1] or "").strip().lower()
            for row in (con.execute("PRAGMA table_info(models)").fetchall() or [])
            if row and len(row) >= 2 and str(row[1] or "").strip()
        }
        registry_cols = {
            str(row[1] or "").strip().lower()
            for row in (con.execute("PRAGMA table_info(model_registry)").fetchall() or [])
            if row and len(row) >= 2 and str(row[1] or "").strip()
        }
        model_indexes = {
            str(row[1] or "").strip().lower()
            for row in (con.execute("PRAGMA index_list(models)").fetchall() or [])
            if row and len(row) >= 2 and str(row[1] or "").strip()
        }
    except Exception as e:
        _warn_nonfatal(
            "model_registry_schema_ready_read_failed",
            "MODEL_REGISTRY_SCHEMA_READY_READ_FAILED",
            e,
            warn_key="model_registry_schema_ready_read_failed",
        )
        return False
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass

    required_models_cols = {
        "symbol",
        "model_name",
        "version",
        "model_kind",
        "status",
        "is_active",
        "artifact_uri",
        "training_start_ts_ms",
        "training_end_ts_ms",
        "training_data_window_json",
        "performance_metrics_json",
        "metadata_json",
        "selection_metric_name",
        "selection_metric_value",
        "selection_metric_higher_is_better",
        "created_ts_ms",
        "updated_ts_ms",
    }
    required_registry_cols = {
        "model_name",
        "model_kind",
        "model_ts_ms",
        "stage",
        "regime",
        "metrics_json",
        "created_ts_ms",
        "note",
        "status",
        "last_promotion_ts_ms",
        "performance_metrics_json",
        "updated_ts_ms",
    }
    required_model_indexes = {
        "idx_models_symbol_updated",
        "idx_models_symbol_model_updated",
        "idx_models_symbol_active_updated",
        "idx_models_symbol_selection_metric",
    }
    ready = (
        required_models_cols.issubset(models_cols)
        and required_registry_cols.issubset(registry_cols)
        and required_model_indexes.issubset(model_indexes)
    )
    if ready:
        with _MODEL_REGISTRY_READY_LOCK:
            _MODEL_REGISTRY_READY_PATH = str(ready_key or "")
    return bool(ready)


_DEFAULT_METRIC_DIRECTIONS: Dict[str, bool] = {
    "score": True,
    "quality_score": True,
    "validation_score": True,
    "sharpe": True,
    "sortino": True,
    "win_rate": True,
    "directional_acc": True,
    "directional_accuracy": True,
    "accuracy": True,
    "f1": True,
    "auc": True,
    "r2": True,
    "net_pnl": True,
    "pnl": True,
    "return_pct": True,
    "return": True,
    "rmse": False,
    "mae": False,
    "mape": False,
    "mse": False,
    "loss": False,
    "drawdown": False,
    "max_drawdown": False,
}


def _default_metric_higher_is_better(metric_name: Optional[str]) -> bool:
    key = str(metric_name or "").strip().lower()
    return bool(_DEFAULT_METRIC_DIRECTIONS.get(key, True))


def _infer_selection_metric(
    performance_metrics: Dict[str, Any],
    *,
    selection_metric_name: Optional[str] = None,
    selection_metric_value: Optional[Union[int, float]] = None,
    selection_metric_higher_is_better: Optional[bool] = None,
) -> Tuple[Optional[str], Optional[float], bool]:
    preferred_metrics: Tuple[Tuple[str, bool], ...] = tuple(_DEFAULT_METRIC_DIRECTIONS.items())

    if selection_metric_name:
        explicit_name = str(selection_metric_name).strip()
        explicit_value = _safe_float(selection_metric_value)
        if explicit_value is None:
            explicit_value = _safe_float(performance_metrics.get(explicit_name))
        if explicit_value is not None:
            return explicit_name, float(explicit_value), bool(
                _default_metric_higher_is_better(explicit_name)
                if selection_metric_higher_is_better is None
                else selection_metric_higher_is_better
            )

    primary_metric = performance_metrics.get("primary_metric")
    if isinstance(primary_metric, dict):
        explicit_name = str(primary_metric.get("name") or "").strip()
        explicit_value = _safe_float(primary_metric.get("value"))
        explicit_higher = primary_metric.get("higher_is_better")
        if explicit_name and explicit_value is not None:
            return explicit_name, float(explicit_value), bool(
                _default_metric_higher_is_better(explicit_name) if explicit_higher is None else explicit_higher
            )

    metric_name = str(performance_metrics.get("primary_metric_name") or "").strip()
    metric_value = _safe_float(performance_metrics.get("primary_metric_value"))
    if metric_name and metric_value is not None:
        explicit_higher = performance_metrics.get("primary_metric_higher_is_better")
        return metric_name, float(metric_value), bool(
            _default_metric_higher_is_better(metric_name) if explicit_higher is None else explicit_higher
        )

    for candidate_name, higher_is_better in preferred_metrics:
        candidate_value = _safe_float(performance_metrics.get(candidate_name))
        if candidate_value is not None:
            return candidate_name, float(candidate_value), bool(higher_is_better)

    return None, None, bool(True if selection_metric_higher_is_better is None else selection_metric_higher_is_better)


def _parse_models_row(row: Any) -> Dict[str, Any]:
    metadata = _json_load_dict(row[12])
    record = {
        "id": int(row[0]),
        "symbol": _normalize_symbol(row[1]),
        "model_name": str(row[2] or ""),
        "version": str(row[3] or ""),
        "model_kind": str(row[4] or ""),
        "status": str(row[5] or "registered"),
        "is_active": bool(int(row[6] or 0)),
        "artifact_uri": (str(row[7]) if row[7] is not None and str(row[7]).strip() else None),
        "training_start_ts_ms": _safe_int(row[8]),
        "training_end_ts_ms": _safe_int(row[9]),
        "training_data_window": _json_load_dict(row[10]),
        "performance_metrics": _json_load_dict(row[11]),
        "metadata": metadata,
        "selection_metric_name": (str(row[13]) if row[13] is not None and str(row[13]).strip() else None),
        "selection_metric_value": _safe_float(row[14]),
        "selection_metric_higher_is_better": bool(int(row[15] if row[15] is not None else 1)),
        "created_ts_ms": int(row[16] or 0),
        "updated_ts_ms": int(row[17] or 0),
    }
    try:
        record["artifact_manifest"] = get_artifact_manifest(record)
    except Exception:
        record["artifact_manifest"] = None
    return record


def _normalize_tracking_model_name(name: Any) -> str:
    text = str(name or "").strip()
    if not text:
        raise ValueError("model_name is required")
    return text


def _normalize_tracking_model_version(version: Any) -> str:
    text = str(version or "").strip()
    if not text:
        raise ValueError("model_version is required")
    return text


def _format_tracking_created_at(created_at_ms: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(int(created_at_ms) / 1000.0)) + "Z"


def _tracking_cache_upsert(
    *,
    model_name: str,
    version: str,
    metadata: Dict[str, Any],
    created_at_ms: int,
) -> Dict[str, Any]:
    key = (str(model_name), str(version))
    normalized_created_at_ms = int(max(0, int(created_at_ms)))
    now_ms = _now_ms()
    with _TRACKING_MODEL_CACHE_LOCK:
        existing = dict(_TRACKING_MODEL_CACHE.get(key) or {})
        if int(existing.get("created_at_ms") or 0) > 0:
            normalized_created_at_ms = min(normalized_created_at_ms, int(existing.get("created_at_ms") or 0))
        record = {
            "model_name": str(model_name),
            "version": str(version),
            "created_at": _format_tracking_created_at(normalized_created_at_ms),
            "created_at_ms": int(normalized_created_at_ms),
            "updated_ts_ms": int(now_ms),
            "metadata": dict(metadata or {}),
        }
        _TRACKING_MODEL_CACHE[key] = dict(record)
        return dict(record)


def _tracking_cache_snapshot() -> List[Dict[str, Any]]:
    with _TRACKING_MODEL_CACHE_LOCK:
        return [dict(record) for record in _TRACKING_MODEL_CACHE.values()]


def _load_tracked_registry_rows() -> List[Dict[str, Any]]:
    _init_db()
    con = _connect()
    try:
        try:
            rows = con.execute(
                """
                SELECT model_name, version, created_ts_ms, updated_ts_ms, metadata_json
                FROM tracked_model_registry
                ORDER BY updated_ts_ms DESC, created_ts_ms DESC, model_name ASC, version ASC
                """
            ).fetchall()
        except Exception:
            return []
        out: List[Dict[str, Any]] = []
        for row in rows or []:
            created_ts_ms = int(row[2] or 0)
            out.append(
                {
                    "model_name": str(row[0] or ""),
                    "version": str(row[1] or ""),
                    "created_at": _format_tracking_created_at(created_ts_ms),
                    "created_at_ms": int(created_ts_ms),
                    "updated_ts_ms": int(row[3] or created_ts_ms),
                    "metadata": _json_load_dict(row[4]),
                }
            )
        return out
    finally:
        con.close()


def _merge_tracked_registry_rows(*sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for source in sources:
        for row in source or []:
            key = (
                str(row.get("model_name") or "").strip(),
                str(row.get("version") or "").strip(),
            )
            if not key[0] or not key[1]:
                continue
            created_at_ms = int(row.get("created_at_ms") or _now_ms())
            existing = merged.get(key)
            if existing is None or int(existing.get("updated_ts_ms") or existing.get("created_at_ms") or 0) <= int(
                row.get("updated_ts_ms") or created_at_ms
            ):
                merged[key] = {
                    "model_name": str(key[0]),
                    "version": str(key[1]),
                    "created_at": str(row.get("created_at") or _format_tracking_created_at(created_at_ms)),
                    "created_at_ms": int(created_at_ms),
                    "updated_ts_ms": int(row.get("updated_ts_ms") or created_at_ms),
                    "metadata": dict(row.get("metadata") or {}),
                }
    return list(merged.values())


class ModelRegistry:
    def register_model(self, name: Any, version: Any, metadata: Any) -> Dict[str, Any]:
        model_name = _normalize_tracking_model_name(name)
        model_version = _normalize_tracking_model_version(version)
        metadata_dict = dict(metadata or {}) if isinstance(metadata, dict) or metadata is None else dict(metadata)
        record = _tracking_cache_upsert(
            model_name=model_name,
            version=model_version,
            metadata=metadata_dict,
            created_at_ms=_now_ms(),
        )
        try:
            submit_model_registry_record(
                model_name=str(record["model_name"]),
                version=str(record["version"]),
                metadata=dict(record.get("metadata") or {}),
                created_at=int(record.get("created_at_ms") or _now_ms()),
            )
        except Exception as e:
            _warn_nonfatal(
                "MODEL_REGISTRY_TRACKING_ENQUEUE_FAILED",
                "MODEL_REGISTRY_TRACKING_ENQUEUE_FAILED",
                e,
                warn_key=None,
                model_name=str(model_name),
                version=str(model_version),
            )
        return dict(record)

    def get_model(self, name: Any, version: Any = None) -> Optional[Dict[str, Any]]:
        model_name = _normalize_tracking_model_name(name)
        requested_version = str(version or "").strip()
        rows = _merge_tracked_registry_rows(_load_tracked_registry_rows(), _tracking_cache_snapshot())
        matches = [
            dict(record)
            for record in rows
            if str(record.get("model_name") or "") == str(model_name)
            and (not requested_version or str(record.get("version") or "") == str(requested_version))
        ]
        if not matches:
            return None
        matches.sort(
            key=lambda record: (
                int(record.get("updated_ts_ms") or record.get("created_at_ms") or 0),
                int(record.get("created_at_ms") or 0),
                str(record.get("version") or ""),
            ),
            reverse=True,
        )
        return dict(matches[0])

    def list_models(self) -> List[Dict[str, Any]]:
        rows = _merge_tracked_registry_rows(_load_tracked_registry_rows(), _tracking_cache_snapshot())
        rows.sort(
            key=lambda record: (
                str(record.get("model_name") or ""),
                -int(record.get("updated_ts_ms") or record.get("created_at_ms") or 0),
                str(record.get("version") or ""),
            ),
        )
        return rows

    def flush(self, timeout_s: float | None = None) -> bool:
        return bool(flush_prediction_tracking(timeout_s=timeout_s))


DEFAULT_MODEL_REGISTRY = ModelRegistry()


_MODEL_FAMILY_REGISTRY: Dict[str, Dict[str, Any]] = {
    "regime_stats_v2": {
        "family": "regime_stats",
        "training_entrypoint": "engine.strategy.jobs.train_model_v2",
        "inference_entrypoint": "engine.strategy.predictor._predict_via_regime_stats_adapter",
        "default_stage": "shadow",
        "promotion_guard": "engine.strategy.promotion_guard.assess_challenger",
    },
    "embed_regressor": {
        "family": "embed_regressor",
        "training_entrypoint": "engine.strategy.jobs.train_embed_models",
        "inference_entrypoint": "engine.strategy.embed_regressor.predict_with_embed_model",
        "default_stage": "shadow",
        "promotion_guard": "engine.strategy.promotion_guard.assess_challenger",
    },
    "temporal_predictor": {
        "family": "temporal_predictor",
        "training_entrypoint": "engine.strategy.jobs.train_temporal_predictor",
        "inference_entrypoint": "engine.strategy.temporal_predictor.predict_temporal_live",
        "default_stage": "shadow",
        "promotion_guard": "engine.strategy.promotion_guard.assess_challenger",
    },
    "gbm_regressor": {
        "family": "gbm_regressor",
        "training_entrypoint": "engine.strategy.jobs.train_gbm_regressor",
        "inference_entrypoint": "engine.strategy.gbm_regressor.predict_with_gbm_model",
        "default_stage": "shadow",
        "promotion_guard": "engine.strategy.promotion_guard.assess_challenger",
    },
}


def register_model_family(
    family_name: str,
    *,
    training_entrypoint: str,
    inference_entrypoint: str,
    default_stage: str = "shadow",
    promotion_guard: str = "engine.strategy.promotion_guard.assess_challenger",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Register a first-class model family for routing and governance metadata."""
    family = str(family_name or "").strip()
    if not family:
        raise ValueError("family_name_required")
    record = {
        "family": str(family),
        "training_entrypoint": str(training_entrypoint or "").strip(),
        "inference_entrypoint": str(inference_entrypoint or "").strip(),
        "default_stage": str(default_stage or "shadow").strip() or "shadow",
        "promotion_guard": str(promotion_guard or "").strip(),
        "metadata": dict(metadata or {}),
    }
    _MODEL_FAMILY_REGISTRY[str(family)] = dict(record)
    return dict(record)


def get_registered_model_family(family_name: str) -> Dict[str, Any]:
    family = _infer_model_family_name(str(family_name or "").strip()) or str(family_name or "").strip()
    return dict(_MODEL_FAMILY_REGISTRY.get(str(family), {}))


def registered_model_families() -> List[str]:
    return sorted(str(name) for name in _MODEL_FAMILY_REGISTRY.keys())


for _family_name, _training_entrypoint, _inference_entrypoint in (
    (
        "lgbm_regressor",
        "engine.strategy.jobs.train_lgbm_models",
        "engine.strategy.models.lgbm_regressor.LGBMRegressorModel",
    ),
    (
        "xgb_regressor",
        "engine.strategy.jobs.train_xgb_models",
        "engine.strategy.models.xgb_regressor.XGBRegressorModel",
    ),
    (
        "patchtst",
        "engine.strategy.jobs.train_patchtst_models",
        "engine.strategy.models.patchtst.PatchTSTRegressor",
    ),
):
    register_model_family(
        _family_name,
        training_entrypoint=_training_entrypoint,
        inference_entrypoint=_inference_entrypoint,
        default_stage="shadow",
        promotion_guard="engine.strategy.promotion_guard.assess_challenger",
    )


def _ensure_models_table_schema(con) -> None:
    now_ms = _now_ms()
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS models (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL,
          model_name TEXT NOT NULL,
          version TEXT NOT NULL,
          model_kind TEXT NOT NULL DEFAULT '',
          status TEXT NOT NULL DEFAULT 'registered',
          is_active INTEGER NOT NULL DEFAULT 0,
          artifact_uri TEXT,
          training_start_ts_ms INTEGER,
          training_end_ts_ms INTEGER,
          training_data_window_json TEXT NOT NULL DEFAULT '{}',
          performance_metrics_json TEXT NOT NULL DEFAULT '{}',
          metadata_json TEXT NOT NULL DEFAULT '{}',
          selection_metric_name TEXT,
          selection_metric_value REAL,
          selection_metric_higher_is_better INTEGER NOT NULL DEFAULT 1,
          created_ts_ms INTEGER NOT NULL,
          updated_ts_ms INTEGER NOT NULL,
          UNIQUE(symbol, model_name, version)
        )
        """
    )

    cols = {
        str(r[1] or "").strip().lower()
        for r in (con.execute("PRAGMA table_info(models)").fetchall() or [])
    }
    if "model_kind" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN model_kind TEXT NOT NULL DEFAULT ''")
    if "status" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN status TEXT NOT NULL DEFAULT 'registered'")
    if "is_active" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN is_active INTEGER NOT NULL DEFAULT 0")
    if "artifact_uri" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN artifact_uri TEXT")
    if "training_start_ts_ms" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN training_start_ts_ms INTEGER")
    if "training_end_ts_ms" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN training_end_ts_ms INTEGER")
    if "training_data_window_json" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN training_data_window_json TEXT NOT NULL DEFAULT '{}'")
    if "performance_metrics_json" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN performance_metrics_json TEXT NOT NULL DEFAULT '{}'")
    if "metadata_json" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}'")
    if "selection_metric_name" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN selection_metric_name TEXT")
    if "selection_metric_value" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN selection_metric_value REAL")
    if "selection_metric_higher_is_better" not in cols:
        con.execute("ALTER TABLE models ADD COLUMN selection_metric_higher_is_better INTEGER NOT NULL DEFAULT 1")
    if "created_ts_ms" not in cols:
        con.execute(f"ALTER TABLE models ADD COLUMN created_ts_ms INTEGER NOT NULL DEFAULT {int(now_ms)}")
    if "updated_ts_ms" not in cols:
        con.execute(f"ALTER TABLE models ADD COLUMN updated_ts_ms INTEGER NOT NULL DEFAULT {int(now_ms)}")

    con.execute("UPDATE models SET status=COALESCE(NULLIF(TRIM(status), ''), 'registered')")
    con.execute("UPDATE models SET is_active=COALESCE(is_active, 0)")
    con.execute("UPDATE models SET training_data_window_json=COALESCE(training_data_window_json, '{}')")
    con.execute("UPDATE models SET performance_metrics_json=COALESCE(performance_metrics_json, '{}')")
    con.execute("UPDATE models SET metadata_json=COALESCE(metadata_json, '{}')")
    con.execute(
        """
        UPDATE models
        SET selection_metric_higher_is_better=COALESCE(selection_metric_higher_is_better, 1)
        """
    )
    con.execute(
        """
        UPDATE models
        SET updated_ts_ms=COALESCE(updated_ts_ms, created_ts_ms, ?),
            created_ts_ms=COALESCE(created_ts_ms, ?, updated_ts_ms)
        """,
        (int(now_ms), int(now_ms)),
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_models_symbol_updated
          ON models(symbol, updated_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_models_symbol_model_updated
          ON models(symbol, model_name, updated_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_models_symbol_active_updated
          ON models(symbol, is_active, updated_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_models_symbol_selection_metric
          ON models(symbol, selection_metric_name, selection_metric_value DESC)
        """
    )


def _normalized_model_spec(
    model_name: str,
    regime: str,
    rec: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    rec = dict(rec or {})
    metrics = rec.get("metrics") if isinstance(rec.get("metrics"), dict) else {}
    feature_ids = []
    feature_set_tag = ""
    feature_schema: Dict[str, Any] = {}
    model_version = ""
    model_id = ""
    model_family = ""
    instance_name = ""
    horizon_s = 0
    horizons_s: List[int] = []
    symbol_universe: List[str] = []
    risk_profile = ""
    training_window_days = 0

    if isinstance(metrics, dict):
        raw_ids = metrics.get("feature_ids")
        if isinstance(raw_ids, list):
            feature_ids = [str(x) for x in raw_ids if str(x or "").strip()]
        raw_tag = metrics.get("feature_set_tag")
        if raw_tag is not None:
            feature_set_tag = str(raw_tag or "").strip()
        raw_schema = metrics.get("feature_schema")
        if isinstance(raw_schema, dict):
            feature_schema = dict(raw_schema)
        if metrics.get("model_version") is not None:
            model_version = str(metrics.get("model_version") or "").strip()
        if metrics.get("model_id") is not None:
            model_id = str(metrics.get("model_id") or "").strip()
        if metrics.get("model_family") is not None:
            model_family = str(metrics.get("model_family") or "").strip()
        if metrics.get("instance_name") is not None:
            instance_name = str(metrics.get("instance_name") or "").strip()
        try:
            horizon_s = int(metrics.get("horizon_s") or 0)
        except Exception:
            horizon_s = 0
        raw_horizons = metrics.get("horizons_s")
        if isinstance(raw_horizons, list):
            horizons_s = []
            for value in raw_horizons:
                try:
                    hs = int(value)
                except Exception as e:
                    sys.stderr.write(
                        f"[model_registry] horizon_parse_failed model={model_name!r} value={value!r}: "
                        f"{type(e).__name__}: {e}\n"
                    )
                    sys.stderr.flush()
                    continue
                if hs > 0 and hs not in horizons_s:
                    horizons_s.append(hs)
        raw_universe = metrics.get("symbol_universe")
        if isinstance(raw_universe, list):
            symbol_universe = [str(x).upper().strip() for x in raw_universe if str(x or "").strip()]
        if metrics.get("risk_profile") is not None:
            risk_profile = str(metrics.get("risk_profile") or "").strip()
        try:
            training_window_days = int(metrics.get("training_window_days") or 0)
        except Exception:
            training_window_days = 0

    if feature_schema:
        schema_ids = feature_schema.get("feature_ids")
        if isinstance(schema_ids, list) and schema_ids:
            feature_ids = [str(x) for x in schema_ids if str(x or "").strip()]
        schema_tag = feature_schema.get("feature_set_tag")
        if schema_tag is not None:
            feature_set_tag = str(schema_tag or "").strip()

    spec: Dict[str, Any] = {
        "model_name": str(model_name or rec.get("model_name") or ""),
        "model_id": str(model_id or model_name or rec.get("model_name") or ""),
        "model_family": str(model_family or _infer_model_family_name(model_name or rec.get("model_name") or "")),
        "instance_name": str(instance_name or model_name or rec.get("model_name") or ""),
        "model_kind": str(rec.get("model_kind") or ""),
        "model_ts_ms": int(rec.get("model_ts_ms") or 0),
        "regime": str(regime or rec.get("regime") or "global"),
        "source_stage": str(rec.get("stage") or ""),
    }
    artifact_alias = str(metrics.get("artifact_alias") or metrics.get("artifact_uri") or "").strip() if isinstance(metrics, dict) else ""
    artifact_sha256 = str(metrics.get("artifact_sha256") or "").strip() if isinstance(metrics, dict) else ""
    if artifact_alias:
        spec["artifact_alias"] = str(artifact_alias)
    if artifact_sha256:
        spec["artifact_sha256"] = str(artifact_sha256)
    if model_version:
        spec["model_version"] = str(model_version)
    if int(horizon_s) > 0:
        spec["horizon_s"] = int(horizon_s)
    if horizons_s:
        spec["horizons_s"] = list(horizons_s)
    if symbol_universe:
        spec["symbol_universe"] = list(symbol_universe)
    if risk_profile:
        spec["risk_profile"] = str(risk_profile)
    if int(training_window_days) > 0:
        spec["training_window_days"] = int(training_window_days)
    if feature_ids:
        spec["feature_ids"] = list(feature_ids)
    if feature_set_tag:
        spec["feature_set_tag"] = str(feature_set_tag)
    if feature_schema or feature_ids or feature_set_tag:
        merged_schema = dict(feature_schema)
        if feature_ids and not isinstance(merged_schema.get("feature_ids"), list):
            merged_schema["feature_ids"] = list(feature_ids)
        if feature_set_tag and not str(merged_schema.get("feature_set_tag") or "").strip():
            merged_schema["feature_set_tag"] = str(feature_set_tag)
        if int(spec["model_ts_ms"]) > 0 and not int(merged_schema.get("ts_ms") or 0):
            merged_schema["ts_ms"] = int(spec["model_ts_ms"])
        spec["feature_schema"] = merged_schema
    return spec


def init_model_registry(con=None) -> None:
    """
    Ensure DB initialized + registry schema/indexes exist.
    """
    if _model_registry_schema_ready():
        return

    with _MODEL_REGISTRY_READY_LOCK:
        if _model_registry_schema_ready():
            return
        if not _can_reuse_existing_model_registry():
            _init_db()
        owns_con = con is None
        con = con or _connect()
        try:
            _ensure_models_table_schema(con)
            cols = {
                str(r[1] or "").strip().lower()
                for r in (con.execute("PRAGMA table_info(model_registry)").fetchall() or [])
            }
            if "status" not in cols:
                con.execute("ALTER TABLE model_registry ADD COLUMN status TEXT")
            if "last_promotion_ts_ms" not in cols:
                con.execute("ALTER TABLE model_registry ADD COLUMN last_promotion_ts_ms INTEGER")
            if "performance_metrics_json" not in cols:
                con.execute("ALTER TABLE model_registry ADD COLUMN performance_metrics_json TEXT")
            if "updated_ts_ms" not in cols:
                con.execute("ALTER TABLE model_registry ADD COLUMN updated_ts_ms INTEGER")
            con.execute(
                """
                UPDATE model_registry
                SET status=COALESCE(status, CASE
                  WHEN stage='champion' THEN 'champion'
                  WHEN stage='challenger' THEN 'challenger'
                  ELSE 'inactive'
                END)
                """
            )
            con.execute(
                """
                UPDATE model_registry
                SET updated_ts_ms=COALESCE(updated_ts_ms, created_ts_ms)
                """
            )
            if owns_con:
                con.commit()
                _MODEL_REGISTRY_READY_PATH = _model_registry_ready_key()
        finally:
            if owns_con:
                try:
                    con.close()
                except Exception as exc:
                    _warn_nonfatal(
                        "model_registry_init_close_failed",
                        "MODEL_REGISTRY_INIT_CLOSE_FAILED",
                        exc,
                        warn_key="model_registry_init_close_failed",
                    )


def _status_for_stage(stage: str) -> str:
    st = str(stage or "").strip().lower()
    if st == "champion":
        return "champion"
    if st == "challenger":
        return "challenger"
    return "inactive"


def _register_stage_model(
    *,
    model_name: str,
    model_kind: str,
    model_ts_ms: int,
    stage: str,
    metrics: Dict[str, Any],
    note: Optional[str] = None,
    regime: Optional[str] = None,
    key: Optional[str] = None,   # alias for regime
) -> None:
    """
    Insert a model record (append-only).

    The registry keeps training history instead of mutating one "current model"
    row in place. Governance and serving code derive the active champion or
    challenger from stage/state, which preserves auditability and rollback
    history.
    """
    reg = str(regime if regime is not None else (key if key is not None else "global"))
    init_model_registry()
    metrics = dict(metrics or {})
    metrics.setdefault("artifact_alias", _default_artifact_alias(str(model_name), str(reg)))

    def _write(con):
        now_ms = _now_ms()
        con.execute(
            """
            INSERT INTO model_registry(
              model_name, model_kind, model_ts_ms,
              stage, regime,
              metrics_json, created_ts_ms, note,
              status, last_promotion_ts_ms, performance_metrics_json, updated_ts_ms
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                str(model_name),
                str(model_kind),
                int(model_ts_ms),
                str(stage),
                str(reg),
                _json_dumps(metrics or {}),
                now_ms,
                (str(note) if note else None),
                _status_for_stage(str(stage)),
                (int(now_ms) if _status_for_stage(str(stage)) == "champion" else None),
                _json_dumps(metrics or {}),
                int(now_ms),
            ),
        )

    run_write_txn(_write)


def get_stage_latest(
    model_name: str,
    stage: str,
    *,
    regime: Optional[str] = None,
    key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Latest record for (model_name, stage, regime). Returns dict including parsed metrics.
    """
    reg = str(regime if regime is not None else (key if key is not None else "global"))
    init_model_registry()
    con = _connect_ro()
    try:
        r = con.execute(
            """
            SELECT model_kind, model_ts_ms, metrics_json, created_ts_ms, note, regime,
                   COALESCE(status, CASE
                     WHEN stage='champion' THEN 'champion'
                     WHEN stage='challenger' THEN 'challenger'
                     ELSE 'inactive'
                   END),
                   last_promotion_ts_ms,
                   performance_metrics_json,
                   COALESCE(updated_ts_ms, created_ts_ms)
            FROM model_registry
            WHERE model_name=? AND stage=? AND regime=?
            ORDER BY created_ts_ms DESC
            LIMIT 1
            """,
            (str(model_name), str(stage), str(reg)),
        ).fetchone()
        if not r:
            return None
        mk, mts, mj, cts, note, rg, status, last_promotion_ts_ms, perf_json, updated_ts_ms = r
        out = {
            "model_name": str(model_name),
            "model_kind": str(mk),
            "model_ts_ms": int(mts or 0),
            "metrics": _json_load_dict(mj),
            "created_ts_ms": int(cts or 0),
            "note": note,
            "stage": str(stage),
            "regime": str(rg or "global"),
            "status": str(status or _status_for_stage(str(stage))),
            "last_promotion_ts_ms": int(last_promotion_ts_ms or 0),
            "performance_metrics": _json_load_dict(perf_json),
            "updated_ts_ms": int(updated_ts_ms or cts or 0),
        }
        # convenience: flatten common metrics for legacy callers (guards.py reads rmse)
        try:
            if isinstance(out["metrics"], dict):
                for k2, v2 in out["metrics"].items():
                    if k2 not in out:
                        out[k2] = v2
        except Exception as exc:
            _warn_nonfatal(
                "model_registry_metrics_flatten_failed",
                "MODEL_REGISTRY_METRICS_FLATTEN_FAILED",
                exc,
                warn_key="model_registry_metrics_flatten_failed",
                model_name=str(model_name),
                stage=str(stage),
            )
        return out
    finally:
        con.close()


def list_recent(
    model_name: str,
    limit: int = 50,
    *,
    regime: Optional[str] = None,
    key: Optional[str] = None,
) -> List[Dict[str, Any]]:
    limit = max(1, min(500, int(limit or 50)))
    reg = regime if regime is not None else key
    init_model_registry()
    con = _connect()
    try:
        if reg is None:
            rows = con.execute(
                """
                SELECT model_kind, model_ts_ms, stage, regime, metrics_json, created_ts_ms, note,
                       COALESCE(status, CASE
                         WHEN stage='champion' THEN 'champion'
                         WHEN stage='challenger' THEN 'challenger'
                         ELSE 'inactive'
                       END),
                       last_promotion_ts_ms,
                       performance_metrics_json,
                       COALESCE(updated_ts_ms, created_ts_ms)
                FROM model_registry
                WHERE model_name=?
                ORDER BY created_ts_ms DESC
                LIMIT ?
                """,
                (str(model_name), int(limit)),
            ).fetchall()
        else:
            rows = con.execute(
                """
                SELECT model_kind, model_ts_ms, stage, regime, metrics_json, created_ts_ms, note,
                       COALESCE(status, CASE
                         WHEN stage='champion' THEN 'champion'
                         WHEN stage='challenger' THEN 'challenger'
                         ELSE 'inactive'
                       END),
                       last_promotion_ts_ms,
                       performance_metrics_json,
                       COALESCE(updated_ts_ms, created_ts_ms)
                FROM model_registry
                WHERE model_name=? AND regime=?
                ORDER BY created_ts_ms DESC
                LIMIT ?
                """,
                (str(model_name), str(reg), int(limit)),
            ).fetchall()

        out: List[Dict[str, Any]] = []
        for mk, mts, st, rg, mj, cts, note, status, last_promotion_ts_ms, perf_json, updated_ts_ms in rows or []:
            rec = {
                "model_name": str(model_name),
                "model_kind": str(mk),
                "model_ts_ms": int(mts or 0),
                "stage": str(st),
                "regime": str(rg or "global"),
                "metrics": _json_load_dict(mj),
                "created_ts_ms": int(cts or 0),
                "note": note,
                "status": str(status or _status_for_stage(str(st))),
                "last_promotion_ts_ms": int(last_promotion_ts_ms or 0),
                "performance_metrics": _json_load_dict(perf_json),
                "updated_ts_ms": int(updated_ts_ms or cts or 0),
            }
            out.append(rec)
        return out
    finally:
        con.close()


def _require_latest_statistical_evidence_pass(con, *, model_id: str) -> None:
    from engine.strategy.promotion_audit import latest_statistical_evidence_decision

    model_key = str(model_id or "").strip()
    decision = latest_statistical_evidence_decision(model_id=str(model_key), con=con)
    if not bool(decision.get("passed")):
        raise RuntimeError(
            f"cannot promote model={model_key}: latest statistical evidence decision={decision.get('decision') or 'missing'}"
        )


def promote_to_champion(
    model_name: str,
    a: Union[str, None],
    b: Optional[int] = None,
    *,
    regime: Optional[str] = None,
    key: Optional[str] = None,
) -> Union[None, Tuple[Optional[str], Optional[int]]]:
    """
    Supported call patterns:

    1) promote_to_champion(model_name, to_kind, to_ts_ms, regime='global')
       -> Returns (from_kind, from_ts_ms) of previous champion (or (None,None)).

    2) promote_to_champion(model_name, regime_string)
       -> Promotes most recent challenger for that regime_string to champion.
          Returns None.
    """
    init_model_registry()

    # Pattern 2 promotes the latest challenger in-place for a regime. This is
    # the durable champion handoff that serving code later reads.
    if b is None and isinstance(a, str) and (regime is None) and (key is None):
        reg = str(a or "global")
        promoted: dict[str, Any] = {"kind": None, "ts": None}

        def _write(con):
            now_ms = _now_ms()
            row = con.execute(
                """
                SELECT model_kind, model_ts_ms
                FROM model_registry
                WHERE model_name=? AND regime=? AND stage='challenger'
                ORDER BY created_ts_ms DESC
                LIMIT 1
                """,
                (str(model_name), str(reg)),
            ).fetchone()
            if not row:
                raise RuntimeError(f"cannot promote missing challenger model={model_name} regime={reg}")

            promoted["kind"] = str(row[0])
            promoted["ts"] = int(row[1])
            _require_latest_statistical_evidence_pass(con, model_id=str(model_name))

            con.execute(
                """
                UPDATE model_registry
                SET stage='retired', status='inactive', updated_ts_ms=?
                WHERE model_name=? AND regime=? AND stage='champion'
                """,
                (int(now_ms), str(model_name), str(reg)),
            )
            con.execute(
                """
                UPDATE model_registry
                SET stage='champion',
                    status='champion',
                    last_promotion_ts_ms=?,
                    updated_ts_ms=?
                WHERE model_name=? AND model_kind=? AND model_ts_ms=? AND regime=?
                """,
                (int(now_ms), int(now_ms), str(model_name), str(promoted["kind"]), int(promoted["ts"]), str(reg)),
            )

        run_write_txn(_write)
        logging.info(
            "PROMOTED champion model=%s regime=%s kind=%s ts=%s",
            model_name,
            reg,
            promoted["kind"],
            promoted["ts"],
        )
        return None

    # Pattern 1
    to_kind = str(a) if a is not None else ""
    if b is None:
        raise TypeError("promote_to_champion(model_name, to_kind, to_ts_ms[, regime=...]) missing to_ts_ms")
    to_ts_ms = int(b)
    reg = str(regime if regime is not None else (key if key is not None else "global"))

    prev_state: dict[str, Any] = {"kind": None, "ts": None}

    def _write(con):
        now_ms = _now_ms()
        prev = con.execute(
            """
            SELECT model_kind, model_ts_ms
            FROM model_registry
            WHERE model_name=? AND regime=? AND stage='champion'
            ORDER BY created_ts_ms DESC
            LIMIT 1
            """,
            (str(model_name), str(reg)),
        ).fetchone()

        prev_state["kind"] = prev[0] if prev else None
        prev_state["ts"] = int(prev[1]) if prev and prev[1] is not None else None

        exists = con.execute(
            """
            SELECT 1
            FROM model_registry
            WHERE model_name=? AND model_kind=? AND model_ts_ms=? AND regime=?
            LIMIT 1
            """,
            (str(model_name), str(to_kind), int(to_ts_ms), str(reg)),
        ).fetchone()
        if not exists:
            raise RuntimeError(
                f"cannot promote missing model record model={model_name} regime={reg} kind={to_kind} ts={to_ts_ms}"
            )
        _require_latest_statistical_evidence_pass(con, model_id=str(model_name))

        con.execute(
            """
            UPDATE model_registry
            SET stage='retired', status='inactive', updated_ts_ms=?
            WHERE model_name=? AND regime=? AND stage='champion'
            """,
            (int(now_ms), str(model_name), str(reg)),
        )
        con.execute(
            """
            UPDATE model_registry
            SET stage='champion',
                status='champion',
                last_promotion_ts_ms=?,
                updated_ts_ms=?
            WHERE model_name=? AND model_kind=? AND model_ts_ms=? AND regime=?
            """,
            (int(now_ms), int(now_ms), str(model_name), str(to_kind), int(to_ts_ms), str(reg)),
        )

    run_write_txn(_write)

    logging.info("PROMOTED champion model=%s regime=%s kind=%s ts=%s", model_name, reg, to_kind, to_ts_ms)
    return (prev_state["kind"], prev_state["ts"])


def update_model_runtime(
    model_name: str,
    *,
    regime: Optional[str] = None,
    model_kind: Optional[str] = None,
    model_ts_ms: Optional[int] = None,
    status: Optional[str] = None,
    performance_metrics: Optional[Dict[str, Any]] = None,
    last_promotion_ts_ms: Optional[int] = None,
) -> None:
    init_model_registry()
    reg = str(regime or "global")
    perf_json = None
    if isinstance(performance_metrics, dict):
        perf_json = json.dumps(performance_metrics, separators=(",", ":"), sort_keys=True)

    def _write(con):
        now_ms = _now_ms()
        params: List[Any] = [int(now_ms)]
        sets = ["updated_ts_ms=?"]
        if status is not None:
            sets.append("status=?")
            params.append(str(status))
        if perf_json is not None:
            sets.append("performance_metrics_json=?")
            params.append(str(perf_json))
        if last_promotion_ts_ms is not None:
            sets.append("last_promotion_ts_ms=?")
            params.append(int(last_promotion_ts_ms))
        where = ["model_name=?", "regime=?"]
        params.extend([str(model_name), str(reg)])
        if model_kind is not None:
            where.append("model_kind=?")
            params.append(str(model_kind))
        if model_ts_ms is not None:
            where.append("model_ts_ms=?")
            params.append(int(model_ts_ms))
        con.execute(
            f"""
            UPDATE model_registry
            SET {", ".join(sets)}
            WHERE {" AND ".join(where)}
            """,
            tuple(params),
        )

    run_write_txn(_write)


def register_model(
    *,
    model_name: str,
    model_kind: Optional[str] = None,
    model_ts_ms: Optional[int] = None,
    stage: Optional[str] = None,
    metrics: Optional[Dict[str, Any]] = None,
    note: Optional[str] = None,
    regime: Optional[str] = None,
    key: Optional[str] = None,
    symbol: Optional[str] = None,
    version: Optional[str] = None,
    training_data_window: Optional[Dict[str, Any]] = None,
    performance_metrics: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    artifact_uri: Optional[str] = None,
    status: Optional[str] = None,
    is_active: Optional[bool] = None,
    selection_metric_name: Optional[str] = None,
    selection_metric_value: Optional[Union[int, float]] = None,
    selection_metric_higher_is_better: Optional[bool] = None,
) -> Optional[Dict[str, Any]]:
    """
    Public registry entrypoint supporting:

    1) Legacy stage-based writes into `model_registry`.
    2) New per-symbol/versioned catalog writes into `models`.

    The catalog path stores metadata only. It does not serialize or train model
    artifacts yet.
    """
    use_legacy_stage_registry = bool(stage is not None or model_ts_ms is not None)
    if use_legacy_stage_registry:
        if not model_kind or model_ts_ms is None or not stage:
            raise TypeError(
                "legacy register_model requires model_kind, model_ts_ms, and stage"
            )
        _register_stage_model(
            model_name=str(model_name),
            model_kind=str(model_kind),
            model_ts_ms=int(model_ts_ms),
            stage=str(stage),
            metrics=dict(metrics or {}),
            note=note,
            regime=regime,
            key=key,
        )
        return None

    symbol_u = _normalize_symbol(symbol or "")
    version_s = str(version or "").strip()
    model_name_s = str(model_name or "").strip()
    if not symbol_u:
        raise ValueError("register_model requires symbol for catalog registration")
    if not model_name_s:
        raise ValueError("register_model requires model_name")
    if not version_s:
        raise ValueError("register_model requires version for catalog registration")

    init_model_registry()
    now_ms = _now_ms()
    performance_metrics_dict = dict(
        performance_metrics if isinstance(performance_metrics, dict) else (metrics if isinstance(metrics, dict) else {})
    )
    metadata_dict = dict(metadata or {})
    if artifact_uri is None:
        manifest_alias = ""
        manifest = metadata_dict.get("artifact_manifest")
        if isinstance(manifest, dict):
            manifest_alias = str(manifest.get("alias") or "").strip()
        artifact_uri = manifest_alias or _default_artifact_alias(model_name_s, symbol_u)
    artifact_uri_text, metadata_dict, artifact_manifest = normalize_artifact_registration(
        artifact_uri=artifact_uri,
        metadata=metadata_dict,
    )
    training_data_window_dict = _normalize_training_data_window(training_data_window)
    training_start_ts_ms, training_end_ts_ms = _extract_training_window_bounds(training_data_window_dict)
    inferred_metric_name, inferred_metric_value, inferred_metric_higher_is_better = _infer_selection_metric(
        performance_metrics_dict,
        selection_metric_name=selection_metric_name,
        selection_metric_value=selection_metric_value,
        selection_metric_higher_is_better=selection_metric_higher_is_better,
    )
    final_status = str(status or "registered").strip() or "registered"
    final_is_active = bool(is_active) if is_active is not None else False

    def _write(con) -> None:
        if final_is_active:
            con.execute(
                """
                UPDATE models
                SET is_active=0, updated_ts_ms=?
                WHERE symbol=? AND model_name=? AND version<>?
                """,
                (int(now_ms), str(symbol_u), str(model_name_s), str(version_s)),
            )

        existing = con.execute(
            """
            SELECT created_ts_ms
            FROM models
            WHERE symbol=? AND model_name=? AND version=?
            LIMIT 1
            """,
            (str(symbol_u), str(model_name_s), str(version_s)),
        ).fetchone()
        created_ts_ms = int(existing[0] or now_ms) if existing else int(now_ms)

        con.execute(
            """
            INSERT INTO models(
              symbol, model_name, version, model_kind, status, is_active, artifact_uri,
              training_start_ts_ms, training_end_ts_ms, training_data_window_json,
              performance_metrics_json, metadata_json, selection_metric_name,
              selection_metric_value, selection_metric_higher_is_better,
              created_ts_ms, updated_ts_ms
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, model_name, version) DO UPDATE SET
              model_kind=excluded.model_kind,
              status=excluded.status,
              is_active=excluded.is_active,
              artifact_uri=excluded.artifact_uri,
              training_start_ts_ms=excluded.training_start_ts_ms,
              training_end_ts_ms=excluded.training_end_ts_ms,
              training_data_window_json=excluded.training_data_window_json,
              performance_metrics_json=excluded.performance_metrics_json,
              metadata_json=excluded.metadata_json,
              selection_metric_name=excluded.selection_metric_name,
              selection_metric_value=excluded.selection_metric_value,
              selection_metric_higher_is_better=excluded.selection_metric_higher_is_better,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (
                str(symbol_u),
                str(model_name_s),
                str(version_s),
                str(model_kind or ""),
                str(final_status),
                1 if final_is_active else 0,
                artifact_uri_text,
                training_start_ts_ms,
                training_end_ts_ms,
                _json_dumps(training_data_window_dict),
                _json_dumps(performance_metrics_dict),
                _json_dumps(metadata_dict),
                (str(inferred_metric_name) if inferred_metric_name else None),
                inferred_metric_value,
                1 if inferred_metric_higher_is_better else 0,
                int(created_ts_ms),
                int(now_ms),
            ),
        )

    run_write_txn(_write, table="models", operation="register_model")
    try:
        DEFAULT_MODEL_REGISTRY.register_model(
            name=str(model_name_s),
            version=str(version_s),
            metadata={
                "symbol": str(symbol_u),
                "model_kind": str(model_kind or ""),
                "status": str(final_status),
                "is_active": bool(final_is_active),
                "artifact_uri": artifact_uri_text,
                "artifact_manifest": dict(artifact_manifest or {}),
                "training_start_ts_ms": training_start_ts_ms,
                "training_end_ts_ms": training_end_ts_ms,
                "selection_metric_name": inferred_metric_name,
                "selection_metric_value": inferred_metric_value,
                "selection_metric_higher_is_better": bool(inferred_metric_higher_is_better),
                "metadata": dict(metadata_dict),
                "performance_metrics": dict(performance_metrics_dict),
            },
        )
    except Exception as e:
        _warn_nonfatal(
            "model_registry_tracking_sync_failed",
            "MODEL_REGISTRY_TRACKING_SYNC_FAILED",
            e,
            warn_key=None,
            symbol=str(symbol_u),
            model_name=str(model_name_s),
            version=str(version_s),
        )
    loaded = load_model(symbol_u, model_name=model_name_s, version=version_s)
    try:
        from engine.runtime.model_cache import warm_model_catalog

        warm_model_catalog(force=True)
    except Exception as e:
        _warn_nonfatal(
            "model_registry_runtime_model_cache_refresh_failed",
            "MODEL_REGISTRY_RUNTIME_MODEL_CACHE_REFRESH_FAILED",
            e,
            warn_key=None,
            symbol=str(symbol_u),
            model_name=str(model_name_s),
            version=str(version_s),
        )
    return loaded


def load_model(
    symbol: str,
    *,
    model_name: Optional[str] = None,
    version: Optional[str] = None,
    active_only: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Load a catalog record for a trained model.

    This returns persisted metadata and references only. Artifact deserialization
    is intentionally deferred until training/artifact management exists.
    """
    symbol_u = _normalize_symbol(symbol)
    if not symbol_u:
        return None

    init_model_registry()
    con = _connect()
    try:
        where = ["symbol=?"]
        params: List[Any] = [str(symbol_u)]
        if model_name:
            where.append("model_name=?")
            params.append(str(model_name))
        if version:
            where.append("version=?")
            params.append(str(version))
        if active_only:
            where.append("is_active=1")
        row = con.execute(
            f"""
            SELECT id, symbol, model_name, version, model_kind, status, is_active, artifact_uri,
                   training_start_ts_ms, training_end_ts_ms, training_data_window_json,
                   performance_metrics_json, metadata_json, selection_metric_name,
                   selection_metric_value, selection_metric_higher_is_better,
                   created_ts_ms, updated_ts_ms
            FROM models
            WHERE {" AND ".join(where)}
            ORDER BY is_active DESC, updated_ts_ms DESC, created_ts_ms DESC
            LIMIT 1
            """,
            tuple(params),
        ).fetchone()
        return _parse_models_row(row) if row else None
    finally:
        con.close()


def list_models(
    symbol: Optional[str] = None,
    *,
    model_name: Optional[str] = None,
    status: Optional[str] = None,
    active_only: bool = False,
    limit: int = 100,
    readonly: bool = False,
) -> List[Dict[str, Any]]:
    limit = max(1, min(1000, int(limit or 100)))
    if not bool(readonly):
        init_model_registry()
        con = _connect()
    else:
        con = _connect_ro()
    try:
        where: List[str] = ["1=1"]
        params: List[Any] = []
        if symbol:
            where.append("symbol=?")
            params.append(str(_normalize_symbol(symbol)))
        if model_name:
            where.append("model_name=?")
            params.append(str(model_name))
        if status:
            where.append("status=?")
            params.append(str(status))
        if active_only:
            where.append("is_active=1")
        rows = con.execute(
            f"""
            SELECT id, symbol, model_name, version, model_kind, status, is_active, artifact_uri,
                   training_start_ts_ms, training_end_ts_ms, training_data_window_json,
                   performance_metrics_json, metadata_json, selection_metric_name,
                   selection_metric_value, selection_metric_higher_is_better,
                   created_ts_ms, updated_ts_ms
            FROM models
            WHERE {" AND ".join(where)}
            ORDER BY symbol ASC, is_active DESC, updated_ts_ms DESC, created_ts_ms DESC
            LIMIT ?
            """,
            tuple(params + [int(limit)]),
        ).fetchall()
        return [_parse_models_row(row) for row in rows or []]
    except Exception:
        if bool(readonly):
            return []
        raise
    finally:
        con.close()


def get_best_model(
    symbol: str,
    *,
    model_name: Optional[str] = None,
    metric_name: Optional[str] = None,
    higher_is_better: Optional[bool] = None,
) -> Optional[Dict[str, Any]]:
    symbol_u = _normalize_symbol(symbol)
    if not symbol_u:
        return None

    candidates = list_models(symbol_u, model_name=model_name, limit=500)
    if not candidates:
        return None

    ranked: List[Tuple[float, int, int, Dict[str, Any]]] = []
    for rec in candidates:
        performance_metrics_dict = dict(rec.get("performance_metrics") or {})
        metric_key = str(metric_name or rec.get("selection_metric_name") or "").strip() or None
        metric_value = None
        metric_direction = higher_is_better

        if metric_key:
            metric_value = _safe_float(performance_metrics_dict.get(metric_key))
            if rec.get("selection_metric_name") == metric_key and metric_direction is None:
                metric_direction = bool(rec.get("selection_metric_higher_is_better"))
            if metric_value is None and rec.get("selection_metric_name") == metric_key:
                metric_value = _safe_float(rec.get("selection_metric_value"))
            if metric_direction is None:
                metric_direction = _default_metric_higher_is_better(metric_key)
        else:
            metric_key = rec.get("selection_metric_name")
            metric_value = _safe_float(rec.get("selection_metric_value"))
            if metric_direction is None and rec.get("selection_metric_name"):
                metric_direction = bool(rec.get("selection_metric_higher_is_better"))
            if metric_value is None:
                metric_key, metric_value, inferred_direction = _infer_selection_metric(performance_metrics_dict)
                if metric_direction is None:
                    metric_direction = inferred_direction

        if metric_value is None:
            continue

        effective_direction = True if metric_direction is None else bool(metric_direction)
        comparable_score = float(metric_value) if effective_direction else (-1.0 * float(metric_value))
        rec["best_metric_name"] = str(metric_key or "")
        rec["best_metric_value"] = float(metric_value)
        rec["best_metric_higher_is_better"] = bool(effective_direction)
        ranked.append(
            (
                float(comparable_score),
                1 if bool(rec.get("is_active")) else 0,
                int(rec.get("updated_ts_ms") or 0),
                rec,
            )
        )

    if not ranked:
        return None

    ranked.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    return ranked[0][3]


def rollback_champion(
    model_name: str,
    *,
    regime: Optional[str] = None,
    key: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Rollback champion to most recent retired model for (model_name, regime).
    Returns new champion record or None.
    """
    init_model_registry()
    reg = str(regime if regime is not None else (key if key is not None else "global"))

    next_state: dict[str, Any] = {"kind": None, "ts": None}

    def _write(con):
        cur = con.execute(
            """
            SELECT model_kind, model_ts_ms
            FROM model_registry
            WHERE model_name=? AND regime=? AND stage='retired'
            ORDER BY created_ts_ms DESC
            LIMIT 1
            """,
            (str(model_name), str(reg)),
        ).fetchone()
        if not cur:
            return False

        next_state["kind"] = str(cur[0])
        next_state["ts"] = int(cur[1])

        con.execute(
            """
            UPDATE model_registry
            SET stage='retired'
            WHERE model_name=? AND regime=? AND stage='champion'
            """,
            (str(model_name), str(reg)),
        )
        con.execute(
            """
            UPDATE model_registry
            SET stage='champion'
            WHERE model_name=? AND model_kind=? AND model_ts_ms=? AND regime=?
            """,
            (str(model_name), str(next_state["kind"]), int(next_state["ts"]), str(reg)),
        )
        return True

    if not run_write_txn(_write):
        return None

    return get_stage_latest(model_name, "champion", regime=reg)


def get_active_model_name(*, regime: Optional[str] = None) -> str:
    reg = str(regime or "global")
    env_name = str(os.environ.get("MODEL_NAME", "embed_regressor") or "embed_regressor").strip() or "embed_regressor"

    try:
        row = get_stage_latest(env_name, "champion", regime=reg)
        if row:
            return str(row.get("model_name") or env_name)
    except Exception as exc:
        _warn_nonfatal(
            "model_registry_active_model_stage_lookup_failed",
            "MODEL_REGISTRY_ACTIVE_MODEL_STAGE_LOOKUP_FAILED",
            exc,
            warn_key="model_registry_active_model_stage_lookup_failed",
            regime=str(reg),
        )

    init_model_registry()
    con = _connect()
    try:
        r = con.execute(
            """
            SELECT model_name
            FROM model_registry
            WHERE stage='champion' AND regime=?
            ORDER BY created_ts_ms DESC
            LIMIT 1
            """,
            (str(reg),),
        ).fetchone()
        if r and r[0]:
            return str(r[0])
    except Exception as exc:
        _warn_nonfatal(
            "model_registry_active_model_query_failed",
            "MODEL_REGISTRY_ACTIVE_MODEL_QUERY_FAILED",
            exc,
            warn_key="model_registry_active_model_query_failed",
            regime=str(reg),
        )
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "model_registry_active_model_close_failed",
                "MODEL_REGISTRY_ACTIVE_MODEL_CLOSE_FAILED",
                exc,
                warn_key="model_registry_active_model_close_failed",
                regime=str(reg),
            )

    return env_name


def get_active_model_spec(*, regime: Optional[str] = None) -> Dict[str, Any]:
    reg = str(regime or "global")
    model_name = get_active_model_name(regime=reg)
    return get_model_spec(model_name, regime=reg)


def get_model_spec(model_name: str, *, regime: Optional[str] = None) -> Dict[str, Any]:
    reg = str(regime or "global")
    name = str(model_name or "").strip()
    if not name:
        return {}

    for stage in ("champion", "challenger", "shadow", "retired"):
        rec = get_stage_latest(name, stage, regime=reg)
        if not rec:
            continue
        spec = _normalized_model_spec(name, reg, rec)
        if spec.get("feature_ids") or spec.get("feature_schema") or spec.get("model_kind") or spec.get("model_ts_ms"):
            return spec

    return {
        "model_name": str(name),
        "model_kind": "",
        "model_ts_ms": 0,
        "regime": str(reg),
        "source_stage": "",
    }
