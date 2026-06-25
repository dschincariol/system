"""LightGBM-based structured-feature regressor family."""

from __future__ import annotations

import json
import logging
import os
import socket
import struct
import time
import uuid
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

from engine.artifacts.store import LocalArtifactStore
from engine.runtime import dbapi_compat as dbapi
from engine.data.universe_pit import resolve_training_window_universe
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    fetch_latest_model_hyperparameters,
    init_db,
    release_job_lock,
)
from engine.strategy.feature_registry import (
    build_feature_snapshot,
    feature_schema_flags,
    feature_set_tag_from_ids,
    resolve_feature_ids,
)
from engine.strategy.learning_loop import build_dataset_snapshot
from engine.strategy.model_config import (
    build_model_registration_metadata,
    get_model_config,
    load_model_configs,
)
from engine.strategy.model_lifecycle import (
    finish_lifecycle_run,
    load_lifecycle_plan,
    publish_lifecycle_status,
    record_version_performance,
    register_model_version,
    start_lifecycle_run,
    update_model_version_status,
    version_from_ts,
)
from engine.strategy.ensemble.oos_store import upsert_oos_predictions
from engine.training_guard import training_allowed

LOG = get_logger("engine.strategy.gbm_regressor")
_WARNED_NONFATAL_KEYS: set[str] = set()

_GBM_MAGIC = b"GBM1"
_GBM_BLOB_VERSION = 1
_GBM_FAMILY = "gbm_regressor"
_DEFAULT_MODEL_NAME = str(os.environ.get("GBM_MODEL_NAME", _GBM_FAMILY) or _GBM_FAMILY).strip() or _GBM_FAMILY
_DEFAULT_LOOKBACK_DAYS = int(os.environ.get("GBM_LOOKBACK_DAYS", "365"))
_DEFAULT_MIN_SAMPLES = int(os.environ.get("GBM_MIN_SAMPLES", "50"))
_DEFAULT_MIN_NEW_LABELS = int(os.environ.get("GBM_MIN_NEW_LABELS", "25"))
_DEFAULT_HORIZON_S = int(os.environ.get("GBM_HORIZON_S", os.environ.get("MODEL_HORIZON_MEDIUM_S", "3600")))
_DEFAULT_SYMBOLS = ["*"]
_DEFAULT_FEATURE_IDS: List[str] = []
_DEFAULT_HYPERPARAMS = {
    "num_leaves": int(os.environ.get("GBM_NUM_LEAVES", "31")),
    "learning_rate": float(os.environ.get("GBM_LEARNING_RATE", "0.05")),
    "n_estimators": int(os.environ.get("GBM_N_ESTIMATORS", "200")),
    "min_child_samples": int(os.environ.get("GBM_MIN_CHILD_SAMPLES", "20")),
}
_USE_TUNED_HYPERPARAMS = os.environ.get("GBM_USE_TUNED_HYPERPARAMS", "0") == "1"
_GBM_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS gbm_models (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  model_name TEXT NOT NULL,
  version TEXT NOT NULL,
  created_ts INTEGER NOT NULL,
  blob BLOB,
  artifact_sha256 TEXT,
  artifact_alias TEXT,
  feature_schema_json TEXT NOT NULL,
  training_metrics_json TEXT,
  UNIQUE(model_name, version)
);
"""
OWNER = socket.gethostname()
PID = os.getpid()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.gbm_regressor",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _import_lightgbm():
    try:
        import lightgbm as lgb
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("lightgbm_not_installed") from exc
    return lgb


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return float(default)
    if isinstance(value, str) and not value.strip():
        return float(default)
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not np.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return int(default)
    if isinstance(value, str) and not value.strip():
        return int(default)
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            obj = json.loads(value)
        except Exception:
            return {}
        return dict(obj) if isinstance(obj, dict) else {}
    return {}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _normalize_feature_ids(feature_ids: Sequence[Any] | None) -> List[str]:
    return [str(feature_id).strip() for feature_id in (feature_ids or []) if str(feature_id or "").strip()]


def _normalize_symbol_universe(values: Sequence[Any] | None) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values or []:
        item = str(raw or "").upper().strip()
        if not item:
            continue
        if item == "*":
            return ["*"]
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _runtime_symbol_filter(symbol_universe: Sequence[str]) -> set[str]:
    normalized = _normalize_symbol_universe(symbol_universe)
    if "*" in normalized:
        return set()
    return set(normalized)


def _normalized_hyperparams(hyperparams: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    raw = dict(_DEFAULT_HYPERPARAMS)
    raw.update(dict(hyperparams or {}))
    return {
        "objective": "regression",
        "num_leaves": max(2, _safe_int(raw.get("num_leaves"), _DEFAULT_HYPERPARAMS["num_leaves"])),
        "learning_rate": max(1e-4, _safe_float(raw.get("learning_rate"), _DEFAULT_HYPERPARAMS["learning_rate"])),
        "n_estimators": max(1, _safe_int(raw.get("n_estimators"), _DEFAULT_HYPERPARAMS["n_estimators"])),
        "min_child_samples": max(1, _safe_int(raw.get("min_child_samples"), _DEFAULT_HYPERPARAMS["min_child_samples"])),
        "random_state": max(0, _safe_int(raw.get("random_state"), 42)),
        "n_jobs": max(1, _safe_int(raw.get("n_jobs"), 1)),
        "verbosity": -1,
        "deterministic": True,
        "force_col_wise": True,
    }


def _feature_schema(feature_ids: Sequence[Any], *, ts_ms: int | None = None) -> Dict[str, Any]:
    ids = _normalize_feature_ids(feature_ids)
    schema = {
        "feature_ids": list(ids),
        "feature_set_tag": str(feature_set_tag_from_ids(list(ids))),
        "feature_count": int(len(ids)),
        "feature_flags": feature_schema_flags(list(ids)),
    }
    if ts_ms is not None and int(ts_ms) > 0:
        schema["ts_ms"] = int(ts_ms)
    return schema


def _encode_blob(meta: Dict[str, Any], model_text: str) -> bytes:
    meta_bytes = _json_dumps(meta).encode("utf-8")
    model_bytes = str(model_text).encode("utf-8")
    return b"".join([_GBM_MAGIC, struct.pack("<I", int(len(meta_bytes))), meta_bytes, model_bytes])


def _decode_blob(blob: bytes) -> Tuple[Dict[str, Any], str]:
    raw = bytes(blob or b"")
    if not raw.startswith(_GBM_MAGIC):
        raise ValueError("invalid_gbm_blob_magic")
    if len(raw) < len(_GBM_MAGIC) + 4:
        raise ValueError("invalid_gbm_blob_length")
    meta_len = struct.unpack("<I", raw[len(_GBM_MAGIC) : len(_GBM_MAGIC) + 4])[0]
    meta_start = len(_GBM_MAGIC) + 4
    meta_end = meta_start + int(meta_len)
    if len(raw) < meta_end:
        raise ValueError("truncated_gbm_blob_meta")
    meta = _safe_json_dict(raw[meta_start:meta_end].decode("utf-8", errors="strict"))
    if int(meta.get("blob_version") or 0) != int(_GBM_BLOB_VERSION):
        raise ValueError("unsupported_gbm_blob_version")
    model_text = raw[meta_end:].decode("utf-8", errors="strict")
    if not model_text.strip():
        raise ValueError("empty_gbm_model_text")
    return meta, model_text


def _ensure_gbm_models_table(con) -> None:
    con.executescript(_GBM_SCHEMA_SQL)
    cols = {
        str(row[1] or "").strip().lower()
        for row in (con.execute("PRAGMA table_info(gbm_models)").fetchall() or [])
        if row and len(row) >= 2
    }
    if "artifact_sha256" not in cols:
        con.execute("ALTER TABLE gbm_models ADD COLUMN artifact_sha256 TEXT")
    if "artifact_alias" not in cols:
        con.execute("ALTER TABLE gbm_models ADD COLUMN artifact_alias TEXT")


def init_gbm_models_db() -> None:
    """Ensure the GBM model registry table exists in the backing store."""
    init_db()
    con = connect()
    try:
        _ensure_gbm_models_table(con)
        con.commit()
    finally:
        con.close()


def train_gbm_model(
    X,
    y,
    feature_ids,
    hyperparams,
) -> bytes:
    """Train a LightGBM regressor and return a versioned opaque blob."""
    lgb = _import_lightgbm()

    ids = _normalize_feature_ids(feature_ids)
    X_arr = np.asarray(X, dtype=np.float32)
    y_arr = np.asarray(y, dtype=np.float32).reshape(-1)

    if X_arr.ndim != 2:
        raise ValueError("gbm_train_requires_2d_features")
    if y_arr.ndim != 1:
        raise ValueError("gbm_train_requires_1d_targets")
    if int(X_arr.shape[0]) != int(y_arr.shape[0]):
        raise ValueError("gbm_train_row_mismatch")
    if ids and int(X_arr.shape[1]) != int(len(ids)):
        raise ValueError("gbm_train_feature_schema_mismatch")

    X_arr = np.nan_to_num(X_arr, nan=0.0, posinf=0.0, neginf=0.0)
    y_arr = np.nan_to_num(y_arr, nan=0.0, posinf=0.0, neginf=0.0)
    params = _normalized_hyperparams(dict(hyperparams or {}))

    model = lgb.LGBMRegressor(**params)
    fit_kwargs: Dict[str, Any] = {}
    if ids:
        fit_kwargs["feature_name"] = list(ids)
    model.fit(X_arr, y_arr, **fit_kwargs)
    booster = model.booster_
    if booster is None:
        raise RuntimeError("gbm_training_missing_booster")

    created_ts = int(time.time() * 1000)
    schema = _feature_schema(ids, ts_ms=created_ts)
    meta = {
        "blob_version": int(_GBM_BLOB_VERSION),
        "model_kind": "lightgbm",
        "schema": dict(schema),
        "hyperparams": dict(params),
        "n_train": int(X_arr.shape[0]),
        "created_ts_ms": int(created_ts),
    }
    return _encode_blob(meta, booster.model_to_string())


def load_gbm_model(blob) -> tuple[Any, Dict[str, Any]]:
    """Load a LightGBM model blob and return `(model, schema)`."""
    lgb = _import_lightgbm()
    meta, model_text = _decode_blob(blob)
    model = lgb.Booster(model_str=model_text)
    schema = dict(meta.get("schema") or {})
    if not isinstance(schema.get("feature_ids"), list):
        schema["feature_ids"] = []
    return model, schema


def _coerce_feature_map(feature_snapshot: Any) -> Dict[str, Any]:
    if isinstance(feature_snapshot, dict):
        nested = feature_snapshot.get("features")
        if isinstance(nested, dict):
            return dict(nested)
        return dict(feature_snapshot)
    return {}


def _vectorize_feature_snapshot(
    schema: Dict[str, Any],
    feature_snapshot: Any,
) -> Tuple[np.ndarray, Dict[str, Any]]:
    feature_ids = _normalize_feature_ids((schema or {}).get("feature_ids"))
    feature_map = _coerce_feature_map(feature_snapshot)
    values: List[float] = []
    missing: List[str] = []

    for feature_id in feature_ids:
        if feature_id in feature_map:
            values.append(_safe_float(feature_map.get(feature_id), 0.0))
        else:
            values.append(0.0)
            missing.append(str(feature_id))

    coverage = float((len(feature_ids) - len(missing)) / max(1, len(feature_ids)))
    diagnostics = {
        "feature_ids": list(feature_ids),
        "feature_set_tag": str((schema or {}).get("feature_set_tag") or ""),
        "feature_count": int(len(feature_ids)),
        "missing_feature_ids": list(missing),
        "feature_coverage": float(coverage),
        "provided_feature_count": int(len(feature_map)),
    }
    return np.asarray(values, dtype=np.float32), diagnostics


def predict_with_gbm_model(blob, feature_snapshot) -> tuple[float, Dict[str, Any]]:
    """Predict from a persisted LightGBM blob using a feature snapshot payload."""
    model, schema = load_gbm_model(blob)
    vector, diagnostics = _vectorize_feature_snapshot(schema, feature_snapshot)
    raw = model.predict(vector.reshape(1, -1).astype(np.float32, copy=False))
    pred = float(np.asarray(raw, dtype=float).reshape(-1)[0])
    diagnostics.update({"model_kind": "lightgbm", "schema": dict(schema)})
    return float(pred), diagnostics


def persist_gbm_model_record(
    con,
    *,
    model_name: str,
    version: str,
    created_ts: int,
    blob: bytes,
    feature_schema: Dict[str, Any],
    training_metrics: Optional[Dict[str, Any]] = None,
) -> None:
    """Persist one trained GBM model blob with schema and training metadata."""
    _ensure_gbm_models_table(con)
    artifact_alias = f"model:gbm_regressor:{str(model_name)}:current"
    ref = LocalArtifactStore().put(
        bytes(blob or b""),
        content_type="application/vnd.lightgbm.text+json",
        kind="model",
        alias=artifact_alias,
        metadata={
            "model_name": str(model_name),
            "version": str(version),
            "created_ts": int(created_ts),
            "feature_schema": dict(feature_schema or {}),
            "training_metrics": dict(training_metrics or {}),
        },
    )
    con.execute(
        """
        INSERT INTO gbm_models(
          model_name, version, created_ts, blob, artifact_sha256, artifact_alias,
          feature_schema_json, training_metrics_json
        )
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(model_name, version) DO UPDATE SET
          created_ts=excluded.created_ts,
          blob=excluded.blob,
          artifact_sha256=excluded.artifact_sha256,
          artifact_alias=excluded.artifact_alias,
          feature_schema_json=excluded.feature_schema_json,
          training_metrics_json=excluded.training_metrics_json
        """,
        (
            str(model_name),
            str(version),
            int(created_ts),
            dbapi.Binary(b""),
            str(ref.sha256),
            str(artifact_alias),
            _json_dumps(dict(feature_schema or {})),
            (_json_dumps(dict(training_metrics or {})) if training_metrics is not None else None),
        ),
    )


def load_gbm_model_record(model_name: str, version: str) -> Optional[Dict[str, Any]]:
    """Load one persisted GBM model record by model name and version."""
    if not str(model_name or "").strip() or not str(version or "").strip():
        return None
    init_gbm_models_db()
    con = connect(readonly=True)
    try:
        row = con.execute(
            """
            SELECT model_name, version, created_ts, blob, feature_schema_json, training_metrics_json,
                   artifact_sha256, artifact_alias
            FROM gbm_models
            WHERE model_name=? AND version=?
            LIMIT 1
            """,
            (str(model_name), str(version)),
        ).fetchone()
    finally:
        con.close()

    if not row:
        return None
    blob = bytes(row[3] or b"")
    artifact_sha = str(row[6] or "").strip() if len(row) > 6 else ""
    artifact_alias = str(row[7] or "").strip() if len(row) > 7 else ""
    if artifact_alias or artifact_sha:
        try:
            store = LocalArtifactStore()
            ref = store.resolve(artifact_alias) if artifact_alias else None
            if ref is None and artifact_sha:
                from engine.artifacts.refs import ArtifactRef
                from datetime import datetime, timezone

                ref = ArtifactRef(
                    sha256=artifact_sha,
                    size=0,
                    content_type="application/octet-stream",
                    kind="model",
                    created_ts=datetime.now(timezone.utc),
                    metadata={},
                )
            if ref is not None:
                blob = store.get_bytes(ref)
        except Exception as exc:
            _warn_nonfatal(
                "GBM_ARTIFACT_LOAD_FAILED",
                exc,
                once_key=f"gbm_artifact_load_failed:{model_name}:{version}",
                model_name=str(model_name),
                version=str(version),
                artifact_sha256=str(artifact_sha),
                artifact_alias=str(artifact_alias),
            )
    return {
        "model_name": str(row[0] or ""),
        "version": str(row[1] or ""),
        "created_ts": int(row[2] or 0),
        "blob": blob,
        "feature_schema": _safe_json_dict(row[4]),
        "training_metrics": _safe_json_dict(row[5]),
        "artifact_sha256": artifact_sha,
        "artifact_alias": artifact_alias,
    }


def _eval_predictions(y_true: np.ndarray, y_pred: np.ndarray) -> Tuple[float, float, float]:
    y_true = np.asarray(y_true, dtype=float)
    y_pred = np.asarray(y_pred, dtype=float)
    if y_true.size == 0:
        return 0.0, 0.0, 0.0

    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))

    try:
        rt = y_true.argsort().argsort()
        rp = y_pred.argsort().argsort()
        spearman = float(np.corrcoef(rt, rp)[0, 1])
        if not np.isfinite(spearman):
            spearman = 0.0
    except Exception:
        spearman = 0.0

    try:
        eps = 1e-9
        yt_s = np.sign(np.where(np.abs(y_true) < eps, 0.0, y_true))
        yp_s = np.sign(np.where(np.abs(y_pred) < eps, 0.0, y_pred))
        directional = float(np.mean(yt_s == yp_s))
    except Exception:
        directional = 0.0

    return rmse, spearman, directional


def _ensure_meta(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_runs (
          key TEXT PRIMARY KEY,
          last_count INTEGER NOT NULL,
          last_max_created_at_ms INTEGER NOT NULL,
          last_run_ms INTEGER NOT NULL
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_model_runs_last_run ON model_runs(last_run_ms)")


def _labels_stamp(con) -> Tuple[int, int]:
    row = con.execute(
        """
        SELECT COUNT(*), MAX(created_at_ms)
        FROM labels
        WHERE impact_z IS NOT NULL
        """
    ).fetchone()
    return int((row[0] or 0) if row else 0), int((row[1] or 0) if row else 0)


def _load_tuned_hyperparams(model_name: str) -> Dict[str, Any]:
    if not _USE_TUNED_HYPERPARAMS:
        return {}
    try:
        row = fetch_latest_model_hyperparameters(
            model_name=str(model_name or "").strip(),
            model_family=_GBM_FAMILY,
            tuner="optuna_cpcv",
        )
    except Exception as exc:
        _warn_nonfatal(
            "GBM_TUNED_HYPERPARAMS_LOOKUP_FAILED",
            exc,
            once_key=f"gbm_tuned_hyperparams_lookup:{model_name}",
            model_name=str(model_name or ""),
        )
        return {}
    params = dict((row or {}).get("params") or {})
    return params if isinstance(params, dict) else {}


def _resolve_training_config(plan: Dict[str, Any]) -> Dict[str, Any]:
    requested_name = str((plan or {}).get("model_name") or "").strip()
    cfg = get_model_config(requested_name) if requested_name else {}
    if not cfg:
        configs = load_model_configs(family=_GBM_FAMILY, include_disabled=True)
        cfg = dict(configs[0]) if configs else {}

    model_name = str(requested_name or cfg.get("model_name") or _DEFAULT_MODEL_NAME).strip() or _DEFAULT_MODEL_NAME
    feature_ids = resolve_feature_ids(
        list(cfg.get("feature_ids") or _DEFAULT_FEATURE_IDS),
        model_name=str(model_name),
    )
    horizons = [int(h) for h in list(cfg.get("horizons_s") or cfg.get("horizons") or [_DEFAULT_HORIZON_S]) if int(h) > 0]
    horizon_s = int(cfg.get("horizon_s") or (horizons[0] if horizons else _DEFAULT_HORIZON_S) or _DEFAULT_HORIZON_S)
    tuned_hyperparams = _load_tuned_hyperparams(str(model_name))
    hyperparams = _normalized_hyperparams({**dict(cfg.get("hyperparams") or {}), **dict(tuned_hyperparams or {})})
    return {
        **cfg,
        "family": str(cfg.get("family") or _GBM_FAMILY).strip() or _GBM_FAMILY,
        "model_name": str(cfg.get("model_name") or model_name).strip() or model_name,
        "model_id": str(cfg.get("model_id") or cfg.get("model_name") or model_name).strip() or model_name,
        "instance_name": str(cfg.get("instance_name") or cfg.get("model_name") or model_name).strip() or model_name,
        "model_kind": str(cfg.get("model_kind") or "lightgbm").strip().lower() or "lightgbm",
        "symbol_universe": _normalize_symbol_universe(cfg.get("symbol_universe") or cfg.get("symbols") or _DEFAULT_SYMBOLS),
        "horizons_s": [int(horizon_s)],
        "horizon_s": int(horizon_s),
        "feature_ids": list(feature_ids),
        "feature_set_tag": str(cfg.get("feature_set_tag") or feature_set_tag_from_ids(list(feature_ids))).strip(),
        "training_window_days": int(cfg.get("training_window_days") or cfg.get("lookback_days") or _DEFAULT_LOOKBACK_DAYS),
        "risk_profile": str(cfg.get("risk_profile") or "balanced").strip().lower() or "balanced",
        "hyperparams": dict(hyperparams),
        "tuned_hyperparams": dict(tuned_hyperparams or {}),
        "use_tuned_hyperparams": bool(_USE_TUNED_HYPERPARAMS and bool(tuned_hyperparams)),
    }


def _load_training_rows(
    con,
    *,
    cutoff_ms: int,
    symbol_filter: set[str],
    horizon_s: int,
    feature_ids: Sequence[str],
) -> List[Tuple[np.ndarray, float, int, str]]:
    rows = []
    query = """
        SELECT
          l.symbol,
          l.horizon_s,
          COALESCE(le.net_z, l.impact_z) AS impact_z,
          e.ts_ms,
          e.title,
          e.body,
          e.source
        FROM labels l
        JOIN events e ON e.id = l.event_id
        LEFT JOIN labels_exec le
          ON le.event_id = l.event_id
         AND le.symbol = l.symbol
         AND le.horizon_s = l.horizon_s
         AND le.realized = 1
        WHERE e.ts_ms >= ?
          AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
        ORDER BY e.ts_ms ASC, l.event_id ASC, l.symbol ASC
    """
    fallback_query = """
        SELECT
          l.symbol,
          l.horizon_s,
          l.impact_z AS impact_z,
          e.ts_ms,
          e.title,
          e.body,
          e.source
        FROM labels l
        JOIN events e ON e.id = l.event_id
        WHERE e.ts_ms >= ?
          AND l.impact_z IS NOT NULL
        ORDER BY e.ts_ms ASC, l.event_id ASC, l.symbol ASC
    """
    try:
        source_rows = con.execute(query, (int(cutoff_ms),)).fetchall()
    except Exception:
        source_rows = con.execute(fallback_query, (int(cutoff_ms),)).fetchall()

    for symbol, row_horizon_s, impact_z, ts_ms, title, body, source in source_rows or []:
        sym = str(symbol or "").upper().strip()
        if not sym:
            continue
        if symbol_filter and sym not in symbol_filter:
            continue
        if int(row_horizon_s or 0) != int(horizon_s):
            continue

        event = {
            "ts_ms": int(ts_ms or 0),
            "title": str(title or ""),
            "body": str(body or ""),
            "source": str(source or ""),
        }
        try:
            snapshot = build_feature_snapshot(event=event, symbol=str(sym), feature_ids=list(feature_ids))
        except Exception as exc:
            _warn_nonfatal(
                "GBM_TRAINING_FEATURE_SNAPSHOT_FAILED",
                exc,
                once_key=f"gbm_training_feature_snapshot_failed:{sym}:{int(horizon_s)}",
                symbol=str(sym),
                horizon_s=int(horizon_s),
            )
            continue

        values = np.asarray(
            [_safe_float(dict(snapshot or {}).get(feature_id), 0.0) for feature_id in list(feature_ids)],
            dtype=np.float32,
        )
        label = _safe_float(impact_z, float("nan"))
        if not np.isfinite(label):
            continue
        rows.append((values, float(label), int(ts_ms or 0), str(sym)))
    return rows


def _predict_matrix_from_blob(blob: bytes, X: np.ndarray) -> np.ndarray:
    model, _schema = load_gbm_model(blob)
    preds = model.predict(np.asarray(X, dtype=np.float32))
    return np.asarray(preds, dtype=np.float32).reshape(-1)


def main() -> int:
    """Train, register, and publish one GBM model version from lifecycle inputs."""
    init_gbm_models_db()
    raw_plan = load_lifecycle_plan()
    plan_model_name = str((raw_plan or {}).get("model_name") or "").strip().lower()
    plan = dict(raw_plan or {}) if (not plan_model_name or plan_model_name.startswith(_GBM_FAMILY)) else {}
    train_cfg = _resolve_training_config(plan)
    pit_universe = {
        "pit_enabled": False,
        "pit_applied": False,
        "symbols": list(train_cfg.get("symbol_universe") or _DEFAULT_SYMBOLS),
        "fallback_reason": "not_resolved",
    }
    model_name = str(train_cfg.get("model_name") or _DEFAULT_MODEL_NAME).strip() or _DEFAULT_MODEL_NAME
    model_run_key = f"gbm_models:{model_name}"
    lifecycle_run_id = int(plan.get("lifecycle_run_id") or 0)
    version = ""

    if not training_allowed():
        print("gbm_regressor: training disabled by training_guard")
        return 0

    if not acquire_job_lock("train_gbm_regressor", OWNER, PID):
        print("gbm_regressor: another training job is running; exiting")
        return 0

    try:
        con = connect()
        try:
            _ensure_meta(con)
            cur_n, cur_mx = _labels_stamp(con)
            row = con.execute(
                """
                SELECT last_count, last_max_created_at_ms
                FROM model_runs
                WHERE key=?
                """,
                (str(model_run_key),),
            ).fetchone()
            last_n = int(row[0]) if row else 0
            last_mx = int(row[1]) if row else 0
            new_labels = max(0, int(cur_n) - int(last_n))
            changed = int(cur_mx) != int(last_mx)
            if (not plan) and ((not changed) or (new_labels < int(_DEFAULT_MIN_NEW_LABELS))):
                print(
                    f"gbm_regressor: SKIP cur_n={cur_n} last_n={last_n} "
                    f"new={new_labels} cur_mx={cur_mx} last_mx={last_mx} "
                    f"min_new={_DEFAULT_MIN_NEW_LABELS}"
                )
                con.execute(
                    """
                    INSERT INTO model_runs(key, last_count, last_max_created_at_ms, last_run_ms)
                    VALUES(?,?,?,?)
                    ON CONFLICT(key) DO UPDATE SET
                      last_count=excluded.last_count,
                      last_max_created_at_ms=excluded.last_max_created_at_ms,
                      last_run_ms=excluded.last_run_ms
                    """,
                    (str(model_run_key), int(last_n), int(last_mx), int(time.time() * 1000)),
                )
                con.commit()
                return 0
        finally:
            con.close()

        if lifecycle_run_id <= 0:
            lifecycle_run_id = int(
                start_lifecycle_run(
                    model_name=str(model_name),
                    model_version=str(plan.get("model_version") or ""),
                    parent_version=plan.get("parent_version"),
                    action="train_gbm_regressor",
                    status="running",
                    triggered_by="train_gbm_regressor",
                    mutation_kind=plan.get("mutation_kind"),
                    details={"variation": dict(plan or {})},
                )
                or 0
            )

        con_universe = connect(readonly=True)
        try:
            pit_universe = resolve_training_window_universe(
                con_universe,
                configured_symbols=list(train_cfg.get("symbol_universe") or _DEFAULT_SYMBOLS),
                lookback_days=int(train_cfg.get("training_window_days") or _DEFAULT_LOOKBACK_DAYS),
            )
        finally:
            con_universe.close()
        if list(pit_universe.get("symbols") or []):
            train_cfg["symbol_universe"] = list(pit_universe.get("symbols") or [])

        runtime_symbols = _normalize_symbol_universe(train_cfg.get("symbol_universe") or _DEFAULT_SYMBOLS)
        dataset_symbols = [] if "*" in runtime_symbols else list(runtime_symbols)
        feature_ids = list(train_cfg.get("feature_ids") or _DEFAULT_FEATURE_IDS)
        horizon_s = int(train_cfg.get("horizon_s") or _DEFAULT_HORIZON_S)
        lookback_days = int(train_cfg.get("training_window_days") or _DEFAULT_LOOKBACK_DAYS)
        hyperparams = dict(train_cfg.get("hyperparams") or _DEFAULT_HYPERPARAMS)

        print(
            f"gbm_regressor: TRAIN model_name={model_name} "
            f"lookback_days={lookback_days} min_samples={_DEFAULT_MIN_SAMPLES} "
            f"horizon_s={horizon_s} symbols={json.dumps(list(runtime_symbols))} "
            f"feature_ids={json.dumps(list(feature_ids))} "
            f"hyperparams={json.dumps(dict(hyperparams), separators=(',', ':'), sort_keys=True)}"
        )

        training_started_ts_ms = int(time.time() * 1000)
        dataset_feature_schema = _feature_schema(feature_ids, ts_ms=training_started_ts_ms)
        dataset_training_window = {
            "lookback_days": int(lookback_days),
            "end_ts_ms": int(training_started_ts_ms),
            "start_ts_ms": int(training_started_ts_ms - (int(lookback_days) * 24 * 60 * 60 * 1000)),
            "horizon_s": int(horizon_s),
        }
        dataset_used = build_dataset_snapshot(
            model_name=str(model_name),
            lookback_days=int(lookback_days),
            symbols=list(dataset_symbols),
            horizons=[int(horizon_s)],
            feature_ids=list(feature_ids),
            feature_schema=dict(dataset_feature_schema),
            training_window=dict(dataset_training_window),
            extra={
                "job_name": "train_gbm_regressor",
                "hyperparams": dict(hyperparams),
                "pit_universe": dict(pit_universe or {}),
            },
        )

        cutoff_ms = int(time.time() * 1000) - (int(lookback_days) * 24 * 60 * 60 * 1000)
        con_train = connect()
        try:
            rows = _load_training_rows(
                con_train,
                cutoff_ms=int(cutoff_ms),
                symbol_filter=_runtime_symbol_filter(runtime_symbols),
                horizon_s=int(horizon_s),
                feature_ids=list(feature_ids),
            )
        finally:
            con_train.close()

        if int(len(rows)) < int(max(2, _DEFAULT_MIN_SAMPLES)):
            print(f"gbm_regressor: insufficient_samples n={len(rows)} min_required={max(2, _DEFAULT_MIN_SAMPLES)}")
            return 0

        X = np.stack([row[0] for row in rows]).astype(np.float32, copy=False)
        y = np.asarray([row[1] for row in rows], dtype=np.float32)
        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
        y = np.nan_to_num(y, nan=0.0, posinf=0.0, neginf=0.0)

        n = int(len(y))
        split = min(max(1, int(n * 0.8)), int(n - 1))
        if split <= 0 or split >= int(n):
            print(f"gbm_regressor: invalid_split n={n} split={split}")
            return 0

        Xtr, Xev = X[:split], X[split:]
        ytr, yev = y[:split], y[split:]
        blob = train_gbm_model(Xtr, ytr, feature_ids=list(feature_ids), hyperparams=dict(hyperparams))
        pred = _predict_matrix_from_blob(blob, Xev)
        rmse, spearman, directional_acc = _eval_predictions(yev, pred)
        try:
            oos_run_id = str(uuid.uuid4())
            upsert_oos_predictions(
                [
                    {
                        "symbol": str(rows[split + idx][3] or "*"),
                        "horizon": int(horizon_s),
                        "family": _GBM_FAMILY,
                        "ts": int(rows[split + idx][2] or 0),
                        "run_id": str(oos_run_id),
                        "prediction": float(pred[idx]),
                        "target": float(yev[idx]),
                    }
                    for idx in range(int(len(yev)))
                ]
            )
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)

        created_ts = int(time.time() * 1000)
        version = str(plan.get("model_version") or version_from_ts(str(model_name), int(created_ts), prefix="gbm"))
        feature_schema = _feature_schema(feature_ids, ts_ms=created_ts)
        training_metrics = {
            "model_name": str(model_name),
            "model_kind": "lightgbm",
            "n_train": int(len(ytr)),
            "n_eval": int(len(yev)),
            "rmse": float(rmse),
            "spearman": float(spearman),
            "directional_acc": float(directional_acc),
            "quality_score": float(max(0.0, min(1.0, directional_acc))),
            "feature_ids": list(feature_ids),
            "feature_set_tag": str(feature_schema.get("feature_set_tag") or ""),
            "feature_schema": dict(feature_schema),
            "model_version": str(version),
            "model_family": _GBM_FAMILY,
        }

        con_write = connect()
        try:
            persist_gbm_model_record(
                con_write,
                model_name=str(model_name),
                version=str(version),
                created_ts=int(created_ts),
                blob=blob,
                feature_schema=dict(feature_schema),
                training_metrics=dict(training_metrics),
            )
            con_write.commit()
        finally:
            con_write.close()

        registration_meta = build_model_registration_metadata(train_cfg)
        register_model_version(
            model_name=str(model_name),
            model_version=str(version),
            model_kind="lightgbm",
            parent_version=plan.get("parent_version"),
            mutation_kind=str(plan.get("mutation_kind") or "baseline_retrain"),
            stage="shadow",
            status="trained",
            live_ready=False,
            training_job_name="train_gbm_regressor",
            train_scope={
                **dict(
                    plan.get("train_scope")
                    or {
                        "symbols": list(runtime_symbols),
                        "horizons": [int(horizon_s)],
                        "lookback_days": int(lookback_days),
                        "feature_ids": list(feature_ids),
                        "risk_profile": str(train_cfg.get("risk_profile") or "balanced"),
                    }
                ),
                "dataset_used": dataset_used,
            },
            meta={
                "standalone_job": not bool(plan),
                "trigger": plan.get("trigger") or {},
                "model_id": str(registration_meta.get("model_id") or model_name),
                "model_family": str(registration_meta.get("model_family") or _GBM_FAMILY),
                "instance_name": str(registration_meta.get("instance_name") or model_name),
                "risk_profile": str(registration_meta.get("risk_profile") or "balanced"),
                "feature_schema": dict(feature_schema),
                "dataset_used": dataset_used,
                "training_started_ts_ms": int(training_started_ts_ms),
                "training_completed_ts_ms": int(created_ts),
            },
        )
        record_version_performance(
            model_name=str(model_name),
            model_version=str(version),
            metric_scope="training",
            metrics={
                "avg_rmse": float(rmse),
                "avg_spearman": float(spearman),
                "avg_directional_acc": float(directional_acc),
                "quality_score": float(max(0.0, min(1.0, directional_acc))),
                "trained_models": 1,
                "eval_ts_ms": int(created_ts),
            },
            sample_n=int(len(yev)),
            meta={"job_name": "train_gbm_regressor"},
        )
        update_model_version_status(
            str(model_name),
            str(version),
            stage="shadow",
            status="trained",
            live_ready=False,
            meta_patch={
                "dataset_used": dataset_used,
                "feature_schema": dict(feature_schema),
                "training_started_ts_ms": int(training_started_ts_ms),
                "training_completed_ts_ms": int(created_ts),
            },
        )
        if lifecycle_run_id > 0:
            finish_lifecycle_run(
                int(lifecycle_run_id),
                status="ok",
                details={
                    "model_version": str(version),
                    "trained_models": 1,
                    "total_eval": int(len(yev)),
                    "dataset_used": dataset_used,
                },
            )
        publish_lifecycle_status(
            {
                "ok": True,
                "model_name": str(model_name),
                "active_job": "train_gbm_regressor",
                "version": str(version),
                "mutation_kind": str(plan.get("mutation_kind") or "baseline_retrain"),
                "trained_models": 1,
                "dataset_used": dataset_used,
                "ts_ms": int(time.time() * 1000),
            }
        )

        con_meta = connect()
        try:
            _ensure_meta(con_meta)
            cur_n2, cur_mx2 = _labels_stamp(con_meta)
            con_meta.execute(
                """
                INSERT INTO model_runs(key, last_count, last_max_created_at_ms, last_run_ms)
                VALUES(?,?,?,?)
                ON CONFLICT(key) DO UPDATE SET
                  last_count=excluded.last_count,
                  last_max_created_at_ms=excluded.last_max_created_at_ms,
                  last_run_ms=excluded.last_run_ms
                """,
                (str(model_run_key), int(cur_n2), int(cur_mx2), int(time.time() * 1000)),
            )
            con_meta.commit()
        finally:
            con_meta.close()

        return 0
    except Exception:
        if version:
            try:
                update_model_version_status(
                    str(model_name),
                    str(version),
                    stage="retired",
                    status="error",
                    live_ready=False,
                    meta_patch={"error_ts_ms": int(time.time() * 1000)},
                )
            except Exception:
                _warn_nonfatal(
                    "TRAIN_GBM_REGRESSOR_VERSION_STATUS_FAILED",
                    RuntimeError("update_model_version_status failed"),
                    once_key="train_gbm_regressor_version_status_failed",
                    version=str(version),
                )
        if lifecycle_run_id > 0:
            try:
                finish_lifecycle_run(
                    int(lifecycle_run_id),
                    status="error",
                    details={"error_ts_ms": int(time.time() * 1000)},
                )
            except Exception:
                _warn_nonfatal(
                    "TRAIN_GBM_REGRESSOR_LIFECYCLE_STATUS_FAILED",
                    RuntimeError("finish_lifecycle_run failed"),
                    once_key="train_gbm_regressor_lifecycle_status_failed",
                    lifecycle_run_id=int(lifecycle_run_id),
                )
        raise
    finally:
        try:
            release_job_lock("train_gbm_regressor", OWNER, PID)
        except Exception as exc:
            _warn_nonfatal(
                "TRAIN_GBM_REGRESSOR_RELEASE_LOCK_FAILED",
                exc,
                once_key="train_gbm_regressor_release_lock_failed",
            )


__all__ = [
    "init_gbm_models_db",
    "load_gbm_model",
    "load_gbm_model_record",
    "main",
    "persist_gbm_model_record",
    "predict_with_gbm_model",
    "train_gbm_model",
]


if __name__ == "__main__":
    raise SystemExit(main())
