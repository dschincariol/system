"""Opt-in additive ensemble blending for legacy predictor outputs.

This module never replaces champion/challenger routing. The predictor still
resolves and serves the canonical single-family model first; ensemble blending
can optionally combine that served output with additional family predictions.
"""

from __future__ import annotations

import json
import logging
import math
import os
import pickle
import threading
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

import numpy as np

from engine.artifacts.serialization import dumps_pickle_artifact
from engine.artifacts.store import LocalArtifactStore
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, run_write_txn

_PREDICTION_CONTEXT = threading.local()
_SUPPORTED_MODES = {"equal", "inverse_variance", "stacked"}
_VARIANCE_LOOKBACK_ROWS = int(os.environ.get("ENSEMBLE_VARIANCE_LOOKBACK_ROWS", "256") or 256)
_CURRENT_WEIGHT_PERSIST_MIN_S = float(os.environ.get("ENSEMBLE_WEIGHT_PERSIST_MIN_S", "300") or 300.0)
LOG = get_logger("engine.strategy.ensemble_blender")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="ensemble_blender_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.ensemble_blender",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def ensemble_blend_enabled() -> bool:
    """Return whether opt-in ensemble blending is enabled for predictor outputs."""
    return bool(_env_flag("ENSEMBLE_BLEND_ENABLED", False))


def ensemble_blend_mode() -> str:
    """Return the configured ensemble weight computation mode."""
    raw = str(os.environ.get("ENSEMBLE_BLEND_MODE", "equal") or "equal").strip().lower()
    return raw if raw in _SUPPORTED_MODES else "equal"


def ensemble_max_weight() -> float:
    """Return the per-family weight cap applied after normalization."""
    try:
        value = float(os.environ.get("ENSEMBLE_MAX_WEIGHT", "0.75") or 0.75)
    except Exception:
        value = 0.75
    return float(min(1.0, max(0.0, value)))


def ensemble_min_agreement() -> float:
    """Return the minimum agreement score required before applying a blend."""
    try:
        value = float(os.environ.get("ENSEMBLE_MIN_AGREEMENT", "0.0") or 0.0)
    except Exception:
        value = 0.0
    return float(min(1.0, max(0.0, value)))


def ensemble_meta_retrain_s() -> int:
    """Return the minimum interval between stacked meta-learner retraining runs."""
    try:
        value = int(os.environ.get("ENSEMBLE_META_RETRAIN_S", "86400") or 86400)
    except Exception:
        value = 86400
    return int(max(1, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)


def _json_loads(payload: Any, default: Any) -> Any:
    if payload in (None, "", b"", bytearray()):
        return default
    if isinstance(payload, (dict, list)):
        return payload
    try:
        out = json.loads(payload.decode("utf-8", errors="replace") if isinstance(payload, (bytes, bytearray)) else str(payload))
    except Exception:
        return default
    if default is None:
        return out
    return out if isinstance(out, type(default)) else default


def _load_artifact_blob(alias: str, sha256: str) -> bytes:
    store = LocalArtifactStore()
    ref = store.resolve(alias) if str(alias or "").strip() else None
    if ref is None and str(sha256 or "").strip():
        from datetime import datetime, timezone

        from engine.artifacts.refs import ArtifactRef

        ref = ArtifactRef(
            sha256=str(sha256).strip(),
            size=0,
            content_type="application/octet-stream",
            kind="model",
            created_ts=datetime.now(timezone.utc),
            metadata={},
        )
    if ref is None:
        return b""
    return store.get_bytes(ref)


def _ensure_ensemble_artifact_columns(con) -> None:
    existing = {
        str(row[1] or "")
        for row in con.execute("PRAGMA table_info(ensemble_blend_weights)").fetchall()
        if row and len(row) > 1
    }
    for column_name in ("meta_artifact_sha256", "meta_artifact_alias"):
        if column_name not in existing:
            con.execute(f"ALTER TABLE ensemble_blend_weights ADD COLUMN {column_name} TEXT")
            existing.add(column_name)


def _ensemble_meta_artifact_alias(mode: str, regime: Optional[str]) -> str:
    mode_name = str(mode or "stacked").strip().lower() or "stacked"
    regime_name = str(regime or "global").strip() or "global"
    return f"model:ensemble_blender:{mode_name}:{regime_name}:current"


def _family_keys(mapping: Mapping[str, Any]) -> List[str]:
    return [
        str(key)
        for key in list(mapping.keys() or [])
        if str(key or "").strip() and not str(key).startswith("__")
    ]


def _normalize_member_payload(family: str, payload: Any) -> Optional[Dict[str, Any]]:
    fam = str(family or "").strip()
    if not fam:
        return None
    if isinstance(payload, (int, float)):
        prediction = _safe_float(payload, float("nan"))
        if not math.isfinite(prediction):
            return None
        return {
            "family": fam,
            "prediction": float(prediction),
            "confidence": 0.0,
            "model_name": "",
            "model_id": "",
            "model_kind": "",
            "model_version": "",
        }
    if not isinstance(payload, Mapping):
        return None
    prediction = _safe_float(payload.get("prediction"), float("nan"))
    if not math.isfinite(prediction):
        return None
    return {
        "family": fam,
        "prediction": float(prediction),
        "confidence": float(max(0.0, min(1.0, _safe_float(payload.get("confidence"), 0.0)))),
        "model_name": str(payload.get("model_name") or ""),
        "model_id": str(payload.get("model_id") or ""),
        "model_kind": str(payload.get("model_kind") or ""),
        "model_version": str(payload.get("model_version") or ""),
        "explain": dict(payload.get("explain") or {}) if isinstance(payload.get("explain"), Mapping) else {},
    }


def _normalize_family_predictions(family_preds: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for family in _family_keys(family_preds):
        normalized = _normalize_member_payload(family, family_preds.get(family))
        if normalized is None:
            continue
        out[str(family)] = normalized
    return out


def _normalize_weight_map(weights: Mapping[str, Any], families: Iterable[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for family in list(families or []):
        key = str(family or "").strip()
        if not key:
            continue
        out[key] = max(0.0, _safe_float(weights.get(key), 0.0))
    return out


def _equal_weights(families: Iterable[str]) -> Dict[str, float]:
    names = [str(family) for family in list(families or []) if str(family or "").strip()]
    if not names:
        return {}
    weight = 1.0 / float(len(names))
    return {name: float(weight) for name in names}


def _normalize_sum(weights: Mapping[str, float], *, fallback: Optional[Mapping[str, float]] = None) -> Dict[str, float]:
    cleaned = {
        str(key): max(0.0, _safe_float(value, 0.0))
        for key, value in dict(weights or {}).items()
        if str(key or "").strip()
    }
    total = float(sum(cleaned.values()))
    if total <= 0.0:
        base = dict(fallback or {})
        base_total = float(sum(max(0.0, _safe_float(value, 0.0)) for value in base.values()))
        if base_total <= 0.0:
            return {}
        return {
            str(key): float(max(0.0, _safe_float(value, 0.0)) / base_total)
            for key, value in base.items()
            if str(key or "").strip()
        }
    return {str(key): float(value / total) for key, value in cleaned.items()}


def _apply_max_weight_cap(weights: Mapping[str, float], max_weight: float) -> Dict[str, float]:
    normalized = _normalize_sum(weights, fallback=weights)
    if not normalized:
        return {}
    cap = float(max(0.0, min(1.0, max_weight)))
    if cap <= 0.0:
        return {}
    if cap >= 1.0:
        return normalized
    if (float(len(normalized)) * cap) + 1e-12 < 1.0:
        return _equal_weights(normalized.keys())

    remaining = dict(normalized)
    capped: Dict[str, float] = {}
    remaining_mass = 1.0
    while remaining:
        subtotal = float(sum(remaining.values()))
        if subtotal <= 0.0:
            if remaining and remaining_mass > 0.0:
                share = remaining_mass / float(len(remaining))
                for family in list(remaining.keys()):
                    capped[family] = float(share)
            break

        scale = remaining_mass / subtotal
        over_limit = [
            family
            for family, value in remaining.items()
            if (float(value) * scale) > (cap + 1e-12)
        ]
        if not over_limit:
            for family, value in remaining.items():
                capped[family] = float(value * scale)
            break

        for family in over_limit:
            capped[family] = float(cap)
            remaining_mass -= float(cap)
            remaining.pop(family, None)
        if remaining_mass <= 1e-12:
            break

    return _normalize_sum(capped, fallback=_equal_weights(normalized.keys()))


def set_prediction_context(**kwargs: Any) -> None:
    """Store per-request predictor context used to query extra family outputs."""
    _PREDICTION_CONTEXT.payload = dict(kwargs or {})


def clear_prediction_context() -> None:
    """Clear thread-local predictor context after one prediction flow completes."""
    try:
        delattr(_PREDICTION_CONTEXT, "payload")
    except Exception as exc:
        _warn_nonfatal(
            "ENSEMBLE_CLEAR_PREDICTION_CONTEXT_FAILED",
            exc,
            once_key="ensemble_clear_prediction_context_failed",
        )
        return


def collect_family_predictions(symbol, ts) -> dict:
    """Collect normalized member predictions for the requested ensemble families."""
    ctx = dict(getattr(_PREDICTION_CONTEXT, "payload", {}) or {})
    attempted_families = [
        str(family)
        for family in list(ctx.get("families") or [])
        if str(family or "").strip()
    ]
    out: Dict[str, Any] = {
        "__attempted_families__": list(attempted_families),
        "__missing_families__": [],
        "__errors__": {},
        "__symbol__": str(symbol or "").upper().strip(),
        "__ts__": int(_safe_int(ts, 0)),
    }
    if not ctx:
        return out

    base_member = _normalize_member_payload(
        str((ctx.get("base_family_pred") or {}).get("family") or ""),
        ctx.get("base_family_pred"),
    )
    if base_member is not None:
        out[str(base_member["family"])] = dict(base_member)

    predict_family = ctx.get("predict_family")
    for family in attempted_families:
        if family in out:
            continue
        if not callable(predict_family):
            out["__missing_families__"].append(str(family))
            out["__errors__"][str(family)] = "prediction_context_missing_predict_family"
            continue
        try:
            payload = predict_family(str(family))
        except Exception as exc:
            _warn_nonfatal(
                "ENSEMBLE_FAMILY_PREDICTION_FAILED",
                exc,
                once_key=f"ensemble_family_prediction_failed:{str(symbol or '').upper().strip()}:{str(family)}",
                symbol=str(symbol or "").upper().strip(),
                family=str(family),
                ts=int(_safe_int(ts, 0)),
            )
            out["__missing_families__"].append(str(family))
            out["__errors__"][str(family)] = f"{type(exc).__name__}:{exc}"
            continue
        member = _normalize_member_payload(str(family), payload)
        if member is None:
            out["__missing_families__"].append(str(family))
            out["__errors__"][str(family)] = "family_unavailable"
            continue
        out[str(family)] = dict(member)
    return out


def _load_recent_prediction_variances(families: Iterable[str]) -> Dict[str, float]:
    family_names = [str(family) for family in list(families or []) if str(family or "").strip()]
    if not family_names:
        return {}
    con = connect(readonly=True)
    try:
        rows = con.execute(
            """
            SELECT family_preds_json
            FROM ensemble_predictions
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            (int(max(1, _VARIANCE_LOOKBACK_ROWS)),),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal(
            "ENSEMBLE_VARIANCE_HISTORY_LOAD_FAILED",
            exc,
            once_key="ensemble_variance_history_load_failed",
            lookback_rows=int(max(1, _VARIANCE_LOOKBACK_ROWS)),
        )
        rows = []
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "ENSEMBLE_VARIANCE_HISTORY_CLOSE_FAILED",
                exc,
                once_key="ensemble_variance_history_close_failed",
            )

    samples: Dict[str, List[float]] = {family: [] for family in family_names}
    for row in rows or []:
        payload = _json_loads((row[0] if row else None), {})
        if not isinstance(payload, Mapping):
            continue
        for family in family_names:
            member = payload.get(family)
            normalized = _normalize_member_payload(family, member)
            if normalized is None:
                continue
            samples[family].append(float(normalized["prediction"]))

    variances: Dict[str, float] = {}
    for family, values in samples.items():
        if len(values) < 2:
            continue
        variance = float(np.var(np.asarray(values, dtype=np.float64)))
        if math.isfinite(variance) and variance > 0.0:
            variances[str(family)] = float(variance)
    return variances


def _load_latest_stacked_weights(regime: Optional[str]) -> Tuple[Dict[str, float], Optional[bytes], float, int]:
    con = connect(readonly=True)
    try:
        params: List[Any] = ["stacked"]
        sql = """
            SELECT weights_json, meta_blob, meta_artifact_sha256, meta_artifact_alias, created_ts
            FROM ensemble_blend_weights
            WHERE mode=?
        """
        if str(regime or "").strip():
            sql += " AND COALESCE(regime,'')=?"
            params.append(str(regime))
        else:
            sql += " AND regime IS NULL"
        sql += " ORDER BY created_ts DESC, id DESC LIMIT 1"
        row = con.execute(sql, tuple(params)).fetchone()
    except Exception as exc:
        try:
            params = ["stacked"]
            sql = """
                SELECT weights_json, meta_blob, created_ts
                FROM ensemble_blend_weights
                WHERE mode=?
            """
            if str(regime or "").strip():
                sql += " AND COALESCE(regime,'')=?"
                params.append(str(regime))
            else:
                sql += " AND regime IS NULL"
            sql += " ORDER BY created_ts DESC, id DESC LIMIT 1"
            legacy_row = con.execute(sql, tuple(params)).fetchone()
            row = (
                (legacy_row[0], legacy_row[1], "", "", legacy_row[2])
                if legacy_row
                else None
            )
        except Exception as legacy_exc:
            _warn_nonfatal(
                "ENSEMBLE_STACKED_WEIGHTS_LOAD_FAILED",
                legacy_exc,
                once_key=f"ensemble_stacked_weights_load_failed:{str(regime or '').strip() or 'global'}",
                regime=str(regime or ""),
                original_error=repr(exc),
            )
            row = None
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "ENSEMBLE_STACKED_WEIGHTS_CLOSE_FAILED",
                exc,
                once_key="ensemble_stacked_weights_close_failed",
            )

    if not row:
        return {}, None, 0.0, 0
    weights_json, meta_blob, meta_artifact_sha256, meta_artifact_alias, created_ts = row
    weights = _json_loads(weights_json, {})
    parsed_weights = {
        str(key): max(0.0, _safe_float(value, 0.0))
        for key, value in dict(weights or {}).items()
        if str(key or "").strip()
    }
    intercept = 0.0
    row_count = 0
    blob_bytes = bytes(meta_blob or b"")
    if not blob_bytes and (meta_artifact_alias or meta_artifact_sha256):
        try:
            blob_bytes = _load_artifact_blob(str(meta_artifact_alias or ""), str(meta_artifact_sha256 or ""))
        except Exception as exc:
            _warn_nonfatal(
                "ENSEMBLE_STACKED_META_ARTIFACT_LOAD_FAILED",
                exc,
                once_key=f"ensemble_stacked_meta_artifact_load_failed:{created_ts}",
                regime=str(regime or ""),
                artifact_sha256=str(meta_artifact_sha256 or ""),
                artifact_alias=str(meta_artifact_alias or ""),
            )
            blob_bytes = b""
    if blob_bytes:
        try:
            payload = pickle.loads(blob_bytes)
        except Exception as exc:
            _warn_nonfatal(
                "ENSEMBLE_STACKED_META_UNPICKLE_FAILED",
                exc,
                once_key=f"ensemble_stacked_meta_unpickle_failed:{created_ts}",
                regime=str(regime or ""),
                trained_ts=int(_safe_int(created_ts, 0)),
            )
            payload = {}
        if isinstance(payload, Mapping):
            intercept = float(_safe_float(payload.get("intercept"), 0.0))
            row_count = int(_safe_int(payload.get("row_count"), 0))
            blob_weights = payload.get("weights")
            if isinstance(blob_weights, Mapping):
                parsed_weights = {
                    str(key): max(0.0, _safe_float(value, 0.0))
                    for key, value in dict(blob_weights or {}).items()
                    if str(key or "").strip()
                }
    return parsed_weights, (blob_bytes or None), float(intercept), int(_safe_int(created_ts, 0))


def compute_blend_weights(families, mode, regime=None) -> dict:
    """Resolve ensemble family weights for the requested mode and regime."""
    if isinstance(families, Mapping):
        family_names = _family_keys(families)
    else:
        family_names = [str(family) for family in list(families or []) if str(family or "").strip()]
    if not family_names:
        return {}

    resolved_mode = str(mode or "equal").strip().lower()
    if resolved_mode not in _SUPPORTED_MODES:
        resolved_mode = "equal"

    if resolved_mode == "equal":
        return _apply_max_weight_cap(_equal_weights(family_names), ensemble_max_weight())

    if resolved_mode == "inverse_variance":
        variances = _load_recent_prediction_variances(family_names)
        if not variances:
            return _apply_max_weight_cap(_equal_weights(family_names), ensemble_max_weight())
        raw_weights = {
            family: float(1.0 / max(1e-9, variances.get(family, np.median(list(variances.values())))))
            for family in family_names
        }
        return _apply_max_weight_cap(_normalize_sum(raw_weights, fallback=_equal_weights(family_names)), ensemble_max_weight())

    stacked_weights, meta_blob, intercept, trained_ts = _load_latest_stacked_weights(regime)
    if not stacked_weights:
        return _apply_max_weight_cap(_equal_weights(family_names), ensemble_max_weight())
    available_weights = {family: stacked_weights.get(family, 0.0) for family in family_names}
    normalized = _apply_max_weight_cap(_normalize_sum(available_weights, fallback=_equal_weights(family_names)), ensemble_max_weight())
    normalized["__mode__"] = "stacked"
    normalized["__intercept__"] = float(intercept)
    normalized["__trained_ts__"] = int(trained_ts)
    normalized["__has_meta_blob__"] = bool(meta_blob)
    return normalized


def train_stacking_meta_learner(history_rows) -> bytes:
    """Fit and serialize the lightweight stacked ensemble meta-learner."""
    rows = list(history_rows or [])
    families_seen: List[str] = []
    xs: List[List[float]] = []
    ys: List[float] = []

    for row in rows:
        if isinstance(row, Mapping):
            preds_raw = row.get("family_preds")
            if preds_raw is None:
                preds_raw = _json_loads(row.get("family_preds_json"), {})
            target = None
            for key in ("target", "realized", "realized_z", "label", "y", "net_z"):
                if row.get(key) is not None:
                    target = row.get(key)
                    break
        elif isinstance(row, (tuple, list)) and len(row) >= 2:
            preds_raw = row[0]
            target = row[1]
        else:
            continue
        if target is None:
            continue
        preds = _normalize_family_predictions(preds_raw if isinstance(preds_raw, Mapping) else {})
        if not preds:
            continue
        for family in list(preds.keys()):
            if family not in families_seen:
                families_seen.append(str(family))
        ys.append(float(_safe_float(target, 0.0)))
        xs.append([float(preds.get(family, {}).get("prediction", 0.0)) for family in families_seen])
        if len(families_seen) > len(xs[-1]):
            xs[-1].extend([0.0] * (len(families_seen) - len(xs[-1])))
        for prior_idx in range(0, len(xs) - 1):
            if len(xs[prior_idx]) < len(families_seen):
                xs[prior_idx].extend([0.0] * (len(families_seen) - len(xs[prior_idx])))

    if not xs or not families_seen:
        payload = {
            "families": [],
            "intercept": 0.0,
            "weights": {},
            "row_count": 0,
            "trained_ts": int(time.time() * 1000),
            "version": 1,
        }
        return dumps_pickle_artifact(payload)

    matrix = np.asarray(xs, dtype=np.float64)
    targets = np.asarray(ys, dtype=np.float64)
    ones = np.ones((matrix.shape[0], 1), dtype=np.float64)
    design = np.concatenate([matrix, ones], axis=1)
    alpha = 1e-3
    penalty = np.eye(design.shape[1], dtype=np.float64)
    penalty[-1, -1] = 0.0
    try:
        beta = np.linalg.solve((design.T @ design) + (alpha * penalty), design.T @ targets)
    except Exception as exc:
        _warn_nonfatal(
            "ENSEMBLE_STACKED_SOLVE_FAILED",
            exc,
            once_key=f"ensemble_stacked_solve_failed:{int(matrix.shape[0])}:{int(matrix.shape[1])}",
            row_count=int(matrix.shape[0]),
            family_count=int(matrix.shape[1]),
        )
        beta, *_rest = np.linalg.lstsq(design, targets, rcond=None)
    raw_weights = np.asarray(beta[:-1], dtype=np.float64)
    raw_weights = np.maximum(raw_weights, 0.0)
    if float(raw_weights.sum()) <= 0.0:
        weights = _equal_weights(families_seen)
    else:
        weights = _apply_max_weight_cap(
            {
                family: float(raw_weights[idx])
                for idx, family in enumerate(families_seen)
            },
            ensemble_max_weight(),
        )
    payload = {
        "families": list(families_seen),
        "intercept": float(_safe_float(beta[-1], 0.0)),
        "weights": dict(weights),
        "row_count": int(matrix.shape[0]),
        "trained_ts": int(time.time() * 1000),
        "version": 1,
    }
    return dumps_pickle_artifact(payload)


def prediction_agreement(family_preds) -> float:
    """Measure directional agreement across normalized family predictions."""
    normalized = _normalize_family_predictions(family_preds if isinstance(family_preds, Mapping) else {})
    values = [float(member.get("prediction", 0.0)) for member in normalized.values()]
    if not values:
        return 0.0
    if len(values) == 1:
        return 1.0
    scores: List[float] = []
    for idx in range(len(values)):
        for jdx in range(idx + 1, len(values)):
            a = float(values[idx])
            b = float(values[jdx])
            denom = abs(a) + abs(b)
            if denom <= 1e-9:
                scores.append(1.0)
                continue
            score = 1.0 - (abs(a - b) / denom)
            scores.append(float(max(0.0, min(1.0, score))))
    if not scores:
        return 1.0
    return float(max(0.0, min(1.0, float(sum(scores) / len(scores)))))


def blend_predictions(family_preds, weights) -> tuple[float, dict]:
    """Blend normalized family predictions into one score plus diagnostics."""
    normalized_preds = _normalize_family_predictions(family_preds if isinstance(family_preds, Mapping) else {})
    available_families = list(normalized_preds.keys())
    if not available_families:
        return 0.0, {
            "agreement": 0.0,
            "applied": False,
            "family_contributions": {},
            "missing_families": [],
            "effective_weights": {},
            "blended_confidence": 0.0,
            "mode": str((weights or {}).get("__mode__") or "equal"),
        }

    weight_map = dict(weights or {}) if isinstance(weights, Mapping) else {}
    missing_families = [
        family
        for family in _family_keys(weight_map)
        if family not in normalized_preds
    ]
    meta_missing = list((family_preds or {}).get("__missing_families__") or []) if isinstance(family_preds, Mapping) else []
    for family in meta_missing:
        name = str(family or "").strip()
        if name and name not in missing_families:
            missing_families.append(name)

    mode = str(weight_map.get("__mode__") or "weighted").strip().lower()
    intercept = float(_safe_float(weight_map.get("__intercept__"), 0.0))
    trained_ts = int(_safe_int(weight_map.get("__trained_ts__"), 0))
    raw_weights = _normalize_weight_map(weight_map, available_families)

    if mode == "stacked":
        effective_weights = _apply_max_weight_cap(_normalize_sum(raw_weights, fallback=_equal_weights(available_families)), ensemble_max_weight())
        blended_prediction = float(intercept)
        family_contributions: Dict[str, Any] = {}
        for family in available_families:
            member = dict(normalized_preds.get(family) or {})
            weight = float(effective_weights.get(family, 0.0))
            contribution = float(weight * float(member.get("prediction") or 0.0))
            blended_prediction += contribution
            family_contributions[str(family)] = {
                "prediction": float(member.get("prediction") or 0.0),
                "confidence": float(member.get("confidence") or 0.0),
                "weight": float(weight),
                "contribution": float(contribution),
                "model_name": str(member.get("model_name") or ""),
                "model_id": str(member.get("model_id") or ""),
            }
        blended_confidence = float(
            sum(
                float(effective_weights.get(family, 0.0)) * float((normalized_preds.get(family) or {}).get("confidence") or 0.0)
                for family in available_families
            )
        )
    else:
        effective_weights = _apply_max_weight_cap(_normalize_sum(raw_weights, fallback=_equal_weights(available_families)), ensemble_max_weight())
        family_contributions = {}
        blended_prediction = 0.0
        blended_confidence = 0.0
        for family in available_families:
            member = dict(normalized_preds.get(family) or {})
            weight = float(effective_weights.get(family, 0.0))
            contribution = float(weight * float(member.get("prediction") or 0.0))
            blended_prediction += contribution
            blended_confidence += float(weight * float(member.get("confidence") or 0.0))
            family_contributions[str(family)] = {
                "prediction": float(member.get("prediction") or 0.0),
                "confidence": float(member.get("confidence") or 0.0),
                "weight": float(weight),
                "contribution": float(contribution),
                "model_name": str(member.get("model_name") or ""),
                "model_id": str(member.get("model_id") or ""),
            }

    diagnostics = {
        "agreement": float(prediction_agreement(normalized_preds)),
        "applied": True,
        "available_families": list(available_families),
        "missing_families": list(missing_families),
        "effective_weights": dict(effective_weights),
        "family_contributions": family_contributions,
        "blended_confidence": float(max(0.0, min(1.0, blended_confidence))),
        "mode": str(mode or "weighted"),
    }
    if mode == "stacked":
        diagnostics["intercept"] = float(intercept)
        diagnostics["trained_ts"] = int(trained_ts)
    return float(blended_prediction), diagnostics


def persist_ensemble_prediction(
    *,
    symbol: str,
    ts: int,
    blended_prediction: float,
    family_preds: Mapping[str, Any],
    weights: Mapping[str, Any],
    agreement: float,
) -> None:
    """Persist one ensemble prediction row for diagnostics and retraining."""
    family_payload = {
        family: dict(payload)
        for family, payload in _normalize_family_predictions(family_preds).items()
    }
    if isinstance(family_preds, Mapping):
        for meta_key in ("__missing_families__", "__attempted_families__"):
            if meta_key in family_preds:
                family_payload[str(meta_key)] = list(family_preds.get(meta_key) or [])
    weights_payload = dict(weights or {})

    def _write(con) -> None:
        con.execute(
            """
            INSERT INTO ensemble_predictions(
              symbol, ts, blended_prediction, family_preds_json, weights_json, agreement
            )
            VALUES (?,?,?,?,?,?)
            """,
            (
                str(symbol or "").upper().strip(),
                int(_safe_int(ts, 0)),
                float(_safe_float(blended_prediction, 0.0)),
                _json_dumps(family_payload),
                _json_dumps(weights_payload),
                float(max(0.0, min(1.0, _safe_float(agreement, 0.0)))),
            ),
        )

    run_write_txn(
        _write,
        table="ensemble_predictions",
        operation="persist_ensemble_prediction",
        context={"symbol": str(symbol or "").upper().strip(), "ts": int(_safe_int(ts, 0))},
    )


def persist_blend_weights(
    *,
    mode: str,
    regime: Optional[str],
    weights: Mapping[str, Any],
    meta_blob: Optional[bytes] = None,
    force: bool = False,
) -> None:
    """Persist the latest ensemble blend weights and optional stacked model blob."""
    mode_name = str(mode or "").strip().lower()
    weight_map = {
        key: float(_safe_float(value, 0.0))
        for key, value in dict(weights or {}).items()
        if str(key or "").strip() and not str(key).startswith("__")
    }
    if not weight_map:
        return
    weight_signature = _json_dumps(weight_map)
    regime_value = str(regime).strip() if str(regime or "").strip() else None
    now_ts = int(time.time() * 1000)
    meta_artifact_sha256 = ""
    meta_artifact_alias = ""
    if meta_blob:
        meta_artifact_alias = _ensemble_meta_artifact_alias(mode_name, regime_value)
        ref = LocalArtifactStore().put(
            bytes(meta_blob),
            content_type="application/python-pickle",
            kind="model",
            alias=meta_artifact_alias,
            metadata={
                "model_name": "ensemble_blender",
                "mode": str(mode_name),
                "regime": str(regime_value or "global"),
                "created_ts": int(now_ts),
                "weights": dict(weight_map),
            },
        )
        meta_artifact_sha256 = ref.sha256
    if not force:
        con = connect(readonly=True)
        try:
            params: List[Any] = [str(mode_name)]
            sql = """
                SELECT created_ts, weights_json
                FROM ensemble_blend_weights
                WHERE mode=?
            """
            if regime_value is None:
                sql += " AND regime IS NULL"
            else:
                sql += " AND regime=?"
                params.append(str(regime_value))
            sql += " ORDER BY created_ts DESC, id DESC LIMIT 1"
            row = con.execute(sql, tuple(params)).fetchone()
        except Exception as exc:
            _warn_nonfatal(
                "ENSEMBLE_PERSIST_WEIGHTS_LOOKUP_FAILED",
                exc,
                once_key=f"ensemble_persist_weights_lookup_failed:{mode_name}:{regime_value or 'global'}",
                mode=str(mode_name),
                regime=regime_value or "",
            )
            row = None
        finally:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "ENSEMBLE_PERSIST_WEIGHTS_CLOSE_FAILED",
                    exc,
                    once_key="ensemble_persist_weights_close_failed",
                )
        if row:
            last_ts = int(_safe_int(row[0], 0))
            last_signature = str(row[1] or "").strip()
            if last_signature == weight_signature and (now_ts - last_ts) < int(_CURRENT_WEIGHT_PERSIST_MIN_S * 1000.0):
                return

    def _write(con) -> None:
        _ensure_ensemble_artifact_columns(con)
        con.execute(
            """
            INSERT INTO ensemble_blend_weights(
              created_ts, mode, regime, weights_json, meta_blob, meta_artifact_sha256, meta_artifact_alias
            )
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                int(now_ts),
                str(mode_name),
                regime_value,
                str(weight_signature),
                None,
                meta_artifact_sha256 or None,
                meta_artifact_alias or None,
            ),
        )

    run_write_txn(
        _write,
        table="ensemble_blend_weights",
        operation="persist_blend_weights",
        context={"mode": str(mode_name), "regime": regime_value or ""},
    )


def persist_family_performance(
    *,
    window_start_ts: int,
    window_end_ts: int,
    rows: Iterable[Mapping[str, Any]],
) -> int:
    """Persist realized family performance summary rows for ensemble monitoring."""
    payloads = [
        {
            "family": str((row or {}).get("family") or "").strip(),
            "n_predictions": int(_safe_int((row or {}).get("n_predictions"), 0)),
            "realized_sharpe": _safe_float((row or {}).get("realized_sharpe"), 0.0),
            "hit_rate": _safe_float((row or {}).get("hit_rate"), 0.0),
        }
        for row in list(rows or [])
        if str((row or {}).get("family") or "").strip()
    ]
    if not payloads:
        return 0

    def _write(con) -> int:
        inserted = 0
        for row in payloads:
            con.execute(
                """
                INSERT INTO ensemble_family_performance(
                  window_start_ts, window_end_ts, family, n_predictions, realized_sharpe, hit_rate
                )
                VALUES (?,?,?,?,?,?)
                """,
                (
                    int(_safe_int(window_start_ts, 0)),
                    int(_safe_int(window_end_ts, 0)),
                    str(row["family"]),
                    int(row["n_predictions"]),
                    float(row["realized_sharpe"]),
                    float(row["hit_rate"]),
                ),
            )
            inserted += 1
        return int(inserted)

    return int(
        run_write_txn(
            _write,
            table="ensemble_family_performance",
            operation="persist_family_performance",
            context={"window_start_ts": int(_safe_int(window_start_ts, 0)), "window_end_ts": int(_safe_int(window_end_ts, 0))},
        )
        or 0
    )
