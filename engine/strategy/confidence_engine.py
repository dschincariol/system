"""
FILE: confidence_engine.py

Shared confidence + uncertainty helpers for prediction calibration, strength
tracking, and stale-signal decay.
"""

from __future__ import annotations

import json
import math
import os
import time
import logging
from typing import Any, Dict, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect

_CALIB_CACHE_TTL_S = float(os.environ.get("CONF_ENGINE_CALIB_CACHE_TTL_S", "60"))
LOG = logging.getLogger("confidence_engine")

SIGNAL_CONF_DECAY_ENABLED = os.environ.get("SIGNAL_CONF_DECAY_ENABLED", "1") == "1"
SIGNAL_CONF_DECAY_START_FRAC = float(os.environ.get("SIGNAL_CONF_DECAY_START_FRAC", "1.0"))
SIGNAL_CONF_DECAY_HALF_LIFE_FRAC = float(os.environ.get("SIGNAL_CONF_DECAY_HALF_LIFE_FRAC", "1.0"))
SIGNAL_CONF_DECAY_MIN_MULT = float(os.environ.get("SIGNAL_CONF_DECAY_MIN_MULT", "0.25"))
SIGNAL_CONF_DECAY_MIN_START_S = float(os.environ.get("SIGNAL_CONF_DECAY_MIN_START_S", "60"))
SIGNAL_CONF_DECAY_MIN_HALF_LIFE_S = float(os.environ.get("SIGNAL_CONF_DECAY_MIN_HALF_LIFE_S", "60"))

PREDICTION_STRENGTH_Z_REF = float(os.environ.get("PREDICTION_STRENGTH_Z_REF", "2.0"))
CONFIDENCE_SIZE_Z_REF = float(os.environ.get("CONFIDENCE_SIZE_Z_REF", str(PREDICTION_STRENGTH_Z_REF)))

_calib_cache: Dict[str, Any] = {
    "rows": {},
}
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.confidence_engine",
        extra=extra,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _clip01(value: Any) -> float:
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal("confidence_engine_clip01_failed", e, once_key="clip01", value=repr(value)[:120])
        return 0.0
    if not math.isfinite(out):
        return 0.0
    return max(0.0, min(1.0, out))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal("confidence_engine_safe_int_failed", e, once_key="safe_int", value=repr(value)[:120])
        return int(default)


def _calibration_cache_key(symbol: str, horizon_s: int) -> Tuple[str, int]:
    return str(symbol or "").upper().strip(), int(horizon_s or 0)


def _table_exists(con, table_name: str) -> bool:
    try:
        row = con.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type='table' AND name=?
            LIMIT 1
            """,
            (str(table_name),),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "confidence_engine_table_exists_failed",
            e,
            once_key=f"table_exists:{table_name}",
            table_name=str(table_name),
        )
        return False
    return bool(row)


def _load_calibration_payload(
    con,
    *,
    symbol: str,
    horizon_s: int,
) -> Optional[Dict[str, Any]]:
    key = _calibration_cache_key(symbol, horizon_s)
    now_s = time.time()
    rows_cache = dict(_calib_cache.get("rows") or {})
    cached = rows_cache.get(key)
    if cached is not None and (now_s - float(cached.get("ts_s") or 0.0)) < float(_CALIB_CACHE_TTL_S):
        payload = cached.get("payload")
        if payload is not None:
            return payload
    star_key = _calibration_cache_key("*", horizon_s)
    cached_star = rows_cache.get(star_key)
    if cached_star is not None and (now_s - float(cached_star.get("ts_s") or 0.0)) < float(_CALIB_CACHE_TTL_S):
        return cached_star.get("payload")

    if not _table_exists(con, "confidence_calibration"):
        return None

    for sym in (str(symbol or "").upper().strip(), "*"):
        if not sym:
            continue
        row = con.execute(
            """
            SELECT symbol, horizon_s, method, updated_ts_ms, payload_json
            FROM confidence_calibration
            WHERE symbol=? AND horizon_s=?
            LIMIT 1
            """,
            (str(sym), int(horizon_s)),
        ).fetchone()
        payload = None
        if row and row[4]:
            try:
                parsed = json.loads(row[4] or "{}")
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                payload = {
                    "symbol": str(row[0] or sym),
                    "horizon_s": int(row[1] or horizon_s),
                    "method": str(row[2] or "identity"),
                    "updated_ts_ms": int(row[3] or 0),
                    "payload": parsed,
                }
        rows_cache[_calibration_cache_key(sym, horizon_s)] = {
            "ts_s": float(now_s),
            "payload": payload,
        }

    _calib_cache["rows"] = rows_cache
    got = rows_cache.get(key) or rows_cache.get(star_key)
    return got.get("payload") if isinstance(got, dict) else None


def calibrate_confidence_score(
    *,
    symbol: str,
    horizon_s: int,
    confidence_raw: float,
    con=None,
) -> Tuple[float, Dict[str, Any]]:
    raw = _clip01(confidence_raw)
    close_con = False
    if con is None:
        con = connect()
        close_con = True

    try:
        calib = _load_calibration_payload(con, symbol=str(symbol), horizon_s=int(horizon_s))
    except Exception as e:
        _warn_nonfatal(
            "confidence_engine_calibration_load_failed",
            e,
            symbol=str(symbol),
            horizon_s=int(horizon_s),
        )
        calib = None
    finally:
        if close_con:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("confidence_engine_db_close_failed", e)

    if not calib:
        return raw, {
            "method": "identity",
            "source_symbol": None,
            "updated_ts_ms": 0,
            "raw_confidence": float(raw),
            "calibrated_confidence": float(raw),
        }

    payload = dict(calib.get("payload") or {})
    bins = list(payload.get("bin_stats") or [])
    edges = list(payload.get("edges") or [])
    bucket_idx = 0
    n_bins = max(1, len(bins))

    if len(edges) >= 2:
        bucket_idx = min(n_bins - 1, max(0, int(raw * max(1, len(edges) - 1))))
    elif n_bins > 1:
        bucket_idx = min(n_bins - 1, max(0, int(raw * n_bins)))

    bucket = bins[bucket_idx] if bucket_idx < len(bins) else {}
    calibrated = _clip01(bucket.get("win_rate", raw))

    return calibrated, {
        "method": str(calib.get("method") or "binning_v1"),
        "source_symbol": str(calib.get("symbol") or ""),
        "updated_ts_ms": int(calib.get("updated_ts_ms") or 0),
        "raw_confidence": float(raw),
        "calibrated_confidence": float(calibrated),
        "bucket_idx": int(bucket_idx),
        "bucket_n": int(bucket.get("n") or 0),
        "bucket_win_rate": _clip01(bucket.get("win_rate", calibrated)),
    }


def compute_prediction_strength(expected_z: float, confidence: float) -> float:
    z_ref = max(1e-6, float(PREDICTION_STRENGTH_Z_REF))
    z_mag = abs(float(expected_z))
    conf = _clip01(confidence)
    strength = math.tanh(z_mag / z_ref) * conf
    return _clip01(strength)


def confidence_size_multiplier(expected_z: float, confidence: float) -> float:
    z_ref = max(1e-6, float(CONFIDENCE_SIZE_Z_REF))
    raw = (abs(float(expected_z)) / z_ref) * _clip01(confidence)
    return _clip01(raw)


def apply_signal_decay(
    *,
    expected_z: float,
    confidence: float,
    signal_ts_ms: Optional[int],
    horizon_s: int,
    now_ms: Optional[int] = None,
) -> Dict[str, Any]:
    base_conf = _clip01(confidence)
    ts_ms = _safe_int(signal_ts_ms, 0)
    current_ms = _safe_int(now_ms, int(time.time() * 1000))
    h_s = max(1, _safe_int(horizon_s, 1))

    decay_mult = 1.0
    age_s = 0.0
    start_s = max(float(SIGNAL_CONF_DECAY_MIN_START_S), float(h_s) * float(SIGNAL_CONF_DECAY_START_FRAC))
    half_life_s = max(
        float(SIGNAL_CONF_DECAY_MIN_HALF_LIFE_S),
        float(h_s) * float(SIGNAL_CONF_DECAY_HALF_LIFE_FRAC),
    )

    if SIGNAL_CONF_DECAY_ENABLED and ts_ms > 0:
        age_s = max(0.0, (int(current_ms) - int(ts_ms)) / 1000.0)
        if age_s > start_s:
            decay_age = (float(age_s) - float(start_s)) / max(1e-6, float(half_life_s))
            decay_mult = math.exp(-math.log(2.0) * decay_age)
            decay_mult = max(float(SIGNAL_CONF_DECAY_MIN_MULT), min(1.0, float(decay_mult)))

    decayed_conf = _clip01(base_conf * decay_mult)
    decayed_strength = compute_prediction_strength(float(expected_z), decayed_conf)
    size_mult = confidence_size_multiplier(float(expected_z), decayed_conf)

    return {
        "enabled": bool(SIGNAL_CONF_DECAY_ENABLED),
        "signal_ts_ms": int(ts_ms) if ts_ms > 0 else None,
        "now_ts_ms": int(current_ms),
        "signal_age_s": float(age_s),
        "start_s": float(start_s),
        "half_life_s": float(half_life_s),
        "multiplier": float(decay_mult),
        "confidence": float(decayed_conf),
        "probability": float(decayed_conf),
        "uncertainty": float(1.0 - decayed_conf),
        "prediction_strength": float(decayed_strength),
        "size_mult": float(size_mult),
    }


def describe_signal_confidence(
    *,
    expected_z: float,
    confidence: float,
    horizon_s: int,
    raw_confidence: Optional[float] = None,
    calibration: Optional[Dict[str, Any]] = None,
    signal_ts_ms: Optional[int] = None,
    now_ms: Optional[int] = None,
    apply_decay: bool = False,
) -> Dict[str, Any]:
    final_conf = _clip01(confidence)
    payload: Dict[str, Any] = {
        "raw_confidence": float(_clip01(raw_confidence if raw_confidence is not None else confidence)),
        "confidence": float(final_conf),
        "probability": float(final_conf),
        "uncertainty": float(1.0 - final_conf),
        "prediction_strength": float(compute_prediction_strength(float(expected_z), float(final_conf))),
        "size_mult": float(confidence_size_multiplier(float(expected_z), float(final_conf))),
        "signal_ts_ms": (_safe_int(signal_ts_ms, 0) or None),
        "horizon_s": int(horizon_s),
        "calibration": dict(calibration or {}),
    }

    if apply_decay:
        payload["decay"] = apply_signal_decay(
            expected_z=float(expected_z),
            confidence=float(final_conf),
            signal_ts_ms=signal_ts_ms,
            horizon_s=int(horizon_s),
            now_ms=now_ms,
        )
        payload["confidence"] = float(payload["decay"]["confidence"])
        payload["probability"] = float(payload["decay"]["probability"])
        payload["uncertainty"] = float(payload["decay"]["uncertainty"])
        payload["prediction_strength"] = float(payload["decay"]["prediction_strength"])
        payload["size_mult"] = float(payload["decay"]["size_mult"])

    return payload


def apply_confidence_payload(explain: Optional[Dict[str, Any]], payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(explain or {})
    engine_blob = dict(out.get("confidence_engine") or {})
    engine_blob.update(dict(payload or {}))
    out["confidence_engine"] = engine_blob
    out["confidence"] = float(payload.get("confidence") or 0.0)
    out["probability"] = float(payload.get("probability") or 0.0)
    out["uncertainty"] = float(payload.get("uncertainty") or 0.0)
    out["prediction_strength"] = float(payload.get("prediction_strength") or 0.0)
    out["signal_confidence"] = float(payload.get("confidence") or 0.0)
    out["signal_probability"] = float(payload.get("probability") or 0.0)
    out["signal_uncertainty"] = float(payload.get("uncertainty") or 0.0)
    out["size_mult"] = float(payload.get("size_mult") or 0.0)
    if payload.get("signal_ts_ms") is not None:
        out["signal_ts_ms"] = _safe_int(payload.get("signal_ts_ms"), 0)
    return out
