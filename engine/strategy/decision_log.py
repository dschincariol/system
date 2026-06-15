"""
FILE: decision_log.py

Persists model decisions and emits matching runtime events. This is the main
write path for auditable prediction metadata before execution happens.
"""

import json
import time
import hashlib
import os
import logging
from typing import Optional, Dict, Any, Sequence

import numpy as np

from engine.audit.chain import append_chain_row
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import init_db, run_write_txn
from engine.runtime.event_log import append_event

# ------            -- ------------------------------------------------------
# Hash helpers
# ------            -- ------------------------------------------------------
def _sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def hash_feature_vector(vec: Optional[Sequence[float]]) -> Optional[str]:
    # Hashing the vector keeps logs compact while still allowing change
    # detection across model versions and feature set revisions.
    if vec is None:
        return None
    try:
        arr = np.asarray(vec, dtype=np.float32)
        return _sha256_bytes(arr.tobytes())
    except Exception as e:
        _warn_nonfatal("DECISION_LOG_HASH_FEATURE_VECTOR_FAILED", e, once_key="hash_feature_vector")
        return None


# ------            -- ------------------------------------------------------
# Logging
# ------            -- ------------------------------------------------------
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [decision_log] %(message)s",
)
LOG = get_logger("strategy.decision_log")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_decision_log_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.decision_log",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _payload_feature_set_tag(
    *,
    explicit: Optional[str],
    features_json: Optional[Dict[str, Any]],
    explain_json: Optional[Dict[str, Any]],
    extra_json: Optional[Dict[str, Any]],
) -> Optional[str]:
    tag = str(explicit or "").strip()
    if tag:
        return tag

    for payload in (features_json, explain_json, extra_json):
        if not isinstance(payload, dict):
            continue
        tag = str(payload.get("feature_set_tag") or "").strip()
        if not tag:
            schema = payload.get("feature_schema")
            if isinstance(schema, dict):
                tag = str(schema.get("feature_set_tag") or "").strip()
        if tag:
            return tag

    feature_ids: list[str] = []
    for payload in (explain_json, features_json):
        if not isinstance(payload, dict):
            continue
        raw_ids = payload.get("feature_ids")
        if raw_ids is None:
            schema = payload.get("feature_schema")
            raw_ids = schema.get("feature_ids") if isinstance(schema, dict) else None
        if isinstance(raw_ids, (list, tuple)):
            feature_ids = [str(value) for value in raw_ids if str(value or "").strip()]
        if feature_ids:
            break
    if not feature_ids:
        return None
    try:
        from engine.strategy.feature_registry import feature_set_tag_from_ids

        tag = str(feature_set_tag_from_ids(feature_ids) or "").strip()
        return tag or None
    except Exception as e:
        _warn_nonfatal(
            "DECISION_LOG_FEATURE_SET_TAG_DERIVE_FAILED",
            e,
            once_key="feature_set_tag_derive",
        )
        return None


# ------            -- ------------------------------------------------------
# Write API
# ------            -- ------------------------------------------------------
def log_decision(
    *,
    event_id: int,
    symbol: str,
    horizon_s: int,
    predicted_z: float,
    confidence: float,
    model_name: str,
    model_kind: Optional[str] = None,
    model_ts_ms: Optional[int] = None,
    model_version: Optional[str] = None,
    features_hash: Optional[str] = None,
    feature_set_tag: Optional[str] = None,
    features_json: Optional[Dict[str, Any]] = None,
    explain_json: Optional[Dict[str, Any]] = None,
    extra_json: Optional[Dict[str, Any]] = None,
    ensemble_components: Optional[Dict[str, Any]] = None,
    ensemble_weights: Optional[Dict[str, Any]] = None,
    component_vector: Optional[Dict[str, Any]] = None,
    ts_ms: Optional[int] = None,
    con=None,
) -> None:
    if con is None:
        init_db()

    now_ms = int(ts_ms if ts_ms is not None else time.time() * 1000)
    explain_payload = dict(explain_json or {})
    extra_payload = dict(extra_json or {})
    if ensemble_components is not None:
        explain_payload["ensemble_components"] = dict(ensemble_components or {})
        extra_payload["ensemble_components"] = dict(ensemble_components or {})
    if ensemble_weights is not None:
        explain_payload["ensemble_weights"] = dict(ensemble_weights or {})
        extra_payload["ensemble_weights"] = dict(ensemble_weights or {})
    if component_vector is not None:
        explain_payload["component_vector"] = dict(component_vector or {})
        extra_payload["component_vector"] = dict(component_vector or {})
    components_payload: Optional[Dict[str, Any]] = component_vector
    if components_payload is None and isinstance(explain_payload.get("component_vector"), dict):
        components_payload = dict(explain_payload.get("component_vector") or {})
    if components_payload is None and ensemble_components is not None:
        components_payload = {"components": dict(ensemble_components or {})}
        if ensemble_weights is not None:
            components_payload["weights"] = dict(ensemble_weights or {})
    feature_set_tag_value = _payload_feature_set_tag(
        explicit=feature_set_tag,
        features_json=features_json,
        explain_json=explain_payload,
        extra_json=extra_payload,
    )
    if not explain_payload and explain_json is None:
        explain_payload = None
    if not extra_payload and extra_json is None:
        extra_payload = None

    def _dump(x):
        # Bound payload size so one oversized explanation blob does not bloat
        # the decision log row indefinitely.
        if x is None:
            return None
        try:
            s = json.dumps(x, ensure_ascii=False)
            return s if len(s) <= 65536 else s[:65536]
        except Exception as e:
            _warn_nonfatal(
                "DECISION_LOG_DUMP_PAYLOAD_FAILED",
                e,
                once_key="dump_payload",
                payload_type=type(x).__name__,
            )
            return None

    def _write(con):
        try:
            decision_log_columns = {
                str(row[1])
                for row in (con.execute("PRAGMA table_info(decision_log)").fetchall() or [])
            }
        except Exception:
            decision_log_columns = set()
        row_payload = {
            "ts_ms": int(now_ms),
            "event_id": int(event_id),
            "symbol": str(symbol),
            "horizon_s": int(horizon_s),
            "predicted_z": float(predicted_z),
            "confidence": float(confidence),
            "model_name": str(model_name),
            "model_kind": (str(model_kind) if model_kind is not None else None),
            "model_ts_ms": (int(model_ts_ms) if model_ts_ms is not None else None),
            "model_version": (str(model_version) if model_version is not None else None),
            "features_hash": (str(features_hash) if features_hash is not None else None),
            "feature_set_tag": feature_set_tag_value,
            "features_json": _dump(features_json),
            "explain_json": _dump(explain_payload),
            "extra_json": _dump(extra_payload),
            "components_json": _dump(components_payload),
        }
        if not decision_log_columns or "component_vector" in decision_log_columns:
            row_payload["component_vector"] = _dump(components_payload)
        append_chain_row(
            "decision_log",
            row_payload,
            con,
        )

        append_event(
            event_type="decision",
            event_source="engine.strategy.decision_log",
            entity_type="symbol",
            entity_id=str(symbol),
            correlation_id=str(event_id),
            payload={
                "event_id": int(event_id),
                "symbol": str(symbol),
                "horizon_s": int(horizon_s),
                "predicted_z": float(predicted_z),
                "confidence": float(confidence),
                "model_name": str(model_name),
                "model_kind": (str(model_kind) if model_kind is not None else None),
                "model_ts_ms": (int(model_ts_ms) if model_ts_ms is not None else None),
                "model_version": (str(model_version) if model_version is not None else None),
                "features_hash": (str(features_hash) if features_hash is not None else None),
                "feature_set_tag": feature_set_tag_value,
                "features_json": (features_json or None),
                "explain_json": (explain_payload or None),
                "extra_json": (extra_payload or None),
                "components_json": (components_payload or None),
                "component_vector": (components_payload or None),
            },
            ts_ms=int(now_ms),
            con=con,
        )

    if con is not None:
        _write(con)
        return

    run_write_txn(_write)
