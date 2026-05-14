"""Hidden Markov regime model helpers for training, inference, and weighting."""

from __future__ import annotations

import json
import logging
import math
import os
import pickle
import threading
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from engine.artifacts.serialization import dumps_pickle_artifact
from engine.artifacts.store import LocalArtifactStore
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect, init_db, run_write_txn

try:
    from hmmlearn.hmm import GaussianHMM as _GaussianHMM
except Exception:
    _GaussianHMM = None


LOG = logging.getLogger("engine.strategy.hmm_regime")
_WARNED_NONFATAL_KEYS: set[str] = set()
_SCHEMA_READY = False
_SCHEMA_LOCK = threading.Lock()
_MODEL_CACHE_LOCK = threading.Lock()
_MODEL_CACHE: Dict[str, Any] = {"ts_s": 0.0, "key": "", "model": None}

HMM_MAX_STATES = 5
DEFAULT_HMM_FEATURE_NAMES = [
    "macro.risk_off",
    "macro.vol_expansion",
    "macro.credit_stress",
    "macro.drawdown_shift",
    "micro.vol_clustered",
    "micro.liquidity_thin",
]
HMM_LABEL_FEATURE_NAMES = [
    "RISK_ON",
    "RECOVERY",
    "NEUTRAL",
    "VOLATILE",
    "RISK_OFF",
]
HMM_FEATURE_IDS = [
    "hmm_regime.enabled",
    "hmm_regime.model_available",
    "hmm_regime.confidence",
    "hmm_regime.entropy",
    "hmm_regime.state_0_prob",
    "hmm_regime.state_1_prob",
    "hmm_regime.state_2_prob",
    "hmm_regime.state_3_prob",
    "hmm_regime.state_4_prob",
    "hmm_regime.label_risk_on_prob",
    "hmm_regime.label_recovery_prob",
    "hmm_regime.label_neutral_prob",
    "hmm_regime.label_volatile_prob",
    "hmm_regime.label_risk_off_prob",
]
_HMM_SCHEMA = """
CREATE TABLE IF NOT EXISTS hmm_regime_models (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_ts_ms INTEGER NOT NULL,
  symbol TEXT NOT NULL DEFAULT 'SPY',
  num_states INTEGER NOT NULL,
  feature_names_json TEXT NOT NULL,
  label_map_json TEXT NOT NULL,
  metrics_json TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  model_blob BLOB,
  artifact_sha256 TEXT,
  artifact_alias TEXT
);
CREATE INDEX IF NOT EXISTS idx_hmm_regime_models_symbol_active_created
  ON hmm_regime_models(symbol, is_active, created_ts_ms DESC);
"""


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
        component="engine.strategy.hmm_regime",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def hmm_regime_enabled() -> bool:
    """Return whether the HMM regime model is enabled for the runtime."""
    return bool(_env_flag("HMM_REGIME_ENABLED", False))


def hmm_ensemble_weight_enabled() -> bool:
    """Return whether HMM regime labels should adjust ensemble family weights."""
    return bool(_env_flag("HMM_REGIME_ENSEMBLE_WEIGHT_ENABLED", False))


def hmm_num_states() -> int:
    """Return the configured HMM state count clamped to the supported range."""
    try:
        value = int(str(os.environ.get("HMM_NUM_STATES", "3") or "3").strip())
    except Exception:
        value = 3
    return int(max(3, min(HMM_MAX_STATES, value)))


def hmm_model_symbol() -> str:
    """Return the symbol used when training and loading the shared HMM model."""
    return str(os.environ.get("HMM_REGIME_MODEL_SYMBOL", "SPY") or "SPY").upper().strip() or "SPY"


def hmm_model_cache_ttl_s() -> float:
    """Return the in-memory HMM model cache TTL in seconds."""
    try:
        value = float(os.environ.get("HMM_REGIME_MODEL_CACHE_TTL_S", "15") or 15.0)
    except Exception:
        value = 15.0
    return float(max(0.0, value))


def hmm_feature_ids() -> List[str]:
    """Return the exported HMM-derived feature ids for downstream model inputs."""
    return list(HMM_FEATURE_IDS)


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
    try:
        value = json.loads(payload.decode("utf-8", errors="replace") if isinstance(payload, (bytes, bytearray)) else str(payload))
    except Exception:
        return default
    if default is None:
        return value
    return value if isinstance(value, type(default)) else default


def _normalize_label_map(label_map: Mapping[Any, Any] | None, *, num_states: int) -> Dict[int, str]:
    out: Dict[int, str] = {}
    for idx in range(max(0, int(num_states))):
        try:
            label = str((label_map or {}).get(idx) or (label_map or {}).get(str(idx)) or "").strip().upper()
        except Exception:
            label = ""
        if not label:
            label = f"STATE_{idx}"
        out[int(idx)] = str(label)
    return out


def _empty_signal(*, enabled: bool | None = None, num_states: int | None = None) -> Dict[str, Any]:
    state_count = int(max(0, num_states if num_states is not None else hmm_num_states()))
    state_probs = {f"state_{idx}": 0.0 for idx in range(HMM_MAX_STATES)}
    return {
        "enabled": bool(hmm_regime_enabled() if enabled is None else enabled),
        "model_available": False,
        "backend": "",
        "num_states": int(state_count),
        "state_probabilities": state_probs,
        "label_probabilities": {label: 0.0 for label in HMM_LABEL_FEATURE_NAMES},
        "most_likely_state": None,
        "regime_label": "UNKNOWN",
        "confidence": 0.0,
        "entropy": 1.0 if state_count > 1 else 0.0,
    }


def _coerce_feature_matrix(
    features: Any,
    *,
    expected_feature_names: Optional[Sequence[str]] = None,
) -> Tuple[np.ndarray, List[str]]:
    feature_names = list(expected_feature_names or [])
    if isinstance(features, np.ndarray):
        arr = np.asarray(features, dtype=np.float64)
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)
        if arr.ndim != 2:
            raise ValueError("expected_2d_feature_matrix")
        if feature_names and arr.shape[1] != len(feature_names):
            raise ValueError("feature_name_dimension_mismatch")
        if not feature_names:
            feature_names = [f"feature_{idx}" for idx in range(int(arr.shape[1]))]
        return np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0), list(feature_names)

    rows = list(features if isinstance(features, (list, tuple)) else [features]) if features is not None else []
    if not rows:
        empty_names = list(feature_names or DEFAULT_HMM_FEATURE_NAMES)
        return np.zeros((0, len(empty_names)), dtype=np.float64), empty_names

    if all(isinstance(row, Mapping) for row in rows):
        if not feature_names:
            feature_names = list(DEFAULT_HMM_FEATURE_NAMES)
            seen = set(feature_names)
            for row in rows:
                for key in list((row or {}).keys()):
                    fid = str(key or "").strip()
                    if fid and fid not in seen:
                        feature_names.append(fid)
                        seen.add(fid)
        arr = np.asarray(
            [
                [_safe_float(dict(row or {}).get(name), 0.0) for name in feature_names]
                for row in rows
            ],
            dtype=np.float64,
        )
        return np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0), list(feature_names)

    arr = np.asarray(rows, dtype=np.float64)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    if arr.ndim != 2:
        raise ValueError("expected_numeric_feature_rows")
    if feature_names and arr.shape[1] != len(feature_names):
        raise ValueError("feature_name_dimension_mismatch")
    if not feature_names:
        feature_names = [f"feature_{idx}" for idx in range(int(arr.shape[1]))]
    return np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0), list(feature_names)


def _normalize_probabilities(values: Iterable[float]) -> List[float]:
    cleaned = [max(0.0, _safe_float(value, 0.0)) for value in list(values or [])]
    total = float(sum(cleaned))
    if total <= 0.0:
        return [0.0 for _ in cleaned]
    return [float(value / total) for value in cleaned]


def _normalized_entropy(probabilities: Sequence[float]) -> float:
    probs = [max(0.0, _safe_float(value, 0.0)) for value in list(probabilities or [])]
    total = float(sum(probs))
    if total <= 0.0:
        return 1.0
    normalized = [float(value / total) for value in probs if value > 0.0]
    if len(normalized) <= 1:
        return 0.0
    entropy = -sum(float(value) * math.log(float(value)) for value in normalized)
    return float(max(0.0, min(1.0, entropy / math.log(len(normalized)))))


def init_hmm_regime_schema() -> None:
    """Create the persisted HMM model table and indexes if they are missing."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    with _SCHEMA_LOCK:
        if _SCHEMA_READY:
            return
        init_db()

        def _write(con) -> None:
            con.executescript(_HMM_SCHEMA)
            _ensure_hmm_artifact_columns(con)

        run_write_txn(
            _write,
            table="hmm_regime_models",
            operation="init_hmm_regime_schema",
        )
        _SCHEMA_READY = True


def _ensure_hmm_artifact_columns(con) -> None:
    for sql in (
        "ALTER TABLE hmm_regime_models ADD COLUMN IF NOT EXISTS artifact_sha256 TEXT",
        "ALTER TABLE hmm_regime_models ADD COLUMN IF NOT EXISTS artifact_alias TEXT",
    ):
        con.execute(sql)


def _hmm_artifact_alias(symbol: str) -> str:
    return f"model:hmm_regime:{str(symbol or hmm_model_symbol()).upper().strip() or hmm_model_symbol()}:current"


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


def _state_feature_means_from_model(
    estimator: Any,
    *,
    feature_mean: np.ndarray,
    feature_std: np.ndarray,
) -> np.ndarray:
    means = np.asarray(getattr(estimator, "means_", []), dtype=np.float64)
    if means.ndim == 1:
        means = means.reshape(1, -1)
    if means.size <= 0:
        return np.zeros((0, int(feature_mean.shape[0])), dtype=np.float64)
    if means.shape[1] != int(feature_mean.shape[0]):
        return np.zeros((int(means.shape[0]), int(feature_mean.shape[0])), dtype=np.float64)
    return np.asarray((means * feature_std) + feature_mean, dtype=np.float64)


def map_states_to_regime_labels(
    model: Mapping[str, Any] | None = None,
    *,
    state_feature_means: Any | None = None,
    feature_names: Optional[Sequence[str]] = None,
) -> Dict[int, str]:
    """Assign ordered regime labels to HMM states from their feature-level stress scores."""
    if state_feature_means is None and isinstance(model, Mapping):
        state_feature_means = model.get("state_feature_means")
        if feature_names is None:
            feature_names = list(model.get("feature_names") or [])
    names = list(feature_names or DEFAULT_HMM_FEATURE_NAMES)
    matrix = np.asarray([] if state_feature_means is None else state_feature_means, dtype=np.float64)
    if matrix.ndim == 1 and matrix.size > 0:
        matrix = matrix.reshape(1, -1)
    if matrix.ndim != 2 or matrix.size <= 0:
        return {}

    stress_weights = {
        "macro.risk_off": 1.15,
        "macro.vol_expansion": 1.0,
        "macro.credit_stress": 1.0,
        "macro.drawdown_shift": 0.90,
        "micro.vol_clustered": 0.90,
        "micro.liquidity_thin": 0.85,
    }
    scores: List[Tuple[int, float]] = []
    for idx in range(int(matrix.shape[0])):
        score = 0.0
        weight_total = 0.0
        for col_idx, name in enumerate(names):
            weight = float(stress_weights.get(str(name), 0.25))
            score += float(weight * _safe_float(matrix[idx, col_idx], 0.0))
            weight_total += float(weight)
        normalized_score = float(score / max(1e-9, weight_total))
        scores.append((int(idx), normalized_score))

    ordered = [idx for idx, _score in sorted(scores, key=lambda item: item[1])]
    label_sequence = {
        1: ["NEUTRAL"],
        2: ["RISK_ON", "RISK_OFF"],
        3: ["RISK_ON", "NEUTRAL", "RISK_OFF"],
        4: ["RISK_ON", "RECOVERY", "VOLATILE", "RISK_OFF"],
        5: ["RISK_ON", "RECOVERY", "NEUTRAL", "VOLATILE", "RISK_OFF"],
    }.get(len(ordered), [f"STATE_{idx}" for idx in range(len(ordered))])
    return {int(state_idx): str(label_sequence[position]) for position, state_idx in enumerate(ordered)}


def train_hmm(features: Any) -> Dict[str, Any]:
    """Train an HMM regime model from normalized feature rows."""
    enabled = hmm_regime_enabled()
    feature_matrix, feature_names = _coerce_feature_matrix(features, expected_feature_names=DEFAULT_HMM_FEATURE_NAMES)
    num_states = int(hmm_num_states())

    unavailable = {
        "available": False,
        "enabled": bool(enabled),
        "backend": "",
        "num_states": int(num_states),
        "feature_names": list(feature_names),
        "label_map": {},
        "state_feature_means": [],
        "trained_ts_ms": int(time.time() * 1000),
        "metrics": {"train_rows": int(feature_matrix.shape[0]), "reason": ""},
    }

    if _GaussianHMM is None:
        unavailable["metrics"]["reason"] = "dependency_unavailable"
        return unavailable

    if feature_matrix.ndim != 2 or int(feature_matrix.shape[0]) < max(24, num_states * 8):
        unavailable["metrics"]["reason"] = "insufficient_rows"
        return unavailable

    feature_mean = np.asarray(np.mean(feature_matrix, axis=0), dtype=np.float64)
    feature_std = np.asarray(np.std(feature_matrix, axis=0), dtype=np.float64)
    feature_std = np.where(feature_std < 1e-6, 1.0, feature_std)
    normalized = np.asarray((feature_matrix - feature_mean) / feature_std, dtype=np.float64)

    best_model = None
    best_score = float("-inf")
    max_iter = max(50, _safe_int(os.environ.get("HMM_TRAIN_MAX_ITER"), 200))
    for seed in (7, 19, 37):
        try:
            estimator = _GaussianHMM(
                n_components=int(num_states),
                covariance_type="diag",
                min_covar=1e-3,
                n_iter=int(max_iter),
                random_state=int(seed),
            )
            estimator.fit(normalized)
            score = float(_safe_float(estimator.score(normalized), float("-inf")))
            if math.isfinite(score) and score > best_score:
                best_model = estimator
                best_score = score
        except Exception as e:
            _warn_nonfatal(
                "hmm_regime_train_attempt_failed",
                "HMM_REGIME_TRAIN_ATTEMPT_FAILED",
                e,
                warn_key=f"hmm_regime_train_attempt_failed:{seed}:{num_states}",
                seed=int(seed),
                num_states=int(num_states),
            )

    if best_model is None:
        unavailable["metrics"]["reason"] = "fit_failed"
        return unavailable

    state_feature_means = _state_feature_means_from_model(
        best_model,
        feature_mean=feature_mean,
        feature_std=feature_std,
    )
    label_map = map_states_to_regime_labels(
        state_feature_means=state_feature_means,
        feature_names=feature_names,
    )
    monitor = getattr(best_model, "monitor_", None)
    metrics = {
        "train_rows": int(feature_matrix.shape[0]),
        "feature_dim": int(feature_matrix.shape[1]),
        "log_likelihood": float(best_score),
        "converged": bool(getattr(monitor, "converged", False)),
        "iterations": int(_safe_int(getattr(monitor, "iter", 0), 0)),
    }
    return {
        "available": True,
        "enabled": bool(enabled),
        "backend": "hmmlearn.gaussian_hmm",
        "num_states": int(num_states),
        "feature_names": list(feature_names),
        "feature_mean": [float(value) for value in feature_mean.tolist()],
        "feature_std": [float(value) for value in feature_std.tolist()],
        "label_map": {int(key): str(value) for key, value in dict(label_map or {}).items()},
        "state_feature_means": state_feature_means.tolist(),
        "trained_ts_ms": int(time.time() * 1000),
        "metrics": metrics,
        "estimator": best_model,
    }


def infer_regime(model: Mapping[str, Any] | None, features: Any) -> Dict[str, Any]:
    """Infer one regime snapshot from a trained HMM model and input features."""
    payload = dict(model or {})
    if not bool(payload.get("available")):
        return _empty_signal(enabled=bool(payload.get("enabled", hmm_regime_enabled())), num_states=_safe_int(payload.get("num_states"), hmm_num_states()))

    estimator = payload.get("estimator")
    if estimator is None:
        return _empty_signal(enabled=bool(payload.get("enabled", hmm_regime_enabled())), num_states=_safe_int(payload.get("num_states"), hmm_num_states()))

    feature_names = list(payload.get("feature_names") or DEFAULT_HMM_FEATURE_NAMES)
    feature_matrix, _resolved_feature_names = _coerce_feature_matrix(features, expected_feature_names=feature_names)
    if feature_matrix.ndim != 2 or int(feature_matrix.shape[0]) <= 0:
        return _empty_signal(enabled=bool(payload.get("enabled", hmm_regime_enabled())), num_states=_safe_int(payload.get("num_states"), hmm_num_states()))

    feature_mean = np.asarray(payload.get("feature_mean") or [0.0] * len(feature_names), dtype=np.float64)
    feature_std = np.asarray(payload.get("feature_std") or [1.0] * len(feature_names), dtype=np.float64)
    if feature_mean.shape[0] != int(feature_matrix.shape[1]):
        feature_mean = np.zeros((int(feature_matrix.shape[1]),), dtype=np.float64)
    if feature_std.shape[0] != int(feature_matrix.shape[1]):
        feature_std = np.ones((int(feature_matrix.shape[1]),), dtype=np.float64)
    feature_std = np.where(feature_std < 1e-6, 1.0, feature_std)
    normalized = np.asarray((feature_matrix - feature_mean) / feature_std, dtype=np.float64)

    try:
        posterior = np.asarray(estimator.predict_proba(normalized), dtype=np.float64)
    except Exception as e:
        _warn_nonfatal(
            "hmm_regime_infer_failed",
            "HMM_REGIME_INFER_FAILED",
            e,
            warn_key="hmm_regime_infer_failed",
        )
        return _empty_signal(enabled=bool(payload.get("enabled", hmm_regime_enabled())), num_states=_safe_int(payload.get("num_states"), hmm_num_states()))

    if posterior.ndim == 1:
        posterior = posterior.reshape(1, -1)
    probabilities = _normalize_probabilities(posterior[-1].tolist())
    state_probabilities = {f"state_{idx}": float(probabilities[idx]) if idx < len(probabilities) else 0.0 for idx in range(HMM_MAX_STATES)}
    label_map = _normalize_label_map(payload.get("label_map"), num_states=int(posterior.shape[1]))
    label_probabilities = {label: 0.0 for label in HMM_LABEL_FEATURE_NAMES}
    for idx, prob in enumerate(probabilities):
        label = str(label_map.get(idx, f"STATE_{idx}") or f"STATE_{idx}").upper()
        if label not in label_probabilities:
            label_probabilities[label] = 0.0
        label_probabilities[label] = float(label_probabilities.get(label, 0.0) + float(prob))

    most_likely_state = int(np.argmax(np.asarray(probabilities, dtype=np.float64))) if probabilities else None
    regime_label = "UNKNOWN"
    if label_probabilities:
        regime_label = str(max(label_probabilities.items(), key=lambda item: float(item[1]))[0])
    elif most_likely_state is not None:
        regime_label = str(label_map.get(int(most_likely_state), f"STATE_{most_likely_state}"))

    return {
        "enabled": bool(payload.get("enabled", hmm_regime_enabled())),
        "model_available": True,
        "backend": str(payload.get("backend") or "hmmlearn.gaussian_hmm"),
        "num_states": int(posterior.shape[1]),
        "state_probabilities": state_probabilities,
        "label_probabilities": {str(key): float(value) for key, value in label_probabilities.items()},
        "most_likely_state": most_likely_state,
        "regime_label": str(regime_label),
        "confidence": float(max(probabilities) if probabilities else 0.0),
        "entropy": float(_normalized_entropy(probabilities)),
    }


def persist_hmm_model(model: Mapping[str, Any], *, symbol: str | None = None) -> Dict[str, Any]:
    """Persist one trained HMM regime model and mark it active for its symbol."""
    payload = dict(model or {})
    if not bool(payload.get("available")) or payload.get("estimator") is None:
        return {"ok": False, "status": "model_unavailable"}

    init_hmm_regime_schema()
    model_symbol = str(symbol or payload.get("symbol") or hmm_model_symbol()).upper().strip() or hmm_model_symbol()
    created_ts_ms = int(_safe_int(payload.get("trained_ts_ms"), int(time.time() * 1000)))
    record = {
        "available": True,
        "enabled": bool(payload.get("enabled", hmm_regime_enabled())),
        "backend": str(payload.get("backend") or "hmmlearn.gaussian_hmm"),
        "num_states": int(_safe_int(payload.get("num_states"), hmm_num_states())),
        "feature_names": list(payload.get("feature_names") or DEFAULT_HMM_FEATURE_NAMES),
        "feature_mean": [float(value) for value in list(payload.get("feature_mean") or [])],
        "feature_std": [float(value) for value in list(payload.get("feature_std") or [])],
        "label_map": {str(key): str(value) for key, value in dict(payload.get("label_map") or {}).items()},
        "state_feature_means": list(payload.get("state_feature_means") or []),
        "trained_ts_ms": int(created_ts_ms),
        "metrics": dict(payload.get("metrics") or {}),
        "estimator": payload.get("estimator"),
    }
    blob = dumps_pickle_artifact(record)
    artifact_alias = _hmm_artifact_alias(model_symbol)
    artifact_ref = LocalArtifactStore().put(
        blob,
        content_type="application/python-pickle",
        kind="model",
        alias=artifact_alias,
        metadata={
            "model_name": "hmm_regime",
            "symbol": str(model_symbol),
            "trained_ts_ms": int(created_ts_ms),
            "num_states": int(record["num_states"]),
            "backend": str(record.get("backend") or ""),
        },
    )

    def _write(con) -> None:
        con.executescript(_HMM_SCHEMA)
        _ensure_hmm_artifact_columns(con)
        con.execute(
            """
            UPDATE hmm_regime_models
            SET is_active=0
            WHERE symbol=?
            """,
            (str(model_symbol),),
        )
        con.execute(
            """
            INSERT INTO hmm_regime_models(
              created_ts_ms, symbol, num_states, feature_names_json, label_map_json, metrics_json, is_active,
              model_blob, artifact_sha256, artifact_alias
            )
            VALUES (?,?,?,?,?,?,1,?,?,?)
            """,
            (
                int(created_ts_ms),
                str(model_symbol),
                int(record["num_states"]),
                _json_dumps(record.get("feature_names") or []),
                _json_dumps(record.get("label_map") or {}),
                _json_dumps(record.get("metrics") or {}),
                b"",
                artifact_ref.sha256,
                artifact_alias,
            ),
        )

    run_write_txn(
        _write,
        table="hmm_regime_models",
        operation="persist_hmm_model",
        context={"symbol": str(model_symbol), "num_states": int(record["num_states"])},
    )
    with _MODEL_CACHE_LOCK:
        _MODEL_CACHE["ts_s"] = time.time()
        _MODEL_CACHE["key"] = str(model_symbol)
        _MODEL_CACHE["model"] = dict(record)
    return {
        "ok": True,
        "status": "persisted",
        "symbol": str(model_symbol),
        "trained_ts_ms": int(created_ts_ms),
        "num_states": int(record["num_states"]),
        "artifact_sha256": artifact_ref.sha256,
        "artifact_alias": artifact_alias,
    }


def load_latest_hmm_model(symbol: str | None = None) -> Dict[str, Any] | None:
    """Load the latest persisted HMM regime model for the requested symbol."""
    init_hmm_regime_schema()
    requested_symbol = str(symbol or "").upper().strip()
    fallback_symbol = hmm_model_symbol()
    cache_key = requested_symbol or fallback_symbol
    ttl_s = float(hmm_model_cache_ttl_s())
    with _MODEL_CACHE_LOCK:
        if (
            cache_key == str(_MODEL_CACHE.get("key") or "")
            and ttl_s > 0.0
            and (time.time() - float(_MODEL_CACHE.get("ts_s") or 0.0)) <= ttl_s
        ):
            cached = _MODEL_CACHE.get("model")
            return dict(cached) if isinstance(cached, Mapping) else cached

    candidates: List[str] = []
    for candidate in (requested_symbol, fallback_symbol, "*"):
        value = str(candidate or "").upper().strip()
        if value and value not in candidates:
            candidates.append(value)
    if not candidates:
        candidates = [fallback_symbol, "*"]

    con = connect(readonly=True)
    try:
        placeholders = ",".join("?" for _ in candidates)
        order_symbol = requested_symbol or fallback_symbol
        row = con.execute(
            f"""
            SELECT symbol, model_blob, artifact_sha256, artifact_alias
            FROM hmm_regime_models
            WHERE is_active=1
              AND symbol IN ({placeholders})
            ORDER BY
              CASE
                WHEN symbol=? THEN 0
                WHEN symbol=? THEN 1
                WHEN symbol='*' THEN 2
                ELSE 3
              END,
              created_ts_ms DESC,
              id DESC
            LIMIT 1
            """,
            tuple(candidates + [order_symbol, fallback_symbol]),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "hmm_regime_model_load_failed",
            "HMM_REGIME_MODEL_LOAD_FAILED",
            e,
            warn_key="hmm_regime_model_load_failed",
            symbol=str(requested_symbol or fallback_symbol),
        )
        row = None
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "hmm_regime_model_close_failed",
                "HMM_REGIME_MODEL_CLOSE_FAILED",
                e,
                warn_key="hmm_regime_model_close_failed",
                symbol=str(requested_symbol or fallback_symbol),
            )

    if not row:
        return None

    model_symbol = str((row[0] or fallback_symbol)).upper().strip() or fallback_symbol
    blob = bytes(row[1] or b"")
    artifact_sha = str(row[2] or "").strip()
    artifact_alias = str(row[3] or "").strip()
    if not blob and (artifact_alias or artifact_sha):
        try:
            blob = _load_artifact_blob(artifact_alias, artifact_sha)
        except Exception as e:
            _warn_nonfatal(
                "hmm_regime_model_artifact_load_failed",
                "HMM_REGIME_MODEL_ARTIFACT_LOAD_FAILED",
                e,
                warn_key=f"hmm_regime_model_artifact_load_failed:{model_symbol}",
                symbol=str(model_symbol),
                artifact_sha256=str(artifact_sha),
                artifact_alias=str(artifact_alias),
            )
            blob = b""
    try:
        payload = pickle.loads(blob)
    except Exception as e:
        _warn_nonfatal(
            "hmm_regime_model_unpickle_failed",
            "HMM_REGIME_MODEL_UNPICKLE_FAILED",
            e,
            warn_key=f"hmm_regime_model_unpickle_failed:{model_symbol}",
            symbol=str(model_symbol),
        )
        return None
    if not isinstance(payload, Mapping):
        return None
    model = dict(payload)
    model["symbol"] = str(model_symbol)
    with _MODEL_CACHE_LOCK:
        _MODEL_CACHE["ts_s"] = time.time()
        _MODEL_CACHE["key"] = str(cache_key)
        _MODEL_CACHE["model"] = dict(model)
    return model


def build_hmm_input_from_regime_vector(regime_vector: Mapping[str, Any] | None) -> Dict[str, float]:
    """Project a regime stack snapshot into the fixed HMM training feature schema."""
    vector = dict(regime_vector or {})
    macro = dict(vector.get("macro") or {})
    micro = dict(vector.get("micro") or {})
    return {
        "macro.risk_off": float(_safe_float(macro.get("risk_off"), 0.0)),
        "macro.vol_expansion": float(_safe_float(macro.get("vol_expansion"), 0.0)),
        "macro.credit_stress": float(_safe_float(macro.get("credit_stress"), 0.0)),
        "macro.drawdown_shift": float(_safe_float(macro.get("drawdown_shift"), 0.0)),
        "micro.vol_clustered": float(_safe_float(micro.get("vol_clustered"), 0.0)),
        "micro.liquidity_thin": float(_safe_float(micro.get("liquidity_thin"), 0.0)),
    }


def resolve_hmm_regime_snapshot(
    *,
    symbol: str,
    ts_ms: int,
    con=None,
    regime_vector: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Resolve an HMM-derived regime snapshot from a regime vector and model."""
    enabled = hmm_regime_enabled()
    if not enabled:
        return _empty_signal(enabled=False, num_states=hmm_num_states())

    model = load_latest_hmm_model(symbol=str(symbol))
    if not model:
        return _empty_signal(enabled=True, num_states=hmm_num_states())

    base_vector = dict(regime_vector or {})
    if not base_vector:
        try:
            from engine.strategy.regime_stack import compute_regime_vector

            base_vector = dict(
                compute_regime_vector(
                    symbol=str(symbol),
                    ts_ms=int(ts_ms),
                    con=con,
                    include_hmm=False,
                )
                or {}
            )
        except Exception as e:
            _warn_nonfatal(
                "hmm_regime_base_vector_failed",
                "HMM_REGIME_BASE_VECTOR_FAILED",
                e,
                warn_key=f"hmm_regime_base_vector_failed:{symbol}",
                symbol=str(symbol),
                ts_ms=int(ts_ms),
            )
            return _empty_signal(enabled=True, num_states=_safe_int(model.get("num_states"), hmm_num_states()))

    signal = infer_regime(model, build_hmm_input_from_regime_vector(base_vector))
    signal["symbol"] = str(symbol or "").upper().strip()
    signal["ts_ms"] = int(_safe_int(ts_ms, 0))
    signal["model_symbol"] = str(model.get("symbol") or hmm_model_symbol())
    signal["feature_names"] = list(model.get("feature_names") or DEFAULT_HMM_FEATURE_NAMES)
    return signal


def build_hmm_feature_map(signal: Mapping[str, Any] | None) -> Dict[str, float]:
    """Project an HMM regime signal into numeric feature values."""
    payload = dict(signal or {})
    state_probs = dict(payload.get("state_probabilities") or {})
    label_probs = dict(payload.get("label_probabilities") or {})
    return {
        "hmm_regime.enabled": 1.0 if bool(payload.get("enabled")) else 0.0,
        "hmm_regime.model_available": 1.0 if bool(payload.get("model_available")) else 0.0,
        "hmm_regime.confidence": float(_safe_float(payload.get("confidence"), 0.0)),
        "hmm_regime.entropy": float(_safe_float(payload.get("entropy"), 1.0)),
        "hmm_regime.state_0_prob": float(_safe_float(state_probs.get("state_0"), 0.0)),
        "hmm_regime.state_1_prob": float(_safe_float(state_probs.get("state_1"), 0.0)),
        "hmm_regime.state_2_prob": float(_safe_float(state_probs.get("state_2"), 0.0)),
        "hmm_regime.state_3_prob": float(_safe_float(state_probs.get("state_3"), 0.0)),
        "hmm_regime.state_4_prob": float(_safe_float(state_probs.get("state_4"), 0.0)),
        "hmm_regime.label_risk_on_prob": float(_safe_float(label_probs.get("RISK_ON"), 0.0)),
        "hmm_regime.label_recovery_prob": float(_safe_float(label_probs.get("RECOVERY"), 0.0)),
        "hmm_regime.label_neutral_prob": float(_safe_float(label_probs.get("NEUTRAL"), 0.0)),
        "hmm_regime.label_volatile_prob": float(_safe_float(label_probs.get("VOLATILE"), 0.0)),
        "hmm_regime.label_risk_off_prob": float(_safe_float(label_probs.get("RISK_OFF"), 0.0)),
    }


def _normalize_weight_sum(weights: Mapping[str, Any], *, families: Sequence[str]) -> Dict[str, float]:
    cleaned = {
        str(family): max(0.0, _safe_float(dict(weights or {}).get(family), 0.0))
        for family in list(families or [])
        if str(family or "").strip()
    }
    total = float(sum(cleaned.values()))
    if total <= 0.0 and cleaned:
        equal = 1.0 / float(len(cleaned))
        return {str(family): float(equal) for family in cleaned.keys()}
    if total <= 0.0:
        return {}
    return {str(family): float(value / total) for family, value in cleaned.items()}


def apply_hmm_uncertainty_to_weights(
    weights: Mapping[str, Any],
    *,
    available_families: Sequence[str],
    signal: Mapping[str, Any] | None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Adjust ensemble-family weights using HMM regime uncertainty."""
    base_payload = dict(weights or {})
    diagnostics = {
        "applied": False,
        "mix": 0.0,
        "reason": "",
        "regime_label": str((signal or {}).get("regime_label") or "UNKNOWN"),
        "confidence": float(_safe_float((signal or {}).get("confidence"), 0.0)),
    }
    families = [str(family) for family in list(available_families or []) if str(family or "").strip()]
    if not hmm_ensemble_weight_enabled():
        diagnostics["reason"] = "disabled"
        return base_payload, diagnostics
    if not families:
        diagnostics["reason"] = "no_available_families"
        return base_payload, diagnostics
    if not bool((signal or {}).get("model_available")):
        diagnostics["reason"] = "model_unavailable"
        return base_payload, diagnostics

    raw_weights = _normalize_weight_sum(base_payload, families=families)
    if not raw_weights:
        diagnostics["reason"] = "empty_weight_map"
        return base_payload, diagnostics

    confidence = float(max(0.0, min(1.0, _safe_float((signal or {}).get("confidence"), 0.0))))
    mix = float(max(0.0, min(0.35, 1.0 - confidence)))
    if mix <= 1e-6:
        diagnostics["reason"] = "confidence_high"
        return base_payload, diagnostics

    equal_weight = 1.0 / float(len(families))
    adjusted = {
        family: float(((1.0 - mix) * float(raw_weights.get(family, 0.0))) + (mix * equal_weight))
        for family in families
    }
    normalized = _normalize_weight_sum(adjusted, families=families)
    updated = dict(base_payload)
    for family, value in normalized.items():
        updated[str(family)] = float(value)
    diagnostics["applied"] = True
    diagnostics["mix"] = float(mix)
    diagnostics["reason"] = "uncertainty_shrink"
    diagnostics["adjusted_weights"] = dict(normalized)
    return updated, diagnostics
