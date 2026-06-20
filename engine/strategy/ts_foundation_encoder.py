"""Shadow-only frozen time-series foundation model feature encoders."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from contextlib import nullcontext
from importlib import import_module, metadata
from typing import Any, Mapping, Sequence

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.hardware import resolve_torch_device
from engine.runtime.logging import get_logger


TS_FOUNDATION_CHRONOS_GROUP = "ts_foundation_chronos"
TS_FOUNDATION_CHRONOS_PREFIX = "tsfm.chronos_v2."
TS_FOUNDATION_MODEL_FAMILY = "chronos"
TS_FOUNDATION_ARTIFACT_KIND = "feature_encoder_manifest"

_LOG = get_logger("engine.strategy.ts_foundation_encoder")
_WARNED_NONFATAL_KEYS: set[str] = set()
_PIPELINE_CACHE: dict[tuple[str, str, bool], Any] = {}
_ARTIFACT_META_CACHE: dict[tuple[str, int, int, str], dict[str, Any]] = {}


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        _LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.strategy.ts_foundation_encoder",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return bool(default)
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        value = int(default)
    else:
        try:
            value = int(str(raw).strip())
        except Exception:
            value = int(default)
    return int(max(int(minimum), min(int(maximum), int(value))))


def _env_text(name: str, default: str = "") -> str:
    raw = os.environ.get(str(name))
    return str(default if raw is None else raw).strip()


def chronos_embedding_dim() -> int:
    return _env_int("TS_FOUNDATION_EMBEDDING_DIM", 16, minimum=1, maximum=512)


def chronos_context_rows() -> int:
    return _env_int("TS_FOUNDATION_CONTEXT_ROWS", 256, minimum=16, maximum=4096)


def chronos_min_context_rows() -> int:
    return _env_int("TS_FOUNDATION_MIN_CONTEXT_ROWS", 32, minimum=4, maximum=4096)


def chronos_model_id() -> str:
    return (
        _env_text("TS_FOUNDATION_CHRONOS_MODEL_ID")
        or _env_text("TS_FOUNDATION_MODEL_ID")
        or "amazon/chronos-2"
    )


def ts_foundation_backend() -> str:
    return (_env_text("TS_FOUNDATION_BACKEND", "chronos") or "chronos").lower()


def ts_foundation_features_enabled() -> bool:
    return bool(_env_bool("USE_TS_FOUNDATION_FEATURES", False) and ts_foundation_backend() == "chronos")


def chronos_local_files_only() -> bool:
    return _env_bool("TS_FOUNDATION_LOCAL_FILES_ONLY", True)


def chronos_device() -> str:
    try:
        torch = import_module("torch")
        resolution = resolve_torch_device(
            torch,
            env_var="TS_FOUNDATION_DEVICE",
            fallback_envs=("TORCH_DEVICE",),
        )
        return resolution.resolved
    except Exception:
        return "cpu"


def chronos_revision() -> str:
    return _env_text("TS_FOUNDATION_MODEL_REVISION", "")


def get_chronos_feature_ids(dim: int | None = None) -> list[str]:
    feature_dim = chronos_embedding_dim() if dim is None else max(1, int(dim))
    return [f"{TS_FOUNDATION_CHRONOS_PREFIX}embedding_{idx:03d}" for idx in range(int(feature_dim))]


TS_FOUNDATION_CHRONOS_FEATURE_IDS = get_chronos_feature_ids()


def _feature_index(feature_id: str) -> int | None:
    text = str(feature_id or "").strip()
    if not text.startswith(TS_FOUNDATION_CHRONOS_PREFIX):
        return None
    match = re.search(r"embedding_(\d+)$", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _requested_feature_ids(feature_ids: Sequence[str] | None) -> list[str]:
    requested = [
        str(fid).strip()
        for fid in list(feature_ids or TS_FOUNDATION_CHRONOS_FEATURE_IDS)
        if str(fid or "").strip().startswith(TS_FOUNDATION_CHRONOS_PREFIX)
    ]
    return requested or list(TS_FOUNDATION_CHRONOS_FEATURE_IDS)


def _feature_dim_for_request(feature_ids: Sequence[str] | None) -> int:
    max_idx = -1
    for fid in _requested_feature_ids(feature_ids):
        idx = _feature_index(fid)
        if idx is not None:
            max_idx = max(max_idx, int(idx))
    return max(chronos_embedding_dim(), int(max_idx) + 1)


def _zero_features(feature_ids: Sequence[str] | None) -> dict[str, float]:
    return {str(fid): 0.0 for fid in _requested_feature_ids(feature_ids)}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _row_value(row: Any, index: int) -> Any:
    try:
        return row[index]
    except Exception:
        return None


def _price_series_asof(con: Any, *, symbol: str, ts_ms: int, limit: int) -> list[tuple[int, float]]:
    try:
        rows = con.execute(
            """
            SELECT ts_ms, COALESCE(price, px) AS value
            FROM prices
            WHERE symbol = ?
              AND ts_ms <= ?
              AND COALESCE(price, px) IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol).upper().strip(), int(ts_ms), int(limit)),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal(
            "TS_FOUNDATION_PRICE_SERIES_LOAD_FAILED",
            exc,
            once_key="ts_foundation_price_series_load_failed",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        rows = []
    points: list[tuple[int, float]] = []
    for row in reversed(list(rows or [])):
        row_ts = int(_row_value(row, 0) or 0)
        value = _safe_float(_row_value(row, 1), math.nan)
        if row_ts <= 0 or not math.isfinite(value):
            continue
        points.append((int(row_ts), float(value)))
    return points


def _normalized_target_values(points: Sequence[tuple[int, float]]) -> np.ndarray:
    values = np.asarray([float(value) for _ts, value in list(points or [])], dtype=np.float32)
    if values.size == 0:
        return values
    if np.all(values > 0.0):
        values = np.log(values)
    mean = float(np.nanmean(values)) if values.size else 0.0
    std = float(np.nanstd(values)) if values.size else 0.0
    if math.isfinite(std) and std > 1e-9:
        values = (values - mean) / std
    else:
        values = values - mean
    values = np.nan_to_num(values, nan=0.0, posinf=0.0, neginf=0.0)
    return values.astype(np.float32, copy=False)


def _package_version(package_name: str) -> str:
    try:
        return str(metadata.version(str(package_name)))
    except Exception:
        return ""


def _safe_alias_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.:-]+", "_", str(value or "").strip())
    return token.strip("_") or "unknown"


def _manifest_payload(*, model_id: str, feature_dim: int, context_rows: int, revision: str) -> dict[str, Any]:
    package_version = _package_version("chronos-forecasting")
    return {
        "artifact_schema_version": 1,
        "artifact_kind": TS_FOUNDATION_ARTIFACT_KIND,
        "backend": "chronos",
        "context_rows": int(context_rows),
        "direct_trading_authority": False,
        "feature_dim": int(feature_dim),
        "feature_group": TS_FOUNDATION_CHRONOS_GROUP,
        "feature_prefix": TS_FOUNDATION_CHRONOS_PREFIX,
        "frozen_encoder": True,
        "model_family": TS_FOUNDATION_MODEL_FAMILY,
        "model_id": str(model_id),
        "package": "chronos-forecasting",
        "package_version": str(package_version),
        "revision": str(revision or ""),
        "source": "pretrained_time_series_foundation_model",
    }


def _encoder_artifact_metadata(*, model_id: str, feature_dim: int, context_rows: int, revision: str) -> dict[str, Any]:
    cache_key = (str(model_id), int(feature_dim), int(context_rows), str(revision or ""))
    if cache_key in _ARTIFACT_META_CACHE:
        return dict(_ARTIFACT_META_CACHE[cache_key])

    manifest = _manifest_payload(
        model_id=str(model_id),
        feature_dim=int(feature_dim),
        context_rows=int(context_rows),
        revision=str(revision or ""),
    )
    payload = json.dumps(manifest, separators=(",", ":"), sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    alias = (
        "feature_encoder:ts_foundation:chronos:"
        f"{_safe_alias_token(model_id)}:dim{int(feature_dim)}:ctx{int(context_rows)}:current"
    )
    out: dict[str, Any] = {
        "artifact_alias": str(alias),
        "artifact_sha256": str(digest),
        "artifact_created_ts_ms": None,
        "artifact_kind": TS_FOUNDATION_ARTIFACT_KIND,
        "artifact_persisted": False,
        "artifact_manifest": dict(manifest),
    }

    try:
        from engine.artifacts.store import LocalArtifactStore

        ref = LocalArtifactStore().put(
            payload,
            content_type="application/json",
            kind=TS_FOUNDATION_ARTIFACT_KIND,
            alias=str(alias),
            metadata=dict(manifest),
        )
        out.update(
            {
                "artifact_sha256": str(ref.sha256),
                "artifact_created_ts_ms": int(ref.created_ts.timestamp() * 1000),
                "artifact_persisted": True,
            }
        )
    except Exception as exc:
        if _env_bool("TS_FOUNDATION_REQUIRE_ARTIFACT_PERSISTENCE", True):
            raise
        out["artifact_error"] = f"{type(exc).__name__}: {exc}"

    _ARTIFACT_META_CACHE[cache_key] = dict(out)
    return dict(out)


def _freeze_pipeline(pipeline: Any) -> Any:
    for candidate in (pipeline, getattr(pipeline, "model", None), getattr(pipeline, "inner_model", None)):
        if candidate is None:
            continue
        candidate_type = f"{type(candidate).__module__}.{type(candidate).__qualname__}"
        try:
            if hasattr(candidate, "eval") and callable(candidate.eval):
                candidate.eval()
        except Exception as exc:
            _warn_nonfatal(
                "TS_FOUNDATION_PIPELINE_EVAL_FREEZE_FAILED",
                exc,
                once_key=f"ts_foundation_pipeline_eval_freeze_failed:{candidate_type}",
                candidate_type=candidate_type,
            )
        try:
            params = candidate.parameters() if hasattr(candidate, "parameters") else []
            for param in params:
                if hasattr(param, "requires_grad_"):
                    param.requires_grad_(False)
        except Exception as exc:
            _warn_nonfatal(
                "TS_FOUNDATION_PIPELINE_PARAM_FREEZE_FAILED",
                exc,
                once_key=f"ts_foundation_pipeline_param_freeze_failed:{candidate_type}",
                candidate_type=candidate_type,
            )
    return pipeline


def _load_chronos_pipeline(*, model_id: str, device: str, local_files_only: bool, revision: str) -> Any:
    cache_key = (str(model_id), str(device), bool(local_files_only))
    if cache_key in _PIPELINE_CACHE:
        return _PIPELINE_CACHE[cache_key]

    chronos = import_module("chronos")
    pipeline_cls = getattr(chronos, "Chronos2Pipeline", None) or getattr(chronos, "ChronosPipeline", None)
    if pipeline_cls is None or not hasattr(pipeline_cls, "from_pretrained"):
        raise ImportError("chronos pipeline class with from_pretrained is unavailable")

    kwargs: dict[str, Any] = {"device_map": str(device)}
    if bool(local_files_only):
        kwargs["local_files_only"] = True
    if str(revision or "").strip():
        kwargs["revision"] = str(revision).strip()

    attempts = [dict(kwargs)]
    if not bool(local_files_only):
        attempts.extend(({k: v for k, v in kwargs.items() if k != "local_files_only"}, {}))
    last_exc: BaseException | None = None
    for attempt_kwargs in attempts:
        try:
            pipeline = pipeline_cls.from_pretrained(str(model_id), **attempt_kwargs)
            pipeline = _freeze_pipeline(pipeline)
            _PIPELINE_CACHE[cache_key] = pipeline
            return pipeline
        except TypeError as exc:
            last_exc = exc
            continue
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("chronos pipeline load failed")


def _no_grad_context() -> Any:
    try:
        torch = import_module("torch")
        return torch.no_grad()
    except Exception:
        return nullcontext()


def _make_context_frame(points: Sequence[tuple[int, float]], values: np.ndarray) -> Any:
    pd = import_module("pandas")
    return pd.DataFrame(
        {
            "id": ["series"] * int(len(values)),
            "timestamp": pd.to_datetime([int(ts) for ts, _value in points], unit="ms", utc=True),
            "target": values.astype(float).tolist(),
        }
    )


def _call_embed(pipeline: Any, points: Sequence[tuple[int, float]], values: np.ndarray) -> Any:
    embed = getattr(pipeline, "embed", None)
    if not callable(embed):
        raise RuntimeError("chronos_embed_unavailable")

    context_df = _make_context_frame(points, values)
    attempts = (
        lambda: embed(context_df, id_column="id", timestamp_column="timestamp", target="target"),
        lambda: embed(context_df, id_column="id", timestamp_column="timestamp", target_column="target"),
        lambda: embed(context_df),
        lambda: embed(values.astype(np.float32)),
        lambda: embed([values.astype(np.float32)]),
    )
    last_exc: BaseException | None = None
    with _no_grad_context():
        for attempt in attempts:
            try:
                return attempt()
            except TypeError as exc:
                last_exc = exc
                continue
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("chronos_embed_failed")


def _coerce_numeric_array(value: Any) -> np.ndarray:
    if value is None:
        return np.asarray([], dtype=np.float32)
    if hasattr(value, "detach") and callable(value.detach):
        try:
            return np.asarray(value.detach().cpu().numpy(), dtype=np.float32)
        except Exception:
            return np.asarray([], dtype=np.float32)
    if isinstance(value, Mapping):
        for key in ("embedding", "embeddings", "encoder_embedding", "encoder_embeddings"):
            if key in value:
                arr = _coerce_numeric_array(value.get(key))
                if arr.size:
                    return arr
        parts = [_coerce_numeric_array(item) for item in value.values()]
        parts = [arr.reshape(-1) for arr in parts if arr.size]
        return np.concatenate(parts).astype(np.float32, copy=False) if parts else np.asarray([], dtype=np.float32)
    if hasattr(value, "to_numpy") and callable(value.to_numpy):
        try:
            arr = value.to_numpy()
            if getattr(getattr(arr, "dtype", None), "kind", "") == "O":
                parts = [_coerce_numeric_array(item).reshape(-1) for item in arr.reshape(-1)]
                parts = [part for part in parts if part.size]
                return np.concatenate(parts).astype(np.float32, copy=False) if parts else np.asarray([], dtype=np.float32)
            return np.asarray(arr, dtype=np.float32)
        except Exception as exc:
            _warn_nonfatal(
                "TS_FOUNDATION_TO_NUMPY_COERCE_FAILED",
                exc,
                once_key=f"ts_foundation_to_numpy_coerce_failed:{type(value).__module__}.{type(value).__qualname__}",
                value_type=f"{type(value).__module__}.{type(value).__qualname__}",
            )
    if isinstance(value, (list, tuple)):
        try:
            return np.asarray(value, dtype=np.float32)
        except Exception:
            parts = [_coerce_numeric_array(item).reshape(-1) for item in value]
            parts = [part for part in parts if part.size]
            return np.concatenate(parts).astype(np.float32, copy=False) if parts else np.asarray([], dtype=np.float32)
    try:
        return np.asarray(value, dtype=np.float32)
    except Exception:
        return np.asarray([], dtype=np.float32)


def _project_embedding(raw_embedding: Any, *, dim: int) -> np.ndarray:
    arr = _coerce_numeric_array(raw_embedding)
    if arr.size == 0:
        raise RuntimeError("chronos_embedding_empty")
    arr = np.nan_to_num(arr.astype(np.float32, copy=False), nan=0.0, posinf=0.0, neginf=0.0)
    if arr.ndim > 1:
        arr = arr.reshape(-1, arr.shape[-1]).mean(axis=0)
    flat = arr.reshape(-1)
    if flat.size >= int(dim):
        chunks = np.array_split(flat, int(dim))
        projected = np.asarray([float(chunk.mean()) if chunk.size else 0.0 for chunk in chunks], dtype=np.float32)
    else:
        projected = np.zeros((int(dim),), dtype=np.float32)
        projected[: flat.size] = flat
    projected = np.nan_to_num(projected, nan=0.0, posinf=0.0, neginf=0.0)
    return np.clip(projected, -10.0, 10.0).astype(np.float32, copy=False)


def _base_meta(*, ts_ms: int, feature_dim: int, context_rows: int, min_context_rows: int, model_id: str) -> dict[str, Any]:
    return {
        "backend": "chronos",
        "direct_trading_authority": False,
        "encoder_mode": "frozen",
        "feature_dim": int(feature_dim),
        "feature_generated_ts_ms": None,
        "feature_group": TS_FOUNDATION_CHRONOS_GROUP,
        "frozen_encoder": True,
        "model_family": TS_FOUNDATION_MODEL_FAMILY,
        "model_family_provenance": {
            "backend": "chronos",
            "direct_trading_authority": False,
            "frozen_encoder": True,
            "model_id": str(model_id),
            "package": "chronos-forecasting",
            "package_version": _package_version("chronos-forecasting"),
            "source": "pretrained_time_series_foundation_model",
        },
        "model_id": str(model_id),
        "price_history_first_ts_ms": None,
        "price_history_last_ts_ms": None,
        "price_history_rows": 0,
        "requested_ts_ms": int(ts_ms),
        "required_min_context_rows": int(min_context_rows),
        "status": "unavailable",
        "window_context_rows": int(context_rows),
    }


def resolve_chronos_foundation_features(
    con: Any,
    *,
    symbol: str,
    ts_ms: int,
    feature_ids: Sequence[str] | None = None,
) -> tuple[dict[str, float], dict[str, Any], bool]:
    """Return Chronos encoder features as a PIT-safe shadow feature group."""

    requested_ids = _requested_feature_ids(feature_ids)
    feature_dim = _feature_dim_for_request(requested_ids)
    context_rows = chronos_context_rows()
    min_context_rows = min(chronos_min_context_rows(), context_rows)
    model_id = chronos_model_id()
    meta = _base_meta(
        ts_ms=int(ts_ms),
        feature_dim=int(feature_dim),
        context_rows=int(context_rows),
        min_context_rows=int(min_context_rows),
        model_id=str(model_id),
    )
    zero = _zero_features(requested_ids)

    if not ts_foundation_features_enabled():
        meta["status"] = "disabled"
        return zero, meta, False

    points = _price_series_asof(con, symbol=str(symbol), ts_ms=int(ts_ms), limit=int(context_rows))
    if points:
        meta["price_history_first_ts_ms"] = int(points[0][0])
        meta["price_history_last_ts_ms"] = int(points[-1][0])
        meta["price_history_rows"] = int(len(points))
    if len(points) < int(min_context_rows):
        meta["status"] = "insufficient_price_history"
        return zero, meta, False

    try:
        artifact_meta = _encoder_artifact_metadata(
            model_id=str(model_id),
            feature_dim=int(feature_dim),
            context_rows=int(context_rows),
            revision=chronos_revision(),
        )
        meta.update(dict(artifact_meta))
        if int(meta.get("artifact_created_ts_ms") or 0) > 0:
            meta["encoder_artifact_created_ts_ms"] = int(meta.get("artifact_created_ts_ms") or 0)
    except Exception as exc:
        _warn_nonfatal(
            "TS_FOUNDATION_ARTIFACT_UNAVAILABLE",
            exc,
            once_key="ts_foundation_artifact_unavailable",
            model_id=str(model_id),
        )
        meta["status"] = "artifact_unavailable"
        meta["artifact_error"] = f"{type(exc).__name__}: {exc}"
        return zero, meta, False

    try:
        pipeline = _load_chronos_pipeline(
            model_id=str(model_id),
            device=chronos_device(),
            local_files_only=chronos_local_files_only(),
            revision=chronos_revision(),
        )
        values = _normalized_target_values(points)
        embedding = _call_embed(pipeline, points, values)
        projected = _project_embedding(embedding, dim=int(feature_dim))
    except Exception as exc:
        _warn_nonfatal(
            "TS_FOUNDATION_CHRONOS_EMBED_FAILED",
            exc,
            once_key="ts_foundation_chronos_embed_failed",
            model_id=str(model_id),
            symbol=str(symbol),
        )
        meta["status"] = "encoder_unavailable"
        meta["encoder_error"] = f"{type(exc).__name__}: {exc}"
        return zero, meta, False

    out: dict[str, float] = {}
    for fid in requested_ids:
        idx = _feature_index(fid)
        out[str(fid)] = float(projected[int(idx)] if idx is not None and int(idx) < projected.size else 0.0)

    meta["feature_generated_ts_ms"] = int(time.time() * 1000)
    meta["embedding_source"] = "chronos_embed"
    meta["status"] = "ok"
    return out, meta, True


__all__ = [
    "TS_FOUNDATION_CHRONOS_FEATURE_IDS",
    "TS_FOUNDATION_CHRONOS_GROUP",
    "TS_FOUNDATION_CHRONOS_PREFIX",
    "chronos_embedding_dim",
    "get_chronos_feature_ids",
    "resolve_chronos_foundation_features",
    "ts_foundation_features_enabled",
]
