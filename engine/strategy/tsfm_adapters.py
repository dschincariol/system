"""Governed time-series foundation model adapter interfaces.

The adapters in this module are intentionally dependency-light at import time.
Provider packages are imported only inside their concrete adapters so the
default CPU/runtime profile can run benchmarks and tests without CUDA or model
downloads.
"""

from __future__ import annotations

import hashlib
import math
import os
from dataclasses import dataclass, field
from importlib import import_module, metadata
from typing import Any, Mapping, Protocol, Sequence

import numpy as np

from engine.runtime.hardware import resolve_torch_device


TSFM_SHADOW_GROUP = "ts_foundation_adapters"
TSFM_SUPPORTED_BACKENDS = ("chronos", "timesfm", "moirai", "toto", "fake")
TSFM_FORECAST_FEATURE_NAMES = (
    "forecast_mean",
    "forecast_p10",
    "forecast_p50",
    "forecast_p90",
    "forecast_vol_proxy",
)
TSFM_DEFAULT_EMBEDDING_DIM = 16
TSFM_BACKEND_PREFIXES: dict[str, str] = {
    "chronos": "tsfm.chronos_v2.",
    "timesfm": "tsfm.timesfm.",
    "moirai": "tsfm.moirai.",
    "toto": "tsfm.toto.",
    "fake": "tsfm.fake.",
}


class TSFMAdapterUnavailable(RuntimeError):
    """Raised when an optional TSFM provider cannot serve a request."""


@dataclass(frozen=True)
class TSFMAdapterConfig:
    backend: str = "chronos"
    model_id: str = ""
    context_length: int = 256
    horizon: int = 1
    embedding_dim: int = TSFM_DEFAULT_EMBEDDING_DIM
    device: str = "cpu"
    local_files_only: bool = True
    quantiles: tuple[float, ...] = (0.1, 0.5, 0.9)
    fallback: str = "skip"
    revision: str = ""
    extra: Mapping[str, Any] = field(default_factory=dict)

    def normalized_backend(self) -> str:
        return normalize_tsfm_backend(self.backend)

    def normalized_model_id(self) -> str:
        model_id = str(self.model_id or "").strip()
        if model_id:
            return model_id
        backend = self.normalized_backend()
        if backend == "chronos":
            return "amazon/chronos-2"
        if backend == "timesfm":
            return "google/timesfm-2.0-500m-pytorch"
        if backend == "moirai":
            return "Salesforce/moirai-1.1-R-small"
        if backend == "toto":
            return "Datadog/Toto-Open-Base-1.0"
        return "deterministic-fake"


@dataclass(frozen=True)
class TSFMSeriesContext:
    symbol: str
    timestamps_ms: tuple[int, ...]
    values: tuple[float, ...]
    asof_ts_ms: int
    asset_class: str = ""
    task: str = "forecast"

    def validate_pit(self) -> None:
        if len(self.timestamps_ms) != len(self.values):
            raise ValueError("tsfm_context_length_mismatch")
        if not self.timestamps_ms:
            raise ValueError("tsfm_context_empty")
        last_ts = int(self.timestamps_ms[-1])
        if last_ts > int(self.asof_ts_ms):
            raise ValueError("tsfm_context_after_asof")
        prev = -1
        for ts_ms in self.timestamps_ms:
            ts_int = int(ts_ms)
            if ts_int <= prev:
                raise ValueError("tsfm_context_timestamps_not_strictly_increasing")
            prev = ts_int


@dataclass(frozen=True)
class TSFMForecastOutput:
    backend: str
    model_id: str
    horizon_path: tuple[float, ...]
    quantiles: Mapping[str, tuple[float, ...]]
    volatility_proxy: float
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def point(self) -> float:
        if self.horizon_path:
            return float(self.horizon_path[-1])
        q50 = self.quantiles.get("0.5") or self.quantiles.get("0.50") or ()
        return float(q50[-1]) if q50 else 0.0


@dataclass(frozen=True)
class TSFMEmbeddingOutput:
    backend: str
    model_id: str
    feature_ids: tuple[str, ...]
    values: tuple[float, ...]
    metadata: Mapping[str, Any] = field(default_factory=dict)


class TSFMAdapter(Protocol):
    config: TSFMAdapterConfig

    def describe(self) -> dict[str, Any]:
        ...

    def forecast(self, context: TSFMSeriesContext, *, horizon: int | None = None) -> TSFMForecastOutput:
        ...

    def embed(self, context: TSFMSeriesContext, *, dim: int | None = None) -> TSFMEmbeddingOutput:
        ...


def normalize_tsfm_backend(value: Any) -> str:
    backend = str(value or "chronos").strip().lower().replace("-", "_")
    if backend == "chronos2":
        backend = "chronos"
    if backend in {"timefm", "times_fm"}:
        backend = "timesfm"
    if backend not in TSFM_SUPPORTED_BACKENDS:
        raise ValueError(f"unsupported_tsfm_backend:{backend}")
    return backend


def _package_version(package_name: str) -> str:
    try:
        return str(metadata.version(str(package_name)))
    except Exception:
        return ""


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _finite_array(values: Sequence[Any]) -> np.ndarray:
    arr = np.asarray([_safe_float(value, math.nan) for value in values], dtype=np.float32)
    return np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)


def _returns(values: np.ndarray) -> np.ndarray:
    if values.size < 2:
        return np.asarray([], dtype=np.float32)
    if np.all(values > 0.0):
        return np.diff(np.log(values.astype(np.float64))).astype(np.float32)
    return np.diff(values.astype(np.float64)).astype(np.float32)


def _volatility_proxy(values: Sequence[Any]) -> float:
    arr = _finite_array(values)
    rets = _returns(arr)
    if rets.size == 0:
        return 0.0
    vol = float(np.nanstd(rets))
    return float(vol if math.isfinite(vol) else 0.0)


def _quantile_key(value: float) -> str:
    return f"{float(value):.2f}".rstrip("0").rstrip(".")


def _coerce_path(values: Any, *, horizon: int, fallback_last: float) -> tuple[float, ...]:
    arr = np.asarray(values, dtype=np.float32)
    if arr.size == 0:
        return tuple(float(fallback_last) for _ in range(int(horizon)))
    arr = np.nan_to_num(arr.reshape(-1), nan=float(fallback_last), posinf=float(fallback_last), neginf=float(fallback_last))
    if arr.size >= int(horizon):
        return tuple(float(v) for v in arr[: int(horizon)].tolist())
    padded = np.full((int(horizon),), float(arr[-1]), dtype=np.float32)
    padded[: arr.size] = arr
    return tuple(float(v) for v in padded.tolist())


def _coerce_forecast_payload(
    payload: Any,
    *,
    backend: str,
    model_id: str,
    horizon: int,
    quantiles: Sequence[float],
    fallback_last: float,
    metadata_extra: Mapping[str, Any] | None = None,
) -> TSFMForecastOutput:
    meta: dict[str, Any] = dict(metadata_extra or {})
    point_payload: Any = None
    quantile_payload: Any = None
    if isinstance(payload, Mapping):
        point_payload = (
            payload.get("mean")
            if "mean" in payload
            else payload.get("median", payload.get("prediction", payload.get("forecast", payload.get("path"))))
        )
        quantile_payload = payload.get("quantiles", payload.get("quantile_forecasts"))
    elif isinstance(payload, tuple) and len(payload) >= 2:
        point_payload = payload[0]
        quantile_payload = payload[1]
    else:
        point_payload = payload

    path = _coerce_path(point_payload, horizon=int(horizon), fallback_last=float(fallback_last))
    q_map: dict[str, tuple[float, ...]] = {}
    if quantile_payload is not None:
        if isinstance(quantile_payload, Mapping):
            for key, value in quantile_payload.items():
                q_map[_quantile_key(_safe_float(key, 0.0))] = _coerce_path(
                    value,
                    horizon=int(horizon),
                    fallback_last=float(path[-1] if path else fallback_last),
                )
        else:
            q_arr = np.asarray(quantile_payload, dtype=np.float32)
            q_arr = np.nan_to_num(q_arr, nan=float(fallback_last), posinf=float(fallback_last), neginf=float(fallback_last))
            if q_arr.ndim >= 3:
                q_arr = q_arr.reshape(-1, q_arr.shape[-2], q_arr.shape[-1])[0]
            if q_arr.ndim == 2:
                if q_arr.shape[0] == len(tuple(quantiles)):
                    rows = q_arr
                else:
                    rows = q_arr.T
                for idx, q in enumerate(tuple(quantiles)):
                    if idx < rows.shape[0]:
                        q_map[_quantile_key(float(q))] = _coerce_path(
                            rows[idx],
                            horizon=int(horizon),
                            fallback_last=float(path[-1] if path else fallback_last),
                        )

    if not q_map:
        vol = max(_volatility_proxy(path), 1e-9)
        for q in tuple(quantiles):
            offset = (float(q) - 0.5) * 2.0 * vol
            q_map[_quantile_key(float(q))] = tuple(float(v + offset) for v in path)
    vol_proxy = _volatility_proxy(path)
    meta["forecast_payload_type"] = f"{type(payload).__module__}.{type(payload).__qualname__}"
    return TSFMForecastOutput(
        backend=str(backend),
        model_id=str(model_id),
        horizon_path=tuple(path),
        quantiles=dict(q_map),
        volatility_proxy=float(vol_proxy),
        metadata=meta,
    )


def resolve_tsfm_device(requested: Any = None) -> dict[str, Any]:
    try:
        torch = import_module("torch")
        resolution = resolve_torch_device(
            torch,
            requested=requested,
            env_var="TS_FOUNDATION_DEVICE",
            fallback_envs=("TORCH_DEVICE",),
        )
        return {
            "requested": resolution.requested,
            "resolved": resolution.resolved,
            "source": resolution.source,
            "profile": resolution.profile,
            "cuda_available": bool(resolution.cuda_available),
            "accelerator_enabled": bool(resolution.accelerator_enabled),
            "disabled_accelerator_reason": str(resolution.disabled_accelerator_reason or ""),
            "hip_version": str(resolution.hip_version or ""),
            "rocm_available": bool(resolution.rocm_available),
            "torch_cuda_device_count": int(resolution.torch_cuda_device_count),
        }
    except Exception as exc:
        return {
            "requested": str(requested or "cpu"),
            "resolved": "cpu",
            "source": "fallback",
            "profile": "cpu",
            "cuda_available": False,
            "accelerator_enabled": False,
            "disabled_accelerator_reason": f"{type(exc).__name__}: {exc}",
            "hip_version": "",
            "rocm_available": False,
            "torch_cuda_device_count": 0,
        }


def tsfm_feature_ids_for_backend(backend: str, *, embedding_dim: int = TSFM_DEFAULT_EMBEDDING_DIM) -> list[str]:
    backend_key = normalize_tsfm_backend(backend)
    prefix = TSFM_BACKEND_PREFIXES[backend_key]
    ids = [f"{prefix}{name}" for name in TSFM_FORECAST_FEATURE_NAMES]
    if backend_key != "chronos":
        dim = max(1, int(embedding_dim or TSFM_DEFAULT_EMBEDDING_DIM))
        ids.extend(f"{prefix}embedding_{idx:03d}" for idx in range(dim))
    return ids


def all_tsfm_shadow_feature_ids(*, embedding_dim: int = TSFM_DEFAULT_EMBEDDING_DIM) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for backend in TSFM_SUPPORTED_BACKENDS:
        for fid in tsfm_feature_ids_for_backend(backend, embedding_dim=embedding_dim):
            if fid not in seen:
                seen.add(fid)
                out.append(fid)
    return out


def is_tsfm_shadow_feature_id(feature_id: str) -> bool:
    fid = str(feature_id or "").strip()
    if not fid:
        return False
    return any(fid.startswith(prefix) for prefix in TSFM_BACKEND_PREFIXES.values())


TSFM_ADAPTER_FEATURE_IDS = all_tsfm_shadow_feature_ids()


class BaseTSFMAdapter:
    config: TSFMAdapterConfig

    def __init__(self, config: TSFMAdapterConfig) -> None:
        self.config = config
        self.backend = config.normalized_backend()
        self.model_id = config.normalized_model_id()
        self.device_resolution = resolve_tsfm_device(config.device)

    def describe(self) -> dict[str, Any]:
        return {
            "adapter_schema_version": 1,
            "backend": str(self.backend),
            "model_id": str(self.model_id),
            "revision": str(self.config.revision or ""),
            "context_length": int(self.config.context_length),
            "horizon": int(self.config.horizon),
            "embedding_dim": int(self.config.embedding_dim),
            "device": dict(self.device_resolution),
            "local_files_only": bool(self.config.local_files_only),
            "direct_trading_authority": False,
            "stage": "shadow",
            "package_versions": self.package_versions(),
        }

    def package_versions(self) -> dict[str, str]:
        return {}

    def forecast(self, context: TSFMSeriesContext, *, horizon: int | None = None) -> TSFMForecastOutput:
        raise TSFMAdapterUnavailable(f"{self.backend}_forecast_unavailable")

    def embed(self, context: TSFMSeriesContext, *, dim: int | None = None) -> TSFMEmbeddingOutput:
        raise TSFMAdapterUnavailable(f"{self.backend}_embedding_unavailable")


class FakeDeterministicTSFMAdapter(BaseTSFMAdapter):
    """Deterministic adapter used for tests and explicit safe fallback."""

    def __init__(self, config: TSFMAdapterConfig | None = None) -> None:
        super().__init__(config or TSFMAdapterConfig(backend="fake", model_id="deterministic-fake"))
        self.backend = "fake"
        self.model_id = self.config.model_id or "deterministic-fake"

    def package_versions(self) -> dict[str, str]:
        return {}

    def forecast(self, context: TSFMSeriesContext, *, horizon: int | None = None) -> TSFMForecastOutput:
        context.validate_pit()
        h = max(1, int(horizon if horizon is not None else self.config.horizon))
        values = _finite_array(context.values)
        last = float(values[-1])
        if values.size >= 2:
            drift = float(np.nanmean(np.diff(values[-min(values.size, 8) :])))
        else:
            drift = 0.0
        vol = max(_volatility_proxy(values), 1e-9)
        path = tuple(float(last + drift * (idx + 1)) for idx in range(h))
        q_map: dict[str, tuple[float, ...]] = {}
        for q in self.config.quantiles:
            offset = (float(q) - 0.5) * 2.0 * vol
            q_map[_quantile_key(float(q))] = tuple(float(value + offset) for value in path)
        return TSFMForecastOutput(
            backend="fake",
            model_id=str(self.model_id),
            horizon_path=path,
            quantiles=q_map,
            volatility_proxy=float(vol),
            metadata={
                "deterministic": True,
                "context_hash": hashlib.sha256(values.tobytes()).hexdigest(),
                "direct_trading_authority": False,
            },
        )

    def embed(self, context: TSFMSeriesContext, *, dim: int | None = None) -> TSFMEmbeddingOutput:
        context.validate_pit()
        feature_dim = max(1, int(dim if dim is not None else self.config.embedding_dim))
        values = _finite_array(context.values)
        if values.size:
            mean = float(np.mean(values))
            std = float(np.std(values)) or 1.0
            norm = (values - mean) / std
        else:
            norm = np.zeros((1,), dtype=np.float32)
        digest = hashlib.sha256(norm.astype(np.float32).tobytes()).digest()
        raw = np.frombuffer(digest * ((feature_dim // len(digest)) + 1), dtype=np.uint8)[:feature_dim]
        emb = ((raw.astype(np.float32) / 127.5) - 1.0).clip(-1.0, 1.0)
        feature_ids = tuple(tsfm_feature_ids_for_backend("fake", embedding_dim=feature_dim)[len(TSFM_FORECAST_FEATURE_NAMES) :])
        return TSFMEmbeddingOutput(
            backend="fake",
            model_id=str(self.model_id),
            feature_ids=feature_ids,
            values=tuple(float(v) for v in emb.tolist()),
            metadata={"deterministic": True, "embedding_source": "hash_projection"},
        )


class Chronos2Adapter(BaseTSFMAdapter):
    def package_versions(self) -> dict[str, str]:
        return {"chronos-forecasting": _package_version("chronos-forecasting")}

    def _pipeline(self) -> Any:
        from engine.strategy.ts_foundation_encoder import _load_chronos_pipeline

        return _load_chronos_pipeline(
            model_id=str(self.model_id),
            device=str(self.device_resolution.get("resolved") or "cpu"),
            local_files_only=bool(self.config.local_files_only),
            revision=str(self.config.revision or ""),
        )

    def forecast(self, context: TSFMSeriesContext, *, horizon: int | None = None) -> TSFMForecastOutput:
        context.validate_pit()
        h = max(1, int(horizon if horizon is not None else self.config.horizon))
        values = _finite_array(context.values)
        last = float(values[-1])
        try:
            pipeline = self._pipeline()
            payload = _call_forecast_methods(
                pipeline,
                values=values,
                timestamps_ms=context.timestamps_ms,
                horizon=h,
                quantiles=self.config.quantiles,
            )
        except Exception as exc:
            raise TSFMAdapterUnavailable(f"chronos_forecast_unavailable:{type(exc).__name__}: {exc}") from exc
        return _coerce_forecast_payload(
            payload,
            backend="chronos",
            model_id=str(self.model_id),
            horizon=h,
            quantiles=self.config.quantiles,
            fallback_last=last,
            metadata_extra={"direct_trading_authority": False, **self.describe()},
        )

    def embed(self, context: TSFMSeriesContext, *, dim: int | None = None) -> TSFMEmbeddingOutput:
        context.validate_pit()
        feature_dim = max(1, int(dim if dim is not None else self.config.embedding_dim))
        try:
            from engine.strategy.ts_foundation_encoder import (
                _call_embed,
                _normalized_target_values,
                _project_embedding,
                get_chronos_feature_ids,
            )

            points = tuple((int(ts), float(value)) for ts, value in zip(context.timestamps_ms, context.values))
            pipeline = self._pipeline()
            normalized = _normalized_target_values(points)
            raw = _call_embed(pipeline, points, normalized)
            projected = _project_embedding(raw, dim=int(feature_dim))
        except Exception as exc:
            raise TSFMAdapterUnavailable(f"chronos_embedding_unavailable:{type(exc).__name__}: {exc}") from exc
        return TSFMEmbeddingOutput(
            backend="chronos",
            model_id=str(self.model_id),
            feature_ids=tuple(get_chronos_feature_ids(feature_dim)),
            values=tuple(float(v) for v in projected.tolist()),
            metadata={"embedding_source": "chronos_embed", **self.describe()},
        )


class TimesFMAdapter(BaseTSFMAdapter):
    def package_versions(self) -> dict[str, str]:
        return {"timesfm": _package_version("timesfm")}

    def _model(self) -> Any:
        model = self.config.extra.get("model") if isinstance(self.config.extra, Mapping) else None
        if model is not None:
            return model
        if bool(self.config.local_files_only):
            raise TSFMAdapterUnavailable("timesfm_local_files_only_requires_explicit_model_instance")
        try:
            timesfm = import_module("timesfm")
            hparams_cls = getattr(timesfm, "TimesFmHparams", None)
            checkpoint_cls = getattr(timesfm, "TimesFmCheckpoint", None)
            model_cls = getattr(timesfm, "TimesFm", None)
            if model_cls is None:
                raise ImportError("timesfm.TimesFm is unavailable")
            if hparams_cls is None or checkpoint_cls is None:
                return model_cls()
            backend = "gpu" if str(self.device_resolution.get("resolved") or "cpu").startswith("cuda") else "cpu"
            return model_cls(
                hparams=hparams_cls(
                    backend=backend,
                    context_len=int(self.config.context_length),
                    horizon_len=int(self.config.horizon),
                ),
                checkpoint=checkpoint_cls(huggingface_repo_id=str(self.model_id)),
            )
        except Exception as exc:
            raise TSFMAdapterUnavailable(f"timesfm_load_unavailable:{type(exc).__name__}: {exc}") from exc

    def forecast(self, context: TSFMSeriesContext, *, horizon: int | None = None) -> TSFMForecastOutput:
        context.validate_pit()
        h = max(1, int(horizon if horizon is not None else self.config.horizon))
        values = _finite_array(context.values)
        last = float(values[-1])
        try:
            model = self._model()
            if not hasattr(model, "forecast"):
                raise AttributeError("timesfm model has no forecast method")
            payload = model.forecast([values.astype(np.float32)], freq=[0])
        except Exception as exc:
            raise TSFMAdapterUnavailable(f"timesfm_forecast_unavailable:{type(exc).__name__}: {exc}") from exc
        return _coerce_forecast_payload(
            payload,
            backend="timesfm",
            model_id=str(self.model_id),
            horizon=h,
            quantiles=self.config.quantiles,
            fallback_last=last,
            metadata_extra={"direct_trading_authority": False, **self.describe()},
        )


class _OptionalCallableAdapter(BaseTSFMAdapter):
    package_name = ""
    forecast_method_names = ("forecast", "predict")

    def package_versions(self) -> dict[str, str]:
        return {self.package_name: _package_version(self.package_name)} if self.package_name else {}

    def _model(self) -> Any:
        model = self.config.extra.get("model") if isinstance(self.config.extra, Mapping) else None
        if model is not None:
            return model
        raise TSFMAdapterUnavailable(f"{self.backend}_requires_explicit_model_instance")

    def forecast(self, context: TSFMSeriesContext, *, horizon: int | None = None) -> TSFMForecastOutput:
        context.validate_pit()
        h = max(1, int(horizon if horizon is not None else self.config.horizon))
        values = _finite_array(context.values)
        last = float(values[-1])
        model = self._model()
        last_exc: BaseException | None = None
        for method_name in self.forecast_method_names:
            method = getattr(model, method_name, None)
            if not callable(method):
                continue
            try:
                payload = method(values.astype(np.float32), prediction_length=h)
                return _coerce_forecast_payload(
                    payload,
                    backend=str(self.backend),
                    model_id=str(self.model_id),
                    horizon=h,
                    quantiles=self.config.quantiles,
                    fallback_last=last,
                    metadata_extra={"direct_trading_authority": False, **self.describe()},
                )
            except TypeError as exc:
                last_exc = exc
                try:
                    payload = method(values.astype(np.float32), h)
                    return _coerce_forecast_payload(
                        payload,
                        backend=str(self.backend),
                        model_id=str(self.model_id),
                        horizon=h,
                        quantiles=self.config.quantiles,
                        fallback_last=last,
                        metadata_extra={"direct_trading_authority": False, **self.describe()},
                    )
                except Exception as inner_exc:
                    last_exc = inner_exc
            except Exception as exc:
                last_exc = exc
        raise TSFMAdapterUnavailable(f"{self.backend}_forecast_unavailable:{last_exc}")


class MoiraiAdapter(_OptionalCallableAdapter):
    package_name = "uni2ts"

    def _model(self) -> Any:
        model = self.config.extra.get("model") if isinstance(self.config.extra, Mapping) else None
        if model is not None:
            return model
        try:
            import_module("uni2ts")
        except Exception as exc:
            raise TSFMAdapterUnavailable(f"moirai_dependency_unavailable:{type(exc).__name__}: {exc}") from exc
        raise TSFMAdapterUnavailable("moirai_requires_explicit_model_instance_or_repo_cache_binding")


class TotoAdapter(_OptionalCallableAdapter):
    package_name = "toto"

    def _model(self) -> Any:
        model = self.config.extra.get("model") if isinstance(self.config.extra, Mapping) else None
        if model is not None:
            return model
        try:
            import_module("toto")
        except Exception as exc:
            raise TSFMAdapterUnavailable(f"toto_dependency_unavailable:{type(exc).__name__}: {exc}") from exc
        raise TSFMAdapterUnavailable("toto_requires_explicit_model_instance_or_repo_cache_binding")


def _call_forecast_methods(
    pipeline: Any,
    *,
    values: np.ndarray,
    timestamps_ms: Sequence[int],
    horizon: int,
    quantiles: Sequence[float],
) -> Any:
    attempts: list[Any] = []
    predict_quantiles = getattr(pipeline, "predict_quantiles", None)
    if callable(predict_quantiles):
        attempts.extend(
            (
                lambda: predict_quantiles(
                    [values.astype(np.float32)],
                    prediction_length=int(horizon),
                    quantile_levels=list(quantiles),
                ),
                lambda: predict_quantiles(
                    values.astype(np.float32),
                    prediction_length=int(horizon),
                    quantile_levels=list(quantiles),
                ),
            )
        )
    predict = getattr(pipeline, "predict", None)
    if callable(predict):
        attempts.extend(
            (
                lambda: predict(
                    [values.astype(np.float32)],
                    prediction_length=int(horizon),
                    quantile_levels=list(quantiles),
                ),
                lambda: predict(values.astype(np.float32), prediction_length=int(horizon)),
                lambda: predict([values.astype(np.float32)], int(horizon)),
            )
        )
    predict_df = getattr(pipeline, "predict_df", None)
    if callable(predict_df):
        attempts.append(lambda: predict_df(_make_context_frame(timestamps_ms, values), prediction_length=int(horizon)))
    last_exc: BaseException | None = None
    for attempt in attempts:
        try:
            return attempt()
        except TypeError as exc:
            last_exc = exc
            continue
        except Exception as exc:
            last_exc = exc
            continue
    if last_exc is not None:
        raise last_exc
    raise TSFMAdapterUnavailable("provider_has_no_forecast_method")


def _make_context_frame(timestamps_ms: Sequence[int], values: np.ndarray) -> Any:
    pd = import_module("pandas")
    return pd.DataFrame(
        {
            "id": ["series"] * int(len(values)),
            "timestamp": pd.to_datetime([int(ts) for ts in timestamps_ms], unit="ms", utc=True),
            "target": values.astype(float).tolist(),
        }
    )


def create_tsfm_adapter(config: TSFMAdapterConfig) -> TSFMAdapter:
    backend = config.normalized_backend()
    cls_by_backend: dict[str, type[BaseTSFMAdapter]] = {
        "chronos": Chronos2Adapter,
        "timesfm": TimesFMAdapter,
        "moirai": MoiraiAdapter,
        "toto": TotoAdapter,
        "fake": FakeDeterministicTSFMAdapter,
    }
    adapter_cls = cls_by_backend[backend]
    adapter = adapter_cls(config)
    if backend == "fake":
        return adapter
    if str(config.fallback or "").strip().lower() != "fake":
        return adapter
    return _FallbackTSFMAdapter(primary=adapter, fallback=FakeDeterministicTSFMAdapter())


class _FallbackTSFMAdapter(BaseTSFMAdapter):
    def __init__(self, *, primary: TSFMAdapter, fallback: FakeDeterministicTSFMAdapter) -> None:
        self.primary = primary
        self.fallback = fallback
        super().__init__(primary.config)

    def describe(self) -> dict[str, Any]:
        return {**self.primary.describe(), "fallback": self.fallback.describe()}

    def forecast(self, context: TSFMSeriesContext, *, horizon: int | None = None) -> TSFMForecastOutput:
        try:
            return self.primary.forecast(context, horizon=horizon)
        except Exception as exc:
            output = self.fallback.forecast(context, horizon=horizon)
            meta = dict(output.metadata)
            meta["fallback_from_backend"] = str(self.primary.config.backend)
            meta["fallback_reason"] = f"{type(exc).__name__}: {exc}"
            return TSFMForecastOutput(
                backend=str(self.primary.config.backend),
                model_id=str(self.primary.config.normalized_model_id()),
                horizon_path=tuple(output.horizon_path),
                quantiles=dict(output.quantiles),
                volatility_proxy=float(output.volatility_proxy),
                metadata=meta,
            )

    def embed(self, context: TSFMSeriesContext, *, dim: int | None = None) -> TSFMEmbeddingOutput:
        try:
            return self.primary.embed(context, dim=dim)
        except Exception as exc:
            output = self.fallback.embed(context, dim=dim)
            meta = dict(output.metadata)
            meta["fallback_from_backend"] = str(self.primary.config.backend)
            meta["fallback_reason"] = f"{type(exc).__name__}: {exc}"
            return TSFMEmbeddingOutput(
                backend=str(self.primary.config.backend),
                model_id=str(self.primary.config.normalized_model_id()),
                feature_ids=tuple(
                    tsfm_feature_ids_for_backend(
                        str(self.primary.config.backend),
                        embedding_dim=len(output.values),
                    )[len(TSFM_FORECAST_FEATURE_NAMES) :]
                    or output.feature_ids
                ),
                values=tuple(output.values),
                metadata=meta,
            )


def adapter_config_from_env(backend: str | None = None) -> TSFMAdapterConfig:
    backend_key = normalize_tsfm_backend(backend or os.environ.get("TS_FOUNDATION_BACKEND") or "chronos")
    model_env = f"TSFM_{backend_key.upper()}_MODEL_ID"
    model_id = (
        os.environ.get(model_env)
        or os.environ.get("TSFM_BENCHMARK_MODEL_ID")
        or os.environ.get("TS_FOUNDATION_MODEL_ID")
        or os.environ.get("TS_FOUNDATION_CHRONOS_MODEL_ID")
        or ""
    )
    quantiles = tuple(
        _safe_float(part, 0.5)
        for part in str(os.environ.get("TSFM_BENCHMARK_QUANTILES") or "0.1,0.5,0.9").split(",")
        if str(part).strip()
    )
    return TSFMAdapterConfig(
        backend=backend_key,
        model_id=str(model_id),
        context_length=max(1, int(os.environ.get("TSFM_BENCHMARK_CONTEXT_ROWS") or os.environ.get("TS_FOUNDATION_CONTEXT_ROWS") or 256)),
        horizon=max(1, int(os.environ.get("TSFM_BENCHMARK_HORIZON_ROWS") or os.environ.get("TS_FOUNDATION_HORIZON_ROWS") or 1)),
        embedding_dim=max(1, int(os.environ.get("TSFM_BENCHMARK_EMBEDDING_DIM") or os.environ.get("TS_FOUNDATION_EMBEDDING_DIM") or TSFM_DEFAULT_EMBEDDING_DIM)),
        device=str(os.environ.get("TSFM_BENCHMARK_DEVICE") or os.environ.get("TS_FOUNDATION_DEVICE") or "cpu"),
        local_files_only=str(os.environ.get("TS_FOUNDATION_LOCAL_FILES_ONLY") or "1").strip().lower()
        not in {"0", "false", "no", "off"},
        quantiles=quantiles or (0.1, 0.5, 0.9),
        fallback=str(os.environ.get("TSFM_BENCHMARK_FALLBACK") or "skip"),
        revision=str(os.environ.get("TS_FOUNDATION_MODEL_REVISION") or ""),
    )


__all__ = [
    "TSFM_ADAPTER_FEATURE_IDS",
    "TSFMAdapter",
    "TSFMAdapterConfig",
    "TSFMAdapterUnavailable",
    "TSFMEmbeddingOutput",
    "TSFMForecastOutput",
    "TSFMSeriesContext",
    "TSFM_BACKEND_PREFIXES",
    "TSFM_DEFAULT_EMBEDDING_DIM",
    "TSFM_FORECAST_FEATURE_NAMES",
    "TSFM_SHADOW_GROUP",
    "TSFM_SUPPORTED_BACKENDS",
    "adapter_config_from_env",
    "all_tsfm_shadow_feature_ids",
    "create_tsfm_adapter",
    "is_tsfm_shadow_feature_id",
    "resolve_tsfm_device",
    "tsfm_feature_ids_for_backend",
]
