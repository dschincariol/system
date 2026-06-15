"""PatchTST sequence model family for multi-horizon return forecasting."""

from __future__ import annotations
import logging

import json
import math
import os
import time
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import torch
import torch.nn as nn

from engine.artifacts.serialization import dumps_torch_payload, loads_torch_payload
from engine.artifacts.store import LocalArtifactStore
from engine.model_registry import register_model, register_model_family
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect, init_db
from engine.strategy import feature_registry
from engine.strategy.feature_registry import build_feature_snapshot, feature_set_tag_from_ids
from engine.strategy.model_lifecycle import (
    load_lifecycle_plan,
    record_version_performance,
    register_model_version,
    update_model_version_status,
    version_from_ts,
)
from engine.strategy.models.lgbm_regressor import (
    _artifact_payload_from_alias,
    _expected_columns,
    _preprocess_feature_matrix,
    _safe_float,
)
from engine.strategy.models.lgbm_regressor import _resolve_retrain_schema_guard
from engine.strategy.ensemble.oos_store import upsert_oos_predictions
from engine.strategy.ood import build_ood_profile, score_ood, summarize_ood_profile

FAMILY = "patchtst"
DEFAULT_SEQ_LEN = int(os.environ.get("PATCHTST_SEQ_LEN", "128"))
DEFAULT_N_HORIZONS = int(os.environ.get("PATCHTST_N_HORIZONS", "6"))
DEFAULT_PATCH_LEN = int(os.environ.get("PATCHTST_PATCH_LEN", "16"))
DEFAULT_STRIDE = int(os.environ.get("PATCHTST_STRIDE", "8"))
DEFAULT_LAYERS = int(os.environ.get("PATCHTST_LAYERS", "3"))
DEFAULT_HEADS = int(os.environ.get("PATCHTST_HEADS", "4"))
DEFAULT_D_MODEL = int(os.environ.get("PATCHTST_D_MODEL", "64"))
DEFAULT_DROPOUT = float(os.environ.get("PATCHTST_DROPOUT", "0.1"))
DEFAULT_MIN_SAMPLES = int(os.environ.get("PATCHTST_MIN_SAMPLES", "20"))
DEFAULT_LOOKBACK_DAYS = int(os.environ.get("PATCHTST_LOOKBACK_DAYS", "365"))
DEFAULT_HORIZON_S = int(os.environ.get("PATCHTST_HORIZON_S", os.environ.get("MODEL_HORIZON_MEDIUM_S", "3600")))


def _register_family() -> None:
    try:
        register_model_family(
            FAMILY,
            training_entrypoint="engine.strategy.jobs.train_patchtst_models",
            inference_entrypoint="engine.strategy.models.patchtst.PatchTSTRegressor",
            default_stage="shadow",
            promotion_guard="engine.strategy.promotion_guard.assess_challenger",
        )
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


_register_family()

if os.environ.get("PATCHTST_USE_CUDA", "0") == "1" and torch.cuda.is_available():
    DEFAULT_DEVICE = torch.device("cuda")
else:
    DEFAULT_DEVICE = torch.device("cpu")


def _set_seed(seed: int) -> None:
    np.random.seed(int(seed))
    torch.manual_seed(int(seed))
    try:
        torch.use_deterministic_algorithms(True)
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def _feature_schema(
    feature_ids: Sequence[Any],
    *,
    seq_len: int,
    n_horizons: int,
    preprocessing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    ids = [str(item).strip() for item in list(feature_ids or []) if str(item or "").strip()]
    schema = {
        "feature_ids": list(ids),
        "feature_set_tag": str(feature_set_tag_from_ids(list(ids))),
        "feature_count": int(len(ids)),
        "sequence_schema": {
            "seq_len": int(seq_len),
            "n_horizons": int(n_horizons),
            "layout": "batch,seq_len,n_features",
        },
    }
    if isinstance(preprocessing, Mapping) and preprocessing:
        schema["preprocessing"] = dict(preprocessing)
    return schema


def _emit_config_feature_schema_load_failure(
    *,
    config: Mapping[str, Any],
    artifact_schema: Mapping[str, Any],
    current_schema: Mapping[str, Any],
    reason: str,
    error: BaseException,
) -> None:
    artifact_tag = str(artifact_schema.get("feature_set_tag") or "").strip()
    current_tag = str(current_schema.get("feature_set_tag") or "").strip()
    extra = {
        "model_name": str(config.get("model_name") or FAMILY),
        "family": FAMILY,
        "artifact_feature_set_tag": str(artifact_tag),
        "current_feature_set_tag": str(current_tag),
        "artifact_feature_ids": list(artifact_schema.get("feature_ids") or []),
        "current_feature_ids": list(current_schema.get("feature_ids") or []),
        "reason": str(reason),
    }
    logger = logging.getLogger(__name__)
    logger.error(
        "feature_schema_load_validation_failed model_name=%s family=%s artifact_feature_set_tag=%s current_feature_set_tag=%s reason=%s",
        extra["model_name"],
        FAMILY,
        artifact_tag or "<missing>",
        current_tag or "<missing>",
        str(reason),
    )
    log_failure(
        logger,
        event="feature_schema_load_validation_failed",
        code="FEATURE_SCHEMA_LOAD_VALIDATION_FAILED",
        message=str(error),
        error=error,
        level=logging.ERROR,
        component="engine.strategy.models.patchtst",
        extra=extra,
        persist=True,
    )
    try:
        from engine.runtime.alerts import emit_runtime_alert

        emit_runtime_alert(
            event_title="Model feature schema load validation failed",
            symbol="SYSTEM",
            severity="ERROR",
            rule_id="FEATURE_SCHEMA_LOAD_VALIDATION_FAILED",
            explain=extra,
            detail={"error": str(error)},
            source="model_load_validation",
            dedupe_scope=f"{FAMILY}:{extra['model_name']}:{reason}:{artifact_tag}:{current_tag}",
        )
    except Exception:
        logger.debug("Ignored recoverable exception.", exc_info=True)


def _assert_config_feature_schema_current(config: Mapping[str, Any]) -> list[str]:
    feature_ids = [
        str(item).strip()
        for item in list(config.get("feature_ids") or [])
        if str(item or "").strip()
    ]
    artifact_schema = dict(config.get("feature_schema") or {}) if isinstance(config.get("feature_schema"), Mapping) else {}
    if not feature_ids and isinstance(artifact_schema.get("feature_ids"), list):
        feature_ids = [
            str(item).strip()
            for item in list(artifact_schema.get("feature_ids") or [])
            if str(item or "").strip()
        ]
    current = _expected_columns(
        feature_ids,
        model_name=str(config.get("model_name") or FAMILY),
        model_spec={"feature_ids": feature_ids},
    )
    current_schema = _feature_schema(
        current,
        seq_len=int(config.get("seq_len") or DEFAULT_SEQ_LEN),
        n_horizons=int(config.get("n_horizons") or DEFAULT_N_HORIZONS),
    )
    artifact_ids = [
        str(item).strip()
        for item in list(artifact_schema.get("feature_ids") or feature_ids)
        if str(item or "").strip()
    ]
    artifact_tag = str(artifact_schema.get("feature_set_tag") or "").strip()
    current_tag = str(current_schema.get("feature_set_tag") or "").strip()

    error: ValueError | None = None
    reason = ""
    if not artifact_tag:
        reason = "missing_feature_set_tag"
        error = ValueError(
            "feature_schema_drift: artifact_feature_set_tag=<missing> "
            f"current_feature_set_tag={current_tag or '<missing>'}"
        )
    elif artifact_ids != feature_ids:
        reason = "artifact_column_list_mismatch"
        error = ValueError(
            "feature_schema_drift: "
            f"artifact_feature_set_tag={artifact_tag} current_feature_set_tag={current_tag} "
            f"artifact_columns={artifact_ids} config_columns={feature_ids}"
        )
    elif current != feature_ids:
        reason = "registry_column_list_mismatch"
        error = ValueError(
            "feature_schema_drift: "
            f"artifact_feature_set_tag={artifact_tag} current_feature_set_tag={current_tag} "
            f"artifact_columns={feature_ids} current_columns={current}"
        )
    elif artifact_tag != current_tag:
        reason = "feature_set_tag_mismatch"
        error = ValueError(
            "feature_schema_drift: "
            f"artifact_feature_set_tag={artifact_tag} current_feature_set_tag={current_tag} "
            f"artifact_columns={feature_ids} current_columns={current}"
        )
    if error is not None:
        _emit_config_feature_schema_load_failure(
            config=config,
            artifact_schema={**dict(artifact_schema), "feature_ids": list(artifact_ids)},
            current_schema=current_schema,
            reason=reason,
            error=error,
        )
        raise error
    return feature_ids


def _resolve_load_device(config: Mapping[str, Any]) -> torch.device:
    trained_device = str(config.get("device_at_train") or "cpu").strip().lower()
    if trained_device.startswith("cuda") and torch.cuda.is_available():
        return torch.device("cuda")
    if trained_device.startswith("cuda"):
        logging.getLogger(__name__).warning(
            "patchtst_cuda_trained_loaded_on_cpu model_name=%s device_at_train=%s",
            str(config.get("model_name") or FAMILY),
            str(config.get("device_at_train") or "cuda"),
        )
    return torch.device("cpu")


def _sequence_array_from_features(features: Any, columns: Sequence[str], *, seq_len: int) -> np.ndarray:
    cols = [str(col) for col in list(columns or [])]
    if not cols:
        raise ValueError("patchtst_feature_columns_required")
    if isinstance(features, np.ndarray):
        arr = np.asarray(features, dtype=np.float32)
        if arr.ndim != 3:
            raise ValueError("patchtst_requires_3d_input")
        if int(arr.shape[1]) != int(seq_len):
            raise ValueError(f"patchtst_seq_len_mismatch:{int(arr.shape[1])}:{int(seq_len)}")
        if int(arr.shape[2]) != int(len(cols)):
            raise ValueError(f"patchtst_feature_count_mismatch:{int(arr.shape[2])}:{int(len(cols))}")
        return arr.astype(np.float32, copy=False)

    if isinstance(features, Sequence) and not isinstance(features, (str, bytes, bytearray)):
        samples = list(features)
        if samples and all(isinstance(sample, Sequence) and not isinstance(sample, Mapping) for sample in samples):
            rows = []
            for sample in samples:
                steps = list(sample)
                if len(steps) != int(seq_len):
                    raise ValueError("patchtst_sequence_length_mismatch")
                rows.append([
                    [_safe_float(dict(step).get(col), float("nan")) if isinstance(step, Mapping) else float("nan") for col in cols]
                    for step in steps
                ])
            return np.asarray(rows, dtype=np.float32)
    raise TypeError(f"unsupported_patchtst_feature_payload:{type(features).__name__}")


def _preprocess_sequence_array(
    matrix: np.ndarray,
    columns: Sequence[str],
    *,
    feature_schema: Mapping[str, Any] | None = None,
    phase: str = "serve",
    model_name: str = "",
    fit_preprocessing: bool = False,
) -> tuple[np.ndarray, dict[str, Any], dict[str, Any]]:
    arr = np.asarray(matrix, dtype=np.float32)
    if arr.ndim != 3:
        raise ValueError("patchtst_requires_3d_input")
    n_samples, seq_len, n_features = arr.shape
    flat, preprocessing, accounting = _preprocess_feature_matrix(
        arr.reshape(int(n_samples) * int(seq_len), int(n_features)),
        columns,
        feature_schema=feature_schema,
        phase=phase,
        model_name=model_name,
        fit_preprocessing=fit_preprocessing,
    )
    return flat.reshape(int(n_samples), int(seq_len), int(n_features)).astype(np.float32), preprocessing, accounting


class PatchTST(nn.Module):
    """Compact PatchTST encoder using patch tokens and a Transformer encoder."""

    def __init__(
        self,
        *,
        seq_len: int,
        n_features: int,
        n_horizons: int,
        patch_len: int = DEFAULT_PATCH_LEN,
        stride: int = DEFAULT_STRIDE,
        d_model: int = DEFAULT_D_MODEL,
        n_layers: int = DEFAULT_LAYERS,
        n_heads: int = DEFAULT_HEADS,
        dropout: float = DEFAULT_DROPOUT,
    ) -> None:
        super().__init__()
        self.seq_len = int(seq_len)
        self.n_features = int(n_features)
        self.n_horizons = int(n_horizons)
        self.patch_len = int(patch_len)
        self.stride = int(stride)
        self.d_model = int(d_model)
        self.n_layers = int(n_layers)
        self.n_heads = int(n_heads)
        self.dropout = float(dropout)
        if self.patch_len <= 0 or self.stride <= 0:
            raise ValueError("patch_len_and_stride_must_be_positive")
        if self.seq_len < self.patch_len:
            raise ValueError("seq_len_must_cover_one_patch")
        self.n_patches = 1 + ((self.seq_len - self.patch_len) // self.stride)
        self.patch_projection = nn.Linear(self.patch_len * self.n_features, self.d_model)
        self.position = nn.Parameter(torch.zeros(1, self.n_patches, self.d_model))
        layer = nn.TransformerEncoderLayer(
            d_model=self.d_model,
            nhead=self.n_heads,
            dim_feedforward=self.d_model * 4,
            dropout=self.dropout,
            batch_first=True,
            activation="gelu",
            norm_first=True,
        )
        self.encoder = nn.TransformerEncoder(layer, num_layers=self.n_layers)
        self.norm = nn.LayerNorm(self.d_model)
        self.head = nn.Linear(self.d_model, self.n_horizons)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if x.ndim != 3:
            raise ValueError("patchtst_forward_requires_3d")
        patches = x.unfold(dimension=1, size=self.patch_len, step=self.stride)
        patches = patches.permute(0, 1, 3, 2).contiguous()
        patches = patches.reshape(x.shape[0], self.n_patches, self.patch_len * self.n_features)
        tokens = self.patch_projection(patches) + self.position
        encoded = self.encoder(tokens)
        pooled = self.norm(encoded.mean(dim=1))
        return self.head(pooled)


class PatchTSTRegressor:
    """Train/serve wrapper around PatchTST with saved config and state dict."""

    family = FAMILY
    model_kind = "patchtst"

    def __init__(
        self,
        *,
        model_name: str = FAMILY,
        feature_ids: Sequence[Any] | None = None,
        seq_len: int = DEFAULT_SEQ_LEN,
        n_horizons: int = DEFAULT_N_HORIZONS,
        patch_len: int = DEFAULT_PATCH_LEN,
        stride: int = DEFAULT_STRIDE,
        n_layers: int = DEFAULT_LAYERS,
        n_heads: int = DEFAULT_HEADS,
        d_model: int = DEFAULT_D_MODEL,
        dropout: float = DEFAULT_DROPOUT,
        seed: int = 42,
        device: str | torch.device | None = None,
    ) -> None:
        self.model_name = str(model_name or FAMILY).strip() or FAMILY
        self.seq_len = int(seq_len)
        self.n_horizons = int(n_horizons)
        self.patch_len = int(patch_len)
        self.stride = int(stride)
        self.n_layers = int(n_layers)
        self.n_heads = int(n_heads)
        self.d_model = int(d_model)
        self.dropout = float(dropout)
        self.seed = int(seed)
        self.device = torch.device(device) if device is not None else DEFAULT_DEVICE
        self.feature_ids = _expected_columns(feature_ids, model_name=self.model_name)
        self.model: PatchTST | None = None
        self.x_mean: np.ndarray | None = None
        self.x_std: np.ndarray | None = None
        self.y_mean: np.ndarray | None = None
        self.y_std: np.ndarray | None = None
        self.feature_preprocessing: dict[str, Any] = {}
        self.training_metrics: dict[str, Any] = {}
        self.ood_profile: dict[str, Any] = {}
        if self.feature_ids:
            self._build_model(n_features=len(self.feature_ids))

    @property
    def feature_schema(self) -> dict[str, Any]:
        return _feature_schema(
            self.feature_ids,
            seq_len=self.seq_len,
            n_horizons=self.n_horizons,
            preprocessing=getattr(self, "feature_preprocessing", {}),
        )

    def _build_model(self, *, n_features: int) -> None:
        _set_seed(self.seed)
        self.model = PatchTST(
            seq_len=self.seq_len,
            n_features=int(n_features),
            n_horizons=self.n_horizons,
            patch_len=self.patch_len,
            stride=self.stride,
            d_model=self.d_model,
            n_layers=self.n_layers,
            n_heads=self.n_heads,
            dropout=self.dropout,
        ).to(self.device)

    def fit(
        self,
        X: Any,
        y: Any,
        *,
        epochs: int = 30,
        lr: float = 1e-3,
        weight_decay: float = 1e-4,
        grad_clip: float = 1.0,
        return_losses: bool = False,
    ) -> list[float] | "PatchTSTRegressor":
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_raw = _sequence_array_from_features(X, columns, seq_len=self.seq_len)
        X_arr, preprocessing, _accounting = _preprocess_sequence_array(
            X_raw,
            columns,
            phase="train",
            model_name=self.model_name,
            fit_preprocessing=True,
        )
        y_arr = np.asarray(y, dtype=np.float32)
        if y_arr.ndim == 1:
            y_arr = y_arr.reshape(-1, 1)
        if int(X_arr.shape[0]) != int(y_arr.shape[0]):
            raise ValueError("patchtst_row_count_mismatch")
        if int(y_arr.shape[1]) != int(self.n_horizons):
            self.n_horizons = int(y_arr.shape[1])
        self.feature_ids = list(columns)
        self.feature_preprocessing = dict(preprocessing or {})
        self._build_model(n_features=int(X_arr.shape[2]))
        assert self.model is not None

        self.x_mean = X_arr.mean(axis=(0, 1)).astype(np.float32)
        self.x_std = X_arr.std(axis=(0, 1)).astype(np.float32)
        self.x_std = np.where(self.x_std < 1e-6, 1.0, self.x_std).astype(np.float32)
        self.y_mean = y_arr.mean(axis=0).astype(np.float32)
        self.y_std = y_arr.std(axis=0).astype(np.float32)
        self.y_std = np.where(self.y_std < 1e-6, 1.0, self.y_std).astype(np.float32)
        self.ood_profile = build_ood_profile(X_arr[:, -1, :], columns)

        Xn = ((X_arr - self.x_mean.reshape(1, 1, -1)) / self.x_std.reshape(1, 1, -1)).astype(np.float32)
        yn = ((y_arr - self.y_mean.reshape(1, -1)) / self.y_std.reshape(1, -1)).astype(np.float32)
        xt = torch.from_numpy(Xn).to(self.device)
        yt = torch.from_numpy(yn).to(self.device)

        _set_seed(self.seed)
        opt = torch.optim.AdamW(self.model.parameters(), lr=float(lr), weight_decay=float(weight_decay))
        sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=max(1, int(epochs)))
        loss_fn = nn.MSELoss()
        losses: list[float] = []
        self.model.train()
        for _epoch in range(int(epochs)):
            opt.zero_grad(set_to_none=True)
            pred = self.model(xt)
            loss = loss_fn(pred, yt)
            loss.backward()
            if float(grad_clip) > 0.0:
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), float(grad_clip))
            opt.step()
            sched.step()
            losses.append(float(loss.detach().cpu().item()))

        self.model.eval()
        with torch.no_grad():
            pred_n = self.model(xt).detach().cpu().numpy().astype(np.float32)
        pred = pred_n * self.y_std.reshape(1, -1) + self.y_mean.reshape(1, -1)
        rmse = float(np.sqrt(np.mean((pred - y_arr) ** 2)))
        self.training_metrics = {
            "n_train": int(y_arr.shape[0]),
            "rmse": float(rmse),
            "loss_initial": float(losses[0] if losses else 0.0),
            "loss_final": float(losses[-1] if losses else 0.0),
            "model_family": FAMILY,
            "model_kind": self.model_kind,
            "feature_schema": dict(self.feature_schema),
            "ood_profile_summary": summarize_ood_profile(self.ood_profile),
        }
        return losses if return_losses else self

    def predict(self, X: Any) -> np.ndarray:
        if self.model is None or self.x_mean is None or self.x_std is None or self.y_mean is None or self.y_std is None:
            raise RuntimeError("patchtst_model_not_fitted")
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_raw = _sequence_array_from_features(X, columns, seq_len=self.seq_len)
        X_arr, _preprocessing, _accounting = _preprocess_sequence_array(
            X_raw,
            columns,
            feature_schema=self.feature_schema,
            phase="serve",
            model_name=self.model_name,
        )
        Xn = ((X_arr - self.x_mean.reshape(1, 1, -1)) / self.x_std.reshape(1, 1, -1)).astype(np.float32)
        self.model.eval()
        with torch.no_grad():
            pred_n = self.model(torch.from_numpy(Xn).to(self.device)).detach().cpu().numpy().astype(np.float32)
        return pred_n * self.y_std.reshape(1, -1) + self.y_mean.reshape(1, -1)

    def save(self, directory: str | Path) -> Path:
        target = Path(directory)
        target.mkdir(parents=True, exist_ok=True)
        if self.model is None:
            raise RuntimeError("patchtst_model_not_fitted")
        config = self._config_payload()
        (target / "config.json").write_text(json.dumps(config, separators=(",", ":"), sort_keys=True), encoding="utf-8")
        (target / "state.pt").write_bytes(dumps_torch_payload({"state_dict": self.model.state_dict()}))
        return target

    @classmethod
    def load(cls, directory: str | Path) -> "PatchTSTRegressor":
        target = Path(directory)
        config = json.loads((target / "config.json").read_text(encoding="utf-8"))
        feature_ids = _assert_config_feature_schema_current(config)
        load_device = _resolve_load_device(config)
        obj = cls(
            model_name=str(config.get("model_name") or FAMILY),
            feature_ids=feature_ids,
            seq_len=int(config.get("seq_len") or DEFAULT_SEQ_LEN),
            n_horizons=int(config.get("n_horizons") or DEFAULT_N_HORIZONS),
            patch_len=int(config.get("patch_len") or DEFAULT_PATCH_LEN),
            stride=int(config.get("stride") or DEFAULT_STRIDE),
            n_layers=int(config.get("n_layers") or DEFAULT_LAYERS),
            n_heads=int(config.get("n_heads") or DEFAULT_HEADS),
            d_model=int(config.get("d_model") or DEFAULT_D_MODEL),
            dropout=float(config.get("dropout") or 0.0),
            seed=int(config.get("seed") or 42),
            device=load_device,
        )
        obj.x_mean = np.asarray(config.get("x_mean"), dtype=np.float32)
        obj.x_std = np.asarray(config.get("x_std"), dtype=np.float32)
        obj.y_mean = np.asarray(config.get("y_mean"), dtype=np.float32)
        obj.y_std = np.asarray(config.get("y_std"), dtype=np.float32)
        schema = config.get("feature_schema")
        obj.feature_preprocessing = (
            dict((schema or {}).get("preprocessing") or {})
            if isinstance(schema, Mapping)
            else {}
        )
        obj.training_metrics = dict(config.get("training_metrics") or {})
        obj.ood_profile = dict(config.get("ood_profile") or {})
        if obj.model is None:
            obj._build_model(n_features=len(obj.feature_ids))
        payload = loads_torch_payload((target / "state.pt").read_bytes(), map_location=str(load_device))
        obj.model.load_state_dict(dict(payload.get("state_dict") or {}))
        obj.model.eval()
        return obj

    def _config_payload(self) -> dict[str, Any]:
        return {
            "model_name": str(self.model_name),
            "feature_ids": list(self.feature_ids),
            "feature_schema": dict(self.feature_schema),
            "seq_len": int(self.seq_len),
            "n_horizons": int(self.n_horizons),
            "patch_len": int(self.patch_len),
            "stride": int(self.stride),
            "n_layers": int(self.n_layers),
            "n_heads": int(self.n_heads),
            "d_model": int(self.d_model),
            "dropout": float(self.dropout),
            "seed": int(self.seed),
            "device_at_train": str(self.device.type),
            "x_mean": [] if self.x_mean is None else self.x_mean.astype(float).tolist(),
            "x_std": [] if self.x_std is None else self.x_std.astype(float).tolist(),
            "y_mean": [] if self.y_mean is None else self.y_mean.astype(float).tolist(),
            "y_std": [] if self.y_std is None else self.y_std.astype(float).tolist(),
            "training_metrics": dict(self.training_metrics or {}),
            "ood_profile": dict(getattr(self, "ood_profile", {}) or {}),
        }

    def to_bytes(self) -> bytes:
        if self.model is None:
            raise RuntimeError("patchtst_model_not_fitted")
        return dumps_torch_payload(
            {"config": self._config_payload(), "state_dict": self.model.state_dict()}
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> "PatchTSTRegressor":
        raw = loads_torch_payload(payload)
        config = dict(raw.get("config") or {})
        feature_ids = _assert_config_feature_schema_current(config)
        load_device = _resolve_load_device(config)
        obj = cls(
            model_name=str(config.get("model_name") or FAMILY),
            feature_ids=feature_ids,
            seq_len=int(config.get("seq_len") or DEFAULT_SEQ_LEN),
            n_horizons=int(config.get("n_horizons") or DEFAULT_N_HORIZONS),
            patch_len=int(config.get("patch_len") or DEFAULT_PATCH_LEN),
            stride=int(config.get("stride") or DEFAULT_STRIDE),
            n_layers=int(config.get("n_layers") or DEFAULT_LAYERS),
            n_heads=int(config.get("n_heads") or DEFAULT_HEADS),
            d_model=int(config.get("d_model") or DEFAULT_D_MODEL),
            dropout=float(config.get("dropout") or 0.0),
            seed=int(config.get("seed") or 42),
            device=load_device,
        )
        obj.x_mean = np.asarray(config.get("x_mean"), dtype=np.float32)
        obj.x_std = np.asarray(config.get("x_std"), dtype=np.float32)
        obj.y_mean = np.asarray(config.get("y_mean"), dtype=np.float32)
        obj.y_std = np.asarray(config.get("y_std"), dtype=np.float32)
        schema = config.get("feature_schema")
        obj.feature_preprocessing = (
            dict((schema or {}).get("preprocessing") or {})
            if isinstance(schema, Mapping)
            else {}
        )
        obj.training_metrics = dict(config.get("training_metrics") or {})
        obj.ood_profile = dict(config.get("ood_profile") or {})
        if obj.model is None:
            obj._build_model(n_features=len(obj.feature_ids))
        obj.model.load_state_dict(dict(raw.get("state_dict") or {}))
        obj.model.eval()
        return obj

    def score_ood(self, features: Any) -> dict[str, Any]:
        return score_ood(getattr(self, "ood_profile", None), features)


def persist_model_artifact(model: PatchTSTRegressor, *, symbol: str = "*", version: str) -> dict[str, Any]:
    alias = f"model:{FAMILY}:{str(model.model_name)}:{str(symbol or '*').upper()}:current"
    ref = LocalArtifactStore().put(
        model.to_bytes(),
        content_type="application/vnd.pytorch",
        kind="model",
        alias=alias,
        metadata={
            "model_name": str(model.model_name),
            "family": FAMILY,
            "symbol": str(symbol or "*").upper(),
            "version": str(version),
            "feature_schema": dict(model.feature_schema),
            "ood_profile_summary": summarize_ood_profile(getattr(model, "ood_profile", None)),
        },
    )
    return {"alias": str(alias), "sha256": str(ref.sha256), "size_bytes": int(ref.size), "content_type": ref.content_type}


def register_shadow_model(
    model: PatchTSTRegressor,
    *,
    symbol: str = "*",
    version: str | None = None,
    performance_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    version_s = str(version or version_from_ts(str(model.model_name), int(time.time() * 1000), prefix=FAMILY))
    manifest = persist_model_artifact(model, symbol=str(symbol), version=str(version_s))
    metrics = {
        **dict(model.training_metrics or {}),
        **dict(performance_metrics or {}),
        "model_name": str(model.model_name),
        "model_version": str(version_s),
        "model_family": FAMILY,
        "model_kind": model.model_kind,
        "feature_ids": list(model.feature_ids),
        "feature_set_tag": str(model.feature_schema.get("feature_set_tag") or ""),
        "feature_schema": dict(model.feature_schema),
        "artifact_alias": str(manifest.get("alias") or ""),
        "artifact_sha256": str(manifest.get("sha256") or ""),
    }
    model_ts_ms = int(time.time() * 1000)
    register_model(
        model_name=str(model.model_name),
        model_kind=model.model_kind,
        model_ts_ms=int(model_ts_ms),
        stage="shadow",
        metrics=dict(metrics),
        regime="global",
    )
    register_model_version(
        model_name=str(model.model_name),
        model_version=str(version_s),
        model_kind=model.model_kind,
        stage="shadow",
        status="trained",
        live_ready=False,
        training_job_name="train_patchtst_models",
        train_scope={
            "symbol": str(symbol or "*").upper(),
            "feature_ids": list(model.feature_ids),
            "feature_schema": dict(model.feature_schema),
            "seq_len": int(model.seq_len),
            "n_horizons": int(model.n_horizons),
        },
        meta=dict(metrics),
    )
    return {"version": version_s, "stage": "shadow", "artifact_manifest": manifest, "metrics": metrics}


def load_model_from_artifact(alias: str = "", sha256: str = "", path: str | Path | None = None) -> PatchTSTRegressor:
    if path is not None and str(path).strip():
        return PatchTSTRegressor.load(Path(path))
    payload = _artifact_payload_from_alias(str(alias or ""), str(sha256 or ""))
    if not payload:
        raise FileNotFoundError("patchtst_artifact_not_found")
    return PatchTSTRegressor.from_bytes(payload)


def _resolve_training_config(plan: Mapping[str, Any] | None = None) -> dict[str, Any]:
    from engine.strategy.model_config import get_model_config, load_model_configs

    plan_dict = dict(plan or {})
    model_name = str(plan_dict.get("model_name") or "").strip()
    cfg = get_model_config(model_name, family=FAMILY) if model_name else {}
    if not cfg:
        configs = load_model_configs(family=FAMILY, include_disabled=True)
        cfg = dict(configs[0]) if configs else {"family": FAMILY, "model_name": FAMILY}
    model_name = str(model_name or cfg.get("model_name") or FAMILY).strip() or FAMILY
    feature_ids = _expected_columns(cfg.get("feature_ids"), model_name=model_name, model_spec=cfg)
    seq_len = int(cfg.get("seq_len") or DEFAULT_SEQ_LEN)
    n_horizons = int(cfg.get("n_horizons") or DEFAULT_N_HORIZONS)
    schema_guard = _resolve_retrain_schema_guard(
        family=FAMILY,
        model_name=str(model_name),
        feature_ids=list(feature_ids),
        cfg=cfg,
        schema_builder=lambda ids: _feature_schema(ids, seq_len=int(seq_len), n_horizons=int(n_horizons)),
    )
    return {
        **cfg,
        **schema_guard,
        "model_name": str(model_name),
        "feature_ids": list(feature_ids),
        "symbol_universe": list(cfg.get("symbol_universe") or cfg.get("symbols") or ["*"]),
        "training_window_days": int(cfg.get("training_window_days") or cfg.get("lookback_days") or DEFAULT_LOOKBACK_DAYS),
        "seq_len": int(seq_len),
        "n_horizons": int(n_horizons),
    }


def _load_sequence_training_rows(cfg: Mapping[str, Any]) -> tuple[np.ndarray, np.ndarray, list[dict[str, int | str]]] | None:
    feature_ids = list(cfg.get("feature_ids") or [])
    seq_len = int(cfg.get("seq_len") or DEFAULT_SEQ_LEN)
    n_horizons = int(cfg.get("n_horizons") or DEFAULT_N_HORIZONS)
    cutoff_ms = int(time.time() * 1000) - int(cfg.get("training_window_days") or DEFAULT_LOOKBACK_DAYS) * 86_400_000
    symbols_filter = {str(s).upper().strip() for s in list(cfg.get("symbol_universe") or []) if str(s or "").strip() and str(s) != "*"}
    con = connect()
    try:
        rows = con.execute(
            """
            SELECT l.symbol, l.horizon_s, l.impact_z, e.ts_ms, e.title, e.body, e.source
            FROM labels l
            JOIN events e ON e.id = l.event_id
            WHERE e.ts_ms >= ?
              AND l.impact_z IS NOT NULL
            ORDER BY l.symbol ASC, e.ts_ms ASC
            """,
            (int(cutoff_ms),),
        ).fetchall()
    finally:
        con.close()
    by_symbol: dict[str, list[tuple[int, dict[str, float], float]]] = {}
    for symbol, _horizon_s, impact_z, ts_ms, title, body, source in rows or []:
        sym = str(symbol or "").upper().strip()
        if not sym or (symbols_filter and sym not in symbols_filter):
            continue
        snapshot = build_feature_snapshot(
            event={"ts_ms": int(ts_ms or 0), "title": str(title or ""), "body": str(body or ""), "source": str(source or "")},
            symbol=str(sym),
            feature_ids=list(feature_ids),
        )
        by_symbol.setdefault(sym, []).append((int(ts_ms or 0), dict(snapshot), _safe_float(impact_z, 0.0)))
    X_rows: list[np.ndarray] = []
    y_rows: list[np.ndarray] = []
    meta_rows: list[dict[str, int | str]] = []
    for sym, items in by_symbol.items():
        ordered = sorted(items, key=lambda row: row[0])
        if len(ordered) < seq_len:
            continue
        for idx in range(seq_len - 1, len(ordered)):
            window = ordered[idx - seq_len + 1 : idx + 1]
            X_rows.append(
                np.asarray(
                    [[_safe_float(step[1].get(feature_id), float("nan")) for feature_id in feature_ids] for step in window],
                    dtype=np.float32,
                )
            )
            y_rows.append(np.full((n_horizons,), float(ordered[idx][2]), dtype=np.float32))
            meta_rows.append({"symbol": str(sym), "ts": int(ordered[idx][0])})
    if not X_rows:
        return None
    return np.stack(X_rows).astype(np.float32), np.stack(y_rows).astype(np.float32), meta_rows


def main() -> int:
    init_db()
    plan = load_lifecycle_plan(FAMILY)
    cfg = _resolve_training_config(plan)
    try:
        from engine.data.universe_pit import resolve_training_window_universe

        con_universe = connect(readonly=True)
        try:
            pit_universe = resolve_training_window_universe(
                con_universe,
                configured_symbols=list(cfg.get("symbol_universe") or ["*"]),
                lookback_days=int(cfg.get("training_window_days") or DEFAULT_LOOKBACK_DAYS),
            )
        finally:
            con_universe.close()
        if list(pit_universe.get("symbols") or []):
            cfg["symbol_universe"] = list(pit_universe.get("symbols") or [])
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    built = _load_sequence_training_rows(cfg)
    min_samples = int(os.environ.get("PATCHTST_MIN_SAMPLES", str(DEFAULT_MIN_SAMPLES)))
    if built is None or int(built[0].shape[0]) < max(2, min_samples):
        n = 0 if built is None else int(built[0].shape[0])
        print(f"{FAMILY}: insufficient_samples n={n} min_required={max(2, min_samples)}")
        return 0
    X, y, meta_rows = built
    n_samples = int(y.shape[0])
    split = min(max(1, int(n_samples * 0.8)), int(n_samples - 1))
    model = PatchTSTRegressor(
        model_name=str(cfg.get("model_name") or FAMILY),
        feature_ids=list(cfg.get("feature_ids") or []),
        seq_len=int(cfg.get("seq_len") or DEFAULT_SEQ_LEN),
        n_horizons=int(cfg.get("n_horizons") or DEFAULT_N_HORIZONS),
    )
    model.fit(
        X[:split],
        y[:split],
        epochs=int(os.environ.get("PATCHTST_EPOCHS", "20")),
        lr=float(os.environ.get("PATCHTST_LR", "0.001")),
    )
    try:
        pred_eval = model.predict(X[split:])
        horizon_s = int(cfg.get("horizon_s") or DEFAULT_HORIZON_S)
        oos_run_id = str(uuid.uuid4())
        upsert_oos_predictions(
            [
                {
                    "symbol": str(meta_rows[split + idx].get("symbol") or "*"),
                    "horizon": int(horizon_s),
                    "family": FAMILY,
                    "ts": int(meta_rows[split + idx].get("ts") or 0),
                    "run_id": str(oos_run_id),
                    "prediction": float(np.asarray(pred_eval[idx]).reshape(-1)[0]),
                    "target": float(np.asarray(y[split + idx]).reshape(-1)[0]),
                }
                for idx in range(int(n_samples - split))
            ]
        )
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    version = str(
        plan.get("model_version")
        or cfg.get("training_version_id")
        or version_from_ts(str(model.model_name), int(time.time() * 1000), prefix=FAMILY)
    )
    result = register_shadow_model(model, symbol="*", version=str(version))
    metrics = dict(result.get("metrics") or {})
    record_version_performance(
        model_name=str(model.model_name),
        model_version=str(version),
        metric_scope="training",
        metrics={"avg_rmse": float(metrics.get("rmse") or 0.0), "quality_score": 0.0, "trained_models": 1},
        sample_n=int(X.shape[0]),
        meta={"job_name": "train_patchtst_models"},
    )
    update_model_version_status(
        str(model.model_name),
        str(version),
        stage="shadow",
        status="trained",
        live_ready=False,
        meta_patch={"training_completed_ts_ms": int(time.time() * 1000)},
    )
    print(json.dumps({"ok": True, "family": FAMILY, "version": str(version), "stage": "shadow"}))
    return 0


__all__ = [
    "FAMILY",
    "PatchTST",
    "PatchTSTRegressor",
    "load_model_from_artifact",
    "main",
    "persist_model_artifact",
    "register_shadow_model",
]


if __name__ == "__main__":
    raise SystemExit(main())
