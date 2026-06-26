"""PatchTST sequence model family for multi-horizon return forecasting."""

from __future__ import annotations
import logging

import json
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
from engine.runtime.hardware import resolve_torch_device, torch_device_is_cuda
from engine.runtime.storage import connect, init_db, table_exists
from engine.strategy.feature_registry import (
    assert_feature_schema_runtime_parity,
    build_feature_snapshot,
    feature_schema_flags,
    feature_set_tag_from_ids,
)
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
DEFAULT_MC_DROPOUT_SAMPLES = int(os.environ.get("PATCHTST_MC_DROPOUT_SAMPLES", "0"))
DEFAULT_MIN_SAMPLES = int(os.environ.get("PATCHTST_MIN_SAMPLES", "20"))
DEFAULT_LOOKBACK_DAYS = int(os.environ.get("PATCHTST_LOOKBACK_DAYS", "365"))
DEFAULT_HORIZON_S = int(os.environ.get("PATCHTST_HORIZON_S", os.environ.get("MODEL_HORIZON_MEDIUM_S", "3600")))
DEFAULT_PRETRAIN_MASK_RATIO = float(os.environ.get("PATCHTST_PRETRAIN_MASK_RATIO", "0.35"))
DEFAULT_PRETRAIN_EPOCHS = int(os.environ.get("PATCHTST_PRETRAIN_EPOCHS", "20"))
DEFAULT_PRETRAIN_LR = float(os.environ.get("PATCHTST_PRETRAIN_LR", "0.001"))
DEFAULT_PRETRAIN_MIN_SAMPLES = int(os.environ.get("PATCHTST_PRETRAIN_MIN_SAMPLES", str(DEFAULT_MIN_SAMPLES)))
DEFAULT_PRETRAIN_MAX_SAMPLES = int(os.environ.get("PATCHTST_PRETRAIN_MAX_SAMPLES", "2048"))
PRETRAIN_ARTIFACT_KIND = "patchtst_masked_pretraining"


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

def _default_device() -> torch.device:
    resolution = resolve_torch_device(
        torch,
        env_var="PATCHTST_DEVICE",
        fallback_envs=("TORCH_DEVICE",),
        legacy_cuda_flag="PATCHTST_USE_CUDA",
    )
    return torch.device(resolution.resolved)


DEFAULT_DEVICE = _default_device()


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
        "feature_flags": feature_schema_flags(list(ids)),
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
    else:
        try:
            assert_feature_schema_runtime_parity(
                artifact_schema,
                current_schema=current_schema,
                context="feature_schema_drift",
                model_name=str(config.get("model_name") or FAMILY),
            )
        except ValueError as exc:
            reason = "runtime_feature_flag_mismatch"
            error = exc
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
    resolution = resolve_torch_device(
        torch,
        env_var="PATCHTST_DEVICE",
        fallback_envs=("TORCH_DEVICE",),
        legacy_cuda_flag="PATCHTST_USE_CUDA",
    )
    if torch_device_is_cuda(torch, resolution):
        return torch.device(resolution.resolved)
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


def _table_columns(con: Any, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({str(table_name)})").fetchall() or []
        return {str(row[1] or "").strip() for row in rows if len(row) > 1 and str(row[1] or "").strip()}
    except Exception:
        return set()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(value)
    except Exception:
        return int(default)


def _feature_schema_core(schema: Mapping[str, Any] | None) -> dict[str, Any]:
    raw = dict(schema or {})
    return {
        "feature_ids": [str(item) for item in list(raw.get("feature_ids") or [])],
        "feature_set_tag": str(raw.get("feature_set_tag") or ""),
        "feature_count": _safe_int(raw.get("feature_count"), 0),
        "sequence_schema": dict(raw.get("sequence_schema") or {}),
    }


def _assert_pretraining_schema_compatible(
    pretraining: Mapping[str, Any] | None,
    *,
    model_config: Mapping[str, Any] | None = None,
    model_schema: Mapping[str, Any] | None = None,
) -> None:
    meta = dict(pretraining or {})
    if not meta:
        return
    pre_schema = dict(meta.get("feature_schema") or {})
    if not pre_schema and isinstance(meta.get("config"), Mapping):
        pre_schema = dict((meta.get("config") or {}).get("feature_schema") or {})
    expected_schema = dict(model_schema or {})
    if not expected_schema and isinstance(model_config, Mapping):
        expected_schema = dict((model_config or {}).get("feature_schema") or {})
    pre_core = _feature_schema_core(pre_schema)
    expected_core = _feature_schema_core(expected_schema)
    if pre_core.get("feature_ids") != expected_core.get("feature_ids"):
        raise ValueError(
            "patchtst_pretraining_schema_drift:"
            f" pretraining_feature_ids={pre_core.get('feature_ids')}"
            f" model_feature_ids={expected_core.get('feature_ids')}"
        )
    if str(pre_core.get("feature_set_tag") or "") != str(expected_core.get("feature_set_tag") or ""):
        raise ValueError(
            "patchtst_pretraining_schema_drift:"
            f" pretraining_feature_set_tag={pre_core.get('feature_set_tag') or '<missing>'}"
            f" model_feature_set_tag={expected_core.get('feature_set_tag') or '<missing>'}"
        )
    pre_seq = dict(pre_core.get("sequence_schema") or {})
    expected_seq = dict(expected_core.get("sequence_schema") or {})
    for key in ("seq_len",):
        if _safe_int(pre_seq.get(key), 0) != _safe_int(expected_seq.get(key), 0):
            raise ValueError(
                "patchtst_pretraining_sequence_drift:"
                f" {key}={_safe_int(pre_seq.get(key), 0)}"
                f" model_{key}={_safe_int(expected_seq.get(key), 0)}"
            )
    if isinstance(model_config, Mapping):
        for key in ("patch_len", "stride", "d_model", "n_layers", "n_heads"):
            pre_value = meta.get(key)
            if pre_value is None and isinstance(meta.get("config"), Mapping):
                pre_value = (meta.get("config") or {}).get(key)
            model_value = model_config.get(key)
            if pre_value is None or model_value is None:
                continue
            if _safe_int(pre_value, 0) != _safe_int(model_value, 0):
                raise ValueError(
                    "patchtst_pretraining_architecture_drift:"
                    f" {key}={_safe_int(pre_value, 0)}"
                    f" model_{key}={_safe_int(model_value, 0)}"
                )


def _pretraining_alias(model_name: str, symbol: str = "*") -> str:
    return f"model:{FAMILY}:{str(model_name or FAMILY)}:{str(symbol or '*').upper()}:pretrained"


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

    def patchify(self, x: torch.Tensor) -> torch.Tensor:
        if x.ndim != 3:
            raise ValueError("patchtst_forward_requires_3d")
        patches = x.unfold(dimension=1, size=self.patch_len, step=self.stride)
        patches = patches.permute(0, 1, 3, 2).contiguous()
        return patches.reshape(x.shape[0], self.n_patches, self.patch_len * self.n_features)

    def encode(
        self,
        x: torch.Tensor,
        *,
        patch_mask: torch.Tensor | None = None,
        mask_token: torch.Tensor | None = None,
    ) -> torch.Tensor:
        patches = self.patchify(x)
        projected = self.patch_projection(patches)
        if patch_mask is not None:
            if mask_token is None:
                raise ValueError("patchtst_mask_token_required")
            mask = patch_mask.to(device=projected.device, dtype=torch.bool)
            if tuple(mask.shape) != tuple(projected.shape[:2]):
                raise ValueError("patchtst_patch_mask_shape_mismatch")
            replacement = mask_token.to(device=projected.device, dtype=projected.dtype).reshape(1, 1, -1)
            projected = torch.where(mask.unsqueeze(-1), replacement.expand_as(projected), projected)
        tokens = projected + self.position
        encoded = self.encoder(tokens)
        return encoded

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        encoded = self.encode(x)
        pooled = self.norm(encoded.mean(dim=1))
        return self.head(pooled)


class MaskedPatchTSTPretrainer(nn.Module):
    """PatchTST encoder trained to reconstruct intentionally masked patches."""

    def __init__(
        self,
        *,
        seq_len: int,
        n_features: int,
        patch_len: int = DEFAULT_PATCH_LEN,
        stride: int = DEFAULT_STRIDE,
        d_model: int = DEFAULT_D_MODEL,
        n_layers: int = DEFAULT_LAYERS,
        n_heads: int = DEFAULT_HEADS,
        dropout: float = DEFAULT_DROPOUT,
    ) -> None:
        super().__init__()
        self.encoder = PatchTST(
            seq_len=int(seq_len),
            n_features=int(n_features),
            n_horizons=1,
            patch_len=int(patch_len),
            stride=int(stride),
            d_model=int(d_model),
            n_layers=int(n_layers),
            n_heads=int(n_heads),
            dropout=float(dropout),
        )
        self.mask_token = nn.Parameter(torch.zeros(int(d_model)))
        self.reconstruction_head = nn.Linear(int(d_model), int(patch_len) * int(n_features))

    @property
    def n_patches(self) -> int:
        return int(self.encoder.n_patches)

    def forward(self, x: torch.Tensor, patch_mask: torch.Tensor) -> torch.Tensor:
        encoded = self.encoder.encode(x, patch_mask=patch_mask, mask_token=self.mask_token)
        return self.reconstruction_head(encoded)

    def target_patches(self, x: torch.Tensor) -> torch.Tensor:
        return self.encoder.patchify(x)


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
        self.pretraining_metadata: dict[str, Any] = {}
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
        pretraining_artifact: Mapping[str, Any] | None = None,
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
        pretraining_applied = self._apply_pretraining_artifact(pretraining_artifact)

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
            "pretraining": dict(self.pretraining_metadata or {}),
            "pretraining_applied": bool(pretraining_applied),
        }
        return losses if return_losses else self

    def _apply_pretraining_artifact(self, pretraining_artifact: Mapping[str, Any] | None) -> bool:
        if not pretraining_artifact:
            self.pretraining_metadata = {}
            return False
        if self.model is None:
            raise RuntimeError("patchtst_model_not_initialized")
        raw = dict(pretraining_artifact or {})
        if str(raw.get("artifact_kind") or raw.get("kind") or "") != PRETRAIN_ARTIFACT_KIND:
            raise ValueError("patchtst_invalid_pretraining_artifact_kind")
        config = dict(raw.get("config") or {})
        artifact_schema = dict(config.get("feature_schema") or raw.get("feature_schema") or {})
        artifact_meta = {
            "artifact_kind": PRETRAIN_ARTIFACT_KIND,
            "model_name": str(config.get("model_name") or raw.get("model_name") or ""),
            "feature_schema": dict(artifact_schema),
            "seq_len": int(config.get("seq_len") or self.seq_len),
            "patch_len": int(config.get("patch_len") or self.patch_len),
            "stride": int(config.get("stride") or self.stride),
            "d_model": int(config.get("d_model") or self.d_model),
            "n_layers": int(config.get("n_layers") or self.n_layers),
            "n_heads": int(config.get("n_heads") or self.n_heads),
            "mask_ratio": float((raw.get("metrics") or {}).get("mask_ratio") or config.get("mask_ratio") or 0.0),
            "artifact_alias": str(raw.get("artifact_alias") or ""),
            "artifact_sha256": str(raw.get("artifact_sha256") or ""),
        }
        _assert_pretraining_schema_compatible(
            artifact_meta,
            model_config=self._config_payload(),
            model_schema=self.feature_schema,
        )
        state = dict(raw.get("encoder_state_dict") or {})
        if not state and isinstance(raw.get("pretraining_state_dict"), Mapping):
            prefix = "encoder."
            state = {
                str(key)[len(prefix):]: value
                for key, value in dict(raw.get("pretraining_state_dict") or {}).items()
                if str(key).startswith(prefix)
            }
        if not state:
            raise ValueError("patchtst_pretraining_encoder_state_missing")
        current_state = self.model.state_dict()
        compatible_state = {
            str(key): value
            for key, value in state.items()
            if str(key) in current_state
            and not str(key).startswith("head.")
            and tuple(getattr(value, "shape", ())) == tuple(current_state[str(key)].shape)
        }
        if not compatible_state:
            raise ValueError("patchtst_pretraining_encoder_state_incompatible")
        self.model.load_state_dict(compatible_state, strict=False)
        self.pretraining_metadata = {
            **artifact_meta,
            "loaded_encoder_tensors": int(len(compatible_state)),
            "metrics": dict(raw.get("metrics") or {}),
        }
        return True

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

    def predict_with_uncertainty(self, X: Any, *, samples: int | None = None) -> dict[str, Any]:
        """Return first-horizon prediction plus optional MC-dropout uncertainty."""

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
        xt = torch.from_numpy(Xn).to(self.device)
        sample_count = max(0, int(samples if samples is not None else DEFAULT_MC_DROPOUT_SAMPLES))

        if sample_count <= 1 or float(self.dropout) <= 0.0:
            self.model.eval()
            with torch.no_grad():
                pred_n = self.model(xt).detach().cpu().numpy().astype(np.float32)
            pred = pred_n * self.y_std.reshape(1, -1) + self.y_mean.reshape(1, -1)
            return {
                "prediction": float(pred.reshape(-1)[0]) if pred.size else 0.0,
                "prediction_vector": pred.astype(float).tolist(),
                "epistemic_uncertainty": 0.0,
                "mc_dropout_samples": int(sample_count),
                "uncertainty_ts_ms": int(time.time() * 1000),
            }

        was_training = bool(self.model.training)
        preds: list[np.ndarray] = []
        try:
            self.model.train()
            with torch.no_grad():
                for _idx in range(int(sample_count)):
                    pred_n = self.model(xt).detach().cpu().numpy().astype(np.float32)
                    preds.append(pred_n * self.y_std.reshape(1, -1) + self.y_mean.reshape(1, -1))
        finally:
            if was_training:
                self.model.train()
            else:
                self.model.eval()

        stacked = np.stack(preds, axis=0).astype(np.float32)
        mean_pred = stacked.mean(axis=0)
        std_pred = stacked.std(axis=0)
        q10 = np.quantile(stacked, 0.10, axis=0).astype(np.float32)
        q50 = np.quantile(stacked, 0.50, axis=0).astype(np.float32)
        q90 = np.quantile(stacked, 0.90, axis=0).astype(np.float32)
        return {
            "prediction": float(mean_pred.reshape(-1)[0]) if mean_pred.size else 0.0,
            "prediction_vector": mean_pred.astype(float).tolist(),
            "prediction_lower": float(q10.reshape(-1)[0]) if q10.size else 0.0,
            "prediction_median": float(q50.reshape(-1)[0]) if q50.size else 0.0,
            "prediction_upper": float(q90.reshape(-1)[0]) if q90.size else 0.0,
            "quantile_forecasts": {
                "0.10": float(q10.reshape(-1)[0]) if q10.size else 0.0,
                "0.50": float(q50.reshape(-1)[0]) if q50.size else 0.0,
                "0.90": float(q90.reshape(-1)[0]) if q90.size else 0.0,
            },
            "epistemic_uncertainty": float(std_pred.reshape(-1)[0]) if std_pred.size else 0.0,
            "epistemic_uncertainty_vector": std_pred.astype(float).tolist(),
            "mc_dropout_samples": int(sample_count),
            "uncertainty_ts_ms": int(time.time() * 1000),
            "uncertainty_detail": {
                "method": "mc_dropout",
                "samples": int(sample_count),
                "dropout": float(self.dropout),
                "quantile_levels": [0.10, 0.50, 0.90],
            },
        }

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
        _assert_pretraining_schema_compatible(
            dict(config.get("pretraining") or {}),
            model_config=config,
            model_schema=dict(config.get("feature_schema") or {}),
        )
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
        obj.pretraining_metadata = dict(config.get("pretraining") or {})
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
            "pretraining": dict(getattr(self, "pretraining_metadata", {}) or {}),
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
        _assert_pretraining_schema_compatible(
            dict(config.get("pretraining") or {}),
            model_config=config,
            model_schema=dict(config.get("feature_schema") or {}),
        )
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
        obj.pretraining_metadata = dict(config.get("pretraining") or {})
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
            "pretraining": dict(getattr(model, "pretraining_metadata", {}) or {}),
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
        "pretraining": dict(getattr(model, "pretraining_metadata", {}) or {}),
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


def _patch_mask(
    *,
    n_samples: int,
    n_patches: int,
    mask_ratio: float,
    device: torch.device,
    seed: int,
) -> torch.Tensor:
    ratio = min(0.95, max(0.05, float(mask_ratio)))
    generator = torch.Generator(device=str(device))
    generator.manual_seed(int(seed))
    mask = torch.rand((int(n_samples), int(n_patches)), generator=generator, device=device) < float(ratio)
    if int(n_patches) <= 1:
        return torch.ones((int(n_samples), int(n_patches)), dtype=torch.bool, device=device)
    for row_idx in range(int(n_samples)):
        if not bool(mask[row_idx].any()):
            mask[row_idx, row_idx % int(n_patches)] = True
        if bool(mask[row_idx].all()):
            mask[row_idx, (row_idx + 1) % int(n_patches)] = False
    return mask.to(dtype=torch.bool)


def train_masked_pretraining_artifact(
    X: Any,
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
    epochs: int = DEFAULT_PRETRAIN_EPOCHS,
    lr: float = DEFAULT_PRETRAIN_LR,
    weight_decay: float = 1e-4,
    grad_clip: float = 1.0,
    mask_ratio: float = DEFAULT_PRETRAIN_MASK_RATIO,
    device: str | torch.device | None = None,
) -> dict[str, Any]:
    columns = _expected_columns(feature_ids, model_name=str(model_name), model_spec={"feature_ids": list(feature_ids or [])})
    X_raw = _sequence_array_from_features(X, columns, seq_len=int(seq_len))
    X_arr, preprocessing, _accounting = _preprocess_sequence_array(
        X_raw,
        columns,
        phase="pretrain",
        model_name=str(model_name),
        fit_preprocessing=True,
    )
    x_mean = X_arr.mean(axis=(0, 1)).astype(np.float32)
    x_std = X_arr.std(axis=(0, 1)).astype(np.float32)
    x_std = np.where(x_std < 1e-6, 1.0, x_std).astype(np.float32)
    Xn = ((X_arr - x_mean.reshape(1, 1, -1)) / x_std.reshape(1, 1, -1)).astype(np.float32)
    train_device = torch.device(device) if device is not None else DEFAULT_DEVICE
    _set_seed(int(seed))
    pretrainer = MaskedPatchTSTPretrainer(
        seq_len=int(seq_len),
        n_features=int(X_arr.shape[2]),
        patch_len=int(patch_len),
        stride=int(stride),
        d_model=int(d_model),
        n_layers=int(n_layers),
        n_heads=int(n_heads),
        dropout=float(dropout),
    ).to(train_device)
    xt = torch.from_numpy(Xn).to(train_device)
    opt = torch.optim.AdamW(pretrainer.parameters(), lr=float(lr), weight_decay=float(weight_decay))
    sched = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=max(1, int(epochs)))
    losses: list[float] = []
    pretrainer.train()
    for epoch in range(int(epochs)):
        mask = _patch_mask(
            n_samples=int(xt.shape[0]),
            n_patches=int(pretrainer.n_patches),
            mask_ratio=float(mask_ratio),
            device=train_device,
            seed=int(seed) + int(epoch) + 17,
        )
        opt.zero_grad(set_to_none=True)
        pred = pretrainer(xt, mask)
        target = pretrainer.target_patches(xt)
        diff = (pred - target) ** 2
        loss = diff[mask].mean()
        loss.backward()
        if float(grad_clip) > 0.0:
            torch.nn.utils.clip_grad_norm_(pretrainer.parameters(), float(grad_clip))
        opt.step()
        sched.step()
        losses.append(float(loss.detach().cpu().item()))
    pretrainer.eval()
    feature_schema = _feature_schema(
        columns,
        seq_len=int(seq_len),
        n_horizons=int(n_horizons),
        preprocessing=dict(preprocessing or {}),
    )
    metrics = {
        "model_family": FAMILY,
        "model_kind": "patchtst_pretraining",
        "pretraining_task": "masked_patch_reconstruction",
        "n_train": int(X_arr.shape[0]),
        "mask_ratio": float(mask_ratio),
        "n_patches": int(pretrainer.n_patches),
        "loss_initial": float(losses[0] if losses else 0.0),
        "loss_final": float(losses[-1] if losses else 0.0),
        "reconstruction_rmse": float(np.sqrt(float(losses[-1] if losses else 0.0))),
        "feature_schema": dict(feature_schema),
    }
    config = {
        "model_name": str(model_name),
        "feature_ids": list(columns),
        "feature_schema": dict(feature_schema),
        "seq_len": int(seq_len),
        "n_horizons": int(n_horizons),
        "patch_len": int(patch_len),
        "stride": int(stride),
        "n_layers": int(n_layers),
        "n_heads": int(n_heads),
        "d_model": int(d_model),
        "dropout": float(dropout),
        "seed": int(seed),
        "device_at_train": str(train_device.type),
        "mask_ratio": float(mask_ratio),
        "x_mean": x_mean.astype(float).tolist(),
        "x_std": x_std.astype(float).tolist(),
    }
    return {
        "artifact_kind": PRETRAIN_ARTIFACT_KIND,
        "config": config,
        "feature_schema": dict(feature_schema),
        "encoder_state_dict": pretrainer.encoder.state_dict(),
        "pretraining_state_dict": pretrainer.state_dict(),
        "metrics": metrics,
    }


def persist_pretraining_artifact(
    payload: Mapping[str, Any],
    *,
    model_name: str,
    symbol: str = "*",
    version: str,
) -> dict[str, Any]:
    raw = dict(payload or {})
    if str(raw.get("artifact_kind") or "") != PRETRAIN_ARTIFACT_KIND:
        raise ValueError("patchtst_invalid_pretraining_artifact_kind")
    alias = _pretraining_alias(str(model_name or FAMILY), str(symbol or "*"))
    ref = LocalArtifactStore().put(
        dumps_torch_payload(raw),
        content_type="application/vnd.pytorch",
        kind="model",
        alias=alias,
        metadata={
            "model_name": str(model_name or FAMILY),
            "family": FAMILY,
            "artifact_kind": PRETRAIN_ARTIFACT_KIND,
            "symbol": str(symbol or "*").upper(),
            "version": str(version),
            "feature_schema": dict(raw.get("feature_schema") or (raw.get("config") or {}).get("feature_schema") or {}),
            "metrics": dict(raw.get("metrics") or {}),
        },
    )
    return {"alias": str(alias), "sha256": str(ref.sha256), "size_bytes": int(ref.size), "content_type": ref.content_type}


def load_pretraining_artifact_from_artifact(alias: str = "", sha256: str = "") -> dict[str, Any]:
    alias_s = str(alias or "").strip()
    sha_s = str(sha256 or "").strip()
    store = LocalArtifactStore()
    ref = store.resolve(alias_s) if alias_s else None
    if ref is None and sha_s:
        from datetime import datetime, timezone

        from engine.artifacts.refs import ArtifactRef

        ref = ArtifactRef(
            sha256=sha_s,
            size=0,
            content_type="application/vnd.pytorch",
            kind="model",
            created_ts=datetime.now(timezone.utc),
            metadata={},
        )
    if ref is None:
        raise FileNotFoundError("patchtst_pretraining_artifact_not_found")
    raw = dict(loads_torch_payload(store.get_bytes(ref)))
    if str(raw.get("artifact_kind") or "") != PRETRAIN_ARTIFACT_KIND:
        raise ValueError("patchtst_invalid_pretraining_artifact_kind")
    raw["artifact_alias"] = alias_s
    raw["artifact_sha256"] = str(ref.sha256)
    return raw


def _resolve_pretraining_artifact_for_finetune(
    cfg: Mapping[str, Any],
    manifest: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    if isinstance(manifest, Mapping) and (manifest.get("alias") or manifest.get("sha256")):
        return load_pretraining_artifact_from_artifact(
            alias=str(manifest.get("alias") or ""),
            sha256=str(manifest.get("sha256") or ""),
        )
    explicit_alias = str(
        os.environ.get("PATCHTST_PRETRAIN_ARTIFACT_ALIAS")
        or cfg.get("pretraining_artifact_alias")
        or ""
    ).strip()
    explicit_sha = str(
        os.environ.get("PATCHTST_PRETRAIN_ARTIFACT_SHA256")
        or cfg.get("pretraining_artifact_sha256")
        or ""
    ).strip()
    if explicit_alias or explicit_sha:
        return load_pretraining_artifact_from_artifact(alias=explicit_alias, sha256=explicit_sha)
    default_alias = _pretraining_alias(str(cfg.get("model_name") or FAMILY), "*")
    try:
        store = LocalArtifactStore()
        if store.resolve(default_alias) is None:
            return None
        return load_pretraining_artifact_from_artifact(alias=default_alias)
    except FileNotFoundError:
        return None


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
    raw_horizons = cfg.get("horizons_s") or cfg.get("horizons") or []
    if not isinstance(raw_horizons, (list, tuple, set)):
        raw_horizons = [raw_horizons]
    first_horizon = next((_safe_int(item, 0) for item in list(raw_horizons or []) if _safe_int(item, 0) > 0), 0)
    horizon_s = _safe_int(cfg.get("horizon_s"), first_horizon or DEFAULT_HORIZON_S)
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
        "horizon_s": int(horizon_s),
        "seq_len": int(seq_len),
        "n_horizons": int(n_horizons),
        "supervised_target": str(cfg.get("supervised_target") or os.environ.get("PATCHTST_SUPERVISED_TARGET", "net_edge")),
    }


def _supervised_target_expression(target_kind: str) -> tuple[str, str]:
    normalized = str(target_kind or "net_edge").strip().lower().replace("-", "_")
    if normalized in {"forward_return", "realized_forward_return", "raw_forward_return"}:
        return "realized_forward_return", "net_after_cost_labels.realized_forward_return"
    if normalized in {"net_edge", "net_return", "edge"}:
        return "net_return", "net_after_cost_labels.net_return"
    return "", "legacy.labels_exec_net_z_or_impact_z"


def _load_supervised_label_points(
    cfg: Mapping[str, Any],
    *,
    cutoff_ms: int,
) -> list[tuple[str, int, float, int, str, str, str, str]]:
    horizon_s = int(cfg.get("horizon_s") or DEFAULT_HORIZON_S)
    target_expr, target_source = _supervised_target_expression(str(cfg.get("supervised_target") or "net_edge"))
    con = connect()
    try:
        if target_expr:
            try:
                net_labels_available = bool(table_exists(con, "net_after_cost_labels"))
            except Exception:
                net_labels_available = False
            if net_labels_available:
                rows = con.execute(
                    f"""
                    SELECT n.symbol, n.horizon_s, n.{target_expr} AS target_value,
                           COALESCE(n.label_ts_ms, e.ts_ms, n.computed_at_ts_ms) AS ts_ms,
                           COALESCE(e.title, '') AS title,
                           COALESCE(e.body, '') AS body,
                           COALESCE(e.source, n.source, 'net_after_cost_labels') AS source
                    FROM net_after_cost_labels n
                    LEFT JOIN events e ON e.id = n.event_id
                    WHERE COALESCE(n.label_ts_ms, e.ts_ms, n.computed_at_ts_ms) >= ?
                      AND n.horizon_s = ?
                      AND n.realized = 1
                      AND n.{target_expr} IS NOT NULL
                    ORDER BY n.symbol ASC, COALESCE(n.label_ts_ms, e.ts_ms, n.computed_at_ts_ms) ASC
                    """,
                    (int(cutoff_ms), int(horizon_s)),
                ).fetchall()
                if rows:
                    return [
                        (
                            str(symbol or ""),
                            int(row_horizon_s or 0),
                            _safe_float(target_value, 0.0),
                            int(ts_ms or 0),
                            str(title or ""),
                            str(body or ""),
                            str(source or ""),
                            str(target_source),
                        )
                        for symbol, row_horizon_s, target_value, ts_ms, title, body, source in rows
                    ]

        rows = con.execute(
            """
            SELECT l.symbol, l.horizon_s, COALESCE(le.net_z, l.impact_z) AS impact_z,
                   e.ts_ms, e.title, e.body, e.source
            FROM labels l
            JOIN events e ON e.id = l.event_id
            LEFT JOIN labels_exec le
              ON le.event_id = l.event_id
             AND le.symbol = l.symbol
             AND le.horizon_s = l.horizon_s
             AND le.realized = 1
            WHERE e.ts_ms >= ?
              AND l.horizon_s = ?
              AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
            ORDER BY l.symbol ASC, e.ts_ms ASC
            """,
            (int(cutoff_ms), int(horizon_s)),
        ).fetchall()
        return [
            (
                str(symbol or ""),
                int(row_horizon_s or 0),
                _safe_float(impact_z, 0.0),
                int(ts_ms or 0),
                str(title or ""),
                str(body or ""),
                str(source or ""),
                "legacy.labels_exec_net_z_or_impact_z",
            )
            for symbol, row_horizon_s, impact_z, ts_ms, title, body, source in rows or []
        ]
    finally:
        con.close()


def _load_sequence_training_rows(cfg: Mapping[str, Any]) -> tuple[np.ndarray, np.ndarray, list[dict[str, int | str]]] | None:
    feature_ids = list(cfg.get("feature_ids") or [])
    seq_len = int(cfg.get("seq_len") or DEFAULT_SEQ_LEN)
    n_horizons = int(cfg.get("n_horizons") or DEFAULT_N_HORIZONS)
    cutoff_ms = int(time.time() * 1000) - int(cfg.get("training_window_days") or DEFAULT_LOOKBACK_DAYS) * 86_400_000
    symbols_filter = {str(s).upper().strip() for s in list(cfg.get("symbol_universe") or []) if str(s or "").strip() and str(s) != "*"}
    rows = _load_supervised_label_points(cfg, cutoff_ms=int(cutoff_ms))
    by_symbol: dict[str, list[tuple[int, dict[str, float], float, str]]] = {}
    for symbol, _horizon_s, target_value, ts_ms, title, body, source, target_source in rows or []:
        sym = str(symbol or "").upper().strip()
        if not sym or (symbols_filter and sym not in symbols_filter):
            continue
        snapshot = build_feature_snapshot(
            event={"ts_ms": int(ts_ms or 0), "title": str(title or ""), "body": str(body or ""), "source": str(source or "")},
            symbol=str(sym),
            feature_ids=list(feature_ids),
        )
        by_symbol.setdefault(sym, []).append((int(ts_ms or 0), dict(snapshot), _safe_float(target_value, 0.0), str(target_source)))
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
            meta_rows.append({"symbol": str(sym), "ts": int(ordered[idx][0]), "target_source": str(ordered[idx][3])})
    if not X_rows:
        return None
    return np.stack(X_rows).astype(np.float32), np.stack(y_rows).astype(np.float32), meta_rows


def _load_pretraining_feature_points(cfg: Mapping[str, Any], *, cutoff_ms: int) -> dict[str, list[tuple[int, dict[str, float], str]]]:
    feature_ids = list(cfg.get("feature_ids") or [])
    symbols_filter = {str(s).upper().strip() for s in list(cfg.get("symbol_universe") or []) if str(s or "").strip() and str(s) != "*"}
    by_symbol: dict[str, list[tuple[int, dict[str, float], str]]] = {}
    con = connect()
    try:
        try:
            if table_exists(con, "events") and table_exists(con, "labels"):
                rows = con.execute(
                    """
                    SELECT DISTINCT l.symbol, e.ts_ms, e.title, e.body, e.source
                    FROM labels l
                    JOIN events e ON e.id = l.event_id
                    WHERE e.ts_ms >= ?
                    ORDER BY l.symbol ASC, e.ts_ms ASC
                    """,
                    (int(cutoff_ms),),
                ).fetchall()
            else:
                rows = []
        except Exception:
            rows = []
        for symbol, ts_ms, title, body, source in rows or []:
            sym = str(symbol or "").upper().strip()
            if not sym or (symbols_filter and sym not in symbols_filter):
                continue
            event = {"ts_ms": int(ts_ms or 0), "title": str(title or ""), "body": str(body or ""), "source": str(source or "")}
            snapshot = build_feature_snapshot(event=event, symbol=str(sym), feature_ids=list(feature_ids))
            by_symbol.setdefault(sym, []).append((int(ts_ms or 0), dict(snapshot), "events"))

        try:
            prices_available = bool(table_exists(con, "prices"))
        except Exception:
            prices_available = False
        if prices_available:
            cols = _table_columns(con, "prices")
            price_col = next((col for col in ("price", "px", "last", "close") if col in cols), "")
            volume_col = next((col for col in ("volume", "vol", "size") if col in cols), "")
            if {"symbol", "ts_ms"}.issubset(cols) and price_col:
                volume_sql = str(volume_col) if volume_col else "NULL"
                price_rows = con.execute(
                    f"""
                    SELECT symbol, ts_ms, {price_col} AS price_value, {volume_sql} AS volume_value
                    FROM prices
                    WHERE ts_ms >= ?
                    ORDER BY symbol ASC, ts_ms ASC
                    """,
                    (int(cutoff_ms),),
                ).fetchall()
                for symbol, ts_ms, price_value, volume_value in price_rows or []:
                    sym = str(symbol or "").upper().strip()
                    if not sym or (symbols_filter and sym not in symbols_filter):
                        continue
                    event = {"ts_ms": int(ts_ms or 0), "title": "", "body": "", "source": "prices"}
                    snapshot = build_feature_snapshot(event=event, symbol=str(sym), feature_ids=list(feature_ids))
                    if "price.last" in feature_ids:
                        snapshot["price.last"] = _safe_float(price_value, 0.0)
                    if "price.volume" in feature_ids:
                        snapshot["price.volume"] = _safe_float(volume_value, 0.0)
                    by_symbol.setdefault(sym, []).append((int(ts_ms or 0), dict(snapshot), "prices"))
    finally:
        con.close()
    return by_symbol


def _load_pretraining_rows(cfg: Mapping[str, Any]) -> tuple[np.ndarray, list[dict[str, int | str]]] | None:
    feature_ids = list(cfg.get("feature_ids") or [])
    seq_len = int(cfg.get("seq_len") or DEFAULT_SEQ_LEN)
    cutoff_ms = int(time.time() * 1000) - int(cfg.get("training_window_days") or DEFAULT_LOOKBACK_DAYS) * 86_400_000
    max_samples = int(cfg.get("pretraining_max_samples") or DEFAULT_PRETRAIN_MAX_SAMPLES)
    by_symbol = _load_pretraining_feature_points(cfg, cutoff_ms=int(cutoff_ms))
    X_rows: list[np.ndarray] = []
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
            meta_rows.append({"symbol": str(sym), "ts": int(ordered[idx][0]), "source": str(ordered[idx][2])})
    if not X_rows:
        return None
    if max_samples > 0 and len(X_rows) > max_samples:
        X_rows = X_rows[-max_samples:]
        meta_rows = meta_rows[-max_samples:]
    return np.stack(X_rows).astype(np.float32), meta_rows


def main(pretraining_manifest: Mapping[str, Any] | None = None) -> int:
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
    pretraining_artifact = _resolve_pretraining_artifact_for_finetune(cfg, pretraining_manifest)
    model.fit(
        X[:split],
        y[:split],
        epochs=int(os.environ.get("PATCHTST_EPOCHS", "20")),
        lr=float(os.environ.get("PATCHTST_LR", "0.001")),
        pretraining_artifact=pretraining_artifact,
    )
    target_sources = sorted({str(row.get("target_source") or "") for row in meta_rows if str(row.get("target_source") or "")})
    model.training_metrics["supervised_target"] = str(cfg.get("supervised_target") or "net_edge")
    model.training_metrics["supervised_target_sources"] = list(target_sources)
    model.training_metrics["horizon_s"] = int(cfg.get("horizon_s") or DEFAULT_HORIZON_S)
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


def pretrain_main() -> int:
    init_db()
    plan = load_lifecycle_plan(FAMILY)
    cfg = _resolve_training_config(plan)
    built = _load_pretraining_rows(cfg)
    min_samples = int(os.environ.get("PATCHTST_PRETRAIN_MIN_SAMPLES", str(DEFAULT_PRETRAIN_MIN_SAMPLES)))
    if built is None or int(built[0].shape[0]) < max(2, min_samples):
        n = 0 if built is None else int(built[0].shape[0])
        print(f"{FAMILY}: insufficient_pretraining_samples n={n} min_required={max(2, min_samples)}")
        return 0
    X_pre, meta_rows = built
    version = str(
        cfg.get("pretraining_version_id")
        or version_from_ts(str(cfg.get("model_name") or FAMILY), int(time.time() * 1000), prefix=f"{FAMILY}-pretrain")
    )
    payload = train_masked_pretraining_artifact(
        X_pre,
        model_name=str(cfg.get("model_name") or FAMILY),
        feature_ids=list(cfg.get("feature_ids") or []),
        seq_len=int(cfg.get("seq_len") or DEFAULT_SEQ_LEN),
        n_horizons=int(cfg.get("n_horizons") or DEFAULT_N_HORIZONS),
        patch_len=int(cfg.get("patch_len") or DEFAULT_PATCH_LEN),
        stride=int(cfg.get("stride") or DEFAULT_STRIDE),
        n_layers=int(cfg.get("n_layers") or DEFAULT_LAYERS),
        n_heads=int(cfg.get("n_heads") or DEFAULT_HEADS),
        d_model=int(cfg.get("d_model") or DEFAULT_D_MODEL),
        dropout=float(cfg.get("dropout") or DEFAULT_DROPOUT),
        seed=int(cfg.get("seed") or 42),
        epochs=int(os.environ.get("PATCHTST_PRETRAIN_EPOCHS", str(DEFAULT_PRETRAIN_EPOCHS))),
        lr=float(os.environ.get("PATCHTST_PRETRAIN_LR", str(DEFAULT_PRETRAIN_LR))),
        mask_ratio=float(os.environ.get("PATCHTST_PRETRAIN_MASK_RATIO", str(DEFAULT_PRETRAIN_MASK_RATIO))),
    )
    sources = sorted({str(row.get("source") or "") for row in meta_rows if str(row.get("source") or "")})
    payload["metrics"] = {
        **dict(payload.get("metrics") or {}),
        "pretraining_sources": list(sources),
        "pretraining_window_count": int(X_pre.shape[0]),
    }
    manifest = persist_pretraining_artifact(
        payload,
        model_name=str(cfg.get("model_name") or FAMILY),
        symbol="*",
        version=str(version),
    )
    register_model_version(
        model_name=str(cfg.get("model_name") or FAMILY),
        model_version=str(version),
        model_kind="patchtst_pretraining",
        stage="shadow",
        status="pretrained",
        live_ready=False,
        training_job_name="pretrain_patchtst_models",
        train_scope={
            "symbol": "*",
            "feature_ids": list(cfg.get("feature_ids") or []),
            "feature_schema": dict(payload.get("feature_schema") or {}),
            "seq_len": int(cfg.get("seq_len") or DEFAULT_SEQ_LEN),
            "mask_ratio": float((payload.get("metrics") or {}).get("mask_ratio") or 0.0),
        },
        meta={**dict(payload.get("metrics") or {}), "artifact_manifest": dict(manifest)},
    )
    record_version_performance(
        model_name=str(cfg.get("model_name") or FAMILY),
        model_version=str(version),
        metric_scope="pretraining",
        metrics={
            "reconstruction_rmse": float((payload.get("metrics") or {}).get("reconstruction_rmse") or 0.0),
            "loss_final": float((payload.get("metrics") or {}).get("loss_final") or 0.0),
            "trained_models": 1,
        },
        sample_n=int(X_pre.shape[0]),
        meta={"job_name": "pretrain_patchtst_models", "artifact_manifest": dict(manifest)},
    )
    if str(os.environ.get("PATCHTST_PRETRAIN_FINE_TUNE", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}:
        return int(main(pretraining_manifest=manifest) or 0)
    print(json.dumps({"ok": True, "family": FAMILY, "version": str(version), "stage": "shadow", "pretraining": dict(manifest)}))
    return 0


__all__ = [
    "FAMILY",
    "MaskedPatchTSTPretrainer",
    "PatchTST",
    "PatchTSTRegressor",
    "load_model_from_artifact",
    "load_pretraining_artifact_from_artifact",
    "main",
    "persist_model_artifact",
    "persist_pretraining_artifact",
    "pretrain_main",
    "register_shadow_model",
    "train_masked_pretraining_artifact",
]


if __name__ == "__main__":
    raise SystemExit(main())
