"""iTransformer sequence model family for shadow time-series challengers."""

from __future__ import annotations

import json
import logging
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
from engine.runtime.storage import init_db, run_write_txn
from engine.strategy.ensemble.oos_store import upsert_oos_predictions
from engine.strategy.feature_registry import feature_set_tag_from_ids
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
    _resolve_retrain_schema_guard,
    _safe_float,
)
from engine.strategy.models.patchtst import _load_sequence_training_rows, _preprocess_sequence_array, _sequence_array_from_features
from engine.strategy.ood import build_ood_profile, score_ood, summarize_ood_profile

FAMILY = "itransformer"
DEFAULT_SEQ_LEN = int(os.environ.get("ITRANSFORMER_SEQ_LEN", "128"))
DEFAULT_N_HORIZONS = int(os.environ.get("ITRANSFORMER_N_HORIZONS", "6"))
DEFAULT_LAYERS = int(os.environ.get("ITRANSFORMER_LAYERS", "2"))
DEFAULT_HEADS = int(os.environ.get("ITRANSFORMER_HEADS", "4"))
DEFAULT_D_MODEL = int(os.environ.get("ITRANSFORMER_D_MODEL", "64"))
DEFAULT_DROPOUT = float(os.environ.get("ITRANSFORMER_DROPOUT", "0.1"))
DEFAULT_MC_DROPOUT_SAMPLES = int(os.environ.get("ITRANSFORMER_MC_DROPOUT_SAMPLES", "0"))
DEFAULT_MIN_SAMPLES = int(os.environ.get("ITRANSFORMER_MIN_SAMPLES", "20"))
DEFAULT_LOOKBACK_DAYS = int(os.environ.get("ITRANSFORMER_LOOKBACK_DAYS", "365"))
DEFAULT_HORIZON_S = int(os.environ.get("ITRANSFORMER_HORIZON_S", os.environ.get("MODEL_HORIZON_MEDIUM_S", "3600")))


def _register_family() -> None:
    try:
        register_model_family(
            FAMILY,
            training_entrypoint="engine.strategy.jobs.train_itransformer_models",
            inference_entrypoint="engine.strategy.models.itransformer.ITransformerRegressor",
            default_stage="shadow",
            promotion_guard="engine.strategy.promotion_guard.assess_challenger",
            metadata={"architecture": "iTransformer", "serving_gate": "champion_only"},
        )
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


_register_family()

def _default_device() -> torch.device:
    resolution = resolve_torch_device(
        torch,
        env_var="ITRANSFORMER_DEVICE",
        fallback_envs=("TORCH_DEVICE",),
        legacy_cuda_flag="ITRANSFORMER_USE_CUDA",
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
    schema: dict[str, Any] = {
        "feature_ids": list(ids),
        "feature_set_tag": str(feature_set_tag_from_ids(list(ids))),
        "feature_count": int(len(ids)),
        "sequence_schema": {
            "seq_len": int(seq_len),
            "n_horizons": int(n_horizons),
            "layout": "batch,seq_len,n_features",
            "architecture": "itransformer_inverted_tokens",
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
        component="engine.strategy.models.itransformer",
        extra=extra,
        persist=True,
    )


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(value)
    except Exception:
        return int(default)


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
    artifact_seq = dict(artifact_schema.get("sequence_schema") or {})

    error: ValueError | None = None
    reason = ""
    if not artifact_tag:
        reason = "missing_feature_set_tag"
        error = ValueError(
            "itransformer_feature_schema_drift: artifact_feature_set_tag=<missing> "
            f"current_feature_set_tag={current_tag or '<missing>'}"
        )
    elif artifact_ids != feature_ids:
        reason = "artifact_column_list_mismatch"
        error = ValueError(
            "itransformer_feature_schema_drift: "
            f"artifact_feature_set_tag={artifact_tag} current_feature_set_tag={current_tag} "
            f"artifact_columns={artifact_ids} config_columns={feature_ids}"
        )
    elif current != feature_ids:
        reason = "registry_column_list_mismatch"
        error = ValueError(
            "itransformer_feature_schema_drift: "
            f"artifact_feature_set_tag={artifact_tag} current_feature_set_tag={current_tag} "
            f"artifact_columns={feature_ids} current_columns={current}"
        )
    elif artifact_tag != current_tag:
        reason = "feature_set_tag_mismatch"
        error = ValueError(
            "itransformer_feature_schema_drift: "
            f"artifact_feature_set_tag={artifact_tag} current_feature_set_tag={current_tag} "
            f"artifact_columns={feature_ids} current_columns={current}"
        )
    elif _safe_int(artifact_seq.get("seq_len"), int(config.get("seq_len") or DEFAULT_SEQ_LEN)) != int(
        config.get("seq_len") or DEFAULT_SEQ_LEN
    ):
        reason = "sequence_length_mismatch"
        error = ValueError(
            "itransformer_sequence_schema_drift: "
            f"artifact_seq_len={_safe_int(artifact_seq.get('seq_len'), 0)} "
            f"config_seq_len={int(config.get('seq_len') or DEFAULT_SEQ_LEN)}"
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
    resolution = resolve_torch_device(
        torch,
        env_var="ITRANSFORMER_DEVICE",
        fallback_envs=("TORCH_DEVICE",),
        legacy_cuda_flag="ITRANSFORMER_USE_CUDA",
    )
    if torch_device_is_cuda(torch, resolution):
        return torch.device(resolution.resolved)
    if trained_device.startswith("cuda"):
        logging.getLogger(__name__).warning(
            "itransformer_cuda_trained_loaded_on_cpu model_name=%s device_at_train=%s",
            str(config.get("model_name") or FAMILY),
            str(config.get("device_at_train") or "cuda"),
        )
    return torch.device("cpu")


class ITransformer(nn.Module):
    """Inverted transformer: feature variables are tokens, history is embedded per token."""

    def __init__(
        self,
        *,
        seq_len: int,
        n_features: int,
        n_horizons: int,
        d_model: int = DEFAULT_D_MODEL,
        n_layers: int = DEFAULT_LAYERS,
        n_heads: int = DEFAULT_HEADS,
        dropout: float = DEFAULT_DROPOUT,
    ) -> None:
        super().__init__()
        self.seq_len = int(seq_len)
        self.n_features = int(n_features)
        self.n_horizons = int(n_horizons)
        self.d_model = int(d_model)
        self.n_layers = int(n_layers)
        self.n_heads = int(n_heads)
        self.dropout = float(dropout)
        if self.seq_len <= 0 or self.n_features <= 0:
            raise ValueError("itransformer_positive_shape_required")
        self.value_projection = nn.Linear(self.seq_len, self.d_model)
        self.variable_embedding = nn.Parameter(torch.zeros(1, self.n_features, self.d_model))
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
        self.dropout_layer = nn.Dropout(self.dropout)
        self.head = nn.Linear(self.d_model, self.n_horizons)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if x.ndim != 3:
            raise ValueError("itransformer_forward_requires_3d")
        if int(x.shape[1]) != self.seq_len or int(x.shape[2]) != self.n_features:
            raise ValueError("itransformer_input_shape_mismatch")
        tokens = self.value_projection(x.transpose(1, 2)) + self.variable_embedding
        encoded = self.encoder(tokens)
        pooled = self.norm(encoded.mean(dim=1))
        return self.head(self.dropout_layer(pooled))


class ITransformerRegressor:
    """Train/serve wrapper around iTransformer with persisted feature contracts."""

    family = FAMILY
    model_kind = "itransformer"

    def __init__(
        self,
        *,
        model_name: str = FAMILY,
        feature_ids: Sequence[Any] | None = None,
        seq_len: int = DEFAULT_SEQ_LEN,
        n_horizons: int = DEFAULT_N_HORIZONS,
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
        self.n_layers = int(n_layers)
        self.n_heads = int(n_heads)
        self.d_model = int(d_model)
        self.dropout = float(dropout)
        self.seed = int(seed)
        self.device = torch.device(device) if device is not None else DEFAULT_DEVICE
        self.feature_ids = _expected_columns(feature_ids, model_name=self.model_name)
        self.model: ITransformer | None = None
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
        self.model = ITransformer(
            seq_len=self.seq_len,
            n_features=int(n_features),
            n_horizons=self.n_horizons,
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
    ) -> list[float] | "ITransformerRegressor":
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
            raise ValueError("itransformer_row_count_mismatch")
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

    def _prepare_input(self, X: Any) -> np.ndarray:
        if self.model is None or self.x_mean is None or self.x_std is None or self.y_mean is None or self.y_std is None:
            raise RuntimeError("itransformer_model_not_fitted")
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_raw = _sequence_array_from_features(X, columns, seq_len=self.seq_len)
        X_arr, _preprocessing, _accounting = _preprocess_sequence_array(
            X_raw,
            columns,
            feature_schema=self.feature_schema,
            phase="serve",
            model_name=self.model_name,
        )
        return ((X_arr - self.x_mean.reshape(1, 1, -1)) / self.x_std.reshape(1, 1, -1)).astype(np.float32)

    def predict(self, X: Any) -> np.ndarray:
        Xn = self._prepare_input(X)
        assert self.model is not None
        self.model.eval()
        with torch.no_grad():
            pred_n = self.model(torch.from_numpy(Xn).to(self.device)).detach().cpu().numpy().astype(np.float32)
        return pred_n * self.y_std.reshape(1, -1) + self.y_mean.reshape(1, -1)

    def predict_with_uncertainty(self, X: Any, *, samples: int | None = None) -> dict[str, Any]:
        Xn = self._prepare_input(X)
        assert self.model is not None and self.y_mean is not None and self.y_std is not None
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
        return {
            "prediction": float(mean_pred.reshape(-1)[0]) if mean_pred.size else 0.0,
            "prediction_vector": mean_pred.astype(float).tolist(),
            "epistemic_uncertainty": float(std_pred.reshape(-1)[0]) if std_pred.size else 0.0,
            "epistemic_uncertainty_vector": std_pred.astype(float).tolist(),
            "mc_dropout_samples": int(sample_count),
            "uncertainty_ts_ms": int(time.time() * 1000),
            "uncertainty_detail": {
                "method": "mc_dropout",
                "samples": int(sample_count),
                "dropout": float(self.dropout),
            },
        }

    def save(self, directory: str | Path) -> Path:
        target = Path(directory)
        target.mkdir(parents=True, exist_ok=True)
        if self.model is None:
            raise RuntimeError("itransformer_model_not_fitted")
        (target / "config.json").write_text(json.dumps(self._config_payload(), separators=(",", ":"), sort_keys=True), encoding="utf-8")
        (target / "state.pt").write_bytes(dumps_torch_payload({"state_dict": self.model.state_dict()}))
        return target

    @classmethod
    def load(cls, directory: str | Path) -> "ITransformerRegressor":
        target = Path(directory)
        config = json.loads((target / "config.json").read_text(encoding="utf-8"))
        obj = cls._from_config(config)
        payload = loads_torch_payload((target / "state.pt").read_bytes(), map_location=str(obj.device))
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
            raise RuntimeError("itransformer_model_not_fitted")
        return dumps_torch_payload({"config": self._config_payload(), "state_dict": self.model.state_dict()})

    @classmethod
    def _from_config(cls, config: Mapping[str, Any]) -> "ITransformerRegressor":
        feature_ids = _assert_config_feature_schema_current(config)
        load_device = _resolve_load_device(config)
        obj = cls(
            model_name=str(config.get("model_name") or FAMILY),
            feature_ids=feature_ids,
            seq_len=int(config.get("seq_len") or DEFAULT_SEQ_LEN),
            n_horizons=int(config.get("n_horizons") or DEFAULT_N_HORIZONS),
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
        obj.feature_preprocessing = dict((schema or {}).get("preprocessing") or {}) if isinstance(schema, Mapping) else {}
        obj.training_metrics = dict(config.get("training_metrics") or {})
        obj.ood_profile = dict(config.get("ood_profile") or {})
        if obj.model is None:
            obj._build_model(n_features=len(obj.feature_ids))
        return obj

    @classmethod
    def from_bytes(cls, payload: bytes) -> "ITransformerRegressor":
        raw = loads_torch_payload(payload)
        obj = cls._from_config(dict(raw.get("config") or {}))
        obj.model.load_state_dict(dict(raw.get("state_dict") or {}))
        obj.model.eval()
        return obj

    def score_ood(self, features: Any) -> dict[str, Any]:
        return score_ood(getattr(self, "ood_profile", None), features)


def persist_model_artifact(model: ITransformerRegressor, *, symbol: str = "*", version: str) -> dict[str, Any]:
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


def _oos_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    predictions: list[float] = []
    targets: list[float] = []
    for row in rows or []:
        try:
            predictions.append(float(row.get("prediction")))
        except Exception:
            continue
        if row.get("target") is not None:
            try:
                targets.append(float(row.get("target")))
            except Exception:
                targets.append(float("nan"))
    paired = [
        (pred, target)
        for pred, target in zip(predictions, targets)
        if np.isfinite(float(pred)) and np.isfinite(float(target))
    ]
    summary: dict[str, Any] = {"n": int(len(predictions))}
    if paired:
        pred_arr = np.asarray([p for p, _t in paired], dtype=np.float64)
        target_arr = np.asarray([t for _p, t in paired], dtype=np.float64)
        summary["target_n"] = int(target_arr.size)
        summary["rmse"] = float(np.sqrt(np.mean((pred_arr - target_arr) ** 2)))
        summary["directional_accuracy"] = float(np.mean(np.sign(pred_arr) == np.sign(target_arr)))
        summary["score"] = float(-summary["rmse"])
    return summary


def _persist_shadow_marketplace_visibility(
    *,
    model: ITransformerRegressor,
    version: str,
    model_ts_ms: int,
    manifest: Mapping[str, Any],
    metrics: Mapping[str, Any],
    oos_rows: Sequence[Mapping[str, Any]],
) -> None:
    summary = _oos_summary(oos_rows)
    if int(summary.get("n") or 0) <= 0:
        return
    first = dict(oos_rows[0] or {})
    symbol = str(first.get("symbol") or "*").upper().strip() or "*"
    horizon_s = _safe_int(first.get("horizon", first.get("horizon_s")), _safe_int(metrics.get("horizon_s"), DEFAULT_HORIZON_S))
    score = _safe_float(summary.get("score"), -_safe_float(metrics.get("rmse"), 0.0))
    meta = {
        "score_source": "model_oos_predictions",
        "promotion_authority": "shadow_only_oos_no_execution_authority",
        "model_family": FAMILY,
        "model_kind": model.model_kind,
        "model_ts_ms": int(model_ts_ms),
        "model_version": str(version),
        "artifact_alias": str(manifest.get("alias") or ""),
        "artifact_sha256": str(manifest.get("sha256") or ""),
        "feature_ids": list(model.feature_ids),
        "feature_set_tag": str(model.feature_schema.get("feature_set_tag") or ""),
        "feature_schema": dict(model.feature_schema),
        "oos": dict(summary),
    }

    def _write(con) -> None:
        con.execute(
            """
            INSERT INTO model_marketplace_scores(
              model_id, model_name, symbol, horizon_s, regime, stage,
              score, trades, wins, losses, gross_pnl, net_pnl,
              avg_confidence, last_signal_ts_ms, updated_ts_ms, meta_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(model_id, model_name, symbol, horizon_s, regime) DO UPDATE SET
              stage=excluded.stage,
              score=excluded.score,
              trades=excluded.trades,
              wins=excluded.wins,
              losses=excluded.losses,
              avg_confidence=excluded.avg_confidence,
              last_signal_ts_ms=excluded.last_signal_ts_ms,
              updated_ts_ms=excluded.updated_ts_ms,
              meta_json=excluded.meta_json
            """,
            (
                f"{str(model.model_name)}:{str(version)}",
                str(model.model_name),
                str(symbol),
                int(horizon_s),
                "global",
                "shadow",
                float(score),
                int(summary.get("target_n") or summary.get("n") or 0),
                0,
                0,
                0.0,
                0.0,
                0.0,
                _safe_int(first.get("ts", first.get("ts_ms")), int(model_ts_ms)),
                int(time.time() * 1000),
                json.dumps(meta, separators=(",", ":"), sort_keys=True),
            ),
        )

    run_write_txn(_write)


def register_shadow_model(
    model: ITransformerRegressor,
    *,
    symbol: str = "*",
    version: str | None = None,
    performance_metrics: Mapping[str, Any] | None = None,
    oos_predictions: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    version_s = str(version or version_from_ts(str(model.model_name), int(time.time() * 1000), prefix=FAMILY))
    manifest = persist_model_artifact(model, symbol=str(symbol), version=str(version_s))
    model_ts_ms = int(time.time() * 1000)
    oos_rows = [dict(row or {}) for row in list(oos_predictions or [])]
    oos_count = 0
    if oos_rows:
        oos_count = int(upsert_oos_predictions(oos_rows))
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
        "oos_prediction_count": int(oos_count),
    }
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
        training_job_name="train_itransformer_models",
        train_scope={
            "symbol": str(symbol or "*").upper(),
            "feature_ids": list(model.feature_ids),
            "feature_schema": dict(model.feature_schema),
            "seq_len": int(model.seq_len),
            "n_horizons": int(model.n_horizons),
        },
        meta=dict(metrics),
    )
    if oos_rows:
        _persist_shadow_marketplace_visibility(
            model=model,
            version=str(version_s),
            model_ts_ms=int(model_ts_ms),
            manifest=dict(manifest),
            metrics=dict(metrics),
            oos_rows=oos_rows,
        )
    return {"version": version_s, "stage": "shadow", "artifact_manifest": manifest, "metrics": metrics}


def load_model_from_artifact(alias: str = "", sha256: str = "", path: str | Path | None = None) -> ITransformerRegressor:
    if path is not None and str(path).strip():
        return ITransformerRegressor.load(Path(path))
    payload = _artifact_payload_from_alias(str(alias or ""), str(sha256 or ""))
    if not payload:
        raise FileNotFoundError("itransformer_artifact_not_found")
    return ITransformerRegressor.from_bytes(payload)


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
        "supervised_target": str(cfg.get("supervised_target") or os.environ.get("ITRANSFORMER_SUPERVISED_TARGET", "net_edge")),
    }


def main() -> int:
    init_db()
    plan = load_lifecycle_plan(FAMILY)
    cfg = _resolve_training_config(plan)
    built = _load_sequence_training_rows(cfg)
    min_samples = int(os.environ.get("ITRANSFORMER_MIN_SAMPLES", str(DEFAULT_MIN_SAMPLES)))
    if built is None or int(built[0].shape[0]) < max(2, min_samples):
        n = 0 if built is None else int(built[0].shape[0])
        print(f"{FAMILY}: insufficient_samples n={n} min_required={max(2, min_samples)}")
        return 0
    X, y, meta_rows = built
    n_samples = int(y.shape[0])
    split = min(max(1, int(n_samples * 0.8)), int(n_samples - 1))
    model = ITransformerRegressor(
        model_name=str(cfg.get("model_name") or FAMILY),
        feature_ids=list(cfg.get("feature_ids") or []),
        seq_len=int(cfg.get("seq_len") or DEFAULT_SEQ_LEN),
        n_horizons=int(cfg.get("n_horizons") or DEFAULT_N_HORIZONS),
    )
    model.fit(
        X[:split],
        y[:split],
        epochs=int(os.environ.get("ITRANSFORMER_EPOCHS", "20")),
        lr=float(os.environ.get("ITRANSFORMER_LR", "0.001")),
    )
    target_sources = sorted({str(row.get("target_source") or "") for row in meta_rows if str(row.get("target_source") or "")})
    model.training_metrics["supervised_target"] = str(cfg.get("supervised_target") or "net_edge")
    model.training_metrics["supervised_target_sources"] = list(target_sources)
    model.training_metrics["horizon_s"] = int(cfg.get("horizon_s") or DEFAULT_HORIZON_S)
    pred_eval = model.predict(X[split:])
    horizon_s = int(cfg.get("horizon_s") or DEFAULT_HORIZON_S)
    oos_run_id = str(uuid.uuid4())
    oos_rows = [
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
    version = str(
        plan.get("model_version")
        or cfg.get("training_version_id")
        or version_from_ts(str(model.model_name), int(time.time() * 1000), prefix=FAMILY)
    )
    result = register_shadow_model(model, symbol="*", version=str(version), oos_predictions=oos_rows)
    metrics = dict(result.get("metrics") or {})
    record_version_performance(
        model_name=str(model.model_name),
        model_version=str(version),
        metric_scope="training",
        metrics={"avg_rmse": float(metrics.get("rmse") or 0.0), "quality_score": 0.0, "trained_models": 1},
        sample_n=int(X.shape[0]),
        meta={"job_name": "train_itransformer_models"},
    )
    update_model_version_status(
        str(model.model_name),
        str(version),
        status="shadow",
        stage="shadow",
        live_ready=False,
    )
    print(
        f"{FAMILY}: registered shadow model={model.model_name} version={version} "
        f"n_train={split} n_oos={len(oos_rows)} artifact={result['artifact_manifest']['sha256']}"
    )
    return 0


__all__ = [
    "FAMILY",
    "ITransformer",
    "ITransformerRegressor",
    "load_model_from_artifact",
    "main",
    "persist_model_artifact",
    "register_shadow_model",
]
