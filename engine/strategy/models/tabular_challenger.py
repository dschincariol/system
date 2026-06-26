"""Governed tabular-foundation challenger family.

This module keeps LightGBM/XGBoost as the production tabular baselines and adds
optional TabPFN/TabM-style challengers behind explicit configuration, optional
dependencies, and license review.  The default backend is a deterministic fake
estimator used for tests and dry-run plumbing only.
"""

from __future__ import annotations

import inspect
import json
import logging
import os
import resource
import signal
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from engine.artifacts.store import LocalArtifactStore
from engine.model_registry import register_model, register_model_family
from engine.runtime.storage import init_db
from engine.runtime.workload_profiles import assert_offline_work_allowed
from engine.strategy.ensemble.oos_store import upsert_oos_predictions
from engine.strategy.model_lifecycle import (
    load_lifecycle_plan,
    record_version_performance,
    register_model_version,
    update_model_version_status,
    version_from_ts,
)
from engine.strategy.models.lgbm_regressor import (
    DEFAULT_HORIZON_S,
    DEFAULT_LOOKBACK_DAYS,
    DEFAULT_MIN_SAMPLES,
    _artifact_payload_from_alias,
    _assert_loaded_feature_schema_current,
    _expected_columns,
    _feature_schema,
    _load_joblib_from_bytes,
    _load_training_rows,
    _matrix_from_features,
    _resolve_retrain_schema_guard,
    _resolve_training_config,
    _safe_float,
    _safe_int,
    _dump_joblib_to_bytes,
)
from engine.strategy.ood import build_ood_profile, score_ood, summarize_ood_profile

FAMILY = "tabular_foundation_challenger"
DEFAULT_MODEL_NAME = FAMILY
DEFAULT_MODEL_KIND = "tabular_foundation"
DEFAULT_BACKEND = "fake"
DEFAULT_TASK = "regression"
LOG = logging.getLogger(__name__)


class OptionalDependencyMissing(RuntimeError):
    """Raised when a configured optional backend is not installed."""


class LicenseReviewRequired(RuntimeError):
    """Raised when a real optional backend is enabled without review ack."""


class NoOpTraining(RuntimeError):
    """Raised when fallback policy requests an explicit no-op."""


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "y", "on"}


def _normalize_backend(value: Any) -> str:
    text = str(value or DEFAULT_BACKEND).strip().lower().replace("-", "_")
    aliases = {
        "deterministic": "fake",
        "deterministic_fake": "fake",
        "test": "fake",
        "tabpfn2": "tabpfn",
        "tabpfn_2_5": "tabpfn",
        "tabpfn25": "tabpfn",
    }
    return aliases.get(text, text or DEFAULT_BACKEND)


def _normalize_task(value: Any) -> str:
    text = str(value or DEFAULT_TASK).strip().lower().replace("-", "_")
    if text in {"classification", "classifier", "meta_label", "meta_labeling", "binary"}:
        return "classification"
    if text in {"rank", "ranking", "cross_sectional_rank"}:
        return "ranking"
    return "regression"


def _fallback_policy(value: Any) -> str:
    text = str(value or os.environ.get("TABULAR_CHALLENGER_FALLBACK_POLICY", "fail_closed")).strip().lower()
    if text in {"fake", "deterministic_fake"}:
        return "fake"
    if text in {"noop", "no_op", "skip"}:
        return "noop"
    return "fail_closed"


def _max_rows(config: Mapping[str, Any] | None = None) -> int:
    cfg = dict(config or {})
    return max(1, _safe_int(cfg.get("max_rows") or os.environ.get("TABULAR_CHALLENGER_MAX_ROWS"), 10_000))


def _max_features(config: Mapping[str, Any] | None = None) -> int:
    cfg = dict(config or {})
    return max(1, _safe_int(cfg.get("max_features") or os.environ.get("TABULAR_CHALLENGER_MAX_FEATURES"), 500))


def _timeout_s(config: Mapping[str, Any] | None = None) -> int:
    cfg = dict(config or {})
    return max(0, _safe_int(cfg.get("timeout_s") or os.environ.get("TABULAR_CHALLENGER_TIMEOUT_S"), 900))


def _device(config: Mapping[str, Any] | None = None) -> str:
    cfg = dict(config or {})
    return str(cfg.get("device") or os.environ.get("TABULAR_CHALLENGER_DEVICE", "cpu") or "cpu").strip() or "cpu"


def _license_review_acknowledged() -> bool:
    return bool(
        _env_bool("TABULAR_CHALLENGER_LICENSE_REVIEW_ACK", False)
        or _env_bool("TABULAR_CHALLENGER_ALLOW_NONCOMMERCIAL", False)
    )


def _assert_real_backend_license_reviewed(backend: str) -> None:
    if backend == "fake":
        return
    if not _license_review_acknowledged():
        raise LicenseReviewRequired(
            "tabular_challenger_license_review_required:"
            f"backend={backend}:set_TABULAR_CHALLENGER_LICENSE_REVIEW_ACK=1_after_review"
        )


@contextmanager
def _training_timeout(seconds: int):
    if int(seconds or 0) <= 0:
        yield
        return
    if threading.current_thread() is not threading.main_thread() or not hasattr(signal, "SIGALRM"):
        yield
        return

    def _alarm(_signum: int, _frame: Any) -> None:
        raise TimeoutError(f"tabular_challenger_training_timeout_s={int(seconds)}")

    old_handler = signal.getsignal(signal.SIGALRM)
    old_timer = signal.setitimer(signal.ITIMER_REAL, float(seconds))
    signal.signal(signal.SIGALRM, _alarm)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        if old_timer and old_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, old_timer[0], old_timer[1])
        signal.signal(signal.SIGALRM, old_handler)


def _current_resource_snapshot() -> dict[str, Any]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {
        "max_rss_kb": int(getattr(usage, "ru_maxrss", 0) or 0),
        "user_cpu_s": float(getattr(usage, "ru_utime", 0.0) or 0.0),
        "system_cpu_s": float(getattr(usage, "ru_stime", 0.0) or 0.0),
    }


def _finite_vector(values: Any) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float64).reshape(-1)
    return arr[np.isfinite(arr)]


def _rank_vector(values: np.ndarray) -> np.ndarray:
    order = np.argsort(np.asarray(values, dtype=np.float64), kind="mergesort")
    ranks = np.empty_like(order, dtype=np.float64)
    ranks[order] = np.arange(int(order.size), dtype=np.float64)
    return ranks


def _safe_corr(a: Any, b: Any, *, rank: bool = False) -> float:
    left = np.asarray(a, dtype=np.float64).reshape(-1)
    right = np.asarray(b, dtype=np.float64).reshape(-1)
    n = min(int(left.size), int(right.size))
    if n <= 1:
        return 0.0
    left = left[:n]
    right = right[:n]
    mask = np.isfinite(left) & np.isfinite(right)
    if int(mask.sum()) <= 1:
        return 0.0
    left = left[mask]
    right = right[mask]
    if rank:
        left = _rank_vector(left)
        right = _rank_vector(right)
    if float(np.std(left)) <= 0.0 or float(np.std(right)) <= 0.0:
        return 0.0
    return float(np.corrcoef(left, right)[0, 1])


def calibration_diagnostics(
    *,
    predictions: Any,
    targets: Any,
    task: str,
    probabilities: Any = None,
) -> dict[str, Any]:
    task_s = _normalize_task(task)
    pred = np.asarray(predictions, dtype=np.float64).reshape(-1)
    truth = np.asarray(targets, dtype=np.float64).reshape(-1)
    n = min(int(pred.size), int(truth.size))
    if n <= 0:
        return {"task": task_s, "n": 0, "status": "empty"}
    pred = pred[:n]
    truth = truth[:n]
    mask = np.isfinite(pred) & np.isfinite(truth)
    pred = pred[mask]
    truth = truth[mask]
    if int(pred.size) <= 0:
        return {"task": task_s, "n": 0, "status": "no_finite_pairs"}
    err = pred - truth
    out: dict[str, Any] = {
        "task": task_s,
        "n": int(pred.size),
        "rmse": float(np.sqrt(np.mean(err * err))),
        "mae": float(np.mean(np.abs(err))),
        "bias": float(np.mean(err)),
        "directional_accuracy": float(np.mean(np.sign(pred) == np.sign(truth))),
    }
    if task_s == "ranking":
        out["rank_ic"] = float(_safe_corr(pred, truth, rank=True))
    if task_s == "classification":
        labels = (truth > 0.5).astype(np.int32)
        pred_labels = (pred > 0.5).astype(np.int32)
        out["accuracy"] = float(np.mean(pred_labels == labels))
        if probabilities is not None:
            proba = np.asarray(probabilities, dtype=np.float64)
            if proba.ndim == 2 and int(proba.shape[1]) >= 2:
                positive = proba[: int(labels.size), 1]
            else:
                positive = proba.reshape(-1)[: int(labels.size)]
            proba_mask = np.isfinite(positive)
            if int(proba_mask.sum()) > 0:
                positive = np.clip(positive[proba_mask], 0.0, 1.0)
                label_subset = labels[proba_mask]
                out["brier"] = float(np.mean((positive - label_subset) ** 2))
    return out


def _model_card(
    *,
    backend: str,
    task: str,
    model_name: str,
    feature_schema: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "family": FAMILY,
        "model_name": str(model_name),
        "backend": str(backend),
        "task": _normalize_task(task),
        "stage": "shadow",
        "primary_baselines": ["lightgbm", "xgboost", "sklearn_gbm"],
        "intended_use_cases": [
            "small_medium_sample_regression",
            "classification_meta_labeling",
            "cross_sectional_rank_labels",
        ],
        "not_intended_for": [
            "replacement_for_gbm_baselines",
            "direct_live_promotion_without_normal_evidence",
            "large_unbounded_training_jobs",
        ],
        "feature_schema": dict(feature_schema),
        "governance": {
            "default_stage": "shadow",
            "promotion_guard": "engine.strategy.promotion_guard.assess_challenger",
            "requires_normal_evidence": True,
            "requires_net_cost_replay_cpcv_fdr_deconfounded_cooldown_gates": True,
        },
        "limits": {
            "max_rows": int(_max_rows(config)),
            "max_features": int(_max_features(config)),
            "timeout_s": int(_timeout_s(config)),
            "device": _device(config),
        },
        "license_review": {
            "required_for_real_backends": True,
            "acknowledged": bool(_license_review_acknowledged()) if str(backend) != "fake" else True,
        },
        "distillation_export": {
            "available": False,
            "reason": "no_repo_approved_backend_export_path",
        },
    }


class DeterministicFakeTabularEstimator:
    """Deterministic estimator used by tests and dry-run fallback policy."""

    backend = "fake"

    def __init__(self, *, task: str = DEFAULT_TASK, seed: int = 17, fallback_reason: str = "") -> None:
        self.task = _normalize_task(task)
        self.seed = int(seed)
        self.fallback_reason = str(fallback_reason or "")
        self.coef_: np.ndarray | None = None
        self.classes_: np.ndarray = np.asarray([0, 1], dtype=np.int64)

    def fit(self, X: Any, y: Any, sample_weight: Any = None) -> "DeterministicFakeTabularEstimator":
        X_arr = np.asarray(X, dtype=np.float64)
        if X_arr.ndim != 2:
            raise ValueError("fake_estimator_X_must_be_2d")
        y_arr = np.asarray(y).reshape(-1)
        if int(X_arr.shape[0]) != int(y_arr.shape[0]):
            raise ValueError("fake_estimator_row_count_mismatch")
        design = np.column_stack([np.ones(int(X_arr.shape[0]), dtype=np.float64), X_arr])
        if self.task == "classification":
            classes = np.unique(y_arr)
            self.classes_ = classes if int(classes.size) > 0 else np.asarray([0, 1])
            if int(self.classes_.size) <= 2:
                target = (y_arr == self.classes_[-1]).astype(np.float64)
                if sample_weight is not None:
                    weights = np.sqrt(np.maximum(np.asarray(sample_weight, dtype=np.float64).reshape(-1), 0.0))
                    design = design * weights[:, None]
                    target = target * weights
                self.coef_ = np.linalg.pinv(design) @ target
            else:
                target_matrix = np.column_stack([(y_arr == cls).astype(np.float64) for cls in self.classes_])
                self.coef_ = np.linalg.pinv(design) @ target_matrix
            return self
        target = np.asarray(y_arr, dtype=np.float64)
        if sample_weight is not None:
            weights = np.sqrt(np.maximum(np.asarray(sample_weight, dtype=np.float64).reshape(-1), 0.0))
            design = design * weights[:, None]
            target = target * weights
        self.coef_ = np.linalg.pinv(design) @ target
        return self

    def _raw_score(self, X: Any) -> np.ndarray:
        if self.coef_ is None:
            raise RuntimeError("fake_estimator_not_fitted")
        X_arr = np.asarray(X, dtype=np.float64)
        if X_arr.ndim != 2:
            raise ValueError("fake_estimator_X_must_be_2d")
        design = np.column_stack([np.ones(int(X_arr.shape[0]), dtype=np.float64), X_arr])
        return np.asarray(design @ self.coef_, dtype=np.float64)

    def predict_proba(self, X: Any) -> np.ndarray:
        raw = np.asarray(self._raw_score(X), dtype=np.float64)
        if raw.ndim == 1:
            p1 = 1.0 / (1.0 + np.exp(-np.clip(raw, -35.0, 35.0)))
            return np.column_stack([1.0 - p1, p1]).astype(np.float64)
        raw = raw - np.max(raw, axis=1, keepdims=True)
        exp = np.exp(np.clip(raw, -35.0, 35.0))
        denom = np.maximum(exp.sum(axis=1, keepdims=True), 1e-12)
        return (exp / denom).astype(np.float64)

    def predict(self, X: Any) -> np.ndarray:
        if self.task == "classification":
            proba = self.predict_proba(X)
            labels = np.argmax(proba, axis=1)
            if int(self.classes_.size) >= int(proba.shape[1]):
                return np.asarray([self.classes_[int(idx)] for idx in labels])
            return labels.astype(np.int64)
        return np.asarray(self._raw_score(X), dtype=np.float64).reshape(-1)

    def metadata(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "task": self.task,
            "seed": int(self.seed),
            "fallback_reason": str(self.fallback_reason),
        }


class TabPFNEstimatorAdapter:
    backend = "tabpfn"

    def __init__(self, *, task: str, device: str, hyperparams: Mapping[str, Any] | None = None) -> None:
        self.task = _normalize_task(task)
        self.device = str(device or "cpu")
        self.hyperparams = dict(hyperparams or {})
        self.estimator: Any = None
        self.backend_version = str(self.hyperparams.get("model_version") or os.environ.get("TABULAR_CHALLENGER_TABPFN_VERSION", "") or "")

    def _new_estimator(self) -> Any:
        try:
            from tabpfn import TabPFNClassifier, TabPFNRegressor
        except ImportError as exc:
            raise OptionalDependencyMissing("tabpfn_not_installed") from exc
        cls = TabPFNClassifier if self.task == "classification" else TabPFNRegressor
        kwargs = dict(self.hyperparams)
        kwargs.pop("model_version", None)
        kwargs.setdefault("device", self.device)
        version = str(self.backend_version or "").strip()
        if version and hasattr(cls, "create_default_for_version"):
            try:
                from tabpfn.constants import ModelVersion

                enum_name = version.upper().replace(".", "_").replace("-", "_")
                enum_value = getattr(ModelVersion, enum_name, None)
                if enum_value is not None:
                    estimator = cls.create_default_for_version(enum_value)
                    for key, value in kwargs.items():
                        if hasattr(estimator, key):
                            setattr(estimator, key, value)
                    return estimator
            except Exception:
                LOG.debug("Ignored recoverable exception.", exc_info=True)
        signature = inspect.signature(cls)
        filtered = {key: value for key, value in kwargs.items() if key in signature.parameters}
        return cls(**filtered)

    def fit(self, X: Any, y: Any, sample_weight: Any = None) -> "TabPFNEstimatorAdapter":
        estimator = self._new_estimator()
        fit_kwargs: dict[str, Any] = {}
        if sample_weight is not None:
            try:
                if "sample_weight" in inspect.signature(estimator.fit).parameters:
                    fit_kwargs["sample_weight"] = sample_weight
            except Exception:
                LOG.debug("Ignored recoverable exception.", exc_info=True)
        estimator.fit(np.asarray(X, dtype=np.float32), np.asarray(y).reshape(-1), **fit_kwargs)
        self.estimator = estimator
        return self

    def predict(self, X: Any) -> np.ndarray:
        if self.estimator is None:
            raise RuntimeError("tabpfn_estimator_not_fitted")
        return np.asarray(self.estimator.predict(np.asarray(X, dtype=np.float32))).reshape(-1)

    def predict_proba(self, X: Any) -> np.ndarray:
        if self.estimator is None or not hasattr(self.estimator, "predict_proba"):
            raise RuntimeError("tabpfn_predict_proba_unavailable")
        return np.asarray(self.estimator.predict_proba(np.asarray(X, dtype=np.float32)))

    def metadata(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "task": self.task,
            "device": self.device,
            "model_version": str(self.backend_version or "package_default"),
        }


class TabMEstimatorAdapter:
    backend = "tabm"

    def __init__(self, *, task: str, device: str, hyperparams: Mapping[str, Any] | None = None) -> None:
        self.task = _normalize_task(task)
        self.device = str(device or "cpu")
        self.hyperparams = dict(hyperparams or {})
        self.model: Any = None
        self.classes_: np.ndarray = np.asarray([0, 1], dtype=np.int64)

    def fit(self, X: Any, y: Any, sample_weight: Any = None) -> "TabMEstimatorAdapter":
        try:
            import torch
            from tabm import TabM
        except ImportError as exc:
            raise OptionalDependencyMissing("tabm_not_installed") from exc

        X_arr = np.asarray(X, dtype=np.float32)
        y_arr = np.asarray(y).reshape(-1)
        if int(X_arr.shape[0]) != int(y_arr.shape[0]):
            raise ValueError("tabm_row_count_mismatch")
        torch.manual_seed(_safe_int(self.hyperparams.get("seed"), 17))
        device = torch.device(self.device)
        X_t = torch.as_tensor(X_arr, dtype=torch.float32, device=device)
        task = self.task
        d_out = 1
        target: Any
        if task == "classification":
            self.classes_ = np.unique(y_arr)
            if int(self.classes_.size) <= 2:
                d_out = 1
                target = torch.as_tensor((y_arr == self.classes_[-1]).astype(np.float32), device=device)
            else:
                d_out = int(self.classes_.size)
                class_to_idx = {value: idx for idx, value in enumerate(list(self.classes_))}
                target = torch.as_tensor([class_to_idx[value] for value in y_arr], dtype=torch.long, device=device)
        else:
            target = torch.as_tensor(np.asarray(y_arr, dtype=np.float32), dtype=torch.float32, device=device)

        model_kwargs = {
            key: value
            for key, value in dict(self.hyperparams).items()
            if key not in {"epochs", "lr", "weight_decay", "batch_size", "seed"}
        }
        model = TabM.make(n_num_features=int(X_arr.shape[1]), cat_cardinalities=[], d_out=int(d_out), **model_kwargs)
        model.to(device)
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=float(self.hyperparams.get("lr", os.environ.get("TABULAR_CHALLENGER_TABM_LR", 0.002))),
            weight_decay=float(
                self.hyperparams.get("weight_decay", os.environ.get("TABULAR_CHALLENGER_TABM_WEIGHT_DECAY", 0.0003))
            ),
        )
        epochs = max(1, _safe_int(self.hyperparams.get("epochs") or os.environ.get("TABULAR_CHALLENGER_TABM_EPOCHS"), 20))
        weights = None
        if sample_weight is not None:
            weights = torch.as_tensor(np.asarray(sample_weight, dtype=np.float32).reshape(-1), device=device)

        model.train()
        for _epoch in range(int(epochs)):
            optimizer.zero_grad(set_to_none=True)
            raw = model(X_t)
            if task == "classification":
                if int(d_out) == 1:
                    loss_matrix = torch.nn.functional.binary_cross_entropy_with_logits(
                        raw[..., 0],
                        target[:, None].expand_as(raw[..., 0]),
                        reduction="none",
                    )
                    if weights is not None:
                        loss_matrix = loss_matrix * weights[:, None]
                    loss = loss_matrix.mean()
                else:
                    flat_raw = raw.reshape(-1, int(d_out))
                    repeated_target = target[:, None].expand(int(target.shape[0]), int(raw.shape[1])).reshape(-1)
                    loss = torch.nn.functional.cross_entropy(flat_raw, repeated_target)
            else:
                loss_matrix = (raw[..., 0] - target[:, None].expand_as(raw[..., 0])) ** 2
                if weights is not None:
                    loss_matrix = loss_matrix * weights[:, None]
                loss = loss_matrix.mean()
            loss.backward()
            optimizer.step()
        model.eval()
        self.model = model
        return self

    def _raw(self, X: Any) -> Any:
        if self.model is None:
            raise RuntimeError("tabm_estimator_not_fitted")
        import torch

        X_t = torch.as_tensor(np.asarray(X, dtype=np.float32), dtype=torch.float32, device=next(self.model.parameters()).device)
        with torch.no_grad():
            raw = self.model(X_t).mean(dim=1)
        return raw.detach().cpu().numpy()

    def predict_proba(self, X: Any) -> np.ndarray:
        raw = np.asarray(self._raw(X), dtype=np.float64)
        if raw.ndim == 1 or int(raw.shape[1]) == 1:
            logits = raw.reshape(-1)
            p1 = 1.0 / (1.0 + np.exp(-np.clip(logits, -35.0, 35.0)))
            return np.column_stack([1.0 - p1, p1])
        raw = raw - np.max(raw, axis=1, keepdims=True)
        exp = np.exp(np.clip(raw, -35.0, 35.0))
        return exp / np.maximum(exp.sum(axis=1, keepdims=True), 1e-12)

    def predict(self, X: Any) -> np.ndarray:
        if self.task == "classification":
            proba = self.predict_proba(X)
            labels = np.argmax(proba, axis=1)
            if int(self.classes_.size) >= int(proba.shape[1]):
                return np.asarray([self.classes_[int(idx)] for idx in labels])
            return labels.astype(np.int64)
        return np.asarray(self._raw(X), dtype=np.float64).reshape(-1)

    def metadata(self) -> dict[str, Any]:
        return {"backend": self.backend, "task": self.task, "device": self.device}


def _select_adapter(
    *,
    backend: str,
    task: str,
    device: str,
    hyperparams: Mapping[str, Any] | None = None,
    fallback_policy: str = "fail_closed",
) -> Any:
    backend_s = _normalize_backend(backend)
    task_s = _normalize_task(task)
    if backend_s == "fake":
        return DeterministicFakeTabularEstimator(task=task_s)
    try:
        _assert_real_backend_license_reviewed(backend_s)
        if backend_s == "tabpfn":
            return TabPFNEstimatorAdapter(task=task_s, device=device, hyperparams=hyperparams)
        if backend_s == "tabm":
            return TabMEstimatorAdapter(task=task_s, device=device, hyperparams=hyperparams)
    except (OptionalDependencyMissing, LicenseReviewRequired):
        raise
    except Exception as exc:
        raise RuntimeError(f"tabular_challenger_backend_init_failed:{backend_s}:{type(exc).__name__}") from exc
    raise ValueError(f"unsupported_tabular_challenger_backend:{backend_s}")


def _adapter_with_fallback(
    *,
    backend: str,
    task: str,
    device: str,
    hyperparams: Mapping[str, Any] | None = None,
    fallback_policy: str,
) -> Any:
    backend_s = _normalize_backend(backend)
    try:
        return _select_adapter(
            backend=backend_s,
            task=task,
            device=device,
            hyperparams=hyperparams,
            fallback_policy=fallback_policy,
        )
    except (OptionalDependencyMissing, LicenseReviewRequired, ValueError) as exc:
        policy = _fallback_policy(fallback_policy)
        if policy == "fake":
            return DeterministicFakeTabularEstimator(task=task, fallback_reason=str(exc))
        if policy == "noop":
            raise NoOpTraining(str(exc)) from exc
        raise


class TabularFoundationChallengerModel:
    """Schema-bound tabular foundation/deep-tabular challenger wrapper."""

    family = FAMILY
    model_kind = DEFAULT_MODEL_KIND

    def __init__(
        self,
        *,
        model_name: str = DEFAULT_MODEL_NAME,
        feature_ids: Sequence[Any] | None = None,
        backend: str = DEFAULT_BACKEND,
        task: str = DEFAULT_TASK,
        hyperparams: Mapping[str, Any] | None = None,
        config: Mapping[str, Any] | None = None,
        estimator: Any = None,
        training_metrics: Mapping[str, Any] | None = None,
    ) -> None:
        self.model_name = str(model_name or DEFAULT_MODEL_NAME).strip() or DEFAULT_MODEL_NAME
        self.backend = _normalize_backend(backend)
        self.task = _normalize_task(task)
        self.config = dict(config or {})
        self.feature_ids = _expected_columns(feature_ids, model_name=self.model_name)
        self.hyperparams = dict(hyperparams or {})
        self.estimator = estimator
        self.training_metrics = dict(training_metrics or {})
        metrics_schema = self.training_metrics.get("feature_schema") if isinstance(self.training_metrics, Mapping) else None
        self.persisted_feature_schema = dict(metrics_schema) if isinstance(metrics_schema, Mapping) else dict(self.feature_schema)
        self.ood_profile = dict(getattr(self, "ood_profile", {}) or {})

    @property
    def feature_schema(self) -> dict[str, Any]:
        return _feature_schema(self.feature_ids)

    def _limits_diagnostics(self, X_arr: np.ndarray) -> dict[str, Any]:
        return {
            "rows": int(X_arr.shape[0]),
            "features": int(X_arr.shape[1]),
            "max_rows": int(_max_rows(self.config)),
            "max_features": int(_max_features(self.config)),
            "timeout_s": int(_timeout_s(self.config)),
            "device": _device(self.config),
        }

    def _assert_within_limits(self, X_arr: np.ndarray) -> None:
        if int(X_arr.shape[0]) > int(_max_rows(self.config)):
            raise RuntimeError(f"tabular_challenger_max_rows_exceeded:{int(X_arr.shape[0])}>{int(_max_rows(self.config))}")
        if int(X_arr.shape[1]) > int(_max_features(self.config)):
            raise RuntimeError(
                f"tabular_challenger_max_features_exceeded:{int(X_arr.shape[1])}>{int(_max_features(self.config))}"
            )

    def fit(self, X: Any, y: Any, sample_weight: Any = None, **_kwargs: Any) -> "TabularFoundationChallengerModel":
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_arr = _matrix_from_features(
            X,
            columns,
            feature_schema=self.feature_schema,
            phase="train",
            model_name=self.model_name,
        )
        y_arr = np.asarray(y).reshape(-1)
        if int(X_arr.shape[0]) != int(y_arr.shape[0]):
            raise ValueError("tabular_challenger_row_count_mismatch")
        self._assert_within_limits(X_arr)
        self.feature_ids = list(columns)
        started = time.perf_counter()
        resource_before = _current_resource_snapshot()
        adapter = self.estimator or _adapter_with_fallback(
            backend=self.backend,
            task=self.task,
            device=_device(self.config),
            hyperparams=self.hyperparams,
            fallback_policy=_fallback_policy(self.config.get("fallback_policy")),
        )
        with _training_timeout(_timeout_s(self.config)):
            adapter.fit(X_arr, y_arr, sample_weight=sample_weight)
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        resource_after = _current_resource_snapshot()
        if _timeout_s(self.config) > 0 and elapsed_ms > int(_timeout_s(self.config) * 1000):
            raise TimeoutError(f"tabular_challenger_training_timeout_s={_timeout_s(self.config)}")
        self.estimator = adapter
        self.ood_profile = build_ood_profile(X_arr, columns)
        predictions = np.asarray(adapter.predict(X_arr)).reshape(-1)
        probabilities = None
        if self.task == "classification" and hasattr(adapter, "predict_proba"):
            try:
                probabilities = adapter.predict_proba(X_arr)
            except Exception:
                probabilities = None
        calibration = calibration_diagnostics(
            predictions=predictions,
            targets=y_arr,
            task=self.task,
            probabilities=probabilities,
        )
        adapter_meta = adapter.metadata() if hasattr(adapter, "metadata") else {"backend": self.backend}
        self.training_metrics = {
            "n_train": int(y_arr.shape[0]),
            "model_family": FAMILY,
            "model_kind": self.model_kind,
            "backend": str(getattr(adapter, "backend", self.backend)),
            "task": self.task,
            "feature_schema": self.feature_schema,
            "model_card": _model_card(
                backend=str(getattr(adapter, "backend", self.backend)),
                task=self.task,
                model_name=self.model_name,
                feature_schema=self.feature_schema,
                config=self.config,
            ),
            "calibration_diagnostics": dict(calibration),
            "latency_resource_diagnostics": {
                "train_elapsed_ms": int(elapsed_ms),
                "limits": self._limits_diagnostics(X_arr),
                "resource_before": dict(resource_before),
                "resource_after": dict(resource_after),
            },
            "backend_metadata": dict(adapter_meta),
            "ood_profile_summary": summarize_ood_profile(self.ood_profile),
            "distillation_export": {
                "available": False,
                "reason": "backend_export_not_enabled_or_not_supported",
            },
        }
        self.persisted_feature_schema = dict(self.feature_schema)
        return self

    def predict(self, X: Any) -> np.ndarray:
        if self.estimator is None:
            raise RuntimeError("tabular_challenger_model_not_fitted")
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_arr = _matrix_from_features(
            X,
            columns,
            feature_schema=getattr(self, "persisted_feature_schema", None) or self.feature_schema,
            phase="serve",
            model_name=self.model_name,
        )
        return np.asarray(self.estimator.predict(X_arr)).reshape(-1)

    def predict_proba(self, X: Any) -> np.ndarray:
        if self.estimator is None or not hasattr(self.estimator, "predict_proba"):
            raise RuntimeError("tabular_challenger_predict_proba_unavailable")
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_arr = _matrix_from_features(
            X,
            columns,
            feature_schema=getattr(self, "persisted_feature_schema", None) or self.feature_schema,
            phase="serve",
            model_name=self.model_name,
        )
        return np.asarray(self.estimator.predict_proba(X_arr))

    def predict_one(self, features: Mapping[str, Any]) -> float:
        pred = self.predict({"features": dict(features)})
        if self.task == "classification":
            return float(_safe_float(pred[0], 0.0))
        return float(pred[0])

    def score_ood(self, features: Any) -> dict[str, Any]:
        return score_ood(getattr(self, "ood_profile", None), features)

    def to_bytes(self) -> bytes:
        return _dump_joblib_to_bytes(self)

    @classmethod
    def from_bytes(cls, payload: bytes) -> "TabularFoundationChallengerModel":
        loaded = _load_joblib_from_bytes(payload)
        if not isinstance(loaded, cls):
            raise TypeError("invalid_tabular_challenger_payload")
        _assert_loaded_feature_schema_current(loaded)
        return loaded


def train_tabular_challenger(
    X: Any,
    y: Any,
    *,
    feature_ids: Sequence[Any] | None = None,
    sample_weight: Any = None,
    backend: str = DEFAULT_BACKEND,
    task: str = DEFAULT_TASK,
    hyperparams: Mapping[str, Any] | None = None,
    model_name: str = DEFAULT_MODEL_NAME,
    config: Mapping[str, Any] | None = None,
) -> TabularFoundationChallengerModel:
    return TabularFoundationChallengerModel(
        model_name=str(model_name or DEFAULT_MODEL_NAME),
        feature_ids=feature_ids,
        backend=backend,
        task=task,
        hyperparams=hyperparams,
        config=config,
    ).fit(X, y, sample_weight=sample_weight)


def persist_model_artifact(
    model: TabularFoundationChallengerModel,
    *,
    symbol: str = "*",
    version: str,
) -> dict[str, Any]:
    alias = f"model:{FAMILY}:{str(model.model_name)}:{str(symbol or '*').upper()}:current"
    ref = LocalArtifactStore().put(
        model.to_bytes(),
        content_type="application/vnd.joblib",
        kind="model",
        alias=alias,
        metadata={
            "model_name": str(model.model_name),
            "family": FAMILY,
            "symbol": str(symbol or "*").upper(),
            "version": str(version),
            "backend": str(model.training_metrics.get("backend") or model.backend),
            "task": str(model.task),
            "feature_schema": dict(model.feature_schema),
            "model_card": dict(model.training_metrics.get("model_card") or {}),
            "training_window": dict(model.training_metrics.get("training_window") or {}),
        },
    )
    return {
        "alias": str(alias),
        "sha256": str(ref.sha256),
        "size_bytes": int(ref.size),
        "content_type": str(ref.content_type),
    }


def register_shadow_model(
    model: TabularFoundationChallengerModel,
    *,
    symbol: str = "*",
    version: str | None = None,
    performance_metrics: Mapping[str, Any] | None = None,
    training_window: Mapping[str, Any] | None = None,
    oos_predictions: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    version_s = str(version or version_from_ts(str(model.model_name), int(time.time() * 1000), prefix=FAMILY))
    if training_window:
        model.training_metrics["training_window"] = dict(training_window)
    manifest = persist_model_artifact(model, symbol=str(symbol), version=str(version_s))
    model_ts_ms = int(time.time() * 1000)
    oos_rows = [dict(row or {}) for row in list(oos_predictions or [])]
    oos_count = int(upsert_oos_predictions(oos_rows)) if oos_rows else 0
    promotion_requirements = {
        "source": "tabular_foundation_challenger_shadow_registration",
        "require_stat_gate": True,
        "require_cpcv": True,
        "requires_normal_evidence": True,
    }
    metrics = {
        **dict(model.training_metrics or {}),
        **dict(performance_metrics or {}),
        "model_name": str(model.model_name),
        "model_version": str(version_s),
        "model_id": f"{str(model.model_name)}:{str(version_s)}",
        "model_family": FAMILY,
        "model_kind": DEFAULT_MODEL_KIND,
        "backend": str(model.training_metrics.get("backend") or model.backend),
        "task": str(model.task),
        "feature_ids": list(model.feature_ids),
        "feature_set_tag": str(model.feature_schema.get("feature_set_tag") or ""),
        "feature_schema": dict(model.feature_schema),
        "artifact_alias": str(manifest.get("alias") or ""),
        "artifact_sha256": str(manifest.get("sha256") or ""),
        "oos_prediction_count": int(oos_count),
        "training_window": dict(training_window or model.training_metrics.get("training_window") or {}),
        "promotion_requirements": dict(promotion_requirements),
    }
    register_model(
        model_name=str(model.model_name),
        model_kind=DEFAULT_MODEL_KIND,
        model_ts_ms=int(model_ts_ms),
        stage="shadow",
        metrics=dict(metrics),
        regime="global",
    )
    register_model_version(
        model_name=str(model.model_name),
        model_version=str(version_s),
        model_kind=DEFAULT_MODEL_KIND,
        stage="shadow",
        status="trained",
        live_ready=False,
        training_job_name="train_tabular_challenger_models",
        train_scope={
            "symbol": str(symbol or "*").upper(),
            "feature_ids": list(model.feature_ids),
            "feature_schema": dict(model.feature_schema),
            "training_window": dict(training_window or {}),
            "promotion_requirements": dict(promotion_requirements),
        },
        meta=dict(metrics),
    )
    return {"version": str(version_s), "stage": "shadow", "artifact_manifest": manifest, "metrics": metrics}


def load_model_from_artifact(
    alias: str = "",
    sha256: str = "",
    path: str | Path | None = None,
) -> TabularFoundationChallengerModel:
    if path is not None and str(path).strip():
        payload = Path(path).read_bytes()
        return TabularFoundationChallengerModel.from_bytes(payload)
    payload = _artifact_payload_from_alias(str(alias or ""), str(sha256 or ""))
    if not payload:
        raise FileNotFoundError("tabular_challenger_artifact_not_found")
    return TabularFoundationChallengerModel.from_bytes(payload)


def _family_enabled(cfg: Mapping[str, Any]) -> bool:
    return bool(_env_bool("USE_TABULAR_CHALLENGERS", False) or bool(cfg.get("enabled")) or bool(cfg.get("active")))


def _resolve_challenger_training_config(plan: Mapping[str, Any] | None = None) -> dict[str, Any]:
    cfg = _resolve_training_config(FAMILY, plan)
    schema_guard = _resolve_retrain_schema_guard(
        family=FAMILY,
        model_name=str(cfg.get("model_name") or FAMILY),
        feature_ids=list(cfg.get("feature_ids") or []),
        cfg=cfg,
    )
    return {
        **cfg,
        **schema_guard,
        "backend": _normalize_backend(cfg.get("backend") or os.environ.get("TABULAR_CHALLENGER_BACKEND", DEFAULT_BACKEND)),
        "task": _normalize_task(cfg.get("task") or os.environ.get("TABULAR_CHALLENGER_TASK", DEFAULT_TASK)),
        "fallback_policy": _fallback_policy(cfg.get("fallback_policy")),
        "max_rows": _max_rows(cfg),
        "max_features": _max_features(cfg),
        "timeout_s": _timeout_s(cfg),
        "device": _device(cfg),
    }


def run_training_job() -> int:
    try:
        assert_offline_work_allowed(job_name="train_tabular_challenger_models")
    except RuntimeError as exc:
        print(f"[workload_profile] {exc}")
        return 3
    init_db()
    plan = load_lifecycle_plan(FAMILY)
    cfg = _resolve_challenger_training_config(plan)
    if not _family_enabled(cfg):
        print(f"{FAMILY}: disabled")
        return 0
    now_ms = int(time.time() * 1000)
    lookback_days = int(cfg.get("training_window_days") or DEFAULT_LOOKBACK_DAYS)
    cutoff_ms = now_ms - int(lookback_days) * 86_400_000
    feature_ids = list(cfg.get("feature_ids") or [])
    X_rows, y_rows, meta_rows = _load_training_rows(
        cutoff_ms=int(cutoff_ms),
        horizon_s=int(cfg.get("horizon_s") or DEFAULT_HORIZON_S),
        symbols=list(cfg.get("symbol_universe") or ["*"]),
        feature_ids=list(feature_ids),
        include_metadata=True,
    )
    min_samples = int(os.environ.get("TABULAR_CHALLENGER_MIN_SAMPLES", str(DEFAULT_MIN_SAMPLES)))
    if len(y_rows) < max(2, min_samples):
        print(f"{FAMILY}: insufficient_samples n={len(y_rows)} min_required={max(2, min_samples)}")
        return 0
    if len(y_rows) > int(cfg.get("max_rows") or _max_rows(cfg)):
        X_rows = list(X_rows)[-int(cfg.get("max_rows") or _max_rows(cfg)) :]
        y_rows = list(y_rows)[-int(cfg.get("max_rows") or _max_rows(cfg)) :]
        meta_rows = list(meta_rows)[-int(cfg.get("max_rows") or _max_rows(cfg)) :]
    split = min(max(1, int(len(y_rows) * 0.8)), int(len(y_rows) - 1))
    model = TabularFoundationChallengerModel(
        model_name=str(cfg.get("model_name") or FAMILY),
        feature_ids=list(feature_ids),
        backend=str(cfg.get("backend") or DEFAULT_BACKEND),
        task=str(cfg.get("task") or DEFAULT_TASK),
        hyperparams=dict(cfg.get("hyperparams") or {}),
        config=dict(cfg),
    )
    try:
        model.fit(X_rows[:split], y_rows[:split])
    except NoOpTraining as exc:
        print(f"{FAMILY}: no_op reason={str(exc)}")
        return 0
    pred_eval = model.predict(X_rows[split:])
    oos_run_id = str(uuid.uuid4())
    horizon_s = int(cfg.get("horizon_s") or DEFAULT_HORIZON_S)
    oos_rows = [
        {
            "symbol": str(meta_rows[split + idx].get("symbol") or "*"),
            "horizon": int(horizon_s),
            "family": FAMILY,
            "ts": int(meta_rows[split + idx].get("ts") or 0),
            "run_id": str(oos_run_id),
            "prediction": float(_safe_float(np.asarray(pred_eval[idx]).reshape(-1)[0], 0.0)),
            "target": float(_safe_float(y_rows[split + idx], 0.0)),
        }
        for idx in range(int(len(y_rows) - split))
    ]
    training_window = {
        "start_ts_ms": int(cutoff_ms),
        "end_ts_ms": int(now_ms),
        "lookback_days": int(lookback_days),
        "train_rows": int(split),
        "oos_rows": int(len(oos_rows)),
        "horizon_s": int(horizon_s),
    }
    version = str(
        plan.get("model_version")
        or cfg.get("training_version_id")
        or version_from_ts(str(model.model_name), int(now_ms), prefix=FAMILY)
    )
    result = register_shadow_model(
        model,
        symbol="*",
        version=str(version),
        training_window=training_window,
        oos_predictions=oos_rows,
    )
    metrics = dict(result.get("metrics") or {})
    calibration = dict(metrics.get("calibration_diagnostics") or {})
    record_version_performance(
        model_name=str(model.model_name),
        model_version=str(version),
        metric_scope="training",
        metrics={
            "avg_rmse": float(calibration.get("rmse") or metrics.get("rmse") or 0.0),
            "avg_directional_acc": float(calibration.get("directional_accuracy") or 0.0),
            "quality_score": float(max(0.0, min(1.0, _safe_float(calibration.get("directional_accuracy"), 0.0)))),
            "trained_models": 1,
        },
        sample_n=int(len(y_rows)),
        meta={"job_name": "train_tabular_challenger_models"},
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


def benchmark_against_sklearn_gbm_baseline(
    X: Any,
    y: Any,
    *,
    feature_ids: Sequence[Any],
    task: str = "regression",
    challenger_backend: str = "fake",
) -> dict[str, Any]:
    task_s = _normalize_task(task)
    model = train_tabular_challenger(
        X,
        y,
        feature_ids=list(feature_ids),
        task=task_s,
        backend=challenger_backend,
        model_name=f"{FAMILY}.benchmark",
        config={"fallback_policy": "fail_closed"},
    )
    columns = _expected_columns(feature_ids, model_name=model.model_name, model_spec=model.feature_schema)
    X_arr = _matrix_from_features(X, columns, feature_schema=model.feature_schema, phase="serve", model_name=model.model_name)
    y_arr = np.asarray(y).reshape(-1)
    if task_s == "classification":
        from sklearn.ensemble import GradientBoostingClassifier

        baseline = GradientBoostingClassifier(random_state=17)
    else:
        from sklearn.ensemble import GradientBoostingRegressor

        baseline = GradientBoostingRegressor(random_state=17)
    baseline.fit(X_arr, y_arr)
    challenger_pred = model.predict(X)
    baseline_pred = baseline.predict(X_arr)
    return {
        "challenger": calibration_diagnostics(predictions=challenger_pred, targets=y_arr, task=task_s),
        "sklearn_gbm_baseline": calibration_diagnostics(predictions=baseline_pred, targets=y_arr, task=task_s),
        "feature_schema": dict(model.feature_schema),
        "primary_baseline": "sklearn_gbm",
    }


def main() -> int:
    return run_training_job()


def _register_family() -> None:
    try:
        register_model_family(
            FAMILY,
            training_entrypoint="engine.strategy.jobs.train_tabular_challenger_models",
            inference_entrypoint="engine.strategy.models.tabular_challenger.TabularFoundationChallengerModel",
            default_stage="shadow",
            promotion_guard="engine.strategy.promotion_guard.assess_challenger",
            metadata={
                "shadow_only_default": True,
                "optional_backends": ["fake", "tabpfn", "tabm"],
                "primary_baselines": ["lightgbm", "xgboost"],
            },
        )
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)


_register_family()


__all__ = [
    "FAMILY",
    "DeterministicFakeTabularEstimator",
    "LicenseReviewRequired",
    "NoOpTraining",
    "OptionalDependencyMissing",
    "TabularFoundationChallengerModel",
    "benchmark_against_sklearn_gbm_baseline",
    "calibration_diagnostics",
    "load_model_from_artifact",
    "main",
    "persist_model_artifact",
    "register_shadow_model",
    "run_training_job",
    "train_tabular_challenger",
]


if __name__ == "__main__":
    raise SystemExit(main())
