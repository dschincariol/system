"""First-class XGBoost tabular regressor family."""

from __future__ import annotations
import logging

from pathlib import Path
from typing import Any, Mapping, Sequence

import joblib
import numpy as np

from engine.model_registry import register_model_family
from engine.runtime.workload_profiles import model_family_n_jobs
from engine.strategy.models.lgbm_regressor import (
    LGBMRegressorModel,
    _assert_loaded_feature_schema_current,
    _artifact_payload_from_alias,
    _expected_columns,
    _feature_schema,
    _fit_eval_metrics,
    _load_joblib_from_bytes,
    _matrix_from_features,
    persist_model_artifact as _persist_tabular_model_artifact,
    register_shadow_model as _register_tabular_shadow_model,
    run_tabular_training_job,
)
from engine.strategy.ood import build_ood_profile, summarize_ood_profile

FAMILY = "xgb_regressor"
DEFAULT_MODEL_NAME = FAMILY
DEFAULT_MODEL_KIND = "xgboost"


def _register_family() -> None:
    try:
        register_model_family(
            FAMILY,
            training_entrypoint="engine.strategy.jobs.train_xgb_models",
            inference_entrypoint="engine.strategy.models.xgb_regressor.XGBRegressorModel",
            default_stage="shadow",
            promotion_guard="engine.strategy.promotion_guard.assess_challenger",
        )
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


_register_family()


class XGBRegressorModel(LGBMRegressorModel):
    """XGBoost regressor with the same schema-bound surface as LightGBM."""

    family = FAMILY
    model_kind = DEFAULT_MODEL_KIND

    def __init__(
        self,
        *,
        model_name: str = DEFAULT_MODEL_NAME,
        feature_ids: Sequence[Any] | None = None,
        hyperparams: Mapping[str, Any] | None = None,
        model: Any = None,
        training_metrics: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(
            model_name=str(model_name or DEFAULT_MODEL_NAME),
            feature_ids=feature_ids,
            hyperparams=hyperparams,
            model=model,
            training_metrics=training_metrics,
        )

    @staticmethod
    def _default_hyperparams() -> dict[str, Any]:
        return {
            "objective": "reg:squarederror",
            "max_depth": 3,
            "learning_rate": 0.05,
            "n_estimators": 100,
            "subsample": 1.0,
            "colsample_bytree": 1.0,
            "random_state": 42,
            "n_jobs": model_family_n_jobs("XGB_N_JOBS"),
            "verbosity": 0,
        }

    @property
    def feature_schema(self) -> dict[str, Any]:
        return _feature_schema(self.feature_ids, preprocessing=getattr(self, "feature_preprocessing", {}))

    def _new_estimator(self) -> Any:
        try:
            import xgboost as xgb

            return xgb.XGBRegressor(**dict(self.hyperparams))
        except ImportError as exc:
            raise RuntimeError("xgboost is required for the xgb_regressor family") from exc

    def fit(self, X: Any, y: Any, sample_weight: Any = None) -> "XGBRegressorModel":
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_arr, preprocessing, _accounting = _matrix_from_features(
            X,
            columns,
            phase="train",
            model_name=self.model_name,
            fit_preprocessing=True,
            return_metadata=True,
        )
        y_arr = np.asarray(y, dtype=np.float32).reshape(-1)
        if int(X_arr.shape[0]) != int(y_arr.shape[0]):
            raise ValueError("xgb_row_count_mismatch")
        self.feature_ids = list(columns)
        self.feature_preprocessing = dict(preprocessing or {})
        model = self._new_estimator()
        fit_kwargs: dict[str, Any] = {}
        if sample_weight is not None:
            fit_kwargs["sample_weight"] = np.asarray(sample_weight, dtype=np.float32).reshape(-1)
        model.fit(X_arr, y_arr, **fit_kwargs)
        self.model = model
        self.ood_profile = build_ood_profile(X_arr, columns)
        self.training_metrics = {
            "n_train": int(y_arr.shape[0]),
            "model_family": FAMILY,
            "model_kind": DEFAULT_MODEL_KIND,
            "backend": "xgboost",
            "feature_schema": self.feature_schema,
            "ood_profile_summary": summarize_ood_profile(self.ood_profile),
            **_fit_eval_metrics(model, X_arr, y_arr),
        }
        self.persisted_feature_schema = dict(self.feature_schema)
        return self

    @classmethod
    def load(cls, path: str | Path) -> "XGBRegressorModel":
        loaded = joblib.load(Path(path))
        if not isinstance(loaded, cls):
            raise TypeError("invalid_xgb_regressor_artifact")
        _assert_loaded_feature_schema_current(loaded)
        return loaded

    @classmethod
    def from_bytes(cls, payload: bytes) -> "XGBRegressorModel":
        loaded = _load_joblib_from_bytes(payload)
        if not isinstance(loaded, cls):
            raise TypeError("invalid_xgb_regressor_payload")
        _assert_loaded_feature_schema_current(loaded)
        return loaded


def train_xgb_regressor(
    X: Any,
    y: Any,
    *,
    feature_ids: Sequence[Any] | None = None,
    sample_weight: Any = None,
    hyperparams: Mapping[str, Any] | None = None,
    model_name: str = DEFAULT_MODEL_NAME,
) -> XGBRegressorModel:
    return XGBRegressorModel(
        model_name=str(model_name or DEFAULT_MODEL_NAME),
        feature_ids=feature_ids,
        hyperparams=hyperparams,
    ).fit(X, y, sample_weight=sample_weight)


def load_model_from_artifact(alias: str = "", sha256: str = "", path: str | Path | None = None) -> XGBRegressorModel:
    if path is not None and str(path).strip():
        return XGBRegressorModel.load(Path(path))
    payload = _artifact_payload_from_alias(str(alias or ""), str(sha256 or ""))
    if not payload:
        raise FileNotFoundError("xgb_artifact_not_found")
    return XGBRegressorModel.from_bytes(payload)


def persist_model_artifact(
    model: XGBRegressorModel,
    *,
    symbol: str = "*",
    version: str,
) -> dict[str, Any]:
    return _persist_tabular_model_artifact(
        model,
        family=FAMILY,
        symbol=str(symbol),
        version=str(version),
    )


def register_shadow_model(
    model: XGBRegressorModel,
    *,
    symbol: str = "*",
    version: str | None = None,
    performance_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return _register_tabular_shadow_model(
        model,
        symbol=str(symbol),
        version=version,
        family=FAMILY,
        model_kind=DEFAULT_MODEL_KIND,
        performance_metrics=performance_metrics,
    )


def main() -> int:
    return run_tabular_training_job(
        family=FAMILY,
        model_cls=XGBRegressorModel,
        model_kind=DEFAULT_MODEL_KIND,
        version_prefix="xgb",
    )


__all__ = [
    "FAMILY",
    "XGBRegressorModel",
    "load_model_from_artifact",
    "main",
    "persist_model_artifact",
    "register_shadow_model",
    "train_xgb_regressor",
]


if __name__ == "__main__":
    raise SystemExit(main())
