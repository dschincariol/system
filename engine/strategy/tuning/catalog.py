"""Declarative hyperparameter catalog used by Optuna tuning jobs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Literal

ParamType = Literal["int", "float", "categorical"]


@dataclass(frozen=True)
class Hyperparam:
    name: str
    model_family: str
    dtype: ParamType
    default: Any
    low: float | int | None = None
    high: float | int | None = None
    log: bool = False
    choices: tuple[Any, ...] = ()
    env_name: str | None = None

    def suggest(self, trial) -> Any:
        if self.dtype == "int":
            if self.low is None or self.high is None:
                raise ValueError(f"int hyperparameter missing range: {self.name}")
            return trial.suggest_int(self.name, int(self.low), int(self.high), log=bool(self.log))
        if self.dtype == "float":
            if self.low is None or self.high is None:
                raise ValueError(f"float hyperparameter missing range: {self.name}")
            return trial.suggest_float(self.name, float(self.low), float(self.high), log=bool(self.log))
        if self.dtype == "categorical":
            if not self.choices:
                raise ValueError(f"categorical hyperparameter missing choices: {self.name}")
            return trial.suggest_categorical(self.name, list(self.choices))
        raise ValueError(f"unsupported hyperparameter dtype: {self.dtype}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "model_family": self.model_family,
            "dtype": self.dtype,
            "default": self.default,
            "low": self.low,
            "high": self.high,
            "log": self.log,
            "choices": list(self.choices),
            "env_name": self.env_name,
        }


CATALOG: tuple[Hyperparam, ...] = (
    Hyperparam("seq_len", "temporal_predictor", "int", 6, low=3, high=32, env_name="TEMPORAL_SEQ_LEN"),
    Hyperparam("conf_k", "temporal_predictor", "float", 75.0, low=10.0, high=250.0, log=True, env_name="TEMPORAL_CONF_K"),
    Hyperparam("hidden_width", "temporal_predictor", "int", 128, low=32, high=512, log=True, env_name="TEMPORAL_HIDDEN_WIDTH"),
    Hyperparam("lr", "temporal_predictor", "float", 0.003, low=1e-4, high=1e-2, log=True, env_name="TEMPORAL_LR"),
    Hyperparam("epochs", "temporal_predictor", "int", 120, low=20, high=240, env_name="TEMPORAL_EPOCHS"),
    Hyperparam("train_split", "embed_regressor", "float", 0.8, low=0.5, high=0.95, env_name="EMBED_TRAIN_SPLIT"),
    Hyperparam("conf_k", "embed_regressor", "float", 75.0, low=10.0, high=250.0, log=True, env_name="EMBED_REGRESSOR_CONF_K"),
    Hyperparam("ridge_alpha", "embed_regressor", "float", 1.0, low=1e-4, high=100.0, log=True, env_name="EMBED_RIDGE_ALPHA"),
    Hyperparam("num_leaves", "lgbm_regressor", "int", 31, low=8, high=256, log=True, env_name="LGBM_NUM_LEAVES"),
    Hyperparam("learning_rate", "lgbm_regressor", "float", 0.05, low=1e-3, high=0.2, log=True, env_name="LGBM_LEARNING_RATE"),
    Hyperparam("n_estimators", "lgbm_regressor", "int", 300, low=50, high=1200, log=True, env_name="LGBM_N_ESTIMATORS"),
    Hyperparam("max_depth", "xgb_regressor", "int", 4, low=2, high=10, env_name="XGB_MAX_DEPTH"),
    Hyperparam("learning_rate", "xgb_regressor", "float", 0.05, low=1e-3, high=0.2, log=True, env_name="XGB_LEARNING_RATE"),
    Hyperparam("n_estimators", "xgb_regressor", "int", 300, low=50, high=1200, log=True, env_name="XGB_N_ESTIMATORS"),
    Hyperparam("seq_len", "patchtst", "int", 128, low=32, high=256, log=True, env_name="PATCHTST_SEQ_LEN"),
    Hyperparam("patch_len", "patchtst", "int", 16, low=4, high=32, log=True, env_name="PATCHTST_PATCH_LEN"),
    Hyperparam("d_model", "patchtst", "int", 64, low=16, high=256, log=True, env_name="PATCHTST_D_MODEL"),
)


def all_hyperparams() -> tuple[Hyperparam, ...]:
    return tuple(CATALOG)


def catalog_for_family(model_family: str) -> tuple[Hyperparam, ...]:
    family = str(model_family or "").strip()
    return tuple(param for param in CATALOG if param.model_family == family)


def catalog_defaults(model_family: str) -> dict[str, Any]:
    return {param.name: param.default for param in catalog_for_family(model_family)}


def managed_env_names(model_family: str | None = None) -> set[str]:
    params: Iterable[Hyperparam] = CATALOG if model_family is None else catalog_for_family(model_family)
    return {str(param.env_name) for param in params if param.env_name}


def default_for(model_family: str, name: str, fallback: Any = None) -> Any:
    family = str(model_family or "").strip()
    key = str(name or "").strip()
    for param in catalog_for_family(family):
        if param.name == key or param.env_name == key:
            return param.default
    return fallback


def suggest_params(trial, model_family: str) -> dict[str, Any]:
    return {param.name: param.suggest(trial) for param in catalog_for_family(model_family)}
