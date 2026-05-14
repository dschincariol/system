"""Opt-in Optuna tuning jobs that score candidates with CPCV/PBO."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional

import numpy as np

from engine.data.universe_pit import resolve_training_window_universe
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db, record_model_hyperparameter_registry
from engine.strategy import gbm_regressor as gbm
from engine.strategy.cpcv import cpcv_backtest, cpcv_config_from_env


LOG = get_logger("engine.strategy.optuna_tuner")
_WARNED_NONFATAL_KEYS: set[str] = set()
_TUNER_NAME = "optuna_cpcv"
_DEFAULT_TRIALS = int(os.environ.get("GBM_OPTUNA_N_TRIALS", "20"))
_DEFAULT_TIMEOUT_S = int(os.environ.get("GBM_OPTUNA_TIMEOUT_S", "0"))
_DEFAULT_PBO_PENALTY = float(os.environ.get("GBM_OPTUNA_PBO_PENALTY", "0.50"))
_DEFAULT_MIN_SAMPLES = int(os.environ.get("GBM_OPTUNA_MIN_SAMPLES", str(gbm._DEFAULT_MIN_SAMPLES)))
_DEFAULT_SEED = int(os.environ.get("GBM_OPTUNA_SEED", "42"))


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_optuna_tuner_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.optuna_tuner",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _import_optuna():
    try:
        import optuna
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("optuna_not_installed") from exc
    return optuna


def _study_name_for_model(model_name: str) -> str:
    explicit = str(os.environ.get("GBM_OPTUNA_STUDY_NAME", "") or "").strip()
    if explicit:
        return explicit
    return f"{_TUNER_NAME}:{str(model_name or '').strip() or gbm._DEFAULT_MODEL_NAME}"


def _load_gbm_training_dataset(model_name: str) -> Dict[str, Any]:
    train_cfg = gbm._resolve_training_config({"model_name": str(model_name or "").strip()})
    feature_ids = list(train_cfg.get("feature_ids") or gbm._DEFAULT_FEATURE_IDS)
    horizon_s = int(train_cfg.get("horizon_s") or gbm._DEFAULT_HORIZON_S)
    lookback_days = int(train_cfg.get("training_window_days") or gbm._DEFAULT_LOOKBACK_DAYS)
    cutoff_ms = int(time.time() * 1000) - (int(lookback_days) * 24 * 60 * 60 * 1000)
    pit_universe = {
        "pit_enabled": False,
        "pit_applied": False,
        "symbols": list(train_cfg.get("symbol_universe") or gbm._DEFAULT_SYMBOLS),
        "fallback_reason": "not_resolved",
    }

    con = connect()
    try:
        pit_universe = resolve_training_window_universe(
            con,
            configured_symbols=list(train_cfg.get("symbol_universe") or gbm._DEFAULT_SYMBOLS),
            lookback_days=int(lookback_days),
        )
        if list(pit_universe.get("symbols") or []):
            train_cfg["symbol_universe"] = list(pit_universe.get("symbols") or [])
        runtime_symbols = gbm._normalize_symbol_universe(train_cfg.get("symbol_universe") or gbm._DEFAULT_SYMBOLS)
        rows = gbm._load_training_rows(
            con,
            cutoff_ms=int(cutoff_ms),
            symbol_filter=gbm._runtime_symbol_filter(runtime_symbols),
            horizon_s=int(horizon_s),
            feature_ids=list(feature_ids),
        )
    finally:
        con.close()

    if int(len(rows)) < int(max(2, _DEFAULT_MIN_SAMPLES)):
        return {
            "ok": False,
            "status": "insufficient_samples",
            "row_count": int(len(rows)),
            "min_samples": int(max(2, _DEFAULT_MIN_SAMPLES)),
            "train_cfg": dict(train_cfg),
            "runtime_symbols": list(runtime_symbols),
            "pit_universe": dict(pit_universe or {}),
        }

    X = np.stack([row[0] for row in rows]).astype(np.float32, copy=False)
    y = np.asarray([row[1] for row in rows], dtype=np.float32)
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)
    y = np.nan_to_num(y, nan=0.0, posinf=0.0, neginf=0.0)
    return {
        "ok": True,
        "status": "loaded",
        "row_count": int(len(rows)),
        "train_cfg": dict(train_cfg),
        "runtime_symbols": list(runtime_symbols),
        "feature_ids": list(feature_ids),
        "horizon_s": int(horizon_s),
        "lookback_days": int(lookback_days),
        "pit_universe": dict(pit_universe or {}),
        "X": X,
        "y": y,
    }


def _make_lgbm_factory(hyperparams: Dict[str, Any]):
    lgb = gbm._import_lightgbm()
    normalized = gbm._normalized_hyperparams(dict(hyperparams or {}))
    return lambda: lgb.LGBMRegressor(**dict(normalized))


def _suggest_gbm_hyperparams(trial: Any, base_hyperparams: Dict[str, Any]) -> Dict[str, Any]:
    return {
        **dict(base_hyperparams or {}),
        "num_leaves": int(trial.suggest_int("num_leaves", 8, 128, log=True)),
        "learning_rate": float(trial.suggest_float("learning_rate", 0.01, 0.20, log=True)),
        "n_estimators": int(trial.suggest_int("n_estimators", 50, 400, step=10)),
        "min_child_samples": int(trial.suggest_int("min_child_samples", 5, 100, log=True)),
    }


def _objective_value(cpcv_result: Dict[str, Any], *, pbo_penalty: float) -> float:
    if not bool((cpcv_result or {}).get("ok")) or int((cpcv_result or {}).get("n_paths") or 0) <= 0:
        return -1e9
    median_sharpe = float((cpcv_result or {}).get("median_sharpe") or 0.0)
    pbo = float((cpcv_result or {}).get("pbo") or 1.0)
    return float(median_sharpe - (float(pbo_penalty) * float(pbo)))


def run_gbm_optuna_tuning_job(
    *,
    model_name: str = "",
    n_trials: Optional[int] = None,
    timeout_s: Optional[int] = None,
) -> Dict[str, Any]:
    """Run Optuna tuning for the GBM family using CPCV/PBO objective scoring."""
    init_db()
    resolved_model_name = str(model_name or os.environ.get("GBM_OPTUNA_MODEL_NAME", gbm._DEFAULT_MODEL_NAME)).strip()
    if not resolved_model_name:
        resolved_model_name = gbm._DEFAULT_MODEL_NAME

    dataset = _load_gbm_training_dataset(resolved_model_name)
    if not bool(dataset.get("ok")):
        return {
            "ok": False,
            "status": str(dataset.get("status") or "dataset_unavailable"),
            "model_name": str(resolved_model_name),
            "diagnostics": {
                "row_count": int(dataset.get("row_count") or 0),
                "min_samples": int(dataset.get("min_samples") or 0),
            },
        }

    optuna = _import_optuna()
    cpcv_cfg = cpcv_config_from_env()
    resolved_trials = int(max(1, int(n_trials if n_trials is not None else _DEFAULT_TRIALS)))
    resolved_timeout_s = int(max(0, int(timeout_s if timeout_s is not None else _DEFAULT_TIMEOUT_S)))
    pbo_penalty = float(max(0.0, _DEFAULT_PBO_PENALTY))
    train_cfg = dict(dataset.get("train_cfg") or {})
    base_hyperparams = dict(train_cfg.get("hyperparams") or {})
    feature_ids = list(dataset.get("feature_ids") or [])
    horizon_s = int(dataset.get("horizon_s") or gbm._DEFAULT_HORIZON_S)
    X = np.asarray(dataset.get("X"), dtype=np.float32)
    y = np.asarray(dataset.get("y"), dtype=np.float32)

    sampler = None
    try:
        sampler_cls = getattr(getattr(optuna, "samplers", None), "TPESampler", None)
        if sampler_cls is not None:
            sampler = sampler_cls(seed=int(_DEFAULT_SEED))
    except Exception as exc:
        _warn_nonfatal("OPTUNA_SAMPLER_SETUP_FAILED", exc, once_key="optuna_sampler_setup")
        sampler = None

    study_kwargs: Dict[str, Any] = {
        "direction": "maximize",
        "study_name": _study_name_for_model(resolved_model_name),
    }
    if sampler is not None:
        study_kwargs["sampler"] = sampler
    study = optuna.create_study(**study_kwargs)

    def objective(trial: Any) -> float:
        hyperparams = gbm._normalized_hyperparams(_suggest_gbm_hyperparams(trial, base_hyperparams))
        cpcv_result = cpcv_backtest(
            X,
            y,
            model_factory=_make_lgbm_factory(hyperparams),
            n_splits=int(cpcv_cfg.get("n_splits") or 6),
            n_test_splits=int(cpcv_cfg.get("n_test_splits") or 2),
            embargo_pct=float(cpcv_cfg.get("embargo_pct") or 0.01),
            label_horizon=int(max(0, int(cpcv_cfg.get("label_horizon") or horizon_s))),
        )
        score = _objective_value(cpcv_result, pbo_penalty=float(pbo_penalty))
        try:
            trial.set_user_attr("hyperparams", dict(hyperparams))
            trial.set_user_attr(
                "cpcv_result",
                {
                    "ok": bool(cpcv_result.get("ok")),
                    "status": str(cpcv_result.get("status") or ""),
                    "n_paths": int(cpcv_result.get("n_paths") or 0),
                    "mean_sharpe": float(cpcv_result.get("mean_sharpe") or 0.0),
                    "median_sharpe": float(cpcv_result.get("median_sharpe") or 0.0),
                    "pbo": float(cpcv_result.get("pbo") or 1.0),
                },
            )
        except Exception as exc:
            _warn_nonfatal("OPTUNA_TRIAL_ATTR_WRITE_FAILED", exc, once_key="optuna_trial_attr_write")
        return float(score)

    optimize_kwargs: Dict[str, Any] = {"n_trials": int(resolved_trials)}
    if int(resolved_timeout_s) > 0:
        optimize_kwargs["timeout"] = int(resolved_timeout_s)
    study.optimize(objective, **optimize_kwargs)

    best_trial = getattr(study, "best_trial", None)
    if best_trial is None:
        return {
            "ok": False,
            "status": "no_trials_completed",
            "model_name": str(resolved_model_name),
            "diagnostics": {"trial_count": 0},
        }

    best_params = gbm._normalized_hyperparams(dict(getattr(best_trial, "user_attrs", {}).get("hyperparams") or {}))
    best_cpcv = dict(getattr(best_trial, "user_attrs", {}).get("cpcv_result") or {})
    diagnostics = {
        "dataset": {
            "row_count": int(dataset.get("row_count") or 0),
            "lookback_days": int(dataset.get("lookback_days") or 0),
            "runtime_symbols": list(dataset.get("runtime_symbols") or []),
            "feature_ids": list(feature_ids),
            "horizon_s": int(horizon_s),
        },
        "base_hyperparams": dict(base_hyperparams),
        "best_cpcv": dict(best_cpcv),
        "cpcv_config": {
            "n_splits": int(cpcv_cfg.get("n_splits") or 0),
            "n_test_splits": int(cpcv_cfg.get("n_test_splits") or 0),
            "embargo_pct": float(cpcv_cfg.get("embargo_pct") or 0.0),
            "label_horizon": int(max(0, int(cpcv_cfg.get("label_horizon") or horizon_s))),
            "pbo_penalty": float(pbo_penalty),
        },
    }
    registry_id = record_model_hyperparameter_registry(
        model_name=str(resolved_model_name),
        model_family=str(train_cfg.get("family") or gbm._GBM_FAMILY),
        tuner=_TUNER_NAME,
        objective="median_sharpe_minus_pbo_penalty",
        metric_value=float(getattr(best_trial, "value", 0.0) or 0.0),
        params=dict(best_params),
        study_name=str(getattr(study, "study_name", "") or _study_name_for_model(resolved_model_name)),
        trial_count=int(len(list(getattr(study, "trials", []) or []))),
        best_trial_number=int(getattr(best_trial, "number", 0) or 0),
        cpcv_mean_sharpe=float(best_cpcv.get("mean_sharpe") or 0.0),
        cpcv_median_sharpe=float(best_cpcv.get("median_sharpe") or 0.0),
        cpcv_pbo=float(best_cpcv.get("pbo") or 1.0),
        diagnostics=diagnostics,
    )

    return {
        "ok": True,
        "status": "completed",
        "model_name": str(resolved_model_name),
        "model_family": str(train_cfg.get("family") or gbm._GBM_FAMILY),
        "study_name": str(getattr(study, "study_name", "") or _study_name_for_model(resolved_model_name)),
        "registry_id": int(registry_id or 0),
        "trial_count": int(len(list(getattr(study, "trials", []) or []))),
        "best_trial_number": int(getattr(best_trial, "number", 0) or 0),
        "objective_value": float(getattr(best_trial, "value", 0.0) or 0.0),
        "best_params": dict(best_params),
        "cpcv": dict(best_cpcv),
        "diagnostics": diagnostics,
    }


def main() -> int:
    """CLI entrypoint for the GBM Optuna tuning job."""
    result = run_gbm_optuna_tuning_job()
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0 if bool(result.get("ok")) else 1


__all__ = [
    "main",
    "run_gbm_optuna_tuning_job",
]
