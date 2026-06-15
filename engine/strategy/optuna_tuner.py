"""Opt-in Optuna tuning jobs that score candidates with CPCV/PBO."""

from __future__ import annotations

import json
import logging
import math
import os
import time
from typing import Any, Callable, Dict, Optional

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
_GBM_TUNED_PARAM_SPACE: Dict[str, Dict[str, Any]] = {
    "num_leaves": {"dtype": "int", "low": 8, "high": 128},
    "learning_rate": {"dtype": "float", "low": 0.01, "high": 0.20},
    "n_estimators": {"dtype": "int", "low": 50, "high": 400, "step": 10},
    "min_child_samples": {"dtype": "int", "low": 5, "high": 100},
}


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


def _gbm_objective_for_hyperparams(
    hyperparams: Dict[str, Any],
    *,
    X: np.ndarray,
    y: np.ndarray,
    cpcv_cfg: Dict[str, Any],
    horizon_s: int,
    pbo_penalty: float,
) -> tuple[float, Dict[str, Any]]:
    cpcv_result = cpcv_backtest(
        X,
        y,
        model_factory=_make_lgbm_factory(hyperparams),
        n_splits=int(cpcv_cfg.get("n_splits") or 6),
        n_test_splits=int(cpcv_cfg.get("n_test_splits") or 2),
        embargo_pct=float(cpcv_cfg.get("embargo_pct") or 0.01),
        label_horizon=int(max(0, int(cpcv_cfg.get("label_horizon") or horizon_s))),
    )
    return float(_objective_value(cpcv_result, pbo_penalty=float(pbo_penalty))), dict(cpcv_result or {})


def _trial_hyperparams(trial: Any) -> Dict[str, Any]:
    attrs = dict(getattr(trial, "user_attrs", {}) or {})
    params = attrs.get("hyperparams")
    if not isinstance(params, dict):
        params = getattr(trial, "params", {})
    return gbm._normalized_hyperparams(dict(params or {}))


def _trial_cpcv_result(trial: Any) -> Dict[str, Any]:
    attrs = dict(getattr(trial, "user_attrs", {}) or {})
    cpcv_result = attrs.get("cpcv_result")
    return dict(cpcv_result or {}) if isinstance(cpcv_result, dict) else {}


def _trial_value(trial: Any, *, pbo_penalty: float) -> float:
    try:
        value = float(getattr(trial, "value", 0.0) or 0.0)
    except Exception:
        value = float("nan")
    if math.isfinite(value):
        return float(value)
    return float(_objective_value(_trial_cpcv_result(trial), pbo_penalty=float(pbo_penalty)))


def _neighbor_step(name: str, value: Any, spec: Dict[str, Any]) -> float:
    if spec.get("step") not in (None, ""):
        try:
            return abs(float(spec.get("step")))
        except Exception as e:
            _warn_nonfatal(
                "OPTUNA_TUNER_INVALID_NEIGHBOR_STEP",
                e,
                once_key=f"neighbor_step:{name}",
                param=str(name),
                step=repr(spec.get("step"))[:120],
            )
    try:
        value_f = abs(float(value))
    except Exception:
        value_f = 0.0
    if value_f > 0.0:
        return max(1.0 if spec.get("dtype") == "int" else 1e-12, value_f * 0.10)
    low = spec.get("low")
    high = spec.get("high")
    try:
        span = abs(float(high) - float(low))
    except Exception:
        span = 0.0
    if span > 0.0:
        return max(1.0 if spec.get("dtype") == "int" else 1e-12, span * 0.10)
    return 1.0


def _neighbor_param_sets(params: Dict[str, Any]) -> list[Dict[str, Any]]:
    out: list[Dict[str, Any]] = []
    base = dict(params or {})
    seen: set[str] = set()
    for name, spec in _GBM_TUNED_PARAM_SPACE.items():
        if name not in base:
            continue
        current = base.get(name)
        step = _neighbor_step(name, current, spec)
        for direction in (-1.0, 1.0):
            try:
                value = float(current) + (float(direction) * float(step))
                low = spec.get("low")
                high = spec.get("high")
                if low is not None:
                    value = max(float(low), value)
                if high is not None:
                    value = min(float(high), value)
                if spec.get("dtype") == "int":
                    value_out: Any = int(round(value))
                else:
                    value_out = float(value)
            except Exception:
                continue
            if str(value_out) == str(current):
                continue
            neighbor = dict(base)
            neighbor[name] = value_out
            normalized = gbm._normalized_hyperparams(neighbor)
            key = json.dumps(normalized, separators=(",", ":"), sort_keys=True, default=str)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "param": str(name),
                    "direction": int(direction),
                    "step": float(step),
                    "value": value_out,
                    "params": normalized,
                }
            )
    return out


def _median(values: list[float]) -> float:
    arr = sorted(float(value) for value in list(values or []) if math.isfinite(float(value)))
    if not arr:
        return 0.0
    mid = len(arr) // 2
    if len(arr) % 2:
        return float(arr[mid])
    return float((arr[mid - 1] + arr[mid]) / 2.0)


def evaluate_parameter_surface_robustness(
    *,
    trials: list[Any],
    best_trial: Any,
    evaluate_params: Callable[[Dict[str, Any]], tuple[float, Dict[str, Any]]],
    pbo_penalty: float,
    max_neighbor_decay: float | None = None,
) -> Dict[str, Any]:
    """Evaluate ±1-step neighborhoods for top-decile Optuna trials."""
    threshold = float(
        max_neighbor_decay
        if max_neighbor_decay is not None
        else max(0.0, float(os.environ.get("HPO_MAX_NEIGHBOR_DECAY", "0.30") or 0.30))
    )
    completed = [trial for trial in list(trials or []) if math.isfinite(_trial_value(trial, pbo_penalty=float(pbo_penalty)))]
    completed.sort(key=lambda trial: _trial_value(trial, pbo_penalty=float(pbo_penalty)), reverse=True)
    top_count = max(1, int(math.ceil(float(len(completed) or 1) * 0.10)))
    if len(completed) >= 2:
        top_count = max(2, top_count)
    top_count = min(len(completed), top_count)
    top_trials = completed[:top_count]
    best_number = int(getattr(best_trial, "number", 0) or 0)
    if best_trial is not None and all(int(getattr(trial, "number", -1) or -1) != best_number for trial in top_trials):
        top_trials.insert(0, best_trial)

    candidate_rows: list[Dict[str, Any]] = []
    for trial in top_trials:
        params = _trial_hyperparams(trial)
        base_cpcv = _trial_cpcv_result(trial)
        base_value = _trial_value(trial, pbo_penalty=float(pbo_penalty))
        base_median = float(base_cpcv.get("median_sharpe") or base_value)
        neighbor_rows: list[Dict[str, Any]] = []
        for neighbor in _neighbor_param_sets(params):
            score, cpcv_result = evaluate_params(dict(neighbor.get("params") or {}))
            median_sharpe = float((cpcv_result or {}).get("median_sharpe") or score)
            neighbor_rows.append(
                {
                    "param": str(neighbor.get("param") or ""),
                    "direction": int(neighbor.get("direction") or 0),
                    "step": float(neighbor.get("step") or 0.0),
                    "value": neighbor.get("value"),
                    "objective_value": float(score),
                    "median_sharpe": float(median_sharpe),
                }
            )
        neighbor_medians = [float(row.get("median_sharpe") or 0.0) for row in neighbor_rows]
        median_neighbor = _median(neighbor_medians)
        denom = max(abs(float(base_median)), 1e-12)
        decay = max(0.0, (float(base_median) - float(median_neighbor)) / float(denom))
        overfit = bool(neighbor_rows and float(decay) > float(threshold))
        candidate_rows.append(
            {
                "trial_number": int(getattr(trial, "number", 0) or 0),
                "objective_value": float(base_value),
                "base_median_sharpe": float(base_median),
                "median_neighbor_sharpe": float(median_neighbor),
                "neighbor_decay": float(decay),
                "max_neighbor_decay": float(threshold),
                "overfit": bool(overfit),
                "params": dict(params),
                "cpcv_result": dict(base_cpcv),
                "neighbors": neighbor_rows,
            }
        )

    if not candidate_rows:
        params = _trial_hyperparams(best_trial)
        return {
            "applied": False,
            "status": "no_completed_trials",
            "passed": True,
            "fallback_applied": False,
            "selected_trial_number": int(getattr(best_trial, "number", 0) or 0),
            "selected_params": dict(params),
            "selected_cpcv": _trial_cpcv_result(best_trial),
            "selected_objective_value": _trial_value(best_trial, pbo_penalty=float(pbo_penalty)),
            "top_decile": [],
        }

    original = next(
        (row for row in candidate_rows if int(row.get("trial_number") or 0) == best_number),
        candidate_rows[0],
    )
    fallback_applied = bool(original.get("overfit"))
    selected = original
    if fallback_applied:
        selected = sorted(
            candidate_rows,
            key=lambda row: (
                float(row.get("neighbor_decay") or 0.0),
                -float(row.get("median_neighbor_sharpe") or 0.0),
                -float(row.get("objective_value") or 0.0),
            ),
        )[0]
    return {
        "applied": True,
        "status": "fallback_applied" if fallback_applied else "evaluated",
        "passed": not bool(original.get("overfit")),
        "fallback_applied": bool(fallback_applied),
        "original_best_trial_number": int(best_number),
        "selected_trial_number": int(selected.get("trial_number") or 0),
        "selected_params": dict(selected.get("params") or {}),
        "selected_cpcv": dict(selected.get("cpcv_result") or {}),
        "selected_objective_value": float(selected.get("objective_value") or 0.0),
        "max_neighbor_decay": float(threshold),
        "top_decile_count": int(len(candidate_rows)),
        "top_decile": candidate_rows,
    }


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
        score, cpcv_result = _gbm_objective_for_hyperparams(
            hyperparams,
            X=X,
            y=y,
            cpcv_cfg=cpcv_cfg,
            horizon_s=int(horizon_s),
            pbo_penalty=float(pbo_penalty),
        )
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

    surface_summary = evaluate_parameter_surface_robustness(
        trials=list(getattr(study, "trials", []) or []),
        best_trial=best_trial,
        evaluate_params=lambda params: _gbm_objective_for_hyperparams(
            gbm._normalized_hyperparams(dict(params or {})),
            X=X,
            y=y,
            cpcv_cfg=cpcv_cfg,
            horizon_s=int(horizon_s),
            pbo_penalty=float(pbo_penalty),
        ),
        pbo_penalty=float(pbo_penalty),
    )
    best_params = gbm._normalized_hyperparams(dict(surface_summary.get("selected_params") or {}))
    best_cpcv = dict(surface_summary.get("selected_cpcv") or {})
    objective_value = float(surface_summary.get("selected_objective_value") or _trial_value(best_trial, pbo_penalty=float(pbo_penalty)))
    best_trial_number = int(surface_summary.get("selected_trial_number") or getattr(best_trial, "number", 0) or 0)
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
        "parameter_surface": dict(surface_summary),
    }
    registry_id = record_model_hyperparameter_registry(
        model_name=str(resolved_model_name),
        model_family=str(train_cfg.get("family") or gbm._GBM_FAMILY),
        tuner=_TUNER_NAME,
        objective="median_sharpe_minus_pbo_penalty",
        metric_value=float(objective_value),
        params=dict(best_params),
        study_name=str(getattr(study, "study_name", "") or _study_name_for_model(resolved_model_name)),
        trial_count=int(len(list(getattr(study, "trials", []) or []))),
        best_trial_number=int(best_trial_number),
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
        "best_trial_number": int(best_trial_number),
        "objective_value": float(objective_value),
        "best_params": dict(best_params),
        "cpcv": dict(best_cpcv),
        "parameter_surface": dict(surface_summary),
        "diagnostics": diagnostics,
    }


def main() -> int:
    """CLI entrypoint for the GBM Optuna tuning job."""
    result = run_gbm_optuna_tuning_job()
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    return 0 if bool(result.get("ok")) else 1


__all__ = [
    "evaluate_parameter_surface_robustness",
    "main",
    "run_gbm_optuna_tuning_job",
]
