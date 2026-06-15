from __future__ import annotations

import numpy as np

from engine.strategy import optuna_tuner
from engine.strategy.optuna_tuner import evaluate_parameter_surface_robustness


def test_neighbor_step_warns_and_uses_typed_fallback(monkeypatch) -> None:
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(optuna_tuner, "_warn_nonfatal", lambda *args, **kwargs: calls.append((args, kwargs)))

    step = optuna_tuner._neighbor_step("learning_rate", 0.2, {"dtype": "float", "step": "bad-step"})

    assert step == 0.020000000000000004
    assert calls
    assert calls[0][0][0] == "OPTUNA_TUNER_INVALID_NEIGHBOR_STEP"
    assert calls[0][1]["param"] == "learning_rate"


class _Trial:
    def __init__(self, number: int, value: float, params: dict[str, float | int], median_sharpe: float) -> None:
        self.number = int(number)
        self.value = float(value)
        self.user_attrs = {
            "hyperparams": dict(params),
            "cpcv_result": {
                "ok": True,
                "status": "evaluated",
                "n_paths": 4,
                "mean_sharpe": float(median_sharpe),
                "median_sharpe": float(median_sharpe),
                "pbo": 0.0,
            },
        }


class _SuggestTrial:
    def __init__(self, number: int, suggestions: dict[str, float | int]) -> None:
        self.number = int(number)
        self._suggestions = dict(suggestions)
        self.user_attrs: dict[str, object] = {}
        self.value: float | None = None

    def suggest_int(self, name: str, low: int, high: int, log: bool = False, step: int | None = None) -> int:
        return int(self._suggestions[name])

    def suggest_float(self, name: str, low: float, high: float, log: bool = False) -> float:
        return float(self._suggestions[name])

    def set_user_attr(self, key: str, value: object) -> None:
        self.user_attrs[str(key)] = value


class _Study:
    def __init__(self, trials: list[_SuggestTrial]) -> None:
        self.study_name = "surface-test"
        self._trials = list(trials)
        self.trials: list[_SuggestTrial] = []
        self.best_trial: _SuggestTrial | None = None

    def optimize(self, objective, n_trials: int, timeout: int | None = None) -> None:
        for trial in self._trials[: int(n_trials)]:
            trial.value = float(objective(trial))
            self.trials.append(trial)
            if self.best_trial is None or float(trial.value or 0.0) > float(self.best_trial.value or 0.0):
                self.best_trial = trial


class _Optuna:
    class samplers:
        class TPESampler:
            def __init__(self, seed: int | None = None) -> None:
                self.seed = seed

    def __init__(self, trials: list[_SuggestTrial]) -> None:
        self._trials = list(trials)

    def create_study(self, direction: str, study_name: str, sampler=None) -> _Study:
        return _Study(list(self._trials))


def _tuned_tuple(params: dict) -> tuple[int, float, int, int]:
    return (
        int(params.get("num_leaves")),
        round(float(params.get("learning_rate")), 6),
        int(params.get("n_estimators")),
        int(params.get("min_child_samples")),
    )


def test_spiky_hpo_surface_triggers_robust_top_decile_fallback() -> None:
    best_params = {
        "num_leaves": 50,
        "learning_rate": 0.05,
        "n_estimators": 100,
        "min_child_samples": 10,
    }
    robust_params = {
        "num_leaves": 20,
        "learning_rate": 0.05,
        "n_estimators": 100,
        "min_child_samples": 10,
    }
    trials = [
        _Trial(0, 2.0, best_params, 2.0),
        _Trial(1, 1.8, robust_params, 1.8),
    ]
    for idx in range(2, 11):
        trials.append(
            _Trial(
                idx,
                1.0 - (idx * 0.01),
                {
                    "num_leaves": 10 + idx,
                    "learning_rate": 0.05,
                    "n_estimators": 100,
                    "min_child_samples": 10,
                },
                1.0 - (idx * 0.01),
            )
        )

    def evaluate(params: dict):
        tuned = _tuned_tuple(params)
        if tuned == _tuned_tuple(best_params):
            median = 2.0
        elif tuned == _tuned_tuple(robust_params):
            median = 1.8
        elif tuned[0] >= 45:
            median = 0.35
        elif tuned[0] == 20:
            median = 1.70
        else:
            median = 1.50
        return median, {
            "ok": True,
            "status": "evaluated",
            "n_paths": 4,
            "mean_sharpe": float(median),
            "median_sharpe": float(median),
            "pbo": 0.0,
        }

    summary = evaluate_parameter_surface_robustness(
        trials=trials,
        best_trial=trials[0],
        evaluate_params=evaluate,
        pbo_penalty=0.0,
        max_neighbor_decay=0.30,
    )

    assert summary["applied"]
    assert summary["fallback_applied"]
    assert summary["selected_trial_number"] == 1
    assert int(summary["selected_params"]["num_leaves"]) == 20
    original = next(row for row in summary["top_decile"] if row["trial_number"] == 0)
    fallback = next(row for row in summary["top_decile"] if row["trial_number"] == 1)
    assert original["neighbor_decay"] > 0.30
    assert fallback["neighbor_decay"] < original["neighbor_decay"]


def test_gbm_optuna_job_records_surface_summary_and_fallback(monkeypatch) -> None:
    best_params = {
        "num_leaves": 50,
        "learning_rate": 0.05,
        "n_estimators": 100,
        "min_child_samples": 10,
    }
    robust_params = {
        "num_leaves": 20,
        "learning_rate": 0.05,
        "n_estimators": 100,
        "min_child_samples": 10,
    }
    fake_optuna = _Optuna(
        [
            _SuggestTrial(0, best_params),
            _SuggestTrial(1, robust_params),
        ]
    )
    dataset = {
        "ok": True,
        "status": "loaded",
        "row_count": 12,
        "train_cfg": {"family": "gbm_regressor", "hyperparams": {}},
        "runtime_symbols": ["AAPL"],
        "feature_ids": ["base.source_credibility"],
        "horizon_s": 300,
        "lookback_days": 30,
        "X": np.ones((12, 1), dtype=np.float32),
        "y": np.ones(12, dtype=np.float32),
    }
    captured: dict[str, object] = {}

    def fake_cpcv(_X, _y, model_factory, **_kwargs):
        params = model_factory()
        tuned = _tuned_tuple(params)
        if tuned == _tuned_tuple(best_params):
            median = 2.0
        elif tuned == _tuned_tuple(robust_params):
            median = 1.8
        elif tuned[0] >= 45:
            median = 0.35
        elif tuned[0] == 20:
            median = 1.70
        else:
            median = 1.50
        return {
            "ok": True,
            "status": "evaluated",
            "n_paths": 4,
            "mean_sharpe": float(median),
            "median_sharpe": float(median),
            "pbo": 0.0,
        }

    def fake_record(**kwargs):
        captured.update(kwargs)
        return 77

    monkeypatch.setattr(optuna_tuner, "init_db", lambda: None)
    monkeypatch.setattr(optuna_tuner, "_import_optuna", lambda: fake_optuna)
    monkeypatch.setattr(optuna_tuner, "_load_gbm_training_dataset", lambda _model_name: dataset)
    monkeypatch.setattr(optuna_tuner, "_make_lgbm_factory", lambda hyperparams: (lambda: dict(hyperparams)))
    monkeypatch.setattr(optuna_tuner, "cpcv_backtest", fake_cpcv)
    monkeypatch.setattr(optuna_tuner, "record_model_hyperparameter_registry", fake_record)

    result = optuna_tuner.run_gbm_optuna_tuning_job(model_name="surface.gbm", n_trials=2)

    assert result["ok"]
    assert result["registry_id"] == 77
    assert result["best_trial_number"] == 1
    assert int(result["best_params"]["num_leaves"]) == 20
    assert result["parameter_surface"]["fallback_applied"]
    diagnostics = captured["diagnostics"]
    assert diagnostics["parameter_surface"]["selected_trial_number"] == 1
    assert diagnostics["parameter_surface"]["fallback_applied"]
