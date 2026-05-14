from __future__ import annotations

from engine.strategy.tuning.study import open_study


def test_optuna_study_is_persistent_and_resumable(tmp_path) -> None:
    db_path = tmp_path / "optuna.sqlite"
    study = open_study(model_family="temporal_predictor", symbol="AAPL", db_path=db_path, seed=11)
    study.optimize(lambda trial: trial.suggest_float("x", 0.0, 1.0), n_trials=2)
    first_count = len(study.trials)

    reopened = open_study(model_family="temporal_predictor", symbol="AAPL", db_path=db_path, seed=11)
    assert len(reopened.trials) == first_count
    reopened.optimize(lambda trial: trial.suggest_float("x", 0.0, 1.0), n_trials=1)
    assert len(reopened.trials) == first_count + 1
