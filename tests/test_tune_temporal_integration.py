from __future__ import annotations

from engine.strategy.jobs import tune_models


def test_tune_models_resumes_and_records_best_params(monkeypatch, tmp_path) -> None:
    recorded = []

    def fake_record_best_params(*, model_family, symbol, study, seed=None, con=None):
        best = study.best_trial
        row = {
            "model_family": model_family,
            "symbol": symbol,
            "study_name": study.study_name,
            "params": dict(best.params),
            "value": float(best.value),
            "trial_number": int(best.number),
            "seed": seed,
        }
        recorded.append(row)
        return row

    monkeypatch.setenv("OPTUNA_DB_PATH", str(tmp_path / "studies.sqlite"))
    monkeypatch.setattr(tune_models, "record_best_params", fake_record_best_params)

    result1 = tune_models.run_tuning_job(
        model_families=["temporal_predictor"],
        symbols=["AAPL"],
        n_trials=2,
        seed=3,
        allow_smoke_objective=True,
    )
    result2 = tune_models.run_tuning_job(
        model_families=["temporal_predictor"],
        symbols=["AAPL"],
        n_trials=1,
        seed=3,
        allow_smoke_objective=True,
    )

    assert result1["ok"] is True
    assert result2["ok"] is True
    assert recorded[-1]["model_family"] == "temporal_predictor"
    assert result2["results"][0]["trials_before"] == result1["results"][0]["trials_after"]
    assert result2["results"][0]["trials_after"] == result1["results"][0]["trials_after"] + 1
