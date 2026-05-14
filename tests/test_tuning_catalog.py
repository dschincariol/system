from __future__ import annotations

from engine.strategy.tuning.catalog import Hyperparam, catalog_defaults, catalog_for_family, managed_env_names


class DummyTrial:
    def __init__(self):
        self.calls = []

    def suggest_int(self, name, low, high, log=False):
        self.calls.append(("int", name, low, high, log))
        return low

    def suggest_float(self, name, low, high, log=False):
        self.calls.append(("float", name, low, high, log))
        return low

    def suggest_categorical(self, name, choices):
        self.calls.append(("categorical", name, tuple(choices)))
        return choices[0]


def test_catalog_exposes_managed_temporal_and_embed_params() -> None:
    assert catalog_defaults("temporal_predictor")["seq_len"] == 6
    assert catalog_defaults("embed_regressor")["train_split"] == 0.8
    assert "TEMPORAL_SEQ_LEN" in managed_env_names("temporal_predictor")
    assert "EMBED_TRAIN_SPLIT" in managed_env_names("embed_regressor")


def test_hyperparam_suggest_uses_expected_optuna_distribution() -> None:
    trial = DummyTrial()
    assert Hyperparam("depth", "x", "int", 3, low=2, high=8, log=True).suggest(trial) == 2
    assert Hyperparam("lr", "x", "float", 0.1, low=0.01, high=0.2).suggest(trial) == 0.01
    assert Hyperparam("loss", "x", "categorical", "a", choices=("a", "b")).suggest(trial) == "a"
    assert trial.calls == [
        ("int", "depth", 2, 8, True),
        ("float", "lr", 0.01, 0.2, False),
        ("categorical", "loss", ("a", "b")),
    ]


def test_catalog_round_trips_to_dict() -> None:
    payloads = [param.to_dict() for param in catalog_for_family("xgb_regressor")]
    assert payloads
    assert {row["env_name"] for row in payloads} >= {"XGB_MAX_DEPTH", "XGB_LEARNING_RATE"}
