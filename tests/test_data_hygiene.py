from __future__ import annotations

import json
import sqlite3
from datetime import datetime

import numpy as np
import pytest


def test_persisted_clip_bounds_round_trip_and_serve_reuses_training_bounds(monkeypatch):
    monkeypatch.setenv("FEATURE_WINSOR_LOWER_PCT", "0")
    monkeypatch.setenv("FEATURE_WINSOR_UPPER_PCT", "75")
    module = pytest.importorskip("engine.strategy.models.lgbm_regressor")
    columns = ["feature.a", "feature.b"]
    train_raw = np.asarray(
        [
            [0.0, 1.0],
            [1.0, 2.0],
            [2.0, 3.0],
            [100.0, 4.0],
            [np.nan, 5.0],
        ],
        dtype=np.float32,
    )

    train_matrix, preprocessing, _accounting = module._matrix_from_features(
        train_raw,
        columns,
        phase="train",
        model_name="unit",
        fit_preprocessing=True,
        return_metadata=True,
    )
    schema = json.loads(json.dumps(module._feature_schema(columns, preprocessing=preprocessing)))
    serve_matrix = module._matrix_from_features(
        np.asarray([[999.0, -999.0]], dtype=np.float32),
        columns,
        feature_schema=schema,
        phase="serve",
        model_name="unit",
    )

    bounds = schema["preprocessing"]["winsorization"]["bounds"]
    assert train_matrix[-1, 0] == 0.0
    assert serve_matrix[0, 0] == pytest.approx(bounds["feature.a"]["upper"])
    assert serve_matrix[0, 1] == pytest.approx(bounds["feature.b"]["lower"])


def test_feature_nan_counters_log_and_alert(monkeypatch):
    monkeypatch.setenv("FEATURE_NAN_ALERT_PCT", "10")
    module = pytest.importorskip("engine.strategy.models.lgbm_regressor")
    monkeypatch.setattr(module, "log_failure", lambda *args, **kwargs: None)
    info_messages: list[str] = []
    warning_messages: list[str] = []
    monkeypatch.setattr(
        module.LOG,
        "info",
        lambda message, *args, **kwargs: info_messages.append(str(message) % args if args else str(message)),
    )
    monkeypatch.setattr(
        module.LOG,
        "warning",
        lambda message, *args, **kwargs: warning_messages.append(str(message) % args if args else str(message)),
    )

    module._matrix_from_features(
        np.asarray([[np.nan, 1.0], [np.nan, 2.0]], dtype=np.float32),
        ["feature.nan", "feature.ok"],
        phase="train",
        model_name="unit_nan",
        fit_preprocessing=True,
    )

    assert any("feature_nan_accounting" in message for message in info_messages)
    assert any("feature_nan_alert" in message for message in warning_messages)
    assert any("feature.nan" in message for message in warning_messages)


def test_naive_datetime_input_raises():
    from engine.data.time_utils import assert_utc_datetime

    with pytest.raises(ValueError, match="naive_datetime_not_utc"):
        assert_utc_datetime(datetime(2026, 1, 1), field_name="unit")


def test_label_generation_stops_at_synthetic_delist_date():
    from engine.data.jobs.label_due_events import compute_return

    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE universe_pit(
          symbol TEXT NOT NULL,
          first_seen_ts INTEGER NOT NULL,
          last_seen_ts INTEGER,
          is_active INTEGER NOT NULL,
          metadata_json TEXT
        )
        """
    )
    con.execute("CREATE TABLE prices(ts_ms INTEGER, symbol TEXT, price REAL, px REAL)")
    con.execute("INSERT INTO universe_pit(symbol, first_seen_ts, last_seen_ts, is_active, metadata_json) VALUES (?,?,?,?,?)", ("DEAD", 0, 2000, 0, "{}"))
    con.executemany(
        "INSERT INTO prices(ts_ms, symbol, price, px) VALUES (?,?,?,?)",
        [(1000, "DEAD", 10.0, 10.0), (1500, "DEAD", 11.0, 11.0), (3000, "DEAD", 20.0, 20.0)],
    )

    assert compute_return(con, "DEAD", 1000, 500) == pytest.approx(0.10)
    assert compute_return(con, "DEAD", 1000, 2500) is None


def test_split_like_price_row_is_flagged_before_training_prices(monkeypatch):
    from engine.data import price_hygiene

    monkeypatch.setattr(price_hygiene, "log_failure", lambda *args, **kwargs: None)

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE prices(ts_ms INTEGER, symbol TEXT, price REAL, px REAL)")
    con.execute("INSERT INTO prices(ts_ms, symbol, price, px) VALUES (?,?,?,?)", (1000, "ABC", 100.0, 100.0))

    accepted, flagged = price_hygiene.filter_split_like_price_rows(
        con,
        [{"ts_ms": 2000, "symbol": "ABC", "price": 40.0, "source": "unit"}],
    )

    assert accepted == []
    assert len(flagged) == 1
