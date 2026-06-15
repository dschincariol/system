from __future__ import annotations

import math
import sqlite3

import numpy as np
import pytest

from engine.strategy import bocpd


def _create_bocpd_tables(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE bocpd_regime_state (
            series_key TEXT NOT NULL,
            series_type TEXT NOT NULL,
            symbol TEXT NOT NULL DEFAULT '*',
            ts_ms INTEGER NOT NULL,
            cp_prob_5d REAL NOT NULL DEFAULT 0.0,
            map_run_length INTEGER NOT NULL DEFAULT 0,
            expected_run_length REAL NOT NULL DEFAULT 0.0,
            run_length_z REAL NOT NULL DEFAULT 0.0,
            active_states INTEGER NOT NULL DEFAULT 0,
            n_obs INTEGER NOT NULL DEFAULT 0,
            posterior_json TEXT,
            created_ts_ms INTEGER NOT NULL,
            PRIMARY KEY(series_key, ts_ms)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE bocpd_ensemble_triggers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts_ms INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            horizon_s INTEGER NOT NULL,
            cp_prob_5d REAL NOT NULL,
            threshold REAL NOT NULL,
            mode TEXT NOT NULL,
            base_window INTEGER NOT NULL,
            effective_window INTEGER NOT NULL,
            series_key TEXT,
            meta_json TEXT
        )
        """
    )


def test_bocpd_detects_planted_breaks_with_low_false_positive_rate() -> None:
    rng = np.random.default_rng(7)
    mean_shift = np.r_[rng.normal(0.0, 1.0, 120), rng.normal(3.0, 1.0, 120)]
    mean_eval = bocpd.evaluate_detection(mean_shift, breakpoints=[120], detection_threshold=0.5, max_delay=10)

    rng = np.random.default_rng(8)
    variance_shift = np.r_[rng.normal(0.0, 1.0, 120), rng.normal(0.0, 4.0, 120)]
    var_eval = bocpd.evaluate_detection(variance_shift, breakpoints=[120], detection_threshold=0.5, max_delay=10)

    rng = np.random.default_rng(9)
    no_break = bocpd.bocpd_series(rng.normal(0.0, 1.0, 240))
    false_positive_rate = sum(float(row["cp_prob_5d"]) >= 0.5 for row in no_break) / len(no_break)

    assert mean_eval["all_detected"] is True
    assert int(mean_eval["max_delay"]) <= 10
    assert var_eval["all_detected"] is True
    assert int(var_eval["max_delay"]) <= 10
    assert false_positive_rate < 0.05


def test_bocpd_numerically_stable_on_long_series() -> None:
    rng = np.random.default_rng(12)
    summaries = bocpd.bocpd_series(rng.standard_t(df=4, size=1200))

    assert len(summaries) == 1200
    assert max(int(row["active_states"]) for row in summaries) < 600
    for row in summaries:
        assert math.isfinite(float(row["cp_prob_5d"]))
        assert 0.0 <= float(row["cp_prob_5d"]) <= 1.0
        assert int(row["map_run_length"]) >= 0


def test_bocpd_features_round_trip_through_persisted_snapshot() -> None:
    con = sqlite3.connect(":memory:")
    _create_bocpd_tables(con)
    bocpd.persist_summary(
        con,
        {
            "series_key": "realized_vol:AAA",
            "ts_ms": 2_000,
            "cp_prob_5d": 0.72,
            "map_run_length": 3,
            "expected_run_length": 4.0,
            "run_length_z": -7.36,
            "active_states": 5,
            "n_obs": 80,
            "posterior": {0: 0.4, 1: 0.32},
        },
        series_type="realized_vol",
        symbol="AAA",
    )
    bocpd.persist_summary(
        con,
        {
            "series_key": "realized_vol:AAA",
            "ts_ms": 4_000,
            "cp_prob_5d": 0.05,
            "map_run_length": 20,
            "expected_run_length": 18.0,
            "run_length_z": -5.16,
            "active_states": 8,
            "n_obs": 100,
            "posterior": {20: 0.6},
        },
        series_type="realized_vol",
        symbol="AAA",
    )

    from engine.strategy import feature_registry, model_feature_snapshots

    assert "bocpd_cp_prob_5d" in feature_registry.registered_feature_ids()
    assert "bocpd_run_length_z" in feature_registry.FEATURE_GROUPS["regime"]

    snap = model_feature_snapshots.build_model_feature_snapshot(
        symbol="AAA",
        ts_ms=3_000,
        feature_ids=["bocpd_cp_prob_5d", "bocpd_run_length_z"],
        con=con,
    )

    assert snap["feature_ids"] == ["bocpd_cp_prob_5d", "bocpd_run_length_z"]
    assert snap["features"]["bocpd_cp_prob_5d"] == pytest.approx(0.72)
    assert snap["features"]["bocpd_run_length_z"] == pytest.approx(-7.36)
    assert snap["availability"]["bocpd_regime"] is True
    assert snap["source_timestamps"]["bocpd_regime"]["summary_ts_ms"] == 2_000


def test_regime_stack_includes_bocpd_asset_layer() -> None:
    con = sqlite3.connect(":memory:")
    _create_bocpd_tables(con)
    bocpd.persist_summary(
        con,
        {
            "series_key": "realized_vol:AAA",
            "ts_ms": 2_000,
            "cp_prob_5d": 0.81,
            "map_run_length": 2,
            "expected_run_length": 2.5,
            "run_length_z": -7.49,
            "active_states": 5,
            "n_obs": 90,
            "posterior": {0: 0.5},
        },
        series_type="realized_vol",
        symbol="AAA",
    )

    from engine.strategy.regime_stack import compute_regime_vector

    result = compute_regime_vector(symbol="AAA", ts_ms=3_000, con=con, include_hmm=False)

    assert result["asset"]["bocpd_cp_prob_5d"] == pytest.approx(0.81)
    assert result["asset"]["bocpd_run_length_z"] == pytest.approx(-7.49)
    assert result["regimes"]["changepoint"] == "BREAK"


def test_bocpd_ensemble_trigger_logs_and_adapts_only_in_adapt_mode(monkeypatch) -> None:
    con = sqlite3.connect(":memory:")
    _create_bocpd_tables(con)
    bocpd.persist_summary(
        con,
        {
            "series_key": "portfolio_mean_correlation",
            "ts_ms": 2_000,
            "cp_prob_5d": 0.80,
            "map_run_length": 1,
            "expected_run_length": 1.5,
            "run_length_z": -7.6,
            "active_states": 4,
            "n_obs": 120,
            "posterior": {0: 0.6},
        },
        series_type="portfolio_correlation",
        symbol="*",
    )

    monkeypatch.setenv("BOCPD_ENSEMBLE_TRIGGER", "0.5")
    monkeypatch.setenv("BOCPD_ENSEMBLE_TRIGGER_MODE", "log_only")
    window, trigger = bocpd.effective_hedge_window(con, symbol="AAA", horizon=300, base_window=60)
    assert window == 60
    assert trigger["triggered"] is True
    assert trigger["recommended_effective_window"] == 30
    assert trigger["adapted"] is False
    bocpd.log_ensemble_trigger(con, trigger)
    assert con.execute("SELECT COUNT(*) FROM bocpd_ensemble_triggers").fetchone()[0] == 1

    monkeypatch.setenv("BOCPD_ENSEMBLE_TRIGGER_MODE", "adapt")
    window, trigger = bocpd.effective_hedge_window(con, symbol="AAA", horizon=300, base_window=60)
    assert window == 30
    assert trigger["effective_window"] == 30
    assert trigger["adapted"] is True


def test_bocpd_job_updates_realized_vol_series_with_supplied_connection() -> None:
    con = sqlite3.connect(":memory:")
    _create_bocpd_tables(con)
    con.execute("CREATE TABLE prices (ts_ms INTEGER, symbol TEXT, px REAL)")
    con.execute("CREATE TABLE crypto_funding_rates (funding_ts_ms INTEGER, funding_rate REAL)")
    rows = []
    funding_rows = []
    px_a = 100.0
    px_b = 50.0
    for idx in range(90):
        ts_ms = 1_000 + idx * 86_400_000
        px_a *= 1.0 + 0.002 * math.sin(idx / 5.0)
        px_b *= 1.0 + 0.0015 * math.cos(idx / 7.0)
        rows.append((ts_ms, "AAA", px_a))
        rows.append((ts_ms, "BBB", px_b))
        funding_rows.append((ts_ms, 0.0001 * math.sin(idx / 9.0)))
    con.executemany("INSERT INTO prices(ts_ms, symbol, px) VALUES (?, ?, ?)", rows)
    con.executemany("INSERT INTO crypto_funding_rates(funding_ts_ms, funding_rate) VALUES (?, ?)", funding_rows)

    from engine.strategy.jobs import bocpd_regime_update

    result = bocpd_regime_update.run_update(con=con, symbols=["AAA", "BBB"], limit=80)

    assert result["ok"] is True
    updated_keys = {row["series_key"] for row in result["updated"]}
    assert "realized_vol:AAA" in updated_keys
    assert "realized_vol:BBB" in updated_keys
    assert "portfolio_mean_correlation" in updated_keys
    assert "crypto_funding_aggregate" in updated_keys
    stored = bocpd.load_latest_summary(con, symbol="AAA", series_type="realized_vol")
    assert stored["series_key"] == "realized_vol:AAA"
    assert stored["n_obs"] >= 60


def test_bocpd_job_warns_when_har_source_falls_back_to_prices(monkeypatch) -> None:
    from engine.strategy.jobs import bocpd_regime_update

    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE har_rv_forecasts (bad INTEGER)")
    con.execute("CREATE TABLE prices (ts_ms INTEGER, symbol TEXT, px REAL)")
    rows = [(1_000 + idx * 86_400_000, "AAA", 100.0 + idx) for idx in range(45)]
    con.executemany("INSERT INTO prices(ts_ms, symbol, px) VALUES (?, ?, ?)", rows)
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(bocpd_regime_update, "_warn_nonfatal", lambda *args, **kwargs: calls.append((args, kwargs)))

    ts, values, source = bocpd_regime_update._realized_vol_series(con, "AAA", 40)

    assert source == "prices_squared_log_returns"
    assert len(ts) == len(values)
    assert values
    assert calls[0][0][0] == "BOCPD_REGIME_HAR_RV_SERIES_QUERY_FAILED"


def test_bocpd_job_commit_failure_raises_and_warns(monkeypatch) -> None:
    from engine.strategy.jobs import bocpd_regime_update

    inner = sqlite3.connect(":memory:")

    class CommitFailsConnection:
        def execute(self, *args, **kwargs):
            return inner.execute(*args, **kwargs)

        def commit(self) -> None:
            raise RuntimeError("commit failed")

    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(bocpd_regime_update, "_warn_nonfatal", lambda *args, **kwargs: calls.append((args, kwargs)))

    with pytest.raises(RuntimeError, match="commit failed"):
        bocpd_regime_update.run_update(con=CommitFailsConnection(), symbols=[], limit=80)

    assert calls[-1][0][0] == "BOCPD_REGIME_COMMIT_FAILED"
