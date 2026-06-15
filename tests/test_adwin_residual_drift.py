from __future__ import annotations

import sqlite3

import numpy as np

from engine.strategy import adwin_drift
from engine.strategy.adwin_drift import (
    ADWIN,
    TRIGGER_TYPE,
    effective_window_after_adwin,
    ensure_adwin_schema,
    run_adwin_residual_drift,
)


def test_table_columns_warns_when_metadata_probes_fail(monkeypatch) -> None:
    class BrokenMetadataConnection:
        def execute(self, *_args, **_kwargs):
            raise RuntimeError("metadata unavailable")

    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(adwin_drift, "_warn_nonfatal", lambda *args, **kwargs: calls.append((args, kwargs)))

    assert adwin_drift._table_columns(BrokenMetadataConnection(), "decision_log") == set()
    assert calls
    assert calls[0][0][0] == "ADWIN_DRIFT_TABLE_COLUMNS_FAILED"
    assert calls[0][1]["table"] == "decision_log"


def test_adwin_detects_planted_shift_and_stationary_stream_stays_quiet() -> None:
    rng = np.random.default_rng(17)
    detector = ADWIN(delta=0.002, min_window=16, max_window=512)
    hit = None
    stream = np.concatenate(
        [
            rng.normal(0.10, 0.015, size=500),
            rng.normal(0.55, 0.015, size=500),
        ]
    )
    for idx, value in enumerate(stream):
        result = detector.update(float(abs(value)))
        if result.get("drift"):
            hit = idx
            break

    assert hit is not None
    assert 500 <= int(hit) <= 600

    stationary = ADWIN(delta=0.002, min_window=16, max_window=2048)
    false_hits = 0
    for value in rng.normal(0.15, 0.02, size=10_000):
        if stationary.update(float(abs(value))).get("drift"):
            false_hits += 1
    assert false_hits == 0


def _make_con() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    ensure_adwin_schema(con)
    con.execute(
        """
        CREATE TABLE champion_assignments (
          scope TEXT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          model_name TEXT NOT NULL,
          challenger_name TEXT,
          regime TEXT,
          state TEXT NOT NULL,
          assigned_ts_ms INTEGER,
          updated_ts_ms INTEGER,
          meta_json TEXT,
          PRIMARY KEY(scope, symbol, horizon_s)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE decision_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          event_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          predicted_z REAL NOT NULL,
          confidence REAL,
          model_name TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE labels (
          event_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL,
          impact_z REAL,
          created_at_ms INTEGER
        )
        """
    )
    return con


def test_synthetic_residual_drift_emits_event_and_queues_retrain() -> None:
    con = _make_con()
    model_name = "lgbm_regressor.live"
    con.execute(
        """
        INSERT INTO champion_assignments(
          scope, symbol, horizon_s, model_name, challenger_name, regime, state,
          assigned_ts_ms, updated_ts_ms, meta_json
        )
        VALUES('symbol', 'AAPL', 300, ?, '', 'global', 'champion', 1, 1, '{}')
        """,
        (model_name,),
    )
    rng = np.random.default_rng(23)
    residuals = np.concatenate(
        [
            rng.normal(0.04, 0.008, size=160),
            rng.normal(0.75, 0.02, size=120),
        ]
    )
    for idx, residual in enumerate(residuals, start=1):
        ts_ms = 1_800_000_000_000 + idx * 1_000
        con.execute(
            """
            INSERT INTO decision_log(ts_ms, event_id, symbol, horizon_s, predicted_z, confidence, model_name)
            VALUES(?,?,?,?,?,?,?)
            """,
            (ts_ms, idx, "AAPL", 300, 0.0, 0.8, model_name),
        )
        con.execute(
            """
            INSERT INTO labels(event_id, symbol, horizon_s, impact_z, created_at_ms)
            VALUES(?,?,?,?,?)
            """,
            (idx, "AAPL", 300, float(residual), ts_ms + 300_000),
        )

    result = run_adwin_residual_drift(con=con, now_ms=1_800_000_999_000, enqueue_retrain=True)

    assert result["event_count"] >= 1
    event_row = con.execute(
        "SELECT trigger_type, action_taken, outcome_status, trigger_metrics FROM drift_retrain_events"
    ).fetchone()
    assert event_row is not None
    assert event_row[0] == TRIGGER_TYPE
    assert event_row[1] == "queue_training"
    assert event_row[2] == "enqueued"

    lifecycle_row = con.execute(
        "SELECT action, status, triggered_by FROM model_lifecycle_runs"
    ).fetchone()
    assert lifecycle_row == ("drift_triggered_retrain", "queued", "adwin_residual_drift")

    state_row = con.execute(
        """
        SELECT n_seen, n_detections, last_decision_ts_ms
        FROM champion_residual_adwin_state
        WHERE model_name=? AND symbol='AAPL' AND horizon_s=300
        """,
        (model_name,),
    ).fetchone()
    assert state_row is not None
    assert int(state_row[0]) == len(residuals)
    assert int(state_row[1]) >= 1
    assert int(state_row[2]) == 1_800_000_000_000 + len(residuals) * 1_000


def test_hedge_window_halves_after_recent_adwin_event() -> None:
    con = _make_con()
    model_name = "lgbm_regressor.live"
    now_ms = 1_800_001_000_000
    con.execute(
        """
        INSERT INTO drift_retrain_events(
          created_ts, model_name, family, trigger_type, trigger_metrics,
          action_taken, cooldown_applied, candidate_version, outcome_status, diagnostics
        )
        VALUES(?,?,?,?,?,?,?,?,?,?)
        """,
        (
            now_ms - 1_000,
            model_name,
            "lgbm_regressor",
            TRIGGER_TYPE,
            '{"symbol":"AAPL","horizon_s":300}',
            "queue_training",
            0,
            "adwin-test",
            "enqueued",
            "{}",
        ),
    )

    effective, trigger = effective_window_after_adwin(
        con,
        symbol="AAPL",
        horizon=300,
        model_names=[model_name, "lgbm_regressor.challenger"],
        base_window=60,
        now_ms=now_ms,
    )

    assert effective == 60
    assert trigger["triggered"] is True
    assert trigger["events"][0]["model_name"] == model_name
    assert trigger["per_model_windows"][model_name] == 30
    assert trigger["per_model_windows"]["lgbm_regressor.challenger"] == 60
