import sqlite3

from engine.strategy.ensemble.oos_store import ensure_schema, read_oos_predictions, upsert_oos_prediction
from engine.strategy.jobs.fill_ensemble_oos_targets import fill_targets_from_labels


def test_oos_store_upsert_and_read_preserves_primary_key():
    con = sqlite3.connect(":memory:")
    ensure_schema(con)

    upsert_oos_prediction(
        symbol="AAPL",
        horizon=3600,
        family="embed_regressor",
        ts=1000,
        prediction=0.25,
        target=0.3,
        con=con,
    )
    upsert_oos_prediction(
        symbol="AAPL",
        horizon=3600,
        family="embed_regressor",
        ts=1000,
        prediction=0.5,
        target=None,
        con=con,
    )

    rows = read_oos_predictions(symbol="AAPL", horizon=3600, con=con)
    assert len(rows) == 1
    assert rows[0]["prediction"] == 0.5
    assert rows[0]["target"] == 0.3

    count = con.execute("SELECT COUNT(*) FROM model_oos_predictions").fetchone()[0]
    assert count == 1


def test_oos_store_keeps_distinct_run_ids_for_same_prediction_key():
    con = sqlite3.connect(":memory:")
    ensure_schema(con)

    upsert_oos_prediction(
        symbol="AAPL",
        horizon=3600,
        family="embed_regressor",
        ts=1000,
        prediction=0.25,
        run_id="run-a",
        target=0.3,
        con=con,
    )
    upsert_oos_prediction(
        symbol="AAPL",
        horizon=3600,
        family="embed_regressor",
        ts=1000,
        prediction=0.5,
        run_id="run-b",
        target=0.4,
        con=con,
    )

    rows = read_oos_predictions(symbol="AAPL", horizon=3600, con=con, latest_per_key=False)
    assert len(rows) == 2
    assert {row["run_id"] for row in rows} == {"run-a", "run-b"}

    latest = read_oos_predictions(symbol="AAPL", horizon=3600, con=con)
    assert len(latest) == 1
    assert latest[0]["run_id"] == "run-b"


def test_oos_store_migration_backfills_legacy_run_id():
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE model_oos_predictions (
            symbol TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            family TEXT NOT NULL,
            ts INTEGER NOT NULL,
            prediction REAL NOT NULL,
            target REAL NULL,
            PRIMARY KEY(symbol, horizon, family, ts)
        )
        """
    )
    con.execute(
        "INSERT INTO model_oos_predictions(symbol, horizon, family, ts, prediction, target) VALUES(?,?,?,?,?,?)",
        ("AAPL", 3600, "embed_regressor", 1000, 0.25, 0.3),
    )

    ensure_schema(con)

    row = read_oos_predictions(symbol="AAPL", horizon=3600, con=con)[0]
    assert row["run_id"] == "legacy"


def test_fill_targets_from_labels_updates_pending_oos_rows():
    con = sqlite3.connect(":memory:")
    ensure_schema(con)
    con.execute(
        """
        CREATE TABLE labels (
            event_id INTEGER NOT NULL,
            horizon_s INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            impact_z REAL,
            realized_z REAL,
            ts_ms INTEGER NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE labels_exec (
            event_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            horizon_s INTEGER NOT NULL,
            realized INTEGER NOT NULL,
            net_z REAL
        )
        """
    )
    upsert_oos_prediction(
        symbol="AAPL",
        horizon=3600,
        family="embed_regressor",
        ts=1000,
        prediction=0.25,
        target=None,
        con=con,
    )
    con.execute(
        "INSERT INTO labels(event_id, horizon_s, symbol, impact_z, realized_z, ts_ms) VALUES(?,?,?,?,?,?)",
        (7, 3600, "AAPL", 0.1, 0.2, 1000),
    )
    con.execute(
        "INSERT INTO labels_exec(event_id, symbol, horizon_s, realized, net_z) VALUES(?,?,?,?,?)",
        (7, "AAPL", 3600, 1, 0.42),
    )

    result = fill_targets_from_labels(con=con, now_ms=2000, delay_ms=0, limit=10)
    rows = read_oos_predictions(symbol="AAPL", horizon=3600, con=con)

    assert result["updated_count"] == 1
    assert rows[0]["target"] == 0.42
