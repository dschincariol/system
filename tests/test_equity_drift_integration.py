import importlib
import os
import sqlite3
import sys
import time
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names):
    modules = []
    for name in module_names:
        if name in sys.modules:
            modules.append(importlib.reload(sys.modules[name]))
        else:
            modules.append(importlib.import_module(name))
    return modules


@pytest.fixture()
def runtime_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "equity_drift.db"
    previous_backend = os.environ.get("TS_STORAGE_BACKEND")
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.delenv("TIMESCALE_DSN", raising=False)
    monkeypatch.delenv("TIMESCALE_URL", raising=False)
    monkeypatch.delenv("TIMESCALE_DATABASE_URL", raising=False)
    monkeypatch.setenv("FEATURE_STORE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_INIT_ON_STARTUP", "0")

    _, storage = _reload_modules(
        "engine.runtime.db_guard",
        "engine.runtime.storage",
    )
    _reload_modules(
        "engine.runtime.alerts",
        "engine.runtime.equity_drift",
        "engine.data.equity_snapshot",
    )
    storage.init_db()

    try:
        yield {"db_path": db_path, "storage": storage}
    finally:
        try:
            storage.shutdown_timeseries_storage(timeout_s=0.1)
        except Exception:
            pass
        try:
            storage.close_pooled_connections()
        except Exception:
            pass
        if previous_backend is None:
            os.environ.pop("TS_STORAGE_BACKEND", None)
        else:
            os.environ["TS_STORAGE_BACKEND"] = previous_backend
        _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.alerts",
            "engine.runtime.equity_drift",
            "engine.data.equity_snapshot",
        )


def _seed_latest_backtest(con, *, run_ts_ms: int, point_ts_ms: int, equity: float) -> int:
    cur = con.execute(
        """
        INSERT INTO portfolio_bt_runs (ts_ms, start_ts_ms, end_ts_ms, metrics_json)
        VALUES (?, ?, ?, ?)
        """,
        (int(run_ts_ms), int(run_ts_ms - 60_000), int(point_ts_ms), "{}"),
    )
    run_id = int(cur.lastrowid)
    con.execute(
        """
        INSERT INTO portfolio_bt_points (run_id, ts_ms, ret, equity, drawdown, detail_json)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (run_id, int(point_ts_ms), 0.0, float(equity), 0.0, "{}"),
    )
    return run_id


def test_snapshot_equity_writes_equity_drift(runtime_modules) -> None:
    equity_snapshot, equity_drift = _reload_modules(
        "engine.data.equity_snapshot",
        "engine.runtime.equity_drift",
    )

    now_ms = int(time.time() * 1000)
    with sqlite3.connect(str(runtime_modules["db_path"])) as con:
        con.execute(
            """
            INSERT INTO broker_account (ts_ms, updated_ts_ms, equity)
            VALUES (?, ?, ?)
            """,
            (now_ms + 1_000, now_ms + 1_000, 110000.0),
        )
        run_id = _seed_latest_backtest(
            con,
            run_ts_ms=now_ms - 5_000,
            point_ts_ms=now_ms - 5_000,
            equity=100000.0,
        )
        con.commit()

    assert equity_snapshot.snapshot_equity(ts_ms=now_ms) is True

    with sqlite3.connect(str(runtime_modules["db_path"])) as con:
        eq_hist = con.execute(
            "SELECT equity FROM equity_history WHERE ts_ms=?",
            (now_ms,),
        ).fetchone()
        drift_row = con.execute(
            """
            SELECT broker_equity, backtest_equity, diff_equity, diff_equity_pct, level, backtest_run_id
            FROM equity_drift
            WHERE ts_ms=?
            """,
            (now_ms,),
        ).fetchone()
        alerts = con.execute(
            """
            SELECT rule_id, severity, symbol
            FROM alerts
            ORDER BY id ASC
            """
        ).fetchall()
        current = equity_drift.get_current_equity_drift(con)

    assert eq_hist == (110000.0,)
    assert drift_row is not None
    assert drift_row[0] == pytest.approx(110000.0)
    assert drift_row[1] == pytest.approx(100000.0)
    assert drift_row[2] == pytest.approx(10000.0)
    assert drift_row[3] == pytest.approx(0.10)
    assert drift_row[4] == "CRIT"
    assert drift_row[5] == run_id
    assert alerts == [("EQUITY_RECON", "CRIT", "PORTFOLIO")]
    assert current["equity_diff_level"] == "CRIT"
    assert current["diff_equity"] == pytest.approx(10000.0)


def test_sync_equity_drift_backfills_existing_history(runtime_modules) -> None:
    (equity_drift,) = _reload_modules("engine.runtime.equity_drift")

    now_ms = int(time.time() * 1000)
    with sqlite3.connect(str(runtime_modules["db_path"])) as con:
        _seed_latest_backtest(
            con,
            run_ts_ms=now_ms - 5_000,
            point_ts_ms=now_ms - 5_000,
            equity=100000.0,
        )
        con.executemany(
            "INSERT INTO equity_history (ts_ms, equity) VALUES (?, ?)",
            [
                (now_ms - 2_000, 101000.0),
                (now_ms - 1_000, 98000.0),
            ],
        )

        summary = equity_drift.sync_equity_drift_from_history(con)
        con.commit()

        rows = con.execute(
            "SELECT ts_ms, diff_equity, level FROM equity_drift ORDER BY ts_ms ASC"
        ).fetchall()

    assert summary["ok"] is True
    assert summary["written"] == 2
    assert rows == [
        (now_ms - 2_000, 1000.0, "OK"),
        (now_ms - 1_000, -2000.0, "OK"),
    ]


def test_snapshot_equity_emits_sustained_equity_drift_alert(runtime_modules) -> None:
    (equity_snapshot,) = _reload_modules("engine.data.equity_snapshot")

    now_ms = int(time.time() * 1000)
    with sqlite3.connect(str(runtime_modules["db_path"])) as con:
        con.execute(
            """
            INSERT INTO broker_account (ts_ms, updated_ts_ms, equity)
            VALUES (?, ?, ?)
            """,
            (now_ms + 1_000, now_ms + 1_000, 112000.0),
        )
        _seed_latest_backtest(
            con,
            run_ts_ms=now_ms - 5_000,
            point_ts_ms=now_ms - 5_000,
            equity=100000.0,
        )
        con.commit()

    assert equity_snapshot.snapshot_equity(ts_ms=now_ms) is True
    assert equity_snapshot.snapshot_equity(ts_ms=now_ms + 301_000) is True

    with sqlite3.connect(str(runtime_modules["db_path"])) as con:
        rows = con.execute(
            """
            SELECT rule_id, severity, COUNT(*)
            FROM alerts
            GROUP BY rule_id, severity
            ORDER BY rule_id ASC, severity ASC
            """
        ).fetchall()

    assert rows == [
        ("EQUITY_DRIFT_SUSTAINED", "CRIT", 1),
        ("EQUITY_RECON", "CRIT", 1),
    ]
