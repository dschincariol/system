import importlib
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
def daemon_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "snapshot_equity_daemon.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
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


def test_snapshot_equity_daemon_run_once_writes_snapshot_and_drift(
    daemon_runtime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now_ms = int(time.time() * 1000)
    with sqlite3.connect(str(daemon_runtime["db_path"])) as con:
        con.execute(
            """
            INSERT INTO broker_account (ts_ms, updated_ts_ms, equity)
            VALUES (?, ?, ?)
            """,
            (now_ms + 1000, now_ms + 1000, 125000.0),
        )
        cur = con.execute(
            """
            INSERT INTO portfolio_bt_runs (ts_ms, start_ts_ms, end_ts_ms, metrics_json)
            VALUES (?, ?, ?, ?)
            """,
            (now_ms, now_ms - 60_000, now_ms, "{}"),
        )
        run_id = int(cur.lastrowid)
        con.execute(
            """
            INSERT INTO portfolio_bt_points (run_id, ts_ms, ret, equity, drawdown, detail_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id, now_ms, 0.0, 100000.0, 0.0, "{}"),
        )
        con.commit()

    monkeypatch.setenv("ENGINE_SUPERVISED", "1")
    monkeypatch.setenv("SNAPSHOT_EQUITY_RUN_ONCE", "1")

    (snapshot_equity_job,) = _reload_modules("engine.runtime.jobs.snapshot_equity")

    assert snapshot_equity_job.main() == 0

    with sqlite3.connect(str(daemon_runtime["db_path"])) as con:
        eq_hist = con.execute("SELECT MAX(equity) FROM equity_history").fetchone()[0]
        drift = con.execute(
            """
            SELECT broker_equity, backtest_equity, level
            FROM equity_drift
            ORDER BY ts_ms DESC
            LIMIT 1
            """
        ).fetchone()
        alert = con.execute(
            """
            SELECT rule_id, severity, symbol
            FROM alerts
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()

    assert eq_hist == pytest.approx(125000.0)
    assert drift is not None
    assert drift[0] == pytest.approx(125000.0)
    assert drift[1] == pytest.approx(100000.0)
    assert drift[2] == "CRIT"
    assert alert == ("EQUITY_RECON", "CRIT", "PORTFOLIO")
