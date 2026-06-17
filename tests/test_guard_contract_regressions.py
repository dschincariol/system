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
def runtime_modules(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "guard_contracts.db"
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


def test_detect_sustained_equity_drift_ignores_missing_table(runtime_modules) -> None:
    (guards,) = _reload_modules("engine.runtime.guards")

    with sqlite3.connect(":memory:") as con:
        result = guards.detect_sustained_equity_drift(
            con,
            window=5,
            min_warn=2,
            min_crit=1,
        )

    assert result is None


def test_promotion_guard_surfaces_missing_equity_drift_signal(runtime_modules) -> None:
    (promotion_guard,) = _reload_modules("engine.strategy.promotion_guard")

    allowed, reason = promotion_guard.promotion_allowed()

    assert isinstance(allowed, bool)
    assert reason["equity_drift_available"] is True
    assert reason["equity_drift_crit_points"] == 0
    assert "equity_drift_crit" not in reason["blockers"]


def test_promotion_guard_blocks_when_equity_drift_is_critical(runtime_modules) -> None:
    (promotion_guard,) = _reload_modules("engine.strategy.promotion_guard")

    now_ms = int(time.time() * 1000)
    with sqlite3.connect(str(runtime_modules["db_path"])) as con:
        con.execute(
            """
            INSERT INTO equity_drift (
                ts_ms,
                broker_equity,
                backtest_equity,
                diff_equity,
                diff_equity_pct,
                level,
                reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (now_ms, 110000.0, 100000.0, 10000.0, 0.10, "CRIT", "test"),
        )
        con.commit()

    allowed, reason = promotion_guard.promotion_allowed()

    assert allowed is False
    assert reason["equity_drift_available"] is True
    assert reason["equity_drift_crit_points"] >= 1
    assert "equity_drift_crit" in reason["blockers"]


def test_promotion_guard_blocks_live_when_backup_restore_evidence_missing(
    runtime_modules,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("ENGINE_MODE", "live")
    monkeypatch.setenv("BACKUP_EVIDENCE_PATH", str(tmp_path / "missing.json"))
    monkeypatch.setenv("TS_BACKUP_BASE_DIR", str(tmp_path / "base"))
    monkeypatch.setenv("TS_BACKUP_WAL_DIR", str(tmp_path / "wal"))
    monkeypatch.setenv("TS_RESTORE_DRILL_DIR", str(tmp_path / "drills"))
    (promotion_guard,) = _reload_modules("engine.strategy.promotion_guard")

    allowed, reason = promotion_guard.promotion_allowed()

    assert allowed is False
    assert "backup_evidence_base_backup_missing" in reason["blockers"]
    assert reason["backup_restore_evidence"]["required"] is True


def test_global_risk_envelope_prefers_runtime_gate_snapshot(runtime_modules, monkeypatch: pytest.MonkeyPatch) -> None:
    gates, global_risk_envelope = _reload_modules(
        "engine.runtime.gates",
        "engine.runtime.global_risk_envelope",
    )

    monkeypatch.setattr(
        gates,
        "get_execution_degraded_snapshot",
        lambda *_args, **_kwargs: {
            "active": True,
            "severity": "CRITICAL",
            "reason": "test_execution_degraded",
            "reason_codes": ["test_execution_degraded"],
        },
    )

    now_ms = int(time.time() * 1000)
    with sqlite3.connect(str(runtime_modules["db_path"])) as con:
        con.executemany(
            """
            INSERT INTO execution_capital_efficiency (
                ts_ms,
                client_order_id,
                symbol,
                pnl_net
            ) VALUES (?, ?, ?, ?)
            """,
            [
                (now_ms - 5_400_000, "o1", "AAPL", 100.0),
                (now_ms - 3_600_000, "o2", "AAPL", -40.0),
                (now_ms - 1_800_000, "o3", "AAPL", 60.0),
            ],
        )
        con.commit()

        result = global_risk_envelope.compute_global_risk_envelope(
            con,
            now_ms=now_ms,
        )

    assert result["ok"] is True
    assert result["components"]["execution_degraded"] is True
    assert result["components"]["exec_scale"] == pytest.approx(
        global_risk_envelope.EXEC_DEGRADE_SCALE
    )
