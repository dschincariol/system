from __future__ import annotations

import importlib
import json
import sqlite3
import time

import pytest

pytestmark = pytest.mark.safety_critical


def _con() -> sqlite3.Connection:
    return sqlite3.connect(":memory:")


def _create_equity_history(con: sqlite3.Connection, rows: list[tuple[int, float]]) -> None:
    con.execute("CREATE TABLE equity_history(ts_ms INTEGER PRIMARY KEY, equity REAL NOT NULL)")
    con.executemany("INSERT INTO equity_history(ts_ms, equity) VALUES (?, ?)", rows)
    con.commit()


def _equity_rows(*, now_ms: int, n: int, equity: float = 100_000.0, step_ms: int = 1_000) -> list[tuple[int, float]]:
    return [(int(now_ms - (n - offset - 1) * step_ms), float(equity)) for offset in range(n)]


def test_sparse_history_without_bootstrap_fails_closed_in_portfolio_gate() -> None:
    drawdown_state = importlib.reload(importlib.import_module("engine.strategy.drawdown_state"))
    portfolio_risk_gate = importlib.reload(importlib.import_module("engine.strategy.portfolio_risk_gate"))
    con = _con()
    try:
        _create_equity_history(con, [(1, 100_000.0), (2, 100_500.0)])

        diagnostic = drawdown_state.evaluate_current_drawdown(con)
        assert diagnostic.ok is False
        assert diagnostic.reason_code == "DRAWDOWN_EQUITY_HISTORY_INSUFFICIENT"

        out, info = portfolio_risk_gate.apply_portfolio_risk_gate(
            con,
            {"AAPL": {"side": "LONG", "weight": 0.20}},
            {},
            now_ms=1_700_000_000_000,
        )

        assert info["blocked"] is True
        assert info["block_reason"]["type"] == "drawdown_state_unavailable"
        assert info["drawdown_state"]["reason_code"] == "DRAWDOWN_EQUITY_HISTORY_INSUFFICIENT"
        assert out["AAPL"]["side"] == "FLAT"
        assert float(out["AAPL"]["weight"]) == 0.0
    finally:
        con.close()


def test_sparse_history_with_audited_bootstrap_baseline_is_explicitly_allowed() -> None:
    drawdown_state = importlib.reload(importlib.import_module("engine.strategy.drawdown_state"))
    con = _con()
    try:
        _create_equity_history(con, [(1, 100_000.0), (2, 100_000.0)])

        audit_row = drawdown_state.record_drawdown_bootstrap_baseline(
            baseline_equity=100_000.0,
            actor="ops@example.com",
            reason="new live account bootstrap before five equity samples",
            source="unit_test",
            ttl_s=3600,
            detail={"ticket": "INC-1"},
            con=con,
        )

        diagnostic = drawdown_state.evaluate_current_drawdown(con)
        assert diagnostic.ok is True
        assert diagnostic.reason_code == "DRAWDOWN_BOOTSTRAP_BASELINE"
        assert diagnostic.bootstrap_actor == "ops@example.com"
        assert drawdown_state.get_current_drawdown(con) == 0.0

        row = con.execute(
            "SELECT actor, reason, row_hash FROM drawdown_bootstrap_baseline WHERE id=?",
            (audit_row["id"],),
        ).fetchone()
        assert row is not None
        assert row[0] == "ops@example.com"
        assert "new live account bootstrap" in row[1]
        assert row[2] is not None
    finally:
        con.close()


def test_missing_equity_history_triggers_kill_switch_capital_breach(monkeypatch) -> None:
    kill_switch = importlib.reload(importlib.import_module("engine.execution.kill_switch"))
    monkeypatch.setenv("EXECUTION_MODE", "live")
    con = _con()
    try:
        kill_switch._ensure_schema(con)
        breach = kill_switch._capital_risk_trigger(con)

        assert breach is not None
        assert breach["meta"]["trigger"] == "drawdown_state_unavailable"
        assert breach["meta"]["reason_code"] == "DRAWDOWN_EQUITY_HISTORY_MISSING"
        assert breach["meta"]["drawdown_state"]["ok"] is False
    finally:
        con.close()


def _reload_live_equity_modules(monkeypatch, **env):
    defaults = {
        "EXECUTION_MODE": "live",
        "ENGINE_MODE": "live",
        "KILL_SWITCH_MAX_EQUITY_AGE_S": "300",
        "DRAWDOWN_MAX_EQUITY_AGE_S": "300",
        "DRAWDOWN_MIN_HISTORY_POINTS": "5",
        "KILL_SWITCH_DAILY_EQUITY_MIN_POINTS": "2",
        "KILL_SWITCH_ROLLING_EQUITY_MIN_POINTS": "5",
        "KILL_SWITCH_VAR_MIN_HISTORY": "30",
        "KILL_SWITCH_VAR_EQUITY_MIN_POINTS": "31",
        "KILL_SWITCH_VAR_LOOKBACK_POINTS": "250",
    }
    defaults.update({str(k): str(v) for k, v in env.items()})
    for key, value in defaults.items():
        monkeypatch.setenv(key, value)
    importlib.reload(importlib.import_module("engine.strategy.drawdown_state"))
    return importlib.reload(importlib.import_module("engine.execution.kill_switch"))


def test_stale_latest_equity_fails_closed_and_is_visible_in_capital_breach(monkeypatch) -> None:
    kill_switch = _reload_live_equity_modules(
        monkeypatch,
        KILL_SWITCH_MAX_EQUITY_AGE_S="60",
        DRAWDOWN_MAX_EQUITY_AGE_S="60",
    )
    now_ms = int(time.time() * 1000)
    con = _con()
    try:
        _create_equity_history(con, _equity_rows(now_ms=now_ms - 120_000, n=31))

        snapshot = kill_switch.capital_equity_freshness_snapshot(con, live_mode=True)
        assert snapshot["ok"] is False
        assert snapshot["reason_code"] == "KILL_SWITCH_EQUITY_LATEST_STALE"
        assert snapshot["windows"]["latest"]["latest_age_s"] >= 120.0

        breach = kill_switch._capital_risk_trigger(con)
        assert breach is not None
        assert breach["meta"]["trigger"] == "drawdown_state_unavailable"
        assert breach["meta"]["reason_code"] == "DRAWDOWN_EQUITY_HISTORY_STALE_LATEST"
        assert breach["meta"]["drawdown_state"]["latest_age_s"] >= 120.0
    finally:
        con.close()


def test_empty_recent_equity_window_fails_closed_with_evidence(monkeypatch) -> None:
    kill_switch = _reload_live_equity_modules(monkeypatch)
    con = _con()
    try:
        con.execute("CREATE TABLE equity_history(ts_ms INTEGER PRIMARY KEY, equity REAL NOT NULL)")
        con.commit()

        snapshot = kill_switch.capital_equity_freshness_snapshot(con, live_mode=True)
        assert snapshot["ok"] is False
        assert snapshot["reason_code"] == "KILL_SWITCH_EQUITY_WINDOW_EMPTY"
        assert "latest:KILL_SWITCH_EQUITY_WINDOW_EMPTY" in snapshot["blockers"]
        assert snapshot["windows"]["latest"]["query_available"] is True
    finally:
        con.close()


def test_insufficient_recent_equity_points_fail_closed_in_capital_trigger(monkeypatch) -> None:
    kill_switch = _reload_live_equity_modules(
        monkeypatch,
        DRAWDOWN_MIN_HISTORY_POINTS="1",
        KILL_SWITCH_DAILY_EQUITY_MIN_POINTS="2",
        KILL_SWITCH_ROLLING_EQUITY_MIN_POINTS="2",
        KILL_SWITCH_VAR_MIN_HISTORY="1",
        KILL_SWITCH_VAR_EQUITY_MIN_POINTS="2",
    )
    now_ms = int(time.time() * 1000)
    con = _con()
    try:
        _create_equity_history(con, [(now_ms, 100_000.0)])

        breach = kill_switch._capital_risk_trigger(con)
        assert breach is not None
        assert breach["meta"]["trigger"] == "equity_availability"
        assert breach["meta"]["reason_code"] == "KILL_SWITCH_EQUITY_WINDOW_INSUFFICIENT_POINTS"
        freshness = breach["meta"]["equity_freshness"]
        assert "daily:KILL_SWITCH_EQUITY_WINDOW_INSUFFICIENT_POINTS" in freshness["blockers"]
        assert freshness["windows"]["daily"]["points"] == 1
    finally:
        con.close()


def test_equity_query_error_fails_closed_with_query_availability_evidence(monkeypatch) -> None:
    kill_switch = _reload_live_equity_modules(monkeypatch)

    class _Cursor:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return list(self._rows)

    class _BrokenEquityConnection:
        def execute(self, sql, params=()):
            text = str(sql).lower()
            if "sqlite_master" in text:
                return _Cursor([("equity_history",)])
            if "equity_history" in text:
                raise sqlite3.OperationalError("synthetic equity query failure")
            return _Cursor([])

    snapshot = kill_switch.capital_equity_freshness_snapshot(_BrokenEquityConnection(), live_mode=True)
    assert snapshot["ok"] is False
    assert snapshot["reason_code"] == "KILL_SWITCH_EQUITY_QUERY_ERROR"
    assert snapshot["windows"]["latest"]["query_available"] is False
    assert snapshot["windows"]["latest"]["error_type"] == "OperationalError"


def test_prod_preflight_surfaces_capital_equity_freshness_evidence(monkeypatch) -> None:
    kill_switch = _reload_live_equity_modules(monkeypatch)
    prod_preflight = importlib.reload(importlib.import_module("engine.runtime.prod_preflight"))
    snapshot = {
        "ok": False,
        "required": True,
        "reason_code": "KILL_SWITCH_EQUITY_WINDOW_EMPTY",
        "reason": "latest:KILL_SWITCH_EQUITY_WINDOW_EMPTY",
        "blockers": ["latest:KILL_SWITCH_EQUITY_WINDOW_EMPTY"],
        "windows": {
            "latest": {
                "ok": False,
                "reason_code": "KILL_SWITCH_EQUITY_WINDOW_EMPTY",
                "query_available": True,
            }
        },
    }

    monkeypatch.setattr(
        kill_switch,
        "capital_equity_freshness_snapshot",
        lambda *args, **kwargs: snapshot,
    )

    notes, warnings, errors, state = prod_preflight._capital_equity_freshness_gate()

    assert notes == []
    assert warnings == []
    assert errors == [
        "capital equity freshness invalid: "
        "KILL_SWITCH_EQUITY_WINDOW_EMPTY blockers=latest:KILL_SWITCH_EQUITY_WINDOW_EMPTY"
    ]
    assert state["windows"]["latest"]["reason_code"] == "KILL_SWITCH_EQUITY_WINDOW_EMPTY"


def test_valid_fresh_equity_satisfies_live_capital_availability(monkeypatch) -> None:
    kill_switch = _reload_live_equity_modules(monkeypatch)
    now_ms = int(time.time() * 1000)
    con = _con()
    try:
        _create_equity_history(con, _equity_rows(now_ms=now_ms, n=31))

        snapshot = kill_switch.capital_equity_freshness_snapshot(con, live_mode=True)
        assert snapshot["ok"] is True
        assert snapshot["windows"]["daily"]["points"] == 31
        assert snapshot["windows"]["rolling"]["points"] == 31
        assert snapshot["windows"]["var"]["points"] == 31

        breach = kill_switch._capital_risk_trigger(con)
        assert breach is None
    finally:
        con.close()


def test_db_read_error_stops_capital_guard(monkeypatch) -> None:
    drawdown_state = importlib.reload(importlib.import_module("engine.strategy.drawdown_state"))
    capital_guard = importlib.reload(importlib.import_module("engine.strategy.capital_guard"))

    class BrokenConnection:
        def execute(self, *_args, **_kwargs):
            raise sqlite3.OperationalError("synthetic equity read failure")

    diagnostic = drawdown_state.evaluate_current_drawdown(BrokenConnection())
    assert diagnostic.ok is False
    assert diagnostic.reason_code == "DRAWDOWN_EQUITY_HISTORY_READ_ERROR"
    assert diagnostic.error_type == "OperationalError"

    writes: list[tuple[str, str]] = []
    monkeypatch.setattr(capital_guard, "get_state", lambda _key, default="": "enabled")
    monkeypatch.setattr(capital_guard, "set_state", lambda key, value: writes.append((str(key), str(value))))

    assert capital_guard.trading_allowed(BrokenConnection()) is False
    assert ("trading_state", "stopped") in writes

    diag_json = next(value for key, value in writes if key == "capital_drawdown_diagnostic_json")
    diag_payload = json.loads(diag_json)
    assert diag_payload["reason_code"] == "DRAWDOWN_EQUITY_HISTORY_READ_ERROR"


def test_successful_sufficient_history_computes_drawdown_and_does_not_block(monkeypatch) -> None:
    drawdown_state = importlib.reload(importlib.import_module("engine.strategy.drawdown_state"))
    portfolio_risk_gate = importlib.reload(importlib.import_module("engine.strategy.portfolio_risk_gate"))
    portfolio_risk_engine = importlib.reload(importlib.import_module("engine.risk.portfolio_risk_engine"))
    con = _con()
    now_ms = int(time.time() * 1000)
    try:
        _create_equity_history(
            con,
            [
                (now_ms - 4_000, 100_000.0),
                (now_ms - 3_000, 101_000.0),
                (now_ms - 2_000, 102_000.0),
                (now_ms - 1_000, 103_000.0),
                (now_ms, 102_000.0),
            ],
        )

        diagnostic = drawdown_state.evaluate_current_drawdown(con, now_ms=now_ms)
        assert diagnostic.ok is True
        assert diagnostic.reason_code == "DRAWDOWN_OK"
        assert round(float(diagnostic.drawdown or 0.0), 6) == round(1.0 - (102_000.0 / 103_000.0), 6)

        out, info = portfolio_risk_gate.apply_portfolio_risk_gate(
            con,
            {"AAPL": {"side": "LONG", "weight": 0.10}},
            {},
            now_ms=now_ms,
        )
        assert info.get("blocked") is not True
        assert info["drawdown_state"]["reason_code"] == "DRAWDOWN_OK"
        assert out["AAPL"]["side"] == "LONG"

        monkeypatch.setattr(portfolio_risk_engine, "set_state", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(portfolio_risk_engine, "record_risk_block", lambda **_kwargs: None)
        _out, risk_info = portfolio_risk_engine.apply_portfolio_risk_engine(
            con,
            {},
            {},
            now_ms=now_ms,
        )
        assert risk_info["drawdown_state"]["reason_code"] == "DRAWDOWN_OK"
        assert risk_info["blocked"] is False
    finally:
        con.close()


def test_sparse_history_blocks_portfolio_risk_engine(monkeypatch) -> None:
    portfolio_risk_engine = importlib.reload(importlib.import_module("engine.risk.portfolio_risk_engine"))
    con = _con()
    try:
        _create_equity_history(con, [(1, 100_000.0), (2, 100_000.0)])
        monkeypatch.setattr(portfolio_risk_engine, "set_state", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(portfolio_risk_engine, "record_risk_block", lambda **_kwargs: None)

        _out, info = portfolio_risk_engine.apply_portfolio_risk_engine(
            con,
            {},
            {},
            now_ms=1_700_000_000_000,
        )

        assert info["blocked"] is True
        assert info["block_reason"]["type"] == "drawdown_state_unavailable"
        assert info["drawdown_state"]["reason_code"] == "DRAWDOWN_EQUITY_HISTORY_INSUFFICIENT"
    finally:
        con.close()
