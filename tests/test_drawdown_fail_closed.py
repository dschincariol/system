from __future__ import annotations

import importlib
import json
import sqlite3


def _con() -> sqlite3.Connection:
    return sqlite3.connect(":memory:")


def _create_equity_history(con: sqlite3.Connection, rows: list[tuple[int, float]]) -> None:
    con.execute("CREATE TABLE equity_history(ts_ms INTEGER PRIMARY KEY, equity REAL NOT NULL)")
    con.executemany("INSERT INTO equity_history(ts_ms, equity) VALUES (?, ?)", rows)
    con.commit()


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
    try:
        _create_equity_history(
            con,
            [
                (1, 100_000.0),
                (2, 101_000.0),
                (3, 102_000.0),
                (4, 103_000.0),
                (5, 102_000.0),
            ],
        )

        diagnostic = drawdown_state.evaluate_current_drawdown(con)
        assert diagnostic.ok is True
        assert diagnostic.reason_code == "DRAWDOWN_OK"
        assert round(float(diagnostic.drawdown or 0.0), 6) == round(1.0 - (102_000.0 / 103_000.0), 6)

        out, info = portfolio_risk_gate.apply_portfolio_risk_gate(
            con,
            {"AAPL": {"side": "LONG", "weight": 0.10}},
            {},
            now_ms=1_700_000_000_000,
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
            now_ms=1_700_000_000_000,
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
