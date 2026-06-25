from __future__ import annotations

import importlib
import sqlite3

import pytest


def _engine(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("PORTFOLIO_RISK_USE_MONTE_CARLO", "0")

    import engine.risk.portfolio_risk_engine as portfolio_risk_engine

    return importlib.reload(portfolio_risk_engine)


def test_buying_power_reference_noops_for_missing_and_broker_sim_schemas(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _engine(monkeypatch)
    con = sqlite3.connect(":memory:")

    assert engine._buying_power_reference(con) == (None, "unavailable")

    con.execute(
        """
        CREATE TABLE broker_account (
            id INTEGER PRIMARY KEY,
            cash REAL,
            equity REAL,
            updated_ts_ms INTEGER
        )
        """
    )
    con.execute(
        "INSERT INTO broker_account(id, cash, equity, updated_ts_ms) VALUES (1, 90000, 100000, 10)"
    )

    assert engine._buying_power_reference(con) == (None, "unavailable")


def test_buying_power_reference_reads_id_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _engine(monkeypatch)
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE broker_account (
            id INTEGER PRIMARY KEY,
            equity REAL,
            buying_power REAL
        )
        """
    )
    con.execute("INSERT INTO broker_account(id, equity, buying_power) VALUES (1, 100000, 200000)")

    buying_power, source = engine._buying_power_reference(con)

    assert buying_power == pytest.approx(200_000.0)
    assert source == "broker_account:id=1"


def test_buying_power_reference_reads_latest_ts_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _engine(monkeypatch)
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE broker_account (
            ts_ms INTEGER,
            equity REAL,
            buying_power REAL
        )
        """
    )
    con.executemany(
        "INSERT INTO broker_account(ts_ms, equity, buying_power) VALUES (?, ?, ?)",
        [(10, 100_000.0, 150_000.0), (20, 100_000.0, 250_000.0)],
    )

    buying_power, source = engine._buying_power_reference(con)

    assert buying_power == pytest.approx(250_000.0)
    assert source == "broker_account:ts_ms"
