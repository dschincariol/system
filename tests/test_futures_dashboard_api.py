from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _dashboard_env(monkeypatch: pytest.MonkeyPatch, db_path: Path, canary: str = "") -> None:
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_ENABLED", "0")
    monkeypatch.setenv("FEATURE_STORE_INIT_ON_STARTUP", "0")
    monkeypatch.setenv("ENGINE_PRIMARY_BOOTSTRAP_DONE", "1")
    monkeypatch.setenv("DASHBOARD_ROUTE_CONTRACT_INTROSPECTION", "1")
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("OPERATOR_MODE", "safe")
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("DB_PATH", str(db_path))
    if canary:
        monkeypatch.setenv("DATABENTO_API_KEY", canary)
    for key in ("ENV", "NODE_ENV", "TS_ENV"):
        monkeypatch.delenv(key, raising=False)


def _reload_dashboard(monkeypatch: pytest.MonkeyPatch, db_path: Path, canary: str = ""):
    _dashboard_env(monkeypatch, db_path, canary)
    module = importlib.import_module("dashboard_server")
    return importlib.reload(module)


def _connect_factory(db_path: Path):
    def _connect():
        return sqlite3.connect(db_path)

    return _connect


def _seed_futures_db(db_path: Path) -> None:
    con = sqlite3.connect(db_path)
    try:
        con.executescript(
            """
            CREATE TABLE futures_roll_calendar (
                root TEXT, roll_ts_ms INTEGER, from_contract TEXT, to_contract TEXT,
                gap_ratio REAL, method TEXT, ingested_ts_ms INTEGER
            );
            CREATE TABLE futures_contract_bars (
                contract TEXT, ts_ms INTEGER, open REAL, high REAL, low REAL, close REAL,
                volume REAL, open_interest REAL, source TEXT
            );
            CREATE TABLE futures_continuous_bars (
                continuous_symbol TEXT, ts_ms INTEGER, adj_method TEXT, open REAL, high REAL,
                low REAL, close REAL, volume REAL, roll_flag INTEGER
            );
            CREATE TABLE futures_roll_yield (
                root TEXT, ts_ms INTEGER, roll_yield REAL
            );
            CREATE TABLE cot_symbol_features (
                symbol TEXT, asof_ts_ms INTEGER,
                cot_commercial_net_pctile_3y REAL, cot_noncomm_net_z REAL,
                cot_noncomm_extreme_flag REAL, cot_open_interest_z REAL,
                source_max_availability_ts_ms INTEGER, created_ts_ms INTEGER, meta_json TEXT
            );
            CREATE TABLE symbols (
                symbol TEXT PRIMARY KEY, asset_class TEXT, instrument_kind TEXT,
                fut_root TEXT, fut_exchange TEXT, fut_multiplier REAL, fut_tick_size REAL,
                fut_tick_value REAL, fut_price_ccy TEXT, fut_margin_ref REAL,
                fut_expiry_rule TEXT, fut_roll_method TEXT, fut_continuous_alias TEXT,
                session_calendar TEXT
            );
            CREATE TABLE broker_positions (
                symbol TEXT, qty REAL, avg_px REAL, updated_ts_ms INTEGER
            );
            """
        )
        con.execute(
            "INSERT INTO futures_roll_calendar VALUES (?,?,?,?,?,?,?)",
            ("ES", 1_800_000, "ESZ26", "ESH27", 1.0025, "oi_volume", 1_800_100),
        )
        con.execute(
            "INSERT INTO futures_contract_bars VALUES (?,?,?,?,?,?,?,?,?)",
            ("ESZ26", 1_800_000, 5000.0, 5010.0, 4995.0, 5005.0, 1000.0, 2000.0, "futures"),
        )
        con.execute(
            "INSERT INTO futures_continuous_bars VALUES (?,?,?,?,?,?,?,?,?)",
            ("ES.c.0", 1_800_000, "ratio", 4990.0, 5010.0, 4985.0, 5000.0, 1500.0, 0),
        )
        con.execute("INSERT INTO futures_roll_yield VALUES (?,?,?)", ("ES", 1_800_000, 0.042))
        con.execute(
            "INSERT INTO cot_symbol_features VALUES (?,?,?,?,?,?,?,?,?)",
            ("ES.c.0", 1_790_000, 0.75, 1.25, 1.0, 0.5, 1_789_000, 1_790_001, "{}"),
        )
        con.execute(
            "INSERT INTO symbols VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "ES.c.0",
                "FUTURES",
                "fut_continuous",
                "ES",
                "CME",
                50.0,
                0.25,
                12.5,
                "USD",
                1_000_000.0,
                "quarterly_index_cash_settlement",
                "oi_volume",
                "ES.c.0",
                "CME_EQUITY",
            ),
        )
        con.execute("INSERT INTO broker_positions VALUES (?,?,?,?)", ("ES.c.0", 2.0, 4990.0, 1_800_000))
        con.commit()
    finally:
        con.close()


def test_futures_dashboard_endpoint_returns_bounded_readonly_payload_without_token(monkeypatch, tmp_path):
    db_path = tmp_path / "futures_dashboard.db"
    canary = "DATABENTO_CANARY_TOKEN_DO_NOT_RENDER"
    _seed_futures_db(db_path)
    dashboard_server = _reload_dashboard(monkeypatch, db_path, canary)
    monkeypatch.setattr(dashboard_server, "_dashboard_db_connect", _connect_factory(db_path))

    payload = dashboard_server.api_get_futures_rolls(None)
    rendered = json.dumps(payload, sort_keys=True)

    assert payload["ok"] is True
    assert payload["read_only"] is True
    assert payload["shadow_only"] is True
    assert payload["roll_calendar"][0]["from_contract"] == "ESZ26"
    assert payload["term_structure"]
    assert payload["cot"][0]["symbol"] == "ES.c.0"
    assert payload["margin"][0]["symbol"] == "ES.c.0"
    assert payload["margin"][0]["position_qty"] == 2.0
    assert payload["margin"][0]["one_contract_notional"] == 250000.0
    assert canary not in rendered
    assert any(route.get("path") == "/api/data/futures/rolls" for route in dashboard_server.ROUTE_SPECS)


def test_futures_dashboard_endpoint_empty_data_is_bounded(monkeypatch, tmp_path):
    db_path = tmp_path / "futures_dashboard_empty.db"
    sqlite3.connect(db_path).close()
    dashboard_server = _reload_dashboard(monkeypatch, db_path)
    monkeypatch.setattr(dashboard_server, "_dashboard_db_connect", _connect_factory(db_path))

    payload = dashboard_server.api_get_futures_rolls(None)

    assert payload["ok"] is True
    assert payload["state"] == "empty"
    assert payload["roll_calendar"] == []
    assert payload["term_structure"] == []
    assert payload["cot"] == []
    assert payload["margin"] == []
    assert payload["warnings"]
