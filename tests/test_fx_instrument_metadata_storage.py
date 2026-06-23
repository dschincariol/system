from __future__ import annotations

import importlib


def _reload_storage_sqlite(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "fx_instrument_metadata.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TS_TESTING", "1")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_DB_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_QUEUE_ENABLED", "0")
    return importlib.reload(importlib.import_module("engine.runtime.storage_sqlite"))


def test_fx_instrument_metadata_persists_with_sqlite_affinity(monkeypatch, tmp_path) -> None:
    storage_sqlite = _reload_storage_sqlite(monkeypatch, tmp_path)
    storage_sqlite.init_db()

    import engine.data.universe as universe

    universe = importlib.reload(universe)
    con = storage_sqlite.connect_rw_direct()
    try:
        universe.upsert_symbol(con, "EURUSD", status="ACTIVE", score_delta=1.0)
        con.commit()

        row = con.execute(
            """
            SELECT symbol, asset_class, instrument_kind, base_ccy, quote_ccy,
                   pip_size, contract_size, pnl_ccy, leverage_cap,
                   session_calendar, instrument_meta_source
            FROM symbols
            WHERE symbol='EURUSD'
            """
        ).fetchone()

        assert row is not None
        assert row[0] == "EURUSD"
        assert row[1] == "FX"
        assert row[2] == "fx_spot"
        assert row[3] == "EUR"
        assert row[4] == "USD"
        assert isinstance(row[5], float)
        assert row[5] == 0.0001
        assert isinstance(row[6], float)
        assert row[6] == 100000.0
        assert isinstance(row[7], str)
        assert row[7] == "USD"
        assert isinstance(row[8], float)
        assert row[8] == 20.0
        assert row[9] == "FX_24x5"
        assert row[10] == "parser"

        table_info = {item[1]: item[2].upper() for item in con.execute("PRAGMA table_info(symbols)").fetchall()}
        assert table_info["pip_size"] == "REAL"
        assert table_info["contract_size"] == "REAL"
        assert table_info["leverage_cap"] == "REAL"
        assert table_info["pnl_ccy"] == "TEXT"

        metadata = universe.get_instrument_metadata(con, "EUR/USD")
        assert metadata is not None
        assert metadata["symbol"] == "EURUSD"
        assert metadata["base_ccy"] == "EUR"
        assert metadata["quote_ccy"] == "USD"
        assert metadata["pip_size"] == 0.0001
        assert metadata["pnl_ccy"] == "USD"

        snapshot = universe.get_universe_snapshot(con)
        assert any(item["symbol"] == "EURUSD" for item in snapshot)
    finally:
        con.close()


def test_non_fx_symbol_leaves_instrument_columns_null(monkeypatch, tmp_path) -> None:
    storage_sqlite = _reload_storage_sqlite(monkeypatch, tmp_path)
    storage_sqlite.init_db()

    import engine.data.universe as universe

    universe = importlib.reload(universe)
    con = storage_sqlite.connect_rw_direct()
    try:
        universe.upsert_symbol(con, "SPY")
        con.commit()

        row = con.execute(
            """
            SELECT instrument_kind, base_ccy, quote_ccy, pip_size,
                   contract_size, pnl_ccy, leverage_cap, session_calendar,
                   instrument_meta_source
            FROM symbols
            WHERE symbol='SPY'
            """
        ).fetchone()

        assert row is not None
        assert tuple(row) == (None, None, None, None, None, None, None, None, None)
        assert universe.get_instrument_metadata(con, "SPY") is None
    finally:
        con.close()


def test_accessor_falls_back_to_parser_without_persisted_row(monkeypatch, tmp_path) -> None:
    storage_sqlite = _reload_storage_sqlite(monkeypatch, tmp_path)
    storage_sqlite.init_db()

    import engine.data.universe as universe

    universe = importlib.reload(universe)
    con = storage_sqlite.connect_rw_direct()
    try:
        metadata = universe.get_instrument_metadata(con, "usd_jpy")
        assert metadata is not None
        assert metadata["symbol"] == "USDJPY"
        assert metadata["quote_ccy"] == "JPY"
        assert metadata["pip_size"] == 0.01
    finally:
        con.close()
