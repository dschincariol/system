from __future__ import annotations

import importlib
import json
import sqlite3


def _write_sec_registry(path):
    payload = {
        "fields": ["cik", "name", "ticker", "exchange"],
        "data": [
            [1, "NVIDIA CORP", "NVDA", "Nasdaq"],
            [2, "OTC CORP", "OTCZZ", "OTC"],
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _reload_universe():
    import engine.data.asset_map as asset_map
    import engine.data.universe as universe

    importlib.reload(asset_map)
    return importlib.reload(universe)


def _init_symbols(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE symbols(
          symbol TEXT PRIMARY KEY,
          asset_class TEXT,
          status TEXT,
          score REAL,
          last_seen_event_ts_ms INTEGER,
          meta_json TEXT,
          created_ts_ms INTEGER,
          updated_ts_ms INTEGER
        )
        """
    )


def test_upsert_symbol_persists_registry_equity_asset_class(monkeypatch, tmp_path) -> None:
    registry_path = tmp_path / "sec_company_tickers_exchange.json"
    _write_sec_registry(registry_path)
    monkeypatch.setenv("SEC_TICKER_MAP_CACHE", str(registry_path))
    monkeypatch.delenv("ASSET_CLASS_MAP_JSON", raising=False)
    monkeypatch.delenv("ASSET_MAP_USE_EQUITY_REGISTRY", raising=False)
    universe = _reload_universe()

    con = sqlite3.connect(":memory:")
    try:
        _init_symbols(con)
        universe.upsert_symbol(con, "NVDA")
        universe.upsert_symbol(con, "OTCZZ")
        con.commit()

        rows = dict(con.execute("SELECT symbol, asset_class FROM symbols").fetchall())
    finally:
        con.close()

    assert rows["NVDA"] == "EQUITY"
    assert rows["OTCZZ"] == "UNKNOWN"
