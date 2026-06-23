from __future__ import annotations

import importlib
import sqlite3
from datetime import date


def test_fx_cot_contract_specs_cover_major_fx_futures() -> None:
    import engine.data.cftc_cot as cftc_cot

    mod = importlib.reload(cftc_cot)
    specs = {spec.contract_key: spec for spec in mod.DEFAULT_COT_CONTRACT_SPECS}

    expected = {
        "6E": "EURUSD",
        "6B": "GBPUSD",
        "6J": "USDJPY",
        "6S": "USDCHF",
        "6C": "USDCAD",
        "6A": "AUDUSD",
        "6N": "NZDUSD",
    }
    for contract_key, pair_symbol in expected.items():
        spec = specs[contract_key]
        assert spec.topic == "fx"
        assert pair_symbol in spec.symbols


def test_seed_default_cot_mappings_inserts_fx_pair_mappings() -> None:
    import engine.data.cftc_cot as cftc_cot

    mod = importlib.reload(cftc_cot)
    con = sqlite3.connect(":memory:")
    mod.ensure_cot_tables(con)
    mod.seed_default_cot_mappings(con)

    rows = con.execute(
        """
        SELECT contract_key, symbol, topic
        FROM cot_contract_symbol_map
        WHERE topic = 'fx'
        """
    ).fetchall()
    mappings = {(str(contract), str(symbol)): str(topic) for contract, symbol, topic in rows}

    assert mappings[("6E", "EURUSD")] == "fx"
    assert mappings[("6J", "USDJPY")] == "fx"
    assert mappings[("6N", "NZDUSD")] == "fx"


def test_yen_cot_where_clause_uses_japanese_yen_market_name() -> None:
    import engine.data.cftc_cot as cftc_cot

    mod = importlib.reload(cftc_cot)
    yen = next(spec for spec in mod.DEFAULT_COT_CONTRACT_SPECS if spec.contract_key == "6J")

    where = mod._where_for_spec(yen, since_day=date(2026, 1, 1))

    assert "JAPANESE YEN" in where
    assert "2026-01-01" in where
