from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_runtime(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    monkeypatch.setenv("USE_FUTURES_FEATURES", "1")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "futures_cot.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("TS_TESTING", "1")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_DB_ENABLED", "0")
    monkeypatch.setenv("SQLITE_LIVENESS_QUEUE_ENABLED", "0")
    monkeypatch.setenv("PRICE_READ_BACKEND", "sqlite")
    monkeypatch.setenv("TRADING_FAILURE_DIAGNOSTICS_PERSIST", "0")
    storage_sqlite = importlib.reload(importlib.import_module("engine.runtime.storage_sqlite"))
    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    storage.init_db()
    storage_sqlite.init_db()
    cftc_cot = importlib.reload(importlib.import_module("engine.data.cftc_cot"))
    feature_registry = importlib.reload(importlib.import_module("engine.strategy.feature_registry"))
    return storage, cftc_cot, feature_registry


def _insert_es_position(
    cftc_cot,
    con,
    *,
    report_day: str,
    commercial_long: float,
    commercial_short: float,
    noncommercial_long: float,
    noncommercial_short: float,
    open_interest: float,
) -> dict:
    spec = cftc_cot.CotContractSpec(
        "ES",
        "legacy",
        "E-MINI S&P 500",
        ("SPY", "ES.c.0"),
        "equity_index",
    )
    row = cftc_cot.normalize_cot_record(
        {
            "market_and_exchange_names": "E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE",
            "contract_market_name": "E-MINI S&P 500",
            "cftc_contract_market_code": "13874A",
            "report_date_as_yyyy_mm_dd": str(report_day),
            "open_interest_all": str(open_interest),
            "comm_positions_long_all": str(commercial_long),
            "comm_positions_short_all": str(commercial_short),
            "noncomm_positions_long_all": str(noncommercial_long),
            "noncomm_positions_short_all": str(noncommercial_short),
            "noncomm_postions_spread_all": "0",
        },
        spec=spec,
        ingested_ts_ms=cftc_cot.cot_release_ts_ms(report_day),
    )
    assert row is not None
    cftc_cot.put_cot_position(row, con=con)
    return dict(row)


def test_reanchored_futures_symbol_surfaces_cot_features(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    storage, cftc_cot, fr = _reload_runtime(monkeypatch, tmp_path)
    con = storage.connect()
    try:
        cftc_cot.ensure_cot_tables(con)
        cftc_cot.seed_default_cot_mappings(con)
        mappings = dict(cftc_cot.cot_target_contracts_for_symbol(con, "ES.c.0"))
        assert mappings["ES"] == 1.0
        first = _insert_es_position(
            cftc_cot,
            con,
            report_day="2025-12-16",
            commercial_long=100.0,
            commercial_short=250.0,
            noncommercial_long=250.0,
            noncommercial_short=100.0,
            open_interest=1000.0,
        )
        _insert_es_position(
            cftc_cot,
            con,
            report_day="2025-12-23",
            commercial_long=250.0,
            commercial_short=150.0,
            noncommercial_long=500.0,
            noncommercial_short=100.0,
            open_interest=1100.0,
        )
        last = _insert_es_position(
            cftc_cot,
            con,
            report_day="2025-12-30",
            commercial_long=500.0,
            commercial_short=100.0,
            noncommercial_long=950.0,
            noncommercial_short=100.0,
            open_interest=1250.0,
        )
        con.commit()
    finally:
        con.close()

    assert int(first["availability_ts_ms"]) < int(last["availability_ts_ms"])
    snap = fr.compute_feature_snapshot(
        event={
            "ts_ms": int(last["availability_ts_ms"]) + 1,
            "title": "positioning",
            "body": "",
            "source": "unit-test",
        },
        symbol="ES.c.0",
        feature_ids=list(fr.FUT_FEATURE_IDS),
    )

    assert list(snap.keys()) == list(fr.FUT_FEATURE_IDS)
    assert snap["fut.cot_commercial_net_pctile_3y"] > 0.0
    assert snap["fut.cot_open_interest_z"] > 0.0
    assert snap["fut.cot_noncomm_extreme_flag"] in {0.0, 1.0}
