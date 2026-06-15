from __future__ import annotations

import importlib
import sqlite3
import sys
from datetime import timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _make_cot_db(cftc_cot):
    con = sqlite3.connect(":memory:")
    cftc_cot.ensure_cot_tables(con)
    cftc_cot.seed_default_cot_mappings(con)
    return con


def _spec(cftc_cot):
    return cftc_cot.CotContractSpec("ES", "legacy", "E-MINI S&P 500", ("SPY",), "equity_index")


def _insert_position(
    cftc_cot,
    con,
    *,
    report_day: str,
    commercial_long: float,
    commercial_short: float,
    noncommercial_long: float,
    noncommercial_short: float,
    open_interest: float = 1000.0,
):
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
        spec=_spec(cftc_cot),
        ingested_ts_ms=cftc_cot.cot_release_ts_ms(report_day),
    )
    assert row is not None
    cftc_cot.put_cot_position(row, con=con)
    return row


def test_cot_release_lag_no_lookahead_through_snapshot(monkeypatch) -> None:
    monkeypatch.setenv("USE_COT_FEATURES", "1")
    _reload("engine.strategy.feature_registry")
    cftc_cot, snapshots = _reload("engine.data.cftc_cot", "engine.strategy.model_feature_snapshots")
    con = _make_cot_db(cftc_cot)

    first = _insert_position(
        cftc_cot,
        con,
        report_day="2025-12-30",
        commercial_long=100.0,
        commercial_short=250.0,
        noncommercial_long=250.0,
        noncommercial_short=100.0,
        open_interest=1000.0,
    )
    second = _insert_position(
        cftc_cot,
        con,
        report_day="2026-01-06",
        commercial_long=500.0,
        commercial_short=100.0,
        noncommercial_long=900.0,
        noncommercial_short=100.0,
        open_interest=1000.0,
    )
    con.commit()
    ids = list(snapshots.COT_FEATURE_IDS)

    before = snapshots.build_model_feature_snapshot(
        symbol="SPY",
        ts_ms=int(second["availability_ts_ms"]) - 1,
        feature_ids=ids,
        con=con,
    )
    after = snapshots.build_model_feature_snapshot(
        symbol="SPY",
        ts_ms=int(second["availability_ts_ms"]) + 1,
        feature_ids=ids,
        con=con,
    )

    assert before["source_timestamps"]["cot"]["latest_availability_ts_ms"] == first["availability_ts_ms"]
    assert before["features"]["cot_noncomm_extreme_flag"] == 0.0
    assert after["source_timestamps"]["cot"]["latest_availability_ts_ms"] == second["availability_ts_ms"]
    assert after["features"]["cot_noncomm_extreme_flag"] == 1.0
    assert after["features"]["cot_commercial_net_pctile_3y"] > before["features"]["cot_commercial_net_pctile_3y"]
    assert snapshots.summarize_model_feature_snapshots([before, after])["lookahead_violations"] == 0


def test_cot_percentile_and_z_math() -> None:
    (cftc_cot,) = _reload("engine.data.cftc_cot")
    rows = []
    report_day = cftc_cot.parse_date("2025-01-07")
    for idx in range(10):
        day = report_day + timedelta(days=7 * idx)
        oi = 1000.0 + idx * 25.0
        rows.append(
            {
                "contract_key": "ES",
                "report_type": "legacy",
                "report_date": day.isoformat(),
                "report_ts_ms": cftc_cot.date_to_ms(day),
                "availability_ts_ms": cftc_cot.cot_release_ts_ms(day),
                "open_interest": oi,
                "commercial_long": 100.0 + idx * 100.0,
                "commercial_short": 100.0,
                "noncommercial_long": 1000.0 - idx * 80.0,
                "noncommercial_short": 100.0,
                "noncommercial_spread": 0.0,
            }
        )

    features, meta, available = cftc_cot.compute_cot_contract_features(
        rows,
        asof_ts_ms=int(rows[-1]["availability_ts_ms"]),
    )

    assert available is True
    assert meta["rows"] == 10
    assert features["cot_commercial_net_pctile_3y"] > 0.90
    assert features["cot_noncomm_extreme_flag"] == 1.0
    assert features["cot_noncomm_net_z"] < 0.0
    assert features["cot_open_interest_z"] > 0.0


def test_cot_api_failure_graceful(monkeypatch) -> None:
    (cftc_cot,) = _reload("engine.data.cftc_cot")

    def _raise(*_args, **_kwargs):
        raise RuntimeError("socrata unavailable")

    monkeypatch.setattr(cftc_cot.requests, "get", _raise)
    rows, errors = cftc_cot.fetch_cot_records(specs=[_spec(cftc_cot)], lookback_weeks=1, limit=1)

    assert rows == []
    assert errors
    assert "socrata unavailable" in errors[0]


def test_cot_mapping_config_table() -> None:
    (cftc_cot,) = _reload("engine.data.cftc_cot")
    con = _make_cot_db(cftc_cot)

    spy_mappings = dict(cftc_cot.cot_target_contracts_for_symbol(con, "SPY"))
    assert spy_mappings["ES"] == 1.0

    con.execute(
        """
        INSERT INTO cot_contract_symbol_map(contract_key, symbol, topic, weight, active, updated_ts_ms, meta_json)
        VALUES (?, ?, ?, ?, 1, ?, ?)
        """,
        ("XYZ", "AAPL", "custom", 0.5, 123, "{}"),
    )
    assert cftc_cot.cot_target_contracts_for_symbol(con, "AAPL") == [("XYZ", 0.5)]


def test_cot_registry_round_trip_job_registered_and_regime_macro(monkeypatch) -> None:
    monkeypatch.setenv("USE_COT_FEATURES", "1")
    monkeypatch.setenv("INGEST_CFTC_COT_ENABLED", "0")
    feature_registry, job_registry, _job, cftc_cot, regime_stack = _reload(
        "engine.strategy.feature_registry",
        "engine.runtime.job_registry",
        "engine.data.jobs.ingest_cftc_cot",
        "engine.data.cftc_cot",
        "engine.strategy.regime_stack",
    )

    ids = list(feature_registry.COT_FEATURE_IDS)
    assert ids == [
        "cot_commercial_net_pctile_3y",
        "cot_noncomm_net_z",
        "cot_noncomm_extreme_flag",
        "cot_open_interest_z",
    ]
    assert feature_registry.FEATURE_GROUPS["cot"] == ids
    assert feature_registry.resolve_feature_ids(model_spec={"feature_schema": {"feature_ids": ids}}) == ids
    assert feature_registry.expected_columns(ids, fallback_to_default=False) == ids
    assert "cot" in feature_registry.feature_set_tag_from_ids(ids).split("+")
    assert job_registry.ALLOWED_JOBS["ingest_cftc_cot"][3]["cadence_seconds"] == 86400

    con = _make_cot_db(cftc_cot)
    con.execute("CREATE TABLE prices(symbol TEXT, ts_ms BIGINT, px DOUBLE PRECISION, price DOUBLE PRECISION)")
    con.execute("CREATE TABLE execution_fills(fill_ts_ms BIGINT, liquidity TEXT)")
    con.execute("CREATE TABLE execution_capital_efficiency(ts_ms BIGINT, drawdown_contrib DOUBLE PRECISION)")
    first = _insert_position(
        cftc_cot,
        con,
        report_day="2026-01-06",
        commercial_long=100.0,
        commercial_short=250.0,
        noncommercial_long=250.0,
        noncommercial_short=100.0,
    )
    second = _insert_position(
        cftc_cot,
        con,
        report_day="2026-01-13",
        commercial_long=500.0,
        commercial_short=100.0,
        noncommercial_long=900.0,
        noncommercial_short=100.0,
    )
    con.commit()

    result = regime_stack.compute_regime_vector(
        symbol="SPY",
        ts_ms=int(second["availability_ts_ms"]) + 1,
        con=con,
        include_hmm=False,
    )
    assert result["macro"]["cot_noncomm_extreme_flag"] == 1.0
    assert result["macro"]["cot_positioning_extreme"] == 1.0

    monkeypatch.setenv("USE_COT_FEATURES", "0")
    result_off = regime_stack.compute_regime_vector(
        symbol="SPY",
        ts_ms=int(first["availability_ts_ms"]) + 1,
        con=con,
        include_hmm=False,
    )
    assert "cot_noncomm_extreme_flag" not in result_off["macro"]
