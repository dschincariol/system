from __future__ import annotations

import importlib
import math
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _NoClose:
    def __init__(self, con):
        self._con = con

    def execute(self, *args, **kwargs):
        return self._con.execute(*args, **kwargs)

    def close(self):
        return None


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _make_etf_db(etf_flows):
    con = sqlite3.connect(":memory:")
    etf_flows.ensure_etf_flow_tables(con)
    return con


def _insert_share(etf_flows, con, *, symbol: str, day: str, shares: float, price: float):
    row = etf_flows.normalize_share_reading(
        symbol=symbol,
        asof_day=day,
        shares_outstanding=shares,
        source="unit",
        price=price,
        payload_json={"source": "unit"},
        diagnostics_json={"availability_rule": "next_morning_09_30_et"},
        ingested_ts_ms=etf_flows.availability_ts_ms_for_date(day),
    )
    etf_flows.put_etf_shares_outstanding(row, con=con)
    return row


def test_etf_flow_math_handles_splits_and_normal_flow() -> None:
    (etf_flows,) = _reload("engine.data.etf_flows")

    split_delta, split_flag = etf_flows.split_adjusted_share_delta(
        prev_shares=100.0,
        curr_shares=200.0,
        prev_price=100.0,
        curr_price=50.0,
    )
    normal_delta, normal_flag = etf_flows.split_adjusted_share_delta(
        prev_shares=100.0,
        curr_shares=110.0,
        prev_price=100.0,
        curr_price=101.0,
    )

    assert split_delta == 0.0
    assert split_flag is True
    assert normal_delta == 10.0
    assert normal_flag is False

    rows = [
        {
            "asof_date": "2026-01-01",
            "asof_ts_ms": etf_flows.date_to_ms("2026-01-01"),
            "availability_ts_ms": etf_flows.availability_ts_ms_for_date("2026-01-01"),
            "shares_outstanding": 100.0,
            "price": 100.0,
        },
        {
            "asof_date": "2026-01-02",
            "asof_ts_ms": etf_flows.date_to_ms("2026-01-02"),
            "availability_ts_ms": etf_flows.availability_ts_ms_for_date("2026-01-02"),
            "shares_outstanding": 110.0,
            "price": 101.0,
        },
    ]
    features, meta, available = etf_flows.compute_flow_features(
        rows,
        asof_ts_ms=etf_flows.availability_ts_ms_for_date("2026-01-02") + 1,
    )

    assert available is True
    assert meta["latest_flow_dollars"] == 1010.0
    assert meta["latest_unexpected_flow"] == 1010.0
    assert math.isclose(features["etf_unexpected_flow_z"], 1010.0 / (110.0 * 101.0))


def test_etf_flow_availability_no_lookahead_through_snapshot(monkeypatch) -> None:
    monkeypatch.setenv("USE_ETF_FLOW_FEATURES", "1")
    _reload("engine.strategy.feature_registry")
    etf_flows, snapshots = _reload("engine.data.etf_flows", "engine.strategy.model_feature_snapshots")
    con = _make_etf_db(etf_flows)
    first = _insert_share(etf_flows, con, symbol="SPY", day="2026-01-01", shares=100.0, price=100.0)
    second = _insert_share(etf_flows, con, symbol="SPY", day="2026-01-02", shares=110.0, price=100.0)
    con.commit()
    ids = list(snapshots.ETF_FLOW_FEATURE_IDS)

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
    non_etf = snapshots.build_model_feature_snapshot(
        symbol="AAPL",
        ts_ms=int(second["availability_ts_ms"]) + 1,
        feature_ids=ids,
        con=con,
    )

    assert before["source_timestamps"]["etf_flow"]["latest_availability_ts_ms"] == first["availability_ts_ms"]
    assert before["source_timestamps"]["etf_flow"]["latest_asof_date"] == "2026-01-01"
    assert before["features"] == {fid: 0.0 for fid in ids}
    assert after["source_timestamps"]["etf_flow"]["latest_availability_ts_ms"] == second["availability_ts_ms"]
    assert after["source_timestamps"]["etf_flow"]["latest_asof_date"] == "2026-01-02"
    assert after["features"]["etf_unexpected_flow_z"] > 0.0
    assert non_etf["features"] == {fid: 0.0 for fid in ids}
    assert snapshots.summarize_model_feature_snapshots([before, after, non_etf])["lookahead_violations"] == 0


def test_etf_flow_provider_fallback_and_failure_degrades(monkeypatch) -> None:
    (etf_flows,) = _reload("engine.data.etf_flows")

    def _polygon_fail(_symbol):
        raise RuntimeError("polygon unavailable")

    monkeypatch.setattr(etf_flows, "fetch_polygon_shares_outstanding", _polygon_fail)
    monkeypatch.setattr(etf_flows, "fetch_fmp_profile_shares", lambda _symbol: (123.0, {"provider": "fmp"}))
    shares, source, payload = etf_flows.fetch_shares_outstanding("SPY", provider_order=("polygon", "fmp"))
    assert shares == 123.0
    assert source == "fmp"
    assert payload == {"provider": "fmp"}

    con = _make_etf_db(etf_flows)
    monkeypatch.setattr(etf_flows, "connect", lambda: _NoClose(con))
    monkeypatch.setattr(
        etf_flows,
        "fetch_shares_outstanding",
        lambda _symbol: (None, None, {"errors": ["polygon unavailable", "fmp unavailable"]}),
    )

    summary = etf_flows.ingest_etf_shares_batch(symbols=["SPY"], now_ms=etf_flows.availability_ts_ms_for_date("2026-01-02"))

    assert summary["ok"] is False
    assert summary["rows"] == 0
    assert summary["errors"]


def test_etf_flow_registry_round_trip_job_registered_and_import_clean(monkeypatch) -> None:
    monkeypatch.setenv("USE_ETF_FLOW_FEATURES", "1")
    monkeypatch.setenv("INGEST_ETF_FLOW_ENABLED", "0")
    feature_registry, job_registry, _job = _reload(
        "engine.strategy.feature_registry",
        "engine.runtime.job_registry",
        "engine.data.jobs.ingest_etf_flows",
    )

    ids = list(feature_registry.ETF_FLOW_FEATURE_IDS)
    assert ids == ["etf_unexpected_flow_z", "etf_flow_3d_sum_z", "etf_flow_reversal_flag"]
    assert feature_registry.FEATURE_GROUPS["etf_flow"] == ids
    assert feature_registry.resolve_feature_ids(model_spec={"feature_schema": {"feature_ids": ids}}) == ids
    assert feature_registry.expected_columns(ids, fallback_to_default=False) == ids
    assert "etf_flow" in feature_registry.feature_set_tag_from_ids(ids).split("+")
    assert job_registry.ALLOWED_JOBS["ingest_etf_flows"][3]["cadence_seconds"] == 86400
