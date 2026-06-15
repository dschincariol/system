from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


FIXTURE_13F_XML = """<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="http://www.sec.gov/edgar/document/thirteenf/informationtable">
  <infoTable>
    <nameOfIssuer>APPLE INC</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>037833100</cusip>
    <value>12345</value>
    <shrsOrPrnAmt>
      <sshPrnamt>100000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
    <investmentDiscretion>SOLE</investmentDiscretion>
    <votingAuthority>
      <Sole>100000</Sole>
      <Shared>0</Shared>
      <None>0</None>
    </votingAuthority>
  </infoTable>
  <infoTable>
    <nameOfIssuer>MICROSOFT CORP</nameOfIssuer>
    <titleOfClass>COM</titleOfClass>
    <cusip>594918104</cusip>
    <value>5000</value>
    <shrsOrPrnAmt>
      <sshPrnamt>20000</sshPrnamt>
      <sshPrnamtType>SH</sshPrnamtType>
    </shrsOrPrnAmt>
  </infoTable>
</informationTable>
"""


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _make_13f_db(inst_13f):
    con = sqlite3.connect(":memory:")
    inst_13f.ensure_13f_tables(con)
    return con


def _put_map(con, *, cusip: str, symbol: str) -> None:
    con.execute(
        """
        INSERT INTO inst_13f_cusip_symbol_map(cusip, symbol, source, confidence, updated_ts_ms, payload_json)
        VALUES (?, ?, 'unit', 1.0, 1, '{}')
        """,
        (cusip, symbol),
    )


def _insert_13f(inst_13f, con, *, manager: str, accession: str, report_date: str, available_ts: int, holdings):
    filing = {
        "manager_cik": manager,
        "manager_name": f"Manager {manager}",
        "accession": accession,
        "form": "13F-HR",
        "filing_date": "2026-02-15",
        "report_date": report_date,
        "report_ts_ms": inst_13f.date_to_ms(report_date),
        "acceptance_datetime": "",
        "acceptance_ts_ms": available_ts,
        "availability_ts_ms": available_ts,
        "source_record_id": f"filing:{manager}:{accession}",
    }
    filing_row, holding_rows = inst_13f.normalize_filing_with_holdings(
        filing,
        holdings,
        con=con,
        ingested_ts_ms=available_ts,
        info_table_url=f"https://example.test/{accession}.xml",
    )
    inst_13f.put_13f_filing(filing_row, con=con)
    for row in holding_rows:
        inst_13f.put_13f_holding(row, con=con)
    return filing_row, holding_rows


def test_13f_information_table_parser_fixture() -> None:
    (inst_13f,) = _reload("engine.data.inst_13f")

    rows = inst_13f.parse_13f_information_table(FIXTURE_13F_XML)

    assert len(rows) == 2
    assert rows[0]["issuer_name"] == "APPLE INC"
    assert rows[0]["cusip"] == "037833100"
    assert rows[0]["value_usd"] == 12_345_000.0
    assert rows[0]["shares"] == 100_000.0
    assert rows[1]["issuer_name"] == "MICROSOFT CORP"


def test_13f_turnover_screen() -> None:
    (inst_13f,) = _reload("engine.data.inst_13f")

    low = inst_13f.compute_turnover({"AAPL": 50.0, "MSFT": 50.0}, {"AAPL": 55.0, "MSFT": 45.0})
    high = inst_13f.compute_turnover({"AAPL": 100.0}, {"TSLA": 100.0})

    assert low < 0.25
    assert inst_13f.turnover_screen_passed(low, threshold=0.25) is True
    assert high == 1.0
    assert inst_13f.turnover_screen_passed(high, threshold=0.25) is False


def test_13f_trickle_availability_no_lookahead_through_snapshot(monkeypatch) -> None:
    monkeypatch.setenv("USE_13F_FEATURES", "1")
    _reload("engine.strategy.feature_registry")
    inst_13f, snapshots = _reload("engine.data.inst_13f", "engine.strategy.model_feature_snapshots")
    con = _make_13f_db(inst_13f)
    _put_map(con, cusip="037833100", symbol="AAPL")
    _put_map(con, cusip="594918104", symbol="MSFT")

    q0_avail = inst_13f.parse_ts_ms("2025-11-14T21:00:00+00:00")
    manager_a_avail = inst_13f.parse_ts_ms("2026-01-30T21:00:00+00:00")
    manager_b_avail = inst_13f.parse_ts_ms("2026-02-14T21:00:00+00:00")
    day_35 = inst_13f.parse_ts_ms("2026-02-04T12:00:00+00:00")
    day_46 = inst_13f.parse_ts_ms("2026-02-15T12:00:00+00:00")
    assert q0_avail and manager_a_avail and manager_b_avail and day_35 and day_46

    _insert_13f(
        inst_13f,
        con,
        manager="0000000001",
        accession="a-prev",
        report_date="2025-09-30",
        available_ts=q0_avail,
        holdings=[
            {"cusip": "037833100", "value_usd": 900.0, "shares": 9.0},
            {"cusip": "594918104", "value_usd": 100.0, "shares": 1.0},
        ],
    )
    _insert_13f(
        inst_13f,
        con,
        manager="0000000002",
        accession="b-prev",
        report_date="2025-09-30",
        available_ts=q0_avail,
        holdings=[
            {"cusip": "037833100", "value_usd": 800.0, "shares": 8.0},
            {"cusip": "594918104", "value_usd": 200.0, "shares": 2.0},
        ],
    )
    _insert_13f(
        inst_13f,
        con,
        manager="0000000001",
        accession="a-current",
        report_date="2025-12-31",
        available_ts=manager_a_avail,
        holdings=[
            {"cusip": "037833100", "value_usd": 920.0, "shares": 10.0},
            {"cusip": "594918104", "value_usd": 80.0, "shares": 1.0},
        ],
    )
    _insert_13f(
        inst_13f,
        con,
        manager="0000000002",
        accession="b-current",
        report_date="2025-12-31",
        available_ts=manager_b_avail,
        holdings=[
            {"cusip": "037833100", "value_usd": 810.0, "shares": 9.0},
            {"cusip": "594918104", "value_usd": 190.0, "shares": 2.0},
        ],
    )
    con.commit()
    ids = list(snapshots.INST_13F_FEATURE_IDS)

    before_b = snapshots.build_model_feature_snapshot(symbol="AAPL", ts_ms=day_35, feature_ids=ids, con=con)
    after_b = snapshots.build_model_feature_snapshot(symbol="AAPL", ts_ms=day_46, feature_ids=ids, con=con)

    assert before_b["features"]["13f_consensus_holders"] == 1.0
    assert before_b["source_timestamps"]["inst_13f"]["latest_availability_ts_ms"] == manager_a_avail
    assert after_b["features"]["13f_consensus_holders"] == 2.0
    assert after_b["source_timestamps"]["inst_13f"]["latest_availability_ts_ms"] == manager_b_avail
    assert after_b["features"]["13f_conviction_max"] > 0.80
    assert snapshots.summarize_model_feature_snapshots([before_b, after_b])["lookahead_violations"] == 0


def test_13f_registry_round_trip_job_registered_and_import_clean(monkeypatch) -> None:
    monkeypatch.setenv("USE_13F_FEATURES", "1")
    monkeypatch.setenv("INGEST_13F_ENABLED", "0")
    feature_registry, job_registry, _job = _reload(
        "engine.strategy.feature_registry",
        "engine.runtime.job_registry",
        "engine.data.jobs.ingest_13f",
    )

    ids = list(feature_registry.INST_13F_FEATURE_IDS)
    assert ids == [
        "13f_consensus_holders",
        "13f_conviction_max",
        "13f_new_position_flag",
        "13f_add_flag",
    ]
    assert feature_registry.FEATURE_GROUPS["inst_13f"] == ids
    assert feature_registry.resolve_feature_ids(model_spec={"feature_schema": {"feature_ids": ids}}) == ids
    assert feature_registry.expected_columns(ids, fallback_to_default=False) == ids
    assert "inst_13f" in feature_registry.feature_set_tag_from_ids(ids).split("+")
    assert job_registry.ALLOWED_JOBS["ingest_13f"][3]["cadence_seconds"] == 86400
