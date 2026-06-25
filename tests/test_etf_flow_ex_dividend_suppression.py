from __future__ import annotations

import sqlite3

from engine.data import corporate_actions as ca
from engine.data import etf_flows


def _readings():
    return [
        {
            "symbol": "SPY",
            "asof_date": "2026-01-01",
            "asof_ts_ms": etf_flows.date_to_ms("2026-01-01"),
            "availability_ts_ms": etf_flows.availability_ts_ms_for_date("2026-01-01"),
            "shares_outstanding": 100.0,
            "price": 100.0,
        },
        {
            "symbol": "SPY",
            "asof_date": "2026-01-02",
            "asof_ts_ms": etf_flows.date_to_ms("2026-01-02"),
            "availability_ts_ms": etf_flows.availability_ts_ms_for_date("2026-01-02"),
            "shares_outstanding": 110.0,
            "price": 90.0,
        },
    ]


def _corp_action_con(*, with_dividend: bool) -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    ca.ensure_corporate_actions_tables(con)
    if with_dividend:
        row = ca.normalize_corporate_action(
            {
                "ticker": "SPY",
                "cash_amount": 10.0,
                "ex_dividend_date": "2026-01-02",
                "pay_date": "2026-01-15",
                "declaration_date": "2025-12-15",
            },
            source="polygon_dividends",
            ingested_ts_ms=ca.date_to_ms("2026-01-02"),
        )[0]
        row["availability_ts_ms"] = ca.date_to_ms("2026-01-01")
        ca.put_corporate_action_row(row, con=con)
    return con


def test_ex_dividend_suppresses_unexpected_flow_when_connection_is_supplied() -> None:
    rows = _readings()
    asof = etf_flows.availability_ts_ms_for_date("2026-01-02") + 1
    no_con_output = etf_flows.compute_flow_features(rows, asof_ts_ms=asof)
    no_row_output = etf_flows.compute_flow_features(rows, asof_ts_ms=asof, con=_corp_action_con(with_dividend=False))

    assert no_row_output == no_con_output
    assert no_con_output[0]["etf_unexpected_flow_z"] > 0.0

    features, meta, available = etf_flows.compute_flow_features(rows, asof_ts_ms=asof, con=_corp_action_con(with_dividend=True))

    assert available is True
    assert features["etf_unexpected_flow_z"] == 0.0
    assert features["etf_flow_reversal_flag"] == 0.0
    assert meta["latest_unexpected_flow"] == 0.0
    assert meta["ex_dividend_suppressions"] >= 1
