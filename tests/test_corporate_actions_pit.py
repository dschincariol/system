from __future__ import annotations

import json
import importlib
import sqlite3
import uuid

import pytest

from engine.data import corporate_actions as ca


def _con() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    ca.ensure_corporate_actions_tables(con)
    return con


def _dividend_row(symbol: str = "SPY", *, availability_ts_ms: int | None = None) -> dict:
    row = ca.normalize_corporate_action(
        {
            "ticker": symbol,
            "cash_amount": 1.0,
            "ex_dividend_date": "2026-01-02",
            "pay_date": "2026-01-15",
            "declaration_date": "2025-12-15",
            "currency": "USD",
        },
        source="polygon_dividends",
        ingested_ts_ms=ca.date_to_ms("2026-01-03"),
    )[0]
    if availability_ts_ms is not None:
        row["availability_ts_ms"] = int(availability_ts_ms)
    return row


def test_corporate_action_insert_is_idempotent() -> None:
    con = _con()
    row = _dividend_row()

    assert ca.put_corporate_action_row(row, con=con) == 1
    assert ca.put_corporate_action_row(row, con=con) == 0
    assert con.execute("SELECT COUNT(*) FROM corporate_actions").fetchone()[0] == 1


def test_normalize_polygon_and_fmp_dividends_and_splits() -> None:
    rows = []
    rows.extend(
        ca.normalize_corporate_action(
            {"ticker": "SPY", "cash_amount": 0.5, "ex_dividend_date": "2026-01-02", "pay_date": "2026-01-15"},
            source="polygon_dividends",
            ingested_ts_ms=1,
        )
    )
    rows.extend(
        ca.normalize_corporate_action(
            {"ticker": "SPY", "split_from": 1, "split_to": 2, "execution_date": "2026-02-03"},
            source="polygon_splits",
            ingested_ts_ms=1,
        )
    )
    rows.extend(
        ca.normalize_corporate_action(
            {"symbol": "SPY", "dividend": 0.75, "date": "2026-03-04"},
            source="fmp_dividend",
            ingested_ts_ms=1,
        )
    )
    rows.extend(
        ca.normalize_corporate_action(
            {"symbol": "SPY", "numerator": 3, "denominator": 2, "date": "2026-04-05"},
            source="fmp_split",
            ingested_ts_ms=1,
        )
    )

    assert [row["action_type"] for row in rows] == ["dividend", "split", "dividend", "split"]
    assert rows[0]["cash_amount"] == pytest.approx(0.5)
    assert rows[1]["split_from"] == pytest.approx(1.0)
    assert rows[1]["split_to"] == pytest.approx(2.0)
    assert rows[3]["split_from"] == pytest.approx(2.0)
    assert rows[3]["split_to"] == pytest.approx(3.0)


def test_total_return_factor_is_pit_gated_and_malformed_split_fails_closed() -> None:
    con = _con()
    start = ca.date_to_ms("2026-01-01")
    end = ca.date_to_ms("2026-01-03")
    late = _dividend_row(availability_ts_ms=ca.date_to_ms("2026-01-03"))
    ca.put_corporate_action_row(late, con=con)

    factor, meta = ca.corporate_action_total_return_factor(
        con,
        symbol="SPY",
        start_ts_ms=start,
        end_ts_ms=end,
        entry_px=100.0,
    )
    assert factor == 1.0
    assert meta["reason"] == "no_corporate_action"

    malformed = {
        **ca.normalize_corporate_action(
            {"ticker": "SPY", "split_from": 1, "split_to": 2, "execution_date": "2026-01-02"},
            source="polygon_splits",
            ingested_ts_ms=start,
        )[0],
        "source_record_id": "corp_action:malformed_split",
        "split_to": None,
        "availability_ts_ms": start,
    }
    ca.put_corporate_action_row(malformed, con=con)
    factor, meta = ca.corporate_action_total_return_factor(
        con,
        symbol="SPY",
        start_ts_ms=start,
        end_ts_ms=end,
        entry_px=100.0,
    )
    assert factor == 1.0
    assert meta["reason"] == "corp_action_unparseable"


def test_provider_fetches_are_mocked_and_do_not_leak_api_key(monkeypatch) -> None:
    canary = f"canary-{uuid.uuid4()}"
    seen_params = []

    class _Response:
        def __init__(self, payload):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    def _fake_get(url, params, timeout):
        seen_params.append(dict(params))
        if "splits" in url:
            return _Response({"results": [{"ticker": "SPY", "split_from": 1, "split_to": 2, "execution_date": "2026-01-02"}]})
        return _Response({"results": [{"ticker": "SPY", "cash_amount": 1.0, "ex_dividend_date": "2026-01-02"}]})

    monkeypatch.setattr(ca, "get_data_credential", lambda name: canary if name == "POLYGON_API_KEY" else "")
    monkeypatch.setattr(ca.requests, "get", _fake_get)

    rows, payload = ca.fetch_polygon_corporate_actions("SPY")

    assert rows
    assert any(params.get("apiKey") == canary for params in seen_params)
    serialized = json.dumps({"rows": rows, "payload": payload}, sort_keys=True, default=str)
    assert canary not in serialized


def test_corporate_actions_job_registered_and_import_clean(monkeypatch) -> None:
    monkeypatch.setenv("INGEST_CORPORATE_ACTIONS_ENABLED", "0")
    job_registry = importlib.reload(importlib.import_module("engine.runtime.job_registry"))
    job = importlib.reload(importlib.import_module("engine.data.jobs.ingest_corporate_actions"))

    assert job.JOB_NAME == "ingest_corporate_actions"
    assert job_registry.ALLOWED_JOBS["ingest_corporate_actions"][0] == "engine/data/jobs/ingest_corporate_actions.py"
    assert job_registry.ALLOWED_JOBS["ingest_corporate_actions"][3]["requires_secret_any"] == ["POLYGON_API_KEY", "FMP_API_KEY"]
