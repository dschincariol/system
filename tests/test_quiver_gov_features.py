from __future__ import annotations

import importlib
import sqlite3
import sys
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


def _make_gov_db(quiver_gov):
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    quiver_gov.ensure_gov_tables(con)
    return con


def _seed_mapping(quiver_gov, con) -> None:
    now_ms = quiver_gov.parse_ts_ms("2026-01-01T00:00:00+00:00")
    con.execute(
        """
        INSERT INTO gov_symbol_sector_map(symbol, sector, source, updated_ts_ms, meta_json)
        VALUES ('AAPL', 'technology', 'unit', ?, '{}')
        """,
        (now_ms,),
    )
    con.execute(
        """
        INSERT INTO gov_member_committee_map(member_name, committee, active, updated_ts_ms, meta_json)
        VALUES ('Alice Smith', 'Commerce', 1, ?, '{}')
        """,
        (now_ms,),
    )
    con.execute(
        """
        INSERT INTO gov_committee_sector_map(committee, sector, weight, active, updated_ts_ms, meta_json)
        VALUES ('Commerce', 'technology', 1.0, 1, ?, '{}')
        """,
        (now_ms,),
    )
    con.execute(
        """
        INSERT INTO gov_member_leadership_map(member_name, leadership_role, active, updated_ts_ms, meta_json)
        VALUES ('Alice Smith', 'leadership', 1, ?, '{}')
        """,
        (now_ms,),
    )


class _FakeResponse:
    def __init__(self, payload, *, status_code=200, headers=None):
        self._payload = payload
        self.status_code = status_code
        self.headers = dict(headers or {})

    def json(self):
        return self._payload

    def raise_for_status(self):
        if int(self.status_code) >= 400:
            raise RuntimeError(f"http {self.status_code}")


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise AssertionError("unexpected request")
        return self.responses.pop(0)


def test_quiver_client_auth_pagination_and_rate_limit_backoff() -> None:
    (quiver_gov,) = _reload("engine.data.quiver_gov")
    session = _FakeSession(
        [
            _FakeResponse({}, status_code=429, headers={"Retry-After": "0"}),
            _FakeResponse({"data": [{"Ticker": "AAPL"}], "next_page": 2}),
            _FakeResponse({"data": [{"Ticker": "MSFT"}]}),
        ]
    )
    sleeps = []
    client = quiver_gov.QuiverClient(
        api_key="test-key",
        base_url="https://example.test/beta",
        session=session,
        sleep_fn=sleeps.append,
        max_retries=2,
    )

    rows = client.get_paginated("/historical/congresstrading")

    assert rows == [{"Ticker": "AAPL"}, {"Ticker": "MSFT"}]
    assert sleeps
    assert session.calls[0]["headers"]["Authorization"] == "Bearer test-key"
    assert session.calls[-1]["params"]["page"] == 2


def test_quiver_missing_api_key_is_inert_blocker() -> None:
    (quiver_gov,) = _reload("engine.data.quiver_gov")
    summary = quiver_gov.ingest_quiver_gov_batch(client=quiver_gov.QuiverClient(api_key=""))

    assert summary["ok"] is True
    assert summary["blocked"] is True
    assert summary["blocker"] == "missing_quiver_api_key"


def test_gov_disclosure_lag_no_lookahead_through_snapshot(monkeypatch) -> None:
    monkeypatch.setenv("USE_GOV_FEATURES", "1")
    _reload("engine.strategy.feature_registry")
    quiver_gov, snapshots = _reload("engine.data.quiver_gov", "engine.strategy.model_feature_snapshots")
    con = _make_gov_db(quiver_gov)
    _seed_mapping(quiver_gov, con)

    row = quiver_gov.normalize_quiver_congressional_record(
        {
            "Representative": "Alice Smith",
            "Ticker": "AAPL",
            "Transaction": "Purchase",
            "Amount": "$100,000",
            "TransactionDate": "2026-01-10",
            "ReportDate": "2026-02-15",
        },
        ingested_ts_ms=quiver_gov.parse_ts_ms("2026-02-15T12:00:00+00:00"),
    )
    quiver_gov.put_quiver_congress_trade(row, con=con)
    con.commit()

    before_ts = int(row["disclosure_ts_ms"]) - 1
    after_ts = int(row["disclosure_ts_ms"]) + 1
    ids = list(snapshots.GOV_FEATURE_IDS)
    before = snapshots.build_model_feature_snapshot(symbol="AAPL", ts_ms=before_ts, feature_ids=ids, con=con)
    after = snapshots.build_model_feature_snapshot(symbol="AAPL", ts_ms=after_ts, feature_ids=ids, con=con)

    assert before["features"]["congress_committee_buy_30d"] == 0.0
    assert before["source_timestamps"]["gov"]["latest_availability_ts_ms"] is None
    assert after["features"]["congress_committee_buy_30d"] == 1.0
    assert after["features"]["congress_leadership_trade_flag"] == 1.0
    assert after["source_timestamps"]["gov"]["latest_availability_ts_ms"] == row["disclosure_ts_ms"]
    assert snapshots.summarize_model_feature_snapshots([before, after])["lookahead_violations"] == 0


def test_gov_committee_mapping_unit() -> None:
    (quiver_gov,) = _reload("engine.data.quiver_gov")
    con = _make_gov_db(quiver_gov)
    _seed_mapping(quiver_gov, con)

    assert quiver_gov.member_is_committee_relevant(con, member_name="Alice Smith", symbol="AAPL") is True
    assert quiver_gov.member_is_committee_relevant(con, member_name="Nobody", symbol="AAPL") is False


def test_gov_dedupe_against_existing_congressional_ingestion() -> None:
    (quiver_gov,) = _reload("engine.data.quiver_gov")
    con = _make_gov_db(quiver_gov)
    con.execute(
        """
        CREATE TABLE congressional_trades (
            id INTEGER PRIMARY KEY,
            politician_name TEXT,
            symbol TEXT,
            direction TEXT,
            transaction_type TEXT,
            amount_mid DOUBLE PRECISION,
            transaction_date TEXT,
            transaction_ts_ms BIGINT,
            disclosure_date TEXT,
            disclosure_ts_ms BIGINT,
            source_trade_id TEXT
        )
        """
    )
    disclosure_ts = quiver_gov.parse_ts_ms("2026-02-01")
    transaction_ts = quiver_gov.parse_ts_ms("2026-01-10")
    con.execute(
        """
        INSERT INTO congressional_trades(
          politician_name, symbol, direction, transaction_type, amount_mid,
          transaction_date, transaction_ts_ms, disclosure_date, disclosure_ts_ms, source_trade_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("Alice Smith", "AAPL", "buy", "purchase", 100000.0, "2026-01-10", transaction_ts, "2026-02-01", disclosure_ts, "free:1"),
    )
    row = quiver_gov.normalize_quiver_congressional_record(
        {
            "Representative": "Alice Smith",
            "Ticker": "AAPL",
            "Transaction": "Purchase",
            "Amount": "$100,000",
            "TransactionDate": "2026-01-10",
            "ReportDate": "2026-02-01",
            "id": "quiver:1",
        }
    )

    assert quiver_gov.is_duplicate_existing_congressional_trade(con, row) is True
    quiver_gov.put_quiver_congress_trade(row, con=con)
    features, meta, available = quiver_gov.resolve_gov_features(con, symbol="AAPL", ts_ms=int(disclosure_ts) + 1)

    assert available is True
    assert meta["congress_event_count"] == 1
    assert features["congress_sale_signal_30d"] == 0.0


def test_gov_registry_round_trip_job_registered_and_import_clean(monkeypatch) -> None:
    monkeypatch.setenv("USE_GOV_FEATURES", "1")
    monkeypatch.setenv("INGEST_QUIVER_GOV_ENABLED", "0")
    feature_registry, job_registry, _job = _reload(
        "engine.strategy.feature_registry",
        "engine.runtime.job_registry",
        "engine.data.jobs.ingest_quiver_gov",
    )

    ids = list(feature_registry.GOV_FEATURE_IDS)
    assert ids == [
        "congress_committee_buy_30d",
        "congress_leadership_trade_flag",
        "congress_sale_signal_30d",
        "lobbying_spend_z_yoy",
        "gov_contract_award_z",
    ]
    assert feature_registry.FEATURE_GROUPS["gov"] == ids
    assert feature_registry.resolve_feature_ids(model_spec={"feature_schema": {"feature_ids": ids}}) == ids
    assert feature_registry.expected_columns(ids, fallback_to_default=False) == ids
    assert "gov" in feature_registry.feature_set_tag_from_ids(ids).split("+")
    assert job_registry.ALLOWED_JOBS["ingest_quiver_gov"][3]["cadence_seconds"] == 86400
