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


def _make_fundamentals_db(fundamentals_pit):
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    fundamentals_pit.ensure_fundamentals_tables(con)
    return con


class _FakeResponse:
    def __init__(self, payload, *, text: str = "", status_code: int = 200):
        self._payload = payload
        self.text = str(text or "")
        self.status_code = int(status_code)

    def json(self):
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
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


def _pit_row(fundamentals_pit, *, symbol: str, metric: str, value: float, publish_ts_ms: int, source_id: str):
    return {
        "ts_ms": int(publish_ts_ms),
        "symbol": str(symbol).upper(),
        "fiscal_period": "2025Q4",
        "metric": str(metric),
        "value": float(value),
        "publish_ts_ms": int(publish_ts_ms),
        "publish_date": "2026-02-01",
        "vendor": "simfin",
        "source_record_id": str(source_id),
        "fiscal_year": 2025,
        "fiscal_quarter": 4,
        "statement_type": "quarterly",
        "ingested_ts_ms": int(publish_ts_ms),
        "payload_json": {"symbol": symbol, "metric": metric, "value": value},
        "diagnostics_json": {"availability_rule": "publish_ts_ms"},
    }


def test_fundamentals_restatement_no_lookahead_through_snapshot(monkeypatch) -> None:
    monkeypatch.setenv("USE_FUNDAMENTALS_PIT_FEATURES", "1")
    monkeypatch.setenv("FUNDAMENTALS_PIT_MODE", "pit")
    _reload("engine.strategy.feature_registry")
    fundamentals_pit, snapshots = _reload("engine.data.fundamentals_pit", "engine.strategy.model_feature_snapshots")
    con = _make_fundamentals_db(fundamentals_pit)

    t1 = fundamentals_pit.parse_ts_ms("2026-02-01T21:00:00+00:00")
    t2 = fundamentals_pit.parse_ts_ms("2026-03-01T21:00:00+00:00")
    assert t1 and t2
    fundamentals_pit.put_fundamental_pit_row(
        _pit_row(fundamentals_pit, symbol="AAPL", metric="eps", value=1.00, publish_ts_ms=t1, source_id="orig-eps"),
        con=con,
    )
    fundamentals_pit.put_fundamental_pit_row(
        _pit_row(fundamentals_pit, symbol="AAPL", metric="eps", value=1.20, publish_ts_ms=t2, source_id="restated-eps"),
        con=con,
    )
    con.commit()

    ids = list(snapshots.FUNDAMENTALS_PIT_FEATURE_IDS)
    before_revision = snapshots.build_model_feature_snapshot(symbol="AAPL", ts_ms=t2 - 1, feature_ids=ids, con=con)
    after_revision = snapshots.build_model_feature_snapshot(symbol="AAPL", ts_ms=t2 + 1, feature_ids=ids, con=con)

    assert before_revision["features"]["fund_eps"] == 1.00
    assert before_revision["source_timestamps"]["fundamentals"]["latest_publish_ts_ms"] == t1
    assert after_revision["features"]["fund_eps"] == 1.20
    assert after_revision["source_timestamps"]["fundamentals"]["latest_publish_ts_ms"] == t2
    assert snapshots.summarize_model_feature_snapshots([before_revision, after_revision])["lookahead_violations"] == 0


def test_fundamentals_adapter_contract_simfin_and_sharadar_mock() -> None:
    (fundamentals_pit,) = _reload("engine.data.fundamentals_pit")
    simfin = fundamentals_pit.SimFinAdapter(
        api_key="sim",
        session=_FakeSession(
            [
                _FakeResponse(
                    [
                        {
                            "Ticker": "AAPL",
                            "FiscalPeriod": "2025Q4",
                            "FiscalYear": 2025,
                            "FiscalQuarter": 4,
                            "PublishDate": "2026-02-01",
                            "Revenue": 100.0,
                            "EPS": 1.25,
                        }
                    ]
                )
            ]
        ),
        bulk_url="https://example.test/simfin",
    )
    sharadar = fundamentals_pit.SharadarAdapter(
        api_key="sha",
        session=_FakeSession(
            [
                _FakeResponse(
                    {
                        "datatable": {
                            "columns": [
                                {"name": "ticker"},
                                {"name": "calendardate"},
                                {"name": "datekey"},
                                {"name": "revenue"},
                                {"name": "eps"},
                            ],
                            "data": [["AAPL", "2025-12-31", "2026-02-02", 110.0, 1.30]],
                        }
                    }
                )
            ]
        ),
        bulk_url="https://example.test/sharadar",
    )

    for adapter in (simfin, sharadar):
        assert adapter.enabled is True
        rows = adapter.fetch_publications(symbols=["AAPL"])
        normalized = [
            row
            for raw in rows
            for row in fundamentals_pit.normalize_fundamental_publication(raw, vendor=adapter.vendor)
        ]
        metrics = {row["metric"] for row in normalized}
        assert {"revenue", "eps"}.issubset(metrics)
        assert {row["symbol"] for row in normalized} == {"AAPL"}
        assert all(row["publish_ts_ms"] > 0 for row in normalized)
        assert all(row["vendor"] == adapter.vendor for row in normalized)


def test_fundamentals_ab_check_reports_changed_values() -> None:
    (fundamentals_pit,) = _reload("engine.data.fundamentals_pit")
    con = _make_fundamentals_db(fundamentals_pit)
    t1 = fundamentals_pit.parse_ts_ms("2026-02-01T21:00:00+00:00")
    assert t1
    con.execute(
        """
        CREATE TABLE earnings_calendar (
            symbol TEXT,
            earnings_date TEXT,
            eps_act DOUBLE PRECISION,
            revenue_act DOUBLE PRECISION,
            updated_ts_ms BIGINT
        )
        """
    )
    con.execute(
        """
        INSERT INTO earnings_calendar(symbol, earnings_date, eps_act, revenue_act, updated_ts_ms)
        VALUES ('AAPL', '2026-01-31', 0.90, 90.0, ?)
        """,
        (t1,),
    )
    fundamentals_pit.put_fundamental_pit_row(
        _pit_row(fundamentals_pit, symbol="AAPL", metric="eps", value=1.00, publish_ts_ms=t1, source_id="pit-eps"),
        con=con,
    )
    fundamentals_pit.put_fundamental_pit_row(
        _pit_row(fundamentals_pit, symbol="AAPL", metric="revenue", value=100.0, publish_ts_ms=t1, source_id="pit-revenue"),
        con=con,
    )
    con.commit()

    report = fundamentals_pit.compare_pit_vs_legacy(con, symbol="AAPL", ts_values=[t1 + 1])

    assert report["compared"] == 1
    assert report["changed"] == 1
    assert report["examples"][0]["diffs"]["fund_eps"] == (1.0, 0.9)


def test_fundamentals_ingest_missing_keys_is_blocked_but_healthy() -> None:
    (fundamentals_pit,) = _reload("engine.data.fundamentals_pit")

    summary = fundamentals_pit.ingest_fundamentals_pit_batch(adapters=[])

    assert summary["ok"] is True
    assert summary["blocked"] is True
    assert summary["blocker"] == "missing_simfin_or_sharadar_api_key"


def test_fundamentals_registry_round_trip_job_registered_and_import_clean(monkeypatch) -> None:
    monkeypatch.setenv("USE_FUNDAMENTALS_PIT_FEATURES", "1")
    monkeypatch.setenv("INGEST_FUNDAMENTALS_PIT_ENABLED", "0")
    feature_registry, job_registry, _job = _reload(
        "engine.strategy.feature_registry",
        "engine.runtime.job_registry",
        "engine.data.jobs.ingest_fundamentals_pit",
    )

    ids = list(feature_registry.FUNDAMENTALS_PIT_FEATURE_IDS)
    assert ids == [
        "fund_revenue",
        "fund_eps",
        "fund_gross_margin",
        "fund_net_margin",
        "fund_shares",
        "fund_book_value",
        "fund_fcf",
    ]
    assert feature_registry.FEATURE_GROUPS["fundamentals"] == ids
    assert feature_registry.resolve_feature_ids(model_spec={"feature_schema": {"feature_ids": ids}}) == ids
    assert feature_registry.expected_columns(ids, fallback_to_default=False) == ids
    assert "fundamentals" in feature_registry.feature_set_tag_from_ids(ids).split("+")
    assert job_registry.ALLOWED_JOBS["ingest_fundamentals_pit"][3]["cadence_seconds"] == 86400
