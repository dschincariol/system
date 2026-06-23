from __future__ import annotations

import importlib
import json
import uuid

import pytest
import requests


class _Response:
    def __init__(self, status_code: int = 200, payload=None, text: str = "", headers=None, reason: str = "OK") -> None:
        self.status_code = int(status_code)
        self._payload = payload
        self.text = text
        self.headers = dict(headers or {})
        self.reason = reason
        self.content = text.encode("utf-8")

    def json(self):
        if isinstance(self._payload, BaseException):
            raise self._payload
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            exc = requests.HTTPError(f"{self.status_code} {self.reason}")
            exc.response = self
            raise exc

    def iter_content(self, chunk_size: int = 2048):
        yield self.content[:chunk_size]


def _manager():
    data_source_manager = importlib.import_module("services.data_source_manager")
    data_source_manager._DATA_SOURCE_CONNECTION_TEST_MIN_INTERVAL_MS = 0
    data_source_manager._LAST_DATA_SOURCE_CONNECTION_TEST_PROBE_MS.clear()
    return data_source_manager.DataSourceManager(), data_source_manager


def _source(source_key: str) -> dict:
    settings = {
        "url": "https://example.test/feed.xml",
        "user_agent": "codex-test contact@unit.test",
        "http_ua": "codex-test contact@unit.test",
        "source_url": "https://example.test/congress.json",
    }
    return {"source_key": source_key, "settings": settings}


@pytest.mark.parametrize("status_code,classification,status", [(401, "wrong_credentials", "fail"), (403, "entitlement_missing", "fail"), (429, "rate_limited", "degraded"), (503, "provider_unreachable", "degraded")])
@pytest.mark.parametrize(
    "source_key,handler_name,http_method",
    [
        ("stocktwits", "_test_stocktwits_connection", "get"),
        ("gdelt", "_test_gdelt_connection", "get"),
        ("congressional_trades", "_test_congressional_trades_connection", "get"),
        ("finra_short_volume", "_test_finra_short_volume_connection", "get"),
        ("finra_short_interest", "_test_finra_short_interest_connection", "post"),
        ("weather_alerts", "_test_weather_alerts_connection", "get"),
        ("rss:unit", "_test_rss_connection", "get"),
    ],
)
def test_public_connection_status_classification(monkeypatch, source_key, handler_name, http_method, status_code, classification, status) -> None:
    manager, data_source_manager = _manager()
    response = _Response(status_code=status_code, payload={}, text="", headers={"Retry-After": "17"})
    monkeypatch.setattr(data_source_manager.requests, http_method, lambda *_args, **_kwargs: response)

    result = getattr(manager, handler_name)(_source(source_key))

    assert result.status == status
    assert result.classification == classification
    if status_code == 429:
        assert result.evidence["retry_after_s"] == 17.0
        assert result.evidence["stop_testing"] is True


@pytest.mark.parametrize(
    "source_key,handler_name,http_method,response",
    [
        ("stocktwits", "_test_stocktwits_connection", "get", _Response(payload={"messages": [{"id": 1, "symbols": []}]})),
        ("gdelt", "_test_gdelt_connection", "get", _Response(payload={"articles": [{"title": "Market news"}]})),
        ("congressional_trades", "_test_congressional_trades_connection", "get", _Response(payload=[{"ticker": "AAPL", "transaction_date": "2026-01-01"}])),
        ("finra_short_volume", "_test_finra_short_volume_connection", "get", _Response(text="Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market\n20260102|AAPL|10|0|100|Q,N\n")),
        ("finra_short_interest", "_test_finra_short_interest_connection", "post", _Response(payload=[{"issueSymbolIdentifier": "AAPL", "settlementDate": "2026-01-15", "currentShortShareNumber": 100}])),
        ("weather_alerts", "_test_weather_alerts_connection", "get", _Response(payload={"type": "FeatureCollection", "features": []})),
        ("rss:unit", "_test_rss_connection", "get", _Response(text="<rss><channel><item><title>Market</title></item></channel></rss>")),
    ],
)
def test_public_connection_valid_payloads(monkeypatch, source_key, handler_name, http_method, response) -> None:
    manager, data_source_manager = _manager()
    monkeypatch.setattr(data_source_manager.requests, http_method, lambda *_args, **_kwargs: response)

    result = getattr(manager, handler_name)(_source(source_key))

    assert result.ok is True
    assert result.classification == "success"


@pytest.mark.parametrize(
    "payload,expected_classification,expected_message",
    [
        ({"messages": []}, "empty_payload", "stocktwits_empty_payload"),
        ({"unexpected": []}, "malformed_payload", "stocktwits_malformed_payload"),
        (ValueError("not json"), "empty_payload", "stocktwits_invalid_json"),
    ],
)
def test_public_connection_empty_and_malformed_payloads(monkeypatch, payload, expected_classification, expected_message) -> None:
    manager, data_source_manager = _manager()
    monkeypatch.setattr(data_source_manager.requests, "get", lambda *_args, **_kwargs: _Response(payload=payload))

    result = manager._test_stocktwits_connection(_source("stocktwits"))

    assert result.ok is False
    assert result.classification == expected_classification
    assert result.message == expected_message


def test_sec_identity_placeholder_degrades_before_network(monkeypatch) -> None:
    manager, data_source_manager = _manager()
    monkeypatch.setattr(data_source_manager.requests, "get", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("network should not run")))

    result = manager._test_sec_filings_connection({"source_key": "sec", "settings": {}})

    assert result.status == "degraded"
    assert result.classification == "missing_credentials"
    assert result.message == "sec_identity_missing_or_placeholder"


def test_form4_runtime_discovers_xml_when_primary_document_is_html(monkeypatch) -> None:
    form4_live = importlib.reload(importlib.import_module("engine.data.sec.form4_live"))
    monkeypatch.setattr(form4_live, "_store_filing_body", lambda **_kwargs: {})
    monkeypatch.setattr(form4_live.edgar_live, "fetch_recent_filings", lambda *_args, **_kwargs: [
        {
            "accession": "0000320193-26-000001",
            "form": "4",
            "filed_date": "2026-04-10",
            "acceptance_datetime": "2026-04-10T18:45:36.000Z",
            "cik": "0000320193",
            "primary_doc_url": "https://www.sec.gov/Archives/edgar/data/320193/000032019326000001/primary.htm",
        }
    ])
    xml = """<ownershipDocument><documentType>4</documentType><periodOfReport>2026-04-10</periodOfReport><issuer><issuerCik>0000320193</issuerCik><issuerName>Apple Inc.</issuerName><issuerTradingSymbol>AAPL</issuerTradingSymbol></issuer><reportingOwner><reportingOwnerId><rptOwnerName>Jane Insider</rptOwnerName></reportingOwnerId></reportingOwner><nonDerivativeTable><nonDerivativeTransaction><transactionDate><value>2026-04-09</value></transactionDate><transactionCoding><transactionCode>P</transactionCode></transactionCoding><transactionAmounts><transactionShares><value>100</value></transactionShares><transactionPricePerShare><value>175.50</value></transactionPricePerShare><transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode></transactionAmounts></nonDerivativeTransaction></nonDerivativeTable></ownershipDocument>"""

    class Session:
        def get(self, url, **_kwargs):
            if url.endswith("index.json"):
                return _Response(payload={"directory": {"item": [{"name": "primary.htm"}, {"name": "ownership.xml"}]}})
            if url.endswith("primary.htm"):
                return _Response(text="<html><body>not the info document</body></html>")
            if url.endswith("ownership.xml"):
                return _Response(text=xml)
            raise AssertionError(url)

    rows = form4_live.fetch_form4_transactions("AAPL", filing_limit=1, session=Session(), allowed_symbols=["AAPL"])

    assert len(rows) == 1
    assert rows[0]["filing_url"].endswith("ownership.xml")
    assert rows[0]["symbol"] == "AAPL"


def test_gdelt_rate_limit_sets_cooldown_and_stops_symbol_loop(monkeypatch) -> None:
    gdelt = importlib.reload(importlib.import_module("engine.data.ingest.gdelt_ingest"))
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return _Response(status_code=429, payload={}, headers={"Retry-After": "120"})

    monkeypatch.setattr(gdelt.requests, "get", fake_get)

    items, errors = gdelt.ingest_gdelt_doc(symbols=["SPY", "BTC"], lookback_minutes=5, maxrecords=1)

    assert items == []
    assert len(calls) == 1
    assert "gdelt_rate_limited" in errors[0]
    assert gdelt.gdelt_cooldown_remaining_s() > 0

    items, errors = gdelt.ingest_gdelt_doc(symbols=["SPY"], lookback_minutes=5, maxrecords=1)
    assert items == []
    assert "cooldown_remaining_s" in errors[0]
    assert len(calls) == 1


def test_rss_per_feed_status_isolates_bad_source(monkeypatch) -> None:
    rss = importlib.reload(importlib.import_module("engine.data.ingest.rss_ingest"))

    class Feed:
        def __init__(self, entries, bozo=False) -> None:
            self.entries = entries
            self.bozo = bozo
            self.bozo_exception = RuntimeError("malformed")

    def fake_fetch(url, **_kwargs):
        if "bad" in url:
            response = _Response(status_code=503, headers={"Retry-After": "33"})
            exc = requests.HTTPError("503 Service Unavailable")
            exc.response = response
            raise exc
        return Feed([{"title": "Market", "link": "https://example.test/a", "summary": "AAPL rallies"}])

    monkeypatch.setattr(rss, "fetch_rss", fake_fetch)

    items, errors, statuses = rss.ingest_rss_sources(
        [{"name": "good", "url": "https://example.test/good.xml"}, {"name": "bad", "url": "https://example.test/bad.xml"}],
        include_status=True,
    )

    assert len(items) == 1
    assert len(errors) == 1
    by_name = {row["source_name"]: row for row in statuses}
    assert by_name["good"]["status"] == "pass"
    assert by_name["bad"]["status"] == "degraded"
    assert by_name["bad"]["classification"] == "provider_unreachable"


def test_macro_missing_fred_key_reports_alfred_fallback_and_redacts_canary(monkeypatch) -> None:
    manager, data_source_manager = _manager()
    monkeypatch.setattr(manager, "_connection_effective_env_value", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(data_source_manager.requests, "get", lambda *_args, **_kwargs: _Response(text="DATE,CPIAUCSL\n2026-01-01,100\n"))

    result = manager._test_macro_fred_connection(_source("macro"))

    assert result.status == "degraded"
    assert result.message == "fred_api_key_missing_alfred_fallback_used"
    assert result.evidence["alfred_fallback_used"] is True


def test_macro_connection_payload_does_not_expose_fred_key_canary(monkeypatch) -> None:
    manager, data_source_manager = _manager()
    canary = f"codex-canary-secret-{uuid.uuid4().hex}"
    monkeypatch.setattr(manager, "_connection_effective_env_value", lambda *_args, **_kwargs: canary)
    monkeypatch.setattr(data_source_manager.requests, "get", lambda *_args, **_kwargs: _Response(status_code=403, payload={}))

    result = manager._test_macro_fred_connection(_source("macro"))
    rendered = json.dumps(result.payload(source_key="macro"), sort_keys=True)

    assert result.classification == "entitlement_missing"
    assert canary not in rendered


def test_public_feed_live_smoke_is_gated_by_env(monkeypatch, capsys) -> None:
    live_smoke = importlib.import_module("tools.public_feed_live_smoke")
    monkeypatch.delenv(live_smoke.LIVE_FLAG, raising=False)

    assert live_smoke.main([]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["skipped"] is True
    assert live_smoke.LIVE_FLAG in payload["reason"]
