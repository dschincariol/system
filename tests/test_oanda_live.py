from __future__ import annotations

import importlib
import json
import uuid


class _JsonResponse:
    status_code = 200
    headers: dict[str, str] = {}

    def __init__(self, payload: dict) -> None:
        self._payload = dict(payload)

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return dict(self._payload)


def _reload_oanda_live():
    import engine.data._credentials as credentials
    import engine.data.live_prices.oanda_live as oanda_live

    credentials.clear_data_credential_cache()
    return importlib.reload(oanda_live)


def test_oanda_fetch_last_prices_parses_pricing_payload(monkeypatch) -> None:
    mod = _reload_oanda_live()
    monkeypatch.setenv("OANDA_ACCESS_TOKEN", f"oanda-canary-{uuid.uuid4().hex}")
    monkeypatch.setenv("OANDA_ACCOUNT_ID", "101-001-00000000-001")

    def _fake_get(url, **kwargs):
        assert url.endswith("/v3/accounts/101-001-00000000-001/pricing")
        assert kwargs["params"] == {"instruments": "EUR_USD"}
        assert str(kwargs["headers"]["Authorization"]).startswith("Bearer oanda-canary-")
        return _JsonResponse(
            {
                "prices": [
                    {
                        "instrument": "EUR_USD",
                        "time": "2026-06-23T12:34:56.123456Z",
                        "bids": [{"price": "1.10000", "liquidity": 1000000}],
                        "asks": [{"price": "1.10020", "liquidity": 1000000}],
                    }
                ]
            }
        )

    monkeypatch.setattr(mod.requests, "get", _fake_get)

    out = mod.OANDAPriceProvider().fetch_last_prices({"EURUSD": "EUR_USD"})

    row = out["EURUSD"]
    assert row["source"] == "oanda"
    assert row["price"] == 1.1001
    assert row["bid"] == 1.1
    assert row["ask"] == 1.1002
    assert round(row["spread"], 7) == 0.0002
    assert row["volume"] is None
    assert row["ts_ms"] == 1782218096123


def test_oanda_missing_token_returns_empty(monkeypatch) -> None:
    mod = _reload_oanda_live()
    monkeypatch.delenv("OANDA_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("OANDA_API_KEY", raising=False)
    monkeypatch.setenv("OANDA_ACCOUNT_ID", "101-001-00000000-001")

    out = mod.OANDAPriceProvider().fetch_last_prices({"EURUSD": "EUR_USD"})

    assert out == {}


def test_oanda_canary_token_not_returned_or_logged(monkeypatch, caplog) -> None:
    mod = _reload_oanda_live()
    canary = f"oanda-canary-{uuid.uuid4().hex}"
    monkeypatch.setenv("OANDA_ACCESS_TOKEN", canary)
    monkeypatch.setenv("OANDA_ACCOUNT_ID", "101-001-00000000-001")

    def _fake_get(_url, **kwargs):
        assert kwargs["headers"]["Authorization"] == f"Bearer {canary}"
        return _JsonResponse(
            {
                "prices": [
                    {
                        "instrument": "EUR_USD",
                        "time": "2026-06-23T12:34:56.123456Z",
                        "bids": [{"price": "1.20000"}],
                        "asks": [{"price": "1.20030"}],
                    }
                ]
            }
        )

    monkeypatch.setattr(mod.requests, "get", _fake_get)

    out = mod.OANDAPriceProvider().fetch_last_prices({"EURUSD": "EUR_USD"})

    assert out
    assert canary not in json.dumps(out, sort_keys=True, default=str)
    assert canary not in caplog.text
