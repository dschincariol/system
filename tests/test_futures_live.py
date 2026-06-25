from __future__ import annotations

import importlib
import json
import sqlite3
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


def _reload_futures_live():
    import engine.data._credentials as credentials
    import engine.data.live_prices.futures_live as futures_live

    credentials.clear_data_credential_cache()
    return importlib.reload(futures_live)


def test_futures_fetch_last_prices_parses_databento_payload(monkeypatch) -> None:
    mod = _reload_futures_live()
    monkeypatch.setenv("DATABENTO_API_KEY", f"databento-canary-{uuid.uuid4().hex}")

    def _fake_get(url, **kwargs):
        assert url.endswith("/v0/timeseries.get_range")
        assert kwargs["params"]["dataset"] == "GLBX.MDP3"
        assert kwargs["params"]["schema"] == "ohlcv-1m"
        assert kwargs["params"]["symbols"] == "ESZ26"
        assert str(kwargs["headers"]["Authorization"]).startswith("Bearer databento-canary-")
        return _JsonResponse(
            {
                "records": [
                    {
                        "symbol": "ESZ26",
                        "ts_event": "2026-06-23T12:34:00Z",
                        "open": "5500.00",
                        "high": "5502.00",
                        "low": "5499.25",
                        "close": "5501.25",
                        "volume": "1234",
                        "open_interest": "98765",
                    }
                ]
            }
        )

    monkeypatch.setattr(mod.requests, "get", _fake_get)

    out = mod.FuturesPriceProvider().fetch_last_prices({"ES.c.0": "ESZ26"})

    row = out["ES.c.0"]
    assert row["source"] == "futures"
    assert row["price"] == 5501.25
    assert row["open"] == 5500.0
    assert row["high"] == 5502.0
    assert row["low"] == 5499.25
    assert row["close"] == 5501.25
    assert row["volume"] == 1234.0
    assert row["open_interest"] == 98765.0
    assert row["bid"] is None
    assert row["ask"] is None
    assert row["spread"] is None
    assert row["ts_ms"] == 1782218040000


def test_futures_missing_token_returns_empty(monkeypatch) -> None:
    mod = _reload_futures_live()
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)

    out = mod.FuturesPriceProvider().fetch_last_prices({"ES.c.0": "ESZ26"})

    assert out == {}


def test_futures_canary_token_not_returned_or_logged(monkeypatch, caplog) -> None:
    mod = _reload_futures_live()
    canary = f"databento-canary-{uuid.uuid4().hex}"
    monkeypatch.setenv("DATABENTO_API_KEY", canary)

    def _fake_get(_url, **kwargs):
        assert kwargs["headers"]["Authorization"] == f"Bearer {canary}"
        return _JsonResponse(
            {
                "records": [
                    {
                        "symbol": "CLM26",
                        "ts_event": "2026-06-23T12:34:00Z",
                        "close": "80.12",
                        "volume": "321",
                        "open_interest": "4567",
                    }
                ]
            }
        )

    monkeypatch.setattr(mod.requests, "get", _fake_get)

    out = mod.FuturesPriceProvider().fetch_last_prices({"CL.c.0": "CLM26"})

    assert out
    assert canary not in json.dumps(out, sort_keys=True, default=str)
    assert canary not in caplog.text


def test_ensure_futures_bars_table_creates_sidecar_table() -> None:
    mod = _reload_futures_live()
    con = sqlite3.connect(":memory:")
    try:
        mod.ensure_futures_bars_table(con)
        columns = {row[1]: row[2].upper() for row in con.execute("PRAGMA table_info(futures_contract_bars)")}
        assert columns["contract"] == "TEXT"
        assert columns["ts_ms"] == "BIGINT"
        assert columns["open_interest"] == "REAL"
    finally:
        con.close()


def test_poll_prices_persists_futures_contract_bars(monkeypatch) -> None:
    monkeypatch.setenv("ENGINE_SUPERVISED", "1")
    poll_prices = importlib.import_module("engine.data.poll_prices")
    con = sqlite3.connect(":memory:")
    try:
        bar = poll_prices._build_futures_contract_bar_row(
            provider_name="futures",
            symbol="ES.c.0",
            row={
                "ts_ms": 1782218040000,
                "open": 5500.0,
                "high": 5502.0,
                "low": 5499.25,
                "close": 5501.25,
                "volume": 1234.0,
                "open_interest": 98765.0,
                "source": "futures",
            },
            provider_symbol_map={"ES.c.0": "ESZ26"},
            now_ts_ms=1782218040000,
        )
        assert bar is not None

        poll_prices._put_futures_contract_bars_batch(con, [bar])

        row = con.execute(
            """
            SELECT contract, ts_ms, open, high, low, close, volume, open_interest, source
            FROM futures_contract_bars
            """
        ).fetchone()
        assert row == (
            "ESZ26",
            1782218040000,
            5500.0,
            5502.0,
            5499.25,
            5501.25,
            1234.0,
            98765.0,
            "futures",
        )
    finally:
        con.close()
