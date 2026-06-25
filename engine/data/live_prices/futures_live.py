"""Read-only futures price polling adapter.

FUT-02 intentionally exposes market data only. It does not import or call any
broker order, cancel, replace, flatten, or account-mutation API. Raw continuous
construction and roll calendars are owned by FUT-03.
"""

from __future__ import annotations

import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping

from engine.data._credentials import get_data_credential
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

try:
    import requests

    _REQUESTS_IMPORT_ERROR: BaseException | None = None
except Exception as _requests_import_error:  # pragma: no cover - dependency exists in supported envs
    requests = None  # type: ignore[assignment]
    _REQUESTS_IMPORT_ERROR = _requests_import_error


LOG = get_logger("engine.data.live_prices.futures_live")
_WARNED_NONFATAL_KEYS: set[str] = set()
_DATABENTO_TIMESERIES_URL = "https://hist.databento.com/v0/timeseries.get_range"


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(code),
        error=error,
        level=30,
        component="engine.data.live_prices.futures_live",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _finite_float(value: object) -> float | None:
    try:
        out = float(str(value).replace(",", "").strip())
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _parse_ts_ms(value: object, fallback_ms: int) -> int:
    if value is None or str(value).strip() == "":
        return int(fallback_ms)
    if isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000_000:
            return int(raw / 1_000_000)
        if raw > 10_000_000_000:
            return int(raw)
        return int(raw * 1000)
    text = str(value or "").strip()
    try:
        if text.isdigit():
            return _parse_ts_ms(int(text), fallback_ms)
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp() * 1000)
    except Exception:
        return int(fallback_ms)


def _iter_payload_records(payload: object) -> Iterable[Mapping[str, Any]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, Mapping):
                yield item
        return
    if not isinstance(payload, Mapping):
        return
    for key in ("records", "results", "data", "bars", "prices"):
        value = payload.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, Mapping):
                    yield item
            return
    if any(key in payload for key in ("symbol", "instrument", "close", "price", "last")):
        yield payload


def _record_symbol(record: Mapping[str, Any]) -> str:
    return str(
        record.get("symbol")
        or record.get("raw_symbol")
        or record.get("instrument")
        or record.get("contract")
        or record.get("symbol_out")
        or ""
    ).strip()


def _latest_records_by_symbol(payload: object, reverse: Mapping[str, str], now_ms: int) -> Dict[str, Mapping[str, Any]]:
    latest: Dict[str, Mapping[str, Any]] = {}
    latest_ts: Dict[str, int] = {}
    for record in _iter_payload_records(payload):
        raw_symbol = _record_symbol(record)
        symbol = reverse.get(raw_symbol) or reverse.get(raw_symbol.upper())
        if not symbol and len(reverse) == 1:
            symbol = next(iter(reverse.values()))
        if not symbol:
            continue
        ts_ms = _parse_ts_ms(
            record.get("ts_event") or record.get("ts_recv") or record.get("time") or record.get("timestamp"),
            now_ms,
        )
        if symbol not in latest_ts or ts_ms >= latest_ts[symbol]:
            latest[str(symbol)] = record
            latest_ts[str(symbol)] = int(ts_ms)
    return latest


def ensure_futures_bars_table(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS futures_contract_bars (
            contract TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            open_interest REAL,
            source TEXT,
            PRIMARY KEY(contract, ts_ms)
        )
        """
    )


class FuturesPriceProvider:
    """Duck-typed polling provider for read-only futures pricing."""

    def __init__(
        self,
        *,
        dataset: str | None = None,
        schema: str | None = None,
        stype_in: str | None = None,
        timeout_s: float | None = None,
    ) -> None:
        if requests is None:
            raise RuntimeError(f"futures_unavailable:{type(_REQUESTS_IMPORT_ERROR).__name__}")
        provider = str(os.environ.get("FUTURES_PROVIDER") or "databento").strip().lower()
        if provider not in {"databento", "databento_rest", "db"}:
            _warn_nonfatal(
                "FUTURES_PROVIDER_UNSUPPORTED",
                RuntimeError("unsupported_futures_provider"),
                once_key=f"provider:{provider}",
                provider=provider,
            )
        self.provider = "databento"
        self.dataset = str(dataset or os.environ.get("DATABENTO_DATASET") or "GLBX.MDP3").strip()
        self.schema = str(schema or os.environ.get("DATABENTO_SCHEMA") or "ohlcv-1m").strip()
        self.stype_in = str(stype_in or os.environ.get("DATABENTO_STYPE_IN") or "raw_symbol").strip()
        self.endpoint = str(os.environ.get("DATABENTO_TIMESERIES_URL") or _DATABENTO_TIMESERIES_URL).strip()
        self.timeout_s = float(timeout_s if timeout_s is not None else os.environ.get("DATABENTO_TIMEOUT_S", "10"))

    def _api_key(self) -> str:
        return str(get_data_credential("DATABENTO_API_KEY") or "").strip()

    def _contract_map(self, ticker_map: Dict[str, str]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for raw_symbol, raw_contract in (ticker_map or {}).items():
            symbol = str(raw_symbol or "").strip()
            contract = str(raw_contract or "").strip()
            if symbol and contract:
                out[symbol] = contract
        return out

    def _price_row(self, record: Mapping[str, Any], now_ms: int) -> dict | None:
        close = (
            _finite_float(record.get("close"))
            or _finite_float(record.get("price"))
            or _finite_float(record.get("last"))
            or _finite_float(record.get("settlement_price"))
        )
        bid = _finite_float(record.get("bid") or record.get("bid_px"))
        ask = _finite_float(record.get("ask") or record.get("ask_px"))
        if close is None and bid is not None and ask is not None:
            close = (float(bid) + float(ask)) / 2.0
        if close is None:
            return None
        open_px = _finite_float(record.get("open"))
        high_px = _finite_float(record.get("high"))
        low_px = _finite_float(record.get("low"))
        spread = None
        if bid is not None and ask is not None:
            spread = float(ask) - float(bid)
        ts_ms = _parse_ts_ms(
            record.get("ts_event") or record.get("ts_recv") or record.get("time") or record.get("timestamp"),
            now_ms,
        )
        return {
            "ts_ms": int(ts_ms),
            "price": float(close),
            "open": float(open_px) if open_px is not None else float(close),
            "high": float(high_px) if high_px is not None else float(close),
            "low": float(low_px) if low_px is not None else float(close),
            "close": float(close),
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
            "spread": spread,
            "volume": _finite_float(record.get("volume") or record.get("v")),
            "open_interest": _finite_float(
                record.get("open_interest")
                or record.get("openInterest")
                or record.get("open_interest_qty")
                or record.get("oi")
            ),
            "source": "futures",
        }

    def fetch_last_prices(self, ticker_map: Dict[str, str]) -> Dict[str, dict]:
        contracts_by_symbol = self._contract_map(ticker_map)
        if not contracts_by_symbol:
            return {}
        token = self._api_key()
        if not token:
            _warn_nonfatal(
                "FUTURES_LIVE_MISSING_CREDENTIALS",
                RuntimeError("missing_databento_credentials"),
                once_key="missing_credentials",
            )
            return {}

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - int(float(os.environ.get("DATABENTO_LOOKBACK_S", "900")) * 1000.0)
        reverse = {contract: symbol for symbol, contract in contracts_by_symbol.items()}
        reverse.update({contract.upper(): symbol for symbol, contract in contracts_by_symbol.items()})
        try:
            response = requests.get(
                self.endpoint,
                params={
                    "dataset": self.dataset,
                    "schema": self.schema,
                    "stype_in": self.stype_in,
                    "symbols": ",".join(sorted(set(contracts_by_symbol.values()))),
                    "start": datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc).isoformat(),
                    "end": datetime.fromtimestamp(now_ms / 1000.0, tz=timezone.utc).isoformat(),
                    "encoding": "json",
                    "limit": int(os.environ.get("DATABENTO_SNAPSHOT_LIMIT", "1000")),
                },
                headers={"Authorization": f"Bearer {token}"},
                timeout=float(self.timeout_s),
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            _warn_nonfatal(
                "FUTURES_LIVE_PRICE_REQUEST_FAILED",
                RuntimeError(type(exc).__name__),
                once_key=f"price_request:{type(exc).__name__}",
                endpoint=self.endpoint,
                error_type=type(exc).__name__,
            )
            return {}

        out: Dict[str, dict] = {}
        for symbol, record in _latest_records_by_symbol(payload, reverse, now_ms).items():
            row = self._price_row(record, now_ms)
            if row is not None:
                out[str(symbol)] = row
        return out
