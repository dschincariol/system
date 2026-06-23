"""Read-only OANDA v20 FX price polling adapter."""

from __future__ import annotations

import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Mapping

from engine.data._credentials import get_data_credential
from engine.data.default_symbols import fx_pair_to_oanda_instrument
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

try:
    import requests

    _REQUESTS_IMPORT_ERROR: BaseException | None = None
except Exception as _requests_import_error:  # pragma: no cover - dependency exists in supported envs
    requests = None  # type: ignore[assignment]
    _REQUESTS_IMPORT_ERROR = _requests_import_error


LOG = get_logger("engine.data.live_prices.oanda_live")
_WARNED_NONFATAL_KEYS: set[str] = set()
_PRACTICE_BASE_URL = "https://api-fxpractice.oanda.com"
_LIVE_BASE_URL = "https://api-fxtrade.oanda.com"


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
        component="engine.data.live_prices.oanda_live",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _finite_float(value: object) -> float | None:
    try:
        out = float(str(value).strip())
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _parse_oanda_time_ms(value: object, fallback_ms: int) -> int:
    text = str(value or "").strip()
    if not text:
        return int(fallback_ms)
    try:
        normalized = text.replace("Z", "+00:00")
        return int(datetime.fromisoformat(normalized).astimezone(timezone.utc).timestamp() * 1000)
    except Exception:
        return int(fallback_ms)


def _best_price_component(components: object) -> float | None:
    if not isinstance(components, list):
        return None
    for item in components:
        if not isinstance(item, Mapping):
            continue
        price = _finite_float(item.get("price"))
        if price is not None:
            return price
    return None


def _normalize_environment(value: object) -> str:
    env = str(value or "practice").strip().lower()
    if env == "live":
        return "live"
    return "practice"


def _base_url_for_environment(environment: str) -> str:
    return _LIVE_BASE_URL if _normalize_environment(environment) == "live" else _PRACTICE_BASE_URL


class OANDAPriceProvider:
    """Duck-typed polling provider for OANDA read-only pricing."""

    def __init__(
        self,
        *,
        account_id: str | None = None,
        environment: str | None = None,
        timeout_s: float | None = None,
    ) -> None:
        if requests is None:
            raise RuntimeError(f"oanda_unavailable:{type(_REQUESTS_IMPORT_ERROR).__name__}")
        self.account_id = str(account_id or os.environ.get("OANDA_ACCOUNT_ID") or "").strip()
        self.environment = _normalize_environment(environment or os.environ.get("OANDA_ENVIRONMENT") or "practice")
        self.base_url = _base_url_for_environment(self.environment)
        self.timeout_s = float(timeout_s if timeout_s is not None else os.environ.get("OANDA_TIMEOUT_S", "10"))

    def _access_token(self) -> str:
        return str(get_data_credential("OANDA_ACCESS_TOKEN") or get_data_credential("OANDA_API_KEY") or "").strip()

    def _instrument_map(self, ticker_map: Dict[str, str]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for raw_symbol, raw_instrument in (ticker_map or {}).items():
            symbol = str(raw_symbol or "").strip().upper()
            if not symbol:
                continue
            instrument = str(raw_instrument or "").strip().upper()
            if not instrument:
                try:
                    instrument = fx_pair_to_oanda_instrument(symbol)
                except ValueError:
                    continue
            out[symbol] = instrument
        return out

    def _price_row(self, symbol: str, price: Mapping[str, Any], now_ms: int) -> dict | None:
        bid = _best_price_component(price.get("bids"))
        ask = _best_price_component(price.get("asks"))
        closeout_bid = _finite_float(price.get("closeoutBid"))
        closeout_ask = _finite_float(price.get("closeoutAsk"))
        if bid is None:
            bid = closeout_bid
        if ask is None:
            ask = closeout_ask

        mid = None
        if bid is not None and ask is not None:
            mid = (float(bid) + float(ask)) / 2.0
        elif bid is not None:
            mid = float(bid)
        elif ask is not None:
            mid = float(ask)
        if mid is None:
            return None

        spread = None
        if bid is not None and ask is not None:
            spread = float(ask) - float(bid)
        return {
            "ts_ms": _parse_oanda_time_ms(price.get("time"), now_ms),
            "price": float(mid),
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
            "spread": spread,
            "volume": None,
            "source": "oanda",
        }

    def fetch_last_prices(self, ticker_map: Dict[str, str]) -> Dict[str, dict]:
        instruments_by_symbol = self._instrument_map(ticker_map)
        if not instruments_by_symbol:
            return {}
        token = self._access_token()
        if not token:
            _warn_nonfatal(
                "OANDA_LIVE_MISSING_CREDENTIALS",
                RuntimeError("missing_oanda_credentials"),
                once_key="missing_credentials",
            )
            return {}
        if not self.account_id:
            _warn_nonfatal(
                "OANDA_LIVE_MISSING_ACCOUNT_ID",
                RuntimeError("missing_oanda_account_id"),
                once_key="missing_account_id",
            )
            return {}

        endpoint = f"{self.base_url}/v3/accounts/{self.account_id}/pricing"
        reverse = {instrument: symbol for symbol, instrument in instruments_by_symbol.items()}
        try:
            response = requests.get(
                endpoint,
                params={"instruments": ",".join(sorted(reverse))},
                headers={"Authorization": f"Bearer {token}"},
                timeout=float(self.timeout_s),
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            _warn_nonfatal(
                "OANDA_LIVE_PRICE_REQUEST_FAILED",
                RuntimeError(type(exc).__name__),
                once_key=f"price_request:{type(exc).__name__}",
                endpoint=endpoint,
                error_type=type(exc).__name__,
            )
            return {}

        prices = payload.get("prices") if isinstance(payload, Mapping) else None
        if not isinstance(prices, list):
            return {}

        now_ms = int(time.time() * 1000)
        out: Dict[str, dict] = {}
        for price in prices:
            if not isinstance(price, Mapping):
                continue
            instrument = str(price.get("instrument") or "").strip().upper()
            symbol = reverse.get(instrument)
            if not symbol:
                continue
            row = self._price_row(symbol, price, now_ms)
            if row is not None:
                out[str(symbol)] = row
        return out
