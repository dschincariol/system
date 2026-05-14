"""
FILE: options_polygon.py

Options market-data integration for `options_polygon`.
"""

import time
import logging
import re
from typing import Optional
import requests

from engine.data._credentials import get_data_credential
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger


_BASE = "https://api.polygon.io"
LOG = get_logger("engine.data.options.options_polygon")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "market-impact-dev/1.0 (options_polygon)"})
    return s


def _polygon_key() -> str:
    return get_data_credential("POLYGON_API_KEY")


def _sanitize_error_text(value: object, polygon_key: str | None = None) -> str:
    text = str(value or "")
    key = str(polygon_key or _polygon_key() or "").strip()
    if key:
        text = text.replace(key, "REDACTED")
    text = re.sub(r"([?&](?:apiKey|apikey)=)[^&\\s'\"<>]+", r"\1REDACTED", text, flags=re.IGNORECASE)
    return text


def _sanitize_exception(error: BaseException, polygon_key: str | None = None) -> BaseException:
    message = _sanitize_error_text(repr(error), polygon_key=polygon_key)
    return RuntimeError(message)


def fetch_options_chain_snapshot(
    underlying: str,
    contract_type: Optional[str] = None,          # "call" | "put" | None
    expiration_date: Optional[str] = None,        # "YYYY-MM-DD" | None
    strike_price: Optional[float] = None,         # float | None
    limit: int = 250,
    max_pages: int = 4,
    timeout_s: int = 8,
):
    """
    Returns: (contracts, error)
      contracts: list[dict] with parsed fields for DB insertion
      error: str|None
    """
    polygon_key = _polygon_key()
    if not polygon_key:
        return ([], "POLYGON_API_KEY not set")

    sym = str(underlying).upper().strip()
    if not sym:
        return ([], "empty underlying")

    s = _get_session()
    url = f"{_BASE}/v3/snapshot/options/{sym}"
    params = {"apiKey": polygon_key, "limit": int(limit)}

    if contract_type:
        params["contract_type"] = str(contract_type).lower().strip()
    if expiration_date:
        params["expiration_date"] = str(expiration_date).strip()
    if strike_price is not None:
        params["strike_price"] = float(strike_price)

    out = []
    pages = 0
    err = None

    try:
        while True:
            pages += 1
            r = s.get(url, params=params, timeout=timeout_s)
            r.raise_for_status()
            j = r.json() or {}

            results = j.get("results") or []
            ts_ms = _now_ms()

            for it in results:
                # This adapter normalizes Polygon's nested snapshot payload into
                # the flat contract schema expected by the DB ingestion jobs.
                details = (it.get("details") or {}) if isinstance(it, dict) else {}
                greeks = (it.get("greeks") or {}) if isinstance(it, dict) else {}
                last_quote = (it.get("last_quote") or {}) if isinstance(it, dict) else {}
                last_trade = (it.get("last_trade") or {}) if isinstance(it, dict) else {}

                contract = details.get("ticker") or it.get("ticker")
                exp = details.get("expiration_date")
                ctype = details.get("contract_type")
                strike = details.get("strike_price")

                iv = it.get("implied_volatility")
                oi = it.get("open_interest")

                bid = last_quote.get("bid") or last_quote.get("bid_price")
                ask = last_quote.get("ask") or last_quote.get("ask_price")

                # Polygon commonly uses nanosecond timestamps in some option objects; accept either.
                ts_n = (
                    last_trade.get("sip_timestamp")
                    or last_trade.get("participant_timestamp")
                    or last_quote.get("sip_timestamp")
                    or last_quote.get("participant_timestamp")
                    or None
                )
                if ts_n is not None:
                    try:
                        ts_ms_eff = int(int(ts_n) / 1_000_000)
                    except Exception:
                        ts_ms_eff = ts_ms
                else:
                    ts_ms_eff = ts_ms

                # volume can appear on "day" object or "last_trade"
                day = (it.get("day") or {}) if isinstance(it, dict) else {}
                vol = day.get("volume")
                if vol is None:
                    vol = last_trade.get("size") or last_trade.get("s")

                delta = greeks.get("delta")
                gamma = greeks.get("gamma")
                theta = greeks.get("theta")
                vega = greeks.get("vega")

                if not contract:
                    continue

                out.append(
                    {
                        "ts_ms": int(ts_ms_eff),
                        "underlying": sym,
                        "contract": str(contract),
                        "expiration": (str(exp) if exp is not None else None),
                        "contract_type": (str(ctype) if ctype is not None else None),
                        "strike": (float(strike) if strike is not None else None),
                        "iv": (float(iv) if iv is not None else None),
                        "open_interest": (float(oi) if oi is not None else None),
                        "volume": (float(vol) if vol is not None else None),
                        "bid": (float(bid) if bid is not None else None),
                        "ask": (float(ask) if ask is not None else None),
                        "delta": (float(delta) if delta is not None else None),
                        "gamma": (float(gamma) if gamma is not None else None),
                        "theta": (float(theta) if theta is not None else None),
                        "vega": (float(vega) if vega is not None else None),
                        "source": "polygon",
                    }
                )

            next_url = j.get("next_url")
            if not next_url:
                break
            if pages >= int(max_pages):
                break

            # next_url already includes query params except apiKey in many implementations;
            # safest: follow next_url and pass apiKey again.
            url = str(next_url)
            params = {"apiKey": polygon_key}

        return (out, None)

    except Exception as e:
        err = _sanitize_error_text(repr(e), polygon_key=polygon_key)
        sanitized_error = _sanitize_exception(e, polygon_key=polygon_key)
        log_failure(
            LOG,
            event="options_polygon_fetch_options_chain_snapshot_failed",
            code="OPTIONS_POLYGON_FETCH_OPTIONS_CHAIN_SNAPSHOT_FAILED",
            message="Polygon options chain snapshot fetch failed.",
            error=sanitized_error,
            level=logging.WARNING,
            component="engine.data.options.options_polygon",
            extra={"underlying": sym, "error": err},
            persist=False,
        )
        return (out, err)
