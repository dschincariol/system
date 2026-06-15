"""
FILE: tradier_live.py

Options market-data integration for `tradier_live`.
"""

from __future__ import annotations

import math
import os
import random
import time
from typing import Any, Dict, List, Optional

import requests

from engine.data._credentials import get_data_credential
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.test_isolation import running_python_tests

_BASE = "https://api.tradier.com/v1"
_DEFAULT_TIMEOUT_S = float(os.environ.get("TRADIER_OPTIONS_TIMEOUT_S", "8"))
_MAX_ATTEMPTS = max(1, int(os.environ.get("TRADIER_OPTIONS_MAX_ATTEMPTS", "4")))
_BACKOFF_BASE_S = max(0.1, float(os.environ.get("TRADIER_OPTIONS_BACKOFF_BASE_S", "1.0")))
_BACKOFF_CAP_S = max(_BACKOFF_BASE_S, float(os.environ.get("TRADIER_OPTIONS_BACKOFF_CAP_S", "16.0")))
_MAX_EXPIRIES = max(1, int(os.environ.get("TRADIER_OPTIONS_MAX_EXPIRIES", "3")))
LOG = get_logger("engine.data.options.tradier_live")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="tradier_live_nonfatal",
        code=code,
        message=code,
        error=error,
        level=30,
        component="engine.data.options.tradier_live",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


class TradierFetchError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        kind: str = "request_error",
        status_code: Optional[int] = None,
        retry_after_s: Optional[float] = None,
    ) -> None:
        super().__init__(str(message))
        self.kind = str(kind or "request_error")
        self.status_code = None if status_code is None else int(status_code)
        self.retry_after_s = None if retry_after_s is None else float(retry_after_s)


def _tradier_token() -> str:
    return get_data_credential("TRADIER_API_TOKEN")


def _headers(token: str | None = None) -> Dict[str, str]:
    api_token = str(token if token is not None else _tradier_token()).strip()
    return {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json",
    }


def _clip_error(value: Any) -> str:
    return str(value or "").strip()[:400]


def _safe_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal(
            "TRADIER_LIVE_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float",
            value=repr(value)[:120],
        )
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _safe_int(value: Any) -> Optional[int]:
    out = _safe_float(value)
    if out is None:
        return None
    return int(out)


def _normalize_call_put(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"c", "call"}:
        return "C"
    if text in {"p", "put"}:
        return "P"
    return ""


def _parse_retry_after_s(response: requests.Response) -> Optional[float]:
    raw = str(response.headers.get("Retry-After") or "").strip()
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except Exception as e:
        _warn_nonfatal(
            "TRADIER_LIVE_RETRY_AFTER_PARSE_FAILED",
            e,
            once_key="retry_after_parse",
            retry_after=raw,
        )
        return None


def _backoff_sleep_s(attempt: int, retry_after_s: Optional[float] = None) -> float:
    if retry_after_s is not None and retry_after_s > 0.0:
        base = float(retry_after_s)
    else:
        base = min(_BACKOFF_CAP_S, _BACKOFF_BASE_S * (2 ** max(0, int(attempt))))
    jitter = min(1.0, max(0.0, base * 0.15)) * random.random()
    return min(_BACKOFF_CAP_S, base + jitter)


def _request_json(
    session: requests.Session,
    path: str,
    *,
    params: Dict[str, Any],
    timeout_s: float,
    api_token: str,
) -> Dict[str, Any]:
    last_error: Optional[TradierFetchError] = None

    for attempt in range(_MAX_ATTEMPTS):
        try:
            response = session.get(
                f"{_BASE}{path}",
                params=params,
                headers=_headers(api_token),
                timeout=float(timeout_s),
            )
        except requests.RequestException as exc:
            last_error = TradierFetchError(
                f"tradier_request_failed:{type(exc).__name__}:{exc}",
                kind="transport_error",
            )
            _warn_nonfatal(
                "TRADIER_LIVE_REQUEST_FAILED",
                exc,
                once_key=f"request:{path}",
                path=str(path),
                attempt=int(attempt) + 1,
            )
            if attempt >= (_MAX_ATTEMPTS - 1):
                raise last_error
            time.sleep(_backoff_sleep_s(attempt))
            continue

        retry_after_s = _parse_retry_after_s(response)
        status_code = int(response.status_code or 0)

        if status_code == 429:
            last_error = TradierFetchError(
                f"tradier_rate_limited:{status_code}",
                kind="rate_limit",
                status_code=status_code,
                retry_after_s=retry_after_s,
            )
            if attempt >= (_MAX_ATTEMPTS - 1):
                raise last_error
            time.sleep(_backoff_sleep_s(attempt, retry_after_s))
            continue

        if status_code >= 500:
            last_error = TradierFetchError(
                f"tradier_server_error:{status_code}",
                kind="server_error",
                status_code=status_code,
            )
            if attempt >= (_MAX_ATTEMPTS - 1):
                raise last_error
            time.sleep(_backoff_sleep_s(attempt))
            continue

        if status_code >= 400:
            body = _clip_error(response.text)
            raise TradierFetchError(
                f"tradier_http_error:{status_code}:{body}",
                kind="http_error",
                status_code=status_code,
                retry_after_s=retry_after_s,
            )

        try:
            payload = response.json()
        except ValueError as exc:
            raise TradierFetchError(
                f"tradier_invalid_json:{exc}",
                kind="invalid_json",
                status_code=status_code,
            ) from exc

        if not isinstance(payload, dict):
            raise TradierFetchError(
                "tradier_invalid_payload:expected_object",
                kind="schema_error",
                status_code=status_code,
            )
        return payload

    if last_error is not None:
        raise last_error
    raise TradierFetchError("tradier_request_exhausted", kind="request_error")


def _extract_expirations(payload: Dict[str, Any]) -> List[str]:
    expirations = payload.get("expirations")
    if not isinstance(expirations, dict):
        raise TradierFetchError("tradier_expirations_schema_invalid", kind="schema_error")

    dates = expirations.get("date")
    if dates is None:
        return []
    if isinstance(dates, str):
        dates = [dates]
    if not isinstance(dates, list):
        raise TradierFetchError("tradier_expirations_dates_invalid", kind="schema_error")

    out: List[str] = []
    for value in dates:
        text = str(value or "").strip()
        if text:
            out.append(text)
    return out


def _extract_chain_rows(expiry: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    options = payload.get("options")
    if options is None:
        return []
    if not isinstance(options, dict):
        raise TradierFetchError("tradier_chain_schema_invalid", kind="schema_error")

    raw_rows = options.get("option")
    if raw_rows is None:
        return []
    if isinstance(raw_rows, dict):
        raw_rows = [raw_rows]
    if not isinstance(raw_rows, list):
        raise TradierFetchError("tradier_chain_rows_invalid", kind="schema_error")

    rows: List[Dict[str, Any]] = []
    invalid_rows = 0
    for raw in raw_rows:
        if not isinstance(raw, dict):
            invalid_rows += 1
            continue
        strike = _safe_float(raw.get("strike"))
        call_put = _normalize_call_put(raw.get("option_type"))
        if strike is None or not call_put:
            invalid_rows += 1
            continue
        rows.append(
            {
                "expiry": str(expiry),
                "strike": float(strike),
                "call_put": call_put,
                "iv": _safe_float(raw.get("implied_volatility")),
                "open_interest": _safe_int(raw.get("open_interest")),
                "volume": _safe_int(raw.get("volume")),
            }
        )

    if invalid_rows > 0 and not rows:
        raise TradierFetchError(
            f"tradier_chain_rows_malformed:{invalid_rows}",
            kind="schema_error",
        )
    return rows


def fetch_options_chain(
    symbol: str,
    *,
    session: Optional[requests.Session] = None,
    max_expiries: Optional[int] = None,
    timeout_s: Optional[float] = None,
    api_token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Returns:
      {
        "rows": [{ expiry, strike, call_put, iv, open_interest, volume }, ...],
        "meta": {...}
      }
    """
    token = str(api_token or _tradier_token() or "").strip()
    if not token and session is not None and running_python_tests():
        token = "test-token"
    if not token:
        raise TradierFetchError("tradier_api_token_missing", kind="config_error")

    sym = str(symbol or "").upper().strip()
    if not sym:
        raise TradierFetchError("tradier_symbol_missing", kind="request_error")

    timeout_s = float(timeout_s or _DEFAULT_TIMEOUT_S)
    limit_expiries = max(1, int(max_expiries or _MAX_EXPIRIES))

    own_session = session is None
    session = session or requests.Session()
    try:
        expirations_payload = _request_json(
            session,
            "/markets/options/expirations",
            params={"symbol": sym},
            timeout_s=timeout_s,
            api_token=token,
        )
        expirations = _extract_expirations(expirations_payload)[:limit_expiries]

        rows: List[Dict[str, Any]] = []
        for expiry in expirations:
            chain_payload = _request_json(
                session,
                "/markets/options/chains",
                params={
                    "symbol": sym,
                    "expiration": expiry,
                    "greeks": "true",
                },
                timeout_s=timeout_s,
                api_token=token,
            )
            rows.extend(_extract_chain_rows(expiry, chain_payload))

        return {
            "rows": rows,
            "meta": {
                "provider": "tradier",
                "symbol": sym,
                "expiries": list(expirations),
                "schema_valid": True,
            },
        }
    finally:
        if own_session:
            try:
                session.close()
            except Exception as e:
                _warn_nonfatal("TRADIER_LIVE_SESSION_CLOSE_FAILED", e)
