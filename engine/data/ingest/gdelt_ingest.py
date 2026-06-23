"""
FILE: gdelt_ingest.py

Ingestion pipeline module for `gdelt_ingest`.
"""

import time
import json
import hashlib
import logging
import datetime as _dt
import os
from typing import Dict, Any, List, Tuple

import requests
from engine.data.time_utils import utc_ms_from_datetime
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("data.ingest.gdelt_ingest")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="data_gdelt_ingest_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.ingest.gdelt_ingest",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"
_GDELT_COOLDOWN_UNTIL_S = 0.0
_GDELT_DEFAULT_RETRY_AFTER_S = float(os.environ.get("GDELT_DEFAULT_RETRY_AFTER_S", "300"))
_GDELT_HTTP_UA = os.environ.get("GDELT_HTTP_UA", "trading-system/1.0 gdelt-feed")

# ------            -- ------------------------------------------------------
# Symbol-aware keyword packs (profit-focused, conservative)
# ------            -- ------------------------------------------------------

_SYMBOL_KEYWORDS = {
    "SPY": [
        "fed", "fomc", "rates", "inflation", "cpi", "pce",
        "treasury", "bond yields", "equities", "recession",
        "earnings", "guidance"
    ],
    "BTC": [
        "bitcoin", "btc", "crypto", "cryptocurrency",
        "etf", "spot etf", "sec", "regulation",
        "coinbase", "binance", "on-chain"
    ],
    "ETH": [
        "ethereum", "eth", "defi", "layer 2",
        "staking", "gas fees", "rollup"
    ],
    "OIL": [
        "oil", "crude", "wti", "brent",
        "opec", "production cut", "supply",
        "middle east", "red sea"
    ],
}

# Domains that consistently move markets
_DOMAIN_ALLOW = {
    "reuters.com",
    "bloomberg.com",
    "wsj.com",
    "ft.com",
    "cnbc.com",
    "marketwatch.com",
    "cointelegraph.com",
    "coindesk.com",
}

# Known low-signal / spammy domains (hard filter)
_DOMAIN_DENY = {
    "medium.com",
    "substack.com",
    "reddit.com",
    "twitter.com",
    "facebook.com",
    "youtube.com",
}

def _ts_ms_from_gdelt_seen(seendate: str) -> int:
    # seendate often like "2025-01-02 12:34:56" or ISO-ish; best-effort
    s = (seendate or "").strip()
    if not s:
        return int(time.time() * 1000)

    # Fast path: YYYY-MM-DD HH:MM:SS
    try:
        if len(s) >= 19 and s[4] == "-" and s[7] == "-" and s[10] in (" ", "T"):
            s2 = s[:19].replace("T", " ")
            parsed = _dt.datetime.strptime(s2, "%Y-%m-%d %H:%M:%S").replace(tzinfo=_dt.timezone.utc)
            return utc_ms_from_datetime(parsed, field_name="gdelt_seen_date")
    except Exception as e:
        _warn_nonfatal(
            "GDELT_INGEST_PARSE_TS_FAST_PATH_FAILED",
            e,
            once_key="parse_ts_fast_path",
            seendate=s[:120],
        )

    # Fallback: now
    return int(time.time() * 1000)


def _event_key(url: str, title: str, source: str) -> str:
    h = hashlib.sha1()
    h.update((url or "").encode("utf-8", errors="ignore"))
    h.update(b"\n")
    h.update((title or "").encode("utf-8", errors="ignore"))
    h.update(b"\n")
    h.update((source or "").encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _mk_queries_by_symbol(symbols: List[str]) -> Dict[str, str]:
    """
    Returns { symbol: gdelt_query_string }
    """
    out = {}
    for sym in symbols or []:
        s = sym.upper().strip()
        kws = _SYMBOL_KEYWORDS.get(s)
        if not kws:
            # fallback: symbol literal only
            out[s] = s
            continue

        # Keep GDELT queries narrow and symbol-aware. This is a recall/precision
        # tradeoff: we prefer missing some articles over flooding events with noise.
        # (SYM AND (kw1 OR kw2 OR ...))
        ors = " OR ".join(kws[:12])
        out[s] = f"({s}) AND ({ors})"
    return out


def _retry_after_s(response: Any, default_s: float) -> float:
    raw = ""
    try:
        raw = str((getattr(response, "headers", {}) or {}).get("Retry-After") or "").strip()
        if raw:
            return max(1.0, float(raw))
    except Exception as exc:
        _warn_nonfatal(
            "GDELT_RETRY_AFTER_PARSE_FAILED",
            exc,
            once_key="retry_after_parse",
            retry_after=raw,
        )
    return float(default_s)


def _set_gdelt_cooldown(seconds: float) -> None:
    global _GDELT_COOLDOWN_UNTIL_S
    _GDELT_COOLDOWN_UNTIL_S = max(float(_GDELT_COOLDOWN_UNTIL_S), time.time() + max(1.0, float(seconds)))


def gdelt_cooldown_remaining_s() -> float:
    return max(0.0, float(_GDELT_COOLDOWN_UNTIL_S) - time.time())


def _gdelt_error_for_response(response: Any, *, sym: str) -> str | None:
    status_code = int(getattr(response, "status_code", 0) or 0)
    if status_code == 429:
        retry_after = _retry_after_s(response, _GDELT_DEFAULT_RETRY_AFTER_S)
        _set_gdelt_cooldown(retry_after)
        return f"gdelt_rate_limited sym={sym} status_code=429 retry_after_s={retry_after:.0f}"
    if status_code == 503:
        retry_after = _retry_after_s(response, 300.0)
        _set_gdelt_cooldown(retry_after)
        return f"gdelt_temporarily_unavailable sym={sym} status_code=503 retry_after_s={retry_after:.0f}"
    if status_code == 401:
        return f"gdelt_credentials_rejected sym={sym} status_code=401"
    if status_code == 403:
        return f"gdelt_entitlement_missing sym={sym} status_code=403"
    if status_code >= 400:
        return f"gdelt_http_error sym={sym} status_code={status_code}"
    return None

def ingest_gdelt_doc(
    *,
    symbols: List[str],
    lookback_minutes: int = 60,
    maxrecords: int = 250,
    format_: str = "json",
    language: str = "english",
) -> Tuple[List[Dict[str, Any]], List[str]]:

    """
    Returns (items, errors)

    Each item is compatible with ingest_now -> put_event:
      {
        ts_ms, source, title, body, url, event_key, meta_json
      }
    """
    errors: List[str] = []

    queries = _mk_queries_by_symbol(symbols)
    if not queries:
        return [], errors
    cooldown_remaining = gdelt_cooldown_remaining_s()
    if cooldown_remaining > 0:
        return [], [f"gdelt_rate_limited cooldown_remaining_s={cooldown_remaining:.0f}"]

    now = int(time.time())
    start_ts = now - int(lookback_minutes) * 60

    # GDELT expects STARTDATETIME / ENDDATETIME in YYYYMMDDHHMMSS
    try:
        start_dt = _dt.datetime.fromtimestamp(start_ts, _dt.timezone.utc).strftime("%Y%m%d%H%M%S")
        end_dt = _dt.datetime.fromtimestamp(now, _dt.timezone.utc).strftime("%Y%m%d%H%M%S")
    except Exception:
        start_dt = None
        end_dt = None

    base_params = {
        "mode": "ArtList",
        "format": format_,
        "maxrecords": int(max(1, min(250, maxrecords))),
        "sort": "datedesc",
    }

    arts = []

    for sym, q in queries.items():
        params = dict(base_params)
        params["query"] = q

        if start_dt and end_dt:
            params["startdatetime"] = start_dt
            params["enddatetime"] = end_dt
        if language:
            params["sourcelang"] = language

        try:
            r = requests.get(_BASE, params=params, headers={"User-Agent": _GDELT_HTTP_UA, "Accept": "application/json"}, timeout=20)
            problem = _gdelt_error_for_response(r, sym=str(sym))
            if problem:
                errors.append(problem)
                if "rate_limited" in problem or "temporarily_unavailable" in problem:
                    break
                continue
            r.raise_for_status()
            j = r.json() if format_ == "json" else {}
            if format_ == "json" and not isinstance(j, dict):
                errors.append(f"gdelt_malformed_payload sym={sym} payload_type={type(j).__name__}")
                continue
            arts.extend(
                [
                    {
                        **dict(article or {}),
                        "matched_symbol": str(sym),
                    }
                    for article in ((j or {}).get("articles") or [])
                    if isinstance(article, dict)
                ]
            )
        except Exception as e:
            errors.append(f"gdelt_fetch_failed sym={sym}: {e!r}")

    out: List[Dict[str, Any]] = []

    for a in arts:
        try:
            url = str(a.get("url") or "").strip()
            title = str(a.get("title") or "").strip()
            if not url or not title:
                continue

            seendate = a.get("seendate") or a.get("seenDate") or ""
            ts_ms = _ts_ms_from_gdelt_seen(str(seendate))

            domain = a.get("domain")
            if domain:
                d = str(domain).lower()
                if d in _DOMAIN_DENY:
                    continue
                if _DOMAIN_ALLOW and d not in _DOMAIN_ALLOW:
                    continue

            lang = a.get("language")
            sourcelang = a.get("sourcelang") or a.get("sourceLanguage")
            tone = a.get("tone")
            source_country = a.get("sourceCountry")
            source_collection = a.get("sourceCollectionIdentifier")
            socialimage = a.get("socialimage")
            theme = a.get("theme")
            themes = a.get("themes")
            persons = a.get("persons")
            organizations = a.get("organizations")
            locations = a.get("locations")

            # The event row stays intentionally small; richer provider-specific
            # structure lives in meta_json so the canonical events table stays lean.
            meta = {
                "provider": "gdelt",
                "matched_symbol": str(a.get("matched_symbol") or ""),
                "gdelt": {

                    "domain": domain,
                    "language": lang,
                    "sourcelang": sourcelang,
                    "tone": tone,
                    "source_country": source_country,
                    "source_collection": source_collection,
                    "socialimage": socialimage,
                    "theme": theme,
                    "themes": themes,
                    "persons": persons,
                    "organizations": organizations,
                    "locations": locations,
                },
            }

            # Keep body minimal to avoid bloating DB; structured fields go in meta_json.
            body = ""
            src = "gdelt"

            out.append(
                {
                    "ts_ms": int(ts_ms),
                    "source": src,
                    "title": title,
                    "body": body,
                    "url": url,
                    "event_key": _event_key(url, title, src),
                    "meta_json": json.dumps(meta, separators=(",", ":"), sort_keys=True),
                }
            )
        except Exception as e:
            _warn_nonfatal(
                "GDELT_INGEST_ARTICLE_TRANSFORM_FAILED",
                e,
                once_key="article_transform",
            )
            continue

    return out, errors
