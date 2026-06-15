"""
FILE: edgar_live.py

SEC/EDGAR data integration for `edgar_live`.
"""

import os
import json
import time
import requests
from pathlib import Path

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

SEC_UA = os.environ.get("SEC_USER_AGENT", "market-impact-dev (contact: ops@example.com)")
SEC_FROM = os.environ.get("SEC_FROM")  # optional but recommended by SEC guidance

HEADERS = {"User-Agent": SEC_UA}
if SEC_FROM:
    HEADERS["From"] = SEC_FROM

TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"


def _ticker_map_cache_path() -> Path:
    raw = str(os.environ.get("SEC_TICKER_MAP_CACHE", "data/sec_company_tickers_exchange.json") or "").strip()
    path = Path(raw)
    if path.is_absolute():
        return path
    return (Path(__file__).resolve().parents[3] / path).resolve()


CACHE_PATH = _ticker_map_cache_path()
CACHE_MAX_AGE_S = int(os.environ.get("SEC_TICKER_MAP_MAX_AGE_S", str(24 * 3600)))
LOG = get_logger("engine.data.sec.edgar_live")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.data.sec.edgar_live",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _download_ticker_map() -> dict:
    r = requests.get(TICKER_MAP_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()


def _load_ticker_map() -> dict:
    try:
        if CACHE_PATH.exists():
            age = time.time() - CACHE_PATH.stat().st_mtime
            if age <= CACHE_MAX_AGE_S:
                return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        _warn_nonfatal(
            "EDGAR_LIVE_TICKER_MAP_CACHE_READ_FAILED",
            e,
            once_key="edgar_live_ticker_map_cache_read",
            cache_path=str(CACHE_PATH),
        )

    mp = _download_ticker_map()
    try:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        CACHE_PATH.write_text(json.dumps(mp), encoding="utf-8")
    except Exception as e:
        _warn_nonfatal(
            "EDGAR_LIVE_TICKER_MAP_CACHE_WRITE_FAILED",
            e,
            once_key="edgar_live_ticker_map_cache_write",
            cache_path=str(CACHE_PATH),
        )
    return mp


def _ticker_map_rows(payload: object) -> list[dict]:
    rows: list[dict] = []
    if isinstance(payload, list):
        for row in payload:
            if isinstance(row, dict):
                rows.append(dict(row))
        return rows

    if not isinstance(payload, dict):
        return rows

    fields = payload.get("fields") if isinstance(payload.get("fields"), list) else None
    data_rows = payload.get("data") if isinstance(payload.get("data"), list) else None
    if fields and data_rows is not None:
        field_names = [str(name or "").strip() for name in fields]
        for row in data_rows:
            if isinstance(row, dict):
                rows.append(dict(row))
                continue
            if not isinstance(row, (list, tuple)):
                continue
            normalized: dict = {}
            for idx, field_name in enumerate(field_names):
                if not field_name or idx >= len(row):
                    continue
                normalized[field_name] = row[idx]
            if normalized:
                rows.append(normalized)
        return rows

    for row in payload.values():
        if isinstance(row, dict):
            rows.append(dict(row))
    return rows


def ticker_to_cik(ticker: str) -> str:
    """
    Returns zero-padded 10-digit CIK string, or "" if not found.
    """
    t = (ticker or "").upper().strip()
    if not t:
        return ""

    mp = _load_ticker_map()

    try:
        rows = _ticker_map_rows(mp)
    except Exception as e:
        _warn_nonfatal(
            "EDGAR_LIVE_TICKER_MAP_ROWS_FAILED",
            e,
            once_key="edgar_live_ticker_map_rows",
            payload_type=type(mp).__name__,
        )
        rows = []

    for r in rows:
        try:
            if str(r.get("ticker", "")).upper() == t:
                cik = str(r.get("cik", "") or r.get("cik_str", "")).strip()
                if not cik:
                    continue
                return cik.zfill(10)
        except Exception as e:
            _warn_nonfatal("EDGAR_LIVE_CIK_PARSE_FAILED", e, once_key="cik_parse", row=repr(r)[:200])
            continue
    return ""


def fetch_recent_filings(ticker: str, limit: int = 25) -> list:
    """
    Returns list of dicts:
      { accession, form, filed_date, report_date, cik, company_name, primary_doc_url }
    """
    cik = ticker_to_cik(ticker)
    if not cik:
        return []

    url = SUBMISSIONS_URL.format(cik=cik)
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    j = r.json() or {}

    company_name = j.get("name")
    recent = (j.get("filings") or {}).get("recent") or {}

    forms = recent.get("form") or []
    accs = recent.get("accessionNumber") or []
    fdates = recent.get("filingDate") or []
    acceptance_datetimes = recent.get("acceptanceDateTime") or []
    rdates = recent.get("reportDate") or []
    prim_docs = recent.get("primaryDocument") or []

    out = []
    n = min(len(forms), len(accs), len(fdates), len(prim_docs))
    for i in range(n):
        try:
            acc = str(accs[i])
            form = str(forms[i])
            fd = str(fdates[i])
            rd = str(rdates[i]) if i < len(rdates) and rdates[i] else None
            primary = str(prim_docs[i])
            acceptance_datetime = (
                str(acceptance_datetimes[i])
                if i < len(acceptance_datetimes) and acceptance_datetimes[i]
                else None
            )

            # construct primary doc URL (standard SEC archives path)
            acc_nodash = acc.replace("-", "")
            primary_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_nodash}/{primary}"

            out.append(
                {
                    "accession": acc,
                    "form": form,
                    "filed_date": fd,
                    "acceptance_datetime": acceptance_datetime,
                    "report_date": rd,
                    "cik": cik,
                    "company_name": company_name,
                    "primary_doc_url": primary_url,
                }
            )
        except Exception as e:
            _warn_nonfatal("EDGAR_LIVE_FILING_PARSE_FAILED", e, once_key="filing_parse", filing=f"index={i}")
            continue

        if len(out) >= int(limit):
            break

    return out
