"""Disabled-by-default equity symbol lifecycle retirement job."""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, Optional

from engine.data._credentials import get_data_credential
from engine.data.universe_lifecycle import reference_lifecycle_enabled, run_lifecycle_once, universe_lifecycle_enabled
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    acquire_job_lock,
    connect,
    init_db,
    put_job_heartbeat,
    release_job_lock,
)

JOB_NAME = "retire_delisted_symbols"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
POLYGON_REFERENCE_URL = "https://api.polygon.io/v3/reference/tickers/{symbol}"
FMP_REFERENCE_URL = "https://financialmodelingprep.com/api/v3/profile/{symbol}"

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [retire_delisted_symbols] %(message)s",
)
LOG = get_logger("engine.data.jobs.retire_delisted_symbols")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(code),
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.retire_delisted_symbols",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _polygon_reference_fetcher(api_key: str) -> Callable[[str], dict[str, Any]]:
    def _fetch(symbol: str) -> dict[str, Any]:
        import requests

        response = requests.get(
            POLYGON_REFERENCE_URL.format(symbol=str(symbol).upper().strip()),
            params={"apiKey": api_key},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    return _fetch


def _fmp_reference_fetcher(api_key: str) -> Callable[[str], dict[str, Any]]:
    def _fetch(symbol: str) -> dict[str, Any]:
        import requests

        response = requests.get(
            FMP_REFERENCE_URL.format(symbol=str(symbol).upper().strip()),
            params={"apikey": api_key},
            timeout=10,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list) and payload and isinstance(payload[0], dict):
            return {"results": dict(payload[0])}
        return payload if isinstance(payload, dict) else {}

    return _fetch


def build_reference_fetcher() -> tuple[Optional[Callable[[str], Any]], dict[str, Any]]:
    """Build the optional read-only reference fetcher without exposing secrets."""
    if not reference_lifecycle_enabled():
        return None, {"reference_enabled": False}
    polygon_key = str(get_data_credential("POLYGON_API_KEY") or "").strip()
    if polygon_key:
        return _polygon_reference_fetcher(polygon_key), {
            "reference_enabled": True,
            "reference_fetcher_configured": True,
            "reference_provider": "polygon",
        }
    fmp_key = str(get_data_credential("FMP_API_KEY") or "").strip()
    if fmp_key:
        return _fmp_reference_fetcher(fmp_key), {
            "reference_enabled": True,
            "reference_fetcher_configured": True,
            "reference_provider": "fmp",
        }
    if not polygon_key and not fmp_key:
        return None, {
            "reference_enabled": True,
            "reference_fetcher_configured": False,
            "reference_blocked": True,
            "reference_blocker": "missing_polygon_or_fmp_api_key",
        }
    return None, {"reference_enabled": True, "reference_fetcher_configured": False}


def main() -> int:
    """Run the equity lifecycle retirement pass once and emit a JSON summary."""
    init_db()
    if not universe_lifecycle_enabled():
        summary = {"ok": True, "enabled": False, "job": JOB_NAME}
        print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
        return 0

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        return 2

    con = connect()
    try:
        fetcher, reference_meta = build_reference_fetcher()
        summary = run_lifecycle_once(con, fetch_reference=fetcher)
        summary["job"] = JOB_NAME
        summary.update(reference_meta)
        con.commit()
        try:
            put_job_heartbeat(
                JOB_NAME,
                OWNER,
                PID,
                extra_json=json.dumps(summary, separators=(",", ":"), sort_keys=True),
            )
        except Exception as e:
            _warn_nonfatal("RETIRE_DELISTED_SYMBOLS_HEARTBEAT_FAILED", e, once_key="heartbeat")
        logging.info(
            "equity symbol lifecycle pass complete summary=%s",
            json.dumps(summary, separators=(",", ":"), sort_keys=True),
        )
        print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
        return 0
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("RETIRE_DELISTED_SYMBOLS_CLOSE_FAILED", e, once_key="close")
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("RETIRE_DELISTED_SYMBOLS_RELEASE_LOCK_FAILED", e, once_key="release_lock")


if __name__ == "__main__":
    raise SystemExit(main())
