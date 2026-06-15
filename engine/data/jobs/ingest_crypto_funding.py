"""Disabled-by-default crypto perpetual funding ingestion daemon.

README:
- Source: existing CCXT exchange connections for perpetual funding history,
  live funding, and spot/perp ticker snapshots.
- Cadence: settlement-aligned polling at configured UTC marks (default
  00:00/08:00/16:00 UTC) with ``CRYPTO_FUNDING_POLL_SECONDS`` retained as the
  fallback cadence when alignment is disabled.
- Availability lag: funding rows become available at the exchange funding
  timestamp, and feature joins use that timestamp only.
- Caveats: endpoint support differs by exchange. Missing funding endpoints are
  logged and skipped without failing the scheduler.
"""

from __future__ import annotations

import json
import logging
import os
import time
import calendar
from collections import defaultdict
from typing import Any, Dict, List

from engine.data.crypto_positioning import (
    CryptoPerpMarket,
    build_ccxt_exchange,
    load_crypto_perp_markets,
    poll_exchange_funding,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.ingestion_status import record_pipeline_status
from engine.runtime.storage import (
    acquire_job_lock,
    init_db,
    put_crypto_funding_rate,
    put_job_heartbeat,
    release_job_lock,
    run_write_txn,
    touch_job_lock,
)
from services.data_source_manager import get_manager

JOB_NAME = (os.environ.get("ENGINE_JOB_NAME") or "ingest_crypto_funding").strip() or "ingest_crypto_funding"
OWNER = os.environ.get("JOB_OWNER", os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")))
PID = os.getpid()

INGEST_ENABLED = os.environ.get("INGEST_CRYPTO_FUNDING_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
POLL_SECONDS = float(os.environ.get("CRYPTO_FUNDING_POLL_SECONDS", "3600"))
ALIGN_TO_SETTLEMENT_MARKS = os.environ.get("CRYPTO_FUNDING_ALIGN_TO_SETTLEMENT_MARKS", "1").strip().lower() in {"1", "true", "yes", "on"}
SETTLEMENT_HOURS_UTC = os.environ.get("CRYPTO_FUNDING_SETTLEMENT_HOURS_UTC", "0,8,16")
SETTLEMENT_LAG_SECONDS = float(os.environ.get("CRYPTO_FUNDING_SETTLEMENT_LAG_SECONDS", "60"))
HISTORY_LOOKBACK_HOURS = max(1, int(os.environ.get("CRYPTO_FUNDING_HISTORY_LOOKBACK_HOURS", "72")))
HISTORY_LIMIT = max(1, int(os.environ.get("CRYPTO_FUNDING_HISTORY_LIMIT", "32")))
LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "300"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))
MAX_BACKOFF_SECONDS = float(os.environ.get("CRYPTO_FUNDING_MAX_BACKOFF_SECONDS", "3600"))

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format=f"%(asctime)s %(levelname)s [{JOB_NAME}] %(message)s",
)
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _group_markets(markets: List[CryptoPerpMarket]) -> Dict[str, List[CryptoPerpMarket]]:
    grouped: Dict[str, List[CryptoPerpMarket]] = defaultdict(list)
    for market in markets or []:
        grouped[str(market.exchange_id or "").strip().lower()].append(market)
    return {exchange: rows for exchange, rows in grouped.items() if exchange and rows}


def _settlement_hours_utc() -> List[int]:
    hours = []
    for part in str(SETTLEMENT_HOURS_UTC or "").split(","):
        try:
            hour = int(str(part).strip())
        except Exception:
            continue
        if 0 <= hour <= 23:
            hours.append(int(hour))
    return sorted(set(hours)) or [0, 8, 16]


def seconds_until_next_funding_mark(now_s: float | None = None) -> float:
    """Return seconds until the next configured UTC funding mark plus lag."""
    if not bool(ALIGN_TO_SETTLEMENT_MARKS):
        return max(1.0, float(POLL_SECONDS))
    now = float(time.time() if now_s is None else now_s)
    tm = time.gmtime(now)
    day_start = calendar.timegm((tm.tm_year, tm.tm_mon, tm.tm_mday, 0, 0, 0))
    lag = max(0.0, float(SETTLEMENT_LAG_SECONDS))
    candidates = []
    for day_offset in (0, 1):
        base = int(day_start + (day_offset * 24 * 3600))
        for hour in _settlement_hours_utc():
            candidates.append(float(base + (int(hour) * 3600) + lag))
    future = [candidate for candidate in candidates if candidate > now]
    if not future:
        return max(1.0, float(POLL_SECONDS))
    return max(1.0, float(min(future) - now))


def _run_once() -> bool:
    manager = get_manager()
    markets = load_crypto_perp_markets()
    grouped = _group_markets(markets)
    since_ms = int(time.time() * 1000) - int(HISTORY_LOOKBACK_HOURS * 3600 * 1000)
    rows: List[Dict[str, Any]] = []
    errors: List[str] = []

    for exchange_id, exchange_markets in grouped.items():
        try:
            exchange = build_ccxt_exchange(exchange_id)
        except Exception as exc:
            errors.append(f"{exchange_id}:exchange:{exc}")
            _warn_nonfatal("INGEST_CRYPTO_FUNDING_EXCHANGE_INIT_FAILED", exc, once_key=f"exchange:{exchange_id}", exchange=exchange_id)
            continue
        try:
            exchange_rows, exchange_errors = poll_exchange_funding(
                exchange,
                exchange_markets,
                since_ms=int(since_ms),
                history_limit=int(HISTORY_LIMIT),
                include_live=True,
            )
            rows.extend(exchange_rows)
            errors.extend([f"{exchange_id}:{err}" for err in exchange_errors])
        except Exception as exc:
            errors.append(f"{exchange_id}:poll:{exc}")
            _warn_nonfatal("INGEST_CRYPTO_FUNDING_EXCHANGE_POLL_FAILED", exc, once_key=f"poll:{exchange_id}", exchange=exchange_id)

    def _write(conw) -> int:
        written = 0
        for row in rows:
            written += int(put_crypto_funding_rate(row, con=conw) or 0)
        return int(written)

    written = 0
    if rows:
        written = int(
            run_write_txn(
                _write,
                table="crypto_funding_rates",
                operation="ingest_crypto_funding_rates",
                context={"job": JOB_NAME, "rows": int(len(rows))},
            )
            or 0
        )

    ok = not bool(errors)
    last_ts = max([int(row.get("availability_ts_ms") or row.get("ingested_ts_ms") or 0) for row in rows] or [int(time.time() * 1000)])
    status = record_pipeline_status(
        JOB_NAME,
        ok=ok,
        raw_rows=int(len(rows)),
        event_rows=0,
        last_ingested_ts_ms=int(last_ts),
        error="; ".join(errors[:8]) if errors else None,
        meta={
            "markets": int(len(markets)),
            "exchanges": sorted(grouped.keys()),
            "rows": int(len(rows)),
            "written": int(written),
            "errors": int(len(errors)),
            "poll_seconds": float(POLL_SECONDS),
            "settlement_aligned": bool(ALIGN_TO_SETTLEMENT_MARKS),
            "settlement_hours_utc": _settlement_hours_utc(),
            "settlement_lag_seconds": float(SETTLEMENT_LAG_SECONDS),
        },
    )
    manager.record_job_status(
        JOB_NAME,
        ok=ok,
        message="crypto funding cycle complete" if ok else "crypto funding cycle degraded",
        error="; ".join(errors[:8]) if errors else None,
        meta={"markets": int(len(markets)), "rows": int(len(rows)), "written": int(written), "errors": int(len(errors))},
    )
    put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
    return bool(ok)


def main() -> None:
    """Run the supervised crypto funding ingestion loop."""
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("ingest_crypto_funding must be launched by supervisor")
        raise SystemExit(1)

    manager = get_manager()
    if not INGEST_ENABLED:
        manager.record_job_status(JOB_NAME, ok=True, message="crypto funding disabled by env flag")
        return
    if not manager.is_job_enabled(JOB_NAME, default=False):
        manager.record_job_status(JOB_NAME, ok=True, message="crypto funding disabled by data source control plane")
        return

    init_db()
    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        raise SystemExit(2)

    last_hb_s = 0.0
    backoff_s = 1.0
    try:
        while True:
            if not manager.is_job_enabled(JOB_NAME, default=False):
                manager.record_job_status(JOB_NAME, ok=True, message="crypto funding disabled by data source control plane")
                break
            now_s = time.time()
            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps({"poll_seconds": float(POLL_SECONDS)}, separators=(",", ":"), sort_keys=True),
                )
                last_hb_s = now_s
            try:
                ok = _run_once()
                if ok:
                    backoff_s = 1.0
                    sleep_s = seconds_until_next_funding_mark()
                else:
                    backoff_s = min(float(MAX_BACKOFF_SECONDS), max(2.0, backoff_s * 2.0))
                    sleep_s = float(backoff_s)
            except Exception as exc:
                LOGGER.exception("crypto_funding_cycle_failed")
                _warn_nonfatal("INGEST_CRYPTO_FUNDING_CYCLE_FAILED", exc, once_key="cycle_failed")
                status = record_pipeline_status(
                    JOB_NAME,
                    ok=False,
                    raw_rows=0,
                    event_rows=0,
                    last_ingested_ts_ms=int(time.time() * 1000),
                    error=str(exc),
                    meta={"poll_seconds": float(POLL_SECONDS)},
                )
                manager.record_job_status(JOB_NAME, ok=False, message="crypto funding cycle failed", error=str(exc))
                put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps(status, separators=(",", ":"), sort_keys=True))
                backoff_s = min(float(MAX_BACKOFF_SECONDS), max(2.0, backoff_s * 2.0))
                sleep_s = float(backoff_s)
            time.sleep(max(1.0, float(sleep_s)))
    finally:
        release_job_lock(JOB_NAME, OWNER, PID)


if __name__ == "__main__":
    main()
