"""
FILE: label_due_events.py

Job entrypoint or scheduled task for `label_due_events`.
"""

# label_due_events.py
"""
Creates labels for events where the horizon has passed and price data exists.
Reads raw prices for non-futures symbols. Futures symbols read the
ratio-adjusted continuous series and require roll-calendar coverage.

Writes to labels table.
- Stores vol_proxy + regime at label-time (required for true regime-aware training).

Production wrapper:
- Job locking
- Heartbeats
- Crash safety
"""

import os
import time
import json
import logging
import random
from typing import Optional

from engine.data.asset_map import asset_class_for_symbol
from engine.data.futures_instrument import parse_futures_symbol
from engine.data.futures_roll import (
    futures_label_window_block_reason,
    read_ratio_adjusted_continuous_close_at_or_after,
)
from engine.runtime.failure_diagnostics import log_failure
from engine.data.universe_pit import label_window_within_symbol_lifecycle
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)
from engine.strategy.model_v2 import classify_regime

# ---------------            -- ------------------------------------------------------
# Job / runtime config
# ---------------            -- ------------------------------------------------------

JOB_NAME = "label_due_events"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [label_due_events] %(message)s",
)
LOG = get_logger("engine.data.jobs.label_due_events")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="label_due_events_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.label_due_events",
        extra=extra or None,
        persist=False,
    )

# ---------------            -- ------------------------------------------------------
# Labeling configuration (from old file)
# ---------------            -- ------------------------------------------------------

HORIZONS_S = [300, 3600]  # 5m, 1h
DEFAULT_SYMBOLS = ["SPY", "BTC", "OIL"]

# ---------------            -- ------------------------------------------------------
# Helpers (from old file)
# ---------------            -- ------------------------------------------------------

def price_at_or_after(con, symbol: str, ts_ms: int) -> Optional[float]:
    # Labels must be point-in-time. We take the first observable sample at or
    # after the target timestamp instead of "best available later" data.
    row = con.execute(
        """
        SELECT COALESCE(price, px)
        FROM prices
        WHERE symbol=? AND ts_ms>=?
        ORDER BY ts_ms ASC
        LIMIT 1
        """,
        (symbol, int(ts_ms)),
    ).fetchone()
    if row and row[0] is not None:
        return float(row[0])

    row = con.execute(
        """
        SELECT
          COALESCE(
            last,
            CASE
              WHEN bid IS NOT NULL AND ask IS NOT NULL THEN (bid + ask) / 2.0
              ELSE NULL
            END,
            bid,
            ask
          )
        FROM price_quotes
        WHERE symbol=? AND ts_ms>=?
        ORDER BY ts_ms ASC
        LIMIT 1
        """,
        (symbol, int(ts_ms)),
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def _is_futures_label_symbol(symbol: str) -> bool:
    text = str(symbol or "").upper().strip()
    if parse_futures_symbol(text) is not None:
        return True
    try:
        return str(asset_class_for_symbol(text) or "").upper().strip() == "FUTURES"
    except Exception:
        return False


def _compute_futures_return(con, symbol: str, event_ts: int, horizon_ms: int) -> Optional[float]:
    eval_ts = int(event_ts) + int(horizon_ms)
    if not label_window_within_symbol_lifecycle(
        con,
        symbol=str(symbol),
        start_ts_ms=int(event_ts),
        end_ts_ms=int(eval_ts),
    ):
        return None
    block_reason = futures_label_window_block_reason(con, symbol, int(event_ts), int(eval_ts))
    if block_reason is not None:
        if block_reason == "roll_calendar_unavailable":
            _warn_nonfatal(
                "FUTURES_LABEL_ROLL_CALENDAR_MISSING",
                RuntimeError("futures_roll_calendar_unavailable"),
                symbol=str(symbol),
                event_ts_ms=int(event_ts),
                eval_ts_ms=int(eval_ts),
            )
        return None

    p0 = read_ratio_adjusted_continuous_close_at_or_after(con, symbol, int(event_ts))
    p1 = read_ratio_adjusted_continuous_close_at_or_after(con, symbol, int(eval_ts))
    if p0 is None or p1 is None:
        _warn_nonfatal(
            "FUTURES_LABEL_CONTINUOUS_BARS_MISSING",
            RuntimeError("ratio_adjusted_continuous_bars_missing"),
            symbol=str(symbol),
            event_ts_ms=int(event_ts),
            eval_ts_ms=int(eval_ts),
        )
        return None
    if float(p0) <= 0.0:
        return None
    return (float(p1) - float(p0)) / float(p0)


def compute_return(con, symbol: str, event_ts: int, horizon_ms: int) -> Optional[float]:
    if _is_futures_label_symbol(symbol):
        return _compute_futures_return(con, symbol, event_ts, horizon_ms)
    if not label_window_within_symbol_lifecycle(
        con,
        symbol=str(symbol),
        start_ts_ms=int(event_ts),
        end_ts_ms=int(event_ts) + int(horizon_ms),
    ):
        return None
    p0 = price_at_or_after(con, symbol, event_ts)
    p1 = price_at_or_after(con, symbol, event_ts + horizon_ms)
    if p0 is None or p1 is None:
        return None
    if float(p0) <= 0.0:
        return None
    return (float(p1) - float(p0)) / float(p0)


def realized_vol_proxy(con, symbol: str, lookback_points: int = 80) -> float:
    rows = con.execute(
        """
        SELECT COALESCE(price, px)
        FROM prices
        WHERE symbol=?
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (str(symbol), int(max(30, lookback_points))),
    ).fetchall()

    if not rows or len(rows) < 30:
        # Quote-derived mids are only a fallback for sparse histories; training
        # should still prefer true price rows whenever they exist.
        rows = con.execute(
            """
            SELECT
              COALESCE(
                last,
                CASE
                  WHEN bid IS NOT NULL AND ask IS NOT NULL THEN (bid + ask) / 2.0
                  ELSE NULL
                END,
                bid,
                ask
              )
            FROM price_quotes
            WHERE symbol=?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(max(30, lookback_points))),
        ).fetchall()

    if not rows or len(rows) < 3:
        return 1e-6

    prices = [float(r[0]) for r in rows if r and r[0] is not None]
    if len(prices) < 3:
        return 1e-6

    rets = []
    for i in range(1, len(prices)):
        p0 = prices[i - 1]
        p1 = prices[i]
        if p0 <= 0:
            continue
        rets.append((p1 / p0) - 1.0)

    if not rets:
        return 1e-6

    var = sum(r * r for r in rets) / max(1, len(rets))
    vol = var ** 0.5
    return max(1e-6, float(vol))


def _label_symbols(con) -> list[str]:
    rows = []
    try:
        rows = con.execute(
            """
            SELECT symbol
            FROM symbols
            WHERE status IN ('ACTIVE', 'WATCH')
            ORDER BY score DESC, updated_ts_ms DESC
            LIMIT 200
            """
        ).fetchall()
    except Exception:
        rows = []

    syms = [str(r[0]).upper().strip() for r in (rows or []) if r and r[0]]
    if syms:
        return syms

    try:
        rows = con.execute(
            """
            SELECT symbol, MAX(ts_ms) AS last_ts_ms
            FROM prices
            GROUP BY symbol
            ORDER BY last_ts_ms DESC
            LIMIT 200
            """
        ).fetchall()
    except Exception:
        rows = []

    syms = [str(r[0]).upper().strip() for r in (rows or []) if r and r[0]]
    return syms or list(DEFAULT_SYMBOLS)

# ---------------            -- ------------------------------------------------------
# Core labeling logic (extracted & reusable)
# ---------------            -- ------------------------------------------------------

def label_due_events_internal() -> int:
    con = connect()
    try:
        now_ms = int(time.time() * 1000)
        max_h_ms = max(HORIZONS_S) * 1000

        # Only label fully matured events. Anything newer than the longest
        # horizon is incomplete and would leak future information.
        events = con.execute(
            """
            SELECT id, ts_ms, title
            FROM events
            WHERE ts_ms <= ?
            ORDER BY ts_ms ASC
            LIMIT 200
            """,
            (int(now_ms - max_h_ms),),
        ).fetchall()

        inserted = 0
        symbols = _label_symbols(con)

        for eid, ets, _title in events:
            for sym in symbols:
                sym = sym.upper().strip()
                vol = realized_vol_proxy(con, sym, lookback_points=80)
                regime = classify_regime(vol)

                for h_s in HORIZONS_S:
                    exists = con.execute(
                        """
                        SELECT 1
                        FROM labels
                        WHERE event_id=? AND symbol=? AND horizon_s=?
                        LIMIT 1
                        """,
                        (int(eid), sym, int(h_s)),
                    ).fetchone()
                    if exists:
                        continue

                    ret = compute_return(con, sym, int(ets), int(h_s) * 1000)
                    if ret is None:
                        continue

                    # Persisting vol/regime at label time keeps downstream model
                    # training regime-aware without recomputing from a later state.
                    impact_z = float(ret) / max(float(vol), 1e-6)

                    cur = con.execute(
                        """
                        INSERT OR IGNORE INTO labels(
                          event_id, horizon_s, symbol,
                          baseline_ret, realized_ret,
                          impact_z, created_at_ms,
                          vol_proxy, regime
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            int(eid),
                            int(h_s),
                            sym,
                            0.0,
                            float(ret),
                            float(impact_z),
                            int(now_ms),
                            float(vol),
                            str(regime),
                        ),
                    )
                    if int(getattr(cur, "rowcount", 0) or 0) > 0:
                        inserted += 1

        con.commit()
        return inserted
    finally:
        con.close()

# ---------------            -- ------------------------------------------------------
# Runtime helpers
# ---------------            -- ------------------------------------------------------

def _sleep_with_jitter(seconds: float) -> None:
    if seconds <= 0:
        return
    j = seconds * 0.2
    time.sleep(max(0.05, seconds + random.uniform(-j, j)))

# ---------------            -- ------------------------------------------------------
# Main (production runner)
# ---------------            -- ------------------------------------------------------

def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("label_due_events must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)

    started_ms = int(time.time() * 1000)
    last_hb_s = 0.0

    try:
        logging.info("labeling due events")

        n = label_due_events_internal()

        now_s = time.time()
        if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
            touch_job_lock(JOB_NAME, OWNER, PID)
            put_job_heartbeat(
                JOB_NAME,
                OWNER,
                PID,
                extra_json=json.dumps({"labeled": int(n)}),
            )
            last_hb_s = now_s

        dur_ms = int(time.time() * 1000) - started_ms
        logging.info("labeled_events=%s dur_ms=%s", int(n), int(dur_ms))

    except Exception as e:
        logging.exception("labeling failed: %r", e)
        _sleep_with_jitter(5.0)
        raise
    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal("LABEL_DUE_EVENTS_LOCK_RELEASE_FAILED", e, job=JOB_NAME)


if __name__ == "__main__":
    main()
