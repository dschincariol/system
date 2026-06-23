"""
FILE: backfill_labels_price_from_prices.py

Job entrypoint or scheduled task for `backfill_labels_price_from_prices`.
"""

"""
jobs/backfill_labels_price_from_prices.py

Build price-derived labels for (symbol, horizon_s, ts_pred_ms).

We sample:
- entry_price = nearest price at/after ts_pred_ms
- exit_price  = nearest price at/after ts_pred_ms + horizon_s*1000

Write:
- labels_price (ret, ret_z, dir)

ret_z is a rolling z-score of returns for that symbol/horizon over a lookback window.
"""

import os
import time
import json
import math
import logging
from typing import Optional, Tuple, List

from engine.data.asset_map import asset_class_for_symbol
from engine.data.prices.fx_clock import fx_forward_eval_ms, fx_window_spans_closed_gap
from engine.data.universe_pit import label_window_within_symbol_lifecycle
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)

JOB_NAME = "backfill_labels_price"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [labels_price] %(message)s",
)
LOG = get_logger("engine.data.jobs.backfill_labels_price_from_prices")
_WARNED_NONFATAL_KEYS: set[str] = set()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "15.0"))

HORIZONS = [300, 3600]
# how far back to backfill from "now" (days)
LOOKBACK_DAYS = int(os.environ.get("LABELS_PRICE_LOOKBACK_DAYS", "30"))
# rolling window size for zscore
ZSCORE_LOOKBACK = int(os.environ.get("LABELS_PRICE_ZLOOKBACK", "500"))


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: object) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.data.jobs.backfill_labels_price_from_prices",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _nearest_price_at_or_after(con, symbol: str, ts_ms: int) -> Optional[Tuple[int, float]]:
    # Backfill uses the same point-in-time rule as live labeling: first price at
    # or after the anchor timestamp, never a best-fit interpolation from later data.
    row = con.execute(
        """
        SELECT ts_ms, price
        FROM prices
        WHERE symbol=?
          AND ts_ms >= ?
        ORDER BY ts_ms ASC
        LIMIT 1
        """,
        (symbol, int(ts_ms)),
    ).fetchone()
    if not row:
        return None
    try:
        return int(row[0]), float(row[1])
    except Exception as e:
        _warn_nonfatal(
            "backfill_labels_price_row_parse_failed",
            "BACKFILL_LABELS_PRICE_ROW_PARSE_FAILED",
            e,
            warn_key="labels_price_row_parse",
        )
        return None


def _label_price_eval_target(symbol: str, ts_pred_ms: int, horizon_s: int) -> Tuple[int, dict, bool]:
    naive_eval_ms = int(ts_pred_ms) + int(horizon_s) * 1000
    try:
        asset_class = str(asset_class_for_symbol(str(symbol)) or "UNKNOWN").upper().strip()
    except Exception as exc:
        _warn_nonfatal(
            "backfill_labels_price_asset_class_failed",
            "BACKFILL_LABELS_PRICE_ASSET_CLASS_FAILED",
            exc,
            warn_key=f"backfill_labels_price_asset_class_failed:{symbol}",
            symbol=str(symbol),
        )
        asset_class = "UNKNOWN"
    if asset_class != "FX":
        return int(naive_eval_ms), {}, False
    corrected_eval_ms = fx_forward_eval_ms(int(ts_pred_ms), int(horizon_s) * 1000)
    spans_gap = fx_window_spans_closed_gap(int(ts_pred_ms), int(naive_eval_ms))
    return (
        int(corrected_eval_ms),
        {"fx_clock_corrected": True, "naive_eval_ms": int(naive_eval_ms)},
        bool(spans_gap),
    )


def _rolling_z(con, symbol: str, horizon_s: int, new_ret: float) -> float:
    # This z-score is a convenience normalization for analysis/governance, not a
    # replacement for the raw realized return stored alongside it.
    rows = con.execute(
        """
        SELECT ret
        FROM labels_price
        WHERE symbol=?
          AND horizon_s=?
        ORDER BY ts_pred_ms DESC
        LIMIT ?
        """,
        (symbol, int(horizon_s), int(ZSCORE_LOOKBACK)),
    ).fetchall()

    rets: List[float] = []
    for (r,) in rows:
        try:
            rets.append(float(r))
        except Exception as exc:
            _warn_nonfatal(
                "backfill_labels_price_ret_parse_failed",
                "BACKFILL_LABELS_PRICE_RET_PARSE_FAILED",
                exc,
                warn_key="backfill_labels_price_ret_parse_failed",
                symbol=str(symbol),
                horizon_s=int(horizon_s),
            )

    rets.append(float(new_ret))
    if len(rets) < 20:
        return 0.0

    m = sum(rets) / len(rets)
    v = sum((x - m) ** 2 for x in rets) / max(1, (len(rets) - 1))
    sd = math.sqrt(max(1e-12, v))
    z = (float(new_ret) - m) / sd
    if z != z:
        return 0.0
    return float(max(-8.0, min(8.0, z)))


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance holds lock; exiting")
        raise SystemExit(2)

    last_hb_s = 0.0
    since_ms = int((time.time() - LOOKBACK_DAYS * 86400) * 1000)

    # Crash-safe resume keeps long backfills idempotent across restarts. The job
    # always advances from the newest already-written prediction timestamp.
    # crash-safe resume: skip already-labeled timestamps
    try:
        r = connect().execute(
            "SELECT MAX(ts_pred_ms) FROM labels_price"
        ).fetchone()
        if r and r[0]:
            since_ms = max(since_ms, int(r[0]))
    except Exception as exc:
        _warn_nonfatal(
            "backfill_labels_price_resume_read_failed",
            "BACKFILL_LABELS_PRICE_RESUME_READ_FAILED",
            exc,
            warn_key="backfill_labels_price_resume_read_failed",
        )


    try:
        con = connect()
        try:
            horizon_placeholders = ",".join("?" for _ in HORIZONS)
            # Anchor labels to canonical predictions so the realized outcome is
            # tied to the exact prediction horizon that generated it.
            rows = con.execute(
                f"""
                SELECT ts_ms, symbol, horizon_s
                FROM predictions
                WHERE ts_ms >= ?
                  AND horizon_s IN ({horizon_placeholders})
                GROUP BY symbol, ts_ms, horizon_s
                ORDER BY ts_ms ASC
                """,
                tuple([int(since_ms)] + [int(h) for h in HORIZONS]),
            ).fetchall()

            if not rows:
                logging.info("no predictions found in lookback window")
                return

            wrote = 0
            scanned = 0

            for ts_pred_ms, sym, horizon_s in rows:
                scanned += 1
                now_s = time.time()
                if now_s - last_hb_s >= HEARTBEAT_EVERY_S:
                    try:
                        touch_job_lock(JOB_NAME, OWNER, PID)
                        put_job_heartbeat(JOB_NAME, OWNER, PID, extra_json=json.dumps({"symbol": sym, "ts": int(ts_pred_ms)}))
                    except Exception as exc:
                        _warn_nonfatal(
                            "backfill_labels_price_heartbeat_failed",
                            "BACKFILL_LABELS_PRICE_HEARTBEAT_FAILED",
                            exc,
                            warn_key="backfill_labels_price_heartbeat_failed",
                        )
                    last_hb_s = now_s

                ts_pred_ms = int(ts_pred_ms)
                sym = str(sym)
                horizon_s = int(horizon_s)

                # entry
                entry = _nearest_price_at_or_after(con, sym, ts_pred_ms)
                if not entry:
                    continue
                entry_ts, entry_px = entry
                if entry_px <= 0:
                    continue

                ts_eval_target, fx_clock_meta, skip_fx_gap = _label_price_eval_target(
                    str(sym),
                    int(ts_pred_ms),
                    int(horizon_s),
                )
                if skip_fx_gap:
                    continue
                if not label_window_within_symbol_lifecycle(
                    con,
                    symbol=str(sym),
                    start_ts_ms=int(ts_pred_ms),
                    end_ts_ms=int(ts_eval_target),
                ):
                    continue
                exitp = _nearest_price_at_or_after(con, sym, ts_eval_target)
                if not exitp:
                    continue
                exit_ts, exit_px = exitp
                if exit_px <= 0:
                    continue

                ret = (exit_px - entry_px) / entry_px
                dir_ = 1 if ret > 0 else (-1 if ret < 0 else 0)
                ret_z = _rolling_z(con, sym, horizon_s, float(ret))

                try:
                    meta_payload = {"entry_ts_ms": int(entry_ts), "exit_ts_ms": int(exit_ts)}
                    if fx_clock_meta:
                        meta_payload.update(dict(fx_clock_meta))
                    con.execute(
                        """
                        INSERT OR REPLACE INTO labels_price(
                          ts_pred_ms, ts_eval_ms, symbol, horizon_s,
                          entry_price, exit_price, ret, ret_z, dir, meta_json
                        ) VALUES (?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            int(ts_pred_ms),
                            int(exit_ts),
                            sym,
                            int(horizon_s),
                            float(entry_px),
                            float(exit_px),
                            float(ret),
                            float(ret_z),
                            int(dir_),
                            json.dumps(
                                meta_payload,
                                separators=(",", ":"),
                                sort_keys=True,
                            ),
                        ),
                    )
                    wrote += 1
                except Exception as exc:
                    _warn_nonfatal(
                        "backfill_labels_price_insert_failed",
                        "BACKFILL_LABELS_PRICE_INSERT_FAILED",
                        exc,
                        warn_key="backfill_labels_price_insert_failed",
                        symbol=str(sym),
                        horizon_s=int(horizon_s),
                        ts_pred_ms=int(ts_pred_ms),
                    )

            con.commit()
            logging.info("done scanned=%s wrote=%s", scanned, wrote)

        finally:
            con.close()

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as exc:
            _warn_nonfatal(
                "backfill_labels_price_release_job_lock_failed",
                "BACKFILL_LABELS_PRICE_RELEASE_JOB_LOCK_FAILED",
                exc,
                warn_key="backfill_labels_price_release_job_lock_failed",
            )


if __name__ == "__main__":
    main()
