"""
FILE: alpha_lifecycle_engine.py

Human-readable purpose:
Tracks the lifecycle of alert-driven alpha after a signal exists. This module
does not generate alerts; it records birth, expiry, decay, and explainable
remaining alpha for downstream portfolio and audit logic.

Alpha Lifecycle Engine (ALE)

Tracks alpha lifecycle tied to alerts (signals) WITHOUT changing signal generation.

Responsibilities:
- Register alpha instances (alert_id keyed) on-demand
- Enforce TTL expiry and compute alpha_remaining via half-life decay
- Provide explainable lifecycle state for EPE audits
"""

import json
import logging
import math
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

LOG = get_logger("engine.strategy.alpha_lifecycle_engine")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="alpha_lifecycle_engine_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.alpha_lifecycle_engine",
        extra=extra or None,
        persist=False,
    )


def _ensure_tables(con) -> None:
    # The module bootstraps its own table so lifecycle reads are safe even in
    # lightweight or partially initialized contexts.
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS alpha_lifecycle (
          alert_id INTEGER PRIMARY KEY,
          created_ts_ms INTEGER NOT NULL,
          expires_ts_ms INTEGER NOT NULL,
          half_life_ms INTEGER NOT NULL,
          volatility REAL,
          status TEXT NOT NULL,
          last_touch_ts_ms INTEGER NOT NULL,
          meta_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_alpha_lifecycle_exp ON alpha_lifecycle(expires_ts_ms);
        """
    )


def register_alpha(
    con,
    alert_id: int,
    created_ts_ms: int,
    ttl_ms: int,
    half_life_ms: int,
    volatility: Optional[float] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> None:
    _ensure_tables(con)

    now = _now_ms()
    created = int(created_ts_ms or 0) if int(created_ts_ms or 0) > 0 else int(now)
    ttl = int(ttl_ms or 0)
    ttl = max(1, ttl)
    half_life = int(half_life_ms or 0)
    half_life = max(1, half_life)

    expires = int(created) + int(ttl)
    meta_json = json.dumps(meta or {}, separators=(",", ":"), sort_keys=True)

    # Registration is idempotent by `alert_id`, which lets upstream callers
    # refresh lifecycle parameters without duplicating the alpha record.
    con.execute(
        """
        INSERT INTO alpha_lifecycle(
          alert_id, created_ts_ms, expires_ts_ms, half_life_ms, volatility, status, last_touch_ts_ms, meta_json
        )
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(alert_id) DO UPDATE SET
          created_ts_ms=excluded.created_ts_ms,
          expires_ts_ms=excluded.expires_ts_ms,
          half_life_ms=excluded.half_life_ms,
          volatility=excluded.volatility,
          status=excluded.status,
          last_touch_ts_ms=excluded.last_touch_ts_ms,
          meta_json=excluded.meta_json
        """,
        (
            int(alert_id),
            int(created),
            int(expires),
            int(half_life),
            (float(volatility) if volatility is not None else None),
            "ACTIVE",
            int(now),
            meta_json,
        ),
    )


def ensure_alpha_from_intent(con, intent: Dict[str, Any]) -> None:
    """
    Idempotently register alpha lifecycle for intents that include source_alert_id + signal_ts_ms + ttl/half-life.
    """
    try:
        a_id = intent.get("source_alert_id")
        if a_id is None:
            return
        created = int(intent.get("signal_ts_ms") or 0)
        ttl = int(intent.get("alpha_ttl_ms") or 0)
        hl = int(intent.get("alpha_half_life_ms") or 0)
        vol = intent.get("volatility")
        register_alpha(
            con,
            alert_id=int(a_id),
            created_ts_ms=int(created),
            ttl_ms=int(ttl),
            half_life_ms=int(hl),
            volatility=(float(vol) if vol is not None else None),
            meta={"symbol": intent.get("symbol"), "reason": intent.get("reason")},
        )
    except Exception as e:
        log_failure(
            LOG,
            event="alpha_lifecycle_registration_failed",
            code="ALPHA_LIFECYCLE_REGISTRATION_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.strategy.alpha_lifecycle_engine",
            extra={"symbol": intent.get("symbol"), "reason": intent.get("reason")},
            persist=False,
        )
        # Alpha registration should never break the intent path.
        return


def alpha_state(con, alert_id: int, now_ms: Optional[int] = None) -> Dict[str, Any]:
    _ensure_tables(con)
    now = int(now_ms) if now_ms is not None else _now_ms()

    r = con.execute(
        """
        SELECT created_ts_ms, expires_ts_ms, half_life_ms, volatility, status, meta_json
        FROM alpha_lifecycle
        WHERE alert_id=?
        """,
        (int(alert_id),),
    ).fetchone()

    if not r:
        return {"ok": False, "exists": False}

    created, exp, hl, vol, status, meta_json = r
    created = int(created or 0)
    exp = int(exp or 0)
    hl = int(hl or 1)

    age = max(0, now - created)
    ttl = max(1, exp - created)
    expired = now >= exp

    if expired and str(status or "").upper() != "EXPIRED":
        try:
            con.execute(
                """
                UPDATE alpha_lifecycle
                SET status='EXPIRED', last_touch_ts_ms=?
                WHERE alert_id=?
                """,
                (int(now), int(alert_id)),
            )
        except Exception as e:
            _warn_nonfatal(
                "ALPHA_LIFECYCLE_MARK_EXPIRED_FAILED",
                e,
                alert_id=int(alert_id),
                status=str(status or ""),
            )

    # Alpha remaining is decayed smoothly by half-life, then clipped by the TTL
    # wall so consumers can use it as a continuous weight.
    rem = 0.0
    if not expired:
        try:
            rem_hl = math.pow(0.5, float(age) / float(max(1, hl)))
            ttl_frac = max(0.0, 1.0 - (float(age) / float(max(1, ttl))))
            rem = max(0.0, min(1.0, rem_hl * (0.5 + 0.5 * ttl_frac)))
        except Exception:
            rem = 0.0

    meta = None
    try:
        meta = json.loads(meta_json) if meta_json else None
    except Exception:
        meta = None

    return {
        "ok": True,
        "exists": True,
        "created_ts_ms": created,
        "expires_ts_ms": exp,
        "half_life_ms": int(hl),
        "volatility": (float(vol) if vol is not None else None),
        "status": ("EXPIRED" if expired else "ACTIVE"),
        "age_ms": int(age),
        "ttl_ms": int(ttl),
        "alpha_remaining": float(rem),
        "meta": (meta if isinstance(meta, dict) else None),
    }

def apply_alpha_lifecycle(
    con,
    portfolio_orders_id: Optional[int],
    portfolio_ts_ms: int,
    orders: List[Dict[str, Any]],
    now_ts_ms: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Alpha Lifecycle Engine (ALE) — batch application.

    Inputs:
      - portfolio_ts_ms: timestamp of the portfolio intent batch (ms)
      - orders: list of intents

    Behavior:
      - Computes age_ms from signal_ts_ms (if present) or from portfolio_ts_ms.
      - Computes alpha_remaining using half-life and TTL (if present).
      - Drops expired orders (alpha_remaining <= 0) and annotates survivors with:
          alpha_age_ms, alpha_remaining, alpha_ttl_ms, alpha_half_life_ms, signal_ts_ms
    """
    now_ms = int(now_ts_ms) if now_ts_ms is not None else _now_ms()

    kept: List[Dict[str, Any]] = []
    dropped = 0
    annotated = 0

    for o in list(orders or []):
        if not isinstance(o, dict):
            continue

        # prefer explicit signal_ts_ms; else use portfolio_ts_ms
        try:
            sig_ts = o.get("signal_ts_ms")
            sig_ts = int(sig_ts) if sig_ts is not None else int(portfolio_ts_ms)
        except Exception:
            sig_ts = int(portfolio_ts_ms)

        age_ms = max(0, int(now_ms) - int(sig_ts))

        # pull knobs (optional)
        try:
            ttl_ms = int(o.get("alpha_ttl_ms") or 0)
        except Exception:
            ttl_ms = 0

        try:
            half_life_ms = int(o.get("alpha_half_life_ms") or 0)
        except Exception:
            half_life_ms = 0

        # alpha_remaining
        rem = 1.0
        try:
            if half_life_ms and half_life_ms > 0:
                # exponential half-life decay
                rem = float(0.5 ** (float(age_ms) / float(half_life_ms)))
            else:
                rem = 1.0
        except Exception:
            rem = 1.0

        # TTL cap (hard expiry)
        try:
            if ttl_ms and ttl_ms > 0:
                if age_ms >= ttl_ms:
                    rem = 0.0
                else:
                    # optional linear cap inside ttl to make expiry visible
                    rem = float(min(rem, max(0.0, 1.0 - (float(age_ms) / float(ttl_ms)))))
        except Exception as e:
            _warn_nonfatal(
                "ALPHA_LIFECYCLE_TTL_CAP_FAILED",
                e,
                portfolio_orders_id=(None if portfolio_orders_id is None else int(portfolio_orders_id)),
                portfolio_ts_ms=int(portfolio_ts_ms),
                age_ms=int(age_ms),
                ttl_ms=int(ttl_ms),
            )

        if not (rem > 0.0):
            dropped += 1
            continue

        # annotate (do not overwrite existing keys if upstream already set them)
        if "alpha_age_ms" not in o:
            o["alpha_age_ms"] = int(age_ms)
        if "alpha_remaining" not in o:
            o["alpha_remaining"] = float(rem)
        if "alpha_ttl_ms" not in o and ttl_ms:
            o["alpha_ttl_ms"] = int(ttl_ms)
        if "alpha_half_life_ms" not in o and half_life_ms:
            o["alpha_half_life_ms"] = int(half_life_ms)
        if "signal_ts_ms" not in o:
            o["signal_ts_ms"] = int(sig_ts)

        # Preserve batch context so later audits can tie the intent back to the
        # originating portfolio-order batch.
        if portfolio_orders_id is not None and "portfolio_orders_id" not in o:
            o["portfolio_orders_id"] = int(portfolio_orders_id)

        kept.append(o)
        annotated += 1

    meta = {
        "ok": True,
        "portfolio_orders_id": (int(portfolio_orders_id) if portfolio_orders_id is not None else None),
        "portfolio_ts_ms": int(portfolio_ts_ms),
        "now_ms": int(now_ms),
        "in_n": int(len(list(orders or []))),
        "out_n": int(len(kept)),
        "dropped_expired_n": int(dropped),
        "annotated_n": int(annotated),
    }
    return kept, meta

# ============================================================
# Alpha Decay Tracking
# - Time to MFE
# - Time to mean reversion
# - Signal expiry vs realized move
# ============================================================

def compute_alpha_decay_metrics(
    signal_ts_ms: int,
    entry_ts_ms: int,
    exit_ts_ms: Optional[int],
    prices: list,
    side: str,
    ttl_ms: Optional[int],
) -> Dict[str, Any]:
    """
    prices: list[(ts_ms, price)] sorted ascending
    side: "long" or "short"
    """

    if not prices:
        return {}

    # Metrics are computed relative to the first observed post-entry price.
    entry_price = prices[0][1]
    mfe = 0.0
    mfe_ts = None

    for ts, px in prices:
        move = (px - entry_price) if side == "long" else (entry_price - px)
        if move > mfe:
            mfe = move
            mfe_ts = ts

    time_to_mfe_ms = (mfe_ts - entry_ts_ms) if mfe_ts else None

    mean_rev_ts = None
    for ts, px in prices:
        if side == "long" and px <= entry_price:
            mean_rev_ts = ts
            break
        if side == "short" and px >= entry_price:
            mean_rev_ts = ts
            break

    time_to_mean_rev_ms = (
        (mean_rev_ts - entry_ts_ms) if mean_rev_ts else None
    )

    realized_move = (
        (prices[-1][1] - entry_price)
        if side == "long"
        else (entry_price - prices[-1][1])
    )

    expired = False
    if ttl_ms and exit_ts_ms:
        expired = (exit_ts_ms - signal_ts_ms) > ttl_ms

    return {
        "mfe": mfe,
        "time_to_mfe_ms": time_to_mfe_ms,
        "time_to_mean_rev_ms": time_to_mean_rev_ms,
        "realized_move": realized_move,
        "expired": expired,
    }


def alpha_is_stale(signal_ts_ms: int, ttl_ms: Optional[int]) -> bool:
    if not ttl_ms:
        return False
    now = int(time.time() * 1000)
    return (now - signal_ts_ms) > ttl_ms
