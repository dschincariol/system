"""
FILE: execution_analytics_engine.py

Execution subsystem module for `execution_analytics_engine`.
"""

"""
Execution Analytics & Slippage Attribution Engine

Post-trade diagnostics:
- Realized slippage vs decision ref price
- Alpha decay at fill
- TTL breach detection
- Cancel/replace impact
- Aggressiveness attribution
- Broker performance stats
"""

import json
import logging
import math
import time
from typing import Dict, Any, List, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect, _table_exists


# ============================================================
# Helpers
# ============================================================

def _now_ms() -> int:
    return int(time.time() * 1000)


LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)




def _compute_decay_lifecycle(extra: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """
    Extract lifecycle timestamps from signal metadata.
    Allows alpha-decay diagnostics without breaking existing pipeline or
    requiring every upstream producer to emit a fully normalized schema.
    """

    try:

        signal_ts = int(extra.get("signal_ts_ms") or 0)
        mfe_ts = int(extra.get("mfe_ts_ms") or 0)
        rev_ts = int(extra.get("reversion_ts_ms") or 0)

        return {
            "signal_ts_ms": signal_ts or None,
            "time_to_mfe_ms": (mfe_ts - signal_ts) if signal_ts and mfe_ts else None,
            "time_to_reversion_ms": (rev_ts - signal_ts) if signal_ts and rev_ts else None,
        }

    except Exception as e:
        _warn_nonfatal(
            "execution_analytics_decay_lifecycle_failed",
            "EXECUTION_ANALYTICS_DECAY_LIFECYCLE_FAILED",
            e,
            warn_key="execution_analytics_decay_lifecycle_failed",
        )
        payload: Dict[str, Optional[int]] = {
            "signal_ts_ms": None,
            "time_to_mfe_ms": None,
            "time_to_reversion_ms": None,
        }
        return payload

def _ensure_tables(con):
    # Analytics tables are append-only/post-trade diagnostics. They are not on
    # the critical order path, so schema is widened additively for compatibility.
    con.executescript(
        """
-- ============================================================
-- Canonical execution analytics table (aligned with code below)
-- ============================================================
CREATE TABLE IF NOT EXISTS execution_analytics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  client_order_id TEXT NOT NULL UNIQUE,
  broker TEXT,
  symbol TEXT NOT NULL,

  submit_ts_ms INTEGER,
  fill_ts_ms INTEGER,

  decision_ref_px REAL,
   fill_px REAL,
   qty REAL,

  -- ============================================================
  -- Alpha decay metrics
  -- ============================================================
  signal_ts_ms INTEGER,
  signal_expiry_ts_ms INTEGER,

  time_to_fill_ms INTEGER,
  time_to_mfe_ms INTEGER,
  time_to_reversion_ms INTEGER,

  mfe_px REAL,
  mfe_return REAL,

  reversion_px REAL,
  reversion_return REAL,

  alpha_decay_half_life_ms INTEGER,
  alpha_remaining REAL,

  slippage_bps REAL,
  fee_bps REAL,
  total_cost_bps REAL,                    -- slippage + fees (best-effort)

  alpha_remaining_at_fill REAL,
  ttl_ms INTEGER,
  age_ms INTEGER,                         -- fill_ts_ms - submit_ts_ms

  aggressiveness TEXT,
  order_type TEXT,

  created_ts_ms INTEGER,                  -- insertion time
  meta_json TEXT                          -- serialized extra/meta blob
);

CREATE INDEX IF NOT EXISTS idx_execution_analytics_ts ON execution_analytics(ts_ms);
CREATE INDEX IF NOT EXISTS idx_execution_analytics_symbol ON execution_analytics(symbol);
CREATE INDEX IF NOT EXISTS idx_execution_analytics_broker ON execution_analytics(broker);
CREATE INDEX IF NOT EXISTS idx_execution_analytics_cid ON execution_analytics(client_order_id);

-- ---------------------------------------------
-- Slippage feedback loop (rolling realized costs)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS execution_slippage_feedback (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  broker TEXT,
  order_type TEXT,
  aggressiveness TEXT,
  sample_n INTEGER NOT NULL,
  median_slippage_bps REAL,
  p75_slippage_bps REAL,
  suggested_limit_offset_bps REAL,
  suggested_extra_slip_bps REAL,
  extra_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_exec_slip_fb_ts ON execution_slippage_feedback(ts_ms);
CREATE INDEX IF NOT EXISTS idx_exec_slip_fb_broker ON execution_slippage_feedback(broker);

-- ---------------------------------------------
-- Alpha preservation KPIs (post-trade diagnostics)
-- ---------------------------------------------
CREATE TABLE IF NOT EXISTS alpha_preservation_kpis (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  broker TEXT,
  symbol TEXT,
  order_type TEXT,
  aggressiveness TEXT,
  sample_n INTEGER NOT NULL,
  avg_alpha_remaining REAL,
  avg_total_cost_bps REAL,
  alpha_cost_efficiency REAL,
  extra_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_alpha_kpis_ts ON alpha_preservation_kpis(ts_ms);
CREATE INDEX IF NOT EXISTS idx_alpha_kpis_broker ON alpha_preservation_kpis(broker);
CREATE INDEX IF NOT EXISTS idx_alpha_kpis_symbol ON alpha_preservation_kpis(symbol);

CREATE TABLE IF NOT EXISTS execution_fill_quality (
  ts_ms INTEGER NOT NULL,
  client_order_id TEXT NOT NULL,
  broker TEXT,
  symbol TEXT NOT NULL,
  order_type TEXT,
  aggressiveness TEXT,
  tod_bucket TEXT,
  spread_bps REAL,
  spread_capture_bps REAL,
  slippage_bps REAL,
  fee_bps REAL,
  total_cost_bps REAL,
  passive_flag INTEGER,
  aggressive_flag INTEGER,
  extra_json TEXT,
  PRIMARY KEY (ts_ms, client_order_id)
);

CREATE INDEX IF NOT EXISTS idx_exec_fill_quality_ts ON execution_fill_quality(ts_ms);
CREATE INDEX IF NOT EXISTS idx_exec_fill_quality_symbol_ts ON execution_fill_quality(symbol, ts_ms);

CREATE TABLE IF NOT EXISTS execution_policy_feedback (
  ts_ms INTEGER NOT NULL,
  client_order_id TEXT NOT NULL,
  broker TEXT,
  symbol TEXT NOT NULL,
  order_type TEXT,
  aggressiveness TEXT,
  execution_policy TEXT,
  entry_strategy TEXT,
  entry_delay_ms INTEGER,
  expected_slippage_bps REAL,
  realized_slippage_bps REAL,
  slippage_error_bps REAL,
  expected_fill_latency_ms REAL,
  realized_fill_latency_ms REAL,
  latency_error_ms REAL,
  fill_quality_score REAL,
  extra_json TEXT,
  PRIMARY KEY (ts_ms, client_order_id)
);

CREATE INDEX IF NOT EXISTS idx_exec_policy_feedback_ts
  ON execution_policy_feedback(ts_ms);

CREATE INDEX IF NOT EXISTS idx_exec_policy_feedback_symbol_ts
  ON execution_policy_feedback(symbol, ts_ms);

CREATE TABLE IF NOT EXISTS execution_strategy_attribution (
  ts_ms INTEGER NOT NULL,
  strategy_name TEXT NOT NULL,
  broker TEXT,
  symbol TEXT NOT NULL,
  n_orders INTEGER NOT NULL,
  avg_slippage_bps REAL,
  avg_total_cost_bps REAL,
  avg_fill_latency_ms REAL,
  passive_share REAL,
  aggressive_share REAL,
  avg_spread_capture_bps REAL,
  extra_json TEXT,
  PRIMARY KEY (ts_ms, strategy_name, broker, symbol)
);

CREATE INDEX IF NOT EXISTS idx_exec_strategy_attr_ts
  ON execution_strategy_attribution(ts_ms);

CREATE INDEX IF NOT EXISTS idx_exec_strategy_attr_strategy_ts
  ON execution_strategy_attribution(strategy_name, ts_ms);

-- ============================================================
-- Alpha decay lifecycle tracking
-- ============================================================
CREATE TABLE IF NOT EXISTS alpha_decay_metrics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,

  alert_id INTEGER,
  symbol TEXT,

  signal_ts_ms INTEGER NOT NULL,
  fill_ts_ms INTEGER,

  expiry_ts_ms INTEGER,

  time_to_mfe_ms INTEGER,
  time_to_reversion_ms INTEGER,

  decay_half_life_ms INTEGER,
  alpha_remaining REAL,

  meta_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_alpha_decay_symbol
ON alpha_decay_metrics(symbol);

CREATE INDEX IF NOT EXISTS idx_alpha_decay_signal_ts
ON alpha_decay_metrics(signal_ts_ms);
        """
    )


def _alpha_remaining(age_ms: int, half_life_ms: int, ttl_ms: int) -> float:
    # Analytics mirrors policy-engine decay math so post-trade diagnostics can
    # compare what happened at fill time with what policy intended.
    if ttl_ms <= 0:
        return 0.0
    if age_ms >= ttl_ms:
        return 0.0
    hl = max(1, int(half_life_ms))
    decay = math.pow(0.5, float(age_ms) / float(hl))
    ttl_factor = max(0.0, 1.0 - (float(age_ms) / float(ttl_ms)))
    return max(0.0, min(1.0, decay * ttl_factor))


def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        out = float(value)
        if not math.isfinite(out):
            return default
        return float(out)
    except Exception as e:
        _warn_nonfatal(
            "execution_analytics_safe_float_failed",
            "EXECUTION_ANALYTICS_SAFE_FLOAT_FAILED",
            e,
            warn_key="execution_analytics_safe_float_failed",
            value=value,
            default=default,
        )
        return default


def _safe_int(value: Any, default: Optional[int] = 0) -> Optional[int]:
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "execution_analytics_safe_int_failed",
            "EXECUTION_ANALYTICS_SAFE_INT_FAILED",
            e,
            warn_key="execution_analytics_safe_int_failed",
            value=value,
            default=default,
        )
        return default


def _fill_quality_score(
    *,
    spread_bps: float,
    spread_capture_bps: float,
    slippage_error_bps: Optional[float],
    latency_error_ms: Optional[float],
    expected_slippage_bps: Optional[float],
    expected_fill_latency_ms: Optional[float],
) -> float:
    score = 1.0
    slip_budget = max(1.0, _safe_float(expected_slippage_bps, 0.0) or 0.0, float(spread_bps or 0.0))
    if slippage_error_bps is not None and float(slippage_error_bps) > 0.0:
        score -= min(0.55, (float(slippage_error_bps) / float(slip_budget)) * 0.45)

    lat_budget = max(1000.0, _safe_float(expected_fill_latency_ms, 0.0) or 0.0)
    if latency_error_ms is not None and float(latency_error_ms) > 0.0:
        score -= min(0.35, (float(latency_error_ms) / float(lat_budget)) * 0.20)

    if float(spread_capture_bps or 0.0) > 0.0:
        score += min(0.10, float(spread_capture_bps) / max(5.0, float(spread_bps or 5.0)) * 0.10)

    return max(0.0, min(1.0, float(score)))


def summarize_execution_performance(days: int = 7) -> Dict[str, Any]:
    con = connect()
    try:
        _ensure_tables(con)
        since = _now_ms() - (int(days) * 86400000)

        rows = con.execute(
            """
            SELECT
              broker,
              symbol,
              COUNT(*) AS n,
              AVG(slippage_bps) AS avg_slip,
              AVG(alpha_remaining_at_fill) AS avg_alpha,
              AVG(age_ms) AS avg_age
            FROM execution_analytics
            WHERE ts_ms >= ?
            GROUP BY broker, symbol
            """,
            (int(since),),
        ).fetchall()

        out = []
        for r in rows or []:
            broker, symbol, n, avg_slip, avg_alpha, avg_age = r
            out.append(
                {
                    "broker": broker,
                    "symbol": symbol,
                    "fills": int(n or 0),
                    "avg_slippage_bps": float(avg_slip or 0.0),
                    "avg_alpha_remaining": float(avg_alpha or 0.0),
                    "avg_fill_latency_ms": float(avg_age or 0.0),
                }
            )

        return {"ok": True, "summary": out}

    finally:
        con.close()


# ============================================================
# Core Analytics Builder
# ============================================================

def build_execution_analytics(limit: int = 5000) -> Dict[str, Any]:
    con = connect()
    try:
        _ensure_tables(con)

        rows = con.execute(
            """
            SELECT
              s.client_order_id,
              s.broker,
              s.symbol,
              s.qty,
              s.submit_ts_ms,
              s.ref_px,
              s.expected_px,
              s.mid_px,
              s.spread_bps,
              f.fill_ts_ms,
              f.fill_px,
              f.fill_qty,
              f.fill_latency_ms,
              s.extra_json
            FROM execution_orders s
            JOIN execution_fills f
              ON s.client_order_id = f.client_order_id
            ORDER BY f.fill_ts_ms DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()

        wrote = 0
        fill_quality_wrote = 0
        policy_feedback_wrote = 0
        strategy_attr_wrote = 0
        strategy_buckets: Dict[tuple, Dict[str, float]] = {}

        for r in rows or []:
            (
                cid,
                broker,
                symbol,
                order_qty,
                submit_ts_ms,
                ref_px,
                expected_px,
                mid_px,
                order_spread_bps,
                fill_ts_ms,
                fill_px,
                fill_qty,
                fill_latency_ms,
                extra_json,
            ) = r

            try:
                order_qty = float(order_qty or 0.0)
                fill_qty = float(fill_qty or 0.0)
                ref_px = float(ref_px or 0.0)
                fill_px = float(fill_px or 0.0)
                submit_ts_ms = int(submit_ts_ms or 0)
                fill_ts_ms = int(fill_ts_ms or 0)
            except Exception as e:
                _warn_nonfatal(
                    "execution_analytics_fill_row_parse_failed",
                    "EXECUTION_ANALYTICS_FILL_ROW_PARSE_FAILED",
                    e,
                    warn_key=f"execution_analytics_fill_row_parse_failed:{cid}",
                    client_order_id=str(cid),
                    symbol=str(symbol),
                )
                continue

            if order_qty == 0.0 or fill_qty <= 0.0 or ref_px <= 0.0 or fill_px <= 0.0 or fill_ts_ms <= 0:
                continue

            side_sign = 1.0 if order_qty > 0 else -1.0
            signed_qty = float(fill_qty) * float(side_sign)
            slippage_bps = ((fill_px - ref_px) / ref_px) * 10000.0 * side_sign

            age_ms = max(0, int(fill_ts_ms) - int(submit_ts_ms))
            latency_ms = int(fill_latency_ms) if fill_latency_ms is not None else int(age_ms)

            extra: Dict[str, Any]
            try:
                extra = json.loads(extra_json or "{}")
                if not isinstance(extra, dict):
                    extra = {}
            except Exception:
                extra = {}

            lifecycle = _compute_decay_lifecycle(extra)

            ttl_ms = int(extra.get("alpha_ttl_ms") or 0)
            half_life_ms = int(extra.get("alpha_half_life_ms") or 60000)

            alpha_rem = _alpha_remaining(age_ms, half_life_ms, ttl_ms)

            signal_ts_ms = lifecycle.get("signal_ts_ms")
            time_to_mfe_ms = lifecycle.get("time_to_mfe_ms")
            time_to_reversion_ms = lifecycle.get("time_to_reversion_ms")

            aggressiveness = str(extra.get("aggressiveness") or "").upper().strip()
            order_type = str(extra.get("order_type") or "").upper().strip()
            execution_policy = str(extra.get("execution_policy") or "").strip().lower() or None
            entry_strategy = str(extra.get("entry_strategy") or "").strip().lower() or None
            entry_delay_ms = _safe_int(extra.get("entry_delay_ms"), None)
            expected_slippage_bps = _safe_float(extra.get("expected_slippage_bps"), None)
            expected_fill_latency_ms = _safe_float(extra.get("expected_fill_latency_ms"), None)

            fee_bps = 0.0
            try:
                fee_bps = float(extra.get("fee_bps") or 0.0)
            except Exception:
                fee_bps = 0.0

            spread_bps = None
            for v in (
                extra.get("spread_bps"),
                order_spread_bps,
                None if expected_px in (None, 0) or mid_px in (None, 0) else ((float(expected_px) - float(mid_px)) / float(mid_px)) * 10000.0,
            ):
                try:
                    if v is not None:
                        spread_bps = float(v)
                        break
                except Exception as e:
                    _warn_nonfatal(
                        "execution_analytics_spread_bps_parse_failed",
                        "EXECUTION_ANALYTICS_SPREAD_BPS_PARSE_FAILED",
                        e,
                        warn_key="execution_analytics_spread_bps_parse_failed",
                        value=v,
                    )
            if spread_bps is None:
                spread_bps = 0.0

            total_cost_bps = float(slippage_bps) + float(fee_bps)
            spread_capture_bps = float(spread_bps) - float(slippage_bps)
            slippage_error_bps = (
                float(slippage_bps) - float(expected_slippage_bps)
                if expected_slippage_bps is not None
                else None
            )
            latency_error_ms = (
                float(latency_ms) - float(expected_fill_latency_ms)
                if expected_fill_latency_ms is not None
                else None
            )
            fill_quality_score = _fill_quality_score(
                spread_bps=float(spread_bps),
                spread_capture_bps=float(spread_capture_bps),
                slippage_error_bps=slippage_error_bps,
                latency_error_ms=latency_error_ms,
                expected_slippage_bps=expected_slippage_bps,
                expected_fill_latency_ms=expected_fill_latency_ms,
            )

            fill_hour = int((int(fill_ts_ms) // 3600000) % 24)
            tod_bucket = (
                "overnight" if fill_hour < 6
                else "open" if fill_hour < 10
                else "midday" if fill_hour < 15
                else "close"
            )

            passive_flag = 1 if aggressiveness == "PASSIVE" else 0
            aggressive_flag = 1 if aggressiveness == "AGGRESSIVE" else 0

            strategy_name = None
            try:
                strategy_name = extra.get("strategy_name")
            except Exception:
                strategy_name = None
            if not strategy_name:
                try:
                    explain = extra.get("explain") or {}
                    if isinstance(explain, dict):
                        st = explain.get("strategy") or {}
                        if isinstance(st, dict):
                            strategy_name = st.get("name")
                except Exception:
                    strategy_name = None
            if not strategy_name:
                try:
                    st = extra.get("strategy") or {}
                    if isinstance(st, dict):
                        strategy_name = st.get("name")
                except Exception:
                    strategy_name = None

            meta = {
                **extra,
                "signal_ts_ms": signal_ts_ms,
                "time_to_mfe_ms": time_to_mfe_ms,
                "time_to_reversion_ms": time_to_reversion_ms,
                "spread_bps": float(spread_bps),
                "spread_capture_bps": float(spread_capture_bps),
                "tod_bucket": str(tod_bucket),
                "fill_latency_ms": int(latency_ms),
                "expected_slippage_bps": expected_slippage_bps,
                "slippage_error_bps": slippage_error_bps,
                "expected_fill_latency_ms": expected_fill_latency_ms,
                "latency_error_ms": latency_error_ms,
                "fill_quality_score": float(fill_quality_score),
                "entry_delay_ms": entry_delay_ms,
                "execution_policy": execution_policy,
                "entry_strategy": entry_strategy,
                "strategy_name": (str(strategy_name) if strategy_name else None),
                "passive_flag": int(passive_flag),
                "aggressive_flag": int(aggressive_flag),
            }

            con.execute(
                """
                INSERT OR IGNORE INTO execution_analytics(
                  ts_ms,
                  client_order_id,
                  broker,
                  symbol,
                  submit_ts_ms,
                  fill_ts_ms,
                  decision_ref_px,
                  fill_px,
                  qty,
                  signal_ts_ms,
                  time_to_fill_ms,
                  time_to_mfe_ms,
                  time_to_reversion_ms,
                  alpha_decay_half_life_ms,
                  alpha_remaining,
                  slippage_bps,
                  fee_bps,
                  total_cost_bps,
                  alpha_remaining_at_fill,
                  ttl_ms,
                  age_ms,
                  aggressiveness,
                  order_type,
                  created_ts_ms,
                  meta_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(fill_ts_ms),
                    str(cid),
                    (str(broker) if broker is not None else None),
                    str(symbol),
                    int(submit_ts_ms),
                    int(fill_ts_ms),
                    float(ref_px),
                    float(fill_px),
                    float(signed_qty),
                    int(signal_ts_ms) if signal_ts_ms is not None else None,
                    int(latency_ms),
                    int(time_to_mfe_ms) if time_to_mfe_ms is not None else None,
                    int(time_to_reversion_ms) if time_to_reversion_ms is not None else None,
                    int(half_life_ms),
                    float(alpha_rem),
                    float(slippage_bps),
                    float(fee_bps),
                    float(total_cost_bps),
                    float(alpha_rem),
                    int(ttl_ms),
                    int(age_ms),
                    aggressiveness,
                    order_type,
                    _now_ms(),
                    json.dumps(meta, separators=(",", ":"), sort_keys=True),
                ),
            )

            con.execute(
                """
                INSERT OR REPLACE INTO execution_fill_quality(
                  ts_ms,
                  client_order_id,
                  broker,
                  symbol,
                  order_type,
                  aggressiveness,
                  tod_bucket,
                  spread_bps,
                  spread_capture_bps,
                  slippage_bps,
                  fee_bps,
                  total_cost_bps,
                  passive_flag,
                  aggressive_flag,
                  extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(fill_ts_ms),
                    str(cid),
                    (str(broker) if broker is not None else None),
                    str(symbol),
                    str(order_type),
                    str(aggressiveness),
                    str(tod_bucket),
                    float(spread_bps),
                    float(spread_capture_bps),
                    float(slippage_bps),
                    float(fee_bps),
                    float(total_cost_bps),
                    int(passive_flag),
                    int(aggressive_flag),
                    json.dumps(meta, separators=(",", ":"), sort_keys=True),
                ),
            )

            con.execute(
                """
                INSERT OR REPLACE INTO execution_policy_feedback(
                  ts_ms,
                  client_order_id,
                  broker,
                  symbol,
                  order_type,
                  aggressiveness,
                  execution_policy,
                  entry_strategy,
                  entry_delay_ms,
                  expected_slippage_bps,
                  realized_slippage_bps,
                  slippage_error_bps,
                  expected_fill_latency_ms,
                  realized_fill_latency_ms,
                  latency_error_ms,
                  fill_quality_score,
                  extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(fill_ts_ms),
                    str(cid),
                    (str(broker) if broker is not None else None),
                    str(symbol),
                    str(order_type),
                    str(aggressiveness),
                    (str(execution_policy) if execution_policy else None),
                    (str(entry_strategy) if entry_strategy else None),
                    int(entry_delay_ms) if entry_delay_ms is not None else None,
                    float(expected_slippage_bps) if expected_slippage_bps is not None else None,
                    float(slippage_bps),
                    float(slippage_error_bps) if slippage_error_bps is not None else None,
                    float(expected_fill_latency_ms) if expected_fill_latency_ms is not None else None,
                    float(latency_ms),
                    float(latency_error_ms) if latency_error_ms is not None else None,
                    float(fill_quality_score),
                    json.dumps(meta, separators=(",", ":"), sort_keys=True),
                ),
            )

            if strategy_name:
                key = (
                    str(strategy_name),
                    str(broker or ""),
                    str(symbol),
                )
                cur = strategy_buckets.get(key) or {
                    "n_orders": 0.0,
                    "slippage_bps_sum": 0.0,
                    "total_cost_bps_sum": 0.0,
                    "latency_sum": 0.0,
                    "passive_sum": 0.0,
                    "aggressive_sum": 0.0,
                    "spread_capture_sum": 0.0,
                }
                cur["n_orders"] += 1.0
                cur["slippage_bps_sum"] += float(slippage_bps)
                cur["total_cost_bps_sum"] += float(total_cost_bps)
                cur["latency_sum"] += float(latency_ms)
                cur["passive_sum"] += float(passive_flag)
                cur["aggressive_sum"] += float(aggressive_flag)
                cur["spread_capture_sum"] += float(spread_capture_bps)
                strategy_buckets[key] = cur

            wrote += 1
            fill_quality_wrote += 1
            policy_feedback_wrote += 1

        for (strategy_name, broker, symbol), cur in strategy_buckets.items():
            n_orders = max(1.0, float(cur.get("n_orders") or 0.0))
            con.execute(
                """
                INSERT OR REPLACE INTO execution_strategy_attribution(
                  ts_ms,
                  strategy_name,
                  broker,
                  symbol,
                  n_orders,
                  avg_slippage_bps,
                  avg_total_cost_bps,
                  avg_fill_latency_ms,
                  passive_share,
                  aggressive_share,
                  avg_spread_capture_bps,
                  extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(_now_ms()),
                    str(strategy_name),
                    (str(broker) if broker else None),
                    str(symbol),
                    int(n_orders),
                    float(cur.get("slippage_bps_sum") or 0.0) / n_orders,
                    float(cur.get("total_cost_bps_sum") or 0.0) / n_orders,
                    float(cur.get("latency_sum") or 0.0) / n_orders,
                    float(cur.get("passive_sum") or 0.0) / n_orders,
                    float(cur.get("aggressive_sum") or 0.0) / n_orders,
                    float(cur.get("spread_capture_sum") or 0.0) / n_orders,
                    json.dumps(
                        {
                            "n_orders": int(n_orders),
                            "strategy_name": str(strategy_name),
                            "broker": (str(broker) if broker else None),
                            "symbol": str(symbol),
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                ),
            )
            strategy_attr_wrote += 1

        con.commit()

        try:
            _update_slippage_feedback(con, lookback_n=min(5000, int(limit) * 3))
        except Exception as e:
            _warn_nonfatal(
                "execution_analytics_slippage_feedback_update_failed",
                "EXECUTION_ANALYTICS_SLIPPAGE_FEEDBACK_UPDATE_FAILED",
                e,
                warn_key="execution_analytics_slippage_feedback_update_failed",
                limit=int(limit),
            )
        try:
            _build_alpha_preservation_kpis(con, lookback_n=min(5000, int(limit) * 3))
        except Exception as e:
            _warn_nonfatal(
                "execution_analytics_alpha_preservation_build_failed",
                "EXECUTION_ANALYTICS_ALPHA_PRESERVATION_BUILD_FAILED",
                e,
                warn_key="execution_analytics_alpha_preservation_build_failed",
                limit=int(limit),
            )

        adaptive_slips: List[float] = []
        baseline_slips: List[float] = []
        passive_slips: List[float] = []
        aggressive_slips: List[float] = []

        try:
            tail_rows = con.execute(
                """
                SELECT slippage_bps, meta_json
                FROM execution_analytics
                WHERE slippage_bps IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(max(500, min(20000, int(limit) * 4))),),
            ).fetchall()

            for sl, mj in tail_rows or []:
                try:
                    slv = float(sl)
                except Exception as e:
                    _warn_nonfatal(
                        "execution_analytics_tail_slippage_parse_failed",
                        "EXECUTION_ANALYTICS_TAIL_SLIPPAGE_PARSE_FAILED",
                        e,
                        warn_key="execution_analytics_tail_slippage_parse_failed",
                        slippage=sl,
                    )
                    continue
                try:
                    meta = json.loads(mj or "{}")
                    if isinstance(meta, dict) and bool(meta.get("adaptive_slice", False)):
                        adaptive_slips.append(slv)
                    else:
                        baseline_slips.append(slv)

                    if isinstance(meta, dict) and int(meta.get("passive_flag") or 0) == 1:
                        passive_slips.append(slv)
                    if isinstance(meta, dict) and int(meta.get("aggressive_flag") or 0) == 1:
                        aggressive_slips.append(slv)
                except Exception:
                    baseline_slips.append(slv)

        except Exception as e:
            _warn_nonfatal(
                "execution_analytics_tail_slippage_scan_failed",
                "EXECUTION_ANALYTICS_TAIL_SLIPPAGE_SCAN_FAILED",
                e,
                warn_key="execution_analytics_tail_slippage_scan_failed",
                limit=int(limit),
            )

        def _p(x: List[float], p: float) -> Optional[float]:
            if not x:
                return None
            x2 = sorted([float(v) for v in x])
            if not x2:
                return None
            if len(x2) == 1:
                return float(x2[0])
            idx = int(round((len(x2) - 1) * float(p)))
            idx = max(0, min(len(x2) - 1, idx))
            return float(x2[idx])

        return {
            "ok": True,
            "status": "built",
            "rows_written": int(wrote),
            "fill_quality_rows_written": int(fill_quality_wrote),
            "policy_feedback_rows_written": int(policy_feedback_wrote),
            "strategy_attribution_rows_written": int(strategy_attr_wrote),
            "baseline_p95_slippage_bps": _p(baseline_slips, 0.95),
            "baseline_p99_slippage_bps": _p(baseline_slips, 0.99),
            "adaptive_p95_slippage_bps": _p(adaptive_slips, 0.95),
            "adaptive_p99_slippage_bps": _p(adaptive_slips, 0.99),
            "passive_p95_slippage_bps": _p(passive_slips, 0.95),
            "aggressive_p95_slippage_bps": _p(aggressive_slips, 0.95),
        }

    finally:
        con.close()


def _percentile(sorted_vals: List[float], p: float) -> Optional[float]:
    if not sorted_vals:
        return None
    p = float(p)
    p = max(0.0, min(1.0, p))
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    idx = int(round((len(sorted_vals) - 1) * p))
    idx = max(0, min(len(sorted_vals) - 1, idx))
    return float(sorted_vals[idx])


def _update_slippage_feedback(con, lookback_n: int = 2000) -> None:
    """
    Rolling realized slippage → feedback knobs for execution policy.
    Writes one row per (broker, order_type, aggressiveness).
    """
    lookback_n = int(max(50, min(20000, int(lookback_n))))

    rows = con.execute(
        """
        SELECT
          ts_ms, broker, order_type, aggressiveness, meta_json, slippage_bps
        FROM execution_analytics
        WHERE slippage_bps IS NOT NULL
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (lookback_n,),
    ).fetchall()

    buckets: Dict[tuple, List[float]] = {}

    for ts_ms, broker, order_type, aggressiveness, meta_json, sl_bps in rows or []:
        try:
            b = str(broker or "").strip().lower() or None

            ot = str(order_type or "").upper().strip() or "UNKNOWN"
            ag = str(aggressiveness or "").upper().strip() or "UNKNOWN"

            if (ot == "UNKNOWN" or ag == "UNKNOWN") and meta_json:
                try:
                    ex = json.loads(meta_json or "{}")
                    if isinstance(ex, dict):
                        ot = str(ex.get("order_type") or ot).upper().strip() or ot
                        ag = str(ex.get("aggressiveness") or ag).upper().strip() or ag
                except Exception as e:
                    _warn_nonfatal(
                        "execution_analytics_feedback_meta_parse_failed",
                        "EXECUTION_ANALYTICS_FEEDBACK_META_PARSE_FAILED",
                        e,
                        warn_key="execution_analytics_feedback_meta_parse_failed",
                        broker=str(b),
                    )

            key = (b, ot, ag)
            buckets.setdefault(key, []).append(float(sl_bps))
        except Exception as e:
            _warn_nonfatal(
                "execution_analytics_feedback_bucket_parse_failed",
                "EXECUTION_ANALYTICS_FEEDBACK_BUCKET_PARSE_FAILED",
                e,
                warn_key=f"execution_analytics_feedback_bucket_parse_failed:{ts_ms}:{broker}:{order_type}:{aggressiveness}",
                ts_ms=int(ts_ms or 0),
                broker=str(broker or ""),
                order_type=str(order_type or ""),
                aggressiveness=str(aggressiveness or ""),
            )
            continue

    now_ms = _now_ms()

    for (b, ot, ag), vals in buckets.items():
        vals2 = sorted([float(v) for v in vals if v is not None])
        if len(vals2) < 10:
            continue

        med = _percentile(vals2, 0.50)
        p75 = _percentile(vals2, 0.75)

        suggested_limit_offset = float(max(0.0, (p75 or 0.0)))
        suggested_extra_slip = float(max(0.0, (med or 0.0)))

        con.execute(
            """
            INSERT INTO execution_slippage_feedback(
              ts_ms, broker, order_type, aggressiveness,
              sample_n, median_slippage_bps, p75_slippage_bps,
              suggested_limit_offset_bps, suggested_extra_slip_bps,
              extra_json
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(now_ms),
                (str(b) if b else None),
                str(ot),
                str(ag),
                int(len(vals2)),
                (float(med) if med is not None else None),
                (float(p75) if p75 is not None else None),
                float(suggested_limit_offset),
                float(suggested_extra_slip),
                json.dumps(
                    {"lookback_n": int(lookback_n)},
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )

    con.commit()


def get_slippage_feedback(con, broker: str) -> Dict[str, Dict[str, float]]:
    """
    Returns: { "<ORDER_TYPE>|<AGGR>": {"limit_offset_bps": x, "extra_slip_bps": y} }
    Uses the most recent feedback row per key for the given broker.
    """
    b = str(broker or "").strip().lower()
    out: Dict[str, Dict[str, float]] = {}

    rows = con.execute(
        """
        SELECT order_type, aggressiveness, suggested_limit_offset_bps, suggested_extra_slip_bps
        FROM execution_slippage_feedback
        WHERE broker = ?
        ORDER BY ts_ms DESC
        LIMIT 500
        """,
        (b,),
    ).fetchall()

    for ot, ag, lo, es in rows or []:
        key = f"{str(ot)}|{str(ag)}"
        if key in out:
            continue
        try:
            out[key] = {
                "limit_offset_bps": float(lo or 0.0),
                "extra_slip_bps": float(es or 0.0),
            }
        except Exception as e:
            _warn_nonfatal(
                "execution_analytics_feedback_row_parse_failed",
                "EXECUTION_ANALYTICS_FEEDBACK_ROW_PARSE_FAILED",
                e,
                warn_key=f"execution_analytics_feedback_row_parse_failed:{b}:{ot}:{ag}",
                broker=str(b or ""),
                order_type=str(ot or ""),
                aggressiveness=str(ag or ""),
            )
            continue

    return out


def _build_alpha_preservation_kpis(con, lookback_n: int = 2000) -> None:
    """
    Alpha preservation KPI engine:
    - alpha_remaining_at_fill vs total execution cost
    - produces an efficiency score (alpha_remaining / (1 + cost))
    """
    lookback_n = int(max(50, min(20000, int(lookback_n))))

    rows = con.execute(
        """
        SELECT
          ts_ms, symbol, broker, total_cost_bps, alpha_remaining_at_fill, order_type, aggressiveness, meta_json
        FROM execution_analytics
        WHERE total_cost_bps IS NOT NULL
        ORDER BY ts_ms DESC
        LIMIT ?
        """,
        (lookback_n,),
    ).fetchall()

    buckets: Dict[tuple, List[tuple]] = {}

    for ts_ms, sym, broker, cost_bps, a_rem, order_type, aggressiveness, meta_json in rows or []:
        try:
            ot = str(order_type or "").upper().strip() or "UNKNOWN"
            ag = str(aggressiveness or "").upper().strip() or "UNKNOWN"

            if (ot == "UNKNOWN" or ag == "UNKNOWN") and meta_json:
                try:
                    ex = json.loads(meta_json or "{}")
                    if isinstance(ex, dict):
                        ot = str(ex.get("order_type") or ot).upper().strip() or ot
                        ag = str(ex.get("aggressiveness") or ag).upper().strip() or ag
                except Exception as e:
                    _warn_nonfatal(
                        "execution_analytics_kpi_meta_parse_failed",
                        "EXECUTION_ANALYTICS_KPI_META_PARSE_FAILED",
                        e,
                        warn_key="execution_analytics_kpi_meta_parse_failed",
                        broker=str(broker),
                        symbol=str(sym),
                    )

            key = (
                str(broker or "").strip().lower() or None,
                str(sym or "").upper().strip() or None,
                ot,
                ag,
            )
            buckets.setdefault(key, []).append(
                (float(a_rem or 0.0), float(cost_bps or 0.0))
            )
        except Exception as e:
            _warn_nonfatal(
                "execution_analytics_kpi_bucket_parse_failed",
                "EXECUTION_ANALYTICS_KPI_BUCKET_PARSE_FAILED",
                e,
                warn_key=f"execution_analytics_kpi_bucket_parse_failed:{broker}:{sym}:{order_type}:{aggressiveness}",
                broker=str(broker or ""),
                symbol=str(sym or ""),
                order_type=str(order_type or ""),
                aggressiveness=str(aggressiveness or ""),
            )
            continue

    now_ms = _now_ms()

    for (b, sym, ot, ag), pts in buckets.items():
        if not sym or len(pts) < 10:
            continue

        a_vals = [p[0] for p in pts]
        c_vals = [p[1] for p in pts]

        avg_a = sum(a_vals) / float(len(a_vals))
        avg_c = sum(c_vals) / float(len(c_vals))

        eff = float(avg_a) / float(1.0 + max(0.0, avg_c) / 100.0)

        con.execute(
            """
            INSERT INTO alpha_preservation_kpis(
              ts_ms, broker, symbol, order_type, aggressiveness,
              sample_n, avg_alpha_remaining, avg_total_cost_bps,
              alpha_cost_efficiency, extra_json
            )
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(now_ms),
                (str(b) if b else None),
                str(sym),
                str(ot),
                str(ag),
                int(len(pts)),
                float(avg_a),
                float(avg_c),
                float(eff),
                json.dumps(
                    {"lookback_n": int(lookback_n)},
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )

    con.commit()

# ============================================================
# TSE SUPPORT FUNCTIONS
# ============================================================

def get_slippage_zscore(con):
    try:
        rows = con.execute(
            """
            SELECT slippage_bps
            FROM execution_analytics
            WHERE ts_ms >= (SELECT MAX(ts_ms) - 86400000 FROM execution_analytics)
            """
        ).fetchall()

        vals = [float(r[0]) for r in rows or [] if r and r[0] is not None]
        if len(vals) < 5:
            return 0.0

        mean = sum(vals) / float(len(vals))
        var = sum((x - mean) ** 2 for x in vals) / float(len(vals))
        std = var ** 0.5
        if std <= 1e-12:
            return 0.0

        latest = vals[0]
        return (float(latest) - mean) / std

    except Exception as e:
        _warn_nonfatal(
            "execution_analytics_slippage_zscore_failed",
            "EXECUTION_ANALYTICS_SLIPPAGE_ZSCORE_FAILED",
            e,
            warn_key="execution_analytics_slippage_zscore_failed",
        )
        value = 0.0
        return value

def get_latency_variance_zscore(con):
    try:
        rows = con.execute(
            """
            SELECT age_ms
            FROM execution_analytics
            WHERE ts_ms >= (SELECT MAX(ts_ms) - 86400000 FROM execution_analytics)
            """
        ).fetchall()

        vals = [float(r[0]) for r in rows or [] if r and r[0] is not None]
        if len(vals) < 5:
            return 0.0

        mean = sum(vals) / float(len(vals))
        var = sum((x - mean) ** 2 for x in vals) / float(len(vals))
        std = var ** 0.5
        if std <= 1e-12:
            return 0.0

        latest = vals[0]
        return (float(latest) - mean) / std

    except Exception as e:
        _warn_nonfatal(
            "execution_analytics_latency_variance_zscore_failed",
            "EXECUTION_ANALYTICS_LATENCY_VARIANCE_ZSCORE_FAILED",
            e,
            warn_key="execution_analytics_latency_variance_zscore_failed",
        )
        value = 0.0
        return value

# ============================================================
# Bayesian Rolling Expectancy (for TSE gating)
# ============================================================

def get_rolling_expectancy_stats(con, lookback_n: int = 100):
    """
    Returns:
        {
            mean: float,
            std: float,
            n: int,
            sharpe: float
        }
    Uses realized_pnl from execution_ledger if available.
    """
    try:
        rows = con.execute(
            """
            SELECT realized_pnl
            FROM execution_ledger
            WHERE realized_pnl IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(lookback_n),),
        ).fetchall()
        if not rows:
            return {"mean": 0.0, "std": 0.0, "n": 0, "sharpe": 0.0}

        vals = []
        for r in rows:
            try:
                vals.append(float(r[0]))
            except Exception as e:
                _warn_nonfatal(
                    "execution_analytics_expectancy_value_parse_failed",
                    "EXECUTION_ANALYTICS_EXPECTANCY_VALUE_PARSE_FAILED",
                    e,
                    warn_key="execution_analytics_expectancy_value_parse_failed",
                    row_repr=repr(r),
                )
                continue

        if not vals:
            return {"mean": 0.0, "std": 0.0, "n": 0, "sharpe": 0.0}

        n = len(vals)
        mean = sum(vals) / float(n)

        var = sum((x - mean) ** 2 for x in vals) / float(n) if n > 1 else 0.0
        std = var ** 0.5 if var > 0.0 else 0.0

        sharpe = (mean / std) * (n ** 0.5) if std > 1e-12 else 0.0

        return {
            "mean": float(mean),
            "std": float(std),
            "n": int(n),
            "sharpe": float(sharpe),
        }

    except Exception as e:
        _warn_nonfatal(
            "execution_analytics_expectancy_stats_failed",
            "EXECUTION_ANALYTICS_EXPECTANCY_STATS_FAILED",
            e,
            warn_key="execution_analytics_expectancy_stats_failed",
            lookback_n=int(lookback_n),
        )
        stats = {"mean": 0.0, "std": 0.0, "n": 0, "sharpe": 0.0}
        return stats

def get_expectancy_multiplier(con, lookback_n: int = 100) -> float:
    """
    Converts rolling expectancy into suppression multiplier.
    <1.0 tightens execution
    >1.0 loosens execution
    """
    stats = get_rolling_expectancy_stats(con, lookback_n=lookback_n)

    mean = float(stats.get("mean") or 0.0)
    sharpe = float(stats.get("sharpe") or 0.0)

    # Negative expectancy → tighten
    if mean < 0.0 and sharpe < 0.0:
        return 0.75

    # Strong positive expectancy → allow slight loosen
    if sharpe > 1.0:
        return 1.10

    return 1.0

# ============================================================
# Execution Degradation Snapshot (for auto-pause / TSE)
# ============================================================

def get_execution_degradation_snapshot(con, lookback_n: int = 500) -> Dict[str, Any]:
    """
    Computes:
      - rolling slippage mean
      - rolling latency mean
      - p95 slippage
      - p95 latency
    Used by TSE / EPE for hard auto-pause decisions.
    """
    try:
        if not _table_exists(con, "execution_analytics"):
            return {
                "mean_slippage": 0.0,
                "p95_slippage": 0.0,
                "mean_latency": 0.0,
                "p95_latency": 0.0,
                "n": 0,
            }
        rows = con.execute(
            """
            SELECT slippage_bps, age_ms
            FROM execution_analytics
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(max(50, lookback_n)),),
        ).fetchall()

        slips = []
        lats = []

        for sl, lat in rows or []:
            try:
                slips.append(float(sl))
            except Exception as e:
                _warn_nonfatal(
                    "execution_analytics_summary_slippage_parse_failed",
                    "EXECUTION_ANALYTICS_SUMMARY_SLIPPAGE_PARSE_FAILED",
                    e,
                    warn_key="execution_analytics_summary_slippage_parse_failed",
                    value=sl,
                )
            try:
                lats.append(float(lat))
            except Exception as e:
                _warn_nonfatal(
                    "execution_analytics_summary_latency_parse_failed",
                    "EXECUTION_ANALYTICS_SUMMARY_LATENCY_PARSE_FAILED",
                    e,
                    warn_key="execution_analytics_summary_latency_parse_failed",
                    value=lat,
                )

        if not slips:
            return {
                "mean_slippage": 0.0,
                "p95_slippage": 0.0,
                "mean_latency": 0.0,
                "p95_latency": 0.0,
                "n": 0,
            }

        slips_sorted = sorted(slips)
        lats_sorted = sorted(lats)

        def _p(arr, p):
            if not arr:
                return 0.0
            idx = int(round((len(arr) - 1) * p))
            idx = max(0, min(len(arr) - 1, idx))
            return float(arr[idx])

        return {
            "mean_slippage": sum(slips) / float(len(slips)),
            "p95_slippage": _p(slips_sorted, 0.95),
            "mean_latency": (sum(lats) / float(len(lats)) if lats else 0.0),
            "p95_latency": _p(lats_sorted, 0.95) if lats else 0.0,
            "n": int(len(slips)),
        }

    except Exception as e:
        _warn_nonfatal(
            "execution_analytics_broker_summary_failed",
            "EXECUTION_ANALYTICS_BROKER_SUMMARY_FAILED",
            e,
            warn_key="execution_analytics_broker_summary_failed",
        )
        summary = {
            "mean_slippage": 0.0,
            "p95_slippage": 0.0,
            "mean_latency": 0.0,
            "p95_latency": 0.0,
            "n": 0,
        }
        return summary

# ============================================================
# Adaptive Alpha Half-Life Learning
# ============================================================

def compute_adaptive_half_life(con, symbol: str, lookback_n: int = 500) -> Optional[int]:
    """
    Learns half-life based on alpha_remaining_at_fill decay profile.
    Returns suggested half-life in ms.
    """
    try:
        rows = con.execute(
            """
            SELECT age_ms, alpha_remaining_at_fill
            FROM execution_analytics
            WHERE symbol = ?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(max(50, lookback_n))),
        ).fetchall()

        pairs = [
            (float(age), float(a))
            for age, a in rows or []
            if age is not None and a is not None and a > 0.0
        ]

        if len(pairs) < 20:
            return None

        # Estimate half-life where alpha ≈ 0.5
        diffs = [(abs(a - 0.5), age) for age, a in pairs]
        diffs_sorted = sorted(diffs, key=lambda x: x[0])

        if not diffs_sorted:
            return None

        return int(diffs_sorted[0][1])

    except Exception as e:
        _warn_nonfatal(
            "execution_analytics_adaptive_half_life_failed",
            "EXECUTION_ANALYTICS_ADAPTIVE_HALF_LIFE_FAILED",
            e,
            warn_key=f"execution_analytics_adaptive_half_life_failed:{symbol}",
            symbol=str(symbol),
            lookback_n=int(lookback_n),
        )
        return None

# ============================================================
# Broker Performance Ranking
# ============================================================

def rank_brokers_by_cost(con, lookback_days: int = 7) -> List[Dict[str, Any]]:
    try:
        since = _now_ms() - (int(lookback_days) * 86400000)

        rows = con.execute(
            """
            SELECT broker,
                   COUNT(*) as n,
                   AVG(total_cost_bps) as avg_cost
            FROM execution_analytics
            WHERE ts_ms >= ?
            GROUP BY broker
            ORDER BY avg_cost ASC
            """,
            (int(since),),
        ).fetchall()

        out = []
        for broker, n, avg_cost in rows or []:
            out.append({
                "broker": broker,
                "fills": int(n or 0),
                "avg_total_cost_bps": float(avg_cost or 0.0),
            })

        return out

    except Exception as e:
        _warn_nonfatal(
            "execution_analytics_broker_ranking_failed",
            "EXECUTION_ANALYTICS_BROKER_RANKING_FAILED",
            e,
            warn_key="execution_analytics_broker_ranking_failed",
        )
        rows = []
        return rows
