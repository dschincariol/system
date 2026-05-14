"""
FILE: train_drawdown_policy.py

Execution subsystem module for `train_drawdown_policy`.
"""

# train_drawdown_policy.py
"""
Learn drawdown multipliers dd_factor(drawdown) from realized net outcomes.

Inputs:
- equity_history(ts_ms,equity) -> drawdown at time
- predictions(ts_ms, confidence, event_id,symbol,horizon_s)
- labels_exec(net_ret, realized, ...) preferred

Output:
- stores a new size_policy row with dd_factors included in params_json
  (policy method becomes 'bucket_sharpe_monotone_dd')
"""

import os
import json
import time
import math
import logging
from typing import List, Tuple, Optional

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db, run_write_txn

LOG = get_logger("engine.execution.train_drawdown_policy")

LOOKBACK_DAYS = int(os.environ.get("DD_POLICY_LOOKBACK_DAYS", "180"))
MIN_SAMPLES = int(os.environ.get("DD_POLICY_MIN_SAMPLES", "300"))

# Drawdown bins (fractional drawdown)
# default: 0-5%, 5-10%, 10-20%, 20%+
DD_BINS = os.environ.get("DD_POLICY_BINS", "0.05,0.10,0.20")

PREFER_REALIZED = os.environ.get("DD_POLICY_PREFER_REALIZED", "1") == "1"
SHARPE_NORM = float(os.environ.get("DD_POLICY_SHARPE_NORM", "1.0"))
MIN_FACTOR = float(os.environ.get("DD_POLICY_MIN_FACTOR", "0.2"))   # never go fully to zero by default
MAX_FACTOR = float(os.environ.get("DD_POLICY_MAX_FACTOR", "1.0"))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _std(vals: List[float]) -> float:
    if len(vals) < 2:
        return 0.0
    m = sum(vals) / len(vals)
    var = sum((x - m) ** 2 for x in vals) / max(1, len(vals) - 1)
    return math.sqrt(var)


def _parse_bins() -> List[float]:
    out = []
    for part in (DD_BINS.split(",") if DD_BINS else []):
        part = part.strip()
        if not part:
            continue
        out.append(float(part))
    out = sorted(set(out))
    return out


def _dd_bucket(dd: float, bins: List[float]) -> int:
    # returns bucket index 0..len(bins)
    for i, b in enumerate(bins):
        if dd < b:
            return i
    return len(bins)


def _load_equity_series(con, min_ts: int) -> List[Tuple[int, float]]:
    rows = con.execute(
        """
        SELECT ts_ms, equity
        FROM equity_history
        WHERE ts_ms >= ?
        ORDER BY ts_ms ASC
        """,
        (int(min_ts),),
    ).fetchall()
    out = []
    for ts, eq in rows or []:
        try:
            out.append((int(ts), float(eq)))
        except Exception as e:
            log_failure(
                LOG,
                event="train_drawdown_policy_equity_row_parse_failed",
                code="TRAIN_DRAWDOWN_POLICY_EQUITY_ROW_PARSE_FAILED",
                message="Drawdown policy equity row parse failed.",
                error=e,
                level=logging.WARNING,
                component="engine.execution.train_drawdown_policy",
                persist=False,
            )
    return out


def _drawdown_at_ts(series: List[Tuple[int, float]], ts_ms: int) -> Optional[float]:
    """
    Compute drawdown at the last equity point <= ts_ms using running peak.
    series is sorted asc.
    Returns dd in [0..1], or None if not enough data.
    """
    if not series:
        return None

    peak = -1e30
    last_eq = None
    for t, eq in series:
        if eq > peak:
            peak = eq
        if t <= ts_ms:
            last_eq = eq
        else:
            break

    if last_eq is None or peak <= 0:
        return None

    dd = max(0.0, min(1.0, 1.0 - (float(last_eq) / float(peak))))
    return dd


def _table_columns(con, table_name: str) -> set[str]:
    return {
        str(row[1] or "").strip()
        for row in (con.execute(f"PRAGMA table_info({table_name})").fetchall() or [])
        if row and len(row) > 1
    }


def _store_drawdown_policy(con, *, ts_ms: int, params: dict, metrics: dict) -> int:
    size_policy_columns = _table_columns(con, "size_policy")
    params_json = json.dumps(params, separators=(",", ":"), sort_keys=True)
    metrics_json = json.dumps(metrics, separators=(",", ":"), sort_keys=True)

    if "lookback_days" in size_policy_columns and "buckets" in size_policy_columns:
        con.execute(
            """
            INSERT INTO size_policy(ts_ms, lookback_days, buckets, method, params_json, metrics_json)
            VALUES (?,?,?,?,?,?)
            """,
            (int(ts_ms), int(LOOKBACK_DAYS), 0, "dd_factor_monotone", params_json, metrics_json),
        )
    else:
        con.execute(
            """
            INSERT INTO size_policy(ts_ms, method, params_json, metrics_json)
            VALUES (?,?,?,?)
            """,
            (int(ts_ms), "dd_factor_monotone", params_json, metrics_json),
        )

    row = con.execute("SELECT last_insert_rowid()").fetchone()
    return int((row or [0])[0] or 0)


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("train_drawdown_policy must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    now = _now_ms()
    min_ts = now - LOOKBACK_DAYS * 86400 * 1000

    bins = _parse_bins()

    con = connect(readonly=True)
    try:
        series = _load_equity_series(con, min_ts=min_ts)
        if len(series) < 50:
            raise SystemExit("[dd_policy] not enough equity_history points; enable snapshot_equity automation first")

        # Learn the policy from realized outcome buckets conditioned on the
        # drawdown regime present when the prediction was made.
        # Join confidence + net_ret
        if PREFER_REALIZED:
            q = """
            SELECT p.ts_ms, p.confidence, le.net_ret
            FROM predictions p
            JOIN labels_exec le
              ON le.event_id=p.event_id AND le.symbol=p.symbol AND le.horizon_s=p.horizon_s
            WHERE p.ts_ms >= ?
              AND le.net_ret IS NOT NULL
              AND le.realized=1
              AND COALESCE(le.source, '') <> 'broker_sim_placeholder'
            LIMIT 100000
            """
            rows = con.execute(q, (int(min_ts),)).fetchall()
            if len(rows or []) < MIN_SAMPLES:
                q2 = """
                SELECT p.ts_ms, p.confidence, le.net_ret
                FROM predictions p
                JOIN labels_exec le
                  ON le.event_id=p.event_id AND le.symbol=p.symbol AND le.horizon_s=p.horizon_s
                WHERE p.ts_ms >= ?
                  AND le.net_ret IS NOT NULL
                  AND COALESCE(le.source, '') <> 'broker_sim_placeholder'
                LIMIT 100000
                """
                rows = con.execute(q2, (int(min_ts),)).fetchall()
        else:
            q2 = """
            SELECT p.ts_ms, p.confidence, le.net_ret
            FROM predictions p
            JOIN labels_exec le
              ON le.event_id=p.event_id AND le.symbol=p.symbol AND le.horizon_s=p.horizon_s
            WHERE p.ts_ms >= ?
              AND le.net_ret IS NOT NULL
              AND COALESCE(le.source, '') <> 'broker_sim_placeholder'
            LIMIT 100000
            """
            rows = con.execute(q2, (int(min_ts),)).fetchall()

        # Bin by drawdown
        buckets = [[] for _ in range(len(bins) + 1)]
        used = 0

        for ts_ms, conf, net_ret in rows or []:
            try:
                ts_ms = int(ts_ms)
                rr = float(net_ret)
            except Exception as e:
                log_failure(
                    LOG,
                    event="train_drawdown_policy_row_parse_failed",
                    code="TRAIN_DRAWDOWN_POLICY_ROW_PARSE_FAILED",
                    message="Drawdown policy row parse failed.",
                    error=e,
                    level=logging.WARNING,
                    component="engine.execution.train_drawdown_policy",
                    extra={"ts_ms": repr(ts_ms)[:120], "net_ret": repr(net_ret)[:120]},
                    persist=False,
                )
                continue

            dd = _drawdown_at_ts(series, ts_ms)
            if dd is None:
                continue

            bi = _dd_bucket(dd, bins)
            buckets[bi].append(rr)
            used += 1

        if used < MIN_SAMPLES:
            raise SystemExit(f"[dd_policy] not enough labeled samples after dd join: {used} < {MIN_SAMPLES}")

        # Compute dd_factors from a Sharpe-like proxy, then enforce monotonic
        # conservatism: deeper drawdown must never increase size.
        # Compute dd_factors from Sharpe proxy
        points = []
        for i in range(len(bins) + 1):
            vals = buckets[i]
            n = len(vals)
            mean = sum(vals) / n if n > 0 else 0.0
            sd = _std(vals)
            sharpe = (mean / sd) if sd > 1e-12 else 0.0

            f = sharpe / max(1e-9, SHARPE_NORM)
            if f < 0.0:
                f = 0.0
            f = max(MIN_FACTOR, min(MAX_FACTOR, f))

            lo = 0.0 if i == 0 else bins[i - 1]
            hi = bins[i] if i < len(bins) else 1.0

            points.append({
                "dd_lo": float(lo),
                "dd_hi": float(hi),
                "n": int(n),
                "mean_net_ret": float(mean),
                "std_net_ret": float(sd),
                "factor": float(f),
            })

        # Enforce NON-INCREASING with drawdown (more drawdown => <= factor)
        # i.e., factor[0] >= factor[1] >= ...
        last = points[0]["factor"] if points else 1.0
        for i in range(1, len(points)):
            if points[i]["factor"] > last:
                points[i]["factor"] = last
            else:
                last = points[i]["factor"]

        # Store as a new size_policy record (re-using size_policy table)
        ts = _now_ms()
        params = {
            "lookback_days": LOOKBACK_DAYS,
            "dd_bins": bins,
            "prefer_realized": bool(PREFER_REALIZED),
            "sharpe_norm": SHARPE_NORM,
            "min_factor": MIN_FACTOR,
            "max_factor": MAX_FACTOR,
            "dd_points": points,  # stored also here for retrieval
        }
        metrics = {
            "n_samples": int(used),
            "method": "dd_factor_monotone",
        }

        

        pid = int(
            run_write_txn(
                lambda write_con: _store_drawdown_policy(
                    write_con,
                    ts_ms=int(ts),
                    params=params,
                    metrics=metrics,
                ),
                table="size_policy",
                operation="train_drawdown_policy_store",
                context={"n_samples": int(used), "dd_bins": int(len(points))},
            )
        )
        print(f"[dd_policy] stored policy_id={pid} used={used} bins={bins} factors={[p['factor'] for p in points]}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
