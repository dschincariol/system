"""
FILE: eval_temporal_shadow.py

Evaluates temporal shadow predictions against realized outcomes and baseline
predictions. The resulting table is the promotion gate for temporal models.
"""

import json
import logging
import os
import time
import math
from typing import Dict, Any, Tuple

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db
from engine.strategy.validation import init_validation_db
from engine.execution.execution_ledger import compute_capital_efficiency_snapshot


_MIN_N = int(os.environ.get("TEMPORAL_PROMOTE_MIN_N", "200"))
_MIN_NET_LABELS = int(os.environ.get("TEMPORAL_PROMOTE_MIN_NET_LABELS", str(_MIN_N)))
_MIN_IMPROVE = float(os.environ.get("TEMPORAL_PROMOTE_MIN_IMPROVEMENT", os.environ.get("PROMOTE_MIN_IMPROVEMENT", "0.01")))
_DIR_TOL = float(os.environ.get("TEMPORAL_PROMOTE_DIRACC_TOL", os.environ.get("PROMOTE_DIRACC_TOL", "0.00")))

_SAFETY_W_CAPEFF = float(os.environ.get("TEMPORAL_SAFETY_W_CAPEFF", "2.0"))
_SAFETY_W_DD = float(os.environ.get("TEMPORAL_SAFETY_W_DD", "0.5"))
_SAFETY_W_SLIP = float(os.environ.get("TEMPORAL_SAFETY_W_SLIP", "0.5"))
_SAFETY_W_DIR = float(os.environ.get("TEMPORAL_SAFETY_W_DIR", "1.0"))
LOG = get_logger("engine.strategy.eval_temporal_shadow")


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="eval_temporal_shadow_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.eval_temporal_shadow",
        extra=extra or None,
        persist=False,
    )


SCHEMA = """
CREATE TABLE IF NOT EXISTS temporal_shadow_eval (
  symbol TEXT NOT NULL,
  horizon_s INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL,

  n INTEGER NOT NULL,

  rmse REAL NOT NULL,
  baseline_rmse REAL NOT NULL,

  directional_acc REAL NOT NULL,
  baseline_directional_acc REAL NOT NULL,

  rmse_improvement REAL NOT NULL DEFAULT 0,
  diracc_delta REAL NOT NULL DEFAULT 0,

  pass_all INTEGER NOT NULL,
  detail_json TEXT NOT NULL,

  PRIMARY KEY (symbol, horizon_s)
);

CREATE INDEX IF NOT EXISTS idx_temporal_shadow_eval_ts
  ON temporal_shadow_eval(ts_ms);
"""


def _rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    if y_true.size == 0:
        return 0.0
    e = (y_true - y_pred).astype(float)
    return float(math.sqrt(float(np.mean(e * e))))


def _load_capital_efficiency_by_symbol_horizon(con):

    out = {}

    # The evaluator tolerates missing execution tables so shadow metrics can be
    # computed even before every execution analytic has been backfilled.
    try:
        rows = con.execute(
            """
            SELECT
              symbol,
              CAST(
                COALESCE(
                  json_extract(extra_json, '$.horizon_s'),
                  json_extract(extra_json, '$.explain.horizon_s'),
                  json_extract(extra_json, '$.signal.horizon_s')
                ) AS INTEGER
              ) AS horizon_s,
              SUM(COALESCE(capital_hours, 0.0)) AS capital_hours_sum,
              SUM(COALESCE(pnl_net, 0.0)) AS pnl_net_sum,
              AVG(COALESCE(drawdown_contrib, 0.0)) AS drawdown_contribution_avg
            FROM execution_capital_efficiency
            GROUP BY symbol, horizon_s
            """
        ).fetchall() or []
    except Exception:
        rows = []

    for sym, horizon_s, capital_hours_sum, pnl_net_sum, drawdown_contribution_avg in rows:

        sym_u = str(sym or "").upper().strip()
        if not sym_u:
            continue

        hi = int(horizon_s or 0)
        cap_h = float(capital_hours_sum or 0.0)
        pnl = float(pnl_net_sum or 0.0)

        out[(sym_u, hi)] = {
            "capital_hours": float(cap_h),
            "capital_efficiency": float(pnl) / max(1e-9, float(cap_h)),
            "drawdown_contribution": float(drawdown_contribution_avg or 0.0),
        }

    try:
        rows = con.execute(
            """
            SELECT
              symbol,
              SUM(COALESCE(capital_hours, 0.0)) AS capital_hours_sum,
              SUM(COALESCE(pnl_net, 0.0)) AS pnl_net_sum,
              AVG(COALESCE(drawdown_contrib, 0.0)) AS drawdown_contribution_avg
            FROM execution_capital_efficiency
            GROUP BY symbol
            """
        ).fetchall() or []
    except Exception:
        rows = []

    for sym, capital_hours_sum, pnl_net_sum, drawdown_contribution_avg in rows:

        sym_u = str(sym or "").upper().strip()
        if not sym_u:
            continue

        cap_h = float(capital_hours_sum or 0.0)
        pnl = float(pnl_net_sum or 0.0)

        out[(sym_u, 0)] = {
            "capital_hours": float(cap_h),
            "capital_efficiency": float(pnl) / max(1e-9, float(cap_h)),
            "drawdown_contribution": float(drawdown_contribution_avg or 0.0),
        }

    return out


def _load_slippage_by_symbol_horizon(con):

    out = {}

    try:
        rows = con.execute(
            """
            SELECT
              o.symbol,
              CAST(
                COALESCE(
                  json_extract(o.extra_json, '$.horizon_s'),
                  json_extract(o.extra_json, '$.explain.horizon_s'),
                  json_extract(o.extra_json, '$.signal.horizon_s')
                ) AS INTEGER
              ) AS horizon_s,
              AVG(ABS(COALESCE(m.slippage_bps, 0.0)) / 10000.0) AS avg_slippage_impact
            FROM execution_orders o
            JOIN execution_metrics m
              ON m.client_order_id = o.client_order_id
            GROUP BY o.symbol, horizon_s
            """
        ).fetchall() or []
    except Exception:
        rows = []

    for sym, horizon_s, avg_slippage_impact in rows:

        sym_u = str(sym or "").upper().strip()
        if not sym_u:
            continue

        out[(sym_u, int(horizon_s or 0))] = float(avg_slippage_impact or 0.0)

    try:
        rows = con.execute(
            """
            SELECT
              o.symbol,
              AVG(ABS(COALESCE(m.slippage_bps, 0.0)) / 10000.0) AS avg_slippage_impact
            FROM execution_orders o
            JOIN execution_metrics m
              ON m.client_order_id = o.client_order_id
            GROUP BY o.symbol
            """
        ).fetchall() or []
    except Exception:
        rows = []

    for sym, avg_slippage_impact in rows:

        sym_u = str(sym or "").upper().strip()
        if not sym_u:
            continue

        out[(sym_u, 0)] = float(avg_slippage_impact or 0.0)

    return out


def _diracc(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    if y_true.size == 0:
        return 0.0
    eps = 1e-9
    yt = np.sign(np.where(np.abs(y_true) < eps, 0.0, y_true))
    yp = np.sign(np.where(np.abs(y_pred) < eps, 0.0, y_pred))
    return float(np.mean(yt == yp))


def _table_columns(con, table_name: str) -> set[str]:
    try:
        return {
            str(row[1] or "").strip().lower()
            for row in (con.execute(f"PRAGMA table_info({table_name})").fetchall() or [])
            if len(row) > 1
        }
    except Exception:
        return set()


def main() -> int:

    init_db()
    init_validation_db()

    try:
        compute_capital_efficiency_snapshot()
    except Exception as e:
        _warn_nonfatal("EVAL_TEMPORAL_SHADOW_CAPITAL_EFFICIENCY_REFRESH_FAILED", e)

    now_ms = int(time.time() * 1000)

    lookback_days = int(os.environ.get("TEMPORAL_EVAL_LOOKBACK_DAYS", "90"))
    min_ts = now_ms - int(lookback_days) * 86400 * 1000

    con = connect()

    try:

        con.executescript(SCHEMA)
        con.commit()

        capital_eff = _load_capital_efficiency_by_symbol_horizon(con)
        slippage_exec = _load_slippage_by_symbol_horizon(con)
        temporal_cols = _table_columns(con, "temporal_predictions")
        temporal_pred_expr = "tp.pred_z"
        if "pred_z" not in temporal_cols and "expected_z" in temporal_cols:
            temporal_pred_expr = "tp.expected_z"
        model_ts_expr = "tp.model_ts_ms" if "model_ts_ms" in temporal_cols else "tp.ts_ms"
        model_kind_expr = "tp.model_kind" if "model_kind" in temporal_cols else "'temporal_mlp'"

        rows = con.execute(
            f"""
            SELECT
              tp.symbol,
              tp.horizon_s,
              tp.event_id,
              {temporal_pred_expr} AS temporal_pred,
              COALESCE(le.net_z, l.impact_z) AS y_true,
              p.predicted_z AS baseline_pred,
              {model_ts_expr} AS model_ts_ms,
              {model_kind_expr} AS model_kind,
              CASE WHEN le.net_z IS NOT NULL THEN 1 ELSE 0 END AS net_label_available
            FROM temporal_predictions tp
            JOIN labels l
              ON l.event_id = tp.event_id
             AND l.symbol = tp.symbol
             AND l.horizon_s = tp.horizon_s
            LEFT JOIN labels_exec le
              ON le.event_id = l.event_id
             AND le.symbol = l.symbol
             AND le.horizon_s = l.horizon_s
             AND le.realized = 1
            LEFT JOIN predictions p
              ON p.event_id = tp.event_id
             AND p.symbol = tp.symbol
             AND p.horizon_s = tp.horizon_s
            WHERE tp.ts_ms >= ?
              AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
            """,
            (int(min_ts),),
        ).fetchall()

        by: Dict[Tuple[str, int], Dict[str, Any]] = {}

        for sym, h, eid, tpred, y, bpred, model_ts_ms, model_kind, net_label_available in rows or []:

            sym_u = str(sym or "").upper().strip()
            hi = int(h or 0)

            if not sym_u or hi <= 0:
                continue

            try:
                yt = float(y)
                tpv = float(tpred)
            except Exception as e:
                log_failure(
                    LOG,
                    event="eval_temporal_shadow_row_parse_failed",
                    code="EVAL_TEMPORAL_SHADOW_ROW_PARSE_FAILED",
                    message=str(e),
                    error=e,
                    level=logging.WARNING,
                    component="engine.strategy.eval_temporal_shadow",
                    persist=False,
                )
                continue

            if not np.isfinite(yt) or not np.isfinite(tpv):
                continue

            b_ok = False

            try:
                bpv = float(bpred)
                if np.isfinite(bpv):
                    b_ok = True
            except Exception:
                bpv = 0.0

            k = (sym_u, hi)

            g = by.get(k)

            if g is None:
                g = {
                    "y": [],
                    "t": [],
                    "b_y": [],
                    "b": [],
                    "drawdown": 0.0,
                    "slippage": [],
                    "signed_alpha": 0.0,
                    "latest_model_ts_ms": int(model_ts_ms or 0),
                    "latest_model_kind": str(model_kind or "temporal_mlp"),
                    "net_label_count": 0,
                }
                by[k] = g

            g["y"].append(yt)
            g["t"].append(tpv)
            g["net_label_count"] = int(g.get("net_label_count") or 0) + (1 if int(net_label_available or 0) else 0)

            side = 1.0 if tpv >= 0 else -1.0
            pnl = side * yt

            if pnl < 0:
                g["drawdown"] += abs(pnl)

            g["signed_alpha"] += pnl
            g["slippage"].append(abs(tpv - yt))

            if b_ok:
                g["b_y"].append(yt)
                g["b"].append(bpv)

            try:
                mts = int(model_ts_ms or 0)
                if mts > int(g["latest_model_ts_ms"] or 0):
                    g["latest_model_ts_ms"] = mts
                    g["latest_model_kind"] = str(model_kind or "temporal_mlp")
            except Exception as e:
                _warn_nonfatal(
                    "EVAL_TEMPORAL_SHADOW_MODEL_METADATA_UPDATE_FAILED",
                    e,
                    symbol=str(sym_u),
                    horizon_s=int(hi),
                )

        cur = con.cursor()

        for (sym, hi), g in by.items():

            y = np.asarray(g["y"], dtype=np.float32)
            t = np.asarray(g["t"], dtype=np.float32)

            b_y = np.asarray(g["b_y"], dtype=np.float32)
            b = np.asarray(g["b"], dtype=np.float32)

            n = int(y.size)

            if n <= 0:
                continue

            rmse = _rmse(y, t)
            da = _diracc(y, t)

            if b_y.size >= 10:
                brmse = _rmse(b_y, b)
                bda = _diracc(b_y, b)
            else:
                brmse = float("inf")
                bda = 0.0

            rmse_improvement = (
                (brmse - rmse) / brmse if np.isfinite(brmse) and brmse > 1e-12 else 0.0
            )

            diracc_delta = da - bda

            drawdown = float(g["drawdown"])
            avg_slippage = float(np.mean(g["slippage"])) if g["slippage"] else 0.0
            signed_alpha = float(g["signed_alpha"])

            exec_metrics = (
                capital_eff.get((sym, hi))
                or capital_eff.get((sym, 0))
                or {}
            )

            capital_efficiency = float(exec_metrics.get("capital_efficiency") or 0.0)
            drawdown = float(exec_metrics.get("drawdown_contribution") or drawdown)

            exec_slip = (
                slippage_exec.get((sym, hi))
                or slippage_exec.get((sym, 0))
            )
            if exec_slip is not None:
                avg_slippage = float(exec_slip)

            if not np.isfinite(capital_efficiency):
                capital_efficiency = 0.0

            if not np.isfinite(avg_slippage):
                avg_slippage = 0.0

            safety_score = (
                capital_efficiency * _SAFETY_W_CAPEFF
                - drawdown * _SAFETY_W_DD
                - avg_slippage * _SAFETY_W_SLIP
                + da * _SAFETY_W_DIR
            )

            reasons = []
            pass_all = True
            net_label_count = int(g.get("net_label_count") or 0)
            net_label_coverage = float(net_label_count) / float(max(1, n))

            if n < _MIN_N:
                pass_all = False
                reasons.append(f"n<{_MIN_N}")
            if net_label_count < int(_MIN_NET_LABELS):
                pass_all = False
                reasons.append("net_cost_labels_unavailable")

            if not np.isfinite(brmse) or brmse <= 0:
                pass_all = False
                reasons.append("baseline_unavailable")
            else:
                if rmse_improvement < _MIN_IMPROVE:
                    pass_all = False
                    reasons.append("rmse_not_improved")

                if da < (bda - _DIR_TOL):
                    pass_all = False
                    reasons.append("diracc_regressed")

            if safety_score <= 0:
                pass_all = False
                reasons.append("negative_safety_score")

            detail = {
                "symbol": sym,
                "horizon_s": hi,
                "n": n,
                "rmse": float(rmse),
                "baseline_rmse": float(brmse) if np.isfinite(brmse) else None,
                "directional_acc": float(da),
                "baseline_directional_acc": float(bda),
                "rmse_improvement": float(rmse_improvement),
                "diracc_delta": float(diracc_delta),
                "capital_efficiency": float(capital_efficiency),
                "drawdown_contribution": float(drawdown),
                "avg_slippage_impact": float(avg_slippage),
                "signed_alpha": float(signed_alpha),
                "net_edge": float(signed_alpha / float(max(1, n))),
                "gross_only_eval_blocked": bool(net_label_count < int(_MIN_NET_LABELS)),
                "net_label_count": int(net_label_count),
                "net_label_coverage": float(net_label_coverage),
                "min_net_labels": int(_MIN_NET_LABELS),
                "capital_hours": float(exec_metrics.get("capital_hours") or 0.0),
                "safety_score": float(safety_score),
                "latest_model_ts_ms": int(g.get("latest_model_ts_ms") or 0),
                "latest_model_kind": str(g.get("latest_model_kind") or "temporal_mlp"),
                "reasons": reasons,
            }

            cur.execute(
                """
                INSERT INTO temporal_shadow_eval(
                  symbol, horizon_s, ts_ms,
                  n, rmse, baseline_rmse,
                  directional_acc, baseline_directional_acc,
                  rmse_improvement, diracc_delta,
                  pass_all, detail_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol, horizon_s) DO UPDATE SET
                  ts_ms=excluded.ts_ms,
                  n=excluded.n,
                  rmse=excluded.rmse,
                  baseline_rmse=excluded.baseline_rmse,
                  directional_acc=excluded.directional_acc,
                  baseline_directional_acc=excluded.baseline_directional_acc,
                  rmse_improvement=excluded.rmse_improvement,
                  diracc_delta=excluded.diracc_delta,
                  pass_all=excluded.pass_all,
                  detail_json=excluded.detail_json
                """,
                (
                    sym,
                    int(hi),
                    int(now_ms),
                    int(n),
                    float(rmse),
                    float(brmse) if np.isfinite(brmse) else float("inf"),
                    float(da),
                    float(bda),
                    float(rmse_improvement),
                    float(diracc_delta),
                    1 if pass_all else 0,
                    json.dumps(detail, separators=(",", ":"), sort_keys=True),
                ),
            )

        con.commit()

        print(json.dumps({"ok": True, "groups": len(by)}, indent=2))

        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
