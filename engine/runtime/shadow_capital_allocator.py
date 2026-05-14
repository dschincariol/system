"""
FILE: shadow_capital_allocator.py

Runtime subsystem module for `shadow_capital_allocator`.
"""

# engine/dev_core/shadow_capital_allocator.py
"""
Shadow Capital Allocation Scoring

Computes model-level risk-adjusted governance scores so safer models can beat
higher-PnL risky ones.

Inputs (best-effort, optional):
- shadow_metrics (rmse/dir_acc/net_rmse + n)
- trade_attribution_ledger (slippage_bps, pnl, fees) with model_json hints
- equity_history / portfolio_bt_points (optional drawdown proxy)

Persists to:
- shadow_capital_scores (created in dev_core/storage.py init_db)
"""

import json
import logging
import os
import math
import time
from typing import Dict, Any, List, Optional

from engine.runtime.allocator_status import _table_exists
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect as _db_connect


DEFAULT_WINDOW_S = int(os.environ.get("SHADOW_CAPITAL_WINDOW_S", "86400"))  # 24h
DEFAULT_REGIME = os.environ.get("SHADOW_CAPITAL_REGIME", "global").strip() or "global"

# Composite weights (env overridable)
W_DIR_ACC = float(os.environ.get("SHADOW_W_DIR_ACC", "1.0"))
W_NET_RMSE = float(os.environ.get("SHADOW_W_NET_RMSE", "1.0"))
W_SLIP_MEAN = float(os.environ.get("SHADOW_W_SLIP_MEAN", "1.0"))
W_SLIP_STD = float(os.environ.get("SHADOW_W_SLIP_STD", "0.5"))
W_EXEC_LATENCY = float(os.environ.get("SHADOW_W_EXEC_LATENCY", "0.25"))
W_DD = float(os.environ.get("SHADOW_W_DD", "1.0"))
W_CAP_EFF = float(os.environ.get("SHADOW_W_CAP_EFF", "1.0"))

# Guardrails
MIN_N = int(os.environ.get("SHADOW_CAPITAL_MIN_N", "20"))
MAX_ROWS_ATTR = int(os.environ.get("SHADOW_CAPITAL_MAX_ATTR_ROWS", "200000"))
LOG = get_logger("engine.runtime.shadow_capital_allocator")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.runtime.shadow_capital_allocator",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)





def _safe_float(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception as exc:
        _warn_nonfatal(
            "shadow_capital_allocator_safe_float_failed",
            "SHADOW_CAPITAL_ALLOCATOR_SAFE_FLOAT_FAILED",
            exc,
            warn_key=f"shadow_capital_allocator_safe_float:{x}",
            raw_value=x,
        )
        return default


def _safe_int(x, default=0):
    try:
        if x is None:
            return default
        return int(x)
    except Exception as exc:
        _warn_nonfatal(
            "shadow_capital_allocator_safe_int_failed",
            "SHADOW_CAPITAL_ALLOCATOR_SAFE_INT_FAILED",
            exc,
            warn_key=f"shadow_capital_allocator_safe_int:{x}",
            raw_value=x,
        )
        return default


def _try_json_load(s: Any) -> Dict[str, Any]:
    if s is None:
        return {}
    try:
        if isinstance(s, (bytes, bytearray)):
            s = s.decode("utf-8", errors="replace")
    except Exception as exc:
        _warn_nonfatal(
            "shadow_capital_allocator_json_bytes_decode_failed",
            "SHADOW_CAPITAL_ALLOCATOR_JSON_BYTES_DECODE_FAILED",
            exc,
            warn_key="shadow_capital_allocator_json_bytes_decode_failed",
        )
    try:
        txt = str(s).strip()
        if not txt:
            return {}
        return json.loads(txt)
    except Exception as exc:
        _warn_nonfatal(
            "shadow_capital_allocator_json_load_failed",
            "SHADOW_CAPITAL_ALLOCATOR_JSON_LOAD_FAILED",
            exc,
            warn_key=f"shadow_capital_allocator_json_load:{str(s)[:80]}",
            raw_preview=str(s)[:200],
        )
        return {}


def _extract_model_name_from_model_json(model_json: Any) -> str:
    mj = _try_json_load(model_json)
    for k in ("model_name", "name", "model", "id"):
        v = mj.get(k)
        if v:
            return str(v).strip()
    return ""


def _extract_model_kind_from_model_json(model_json: Any) -> Optional[str]:
    mj = _try_json_load(model_json)
    for k in ("model_kind", "kind", "type"):
        v = mj.get(k)
        if v:
            return str(v).strip()
    return None


def _extract_model_ts_ms_from_model_json(model_json: Any) -> Optional[int]:
    mj = _try_json_load(model_json)
    for k in ("model_ts_ms", "ts_ms", "trained_ts_ms"):
        v = mj.get(k)
        if v is not None:
            try:
                return int(v)
            except Exception as exc:
                _warn_nonfatal(
                    "shadow_capital_allocator_model_ts_parse_failed",
                    "SHADOW_CAPITAL_ALLOCATOR_MODEL_TS_PARSE_FAILED",
                    exc,
                    warn_key="shadow_capital_allocator_model_ts_parse_failed",
                    value=v,
                    key=k,
                )
    return None


def _stddev(vals: List[float]) -> float:
    if not vals:
        return 0.0
    if len(vals) == 1:
        return 0.0
    m = sum(vals) / float(len(vals))
    var = sum((x - m) ** 2 for x in vals) / float(len(vals) - 1)
    return math.sqrt(max(0.0, var))


def _compute_drawdown_proxy(con, since_ms: int) -> Optional[float]:
    """
    Best-effort drawdown proxy:
    - prefer portfolio_bt_points.drawdown if exists
    - else equity_history max drawdown over window
    Returns positive magnitude (e.g. 0.12 for 12% dd) when possible.
    Shadow scoring is governance-only, so missing drawdown data should not
    stop scoring entirely.
    """
    # 1) portfolio_bt_points.drawdown
    if _table_exists(con, "portfolio_bt_points"):
        try:
            rows = con.execute(
                """
                SELECT drawdown
                FROM portfolio_bt_points
                WHERE ts_ms >= ?
                ORDER BY ts_ms ASC
                """,
                (int(since_ms),),
            ).fetchall()
            dds = []
            for r in rows or []:
                dd = _safe_float(r[0], None)
                if dd is None:
                    continue
                # expect dd as positive fraction; if negative, normalize to abs
                dds.append(abs(float(dd)))
            if dds:
                return float(max(dds))
        except Exception as exc:
            _warn_nonfatal(
                "shadow_capital_allocator_drawdown_from_bt_points_failed",
                "SHADOW_CAPITAL_ALLOCATOR_DRAWDOWN_FROM_BT_POINTS_FAILED",
                exc,
                warn_key="shadow_capital_allocator_drawdown_from_bt_points_failed",
                since_ms=int(since_ms),
            )

    # 2) equity_history
    if _table_exists(con, "equity_history"):
        try:
            rows = con.execute(
                """
                SELECT ts_ms, equity
                FROM equity_history
                WHERE ts_ms >= ?
                ORDER BY ts_ms ASC
                """,
                (int(since_ms),),
            ).fetchall()
            eq = []
            for r in rows or []:
                v = _safe_float(r[1], None)
                if v is None:
                    continue
                eq.append(float(v))
            if len(eq) >= 2:
                peak = eq[0]
                max_dd = 0.0
                for v in eq:
                    peak = max(peak, v)
                    if peak > 0:
                        dd = (peak - v) / peak
                        max_dd = max(max_dd, dd)
                return float(max_dd)
        except Exception as exc:
            _warn_nonfatal(
                "shadow_capital_allocator_drawdown_from_equity_history_failed",
                "SHADOW_CAPITAL_ALLOCATOR_DRAWDOWN_FROM_EQUITY_HISTORY_FAILED",
                exc,
                warn_key="shadow_capital_allocator_drawdown_from_equity_history_failed",
                since_ms=int(since_ms),
            )

    return None


def _read_shadow_metrics(con, since_ms: int, regime: str) -> Dict[str, Dict[str, Any]]:
    """
    Returns per model_name:
      {rmse, dir_acc, net_rmse, n}
    Uses latest row per model_name in window.
    """
    out: Dict[str, Dict[str, Any]] = {}

    if not _table_exists(con, "shadow_metrics"):
        return out

    try:
        rows = con.execute(
            """
            SELECT window_end_ms, regime, model_name, horizon_s, rmse, dir_acc, net_rmse, n, extra_json
            FROM shadow_metrics
            WHERE window_end_ms >= ?
            ORDER BY window_end_ms DESC
            LIMIT 5000
            """,
            (int(since_ms),),
        ).fetchall()
    except Exception:
        rows = []

    for r in rows or []:
        try:
            reg = str(r[1] or "global")
            if str(regime) != "global" and reg != str(regime):
                continue
            name = str(r[2] or "").strip()
            if not name:
                continue

            # Keep the latest row per model. Shadow governance should compare
            # current contenders, not average across stale training windows.
            # keep latest per model_name
            if name in out:
                continue

            out[name] = {
                "rmse": _safe_float(r[4], None),
                "dir_acc": _safe_float(r[5], None),
                "net_rmse": _safe_float(r[6], None),
                "n": _safe_int(r[7], 0),
            }
        except Exception as exc:
            _warn_nonfatal(
                "shadow_capital_allocator_metrics_row_parse_failed",
                "SHADOW_CAPITAL_ALLOCATOR_METRICS_ROW_PARSE_FAILED",
                exc,
                warn_key=f"shadow_capital_allocator_metrics_row:{r}",
                raw_row=str(r),
            )
            continue

    return out


def _read_execution_quality_by_model(con, since_ms: int) -> Dict[str, Dict[str, Any]]:
    """
    Reads trade_attribution_ledger execution quality by model.
    Returns:
      model_name -> {n, mean, std, latency_mean, latency_std}
    """
    out: Dict[str, Dict[str, Any]] = {}

    if not _table_exists(con, "trade_attribution_ledger"):
        return out

    cols = set()
    try:
        cols = {str(r[1] or "").strip().lower() for r in (con.execute("PRAGMA table_info(trade_attribution_ledger)").fetchall() or [])}
    except Exception:
        cols = set()

    latency_expr = "execution_latency_ms" if "execution_latency_ms" in cols else "NULL"

    # Pull recent rows with an explicit cap so governance scoring cannot load
    # an unbounded attribution history into memory.
    try:
        rows = con.execute(
            f"""
            SELECT model_json, slippage_bps, {latency_expr}
            FROM trade_attribution_ledger
            WHERE ts_ms >= ?
            ORDER BY ts_ms DESC
            LIMIT {int(MAX_ROWS_ATTR)}
            """,
            (int(since_ms),),
        ).fetchall()
    except Exception:
        rows = []

    buf: Dict[str, List[float]] = {}
    latency_buf: Dict[str, List[float]] = {}

    for r in rows or []:
        try:
            name = _extract_model_name_from_model_json(r[0])
            if not name:
                continue
            slip = _safe_float(r[1], None)
            if slip is None:
                continue
            buf.setdefault(name, []).append(max(0.0, float(slip)))
            latency_val = _safe_float(r[2], None)
            if latency_val is not None:
                latency_buf.setdefault(name, []).append(max(0.0, float(latency_val)))
        except Exception as exc:
            _warn_nonfatal(
                "shadow_capital_allocator_execution_quality_row_parse_failed",
                "SHADOW_CAPITAL_ALLOCATOR_EXECUTION_QUALITY_ROW_PARSE_FAILED",
                exc,
                warn_key=f"shadow_capital_allocator_execution_quality_row:{r}",
                raw_row=str(r),
            )
            continue

    for name, vals in buf.items():
        if not vals:
            continue
        m = sum(vals) / float(len(vals))
        lats = latency_buf.get(name) or []
        lat_mean = (sum(lats) / float(len(lats))) if lats else 0.0
        out[name] = {
            "n": int(len(vals)),
            "mean": float(m),
            "std": float(_stddev(vals)),
            "latency_mean": float(lat_mean),
            "latency_std": float(_stddev(lats)),
        }

    return out


def _read_cap_eff_by_model(con, since_ms: int) -> Dict[str, Dict[str, Any]]:
    """
    Capital efficiency proxy using shadow_predictions:
      cap_eff ~ mean(net_pred_z) / (1 + mean(cost_est))
    If net_pred_z missing, use predicted_z.
    Returns:
      model_name -> {n, cap_eff, mean_cost, mean_edge}
    """
    out: Dict[str, Dict[str, Any]] = {}

    if not _table_exists(con, "shadow_predictions"):
        return out

    try:
        rows = con.execute(
            """
            SELECT model_name, predicted_z, net_pred_z, cost_est
            FROM shadow_predictions
            WHERE ts_ms >= ?
            ORDER BY ts_ms DESC
            LIMIT 200000
            """,
            (int(since_ms),),
        ).fetchall()
    except Exception:
        rows = []

    agg: Dict[str, Dict[str, float]] = {}
    cnt: Dict[str, int] = {}

    for r in rows or []:
        try:
            name = str(r[0] or "").strip()
            if not name:
                continue
            pred = _safe_float(r[1], None)
            netp = _safe_float(r[2], None)
            cost = _safe_float(r[3], 0.0)
            edge = netp if netp is not None else pred
            if edge is None:
                continue

            a = agg.setdefault(name, {"edge_sum": 0.0, "cost_sum": 0.0})
            a["edge_sum"] += float(edge)
            a["cost_sum"] += float(cost or 0.0)
            cnt[name] = cnt.get(name, 0) + 1
        except Exception as exc:
            _warn_nonfatal(
                "shadow_capital_allocator_cap_eff_row_parse_failed",
                "SHADOW_CAPITAL_ALLOCATOR_CAP_EFF_ROW_PARSE_FAILED",
                exc,
                warn_key=f"shadow_capital_allocator_cap_eff_row:{r}",
                raw_row=str(r),
            )
            continue

    for name, a in agg.items():
        n = int(cnt.get(name, 0))
        if n <= 0:
            continue
        mean_edge = float(a["edge_sum"] / float(n))
        mean_cost = float(a["cost_sum"] / float(n))
        cap_eff = float(mean_edge / (1.0 + max(0.0, mean_cost)))
        out[name] = {
            "n": n,
            "cap_eff": cap_eff,
            "mean_cost": mean_cost,
            "mean_edge": mean_edge,
        }

    return out


def compute_and_persist_shadow_capital_scores(
    *,
    window_s: int = DEFAULT_WINDOW_S,
    regime: str = DEFAULT_REGIME,
    min_n: int = MIN_N,
) -> Dict[str, Any]:
    """
    Computes and upserts shadow_capital_scores for window_s/regime.
    This is advisory governance data for model selection and capital ranking;
    it does not place orders or directly resize live positions.

    Composite score (higher better):
      + W_DIR_ACC * dir_acc
      - W_NET_RMSE * net_rmse
      - W_SLIP_MEAN * slip_mean
      - W_SLIP_STD * slip_std
      - W_EXEC_LATENCY * execution_latency_seconds
      - W_DD * drawdown_proxy
      + W_CAP_EFF * cap_eff
    """
    now_ms = _now_ms()
    con = _db_connect()
    try:
        if not _table_exists(con, "shadow_capital_scores"):
            return {"ok": False, "error": "shadow_capital_scores table missing (run init_db?)"}
        if not _table_exists(con, "model_marketplace_scores"):
            return {"ok": False, "error": "model_marketplace_scores table missing"}

        weights = {
            "W_REALIZED_PNL": 1.0,
            "W_UNREALIZED_PNL": 1.0,
            "W_DD": W_DD,
        }

        try:
            rows = con.execute(
                """
                SELECT model_name, regime, trades, score, net_pnl, meta_json
                FROM model_marketplace_scores
                ORDER BY updated_ts_ms DESC
                """
            ).fetchall()
        except Exception as exc:
            _warn_nonfatal(
                "shadow_capital_allocator_marketplace_scores_read_failed",
                "SHADOW_CAPITAL_ALLOCATOR_MARKETPLACE_SCORES_READ_FAILED",
                exc,
                warn_key="shadow_capital_allocator_marketplace_scores_read_failed",
                regime=str(regime),
            )
            rows = []

        agg: Dict[tuple, Dict[str, Any]] = {}
        upserts = 0
        skipped = 0
        for model_name, row_regime, trades, score, net_pnl, meta_json in rows or []:
            name = str(model_name or "").strip()
            reg = str(row_regime or "global").strip() or "global"
            if not name:
                continue
            if str(regime) != "global" and reg != str(regime):
                continue

            meta = _try_json_load(meta_json)
            score_source = str(meta.get("score_source") or "").strip().lower()
            if score_source not in {"pnl_attribution", "execution_fills", "broker_fills"}:
                continue
            realized_pnl = _safe_float(meta.get("realized_pnl"), None)
            unrealized_pnl = _safe_float(meta.get("unrealized_pnl"), None)
            total_pnl = _safe_float(meta.get("total_pnl"), None)
            if realized_pnl is None or unrealized_pnl is None or total_pnl is None:
                skipped += 1
                continue
            realized_pnl = _safe_float(realized_pnl, 0.0)
            unrealized_pnl = _safe_float(unrealized_pnl, 0.0)

            key = (name, reg)
            cur = agg.get(key) or {
                "n": 0,
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "total_pnl": 0.0,
                "drawdown_proxy": 0.0,
            }
            cur["n"] += max(0, int(trades or 0))
            cur["realized_pnl"] += float(realized_pnl or 0.0)
            cur["unrealized_pnl"] += float(unrealized_pnl or 0.0)
            cur["total_pnl"] += float(total_pnl or 0.0)
            agg[key] = cur

        for (model_name, reg), m in agg.items():
            realized_pnl = float(m.get("realized_pnl") or 0.0)
            unrealized_pnl = float(m.get("unrealized_pnl") or 0.0)
            total_pnl = float(m.get("total_pnl") or 0.0)
            drawdown_proxy = float(m.get("drawdown_proxy") or 0.0)
            score = float(realized_pnl) + float(unrealized_pnl) - float(W_DD) * float(drawdown_proxy)
            components = {
                "realized_pnl": float(realized_pnl),
                "unrealized_pnl": float(unrealized_pnl),
                "total_pnl": float(total_pnl),
                "drawdown_proxy": float(drawdown_proxy),
            }
            try:
                con.execute(
                    """
                    INSERT INTO shadow_capital_scores
                      (ts_ms, window_s, regime, model_name, model_kind, model_ts_ms,
                       n, rmse, dir_acc, net_rmse,
                       slippage_bps_mean, slippage_bps_std, execution_latency_ms_mean, execution_latency_ms_std, drawdown_proxy,
                       cap_eff, realized_pnl, unrealized_pnl, total_pnl, score, weights_json, components_json)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(model_name, window_s, regime) DO UPDATE SET
                      ts_ms=excluded.ts_ms,
                      n=excluded.n,
                      rmse=excluded.rmse,
                      dir_acc=excluded.dir_acc,
                      net_rmse=excluded.net_rmse,
                      slippage_bps_mean=excluded.slippage_bps_mean,
                      slippage_bps_std=excluded.slippage_bps_std,
                      execution_latency_ms_mean=excluded.execution_latency_ms_mean,
                      execution_latency_ms_std=excluded.execution_latency_ms_std,
                      drawdown_proxy=excluded.drawdown_proxy,
                      cap_eff=excluded.cap_eff,
                      realized_pnl=excluded.realized_pnl,
                      unrealized_pnl=excluded.unrealized_pnl,
                      total_pnl=excluded.total_pnl,
                      score=excluded.score,
                      weights_json=excluded.weights_json,
                      components_json=excluded.components_json
                    """,
                    (
                        int(now_ms),
                        int(window_s),
                        str(reg),
                        str(model_name),
                        None,
                        None,
                        int(m.get("n") or 0),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        float(drawdown_proxy),
                        None,
                        float(realized_pnl),
                        float(unrealized_pnl),
                        float(total_pnl),
                        float(score),
                        json.dumps(weights, separators=(",", ":"), sort_keys=True),
                        json.dumps(components, separators=(",", ":"), sort_keys=True),
                    ),
                )
                upserts += 1
            except Exception as exc:
                _warn_nonfatal(
                    "shadow_capital_allocator_score_upsert_failed",
                    "SHADOW_CAPITAL_ALLOCATOR_SCORE_UPSERT_FAILED",
                    exc,
                    warn_key=f"shadow_capital_allocator_score_upsert:{name}",
                    model_name=str(name),
                    regime=str(regime),
                )
                skipped += 1
                continue

        try:
            con.commit()
        except Exception as exc:
            _warn_nonfatal(
                "shadow_capital_allocator_commit_failed",
                "SHADOW_CAPITAL_ALLOCATOR_COMMIT_FAILED",
                exc,
                regime=str(regime),
            )

        return {
            "ok": True,
            "ts_ms": int(now_ms),
            "window_s": int(window_s),
            "regime": str(regime),
            "upserts": int(upserts),
            "skipped": int(skipped),
            "weights": weights,
        }
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "shadow_capital_allocator_close_failed",
                "SHADOW_CAPITAL_ALLOCATOR_CLOSE_FAILED",
                exc,
                warn_key="shadow_capital_allocator_close_failed",
            )


def get_shadow_capital_scores(limit: int = 50, regime: str = DEFAULT_REGIME) -> Dict[str, Any]:
    limit = max(1, min(500, int(limit or 50)))
    con = _db_connect()
    try:
        if not _table_exists(con, "shadow_capital_scores"):
            return {"ok": True, "rows": []}

        try:
            rows = con.execute(
                """
                SELECT ts_ms, window_s, regime, model_name, n,
                       realized_pnl, unrealized_pnl, total_pnl, drawdown_proxy,
                       score, components_json
                FROM shadow_capital_scores
                WHERE regime=?
                ORDER BY score DESC
                LIMIT ?
                """,
                (str(regime), int(limit)),
            ).fetchall()
        except Exception:
            rows = []

        out = []
        for r in rows or []:
            try:
                out.append(
                    {
                        "ts_ms": int(r[0] or 0),
                        "window_s": int(r[1] or 0),
                        "regime": str(r[2] or "global"),
                        "model_name": str(r[3] or ""),
                        "n": int(r[4] or 0),
                        "realized_pnl": _safe_float(r[5], None),
                        "unrealized_pnl": _safe_float(r[6], None),
                        "total_pnl": _safe_float(r[7], None),
                        "drawdown_proxy": _safe_float(r[8], None),
                        "score": _safe_float(r[9], None),
                        "components": _try_json_load(r[10]),
                    }
                )
            except Exception as exc:
                _warn_nonfatal(
                    "shadow_capital_allocator_scores_row_parse_failed",
                    "SHADOW_CAPITAL_ALLOCATOR_SCORES_ROW_PARSE_FAILED",
                    exc,
                    warn_key=f"shadow_capital_allocator_scores_row:{r}",
                    raw_row=str(r),
                )
                continue

        return {"ok": True, "rows": out}
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "shadow_capital_allocator_scores_close_failed",
                "SHADOW_CAPITAL_ALLOCATOR_SCORES_CLOSE_FAILED",
                exc,
                warn_key="shadow_capital_allocator_scores_close_failed",
            )
