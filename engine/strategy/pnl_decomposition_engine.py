"""
FILE: pnl_decomposition_engine.py

Breaks realized PnL into signal, sizing, and execution-related components. This
is a forensic/analytics layer used to explain where portfolio outcomes came
from after the fact.
"""

import json
import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.runtime.metrics import emit_counter, emit_gauge
from engine.runtime.tracing import trace_event

LOG = get_logger("engine.strategy.pnl_decomposition_engine")
_WARNED_NONFATAL_KEYS: set[str] = set()
PNL_DECOMP_RECONSTRUCTION_EPSILON = float(os.environ.get("PNL_DECOMP_RECONSTRUCTION_EPSILON", "1e-6"))
PNL_DECOMP_RESIDUAL_DOMINANCE_RATIO = float(os.environ.get("PNL_DECOMP_RESIDUAL_DOMINANCE_RATIO", "0.5"))
PNL_DECOMP_MIN_ABS_PNL_FLOOR = float(os.environ.get("PNL_DECOMP_MIN_ABS_PNL_FLOOR", "1.0"))


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
        component="engine.strategy.pnl_decomposition_engine",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _safe_json_loads(s: Optional[str]) -> Any:
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception as e:
        _warn_nonfatal(
            "pnl_decomposition_json_parse_failed",
            "PNL_DECOMPOSITION_JSON_PARSE_FAILED",
            e,
            warn_key="json_parse",
            value=str(s)[:200],
        )
        return None


def _ensure_tables(con) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS pnl_decomposition (
          ts_ms INTEGER NOT NULL,
          source_alert_id INTEGER NOT NULL,
          symbol TEXT NOT NULL,

          -- signal/model inputs (best-effort)
          expected_z REAL,
          confidence REAL,
          volatility REAL,
          horizon_s INTEGER,

          -- sizing / exposure
          equity REAL,
          to_weight REAL,
          base_weight_est REAL,
          final_mult REAL,
          notional_est REAL,

          -- realized
          realized_pnl REAL NOT NULL,
          fees REAL NOT NULL,
          slippage_bps REAL,

          -- components (in $)
          exec_cost_pnl REAL,
          alpha_expected_pnl REAL,
          sizing_pnl REAL,
          residual_pnl REAL,
          reconstruction_error REAL NOT NULL DEFAULT 0.0,
          residual_share REAL,
          quality_status TEXT NOT NULL DEFAULT 'ok',

          -- provenance
          meta_json TEXT,

          PRIMARY KEY (ts_ms, source_alert_id, symbol)
        );

        CREATE INDEX IF NOT EXISTS idx_pnl_decomp_ts
          ON pnl_decomposition(ts_ms);

        CREATE INDEX IF NOT EXISTS idx_pnl_decomp_alert
          ON pnl_decomposition(source_alert_id);

        CREATE INDEX IF NOT EXISTS idx_pnl_decomp_symbol_ts
          ON pnl_decomposition(symbol, ts_ms);
        """
    )
    try:
        cols = {
            str(row[1]).strip().lower()
            for row in (con.execute("PRAGMA table_info(pnl_decomposition)").fetchall() or [])
        }
    except Exception as e:
        _warn_nonfatal(
            "pnl_decomposition_schema_introspection_failed",
            "PNL_DECOMPOSITION_SCHEMA_INTROSPECTION_FAILED",
            e,
            warn_key="pnl_decomposition_schema_introspection_failed",
        )
        cols = set()
    for name, ddl in (
        ("reconstruction_error", "REAL NOT NULL DEFAULT 0.0"),
        ("residual_share", "REAL"),
        ("quality_status", "TEXT NOT NULL DEFAULT 'ok'"),
    ):
        if name in cols:
            continue
        try:
            con.execute(f"ALTER TABLE pnl_decomposition ADD COLUMN {name} {ddl}")
        except Exception as e:
            _warn_nonfatal(
                "pnl_decomposition_schema_migration_failed",
                "PNL_DECOMPOSITION_SCHEMA_MIGRATION_FAILED",
                e,
                warn_key=f"pnl_decomposition_schema_migration_failed:{name}",
                column=str(name),
            )


def _latest_equity(con, ts_ms: int) -> float:
    try:
        r = con.execute(
            """
            SELECT equity
            FROM equity_history
            WHERE ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (int(ts_ms),),
        ).fetchone()
        if not r:
            return 0.0
        return float(r[0] or 0.0)
    except Exception as e:
        _warn_nonfatal(
            "pnl_decomposition_alert_signal_meta_failed",
            "PNL_DECOMPOSITION_ALERT_SIGNAL_META_FAILED",
            e,
            warn_key=f"latest_equity:{int(ts_ms)}",
            ts_ms=int(ts_ms),
        )
        return 0.0


def _alert_signal_meta(con, alert_id: int) -> Tuple[float, float, float, int]:
    """
    Returns: expected_z, confidence, volatility, horizon_s (best-effort)
    """
    expected_z = 0.0
    confidence = 0.0
    volatility = 0.0
    horizon_s = 0

    try:
        r = con.execute(
            """
            SELECT expected_z, confidence, horizon_s, explain_json
            FROM alerts
            WHERE id=?
            """,
            (int(alert_id),),
        ).fetchone()
        if not r:
            return expected_z, confidence, volatility, horizon_s

        expected_z = float(r[0] or 0.0)
        confidence = float(r[1] or 0.0)
        horizon_s = int(r[2] or 0)

        ex = _safe_json_loads(r[3]) if r[3] else None
        if isinstance(ex, dict):
            for k in ("volatility", "vol", "sigma", "realized_vol"):
                if ex.get(k) is not None:
                    try:
                        volatility = float(ex.get(k) or 0.0)
                        break
                    except Exception as exc:
                        _warn_nonfatal(
                            "pnl_decomposition_signal_volatility_parse_failed",
                            "PNL_DECOMPOSITION_SIGNAL_VOLATILITY_PARSE_FAILED",
                            exc,
                            warn_key="pnl_decomposition_signal_volatility_parse_failed",
                            alert_id=int(alert_id),
                            field=str(k),
                        )
    except Exception as exc:
        _warn_nonfatal(
            "pnl_decomposition_alert_signal_meta_failed",
            "PNL_DECOMPOSITION_ALERT_SIGNAL_META_FAILED",
            exc,
            warn_key="pnl_decomposition_alert_signal_meta_failed",
            alert_id=int(alert_id),
        )

    return float(expected_z), float(confidence), float(volatility), int(horizon_s)


def _alert_market_regime_meta(con, alert_id: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    try:
        r = con.execute(
            """
            SELECT explain_json
            FROM alerts
            WHERE id=?
            """,
            (int(alert_id),),
        ).fetchone()
        if not r:
            return out
        ex = _safe_json_loads(r[0]) if r[0] else None
        if not isinstance(ex, dict):
            return out
        market_regime = ex.get("market_regime")
        if market_regime is None:
            market_regime = ex.get("market_regime_label")
        if market_regime is not None:
            out["market_regime"] = str(market_regime)
        snap = ex.get("market_regime_snapshot")
        if isinstance(snap, dict):
            out["market_regime_snapshot"] = {
                "label": str(snap.get("label") or out.get("market_regime") or "mean_reversion"),
                "volatility": float(snap.get("volatility", 0.0) or 0.0),
                "volatility_baseline": float(snap.get("volatility_baseline", 0.0) or 0.0),
                "trend": float(snap.get("trend", 0.0) or 0.0),
                "trend_strength": float(snap.get("trend_strength", 0.0) or 0.0),
            }
    except Exception as e:
        _warn_nonfatal(
            "pnl_decomposition_regime_snapshot_failed",
            "PNL_DECOMPOSITION_REGIME_SNAPSHOT_FAILED",
            e,
            warn_key="regime_snapshot",
        )
        return {}
    return out


def _order_exposure_meta(con, ts_ms: int, alert_id: int, symbol: str) -> Dict[str, Any]:
    """
    Aggregate exposure from execution_orders for this (alert_id, symbol).
    Uses:
      - execution_orders.qty/ref_px for notional estimate
      - execution_orders.extra_json for to_weight + exec_regime.final_mult (if present)
    """
    sym = str(symbol or "").strip().upper()
    out: Dict[str, Any] = {
        "notional_est": 0.0,
        "to_weight": None,
        "final_mult": None,
        "base_weight_est": None,
        "n_orders": 0,
    }

    rows = []
    try:
        rows = con.execute(
            """
            SELECT qty, ref_px, extra_json, submit_ts_ms
            FROM execution_orders
            WHERE source_alert_id=? AND symbol=?
            ORDER BY submit_ts_ms DESC
            LIMIT 50
            """,
            (int(alert_id), sym),
        ).fetchall()
    except Exception as exc:
        _warn_nonfatal(
            "pnl_decomposition_order_exposure_meta_read_failed",
            "PNL_DECOMPOSITION_ORDER_EXPOSURE_META_READ_FAILED",
            exc,
            warn_key="pnl_decomposition_order_exposure_meta_read_failed",
            alert_id=int(alert_id),
            symbol=str(sym),
        )
        rows = []

    # This is intentionally best-effort because older orders may not carry the
    # full sizing metadata now available in newer runs.
    notional = 0.0
    last_extra = None
    for qty, ref_px, extra_json, _ in rows or []:
        q = float(qty or 0.0)
        px = float(ref_px or 0.0)
        if px > 0.0:
            notional += abs(q * px)
        if last_extra is None and extra_json:
            last_extra = extra_json

    out["notional_est"] = float(notional)
    out["n_orders"] = int(len(rows or []))

    # Pull sizing meta from last_extra (best-effort)
    exo = _safe_json_loads(last_extra) if isinstance(last_extra, str) else None
    if isinstance(exo, dict):
        if exo.get("to_weight") is not None:
            try:
                out["to_weight"] = float(exo.get("to_weight") or 0.0)
            except Exception as exc:
                _warn_nonfatal(
                    "pnl_decomposition_to_weight_parse_failed",
                    "PNL_DECOMPOSITION_TO_WEIGHT_PARSE_FAILED",
                    exc,
                    warn_key="pnl_decomposition_to_weight_parse_failed",
                    alert_id=int(alert_id),
                    symbol=str(sym),
                )

        # portfolio_execution_intents attaches exec_regime{final_mult,...}
        reg = exo.get("exec_regime")
        if isinstance(reg, dict) and reg.get("final_mult") is not None:
            try:
                out["final_mult"] = float(reg.get("final_mult") or 0.0)
            except Exception as exc:
                _warn_nonfatal(
                    "pnl_decomposition_final_mult_parse_failed",
                    "PNL_DECOMPOSITION_FINAL_MULT_PARSE_FAILED",
                    exc,
                    warn_key="pnl_decomposition_final_mult_parse_failed",
                    alert_id=int(alert_id),
                    symbol=str(sym),
                )

        # If final_mult exists, infer pre-stress base_weight_est
        try:
            tw = out.get("to_weight")
            fm = out.get("final_mult")
            if tw is not None and fm is not None and float(fm) > 1e-12:
                out["base_weight_est"] = float(tw) / float(fm)
        except Exception as exc:
            _warn_nonfatal(
                "pnl_decomposition_base_weight_estimate_failed",
                "PNL_DECOMPOSITION_BASE_WEIGHT_ESTIMATE_FAILED",
                exc,
                warn_key="pnl_decomposition_base_weight_estimate_failed",
                alert_id=int(alert_id),
                symbol=str(sym),
            )

    return out


def compute_pnl_decomposition_snapshot() -> Dict[str, Any]:
    """
    Decompose realized PnL into:
      - exec_cost_pnl ($): fees + slippage_bps * notional
      - alpha_expected_pnl ($): base_weight_est * equity * (expected_z * volatility)
      - sizing_pnl ($): (to_weight - base_weight_est) * equity * (expected_z * volatility)
      - residual_pnl ($): realized - (alpha_expected + sizing - exec_cost)
    Writes into pnl_decomposition for latest pnl_attribution snapshot ts_ms.
    """
    con = connect(readonly=False)
    try:
        _ensure_tables(con)

        r = con.execute("SELECT MAX(ts_ms) FROM pnl_attribution").fetchone()
        pts = int(r[0]) if r and r[0] is not None else None
        if pts is None:
            return {"ok": False, "status": "no_pnl_attribution"}

        rows = con.execute(
            """
            SELECT
              ts_ms,
              source_alert_id,
              symbol,
              pnl,
              fees,
              slippage_bps,
              position_size,
              avg_price,
              realized_pnl,
              unrealized_pnl,
              extra_json
            FROM pnl_attribution
            WHERE ts_ms=?
            """,
            (int(pts),),
        ).fetchall()

        n = 0
        for ts_ms, sid, sym, pnl, fees, sl_bps, position_size, avg_price, realized_pnl_db, unrealized_pnl_db, extra_json in rows or []:
            sid_i = int(sid)
            sym_u = str(sym or "").strip().upper()

            pnl_extra = _safe_json_loads(extra_json) if extra_json else None
            realized_pnl = float(realized_pnl_db or 0.0)
            unrealized_pnl = float(unrealized_pnl_db or 0.0)
            fees_f = float(fees or 0.0)
            sl_bps_f = float(sl_bps) if sl_bps is not None else None

            expected_z, confidence, volatility, horizon_s = _alert_signal_meta(con, sid_i)
            market_regime_meta = _alert_market_regime_meta(con, sid_i)
            equity = _latest_equity(con, int(ts_ms))

            exp = _order_exposure_meta(con, int(ts_ms), sid_i, sym_u)
            to_weight = exp.get("to_weight")
            final_mult = exp.get("final_mult")
            base_weight_est = exp.get("base_weight_est")
            notional_est = float(exp.get("notional_est") or 0.0)

            # cost in $
            exec_cost_pnl = fees_f
            if sl_bps_f is not None and notional_est > 0.0:
                exec_cost_pnl += (float(sl_bps_f) / 10000.0) * float(notional_est)

            # expected return proxy (z * vol)
            expected_ret = float(expected_z) * float(volatility)

            alpha_expected_pnl = None
            sizing_pnl = None

            if equity > 0.0 and base_weight_est is not None:
                alpha_expected_pnl = float(base_weight_est) * float(equity) * float(expected_ret)

                if to_weight is not None:
                    sizing_pnl = (float(to_weight) - float(base_weight_est)) * float(equity) * float(expected_ret)
                else:
                    sizing_pnl = 0.0

            # residual
            model_sum = 0.0
            if alpha_expected_pnl is not None:
                model_sum += float(alpha_expected_pnl)
            if sizing_pnl is not None:
                model_sum += float(sizing_pnl)

            residual = float(realized_pnl) - (model_sum - float(exec_cost_pnl))
            reconstruction_error = abs(
                (
                    float(alpha_expected_pnl or 0.0)
                    + float(sizing_pnl or 0.0)
                    + float(residual)
                    - float(exec_cost_pnl)
                )
                - float(realized_pnl)
            )
            residual_share = abs(float(residual)) / max(
                abs(float(realized_pnl)),
                float(PNL_DECOMP_MIN_ABS_PNL_FLOOR),
            )
            quality_status = (
                "warn"
                if (
                    float(residual_share) > float(PNL_DECOMP_RESIDUAL_DOMINANCE_RATIO)
                    or float(reconstruction_error) > float(PNL_DECOMP_RECONSTRUCTION_EPSILON)
                )
                else "ok"
            )

            meta = {
                "pnl_attribution_extra": pnl_extra,
                "position_size": (float(position_size) if position_size is not None else None),
                "avg_price": (float(avg_price) if avg_price is not None else None),
                "unrealized_pnl": float(unrealized_pnl),
                "exposure_meta": exp,
                "market_regime": market_regime_meta.get("market_regime"),
                "market_regime_snapshot": market_regime_meta.get("market_regime_snapshot"),
                "pnl_quality": {
                    "reconstruction_error": float(reconstruction_error),
                    "residual_share": float(residual_share),
                    "quality_status": str(quality_status),
                    "reconstruction_epsilon": float(PNL_DECOMP_RECONSTRUCTION_EPSILON),
                    "residual_dominance_ratio": float(PNL_DECOMP_RESIDUAL_DOMINANCE_RATIO),
                },
            }

            emit_counter(
                "pnl_update",
                1,
                component="engine.strategy.pnl_decomposition_engine",
                symbol=str(sym_u),
            )
            emit_gauge(
                "pnl_update",
                float(realized_pnl),
                component="engine.strategy.pnl_decomposition_engine",
                symbol=str(sym_u),
                extra_tags={"metric_scope": "realized_pnl"},
            )
            trace_event(
                "pnl_update",
                component="engine.strategy.pnl_decomposition_engine",
                entity_type="symbol",
                entity_id=str(sym_u),
                payload={
                    "source_alert_id": int(sid_i),
                    "realized_pnl": float(realized_pnl),
                    "exec_cost_pnl": float(exec_cost_pnl),
                    "alpha_expected_pnl": (float(alpha_expected_pnl) if alpha_expected_pnl is not None else None),
                    "sizing_pnl": (float(sizing_pnl) if sizing_pnl is not None else None),
                    "residual_pnl": float(residual),
                    "reconstruction_error": float(reconstruction_error),
                    "residual_share": float(residual_share),
                    "quality_status": str(quality_status),
                },
                symbol=str(sym_u),
                con=con,
            )
            if str(quality_status) == "warn":
                emit_counter(
                    "pnl_decomposition_quality_warn",
                    1,
                    component="engine.strategy.pnl_decomposition_engine",
                    symbol=str(sym_u),
                )
                emit_gauge(
                    "pnl_decomposition_quality_warn",
                    float(residual_share),
                    component="engine.strategy.pnl_decomposition_engine",
                    symbol=str(sym_u),
                    extra_tags={"metric_scope": "residual_share"},
                )
                trace_event(
                    "pnl_decomposition_quality_warn",
                    component="engine.strategy.pnl_decomposition_engine",
                    entity_type="symbol",
                    entity_id=str(sym_u),
                    payload={
                        "source_alert_id": int(sid_i),
                        "reconstruction_error": float(reconstruction_error),
                        "residual_share": float(residual_share),
                        "quality_status": str(quality_status),
                    },
                    symbol=str(sym_u),
                    con=con,
                )

            con.execute(
                """
                INSERT OR REPLACE INTO pnl_decomposition(
                  ts_ms, source_alert_id, symbol,
                  expected_z, confidence, volatility, horizon_s,
                  equity, to_weight, base_weight_est, final_mult, notional_est,
                  realized_pnl, fees, slippage_bps,
                  exec_cost_pnl, alpha_expected_pnl, sizing_pnl, residual_pnl,
                  reconstruction_error, residual_share, quality_status,
                  meta_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    int(ts_ms),
                    int(sid_i),
                    sym_u,
                    float(expected_z),
                    float(confidence),
                    float(volatility),
                    int(horizon_s),
                    float(equity),
                    (float(to_weight) if to_weight is not None else None),
                    (float(base_weight_est) if base_weight_est is not None else None),
                    (float(final_mult) if final_mult is not None else None),
                    float(notional_est),
                    float(realized_pnl),
                    float(fees_f),
                    (float(sl_bps_f) if sl_bps_f is not None else None),
                    float(exec_cost_pnl),
                    (float(alpha_expected_pnl) if alpha_expected_pnl is not None else None),
                    (float(sizing_pnl) if sizing_pnl is not None else None),
                    float(residual),
                    float(reconstruction_error),
                    float(residual_share),
                    str(quality_status),
                    json.dumps(meta, separators=(",", ":"), sort_keys=True),
                ),
            )
            n += 1

        con.commit()

        emit_gauge(
            "queue_depth",
            int(n),
            component="engine.strategy.pnl_decomposition_engine",
            extra_tags={"queue_name": "pnl_decomposition_rows"},
        )
        trace_event(
            "pnl_update",
            component="engine.strategy.pnl_decomposition_engine",
            entity_type="snapshot",
            entity_id=str(int(pts)),
            payload={"rows_written": int(n), "snapshot_ts_ms": int(pts)},
            con=con,
        )
        return {"ok": True, "snapshot_ts_ms": int(pts), "rows_written": int(n)}
    finally:
        con.close()
