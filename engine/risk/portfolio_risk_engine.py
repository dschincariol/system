"""Portfolio-level exposure, volatility, and drawdown risk controls.

The engine applies additive portfolio constraints to desired allocations,
persists risk snapshots for operator visibility, and raises fail-closed block
signals when projected exposure breaches configured limits.
"""

# engine/risk/portfolio_risk_engine.py
"""
Institutional Portfolio Risk Engine (additive, non-breaking).

Implements:
- Portfolio exposure accounting: gross/net, per-symbol, per-asset-class
- Rolling vol + correlation/cov proxy from prices (engine.strategy.risk)
- Correlated exposure clusters (graph components) capped by budget
- Vol-adjusted per-symbol sizing caps
- Asset-class risk budgets
- Portfolio-level gross/net caps
- Portfolio vol targeting (scale all weights) + hard-block threshold
- Max drawdown throttle + hard-block (engine.strategy.drawdown_state)

Integration:
- Called from engine.strategy.portfolio BEFORE portfolio_risk_gate
- Writes:
    risk_state:
      portfolio_risk_block (0/1)
      portfolio_risk_info  (json)
  and snapshots to portfolio_risk_snapshots (created in storage.init_db)
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.data.asset_map import asset_class_for_symbol
from engine.strategy.drawdown_state import get_current_drawdown
from engine.strategy.risk import realized_vol_from_prices, corr_from_prices
from engine.strategy.har_rv import resolve_vol_forecast
from engine.runtime.risk_state import set_state, get_state_row
from engine.runtime.event_log import record_risk_block
from engine.runtime.storage import _table_exists

LOG = logging.getLogger("engine.risk.portfolio_risk_engine")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.risk.portfolio_risk_engine",
        extra=extra or {},
        include_health=False,
        persist=False,
    )


# -----------------------------
# Enable / controls (env)
# -----------------------------
USE = os.environ.get("PORTFOLIO_USE_RISK_ENGINE", "1") == "1"

# Universe bound for cov/corr computations (top by abs weight)
MAX_SYMBOLS = int(os.environ.get("PORTFOLIO_RISK_MAX_SYMBOLS", "18"))

# Portfolio caps
MAX_GROSS = float(os.environ.get("PORTFOLIO_RISK_MAX_GROSS", os.environ.get("PORTFOLIO_GROSS_CAP", "1.00")))
MAX_NET = float(os.environ.get("PORTFOLIO_RISK_MAX_NET", "0.60"))

# Drawdown throttle/hard block
DD_THROTTLE_START = float(os.environ.get("PORTFOLIO_RISK_DD_THROTTLE_START", "0.06"))
DD_THROTTLE_MIN_SCALE = float(os.environ.get("PORTFOLIO_RISK_DD_THROTTLE_MIN_SCALE", "0.35"))
DD_HARD_BLOCK = float(os.environ.get("PORTFOLIO_RISK_DD_HARD_BLOCK", "0.15"))

# Portfolio vol proxy + targeting
VOL_LOOKBACK = int(os.environ.get("PORTFOLIO_RISK_VOL_LOOKBACK", os.environ.get("PORTFOLIO_VOL_LOOKBACK", "240")))
VOL_TARGET = float(os.environ.get("PORTFOLIO_RISK_VOL_TARGET", os.environ.get("PORTFOLIO_TARGET_VOL", "0.020")))
VOL_FORECAST_SOURCE = str(os.environ.get("VOL_FORECAST_SOURCE", "trailing") or "trailing").strip().lower()
PORTFOLIO_VOL_HARD_BLOCK = float(os.environ.get("PORTFOLIO_RISK_VOL_HARD_BLOCK", "0.0"))  # 0 disables
PORTFOLIO_VOL_FLOOR = float(os.environ.get("PORTFOLIO_RISK_VOL_FLOOR", "0.005"))
PORTFOLIO_VOL_CEIL = float(os.environ.get("PORTFOLIO_RISK_VOL_CEIL", "0.080"))
USE_GEX_VOL_MODIFIER = os.environ.get("PORTFOLIO_RISK_USE_GEX_VOL_MODIFIER", os.environ.get("USE_OPTIONS_FEATURES", "0")) == "1"
PORTFOLIO_RISK_USE_MONTE_CARLO = os.environ.get("PORTFOLIO_RISK_USE_MONTE_CARLO", "1") == "1"
PORTFOLIO_RISK_MC_MAX_AGE_S = int(os.environ.get("PORTFOLIO_RISK_MC_MAX_AGE_S", "900"))
PORTFOLIO_RISK_MC_VAR_95_BLOCK = float(os.environ.get("PORTFOLIO_RISK_MC_VAR_95_BLOCK", "0.0"))
PORTFOLIO_RISK_MC_VAR_99_BLOCK = float(os.environ.get("PORTFOLIO_RISK_MC_VAR_99_BLOCK", "0.0"))
PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK = float(os.environ.get("PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK", "0.0"))
PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK = float(os.environ.get("PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK", "0.0"))

# Per-symbol vol caps (vol-adjusted sizing caps)
USE_VOL_CAPS = os.environ.get("PORTFOLIO_RISK_USE_VOL_CAPS", "1") == "1"
SYMBOL_CAP_MAX_W = float(os.environ.get("PORTFOLIO_RISK_SYMBOL_CAP_MAX_W", "0.35"))
SYMBOL_CAP_MIN_MULT = float(os.environ.get("PORTFOLIO_RISK_SYMBOL_CAP_MIN_MULT", "0.20"))
MAX_SYMBOL_GROSS = float(os.environ.get("PORTFOLIO_RISK_MAX_SYMBOL_GROSS", os.environ.get("PORTFOLIO_RISK_SYMBOL_CAP_MAX_W", "0.35")))

# Correlated exposure cluster caps (graph components)
USE_CORR_CLUSTERS = os.environ.get("PORTFOLIO_RISK_USE_CORR_CLUSTERS", "1") == "1"
CORR_LOOKBACK = int(os.environ.get("PORTFOLIO_RISK_CORR_LOOKBACK", "240"))
CLUSTER_CORR_TH = float(os.environ.get("PORTFOLIO_RISK_CLUSTER_CORR_TH", "0.85"))
CLUSTER_MAX_GROSS = float(os.environ.get("PORTFOLIO_RISK_CLUSTER_MAX_GROSS", "0.45"))
CLUSTER_MAX_COMPONENTS = int(os.environ.get("PORTFOLIO_RISK_CLUSTER_MAX_COMPONENTS", "12"))

# Asset-class budgets
USE_ASSET_CLASS_BUDGETS = os.environ.get("PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS", "1") == "1"
_ASSET_CLASS_BUDGETS_JSON = os.environ.get("PORTFOLIO_RISK_ASSET_CLASS_BUDGETS_JSON", "").strip()

# Strategy-level budgets
USE_STRATEGY_BUDGETS = os.environ.get("PORTFOLIO_RISK_USE_STRATEGY_BUDGETS", "1") == "1"
STRATEGY_MAX_GROSS = float(os.environ.get("PORTFOLIO_RISK_MAX_STRATEGY_GROSS", "0.60"))
STRATEGY_MAX_NET = float(os.environ.get("PORTFOLIO_RISK_MAX_STRATEGY_NET", "0.40"))
USE_ALPHA_DECAY_THROTTLE = os.environ.get("PORTFOLIO_RISK_USE_ALPHA_DECAY_THROTTLE", "1") == "1"
ALPHA_DECAY_THROTTLE_FRESH_S = int(os.environ.get("PORTFOLIO_RISK_ALPHA_DECAY_FRESH_S", "21600"))

_DEFAULT_ASSET_CLASS_BUDGETS = {
    "EQUITY": 1.00,
    "CRYPTO": 0.35,
    "COMMODITY": 0.50,
    "FX": 0.50,
    "RATES": 0.60,
    "UNKNOWN": 0.40,
}

ASSET_CLASS_BUDGETS: Dict[str, float] = dict(_DEFAULT_ASSET_CLASS_BUDGETS)
if _ASSET_CLASS_BUDGETS_JSON:
    try:
        d = json.loads(_ASSET_CLASS_BUDGETS_JSON)
        if isinstance(d, dict):
            for k, v in d.items():
                ASSET_CLASS_BUDGETS[str(k).upper()] = float(v)
    except Exception:
        ASSET_CLASS_BUDGETS = dict(_DEFAULT_ASSET_CLASS_BUDGETS)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v:
            return float(default)
        return float(v)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float_failed",
            value_type=type(x).__name__,
        )
        fallback = float(default)
        return fallback


def _optional_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        if v != v:
            return None
        return float(v)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_OPTIONAL_FLOAT_FAILED",
            e,
            once_key=f"optional_float:{type(x).__name__}:{str(x)[:64]}",
            value_type=type(x).__name__,
        )
        return None


def _load_monte_carlo_risk_summary(now_ms: int) -> Dict[str, Any]:
    if not PORTFOLIO_RISK_USE_MONTE_CARLO:
        return {}

    try:
        raw, ts_ms = get_state_row("monte_carlo_risk_info", "")
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_MONTE_CARLO_STATE_READ_FAILED",
            e,
            once_key="monte_carlo_state_read",
        )
        monte_carlo_info: Dict[str, Any] = {}
        return monte_carlo_info

    if not raw:
        return {}

    try:
        info = json.loads(raw or "{}")
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_MONTE_CARLO_JSON_FAILED",
            e,
            once_key="monte_carlo_json_parse",
        )
        monte_carlo_info = {}
        return monte_carlo_info

    if not isinstance(info, dict):
        return {}

    age_ms = int(max(0, int(now_ms) - int(ts_ms or 0)))
    out: Dict[str, Any] = {
        "ready": bool(info.get("ready", False)),
        "status": str(info.get("status") or ""),
        "ts_ms": int(ts_ms or 0),
        "age_s": float(age_ms) / 1000.0,
        "var_95": float(_safe_float(info.get("var_95"), 0.0)),
        "var_99": float(_safe_float(info.get("var_99"), 0.0)),
        "worst_simulated_drawdown": float(_safe_float(info.get("worst_simulated_drawdown"), 0.0)),
        "drawdown_p95": float(_safe_float((info.get("drawdown_percentiles") or {}).get("p95"), 0.0)),
        "drawdown_p99": float(_safe_float((info.get("drawdown_percentiles") or {}).get("p99"), 0.0)),
    }

    # Monte Carlo results are advisory only while fresh. Stale simulations are
    # surfaced as stale rather than silently trusted for hard blocking decisions.
    if int(PORTFOLIO_RISK_MC_MAX_AGE_S) > 0 and float(out["age_s"]) > float(PORTFOLIO_RISK_MC_MAX_AGE_S):
        out["stale"] = True
        return out

    var_95_loss = max(0.0, -float(out["var_95"]))
    var_99_loss = max(0.0, -float(out["var_99"]))
    dd_p95 = max(0.0, float(out["drawdown_p95"]))
    dd_worst = max(0.0, float(out["worst_simulated_drawdown"]))

    blocked = False
    reasons: List[Dict[str, Any]] = []

    if float(PORTFOLIO_RISK_MC_VAR_95_BLOCK) > 0.0 and var_95_loss >= float(PORTFOLIO_RISK_MC_VAR_95_BLOCK):
        blocked = True
        reasons.append({"type": "monte_carlo_var_95_block", "value": float(var_95_loss), "threshold": float(PORTFOLIO_RISK_MC_VAR_95_BLOCK)})

    if float(PORTFOLIO_RISK_MC_VAR_99_BLOCK) > 0.0 and var_99_loss >= float(PORTFOLIO_RISK_MC_VAR_99_BLOCK):
        blocked = True
        reasons.append({"type": "monte_carlo_var_99_block", "value": float(var_99_loss), "threshold": float(PORTFOLIO_RISK_MC_VAR_99_BLOCK)})

    if float(PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK) > 0.0 and dd_p95 >= float(PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK):
        blocked = True
        reasons.append({"type": "monte_carlo_drawdown_p95_block", "value": float(dd_p95), "threshold": float(PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK)})

    if float(PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK) > 0.0 and dd_worst >= float(PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK):
        blocked = True
        reasons.append({"type": "monte_carlo_worst_drawdown_block", "value": float(dd_worst), "threshold": float(PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK)})

    out["blocked"] = bool(blocked)
    if reasons:
        out["reasons"] = reasons
    return out

def _load_latest_alpha_decay_strategy_metrics(con, now_ms: int) -> Dict[str, Dict[str, Any]]:
    if not USE_ALPHA_DECAY_THROTTLE:
        return {}
    if not _table_exists(con, "strategy_metrics"):
        return {}

    cutoff_ts_ms = int(now_ms) - (int(max(0, ALPHA_DECAY_THROTTLE_FRESH_S)) * 1000)

    try:
        rows = con.execute(
            """
            SELECT m.strategy_name, m.ts_ms, m.metrics_json
            FROM strategy_metrics m
            JOIN (
              SELECT strategy_name, MAX(ts_ms) AS ts_ms
              FROM strategy_metrics
              WHERE window_days=0
              GROUP BY strategy_name
            ) t
            ON t.strategy_name=m.strategy_name AND t.ts_ms=m.ts_ms
            WHERE m.window_days=0 AND m.ts_ms>=?
            """,
            (int(cutoff_ts_ms),),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_ALPHA_DECAY_METRICS_QUERY_FAILED",
            e,
            once_key="alpha_decay_metrics_query",
        )
        alpha_metrics: Dict[str, Dict[str, Any]] = {}
        return alpha_metrics

    out: Dict[str, Dict[str, Any]] = {}
    for strategy_name, ts_ms, metrics_json in rows or []:
        try:
            name = str(strategy_name or "").strip()
            if not name:
                continue
            mj = json.loads(metrics_json or "{}")
            if not isinstance(mj, dict):
                mj = {}
            out[name] = {
                "ts_ms": int(ts_ms or 0),
                "alpha_decay_throttle_mult": float(max(0.0, min(1.0, _safe_float(mj.get("alpha_decay_throttle_mult"), 1.0)))),
                "alpha_decay_severity": str(mj.get("alpha_decay_severity") or "ok").strip().lower(),
                "alpha_decay_severity_score": float(_safe_float(mj.get("alpha_decay_severity_score"), 0.0)),
                "alpha_decay_reasons": list(mj.get("alpha_decay_reasons") or []),
            }
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_RISK_ENGINE_ALPHA_DECAY_ROW_FAILED",
                e,
                strategy_name=str(strategy_name or ""),
            )
            continue
    return out


def _apply_alpha_decay_throttle(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any], now_ms: int) -> Dict[str, Dict[str, Any]]:
    out = dict(desired or {})
    latest = _load_latest_alpha_decay_strategy_metrics(con, now_ms=int(now_ms))
    if not latest:
        return out

    # This stage rescales desired weights; it does not rewrite strategy intent or
    # selection. It is a portfolio-level safety overlay, not a signal generator.
    hit: Dict[str, Any] = {}

    for strategy_name, rec in latest.items():
        mult = float(max(0.0, min(1.0, _safe_float((rec or {}).get("alpha_decay_throttle_mult"), 1.0))))
        if mult >= 0.999999:
            continue

        touched = False
        for sym, row in (out or {}).items():
            if _strategy_bucket_for_row(row) != str(strategy_name):
                continue
            sw = _signed_weight(row)
            out[sym]["weight"] = float(sw) * float(mult)
            out[sym].setdefault("reason", {})
            if isinstance(out[sym]["reason"], dict):
                out[sym]["reason"]["alpha_decay_throttle"] = {
                    "strategy": str(strategy_name),
                    "scale": float(mult),
                    "severity": str((rec or {}).get("alpha_decay_severity") or "ok"),
                    "severity_score": float(_safe_float((rec or {}).get("alpha_decay_severity_score"), 0.0)),
                    "reasons": list((rec or {}).get("alpha_decay_reasons") or []),
                    "metrics_ts_ms": int((rec or {}).get("ts_ms") or 0),
                }
            touched = True

        if touched:
            hit[str(strategy_name)] = {
                "scale": float(mult),
                "severity": str((rec or {}).get("alpha_decay_severity") or "ok"),
                "severity_score": float(_safe_float((rec or {}).get("alpha_decay_severity_score"), 0.0)),
                "metrics_ts_ms": int((rec or {}).get("ts_ms") or 0),
                "reasons": list((rec or {}).get("alpha_decay_reasons") or []),
            }

    if hit:
        info["alpha_decay_throttle"] = {
            "fresh_s": int(ALPHA_DECAY_THROTTLE_FRESH_S),
            "strategies": hit,
        }

    return out


def _side_sign(side: Any) -> float:
    s = str(side or "FLAT").upper()
    if s == "LONG":
        return 1.0
    if s == "SHORT":
        return -1.0
    return 0.0


def _signed_weight(row: Optional[Dict[str, Any]]) -> float:
    if not row:
        return 0.0
    w = _safe_float((row or {}).get("weight", 0.0), 0.0)
    sgn = _side_sign((row or {}).get("side", "FLAT"))
    # tolerate both conventions:
    # - weight is magnitude with side providing sign
    # - weight is already signed
    if w < 0.0:
        return float(w)
    return float(abs(w)) * float(sgn)


def _abs_weight(row: Optional[Dict[str, Any]]) -> float:
    return abs(_signed_weight(row))


def _gross(rows: Dict[str, Dict[str, Any]]) -> float:
    return float(sum(_abs_weight(v) for v in (rows or {}).values()))


def _net(rows: Dict[str, Dict[str, Any]]) -> float:
    return float(sum(_signed_weight(v) for v in (rows or {}).values()))


def _annotate(desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> None:
    for sym in list((desired or {}).keys()):
        try:
            desired[sym].setdefault("reason", {})
            if not isinstance(desired[sym]["reason"], dict):
                desired[sym]["reason"] = {"raw": desired[sym]["reason"]}
            desired[sym]["reason"]["portfolio_risk_engine"] = dict(info)
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_ANNOTATE_FAILED", e, once_key=f"annotate:{sym}", symbol=str(sym))


def _top_symbols_by_abs(desired: Dict[str, Dict[str, Any]], n: int) -> List[str]:
    items: List[Tuple[str, float]] = []
    for sym, row in (desired or {}).items():
        try:
            aw = _abs_weight(row)
            if aw > 0.0:
                items.append((str(sym), float(aw)))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_TOP_SYMBOLS_FAILED", e, once_key=f"top_symbols:{sym}", symbol=str(sym))
    items.sort(key=lambda t: t[1], reverse=True)
    if n > 0:
        items = items[: int(n)]
    return [s for s, _w in items]


def _maybe_parse_json(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
        return dict(parsed) if isinstance(parsed, dict) else {}
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_SAFE_JSON_FAILED",
            e,
            once_key="safe_json_dict",
            raw_type=type(raw).__name__,
        )
        parsed_default: Dict[str, Any] = {}
        return parsed_default


def _strategy_bucket_for_row(row: Optional[Dict[str, Any]]) -> str:
    r = row or {}
    for key in ("strategy_id", "strategy_name", "strategy"):
        val = r.get(key)
        if val not in (None, "", "null"):
            return str(val)

    reason_obj = r.get("reason")
    if isinstance(reason_obj, dict):
        for key in ("strategy_id", "strategy_name", "strategy", "strategy_key"):
            val = reason_obj.get(key)
            if val not in (None, "", "null"):
                return str(val)

    src_rule_id = r.get("source_rule_id")
    if src_rule_id not in (None, "", "null"):
        return f"rule:{src_rule_id}"

    src_alert_id = r.get("source_alert_id")
    if src_alert_id not in (None, "", "null"):
        return f"alert:{src_alert_id}"

    meta = _maybe_parse_json(r.get("explain_json"))
    for key in ("strategy_id", "strategy_name", "strategy", "model_name", "model", "strategy_key"):
        val = meta.get(key)
        if val not in (None, "", "null"):
            return str(val)

    reason = meta.get("reason")
    if isinstance(reason, dict):
        for key in ("strategy_id", "strategy_name", "strategy", "model_name", "model", "strategy_key"):
            val = reason.get(key)
            if val not in (None, "", "null"):
                return str(val)

    for key in ("model_name", "model"):
        val = r.get(key)
        if val not in (None, "", "null"):
            return str(val)

    return "UNKNOWN"


def _model_bucket_for_row(row: Optional[Dict[str, Any]]) -> str:
    r = row or {}
    for key in ("model_id", "model_name", "model"):
        val = r.get(key)
        if val not in (None, "", "null"):
            return str(val)

    meta = _maybe_parse_json(r.get("explain_json"))
    for key in ("model_id", "model_name", "model"):
        val = meta.get(key)
        if val not in (None, "", "null"):
            return str(val)

    reason = meta.get("reason")
    if isinstance(reason, dict):
        competition = reason.get("competition")
        if isinstance(competition, dict):
            val = competition.get("model_name")
            if val not in (None, "", "null"):
                return str(val)

    return "UNKNOWN"


def _exposure_snapshot(rows: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    by_symbol: Dict[str, Dict[str, Any]] = {}
    by_asset_class: Dict[str, Dict[str, float]] = {}
    by_strategy: Dict[str, Dict[str, float]] = {}
    by_model: Dict[str, Dict[str, float]] = {}

    long_gross = 0.0
    short_gross = 0.0

    for sym, row in (rows or {}).items():
        s = str(sym)
        sw = _signed_weight(row)
        aw = abs(sw)
        if aw <= 0.0:
            continue

        side = "LONG" if sw > 0.0 else ("SHORT" if sw < 0.0 else "FLAT")
        by_symbol[s] = {
            "signed": float(sw),
            "gross": float(aw),
            "side": side,
        }

        if sw > 0.0:
            long_gross += float(aw)
        elif sw < 0.0:
            short_gross += float(aw)

        try:
            asset_class = str(asset_class_for_symbol(s) or "UNKNOWN").upper()
        except Exception:
            asset_class = "UNKNOWN"

        ac = by_asset_class.setdefault(asset_class, {"gross": 0.0, "net": 0.0})
        ac["gross"] = float(ac.get("gross", 0.0) + aw)
        ac["net"] = float(ac.get("net", 0.0) + sw)

        strategy = _strategy_bucket_for_row(row)
        st = by_strategy.setdefault(strategy, {"gross": 0.0, "net": 0.0})
        st["gross"] = float(st.get("gross", 0.0) + aw)
        st["net"] = float(st.get("net", 0.0) + sw)

        model_bucket = _model_bucket_for_row(row)
        md = by_model.setdefault(model_bucket, {"gross": 0.0, "net": 0.0})
        md["gross"] = float(md.get("gross", 0.0) + aw)
        md["net"] = float(md.get("net", 0.0) + sw)

    return {
        "gross": float(_gross(rows or {})),
        "net": float(_net(rows or {})),
        "long_gross": float(long_gross),
        "short_gross": float(short_gross),
        "by_symbol": by_symbol,
        "by_asset_class": dict(sorted(by_asset_class.items(), key=lambda kv: kv[0])),
        "by_strategy": dict(sorted(by_strategy.items(), key=lambda kv: kv[0])),
        "by_model": dict(sorted(by_model.items(), key=lambda kv: kv[0])),
    }


def _delta_snapshot(
    current_rows: Dict[str, Dict[str, Any]],
    target_rows: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    syms = sorted(set((current_rows or {}).keys()) | set((target_rows or {}).keys()))
    by_symbol: Dict[str, Dict[str, float]] = {}
    gross_add = 0.0
    gross_reduce = 0.0
    net_delta = 0.0

    for sym in syms:
        cur = _signed_weight((current_rows or {}).get(sym))
        tgt = _signed_weight((target_rows or {}).get(sym))
        delta = float(tgt - cur)
        if abs(delta) <= 1e-12:
            continue
        by_symbol[str(sym)] = {
            "current": float(cur),
            "target": float(tgt),
            "delta": float(delta),
        }
        net_delta += float(delta)
        if abs(tgt) > abs(cur):
            gross_add += float(abs(tgt) - abs(cur))
        elif abs(cur) > abs(tgt):
            gross_reduce += float(abs(cur) - abs(tgt))

    return {
        "gross_add": float(gross_add),
        "gross_reduce": float(gross_reduce),
        "net_delta": float(net_delta),
        "by_symbol": by_symbol,
    }


def _reconciliation_summary(pre_snapshot: Dict[str, Any], post_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    def _bucket_delta(pre_map: Dict[str, Any], post_map: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        keys = sorted(set((pre_map or {}).keys()) | set((post_map or {}).keys()))
        for key in keys:
            pre_gross = _safe_float(((pre_map or {}).get(key) or {}).get("gross"), 0.0)
            pre_net = _safe_float(((pre_map or {}).get(key) or {}).get("net"), 0.0)
            post_gross = _safe_float(((post_map or {}).get(key) or {}).get("gross"), 0.0)
            post_net = _safe_float(((post_map or {}).get(key) or {}).get("net"), 0.0)
            if abs(pre_gross - post_gross) <= 1e-12 and abs(pre_net - post_net) <= 1e-12:
                continue
            out[str(key)] = {
                "gross_pre": float(pre_gross),
                "gross_post": float(post_gross),
                "gross_delta": float(post_gross - pre_gross),
                "net_pre": float(pre_net),
                "net_post": float(post_net),
                "net_delta": float(post_net - pre_net),
            }
        return out

    return {
        "gross_pre": float(_safe_float(pre_snapshot.get("gross"), 0.0)),
        "gross_post": float(_safe_float(post_snapshot.get("gross"), 0.0)),
        "gross_delta": float(_safe_float(post_snapshot.get("gross"), 0.0) - _safe_float(pre_snapshot.get("gross"), 0.0)),
        "net_pre": float(_safe_float(pre_snapshot.get("net"), 0.0)),
        "net_post": float(_safe_float(post_snapshot.get("net"), 0.0)),
        "net_delta": float(_safe_float(post_snapshot.get("net"), 0.0) - _safe_float(pre_snapshot.get("net"), 0.0)),
        "by_strategy": _bucket_delta(
            dict(pre_snapshot.get("by_strategy") or {}),
            dict(post_snapshot.get("by_strategy") or {}),
        ),
        "by_model": _bucket_delta(
            dict(pre_snapshot.get("by_model") or {}),
            dict(post_snapshot.get("by_model") or {}),
        ),
    }


def _last_price(con, symbol: str) -> Optional[float]:
    try:
        row = con.execute(
            "SELECT px FROM prices WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1",
            (str(symbol),),
        ).fetchone()
        if row and row[0] is not None:
            px = float(row[0])
            if px > 0.0:
                return float(px)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_LAST_PRICE_LOOKUP_FAILED", e, once_key=f"last_price_px:{symbol}", symbol=str(symbol), column="px")

    try:
        row = con.execute(
            "SELECT price FROM prices WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1",
            (str(symbol),),
        ).fetchone()
        if row and row[0] is not None:
            px = float(row[0])
            if px > 0.0:
                return float(px)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_LAST_PRICE_LOOKUP_FAILED", e, once_key=f"last_price_price:{symbol}", symbol=str(symbol), column="price")

    return None


def _equity_reference(con) -> Tuple[float, str]:
    if _table_exists(con, "broker_account"):
        try:
            row = con.execute("SELECT equity FROM broker_account WHERE id=1").fetchone()
            if row and row[0] is not None:
                eq = float(row[0])
                if eq > 0.0:
                    return float(eq), "broker_account"
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_EQUITY_REFERENCE_LOOKUP_FAILED", e, once_key="equity_reference:broker_account_id", source="broker_account", query="id=1")

        try:
            row = con.execute("SELECT equity FROM broker_account ORDER BY updated_ts_ms DESC LIMIT 1").fetchone()
            if row and row[0] is not None:
                eq = float(row[0])
                if eq > 0.0:
                    return float(eq), "broker_account"
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_EQUITY_REFERENCE_LOOKUP_FAILED", e, once_key="equity_reference:broker_account_latest", source="broker_account", query="latest")

    if _table_exists(con, "equity_history"):
        try:
            row = con.execute("SELECT equity FROM equity_history ORDER BY ts_ms DESC LIMIT 1").fetchone()
            if row and row[0] is not None:
                eq = float(row[0])
                if eq > 0.0:
                    return float(eq), "equity_history"
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_EQUITY_REFERENCE_LOOKUP_FAILED", e, once_key="equity_reference:equity_history_latest", source="equity_history", query="latest")

    return 0.0, "unknown"


def _row_with_signed_weight(base_row: Optional[Dict[str, Any]], signed_weight: float, source: str) -> Dict[str, Any]:
    row = dict(base_row or {})
    s = float(signed_weight)
    row["weight"] = float(abs(s))
    row["side"] = ("LONG" if s > 0.0 else ("SHORT" if s < 0.0 else "FLAT"))
    row.setdefault("reason", {})
    if not isinstance(row.get("reason"), dict):
        row["reason"] = {"raw": row.get("reason")}
    row["reason"].setdefault("portfolio_risk_engine_source", str(source))
    return row


def _load_live_positions(con) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    info: Dict[str, Any] = {"source": "none", "equity_ref": None, "equity_ref_source": "unknown"}
    out: Dict[str, Dict[str, Any]] = {}

    eq_ref, eq_ref_source = _equity_reference(con)
    info["equity_ref"] = (float(eq_ref) if eq_ref > 0.0 else None)
    info["equity_ref_source"] = str(eq_ref_source)

    if _table_exists(con, "broker_positions"):
        try:
            rows = con.execute(
                """
                SELECT symbol, qty, avg_px, updated_ts_ms
                FROM broker_positions
                """
            ).fetchall()
        except Exception:
            rows = []

        notionals: Dict[str, float] = {}
        gross_notional = 0.0

        for symbol, qty, avg_px, _updated_ts_ms in rows or []:
            try:
                q = float(qty or 0.0)
            except Exception:
                q = 0.0
            if abs(q) <= 1e-12:
                continue

            px = _last_price(con, str(symbol))
            if px is None or px <= 0.0:
                try:
                    px = float(avg_px or 0.0)
                except Exception:
                    px = 0.0
            if px <= 0.0:
                continue

            signed_notional = float(q) * float(px)
            notionals[str(symbol)] = float(signed_notional)
            gross_notional += abs(float(signed_notional))

        denom = float(eq_ref) if float(eq_ref) > 0.0 else float(gross_notional)
        if denom > 1e-12 and notionals:
            for sym, signed_notional in notionals.items():
                signed_weight = float(signed_notional) / float(denom)
                out[str(sym)] = {
                    "symbol": str(sym),
                    "weight": float(abs(signed_weight)),
                    "side": ("LONG" if signed_weight > 0.0 else ("SHORT" if signed_weight < 0.0 else "FLAT")),
                    "reason": {
                        "portfolio_risk_engine_live": {
                            "source": "broker_positions",
                            "equity_ref": float(denom),
                            "equity_ref_source": str(eq_ref_source),
                            "signed_notional": float(signed_notional),
                        }
                    },
                }

            info["source"] = "broker_positions"
            info["positions"] = int(len(out))
            info["gross_notional"] = float(gross_notional)
            return out, info

        if rows:
            info["source"] = "broker_positions_unpriced"
        else:
            info["source"] = "broker_positions_empty"

    if not _table_exists(con, "portfolio_state"):
        return out, info

    try:
        rows = con.execute(
            """
            SELECT
              symbol,
              SUM(CASE
                    WHEN UPPER(COALESCE(side, '')) IN ('LONG','BUY') THEN ABS(COALESCE(weight, 0.0))
                    WHEN UPPER(COALESCE(side, '')) IN ('SHORT','SELL') THEN -ABS(COALESCE(weight, 0.0))
                    ELSE COALESCE(weight, 0.0)
                  END) AS signed_weight,
              MAX(updated_ts_ms) AS updated_ts_ms,
              '{}' AS explain_json
            FROM portfolio_state
            GROUP BY symbol
            """
        ).fetchall()
    except Exception:
        rows = []

    if not rows:
        if info.get("source") == "none":
            info["source"] = "portfolio_state_empty"
        return out, info

    gross = 0.0
    state_rows: Dict[str, Dict[str, Any]] = {}

    for symbol, weight, _updated_ts_ms, explain_json in rows or []:
        try:
            sw = float(weight or 0.0)
        except Exception:
            sw = 0.0

        if abs(sw) <= 1e-12:
            continue

        reason = {}
        try:
            parsed = json.loads(explain_json or "{}")
            if isinstance(parsed, dict):
                reason = parsed
        except Exception:
            reason = {}

        state_rows[str(symbol)] = {
            "symbol": str(symbol),
            "weight": float(abs(sw)),
            "side": ("LONG" if sw > 0.0 else ("SHORT" if sw < 0.0 else "FLAT")),
            "reason": {
                "portfolio_risk_engine_live": {
                    "source": "portfolio_state",
                    "equity_ref": (float(eq_ref) if float(eq_ref) > 0.0 else None),
                    "equity_ref_source": str(eq_ref_source),
                },
                "portfolio_state_explain": reason,
            },
        }
        gross += abs(float(sw))

    if not state_rows:
        if info.get("source") == "none":
            info["source"] = "portfolio_state_empty"
        return out, info

    info["source"] = "portfolio_state"
    info["positions"] = int(len(state_rows))
    info["gross_notional"] = float(gross)
    return state_rows, info


def _project_live_plus_orders(
    live_rows: Dict[str, Dict[str, Any]],
    current_rows: Dict[str, Dict[str, Any]],
    target_rows: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    syms = sorted(set((live_rows or {}).keys()) | set((current_rows or {}).keys()) | set((target_rows or {}).keys()))
    out: Dict[str, Dict[str, Any]] = {}

    for sym in syms:
        live_sw = _signed_weight((live_rows or {}).get(sym))
        cur_sw = _signed_weight((current_rows or {}).get(sym))
        tgt_sw = _signed_weight((target_rows or {}).get(sym))
        projected_sw = float(live_sw + (tgt_sw - cur_sw))

        if abs(projected_sw) <= 1e-12:
            continue

        base_row = (target_rows or {}).get(sym) or (current_rows or {}).get(sym) or (live_rows or {}).get(sym) or {"symbol": str(sym)}
        out[str(sym)] = _row_with_signed_weight(base_row, projected_sw, "live_plus_orders")

    return out


def _projected_to_desired_targets(
    projected_rows: Dict[str, Dict[str, Any]],
    live_rows: Dict[str, Dict[str, Any]],
    current_rows: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    syms = sorted(set((projected_rows or {}).keys()) | set((live_rows or {}).keys()) | set((current_rows or {}).keys()))
    out: Dict[str, Dict[str, Any]] = {}

    for sym in syms:
        projected_sw = _signed_weight((projected_rows or {}).get(sym))
        live_sw = _signed_weight((live_rows or {}).get(sym))
        current_sw = _signed_weight((current_rows or {}).get(sym))
        desired_sw = float(current_sw + (projected_sw - live_sw))

        if abs(desired_sw) <= 1e-12:
            continue

        base_row = (projected_rows or {}).get(sym) or (current_rows or {}).get(sym) or {"symbol": str(sym)}
        out[str(sym)] = _row_with_signed_weight(base_row, desired_sw, "projected_to_desired")

    return out


def _post_constraint_checks(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    checks: Dict[str, Any] = {
        "gross_within_cap": (float(MAX_GROSS) <= 0.0) or (float(snapshot.get("gross", 0.0) or 0.0) <= float(MAX_GROSS) + 1e-9),
        "net_within_cap": (float(MAX_NET) <= 0.0) or (abs(float(snapshot.get("net", 0.0) or 0.0)) <= float(MAX_NET) + 1e-9),
    }

    by_symbol = dict(snapshot.get("by_symbol") or {})
    checks["symbol_within_cap"] = True
    if float(MAX_SYMBOL_GROSS) > 0.0:
        for sym, row in by_symbol.items():
            try:
                if float((row or {}).get("gross", 0.0) or 0.0) > float(MAX_SYMBOL_GROSS) + 1e-9:
                    checks["symbol_within_cap"] = False
                    checks.setdefault("symbol_violations", {})[str(sym)] = float((row or {}).get("gross", 0.0) or 0.0)
            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_RISK_ENGINE_SYMBOL_CAP_CHECK_FAILED",
                    e,
                    symbol=str(sym),
                )
                continue

    checks["asset_class_within_cap"] = True
    if USE_ASSET_CLASS_BUDGETS:
        for cls, row in dict(snapshot.get("by_asset_class") or {}).items():
            cap = float(ASSET_CLASS_BUDGETS.get(str(cls).upper(), ASSET_CLASS_BUDGETS.get("UNKNOWN", 0.40)))
            try:
                gross = float((row or {}).get("gross", 0.0) or 0.0)
            except Exception:
                gross = 0.0
            if cap > 0.0 and gross > cap + 1e-9:
                checks["asset_class_within_cap"] = False
                checks.setdefault("asset_class_violations", {})[str(cls)] = {"gross": float(gross), "cap": float(cap)}

    checks["strategy_within_cap"] = True
    if USE_STRATEGY_BUDGETS:
        for strategy, row in dict(snapshot.get("by_strategy") or {}).items():
            try:
                gross = float((row or {}).get("gross", 0.0) or 0.0)
                net = float((row or {}).get("net", 0.0) or 0.0)
            except Exception:
                gross = 0.0
                net = 0.0
            if (float(STRATEGY_MAX_GROSS) > 0.0 and gross > float(STRATEGY_MAX_GROSS) + 1e-9) or (float(STRATEGY_MAX_NET) > 0.0 and abs(net) > float(STRATEGY_MAX_NET) + 1e-9):
                checks["strategy_within_cap"] = False
                checks.setdefault("strategy_violations", {})[str(strategy)] = {
                    "gross": float(gross),
                    "net": float(net),
                    "max_gross": float(STRATEGY_MAX_GROSS),
                    "max_net": float(STRATEGY_MAX_NET),
                }

    return checks


def _apply_portfolio_caps(desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out = dict(desired or {})

    g = _gross(out)
    n = _net(out)

    info["caps_pre_gross"] = float(g)
    info["caps_pre_net"] = float(n)
    info["cap_max_gross"] = float(MAX_GROSS)
    info["cap_max_net"] = float(MAX_NET)

    # Gross cap: scale all abs weights proportionally
    if float(MAX_GROSS) > 0.0 and g > float(MAX_GROSS) + 1e-12:
        scale = float(MAX_GROSS) / float(g) if g > 1e-12 else 0.0
        for sym in list(out.keys()):
            try:
                sw = _signed_weight(out[sym])
                sgn = 1.0 if sw >= 0.0 else -1.0
                out[sym]["weight"] = float(abs(sw) * scale) * float(sgn)
                out[sym].setdefault("reason", {})
                if isinstance(out[sym]["reason"], dict):
                    out[sym]["reason"]["portfolio_gross_cap"] = {"pre": float(g), "cap": float(MAX_GROSS), "scale": float(scale)}
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_GROSS_CAP_APPLY_FAILED", e, once_key=f"gross_cap:{sym}", symbol=str(sym))
        info["caps_gross_scaled"] = True
        info["caps_gross_scale"] = float(scale)

    # Net cap: if |net| exceeds, scale signed weights down around 0
    g2 = _gross(out)
    n2 = _net(out)

    if float(MAX_NET) > 0.0 and abs(n2) > float(MAX_NET) + 1e-12 and g2 > 1e-12:
        # scale signed weights toward 0 preserving signs
        scaleN = float(MAX_NET) / float(abs(n2)) if abs(n2) > 1e-12 else 0.0
        for sym in list(out.keys()):
            try:
                sw = _signed_weight(out[sym])
                out[sym]["weight"] = float(sw) * float(scaleN)
                out[sym].setdefault("reason", {})
                if isinstance(out[sym]["reason"], dict):
                    out[sym]["reason"]["portfolio_net_cap"] = {"pre": float(n2), "cap": float(MAX_NET), "scale": float(scaleN)}
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_NET_CAP_APPLY_FAILED", e, once_key=f"net_cap:{sym}", symbol=str(sym))
        info["caps_net_scaled"] = True
        info["caps_net_scale"] = float(scaleN)

    info["caps_post_gross"] = float(_gross(out))
    info["caps_post_net"] = float(_net(out))
    return out


def _apply_drawdown_throttle(desired: Dict[str, Dict[str, Any]], dd: float, info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out = dict(desired or {})

    if dd <= 0.0:
        return out

    info["dd"] = float(dd)
    info["dd_throttle_start"] = float(DD_THROTTLE_START)
    info["dd_throttle_min_scale"] = float(DD_THROTTLE_MIN_SCALE)
    info["dd_hard_block"] = float(DD_HARD_BLOCK)

    if float(DD_HARD_BLOCK) > 0.0 and dd >= float(DD_HARD_BLOCK):
        info["dd_hard_block_hit"] = True
        return out

    if float(DD_THROTTLE_START) > 0.0 and dd >= float(DD_THROTTLE_START):
        # linear ramp from 1.0 at start -> min_scale at hard_block (or at start+0.10 if hard_block disabled)
        end = float(DD_HARD_BLOCK) if float(DD_HARD_BLOCK) > float(DD_THROTTLE_START) else (float(DD_THROTTLE_START) + 0.10)
        t = (dd - float(DD_THROTTLE_START)) / max(1e-9, (end - float(DD_THROTTLE_START)))
        t = max(0.0, min(1.0, float(t)))
        scale = 1.0 - t * (1.0 - float(max(0.0, min(1.0, float(DD_THROTTLE_MIN_SCALE)))))
        g = _gross(out)
        if g > 1e-12:
            for sym in list(out.keys()):
                try:
                    sw = _signed_weight(out[sym])
                    sgn = 1.0 if sw >= 0.0 else -1.0
                    out[sym]["weight"] = float(abs(sw) * scale) * float(sgn)
                    out[sym].setdefault("reason", {})
                    if isinstance(out[sym]["reason"], dict):
                        out[sym]["reason"]["drawdown_throttle"] = {"dd": float(dd), "scale": float(scale)}
                except Exception as e:
                    _warn_nonfatal("PORTFOLIO_RISK_DRAWDOWN_THROTTLE_APPLY_FAILED", e, once_key=f"drawdown_throttle:{sym}", symbol=str(sym))
            info["dd_throttle_applied"] = True
            info["dd_throttle_scale"] = float(scale)

    return out


def _apply_asset_class_budgets(desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_ASSET_CLASS_BUDGETS:
        return dict(desired or {})

    out = dict(desired or {})
    by_cls: Dict[str, float] = {}
    for sym, row in (out or {}).items():
        try:
            cls = str(asset_class_for_symbol(str(sym)) or "UNKNOWN").upper()
        except Exception:
            cls = "UNKNOWN"
        by_cls[cls] = float(by_cls.get(cls, 0.0) + _abs_weight(row))

    info["asset_class_gross_pre"] = dict(sorted(by_cls.items(), key=lambda kv: kv[0]))

    hit: Dict[str, Any] = {}
    for cls, gross in list(by_cls.items()):
        cap = float(ASSET_CLASS_BUDGETS.get(str(cls).upper(), ASSET_CLASS_BUDGETS.get("UNKNOWN", 0.40)))
        if cap > 0.0 and float(gross) > float(cap) + 1e-12:
            scale = float(cap) / float(gross) if gross > 1e-12 else 0.0
            for sym in list(out.keys()):
                try:
                    cls2 = str(asset_class_for_symbol(str(sym)) or "UNKNOWN").upper()
                    if cls2 == str(cls).upper():
                        sw = _signed_weight(out[sym])
                        sgn = 1.0 if sw >= 0.0 else -1.0
                        out[sym]["weight"] = float(abs(sw) * scale) * float(sgn)
                        out[sym].setdefault("reason", {})
                        if isinstance(out[sym]["reason"], dict):
                            out[sym]["reason"]["asset_class_budget"] = {
                                "asset_class": str(cls).upper(),
                                "gross_pre": float(gross),
                                "cap": float(cap),
                                "scale": float(scale),
                            }
                except Exception as e:
                    _warn_nonfatal("PORTFOLIO_RISK_ASSET_CLASS_BUDGET_APPLY_FAILED", e, once_key=f"asset_budget:{cls}:{sym}", asset_class=str(cls), symbol=str(sym))
            hit[str(cls).upper()] = {"gross_pre": float(gross), "cap": float(cap), "scale": float(scale)}

    if hit:
        info["asset_class_budgets_hit"] = hit

    # post
    by_cls2: Dict[str, float] = {}
    for sym, row in (out or {}).items():
        try:
            cls = str(asset_class_for_symbol(str(sym)) or "UNKNOWN").upper()
        except Exception:
            cls = "UNKNOWN"
        by_cls2[cls] = float(by_cls2.get(cls, 0.0) + _abs_weight(row))
    info["asset_class_gross_post"] = dict(sorted(by_cls2.items(), key=lambda kv: kv[0]))

    return out


def _symbol_vol_input(con, symbol: str, *, ts_ms: int) -> Dict[str, Any]:
    try:
        resolved = resolve_vol_forecast(
            con,
            str(symbol),
            ts_ms=int(ts_ms),
            source=str(VOL_FORECAST_SOURCE or "trailing"),
            trailing_lookback=int(VOL_LOOKBACK),
        )
        vol = resolved.get("vol")
        if vol is None:
            return {"vol": None, "source": "missing"}
        return {
            "vol": float(vol),
            "source": str(resolved.get("resolved_source") or resolved.get("source") or VOL_FORECAST_SOURCE),
            "forecast_ratio": resolved.get("forecast_ratio"),
            "forecast_ts_ms": resolved.get("ts_ms"),
            "fallback": bool(resolved.get("fallback", False)),
        }
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_VOL_FORECAST_RESOLVE_FAILED",
            e,
            once_key=f"vol_forecast:{symbol}",
            symbol=str(symbol),
            source=str(VOL_FORECAST_SOURCE),
        )
        try:
            v = realized_vol_from_prices(con, str(symbol), lookback=int(VOL_LOOKBACK))
        except Exception:
            v = None
        return {"vol": (None if v is None else float(v)), "source": "trailing_exception_fallback", "fallback": True}


def _apply_symbol_vol_caps(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_VOL_CAPS:
        return dict(desired or {})

    out = dict(desired or {})
    vol_map: Dict[str, float] = {}
    vol_meta: Dict[str, Dict[str, Any]] = {}

    for sym in list(out.keys()):
        try:
            resolved = _symbol_vol_input(con, str(sym), ts_ms=int(info.get("ts_ms") or 0))
            v = resolved.get("vol")
            if v is None:
                continue
            vv = float(v)
            vv = max(float(PORTFOLIO_VOL_FLOOR), min(float(PORTFOLIO_VOL_CEIL), vv))
            vol_map[str(sym)] = float(vv)
            vol_meta[str(sym)] = dict(resolved)
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_RISK_ENGINE_VOL_CAP_ROW_FAILED",
                e,
                symbol=str(sym),
            )
            continue

    info["symbol_vol_n"] = int(len(vol_map))
    info["symbol_max_gross"] = float(MAX_SYMBOL_GROSS)
    info["vol_forecast_source"] = str(VOL_FORECAST_SOURCE or "trailing")
    if vol_meta:
        info["symbol_vol_inputs"] = dict(vol_meta)

    hit: Dict[str, Any] = {}
    for sym, row in list(out.items()):
        sw = _signed_weight(row)
        aw = abs(sw)
        if aw <= 0.0:
            continue

        v = vol_map.get(str(sym))
        meta = vol_meta.get(str(sym), {})
        mult = 1.0
        cap_source = "fallback_max_symbol_gross"

        if v is None or v <= 1e-12:
            cap = float(MAX_SYMBOL_GROSS) if float(MAX_SYMBOL_GROSS) > 0.0 else float(SYMBOL_CAP_MAX_W)
        else:
            mult = float(VOL_TARGET) / float(v)
            mult = max(float(SYMBOL_CAP_MIN_MULT), min(1.0, float(mult)))
            cap = min(float(SYMBOL_CAP_MAX_W), float(SYMBOL_CAP_MAX_W) * float(mult))
            if float(MAX_SYMBOL_GROSS) > 0.0:
                cap = min(float(cap), float(MAX_SYMBOL_GROSS))
            cap_source = str(meta.get("source") or VOL_FORECAST_SOURCE or "realized_vol")

        if cap <= 0.0:
            continue
        if aw > cap + 1e-12:
            scale = float(cap) / float(aw) if aw > 1e-12 else 0.0
            sgn = 1.0 if sw >= 0.0 else -1.0
            out[sym]["weight"] = float(abs(sw) * scale) * float(sgn)
            out[sym].setdefault("reason", {})
            if isinstance(out[sym]["reason"], dict):
                out[sym]["reason"]["symbol_vol_cap"] = {
                    "vol": (float(v) if v is not None else None),
                    "target": float(VOL_TARGET),
                    "mult": float(mult),
                    "cap": float(cap),
                    "pre": float(aw),
                    "scale": float(scale),
                    "cap_source": str(cap_source),
                    "forecast_ratio": meta.get("forecast_ratio"),
                }
            hit[str(sym)] = {
                "vol": (float(v) if v is not None else None),
                "cap": float(cap),
                "pre": float(aw),
                "scale": float(scale),
                "mult": float(mult),
                "cap_source": str(cap_source),
                "forecast_ratio": meta.get("forecast_ratio"),
            }

    if hit:
        info["symbol_vol_caps_hit"] = hit

    return out


def _corr_graph_components(con, syms: List[str]) -> Tuple[List[List[str]], Dict[str, Dict[str, float]]]:
    if not syms or len(syms) < 2:
        return [], {}

    # Build adjacency (undirected) for abs(corr) >= threshold
    adj: Dict[str, List[str]] = {s: [] for s in syms}
    matrix: Dict[str, Dict[str, float]] = {s: {} for s in syms}
    for i in range(len(syms)):
        matrix[syms[i]][syms[i]] = 1.0
        for j in range(i + 1, len(syms)):
            a, b = syms[i], syms[j]
            try:
                c = corr_from_prices(con, a, b, lookback=int(CORR_LOOKBACK))
                if c is None:
                    continue
                cc = max(-1.0, min(1.0, _safe_float(c, 0.0)))
                matrix[a][b] = float(cc)
                matrix[b][a] = float(cc)
                if abs(float(cc)) >= float(CLUSTER_CORR_TH):
                    adj[a].append(b)
                    adj[b].append(a)
            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_RISK_ENGINE_CLUSTER_CORR_ROW_FAILED",
                    e,
                    left_symbol=str(a),
                    right_symbol=str(b),
                )
                continue

    # DFS components
    seen = set()
    comps: List[List[str]] = []
    for s in syms:
        if s in seen:
            continue
        stack = [s]
        comp = []
        seen.add(s)
        while stack:
            x = stack.pop()
            comp.append(x)
            for y in adj.get(x, []):
                if y not in seen:
                    seen.add(y)
                    stack.append(y)
        if len(comp) >= 2:
            comps.append(sorted(comp))

    comps.sort(key=lambda comp: (-len(comp), ",".join(comp)))
    return comps, matrix


def _apply_corr_cluster_caps(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_CORR_CLUSTERS:
        return dict(desired or {})

    out = dict(desired or {})
    syms = _top_symbols_by_abs(out, int(MAX_SYMBOLS))
    comps, corr_matrix = _corr_graph_components(con, syms)

    info["corr_matrix"] = corr_matrix
    info["corr_cluster_symbols"] = list(syms)
    info["corr_cluster_corr_th"] = float(CLUSTER_CORR_TH)

    cluster_exposures = []
    for comp in comps:
        gross = 0.0
        net = 0.0
        for s in comp:
            gross += _abs_weight(out.get(s))
            net += _signed_weight(out.get(s))
        cluster_exposures.append(
            {
                "cluster": list(comp),
                "gross": float(gross),
                "net": float(net),
                "cap": float(CLUSTER_MAX_GROSS),
            }
        )
    if cluster_exposures:
        info["corr_cluster_exposures"] = cluster_exposures

    # bound number of components (worst-case compute)
    if CLUSTER_MAX_COMPONENTS > 0 and len(comps) > int(CLUSTER_MAX_COMPONENTS):
        comps = comps[: int(CLUSTER_MAX_COMPONENTS)]

    hit = []
    for comp in comps:
        gross = 0.0
        net = 0.0
        for s in comp:
            gross += _abs_weight(out.get(s))
            net += _signed_weight(out.get(s))
        if gross <= float(CLUSTER_MAX_GROSS) + 1e-12:
            continue

        scale = float(CLUSTER_MAX_GROSS) / float(gross) if gross > 1e-12 else 0.0
        for s in comp:
            try:
                sw = _signed_weight(out.get(s))
                sgn = 1.0 if sw >= 0.0 else -1.0
                out[s]["weight"] = float(abs(sw) * scale) * float(sgn)
                out[s].setdefault("reason", {})
                if isinstance(out[s]["reason"], dict):
                    out[s]["reason"]["corr_cluster_cap"] = {
                        "cluster": list(comp),
                        "gross_pre": float(gross),
                        "net_pre": float(net),
                        "cap": float(CLUSTER_MAX_GROSS),
                        "scale": float(scale),
                        "corr_th": float(CLUSTER_CORR_TH),
                    }
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_CORR_CLUSTER_APPLY_FAILED", e, once_key=f"corr_cluster:{s}", symbol=str(s), cluster=list(comp))

        hit.append(
            {
                "cluster": list(comp),
                "gross_pre": float(gross),
                "net_pre": float(net),
                "cap": float(CLUSTER_MAX_GROSS),
                "scale": float(scale),
            }
        )

    if hit:
        info["corr_cluster_caps_hit"] = hit

    return out


def _portfolio_vol_proxy(con, desired: Dict[str, Dict[str, Any]], info: Optional[Dict[str, Any]] = None) -> Optional[float]:
    syms = _top_symbols_by_abs(desired, int(MAX_SYMBOLS))
    if not syms:
        return None

    # signed weights (use raw weights as exposure fractions)
    active_syms: List[str] = []
    w: List[float] = []
    vols: List[float] = []
    vol_inputs: Dict[str, Dict[str, Any]] = {}
    for s in syms:
        row = (desired or {}).get(s) or {}
        sw = _signed_weight(row)
        aw = abs(sw)
        if aw <= 0.0:
            continue
        try:
            resolved = _symbol_vol_input(con, str(s), ts_ms=int((info or {}).get("ts_ms") or 0))
            v = resolved.get("vol")
            if v is None:
                return None
            vv = float(v)
            vv = max(float(PORTFOLIO_VOL_FLOOR), min(float(PORTFOLIO_VOL_CEIL), vv))
            vol_inputs[str(s)] = dict(resolved)
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_RISK_ENGINE_PORTFOLIO_VOL_SYMBOL_FAILED",
                e,
                symbol=str(s),
            )
            portfolio_vol = None
            return portfolio_vol
        active_syms.append(str(s))
        w.append(float(sw))
        vols.append(float(vv))

    if not w:
        return None
    if info is not None:
        info["portfolio_vol_inputs"] = dict(vol_inputs)
        info["vol_forecast_source"] = str(VOL_FORECAST_SOURCE or "trailing")

    # normalize by gross to avoid pathological scaling
    gross = sum(abs(x) for x in w)
    if gross <= 1e-12:
        return None
    wn = [float(x / gross) for x in w]

    # covariance proxy via corr
    var = 0.0
    for i in range(len(wn)):
        var += float(wn[i] * wn[i]) * float(vols[i] * vols[i])

    # pairwise cov
    for i in range(len(wn)):
        for j in range(i + 1, len(wn)):
            try:
                c = corr_from_prices(con, active_syms[i], active_syms[j], lookback=int(CORR_LOOKBACK))
                if c is None:
                    continue
                cc = max(-1.0, min(1.0, _safe_float(c, 0.0)))
                cov = float(vols[i]) * float(vols[j]) * float(cc)
                var += 2.0 * float(wn[i]) * float(wn[j]) * float(cov)
            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_RISK_ENGINE_PORTFOLIO_VOL_PAIR_FAILED",
                    e,
                    left_symbol=str(active_syms[i]),
                    right_symbol=str(active_syms[j]),
                )
                continue

    if var < 0.0:
        var = 0.0
    return float(var ** 0.5)


def _latest_options_gex_for_symbol(con, symbol: str, ts_ms: int) -> Optional[Dict[str, float]]:
    try:
        row = con.execute(
            """
            SELECT snapshot_ts_ms, gex_norm, gex_norm_z, gex_sign
            FROM options_symbol_features
            WHERE symbol=?
              AND snapshot_ts_ms <= ?
            ORDER BY snapshot_ts_ms DESC, bucket_ts_ms DESC
            LIMIT 1
            """,
            (str(symbol), int(ts_ms)),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_GEX_LOOKUP_FAILED",
            e,
            once_key=f"gex_lookup:{symbol}",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return None
    if not row:
        return None
    return {
        "snapshot_ts_ms": float(_safe_float(row[0], 0.0)),
        "gex_norm": float(_safe_float(row[1], 0.0)),
        "gex_norm_z": float(max(-10.0, min(10.0, _safe_float(row[2], 0.0)))),
        "gex_sign": float(max(-1.0, min(1.0, _safe_float(row[3], 0.0)))),
    }


def _gex_vol_target_modifier(con, desired: Dict[str, Dict[str, Any]], ts_ms: int) -> Dict[str, Any]:
    """Return a volatility-regime modifier from GEX, never a direction signal."""

    if not USE_GEX_VOL_MODIFIER:
        return {"enabled": False, "modifier": 1.0}
    weighted_norm = 0.0
    weighted_z = 0.0
    total_weight = 0.0
    rows: Dict[str, Dict[str, float]] = {}
    for sym, row in (desired or {}).items():
        abs_w = abs(_signed_weight(row))
        if abs_w <= 0.0:
            continue
        gex = _latest_options_gex_for_symbol(con, str(sym), int(ts_ms))
        if not gex:
            continue
        weighted_norm += float(abs_w) * float(gex.get("gex_norm", 0.0))
        weighted_z += float(abs_w) * float(gex.get("gex_norm_z", 0.0))
        total_weight += float(abs_w)
        rows[str(sym)] = dict(gex)
    if total_weight <= 0.0:
        return {"enabled": True, "modifier": 1.0, "symbols": rows, "coverage": 0.0}
    avg_norm = float(weighted_norm / total_weight)
    avg_z = float(weighted_z / total_weight)
    negative_pressure = max(0.0, min(1.0, -avg_z / 2.0))
    positive_damping = max(0.0, min(1.0, avg_z / 2.0))
    modifier = float(max(0.75, min(1.10, 1.0 - 0.12 * negative_pressure + 0.04 * positive_damping)))
    return {
        "enabled": True,
        "modifier": float(modifier),
        "gex_norm": float(avg_norm),
        "gex_norm_z": float(avg_z),
        "coverage": float(total_weight / max(1e-9, _gross(desired or {}))),
        "symbols": rows,
        "usage": "volatility_regime_conditioning_not_direction",
    }


def _apply_portfolio_vol_target(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out = dict(desired or {})
    pv = _portfolio_vol_proxy(con, out, info)
    if pv is None:
        return out

    info["portfolio_vol_proxy"] = float(pv)
    gex_modifier = _gex_vol_target_modifier(con, out, int(info.get("ts_ms") or 0))
    effective_target = float(VOL_TARGET) * float(gex_modifier.get("modifier", 1.0) or 1.0)
    info["portfolio_vol_target"] = float(VOL_TARGET)
    info["portfolio_vol_effective_target"] = float(effective_target)
    info["portfolio_gex_vol_modifier"] = dict(gex_modifier)
    info["portfolio_vol_hard_block"] = float(PORTFOLIO_VOL_HARD_BLOCK)

    # Hard block if configured
    if float(PORTFOLIO_VOL_HARD_BLOCK) > 0.0 and float(pv) >= float(PORTFOLIO_VOL_HARD_BLOCK):
        info["portfolio_vol_hard_block_hit"] = True
        return out

    # Scale entire portfolio to target (if pv > target)
    if float(effective_target) > 0.0 and float(pv) > float(effective_target) + 1e-12:
        scale = float(effective_target) / float(pv) if pv > 1e-12 else 0.0
        for sym in list(out.keys()):
            try:
                sw = _signed_weight(out[sym])
                out[sym]["weight"] = float(sw) * float(scale)
                out[sym].setdefault("reason", {})
                if isinstance(out[sym]["reason"], dict):
                    out[sym]["reason"]["portfolio_vol_target"] = {
                        "pre_vol": float(pv),
                        "target": float(effective_target),
                        "base_target": float(VOL_TARGET),
                        "scale": float(scale),
                        "vol_source": str(
                            ((info.get("portfolio_vol_inputs") or {}).get(str(sym)) or {}).get("source")
                            or VOL_FORECAST_SOURCE
                            or "trailing"
                        ),
                        "gex_modifier": float(gex_modifier.get("modifier", 1.0) or 1.0),
                    }
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_VOL_TARGET_APPLY_FAILED", e, once_key=f"vol_target:{sym}", symbol=str(sym))
        info["portfolio_vol_scaled"] = True
        info["portfolio_vol_scale"] = float(scale)

    return out


def _persist_snapshot(con, now_ms: int, info: Dict[str, Any]) -> None:
    try:
        con.execute(
            """
            INSERT OR REPLACE INTO portfolio_risk_snapshots(
              ts_ms, gross, net, vol_proxy, drawdown, blocked, info_json
            )
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                int(now_ms),
                float(info.get("final_gross", 0.0) or 0.0),
                float(info.get("final_net", 0.0) or 0.0),
                _optional_float(info.get("portfolio_vol_proxy")),
                _optional_float(info.get("drawdown")),
                (1 if bool(info.get("blocked", False)) else 0),
                json.dumps(info or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_SNAPSHOT_PERSIST_FAILED", e, once_key="persist_snapshot", ts_ms=int(now_ms))

    if not bool(info.get("blocked", False)):
        return

    try:
        block_reason = dict(info.get("block_reason") or {})
        con.execute(
            """
            INSERT INTO risk_events(
              ts_ms, trigger_type, reason, equity, drawdown_pct, var_pct, concentration, positions, metadata_json
            )
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                int(now_ms),
                str(block_reason.get("type") or "portfolio_risk_block"),
                str(block_reason.get("type") or "portfolio_risk_block"),
                _optional_float((info.get("live_positions") or {}).get("equity_ref")),
                _optional_float(info.get("drawdown")),
                _optional_float(info.get("portfolio_vol_proxy")),
                _optional_float(info.get("final_gross")),
                int(len(dict((info.get("target_exposure_post") or {}).get("by_symbol") or {}))),
                json.dumps(info or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_EVENT_PERSIST_FAILED", e, once_key="persist_risk_event", ts_ms=int(now_ms))


def apply_portfolio_risk_engine(
    con,
    desired: Dict[str, Dict[str, Any]],
    state: Dict[str, Dict[str, Any]],
    now_ms: int,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Apply portfolio-level risk limits to desired allocations.

    Parameters
    ----------
    con : storage connection
        Open runtime database connection used for prices, live positions, and
        persistence of risk state.
    desired : dict
        Proposed target allocations before portfolio-risk adjustment. The values
        are strategy/position payloads consumed by the portfolio engine's
        exposure helpers.
    state : dict
        Current portfolio state in the same structural shape as ``desired``.
    now_ms : int
        Evaluation timestamp in epoch milliseconds.

    Returns
    -------
    tuple[dict, dict]
        Two-tuple ``(adjusted_desired, info)``. ``adjusted_desired`` is the
        post-risk target map, and ``info`` contains the detailed risk summary
        that is also persisted for dashboards and audit trails.

    Notes
    -----
    Drawdown thresholds are fractions of equity rather than percentage
    integers. Hard blocks can be triggered by drawdown, portfolio volatility,
    Monte Carlo summaries, or post-cap validation. When the engine is disabled,
    the input allocations are returned unchanged with ``{"enabled": False}``.

    Side Effects
    ------------
    Updates ``portfolio_risk_*`` keys in runtime risk state, records a risk
    block event, persists a snapshot row, and annotates returned allocations
    with portfolio-risk metadata.
    """
    if not USE:
        try:
            clear_info = {"enabled": False, "ts_ms": int(now_ms), "blocked": False}
            set_state("portfolio_risk_block", "0")
            set_state("portfolio_risk_info", json.dumps(clear_info, separators=(",", ":"), sort_keys=True))
            set_state("portfolio_risk_summary", json.dumps(clear_info, separators=(",", ":"), sort_keys=True))
            set_state("portfolio_risk_status", "disabled")
            set_state("portfolio_risk_ts_ms", str(int(now_ms)))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_DISABLE_STATE_SET_FAILED", e, once_key="disable_state_set", ts_ms=int(now_ms))
        return desired, {"enabled": False}

    info: Dict[str, Any] = {"enabled": True, "ts_ms": int(now_ms)}

    dd = 0.0
    try:
        dd = float(get_current_drawdown(con))
    except Exception:
        dd = 0.0
    info["drawdown"] = float(dd)

    state_snapshot = _exposure_snapshot(state or {})
    info["state_exposure"] = state_snapshot

    live_rows, live_info = _load_live_positions(con)
    live_snapshot = _exposure_snapshot(live_rows or {})
    info["live_positions"] = dict(live_info or {})
    info["live_exposure"] = live_snapshot
    info["current_exposure"] = live_snapshot if live_rows else state_snapshot
    info["cur_gross"] = float(info["current_exposure"].get("gross", 0.0) or 0.0)
    info["cur_net"] = float(info["current_exposure"].get("net", 0.0) or 0.0)

    raw_desired = dict(desired or {})
    info["desired_exposure_raw"] = _exposure_snapshot(raw_desired)
    info["state_to_desired_delta"] = _delta_snapshot(state or {}, raw_desired)

    out = _project_live_plus_orders(live_rows or {}, state or {}, raw_desired)
    info["projected_live_plus_orders_pre"] = _exposure_snapshot(out)
    info["target_exposure_pre"] = dict(info["projected_live_plus_orders_pre"])
    info["target_delta_pre"] = _delta_snapshot(live_rows or {}, out)

    blocked = False
    block_reason: Optional[Dict[str, Any]] = None

    if float(DD_HARD_BLOCK) > 0.0 and float(dd) >= float(DD_HARD_BLOCK):
        blocked = True
        block_reason = {"type": "drawdown_hard_block", "dd": float(dd), "threshold": float(DD_HARD_BLOCK)}

    try:
        out = _apply_drawdown_throttle(out, float(dd), info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_DRAWDOWN_THROTTLE_FAILED", e, once_key="apply_drawdown_throttle", ts_ms=int(now_ms))

    try:
        out = _apply_asset_class_budgets(out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_ASSET_CLASS_BUDGETS_FAILED", e, once_key="apply_asset_class_budgets", ts_ms=int(now_ms))

    try:
        out = _apply_strategy_budgets(out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_STRATEGY_BUDGETS_FAILED", e, once_key="apply_strategy_budgets", ts_ms=int(now_ms))

    try:
        out = _apply_alpha_decay_throttle(con, out, info, int(now_ms))
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_ALPHA_DECAY_THROTTLE_FAILED", e, once_key="apply_alpha_decay_throttle", ts_ms=int(now_ms))

    try:
        out = _apply_symbol_vol_caps(con, out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_SYMBOL_VOL_CAPS_FAILED", e, once_key="apply_symbol_vol_caps", ts_ms=int(now_ms))

    try:
        out = _apply_corr_cluster_caps(con, out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_CORR_CLUSTER_CAPS_FAILED", e, once_key="apply_corr_cluster_caps", ts_ms=int(now_ms))

    try:
        out = _apply_portfolio_vol_target(con, out, info)
        if bool(info.get("portfolio_vol_hard_block_hit", False)):
            blocked = True
            block_reason = {
                "type": "portfolio_vol_hard_block",
                "vol": float(info.get("portfolio_vol_proxy", 0.0) or 0.0),
                "threshold": float(PORTFOLIO_VOL_HARD_BLOCK),
            }
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_VOL_TARGET_FAILED", e, once_key="apply_portfolio_vol_target", ts_ms=int(now_ms))

    try:
        mc_info = _load_monte_carlo_risk_summary(int(now_ms))
        if mc_info:
            info["monte_carlo_risk"] = mc_info
            if bool(mc_info.get("blocked", False)):
                blocked = True
                block_reason = {
                    "type": "monte_carlo_risk_block",
                    "monte_carlo": dict(mc_info),
                }
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_MONTE_CARLO_SUMMARY_FAILED", e, once_key="monte_carlo_summary", ts_ms=int(now_ms))

    try:
        out = _apply_portfolio_caps(out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_PORTFOLIO_CAPS_FAILED", e, once_key="apply_portfolio_caps", ts_ms=int(now_ms))

    final_snapshot = _exposure_snapshot(out)
    info["projected_live_plus_orders_post"] = final_snapshot
    info["target_exposure_post"] = final_snapshot
    info["target_delta_post"] = _delta_snapshot(live_rows or {}, out)
    info["state_to_target_delta_post"] = _delta_snapshot(state or {}, _projected_to_desired_targets(out, live_rows or {}, state or {}))
    info["final_gross"] = float(final_snapshot.get("gross", 0.0) or 0.0)
    info["final_net"] = float(final_snapshot.get("net", 0.0) or 0.0)

    post_checks = _post_constraint_checks(final_snapshot)
    info["post_checks"] = post_checks

    if (not blocked) and (not all(bool(v) for k, v in post_checks.items() if not str(k).endswith("_violations"))):
        blocked = True
        block_reason = {
            "type": "post_cap_validation_failed",
            "final_gross": float(info["final_gross"]),
            "final_net": float(info["final_net"]),
            "max_gross": float(MAX_GROSS),
            "max_net": float(MAX_NET),
            "checks": dict(post_checks),
        }

    out = _projected_to_desired_targets(out, live_rows or {}, state or {})
    info["desired_exposure_post"] = _exposure_snapshot(out)
    info["allocation_reconciliation"] = _reconciliation_summary(
        dict(info.get("desired_exposure_raw") or {}),
        dict(info.get("desired_exposure_post") or {}),
    )

    info["blocked"] = bool(blocked)
    if block_reason is not None:
        info["block_reason"] = dict(block_reason)

    try:
        state_blob = json.dumps(info or {}, separators=(",", ":"), sort_keys=True)
        summary_blob = json.dumps(
            {
                "enabled": True,
                "ts_ms": int(now_ms),
                "blocked": bool(blocked),
                "block_reason": (dict(block_reason) if block_reason is not None else None),
                "drawdown": float(info.get("drawdown", 0.0) or 0.0),
                "cur_gross": float(info.get("cur_gross", 0.0) or 0.0),
                "cur_net": float(info.get("cur_net", 0.0) or 0.0),
                "final_gross": float(info.get("final_gross", 0.0) or 0.0),
                "final_net": float(info.get("final_net", 0.0) or 0.0),
                "portfolio_vol_proxy": info.get("portfolio_vol_proxy"),
            },
            separators=(",", ":"),
            sort_keys=True,
        )

        set_state("portfolio_risk_block", "1" if blocked else "0")
        set_state("portfolio_risk_info", state_blob)
        set_state("portfolio_risk_summary", summary_blob)
        set_state("portfolio_risk_status", ("blocked" if blocked else "clear"))
        set_state("portfolio_risk_ts_ms", str(int(now_ms)))
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_STATE_SET_FAILED", e, once_key="state_set", ts_ms=int(now_ms), blocked=bool(blocked))

    try:
        record_risk_block(
            name="portfolio_risk_engine",
            blocked=bool(blocked),
            info=info,
            ts_ms=int(now_ms),
            con=con,
        )
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_RECORD_BLOCK_FAILED", e, once_key="record_risk_block", ts_ms=int(now_ms), blocked=bool(blocked))

    try:
        _persist_snapshot(con, int(now_ms), info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_PERSIST_SNAPSHOT_FAILED", e, once_key="persist_snapshot_call", ts_ms=int(now_ms))

    _annotate(out, info)
    return out, info

def _apply_strategy_budgets(desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_STRATEGY_BUDGETS:
        return dict(desired or {})

    out = dict(desired or {})

    strat_gross: Dict[str, float] = {}
    strat_net: Dict[str, float] = {}

    for sym, row in (out or {}).items():
        sid = _strategy_bucket_for_row(row)
        sw = _signed_weight(row)
        strat_gross[sid] = float(strat_gross.get(sid, 0.0) + abs(sw))
        strat_net[sid] = float(strat_net.get(sid, 0.0) + sw)

    info["strategy_gross_pre"] = dict(strat_gross)
    info["strategy_net_pre"] = dict(strat_net)

    hit: Dict[str, Any] = {}

    for sid in strat_gross.keys():
        g = float(strat_gross.get(sid, 0.0))
        n = float(strat_net.get(sid, 0.0))

        scale = 1.0

        if STRATEGY_MAX_GROSS > 0.0 and g > STRATEGY_MAX_GROSS:
            scale = min(scale, float(STRATEGY_MAX_GROSS) / float(g) if g > 1e-12 else 0.0)

        if STRATEGY_MAX_NET > 0.0 and abs(n) > STRATEGY_MAX_NET:
            scale = min(scale, float(STRATEGY_MAX_NET) / float(abs(n)) if abs(n) > 1e-12 else 0.0)

        if scale < 1.0:
            for sym, row in (out or {}).items():
                if _strategy_bucket_for_row(row) == sid:
                    sw = _signed_weight(row)
                    out[sym]["weight"] = float(sw) * float(scale)
                    out[sym].setdefault("reason", {})
                    if isinstance(out[sym]["reason"], dict):
                        out[sym]["reason"]["strategy_budget"] = {
                            "strategy": sid,
                            "gross_pre": g,
                            "net_pre": n,
                            "scale": float(scale),
                            "max_gross": float(STRATEGY_MAX_GROSS),
                            "max_net": float(STRATEGY_MAX_NET),
                        }

            hit[sid] = {
                "gross_pre": g,
                "net_pre": n,
                "scale": float(scale),
            }

    if hit:
        info["strategy_budgets_hit"] = hit

    strat_gross_post: Dict[str, float] = {}
    strat_net_post: Dict[str, float] = {}

    for sym, row in (out or {}).items():
        sid = _strategy_bucket_for_row(row)
        sw = _signed_weight(row)
        strat_gross_post[sid] = float(strat_gross_post.get(sid, 0.0) + abs(sw))
        strat_net_post[sid] = float(strat_net_post.get(sid, 0.0) + sw)

    info["strategy_gross_post"] = strat_gross_post
    info["strategy_net_post"] = strat_net_post

    return out
