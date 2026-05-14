"""
FILE: alerts.py

Runtime subsystem module for `alerts`.

This module owns alert-table schema, rule thresholds, dedupe/cooldown logic,
and publication of operator-facing alert events derived from model and
attribution state.
"""

# dev_core/alerts.py

import json
import os
import time
import logging
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db
from engine.runtime.state_cache import cache_get_or_load, cache_invalidate_namespace
from engine.strategy.model_v2 import get_current_regime, get_regime_prior
from engine.strategy.learning import get_global_prior
from engine.strategy.edge_filter import adjust_expected_z_for_costs
from engine.runtime.event_bus import publish_event

# ------            -- ------------------------------------------------------
# Schema
# ------            -- ------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  event_id INTEGER,
  event_title TEXT NOT NULL,
  symbol TEXT NOT NULL,
  horizon_s INTEGER NOT NULL,
  expected_z REAL NOT NULL,
  confidence REAL NOT NULL,
  severity TEXT NOT NULL,
  rule_id TEXT NOT NULL,
  explain_json TEXT,
  dedupe_key TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_alerts_ts ON alerts(ts_ms);
CREATE INDEX IF NOT EXISTS idx_alerts_sym ON alerts(symbol);
"""

# ------            -- ------------------------------------------------------
# Defaults / thresholds
# ------            -- ------------------------------------------------------

DEFAULT_RULES = [
    {"rule_id": "info_z075_conf45", "min_abs_z": 0.75, "min_conf": 0.45, "severity": "INFO"},
    {"rule_id": "warn_z1_conf55", "min_abs_z": 1.0, "min_conf": 0.55, "severity": "WARN"},
    {"rule_id": "high_z15_conf60", "min_abs_z": 1.5, "min_conf": 0.60, "severity": "HIGH"},
    {"rule_id": "crit_z2_conf70", "min_abs_z": 2.0, "min_conf": 0.70, "severity": "CRIT"},
]

MIN_SUPPORT_N = int(os.environ.get("ALERT_MIN_SUPPORT_N", "12"))
MIN_VALIDATION_N = int(os.environ.get("ALERT_MIN_VALIDATION_N", "12"))
MAX_RMSE = float(os.environ.get("ALERT_MAX_RMSE", "1.75"))

MAX_DRIFT_RATIO = float(os.environ.get("ALERT_MAX_DRIFT_RATIO", "2.25"))
LOW_CONF_COOLDOWN_MULT = float(os.environ.get("ALERT_LOW_CONF_COOLDOWN_MULT", "2.0"))
LOW_CONF_LEVEL = float(os.environ.get("ALERT_LOW_CONF_LEVEL", "0.60"))

MIN_RELEVANCE = float(os.environ.get("ALERT_MIN_RELEVANCE", "0.35"))

COOLDOWN_WARN_S = int(os.environ.get("ALERT_COOLDOWN_WARN_S", "600"))
COOLDOWN_HIGH_S = int(os.environ.get("ALERT_COOLDOWN_HIGH_S", "1800"))
COOLDOWN_CRIT_S = int(os.environ.get("ALERT_COOLDOWN_CRIT_S", "3600"))

ALERT_DEDUPE_WINDOW_S = int(os.environ.get("ALERT_DEDUPE_WINDOW_S", "300"))

# ------            -- ------------------------------------------------------
# Rate limits (production safety)
# ------            -- ------------------------------------------------------
ALERT_RATE_WINDOW_S = int(os.environ.get("ALERT_RATE_WINDOW_S", "3600"))  # 1h
ALERT_MAX_PER_WINDOW_GLOBAL = int(os.environ.get("ALERT_MAX_PER_WINDOW_GLOBAL", "250"))
ALERT_MAX_PER_WINDOW_PER_SYMBOL = int(os.environ.get("ALERT_MAX_PER_WINDOW_PER_SYMBOL", "40"))

ALERT_SEV_WARN = float(os.environ.get("ALERT_SEV_WARN", "0.75"))
ALERT_SEV_CRIT = float(os.environ.get("ALERT_SEV_CRIT", "1.50"))

ALERT_PLAYBOOKS = {
    # Default playbooks by severity (used when a rule_id-specific playbook is not present)
    "INFO": {
        "summary": "Monitor only. No action required.",
        "steps": [
            "Verify the alert matches expected news/event flow.",
            "No changes needed unless alerts become frequent or drift increases.",
        ],
    },
    "WARN": {
        "summary": "Investigate. Confirm data freshness and model stability; consider reducing sizing.",
        "steps": [
            "Check /api/health for stale prices/events/predictions.",
            "Review drift dashboard and recent validation scores.",
            "If warnings persist, reduce sizing or raise confidence threshold temporarily.",
        ],
    },
    "HIGH": {
        "summary": "Elevated risk. Validate model performance and recent changes before acting.",
        "steps": [
            "Check model metrics and validation for the affected symbol/horizon.",
            "Review recent job history for failures or repeated restarts.",
            "Consider pausing execution for the affected universe until resolved.",
        ],
    },
    "CRIT": {
        "summary": "High risk. Pause execution and investigate immediately.",
        "steps": [
            "Pause/disable execution (broker_apply_orders) until root cause is identified.",
            "Verify data freshness, drift, and recent model promotions/rollbacks.",
            "Resolve the underlying issue, then resume with reduced sizing and close monitoring.",
        ],
    },
}

RULE_PLAYBOOKS = {
    # Rule-specific overrides (keyed by rule_id)
    "EQUITY_RECON": {
        "summary": "Broker vs backtest mismatch. Stop execution and reconcile fills/prices.",
        "steps": [
            "Stop broker_apply_orders to prevent compounding errors.",
            "Open broker snapshot and compare to backtest latest run (timestamps & prices).",
            "Inspect recent fills for abnormal slippage/fees and missing price updates.",
            "After reconciliation, acknowledge the alert and resume with reduced sizing.",
        ],
    },
    "EQUITY_DRIFT_SUSTAINED": {
        "summary": "Sustained equity drift trend. Investigate execution quality and pricing.",
        "steps": [
            "Check execution_metrics (fees/slippage/cost bps) for sudden changes.",
            "Check poll_prices health and ensure pricing source is stable.",
            "If drift persists, pause execution and re-run portfolio_backtest to verify assumptions.",
        ],
    },
}


def _get_playbook(severity: str, rule_id: str = "") -> dict:
    # Rule-specific playbooks take precedence so responders see the most
    # relevant operating steps for that exact alert family.
    rid = str(rule_id or "").strip()
    if rid and rid in RULE_PLAYBOOKS:
        return dict(RULE_PLAYBOOKS[rid])
    sev = str(severity or "INFO").strip().upper() or "INFO"
    return dict(ALERT_PLAYBOOKS.get(sev) or ALERT_PLAYBOOKS["INFO"])


REGIME_Z_MULT = {"LOW": 0.9, "MID": 1.0, "HIGH": 1.2}

# ------            -- ------------------------------------------------------
# Logging
# ------            -- ------------------------------------------------------

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [alerts] %(message)s",
)

ALERT_LOG_COST_REJECTS = os.environ.get("ALERT_LOG_COST_REJECTS", "1") == "1"
LOGGER = get_logger("runtime.alerts")


def _warn(scope: str, err: Exception, **extra) -> None:
    try:
        log_failure(
            LOGGER,
            event=str(scope),
            code=f"ALERTS_{str(scope).upper().replace('.', '_').replace('-', '_')}_FAILED",
            message=str(err),
            error=err,
            level=logging.WARNING,
            component="engine.runtime.alerts",
            extra=extra or None,
            persist=False,
        )
    except Exception as log_err:
        LOGGER.log(
            logging.WARNING,
            "alerts_warn_fallback",
            extra={
                "event": "alerts_warn_fallback",
                "component": "engine.runtime.alerts",
                "extra_json": {
                    "scope": str(scope),
                    "error_type": type(err).__name__,
                    "error_message": str(err),
                    "log_failure_error_type": type(log_err).__name__,
                    "log_failure_error_message": str(log_err),
                    "extra": dict(extra or {}),
                },
            },
        )

# ------            -- ------------------------------------------------------
# DB init
# ------            -- ------------------------------------------------------

def init_alerts_db() -> None:
    init_db()

# ------            -- ------------------------------------------------------
# Helpers
# ------            -- ------------------------------------------------------

def _begin_managed_write(con: Any) -> None:
    begin_write = getattr(con, "begin_managed_write", None)
    if callable(begin_write):
        begin_write()


def _has_column(con: Any, table_name: str, column_name: str) -> bool:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
    except Exception as e:
        _warn(
            "alerts.has_column",
            e,
            table_name=str(table_name),
            column_name=str(column_name),
        )
        return False
    wanted = str(column_name or "").strip().lower()
    return any(str(row[1] or "").strip().lower() == wanted for row in rows)


def _resolve_prediction_id_for_alert(
    con: Any,
    *,
    event_id: Optional[int],
    symbol: str,
    horizon_s: int,
    model_id: Optional[str],
    model_name: Optional[str],
) -> Optional[int]:
    event_id_i = int(event_id) if event_id is not None else 0
    symbol_u = str(symbol or "").strip().upper()
    horizon_i = int(horizon_s or 0)
    if event_id_i <= 0 or not symbol_u or horizon_i <= 0:
        return None
    try:
        rows = con.execute(
            """
            SELECT id, model_name, model_id, ts_ms
            FROM predictions
            WHERE event_id=? AND UPPER(TRIM(symbol))=? AND horizon_s=?
            ORDER BY ts_ms DESC, id DESC
            """,
            (int(event_id_i), str(symbol_u), int(horizon_i)),
        ).fetchall()
    except Exception as e:
        _warn(
            "alerts.resolve_prediction_id.lookup",
            e,
            event_id=int(event_id_i),
            symbol=str(symbol_u),
            horizon_s=int(horizon_i),
            model_id=str(model_id_s if 'model_id_s' in locals() else model_id or ""),
            model_name=str(model_name or ""),
        )
        rows = []
    if not rows:
        return None

    model_id_s = str(model_id or "").strip()
    model_name_s = str(model_name or "").strip()

    def _rank(row: Any) -> tuple[int, int, int]:
        row_model_name = str(row[1] or "").strip()
        row_model_id = str(row[2] or "").strip()
        match = 2
        if model_id_s and row_model_id == model_id_s:
            match = 0
        elif model_name_s and row_model_name == model_name_s:
            match = 1
        return (match, -int(row[3] or 0), -int(row[0] or 0))

    best = min(rows, key=_rank)
    try:
        return int(best[0])
    except Exception as e:
        _warn(
            "alerts.resolve_prediction_id.coerce",
            e,
            event_id=int(event_id_i),
            symbol=str(symbol_u),
            horizon_s=int(horizon_i),
            prediction_id=repr(best[0]),
        )
        return None

def severity_rank(s: str) -> int:
    return {"INFO": 0, "WARN": 1, "HIGH": 2, "CRIT": 3}.get((s or "").upper(), 0)


def _extract_signal_model_name(explain: Optional[Dict]) -> str:
    ex = explain if isinstance(explain, dict) else {}
    for key in ("model_name", "strategy_name", "strategy", "model"):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    strategy_obj = ex.get("strategy")
    if isinstance(strategy_obj, dict):
        val = strategy_obj.get("name")
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    model_obj = ex.get("model")
    if isinstance(model_obj, dict):
        for key in ("model_name", "name", "id"):
            val = model_obj.get(key)
            if isinstance(val, str) and val.strip():
                return str(val).strip()

    return "default_challenger"


def _extract_signal_model_id(explain: Optional[Dict]) -> Optional[str]:
    ex = explain if isinstance(explain, dict) else {}
    for key in ("model_id",):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    model_obj = ex.get("model")
    if isinstance(model_obj, dict):
        val = model_obj.get("model_id")
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    model_meta_obj = ex.get("model_meta")
    if isinstance(model_meta_obj, dict):
        val = model_meta_obj.get("model_id")
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    return None


def _extract_signal_model_version(explain: Optional[Dict]) -> Optional[str]:
    ex = explain if isinstance(explain, dict) else {}
    for key in ("model_version",):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    model_obj = ex.get("model")
    if isinstance(model_obj, dict):
        val = model_obj.get("model_version")
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    model_meta_obj = ex.get("model_meta")
    if isinstance(model_meta_obj, dict):
        val = model_meta_obj.get("model_version")
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    return None


def _extract_signal_regime(symbol: str, explain: Optional[Dict]) -> str:
    ex = explain if isinstance(explain, dict) else {}
    for key in ("regime", "current_regime", "regime_label"):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            return str(val).strip()
    try:
        reg = get_current_regime(str(symbol))
        if isinstance(reg, str) and reg.strip():
            return str(reg).strip()
    except Exception as e:
        _warn("alerts.extract_signal_regime", e, symbol=str(symbol))
    return "global"


def _extract_market_regime(explain: Optional[Dict]) -> str:
    ex = explain if isinstance(explain, dict) else {}
    for key in ("market_regime", "market_regime_label"):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            return str(val).strip()
    nested = ex.get("market_regime_snapshot")
    if isinstance(nested, dict):
        val = nested.get("label")
        if isinstance(val, str) and val.strip():
            return str(val).strip()
    return "mean_reversion"


def _attach_market_regime(symbol: str, ts_ms: int, explain: Dict) -> None:
    if not isinstance(explain, dict):
        return
    try:
        from engine.strategy.tech_indicators import get_market_regime_snapshot

        snap = get_market_regime_snapshot(str(symbol), int(ts_ms)) or {}
        label = str(snap.get("label") or "mean_reversion")
        explain["market_regime"] = label
        explain["market_regime_label"] = label
        explain["market_regime_snapshot"] = {
            "label": label,
            "volatility": float(snap.get("volatility", 0.0) or 0.0),
            "volatility_baseline": float(snap.get("volatility_baseline", 0.0) or 0.0),
            "trend": float(snap.get("trend", 0.0) or 0.0),
            "trend_strength": float(snap.get("trend_strength", 0.0) or 0.0),
        }
    except Exception as e:
        _warn("alerts.market_regime", e, symbol=str(symbol), ts_ms=int(ts_ms))


def _signal_side(expected_z: float) -> str:
    try:
        if float(expected_z) > 0.0:
            return "buy"
        if float(expected_z) < 0.0:
            return "sell"
    except Exception as e:
        _warn("alerts.signal_side", e, expected_z=expected_z)
    return "hold"


def build_strategy_signal_payload(
    *,
    alert_id: Optional[int],
    event_title: str,
    symbol: str,
    horizon_s: int,
    expected_z: float,
    confidence: float,
    explain: Optional[Dict] = None,
    event_id: Optional[int] = None,
    ts_ms: Optional[int] = None,
) -> Dict:
    explain_obj = dict(explain or {})
    payload = {
        "source_alert_id": (int(alert_id) if alert_id is not None else None),
        "event_title": str(event_title),
        "symbol": str(symbol).upper().strip(),
        "horizon_s": int(horizon_s),
        "expected_z": float(expected_z),
        "confidence": float(confidence),
        "signal": _signal_side(float(expected_z)),
        "side": _signal_side(float(expected_z)),
        "model_name": _extract_signal_model_name(explain_obj),
        "model_id": _extract_signal_model_id(explain_obj),
        "model_version": _extract_signal_model_version(explain_obj),
        "regime": _extract_signal_regime(symbol, explain_obj),
        "market_regime": _extract_market_regime(explain_obj),
        "explain": explain_obj,
        "ts_ms": int(ts_ms if ts_ms is not None else int(time.time() * 1000)),
    }
    if event_id is not None:
        payload["event_id"] = int(event_id)
    return payload


def _get_validation_row(symbol: str, horizon_s: int) -> Optional[Tuple[float, float, int]]:
    con = connect()
    try:
        row = con.execute(
            """
            SELECT mae, rmse, n
            FROM validation_scores
            WHERE symbol=? AND horizon_s=?
            """,
            (symbol, horizon_s),
        ).fetchone()
        if not row:
            return None
        return float(row[0]), float(row[1]), int(row[2])
    except Exception as e:
        _warn("alerts.suppression_row_parse", e)
        return None
    finally:
        con.close()


def _support_n(symbol: str, horizon_s: int) -> int:
    _, reg_n, _ = get_regime_prior(symbol, horizon_s)
    _, glob_n = get_global_prior(symbol, horizon_s)
    return int(reg_n) if int(reg_n) > 0 else int(glob_n)


def _passes_quality_gates(symbol: str, horizon_s: int) -> bool:
    if _support_n(symbol, horizon_s) < MIN_SUPPORT_N:
        return False

    v = _get_validation_row(symbol, horizon_s)
    if not v:
        return False

    _, rmse, n = v
    return n >= MIN_VALIDATION_N and rmse <= MAX_RMSE


def _cooldown_ms(sev: str) -> int:
    return {
        "WARN": COOLDOWN_WARN_S,
        "HIGH": COOLDOWN_HIGH_S,
        "CRIT": COOLDOWN_CRIT_S,
    }.get(sev.upper(), 0) * 1000


def _passes_rate_limit(symbol: str, now_ms: int) -> bool:
    """
    Hard cap on alert volume to prevent runaway alert storms.
    Fail-closed if DB queries fail so a degraded DB cannot turn into
    a notification spam source.
    """
    try:
        win_ms = int(ALERT_RATE_WINDOW_S) * 1000
        since_ms = int(now_ms) - int(win_ms)

        con = connect()
        try:
            # Global
            row = con.execute(
                "SELECT COUNT(*) FROM alerts WHERE ts_ms >= ?",
                (since_ms,),
            ).fetchone()
            global_n = int(row[0] or 0) if row else 0
            if global_n >= int(ALERT_MAX_PER_WINDOW_GLOBAL):
                return False

            # Per-symbol
            row = con.execute(
                "SELECT COUNT(*) FROM alerts WHERE ts_ms >= ? AND symbol = ?",
                (since_ms, str(symbol)),
            ).fetchone()
            sym_n = int(row[0] or 0) if row else 0
            if sym_n >= int(ALERT_MAX_PER_WINDOW_PER_SYMBOL):
                return False

            return True
        finally:
            try:
                con.close()
            except Exception as e:
                _warn("alerts.rate_limit.close", e, symbol=str(symbol))
    except Exception as e:
        _warn("alerts.rate_limit", e, symbol=str(symbol), now_ms=int(now_ms))
        return False


def _passes_cooldown(symbol: str, horizon_s: int, severity: str, now_ms: int) -> bool:
    cutoff = now_ms - _cooldown_ms(severity)
    if cutoff <= 0:
        return True

    con = connect()
    try:
        row = con.execute(
            """
            SELECT severity
            FROM alerts
            WHERE symbol=? AND horizon_s=? AND ts_ms>=?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (symbol, horizon_s, cutoff),
        ).fetchone()
    finally:
        con.close()

    if not row:
        return True

    return severity_rank(row[0]) < severity_rank(severity)

# ------            -- ------------------------------------------------------
# Rule selection
# ------            -- ------------------------------------------------------

def choose_rule(
    expected_z: float,
    conf: float,
    symbol: str,
    horizon_s: int,
    rules: Optional[List[Dict]] = None,
    regime: Optional[str] = None,
) -> Optional[Dict]:
    rules = rules or DEFAULT_RULES
    reg = str(regime or "").strip() or get_current_regime(symbol) or "MID"
    mult = REGIME_Z_MULT.get(reg, 1.0)

    az = abs(expected_z)
    best = None
    for r in rules:
        # Pick the highest-severity rule that still clears the regime-adjusted
        # z threshold so downstream routing has a single canonical alert level.
        if az >= r["min_abs_z"] * mult and conf >= r["min_conf"]:
            if not best or severity_rank(r["severity"]) > severity_rank(best["severity"]):
                best = dict(r)
                best["regime"] = reg
                best["min_abs_z_resolved"] = r["min_abs_z"] * mult
    return best


def emit_runtime_alert(
    *,
    event_title: str,
    symbol: str,
    severity: str,
    rule_id: str,
    horizon_s: int = 0,
    expected_z: float = 0.0,
    confidence: float = 1.0,
    explain: Optional[Dict] = None,
    detail: Optional[Dict] = None,
    source: str = "runtime",
    dedupe_scope: str = "",
    ts_ms: Optional[int] = None,
    con=None,
    return_details: bool = False,
):
    if con is None:
        init_alerts_db()

    now_ms = int(ts_ms if ts_ms is not None else int(time.time() * 1000))
    severity_u = str(severity or "WARN").strip().upper() or "WARN"
    explain_obj = dict(explain or {})
    detail_obj = dict(detail or {})

    dedupe_bucket = int(now_ms // max(1, int(ALERT_DEDUPE_WINDOW_S) * 1000))
    dedupe_bits = [
        str(symbol or "").strip().upper() or "SYSTEM",
        str(int(horizon_s or 0)),
        str(rule_id or "").strip() or "runtime_alert",
        str(dedupe_scope or severity_u).strip() or severity_u,
        str(dedupe_bucket),
    ]
    dedupe_key = ":".join(dedupe_bits)

    caller_owned_con = con is not None
    write_con = con if caller_owned_con else connect()
    owns_txn = False
    alert_id = None
    inserted = False
    payload = {
        "alert_id": None,
        "event_title": str(event_title or ""),
        "symbol": str(symbol or "").strip().upper() or "SYSTEM",
        "horizon_s": int(horizon_s or 0),
        "expected_z": float(expected_z or 0.0),
        "confidence": float(confidence or 0.0),
        "severity": str(severity_u),
        "rule_id": str(rule_id or "").strip(),
        "source": str(source or "runtime").strip() or "runtime",
        "explain": explain_obj,
        "detail": detail_obj,
        "ts_ms": int(now_ms),
    }
    try:
        if not caller_owned_con and bool(getattr(write_con, "in_transaction", False)):
            write_con.rollback()
        if not caller_owned_con and not bool(getattr(write_con, "in_transaction", False)):
            _begin_managed_write(write_con)
            owns_txn = True

        cols = [
            "ts_ms",
            "event_id",
            "event_title",
            "symbol",
            "horizon_s",
            "expected_z",
            "confidence",
            "severity",
            "rule_id",
            "explain_json",
            "dedupe_key",
        ]
        values = [
            int(now_ms),
            None,
            str(event_title or ""),
            str(symbol or "").strip().upper() or "SYSTEM",
            int(horizon_s or 0),
            float(expected_z or 0.0),
            float(confidence or 0.0),
            str(severity_u),
            str(rule_id or "").strip(),
            json.dumps(explain_obj, separators=(",", ":"), sort_keys=True),
            str(dedupe_key),
        ]

        if _has_column(write_con, "alerts", "title"):
            cols.append("title")
            values.append(str(event_title or ""))
        if _has_column(write_con, "alerts", "message"):
            cols.append("message")
            values.append(json.dumps(detail_obj, separators=(",", ":"), sort_keys=True))
        if _has_column(write_con, "alerts", "source"):
            cols.append("source")
            values.append(str(source or "runtime").strip() or "runtime")
        if _has_column(write_con, "alerts", "status"):
            cols.append("status")
            values.append("open")
        if _has_column(write_con, "alerts", "detail_json"):
            cols.append("detail_json")
            values.append(json.dumps(detail_obj, separators=(",", ":"), sort_keys=True))
        if _has_column(write_con, "alerts", "updated_ts_ms"):
            cols.append("updated_ts_ms")
            values.append(int(now_ms))

        cur = write_con.execute(
            f"""
            INSERT OR IGNORE INTO alerts ({", ".join(cols)})
            VALUES ({", ".join(["?"] * len(cols))})
            """,
            tuple(values),
        )
        inserted = int(getattr(cur, "rowcount", 0) or 0) > 0
        if inserted:
            try:
                alert_id = int(cur.lastrowid or 0)
            except Exception as e:
                _warn(
                    "alerts.emit_runtime_alert.lastrowid",
                    e,
                    symbol=str(symbol),
                    horizon_s=int(horizon_s),
                    dedupe_key=str(dedupe_key),
                    rule_id=str(rule_id),
                )
                alert_id = None
        if alert_id is None:
            row = write_con.execute(
                "SELECT id FROM alerts WHERE dedupe_key=?",
                (str(dedupe_key),),
            ).fetchone()
            if row and row[0] is not None:
                try:
                    alert_id = int(row[0])
                except Exception as e:
                    _warn(
                        "alerts.emit_runtime_alert.lookup_alert_id",
                        e,
                        symbol=str(symbol),
                        horizon_s=int(horizon_s),
                        dedupe_key=str(dedupe_key),
                        rule_id=str(rule_id),
                    )
                    alert_id = None
        payload["alert_id"] = int(alert_id) if alert_id is not None else None
        if owns_txn:
            write_con.commit()
            cache_invalidate_namespace("alerts")
            cache_invalidate_namespace("api_read", prefix="alerts")
    except Exception:
        if owns_txn:
            try:
                write_con.rollback()
            except Exception as rollback_err:
                _warn(
                    "alerts.emit_runtime_alert.rollback",
                    rollback_err,
                    symbol=str(symbol),
                    horizon_s=int(horizon_s),
                    owns_txn=bool(owns_txn),
                    rule_id=str(rule_id),
                )
        raise
    finally:
        if con is None:
            if bool(getattr(write_con, "in_transaction", False)):
                try:
                    write_con.rollback()
                except Exception as rollback_err:
                    _warn(
                        "alerts.emit_runtime_alert.finally_rollback",
                        rollback_err,
                        symbol=str(symbol),
                        horizon_s=int(horizon_s),
                        owns_txn=bool(owns_txn),
                        rule_id=str(rule_id),
                    )

    try:
        if inserted and owns_txn:
            try:
                publish_event("risk.alert", payload)
            except Exception as e:
                _warn(
                    "alerts.emit_runtime_alert.publish_event",
                    e,
                    symbol=str(symbol),
                    alert_id=alert_id,
                    rule_id=str(rule_id),
                )
        if return_details:
            return {
                "alert_id": (int(alert_id) if alert_id is not None else None),
                "inserted": bool(inserted),
                "payload": (payload if inserted else None),
            }
        return alert_id
    finally:
        if con is None:
            from engine.runtime import storage as _storage

            _storage.close_pooled_connections()

# ------            -- ------------------------------------------------------
# Emit alert
# ------            -- ------------------------------------------------------
def emit_alert(
    *,
    event_id: Optional[int] = None,
    event_title: str,
    symbol: str,
    horizon_s: int,
    expected_z: float,
    confidence: float,
    explain: Optional[Dict] = None,
    rules: Optional[List[Dict]] = None,
    con=None,
    return_details: bool = False,
):
    if con is None:
        init_alerts_db()
    explain = explain or {}
    now_ms = int(time.time() * 1000)
    _attach_market_regime(symbol, now_ms, explain)

    # ------------------------------------------------------------
    # Informational: market stress context (read-only)
    # ------------------------------------------------------------
    try:
        from engine.strategy.market_stress import get_market_stress_snapshot
        ms = get_market_stress_snapshot(ts_ms=now_ms) or {}
        explain["market_stress"] = {
            "score": float(ms.get("stress_score", 0.0)),
            "explain": (
                "elevated market stress"
                if float(ms.get("stress_score", 0.0)) >= 0.7
                else "normal market stress"
            ),
        }
    except Exception as e:
        _warn("alerts.market_stress", e, symbol=str(symbol), horizon_s=int(horizon_s))
        explain["market_stress"] = {
            "score": 0.0,
            "explain": "unavailable",
        }

    # Stale prices do not block alert generation outright, but they do degrade
    # confidence so stale market data naturally suppresses borderline alerts.
    try:
        read_con = connect()
        row = read_con.execute(
            "SELECT meta_json FROM symbols WHERE symbol=?",
            (symbol,),
        ).fetchone()
        read_con.close()

        if row and row[0]:
            meta = json.loads(row[0])
            ps = meta.get("price_status") or {}
            if ps.get("stale"):
                decay = float(os.environ.get("STALE_PRICE_CONF_DECAY", "0.65"))
                confidence *= decay
                explain["confidence_decay"] = {
                    "reason": "price_stale",
                    "multiplier": decay,
                }
    except Exception as e:
        _warn("alerts.confidence_decay", e, symbol=str(symbol))

    # ------------------------------------------------------------
    # Execution-cost net-edge filter (opt-in)
    # ------------------------------------------------------------
    # This is the last gate before persistence. If estimated costs fully erase
    # the edge, we drop the alert instead of emitting a signal operators
    # cannot realistically trade.
    try:
        adj = adjust_expected_z_for_costs(
            symbol=str(symbol),
            horizon_s=int(horizon_s),
            expected_z=float(expected_z),
            side=1,
        )
        if adj:
            explain["exec_cost_filter"] = {
                "cost_bps": float(adj.get("cost_bps", 0.0)),
                "cost_z": float(adj.get("cost_z", 0.0)),
                "vol_step": float(adj.get("vol_step", 0.0)),
                "vol_horizon": float(adj.get("vol_horizon", 0.0)),
            }
            
            ez_adj = adj.get("expected_z_adj", None)
            # If helper signals rejection it returns NaN
            if ez_adj is not None and ez_adj == ez_adj:
                expected_z = float(ez_adj)
            else:
                explain["exec_cost_reject"] = True
                if ALERT_LOG_COST_REJECTS:
                    try:
                        logging.info(
                            "exec_cost_reject symbol=%s horizon_s=%s expected_z=%s conf=%s",
                            str(symbol),
                            str(horizon_s),
                            str(expected_z),
                            str(confidence),
                        )
                    except Exception as e:
                        _warn("alerts.exec_cost_reject_log", e, symbol=str(symbol), horizon_s=int(horizon_s))
                return None

    except Exception as e:
        _warn("alerts.exec_cost_filter", e, symbol=str(symbol), horizon_s=int(horizon_s))

    signal_regime = _extract_signal_regime(symbol, explain)
    rule = choose_rule(expected_z, confidence, symbol, horizon_s, rules, regime=signal_regime)
    if not rule:
        return None

    caller_owned_con = con is not None
    write_con = con if caller_owned_con else connect()
    owns_txn = False
    alert_id = None
    inserted = False
    model_name = _extract_signal_model_name(explain)
    model_id = _extract_signal_model_id(explain)
    model_version = _extract_signal_model_version(explain)
    try:
        if not caller_owned_con and bool(getattr(write_con, "in_transaction", False)):
            write_con.rollback()
        if not caller_owned_con and not bool(getattr(write_con, "in_transaction", False)):
            _begin_managed_write(write_con)
            owns_txn = True
        alerts_has_prediction_id = _has_column(write_con, "alerts", "prediction_id")
        prediction_id = (
            _resolve_prediction_id_for_alert(
                write_con,
                event_id=event_id,
                symbol=str(symbol),
                horizon_s=int(horizon_s),
                model_id=model_id,
                model_name=model_name,
            )
            if alerts_has_prediction_id
            else None
        )
        # Dedupe is bucketed by symbol/horizon/rule so repeated evaluations of
        # the same condition collapse into one alert record per window.
        dedupe_bucket = int(now_ms // max(1, int(ALERT_DEDUPE_WINDOW_S) * 1000))
        dedupe_key = f"{symbol}:{horizon_s}:{rule['rule_id']}:{dedupe_bucket}"
        if alerts_has_prediction_id:
            cur = write_con.execute(
                """
                INSERT OR IGNORE INTO alerts
                (ts_ms, event_id, prediction_id, event_title, symbol, horizon_s, expected_z, confidence,
                 severity, rule_id, explain_json, dedupe_key, model_name, model_id, model_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_ms,
                    (int(event_id) if event_id is not None else None),
                    (int(prediction_id) if prediction_id is not None else None),
                    event_title,
                    symbol,
                    horizon_s,
                    float(expected_z),
                    float(confidence),
                    rule["severity"],
                    rule["rule_id"],
                    json.dumps(explain, separators=(",", ":"), sort_keys=True),
                    dedupe_key,
                    model_name,
                    model_id,
                    model_version,
                ),
            )
        else:
            cur = write_con.execute(
                """
                INSERT OR IGNORE INTO alerts
                (ts_ms, event_id, event_title, symbol, horizon_s, expected_z, confidence,
                 severity, rule_id, explain_json, dedupe_key, model_name, model_id, model_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_ms,
                    (int(event_id) if event_id is not None else None),
                    event_title,
                    symbol,
                    horizon_s,
                    float(expected_z),
                    float(confidence),
                    rule["severity"],
                    rule["rule_id"],
                    json.dumps(explain, separators=(",", ":"), sort_keys=True),
                    dedupe_key,
                    model_name,
                    model_id,
                    model_version,
                ),
            )
        inserted = int(getattr(cur, "rowcount", 0) or 0) > 0
        if inserted:
            try:
                alert_id = int(cur.lastrowid or 0)
            except Exception as e:
                _warn(
                    "alerts.emit_alert.lastrowid",
                    e,
                    symbol=str(symbol),
                    horizon_s=int(horizon_s),
                    dedupe_key=str(dedupe_key),
                )
                alert_id = None
        if alert_id is None:
            row = write_con.execute(
                "SELECT id FROM alerts WHERE dedupe_key=?",
                (str(dedupe_key),),
            ).fetchone()
            if row and row[0] is not None:
                try:
                    alert_id = int(row[0])
                except Exception as e:
                    _warn(
                        "alerts.emit_alert.lookup_alert_id",
                        e,
                        symbol=str(symbol),
                        horizon_s=int(horizon_s),
                        dedupe_key=str(dedupe_key),
                    )
                    alert_id = None
        if owns_txn:
            write_con.commit()
            cache_invalidate_namespace("alerts")
            cache_invalidate_namespace("api_read", prefix="alerts")
    except Exception:
        if owns_txn:
            try:
                write_con.rollback()
            except Exception as rollback_err:
                _warn(
                    "alerts.emit_alert.rollback",
                    rollback_err,
                    symbol=str(symbol),
                    horizon_s=int(horizon_s),
                    owns_txn=bool(owns_txn),
                )
        raise
    finally:
        if con is None:
            if bool(getattr(write_con, "in_transaction", False)):
                try:
                    write_con.rollback()
                except Exception as rollback_err:
                    _warn(
                        "alerts.emit_alert.finally_rollback",
                        rollback_err,
                        symbol=str(symbol),
                        horizon_s=int(horizon_s),
                        owns_txn=bool(owns_txn),
                    )

    try:
        payload = build_strategy_signal_payload(
            alert_id=alert_id,
            event_id=event_id,
            event_title=event_title,
            symbol=symbol,
            horizon_s=horizon_s,
            expected_z=expected_z,
            confidence=confidence,
            explain=explain,
            ts_ms=now_ms,
        )

        if inserted and owns_txn:
            try:
                publish_event("strategy_signal", payload)
            except Exception as e:
                _warn("alerts.publish_event", e, symbol=str(symbol), alert_id=alert_id)

        if return_details:
            return {
                "alert_id": (int(alert_id) if alert_id is not None else None),
                "inserted": bool(inserted),
                "payload": (payload if inserted else None),
            }
        return alert_id
    finally:
        if con is None:
            from engine.runtime import storage as _storage

            _storage.close_pooled_connections()

# ------------------------------------------------------------
# Query
# ------------------------------------------------------------


def get_recent_alerts(limit: int = 50):
    init_alerts_db()
    limit_n = int(limit)

    def _load():
        con = connect()
        try:
            return con.execute(
                """
                SELECT ts_ms, severity, rule_id, event_title,
                       symbol, horizon_s, expected_z, confidence, explain_json
                FROM alerts
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (limit_n,),
            ).fetchall()
        finally:
            con.close()

    return cache_get_or_load("alerts", f"recent:{limit_n}", _load, ttl_s=0.75)
