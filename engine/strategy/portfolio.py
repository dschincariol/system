"""
FILE: portfolio.py

Human-readable purpose:
Core portfolio-construction layer. It reads alerts and current portfolio state,
filters them through portfolio/risk rules, and produces target positions and
intent-level rebalance outputs without directly routing broker orders.

Portfolio / Strategy layer (paper-trading / intent only)

Reads recent ALERTS (already quality-gated) and produces:
- target positions (weights)
- order intents (delta from current state)

Design goals:
- Minimal & production-safe
- No broker execution
- Explainable decisions
- Anti-flip-flop: min hold time before reversing
"""

import json
import os
import time
import math
import logging
import random
import threading
from typing import Dict, List, Optional, Tuple, Any

from engine.runtime.storage import connect, connect_rw_direct, init_db
from engine.runtime.failure_diagnostics import log_failure
from engine.strategy.strategy_selector import choose_strategy_name, load_strategy_module
from engine.strategy.black_litterman import (
    black_litterman_posterior,
    build_view_matrix,
    compute_equilibrium_returns,
)
from engine.strategy.confidence_engine import describe_signal_confidence
from engine.data.universe import get_active_symbols
from engine.strategy.symbol_blacklist import is_blacklisted
from engine.strategy.portfolio_risk_gate import apply_portfolio_risk_gate
from engine.risk.portfolio_risk_engine import apply_portfolio_risk_engine
from engine.risk.monte_carlo_risk_engine import request_monte_carlo_refresh
from engine.runtime.risk_state import get_state
from engine.runtime.factor_universe import _get_feature_asof as _get_factor_feature_asof
from engine.strategy.model_intent import is_canonical_model_intent

LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()
_PORTFOLIO_DB_INIT_LOCK = threading.Lock()
_PORTFOLIO_DB_INIT_RETRY_ATTEMPTS = max(
    1,
    int(os.environ.get("PORTFOLIO_DB_INIT_RETRY_ATTEMPTS", os.environ.get("SQLITE_WRITE_RETRY_ATTEMPTS", "5"))),
)
_PORTFOLIO_DB_INIT_RETRY_BASE_MS = max(
    25,
    int(os.environ.get("PORTFOLIO_DB_INIT_RETRY_BASE_MS", os.environ.get("SQLITE_WRITE_RETRY_BASE_MS", "150"))),
)
_PORTFOLIO_DB_INIT_RETRY_MAX_MS = max(
    _PORTFOLIO_DB_INIT_RETRY_BASE_MS,
    int(os.environ.get("PORTFOLIO_DB_INIT_RETRY_MAX_MS", os.environ.get("SQLITE_WRITE_RETRY_MAX_MS", "2000"))),
)
_PORTFOLIO_DB_INIT_BUSY_TIMEOUT_MS = max(
    250,
    int(os.environ.get("PORTFOLIO_DB_INIT_BUSY_TIMEOUT_MS", os.environ.get("SQLITE_BUSY_TIMEOUT_MS", "5000"))),
)


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.portfolio",
        extra=extra or None,
        include_health=False,
        persist=False,
    )


def _is_sqlite_busy_error(error: BaseException | None) -> bool:
    message = str(error or "").strip().lower()
    return (
        "database is locked" in message
        or "database busy" in message
        or "database table is locked" in message
    )


def _table_exists(con, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ANY (current_schemas(false))
          AND table_name=?
        LIMIT 1
        """,
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _prediction_exists(con, prediction_id: Optional[int]) -> bool:
    if prediction_id in (None, "") or not _table_exists(con, "predictions"):
        return False
    row = con.execute(
        "SELECT 1 FROM predictions WHERE id=? LIMIT 1",
        (int(prediction_id),),
    ).fetchone()
    return bool(row)


def _validated_prediction_id(con, prediction_id: Any) -> Optional[int]:
    if prediction_id in (None, ""):
        return None
    try:
        value = int(prediction_id)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_PREDICTION_ID_PARSE_FAILED",
            e,
            once_key="portfolio_prediction_id_parse_failed",
            prediction_id=prediction_id,
        )
        return None
    return value if _prediction_exists(con, value) else None


def _alert_prediction_lineage(con, source_alert_id: Any) -> tuple[bool, Optional[int]]:
    try:
        alert_id = int(source_alert_id) if source_alert_id not in (None, "") else None
    except Exception:
        alert_id = None
    if alert_id is None or not _table_exists(con, "alerts"):
        return False, None
    alert_cols = {
        str(r[1]).strip().lower(): r
        for r in (con.execute("PRAGMA table_info(alerts)").fetchall() or [])
    }
    if "prediction_id" not in alert_cols:
        row = con.execute(
            "SELECT 1 FROM alerts WHERE id=? LIMIT 1",
            (int(alert_id),),
        ).fetchone()
        return bool(row), None
    row = con.execute(
        "SELECT prediction_id FROM alerts WHERE id=? LIMIT 1",
        (int(alert_id),),
    ).fetchone()
    if not row:
        return False, None
    return True, _validated_prediction_id(con, row[0])


def _resolve_portfolio_order_prediction_id(
    con,
    *,
    source_alert_id: Any,
    prediction_id: Any,
) -> Optional[int]:
    alert_found, alert_prediction_id = _alert_prediction_lineage(con, source_alert_id)
    if alert_found:
        return alert_prediction_id
    return _validated_prediction_id(con, prediction_id)


def _backfill_portfolio_order_prediction_ids(con) -> None:
    if not _table_exists(con, "portfolio_orders"):
        return
    cols = {
        str(r[1]).strip().lower(): r
        for r in (con.execute("PRAGMA table_info(portfolio_orders)").fetchall() or [])
    }
    if "prediction_id" not in cols:
        return
    if _table_exists(con, "alerts"):
        alert_cols = {
            str(r[1]).strip().lower(): r
            for r in (con.execute("PRAGMA table_info(alerts)").fetchall() or [])
        }
        if "prediction_id" in alert_cols:
            con.execute(
                """
                UPDATE portfolio_orders
                SET prediction_id = (
                  SELECT a.prediction_id
                  FROM alerts a
                  WHERE a.id = portfolio_orders.source_alert_id
                  LIMIT 1
                )
                WHERE prediction_id IS NULL
                  AND source_alert_id IS NOT NULL
                  AND EXISTS(
                    SELECT 1
                    FROM alerts a
                    WHERE a.id = portfolio_orders.source_alert_id
                      AND a.prediction_id IS NOT NULL
                  )
                """
            )
            con.execute(
                """
                UPDATE portfolio_orders
                SET prediction_id = (
                  SELECT a.prediction_id
                  FROM alerts a
                  WHERE a.id = portfolio_orders.source_alert_id
                  LIMIT 1
                )
                WHERE source_alert_id IS NOT NULL
                  AND prediction_id IS NOT NULL
                  AND EXISTS(
                    SELECT 1
                    FROM alerts a
                    WHERE a.id = portfolio_orders.source_alert_id
                      AND a.prediction_id IS NOT NULL
                      AND a.prediction_id <> portfolio_orders.prediction_id
                  )
                """
            )
            con.execute(
                """
                UPDATE portfolio_orders
                SET prediction_id = NULL
                WHERE source_alert_id IS NOT NULL
                  AND prediction_id IS NOT NULL
                  AND EXISTS(
                    SELECT 1
                    FROM alerts a
                    WHERE a.id = portfolio_orders.source_alert_id
                      AND a.prediction_id IS NULL
                  )
                """
            )
    if _table_exists(con, "predictions"):
        con.execute(
            """
            UPDATE portfolio_orders
            SET prediction_id = NULL
            WHERE prediction_id IS NOT NULL
              AND NOT EXISTS(
                SELECT 1
                FROM predictions p
                WHERE p.id = portfolio_orders.prediction_id
              )
            """
        )
    else:
        con.execute("UPDATE portfolio_orders SET prediction_id = NULL WHERE prediction_id IS NOT NULL")


def _apply_portfolio_db_schema(con) -> None:
    con.executescript(SCHEMA)
    cols = {
        str(r[1]).strip().lower(): r
        for r in (con.execute("PRAGMA table_info(portfolio_orders)").fetchall() or [])
    }
    sql_row = con.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='table' AND name='portfolio_orders'
        """
    ).fetchone()
    order_sql = str(sql_row[0] or "") if sql_row else ""
    order_sql_sig = order_sql.upper().replace(" ", "").replace("\n", "")
    predictions_available = _table_exists(con, "predictions")
    alerts_available = _table_exists(con, "alerts")
    alert_cols = {}
    if alerts_available:
        alert_cols = {
            str(r[1]).strip().lower(): r
            for r in (con.execute("PRAGMA table_info(alerts)").fetchall() or [])
        }
    alert_prediction_lineage_available = alerts_available and ("prediction_id" in alert_cols)
    if alert_prediction_lineage_available and predictions_available:
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_alerts_id_prediction_lineage
              ON alerts(id, prediction_id)
            """
        )
    is_postgres = hasattr(con, "raw")
    if is_postgres and "prediction_id" not in cols:
        con.execute("ALTER TABLE portfolio_orders ADD COLUMN prediction_id BIGINT")
        cols = {
            str(r[1]).strip().lower(): r
            for r in (con.execute("PRAGMA table_info(portfolio_orders)").fetchall() or [])
        }
    if not is_postgres and ("prediction_id" not in cols or (
        predictions_available
        and "FOREIGNKEY(PREDICTION_ID)REFERENCESPREDICTIONS(ID)ONDELETESETNULL" not in order_sql_sig
    ) or (
        alert_prediction_lineage_available
        and predictions_available
        and "FOREIGNKEY(SOURCE_ALERT_ID,PREDICTION_ID)REFERENCESALERTS(ID,PREDICTION_ID)ONDELETESETNULL"
        not in order_sql_sig
    )):
        con.execute("ALTER TABLE portfolio_orders RENAME TO portfolio_orders_legacy_lineage")
        legacy_cols = {
            str(r[1]).strip().lower(): r
            for r in (con.execute("PRAGMA table_info(portfolio_orders_legacy_lineage)").fetchall() or [])
        }

        def _legacy_expr(column_name: str, fallback_sql: str) -> str:
            if column_name in legacy_cols:
                return column_name
            return fallback_sql

        legacy_source_alert_expr = _legacy_expr("source_alert_id", "NULL")
        legacy_prediction_value_expr = _legacy_expr("prediction_id", "NULL")
        legacy_prediction_expr = legacy_prediction_value_expr
        if predictions_available and alert_prediction_lineage_available:
            legacy_prediction_expr = (
                "CASE "
                f"WHEN {legacy_source_alert_expr} IS NOT NULL THEN ("
                "SELECT CASE "
                "WHEN a.prediction_id IS NOT NULL "
                "  AND EXISTS(SELECT 1 FROM predictions p WHERE p.id = a.prediction_id) "
                "THEN a.prediction_id "
                "ELSE NULL END "
                f"FROM alerts a WHERE a.id = {legacy_source_alert_expr} LIMIT 1"
                ") "
                f"WHEN {legacy_prediction_value_expr} IS NOT NULL "
                f"AND EXISTS(SELECT 1 FROM predictions p WHERE p.id = {legacy_prediction_value_expr}) "
                f"THEN {legacy_prediction_value_expr} "
                "ELSE NULL END"
            )
        elif predictions_available:
            legacy_prediction_expr = (
                "CASE "
                f"WHEN {legacy_prediction_value_expr} IS NOT NULL "
                f"AND EXISTS(SELECT 1 FROM predictions p WHERE p.id = {legacy_prediction_value_expr}) "
                f"THEN {legacy_prediction_value_expr} "
                "ELSE NULL END"
            )
        else:
            legacy_prediction_expr = "NULL"

        fk_clauses = []
        if predictions_available:
            fk_clauses.append("FOREIGN KEY(prediction_id) REFERENCES predictions(id) ON DELETE SET NULL")
        if alert_prediction_lineage_available and predictions_available:
            fk_clauses.append(
                "FOREIGN KEY(source_alert_id, prediction_id) REFERENCES alerts(id, prediction_id) ON DELETE SET NULL"
            )
        fk_clause = ""
        if fk_clauses:
            fk_clause = ",\n              " + ",\n              ".join(fk_clauses)

        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS portfolio_orders (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              model_id TEXT NOT NULL DEFAULT 'baseline',
              symbol TEXT NOT NULL,
              action TEXT NOT NULL,
              from_side TEXT NOT NULL,
              to_side TEXT NOT NULL,
              from_weight REAL NOT NULL,
              to_weight REAL NOT NULL,
              delta_weight REAL NOT NULL,
              source_alert_id INTEGER,
              prediction_id INTEGER,
              explain_json TEXT{fk_clause}
            )
            """
        )
        con.execute(
            f"""
            INSERT INTO portfolio_orders(
              id, ts_ms, model_id, symbol, action, from_side, to_side,
              from_weight, to_weight, delta_weight, source_alert_id, prediction_id, explain_json
            )
            SELECT
              id,
              {_legacy_expr("ts_ms", "0")},
              COALESCE(NULLIF(TRIM({_legacy_expr("model_id", "'baseline'")}), ''), 'baseline'),
              {_legacy_expr("symbol", "''")},
              {_legacy_expr("action", "''")},
              {_legacy_expr("from_side", "'FLAT'")},
              {_legacy_expr("to_side", "'FLAT'")},
              {_legacy_expr("from_weight", "0.0")},
              {_legacy_expr("to_weight", "0.0")},
              {_legacy_expr("delta_weight", "0.0")},
              {_legacy_expr("source_alert_id", "NULL")},
              {legacy_prediction_expr},
              {_legacy_expr("explain_json", "NULL")}
            FROM portfolio_orders_legacy_lineage
            """
        )
        con.execute("DROP TABLE portfolio_orders_legacy_lineage")
        cols = {
            str(r[1]).strip().lower(): r
            for r in (con.execute("PRAGMA table_info(portfolio_orders)").fetchall() or [])
        }
    if "model_id" not in cols:
        con.execute("ALTER TABLE portfolio_orders ADD COLUMN model_id TEXT NOT NULL DEFAULT 'baseline'")
        con.execute("UPDATE portfolio_orders SET model_id='baseline' WHERE model_id IS NULL OR TRIM(model_id)=''")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_orders_ts ON portfolio_orders(ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_orders_model_ts ON portfolio_orders(model_id, ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_orders_symbol_ts ON portfolio_orders(symbol, ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_orders_source_alert_ts ON portfolio_orders(source_alert_id, ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_orders_prediction_ts ON portfolio_orders(prediction_id, ts_ms)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_orders_source_alert_prediction_ts ON portfolio_orders(source_alert_id, prediction_id, ts_ms)"
    )
    con.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_orders_id_source_prediction_lineage ON portfolio_orders(id, source_alert_id, prediction_id)"
    )
    _backfill_portfolio_order_prediction_ids(con)

    state_sql_row = con.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='table' AND name='portfolio_state'
        """
    ).fetchone()
    state_sql = str(state_sql_row[0] or "") if state_sql_row else ""
    state_cols = {
        str(r[1]).strip().lower(): r
        for r in (con.execute("PRAGMA table_info(portfolio_state)").fetchall() or [])
    }
    if is_postgres and "model_id" not in state_cols:
        con.execute("ALTER TABLE portfolio_state ADD COLUMN model_id TEXT NOT NULL DEFAULT 'baseline'")
        con.execute("UPDATE portfolio_state SET model_id='baseline' WHERE model_id IS NULL OR TRIM(model_id)=''")
        state_cols = {
            str(r[1]).strip().lower(): r
            for r in (con.execute("PRAGMA table_info(portfolio_state)").fetchall() or [])
        }
    if not is_postgres and ("model_id" not in state_cols or "PRIMARY KEY (model_id, symbol)" not in state_sql):
        con.execute("ALTER TABLE portfolio_state RENAME TO portfolio_state_legacy_model_id")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_state (
              model_id TEXT NOT NULL DEFAULT 'baseline',
              symbol TEXT NOT NULL,
              side TEXT NOT NULL,
              weight REAL NOT NULL,
              opened_ts_ms INTEGER NOT NULL,
              updated_ts_ms INTEGER NOT NULL,
              source_alert_id INTEGER,
              explain_json TEXT,
              PRIMARY KEY (model_id, symbol)
            )
            """
        )
        legacy_cols = {
            str(r[1]).strip().lower()
            for r in (con.execute("PRAGMA table_info(portfolio_state_legacy_model_id)").fetchall() or [])
        }
        if "model_id" in legacy_cols:
            con.execute(
                """
                INSERT OR REPLACE INTO portfolio_state(
                  model_id, symbol, side, weight, opened_ts_ms, updated_ts_ms, source_alert_id, explain_json
                )
                SELECT
                  COALESCE(NULLIF(TRIM(model_id), ''), 'baseline'),
                  symbol, side, weight, opened_ts_ms, updated_ts_ms, source_alert_id, explain_json
                FROM portfolio_state_legacy_model_id
                """
            )
        else:
            con.execute(
                """
                INSERT OR REPLACE INTO portfolio_state(
                  model_id, symbol, side, weight, opened_ts_ms, updated_ts_ms, source_alert_id, explain_json
                )
                SELECT
                  'baseline',
                  symbol, side, weight, opened_ts_ms, updated_ts_ms, source_alert_id, explain_json
                FROM portfolio_state_legacy_model_id
                """
            )
        con.execute("DROP TABLE portfolio_state_legacy_model_id")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_portfolio_state_updated_ts ON portfolio_state(updated_ts_ms)"
        )
    con.commit()

# -----------------------------
# Strategy controls (env)
# -----------------------------

# Only consider alerts within lookback window
PORTFOLIO_LOOKBACK_S = int(os.environ.get("PORTFOLIO_LOOKBACK_S", "21600"))  # 6h

# Must pass these to be tradable
PORTFOLIO_MIN_CONF = float(os.environ.get("PORTFOLIO_MIN_CONF", "0.55"))
PORTFOLIO_MIN_ABS_Z = float(os.environ.get("PORTFOLIO_MIN_ABS_Z", "0.75"))

# Max number of concurrent positions
PORTFOLIO_MAX_POSITIONS = int(os.environ.get("PORTFOLIO_MAX_POSITIONS", "3"))
PORTFOLIO_MODEL_INTENT_MAX_POSITIONS = int(os.environ.get("PORTFOLIO_MODEL_INTENT_MAX_POSITIONS", "0"))
# Dynamic universe promotion gates
UNIVERSE_MIN_SEEN = int(os.environ.get("UNIVERSE_MIN_SEEN", "3"))
UNIVERSE_MIN_PRICE_AGE_S = int(os.environ.get("UNIVERSE_MIN_PRICE_AGE_S", "180"))
UNIVERSE_MIN_VOLUME = float(os.environ.get("UNIVERSE_MIN_VOLUME", "0"))
UNIVERSE_MAX_PROMOTIONS_PER_DAY = int(os.environ.get("UNIVERSE_MAX_PROMOTIONS_PER_DAY", "3"))

# Gross exposure cap (sum of abs weights)
PORTFOLIO_GROSS_CAP = float(os.environ.get("PORTFOLIO_GROSS_CAP", "1.00"))

# -----------------------------
# Capital Preservation Mode (CPM)
# -----------------------------
PORTFOLIO_PRESERVE_GROSS_FACTOR = float(os.environ.get("PORTFOLIO_PRESERVE_GROSS_FACTOR", "0.35"))
PORTFOLIO_PRESERVE_MIN_CONF_ADD = float(os.environ.get("PORTFOLIO_PRESERVE_MIN_CONF_ADD", "0.10"))
PORTFOLIO_PRESERVE_MIN_ABS_Z_ADD = float(os.environ.get("PORTFOLIO_PRESERVE_MIN_ABS_Z_ADD", "0.25"))
PORTFOLIO_PRESERVE_MAX_POSITIONS = int(os.environ.get("PORTFOLIO_PRESERVE_MAX_POSITIONS", "1"))
PORTFOLIO_PRESERVE_REBALANCE_COOLDOWN_MULT = float(os.environ.get("PORTFOLIO_PRESERVE_REBALANCE_COOLDOWN_MULT", "3.0"))


def _capital_mode() -> str:
    try:
        return str(get_state("capital_mode", "normal") or "normal")
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_CAPITAL_MODE_READ_FAILED", e, once_key="capital_mode_read")
        capital_mode = "normal"
        return capital_mode


def _eff_min_conf() -> float:
    v = float(PORTFOLIO_MIN_CONF)
    if _capital_mode() == "preserve":
        v = min(0.99, v + float(PORTFOLIO_PRESERVE_MIN_CONF_ADD))
    return float(v)


def _eff_min_abs_z() -> float:
    v = float(PORTFOLIO_MIN_ABS_Z)
    if _capital_mode() == "preserve":
        v = v + float(PORTFOLIO_PRESERVE_MIN_ABS_Z_ADD)
    return float(v)


def _eff_max_positions() -> int:
    v = int(PORTFOLIO_MAX_POSITIONS)
    if _capital_mode() == "preserve":
        v = int(max(0, int(PORTFOLIO_PRESERVE_MAX_POSITIONS)))
    return int(v)


def _eff_gross_cap() -> float:
    v = float(PORTFOLIO_GROSS_CAP)
    if _capital_mode() == "preserve":
        v = float(v) * float(max(0.0, min(1.0, float(PORTFOLIO_PRESERVE_GROSS_FACTOR))))
    return float(v)


def _eff_rebalance_cooldown_s() -> int:
    # NOTE: do NOT reference PORTFOLIO_REBALANCE_COOLDOWN_S here because that constant
    # is defined later in the file (import-time NameError). Read env directly.
    v = int(os.environ.get("PORTFOLIO_REBALANCE_COOLDOWN_S", "60"))
    if _capital_mode() == "preserve":
        try:
            v = int(float(v) * float(max(1.0, float(PORTFOLIO_PRESERVE_REBALANCE_COOLDOWN_MULT))))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_REBALANCE_COOLDOWN_SCALE_FAILED", e, once_key="rebalance_cooldown_scale")
    return int(v)

# -----------------------------
# Stress / regime risk gate (opt-in)
# -----------------------------
# Compress exposure under elevated stress (e.g., VIX regime).
PORTFOLIO_USE_STRESS_GATE = os.environ.get("PORTFOLIO_USE_STRESS_GATE", "0") == "1"

# Trigger on VIX z-score vs trailing window (computed from prices where symbol='VIX')
PORTFOLIO_STRESS_VIX_Z_TH = float(os.environ.get("PORTFOLIO_STRESS_VIX_Z_TH", "1.25"))

# When above threshold, linearly compress down to min factor
PORTFOLIO_STRESS_MIN_FACTOR = float(os.environ.get("PORTFOLIO_STRESS_MIN_FACTOR", "0.35"))
PORTFOLIO_STRESS_Z_AT_MIN = float(os.environ.get("PORTFOLIO_STRESS_Z_AT_MIN", "3.0"))

# -----------------------------
# Social manipulation / attention gate (opt-in)
# -----------------------------
PORTFOLIO_USE_SOCIAL_GATE = os.environ.get("PORTFOLIO_USE_SOCIAL_GATE", "0") == "1"
PORTFOLIO_SOCIAL_BUCKET_SEC = int(os.environ.get("PORTFOLIO_SOCIAL_BUCKET_SEC", "300"))
PORTFOLIO_SOCIAL_MANIP_BLOCK_TH = float(os.environ.get("PORTFOLIO_SOCIAL_MANIP_BLOCK_TH", "0.85"))
PORTFOLIO_SOCIAL_ATTEN_SHOCK_TH = float(os.environ.get("PORTFOLIO_SOCIAL_ATTEN_SHOCK_TH", "0.80"))
PORTFOLIO_SOCIAL_SHOCK_FACTOR = float(os.environ.get("PORTFOLIO_SOCIAL_SHOCK_FACTOR", "0.60"))

# Optional per-symbol "vol-of-vol" compression (uses price-only proxy)
PORTFOLIO_USE_VOV_GATE = os.environ.get("PORTFOLIO_USE_VOV_GATE", "0") == "1"
PORTFOLIO_VOV_ALPHA = float(os.environ.get("PORTFOLIO_VOV_ALPHA", "6.0"))  # strength of penalty
PORTFOLIO_VOV_FLOOR = float(os.environ.get("PORTFOLIO_VOV_FLOOR", "0.0"))
PORTFOLIO_VOV_CEIL = float(os.environ.get("PORTFOLIO_VOV_CEIL", "0.020"))

# Impact-aware sizing (uses realized slippage from execution_metrics)
PORTFOLIO_IMPACT_SIZING = os.environ.get("PORTFOLIO_IMPACT_SIZING", "1") == "1"
PORTFOLIO_IMPACT_LOOKBACK_METRICS = int(os.environ.get("PORTFOLIO_IMPACT_LOOKBACK_METRICS", "5000"))
PORTFOLIO_IMPACT_BAD_BPS = float(os.environ.get("PORTFOLIO_IMPACT_BAD_BPS", "25.0"))
PORTFOLIO_IMPACT_FLOOR = float(os.environ.get("PORTFOLIO_IMPACT_FLOOR", "0.25"))

# Capital allocation optimizer (reweights desired based on expected_ret_net / expected_dd)
PORTFOLIO_ALLOC_OPT = os.environ.get("PORTFOLIO_ALLOC_OPT", "1") == "1"
PORTFOLIO_ALLOC_ALPHA = float(os.environ.get("PORTFOLIO_ALLOC_ALPHA", "1.0"))   # return weight
PORTFOLIO_ALLOC_BETA = float(os.environ.get("PORTFOLIO_ALLOC_BETA", "1.0"))     # dd penalty
PORTFOLIO_ALLOC_FLOOR = float(os.environ.get("PORTFOLIO_ALLOC_FLOOR", "0.20"))  # min factor vs original
PORTFOLIO_ALLOC_CEIL = float(os.environ.get("PORTFOLIO_ALLOC_CEIL", "2.00"))    # max factor vs original

# Optional expected-return blending before allocation.
BLACK_LITTERMAN_ENABLED = os.environ.get("BLACK_LITTERMAN_ENABLED", "0") == "1"
BLACK_LITTERMAN_TAU = float(os.environ.get("BLACK_LITTERMAN_TAU", "0.05"))
BLACK_LITTERMAN_VIEW_CONFIDENCE = float(os.environ.get("BLACK_LITTERMAN_VIEW_CONFIDENCE", "0.60"))

# ------------------------------------------------------
# Capital Efficiency Native Weighting
# ------------------------------------------------------
PORTFOLIO_USE_EFFICIENCY_WEIGHTING = os.environ.get("PORTFOLIO_USE_EFFICIENCY_WEIGHTING", "1") == "1"
PORTFOLIO_EFF_ALPHA = float(os.environ.get("PORTFOLIO_EFF_ALPHA", "1.0"))
PORTFOLIO_EFF_DD_PENALTY = float(os.environ.get("PORTFOLIO_EFF_DD_PENALTY", "0.50"))
PORTFOLIO_EFF_FLOOR = float(os.environ.get("PORTFOLIO_EFF_FLOOR", "0.25"))
PORTFOLIO_EFF_CEIL = float(os.environ.get("PORTFOLIO_EFF_CEIL", "2.50"))

# Capital-at-Risk gate (tail-risk budget)
# risk_i = weight_i * expected_dd_i  (expected_dd from tradability block)
PORTFOLIO_CAR_MAX = float(os.environ.get("PORTFOLIO_CAR_MAX", "0.06"))  # max portfolio risk budget
PORTFOLIO_CAR_MAX_PER_SYMBOL = float(os.environ.get("PORTFOLIO_CAR_MAX_PER_SYMBOL", "0.03"))
COMPETITION_CAPITAL_PLAN_MAX_AGE_MS = int(
    os.environ.get("COMPETITION_CAPITAL_PLAN_MAX_AGE_MS", "15000")
)

# Per-symbol weight cap
PORTFOLIO_MAX_W_PER_SYMBOL = float(os.environ.get("PORTFOLIO_MAX_W_PER_SYMBOL", "0.45"))

# Score normalization (weight = gross_cap * score/score_norm)
PORTFOLIO_SCORE_NORM = float(os.environ.get("PORTFOLIO_SCORE_NORM", "3.0"))

# Minimum hold time before allowing reversal
PORTFOLIO_MIN_HOLD_S = int(os.environ.get("PORTFOLIO_MIN_HOLD_S", "1800"))  # 30 min

# Cooldown between rebalances (avoid constant churn)
PORTFOLIO_REBALANCE_COOLDOWN_S = int(os.environ.get("PORTFOLIO_REBALANCE_COOLDOWN_S", "60"))
# stale price block
PORTFOLIO_EXEC_STALE_HALF_FACTOR = float(os.environ.get("PORTFOLIO_EXEC_STALE_HALF_FACTOR", "0.50"))

# stress throttle (Market Stress Score 0..1)
PORTFOLIO_EXEC_STRESS_TH = float(os.environ.get("PORTFOLIO_EXEC_STRESS_TH", "0.75"))
PORTFOLIO_EXEC_STRESS_FACTOR = float(os.environ.get("PORTFOLIO_EXEC_STRESS_FACTOR", "0.60"))

# ------            -- ------------------------------------------------------
# Execution realism sizing (opt-in, recommended)
# - staleness per symbol (price age)
# - global stress proxy (VIX z-score if available)
# - volatility proxy (ATR% from price-only series)
# ------            -- ------------------------------------------------------
PORTFOLIO_USE_EXEC_REALISM = os.environ.get("PORTFOLIO_USE_EXEC_REALISM", "1") == "1"

# If symbol price is older than this, size -> 0 (blocks new exposure via portfolio intents)
PORTFOLIO_EXEC_MAX_PRICE_AGE_S = float(os.environ.get("PORTFOLIO_EXEC_MAX_PRICE_AGE_S", "120"))

# Stress throttle (requires VIX being present in prices as symbol="VIX")
PORTFOLIO_EXEC_VIX_Z_TH = float(os.environ.get("PORTFOLIO_EXEC_VIX_Z_TH", "2.0"))
PORTFOLIO_EXEC_VIX_FACTOR = float(os.environ.get("PORTFOLIO_EXEC_VIX_FACTOR", "0.60"))

# Volatility throttle based on ATR% (price-only proxy)
PORTFOLIO_EXEC_ATR_PCT_TH = float(os.environ.get("PORTFOLIO_EXEC_ATR_PCT_TH", "0.02"))
# Extra slippage estimate (bps) ~ atr_pct * 1e4 * multiplier (audit only; does not change expected_ret here)
PORTFOLIO_EXEC_ATR_SLIP_MULT = float(os.environ.get("PORTFOLIO_EXEC_ATR_SLIP_MULT", "0.25"))

# ------------------------------------------------------
# Execution Regime Sizing (model-aware execution layer)
# ------------------------------------------------------
PORTFOLIO_USE_EXEC_REGIME = os.environ.get("PORTFOLIO_USE_EXEC_REGIME", "1") == "1"

PORTFOLIO_EXEC_SKEW_Z_TH = float(os.environ.get("PORTFOLIO_EXEC_SKEW_Z_TH", "1.5"))
PORTFOLIO_EXEC_FLOW_Z_TH = float(os.environ.get("PORTFOLIO_EXEC_FLOW_Z_TH", "2.0"))

PORTFOLIO_EXEC_STRESS_SIZE_REDUCTION = float(os.environ.get("PORTFOLIO_EXEC_STRESS_SIZE_REDUCTION", "0.35"))
PORTFOLIO_EXEC_EARNINGS_SIZE_REDUCTION = float(os.environ.get("PORTFOLIO_EXEC_EARNINGS_SIZE_REDUCTION", "0.55"))

PORTFOLIO_EXEC_REGIME_FLOOR = float(os.environ.get("PORTFOLIO_EXEC_REGIME_FLOOR", "0.20"))

# Temporal clustering dampener (burst control)
PORTFOLIO_TD_WINDOW_S = int(os.environ.get("PORTFOLIO_TD_WINDOW_S", "1800"))  # 30 min
PORTFOLIO_TD_MAX_SIGNALS = int(os.environ.get("PORTFOLIO_TD_MAX_SIGNALS", "3"))
PORTFOLIO_TD_SCALE = float(os.environ.get("PORTFOLIO_TD_SCALE", "0.65"))  # scale once threshold exceeded

# Correlation controls (avoid redundant bets)
PORTFOLIO_CORR_PRUNE = os.environ.get("PORTFOLIO_CORR_PRUNE", "1") == "1"
PORTFOLIO_CORR_LOOKBACK = int(os.environ.get("PORTFOLIO_CORR_LOOKBACK", "240"))
PORTFOLIO_CORR_MAX = float(os.environ.get("PORTFOLIO_CORR_MAX", "0.92"))

# Correlation-aware convex optimizer (preferred over prune)
PORTFOLIO_CORR_OPT = os.environ.get("PORTFOLIO_CORR_OPT", "1") == "1"
PORTFOLIO_CORR_OPT_RIDGE = float(os.environ.get("PORTFOLIO_CORR_OPT_RIDGE", "1e-6"))
PORTFOLIO_CORR_OPT_ITERS = int(os.environ.get("PORTFOLIO_CORR_OPT_ITERS", "30"))
# Blank or "existing_mode" preserves the current corr-opt -> corr-prune ->
# legacy fallback chain. "hrp" enables hierarchical risk parity.
PORTFOLIO_ALLOCATION_MODE = str(os.environ.get("PORTFOLIO_ALLOCATION_MODE", "") or "").strip().lower()

# Portfolio-aware diagnostics / diversification / netting
PORTFOLIO_DIAG_CORR_TOP_N = int(os.environ.get("PORTFOLIO_DIAG_CORR_TOP_N", "10"))
PORTFOLIO_CORR_SNAPSHOT_RETENTION_DAYS = int(os.environ.get("PORTFOLIO_CORR_SNAPSHOT_RETENTION_DAYS", "30"))

PORTFOLIO_MODEL_DIVERSIFICATION = os.environ.get("PORTFOLIO_MODEL_DIVERSIFICATION", "1") == "1"
PORTFOLIO_MODEL_DIVERSIFICATION_LOOKBACK = int(
    os.environ.get("PORTFOLIO_MODEL_DIVERSIFICATION_LOOKBACK", str(PORTFOLIO_CORR_LOOKBACK))
)
PORTFOLIO_MODEL_DIVERSIFICATION_ALPHA = float(
    os.environ.get("PORTFOLIO_MODEL_DIVERSIFICATION_ALPHA", "0.30")
)
PORTFOLIO_MODEL_DIVERSIFICATION_MIN = float(
    os.environ.get("PORTFOLIO_MODEL_DIVERSIFICATION_MIN", "0.80")
)
PORTFOLIO_MODEL_DIVERSIFICATION_MAX = float(
    os.environ.get("PORTFOLIO_MODEL_DIVERSIFICATION_MAX", "1.20")
)

PORTFOLIO_EXPOSURE_NETTING = os.environ.get("PORTFOLIO_EXPOSURE_NETTING", "1") == "1"
PORTFOLIO_EXPOSURE_NETTING_LOOKBACK = int(
    os.environ.get("PORTFOLIO_EXPOSURE_NETTING_LOOKBACK", str(PORTFOLIO_CORR_LOOKBACK))
)
PORTFOLIO_EXPOSURE_NETTING_LIMIT = float(os.environ.get("PORTFOLIO_EXPOSURE_NETTING_LIMIT", "0.85"))
PORTFOLIO_EXPOSURE_NETTING_ALPHA = float(os.environ.get("PORTFOLIO_EXPOSURE_NETTING_ALPHA", "1.0"))

PORTFOLIO_TOTAL_RISK_LIMIT = float(os.environ.get("PORTFOLIO_TOTAL_RISK_LIMIT", "0.030"))

# Adaptive gamma by regime (LOW/MID/HIGH)
# gamma controls how strongly utility competes with risk in the convex optimizer.
PORTFOLIO_CORR_OPT_GAMMA_BASE = float(os.environ.get("PORTFOLIO_CORR_OPT_GAMMA_BASE", "1.0"))
PORTFOLIO_CORR_OPT_GAMMA_LOW = float(os.environ.get("PORTFOLIO_CORR_OPT_GAMMA_LOW", "1.20"))
PORTFOLIO_CORR_OPT_GAMMA_MID = float(os.environ.get("PORTFOLIO_CORR_OPT_GAMMA_MID", "1.00"))
PORTFOLIO_CORR_OPT_GAMMA_HIGH = float(os.environ.get("PORTFOLIO_CORR_OPT_GAMMA_HIGH", "0.70"))

# Regime sizing anchor (used by dev_core.regime_size)
PORTFOLIO_REGIME_ANCHOR = os.environ.get("PORTFOLIO_REGIME_ANCHOR", "SPY").strip().upper()

# Price freshness (symbol-level data quality gate)
PORTFOLIO_MAX_PRICE_STALE_S = int(os.environ.get("PORTFOLIO_MAX_PRICE_STALE_S", "180"))  # 3 min

# Optional per-symbol caps via JSON, e.g.:
#   set PORTFOLIO_SYMBOL_CAPS={"SPY":0.5,"BTC":0.35,"OIL":0.35}
_SYMBOL_CAPS_RAW = os.environ.get("PORTFOLIO_SYMBOL_CAPS", "").strip()
PORTFOLIO_SYMBOL_CAPS = {}
if _SYMBOL_CAPS_RAW:
    try:
        PORTFOLIO_SYMBOL_CAPS = json.loads(_SYMBOL_CAPS_RAW)
        if not isinstance(PORTFOLIO_SYMBOL_CAPS, dict):
            PORTFOLIO_SYMBOL_CAPS = {}
    except Exception:
        PORTFOLIO_SYMBOL_CAPS = {}

# -----------------------------
# DB schema (owned by this module)
# -----------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS portfolio_state (
  model_id TEXT NOT NULL DEFAULT 'baseline',
  symbol TEXT NOT NULL,
  side TEXT NOT NULL,              -- LONG / SHORT / FLAT
  weight REAL NOT NULL,            -- 0..1 (fraction of capital)
  opened_ts_ms INTEGER NOT NULL,
  updated_ts_ms INTEGER NOT NULL,
  source_alert_id INTEGER,
  explain_json TEXT,
  PRIMARY KEY (model_id, symbol)
);

CREATE TABLE IF NOT EXISTS portfolio_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  model_id TEXT NOT NULL DEFAULT 'baseline',
  symbol TEXT NOT NULL,
  action TEXT NOT NULL,            -- OPEN / INCREASE / DECREASE / CLOSE / REVERSE / HOLD
  from_side TEXT NOT NULL,
  to_side TEXT NOT NULL,
  from_weight REAL NOT NULL,
  to_weight REAL NOT NULL,
  delta_weight REAL NOT NULL,
  source_alert_id INTEGER,
  prediction_id INTEGER,
  explain_json TEXT
);

CREATE TABLE IF NOT EXISTS portfolio_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_promotion_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  strategy_name TEXT NOT NULL,
  reason TEXT
);

CREATE TABLE IF NOT EXISTS strategy_shadow_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  strategy_name TEXT NOT NULL,
  desired_json TEXT NOT NULL,
  metrics_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_strategy_shadow_runs_ts
  ON strategy_shadow_runs(ts_ms);

CREATE INDEX IF NOT EXISTS idx_strategy_shadow_runs_name_ts
  ON strategy_shadow_runs(strategy_name, ts_ms);

CREATE TABLE IF NOT EXISTS portfolio_position_corr_snapshots (
  ts_ms INTEGER NOT NULL,
  model_id_a TEXT NOT NULL,
  symbol_a TEXT NOT NULL,
  model_id_b TEXT NOT NULL,
  symbol_b TEXT NOT NULL,
  corr REAL NOT NULL,
  same_direction INTEGER NOT NULL DEFAULT 0,
  signed_weight_a REAL NOT NULL,
  signed_weight_b REAL NOT NULL,
  abs_weight_product REAL NOT NULL DEFAULT 0,
  same_direction_risk REAL NOT NULL DEFAULT 0,
  PRIMARY KEY (ts_ms, model_id_a, symbol_a, model_id_b, symbol_b)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_position_corr_ts
  ON portfolio_position_corr_snapshots(ts_ms);

CREATE TABLE IF NOT EXISTS portfolio_model_corr_snapshots (
  ts_ms INTEGER NOT NULL,
  model_id_a TEXT NOT NULL,
  model_id_b TEXT NOT NULL,
  corr REAL NOT NULL,
  gross_a REAL NOT NULL DEFAULT 0,
  gross_b REAL NOT NULL DEFAULT 0,
  weight_product REAL NOT NULL DEFAULT 0,
  same_direction_overlap REAL NOT NULL DEFAULT 0,
  diversification_score_a REAL NOT NULL DEFAULT 1,
  diversification_score_b REAL NOT NULL DEFAULT 1,
  PRIMARY KEY (ts_ms, model_id_a, model_id_b)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_model_corr_ts
  ON portfolio_model_corr_snapshots(ts_ms);
"""
# =========================
# SECTION 2 / ~200 lines
# =========================

def init_portfolio_db():
    """Ensure portfolio state/order storage exists and apply light migrations.

    Returns
    -------
    None

    Notes
    -----
    The migration logic preserves historical rows while normalizing
    ``model_id`` into the portfolio primary keys. This helper is safe to call
    repeatedly before read or write paths.

    Side Effects
    ------------
    Creates tables and indexes, may rename and rebuild legacy tables, and
    commits schema/data backfills in the runtime database.
    """
    started = time.perf_counter()
    with _PORTFOLIO_DB_INIT_LOCK:
        last_error: BaseException | None = None
        for attempt in range(max(1, _PORTFOLIO_DB_INIT_RETRY_ATTEMPTS)):
            con = None
            attempt_started = time.perf_counter()
            try:
                con = connect_rw_direct(
                    timeout_s=30.0,
                    busy_timeout_ms=int(_PORTFOLIO_DB_INIT_BUSY_TIMEOUT_MS),
                )
                _apply_portfolio_db_schema(con)
                if attempt > 0:
                    LOGGER.info(
                        "portfolio_db_init_retry_recovered attempts=%s duration_ms=%.1f",
                        int(attempt) + 1,
                        (time.perf_counter() - started) * 1000.0,
                    )
                return
            except Exception as e:
                last_error = e
                transient = _is_sqlite_busy_error(e)
                try:
                    if con is not None and bool(getattr(con, "in_transaction", False)):
                        con.rollback()
                except Exception as rollback_err:
                    _warn_nonfatal(
                        "PORTFOLIO_DB_INIT_ROLLBACK_FAILED",
                        rollback_err,
                        once_key="portfolio_db_init_rollback",
                    )
                if (not transient) or attempt >= (_PORTFOLIO_DB_INIT_RETRY_ATTEMPTS - 1):
                    raise
                delay_ms = min(
                    int(_PORTFOLIO_DB_INIT_RETRY_MAX_MS),
                    int(_PORTFOLIO_DB_INIT_RETRY_BASE_MS * (2 ** attempt)),
                )
                delay_ms += random.randint(0, max(25, int(_PORTFOLIO_DB_INIT_RETRY_BASE_MS) // 2))
                LOGGER.warning(
                    "portfolio_db_init_busy_retry attempt=%s/%s attempt_duration_ms=%.1f delay_ms=%s error=%s",
                    int(attempt) + 1,
                    int(_PORTFOLIO_DB_INIT_RETRY_ATTEMPTS),
                    (time.perf_counter() - attempt_started) * 1000.0,
                    int(delay_ms),
                    str(e),
                )
                time.sleep(delay_ms / 1000.0)
            finally:
                if con is not None:
                    con.close()
        if last_error is not None:
            raise last_error


def _portfolio_meta_cache_key(con, key: str) -> str:
    key_s = str(key)
    db_path = ""
    try:
        row = con.execute("PRAGMA database_list").fetchone()
        if row and len(row) >= 3 and row[2]:
            db_path = str(row[2])
    except Exception:
        db_path = ""
    if db_path:
        return f"{db_path}:{key_s}"
    return key_s


def _get_meta(con, key: str) -> Optional[str]:
    from engine.runtime.state_cache import cache_get, cache_set

    key_s = _portfolio_meta_cache_key(con, key)
    cached = cache_get("portfolio_meta", key_s)
    if cached is not None:
        return str(cached) if cached is not None else None

    row = con.execute("SELECT value FROM portfolio_meta WHERE key=?", (str(key),)).fetchone()
    value = str(row[0]) if row and row[0] is not None else None
    cache_set("portfolio_meta", key_s, value, ttl_s=3600.0)
    return value


def _set_meta(con, key: str, value: str) -> None:
    from engine.runtime.state_cache import cache_invalidate_namespace, cache_set

    cache_key = _portfolio_meta_cache_key(con, key)
    key_s = str(key)
    value_s = str(value)

    con.execute(
        """
        INSERT INTO portfolio_meta(key, value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key_s, value_s),
    )
    cache_set("portfolio_meta", cache_key, value_s, ttl_s=3600.0)
    cache_invalidate_namespace("portfolio_snapshot")


# Alias for consistency with other modules
_put_meta = _set_meta


def _set_risk_state_inline(con, key: str, value: str) -> None:
    from engine.runtime.state_cache import cache_invalidate_namespace, cache_set

    key_s = str(key)
    value_s = str(value)
    ts_ms = int(_now_ms())
    con.execute(
        """
        INSERT OR REPLACE INTO risk_state(key, value, updated_ts_ms)
        VALUES (?,?,?)
        """,
        (key_s, value_s, ts_ms),
    )
    try:
        from engine.runtime.risk_state import _cache_key as _risk_state_cache_key

        cache_key = _risk_state_cache_key(key_s)
        cache_set("risk_state", cache_key, value_s, ttl_s=3600.0)
        cache_set("risk_state_row", cache_key, (value_s, int(ts_ms)), ttl_s=3600.0)
        cache_invalidate_namespace("api_read", prefix="execution_stats")
        cache_invalidate_namespace("api_read", prefix="execution_metrics")
        cache_invalidate_namespace("portfolio_snapshot")
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_STATE_CACHE_INVALIDATE_FAILED",
            e,
            once_key="risk_state_cache_invalidate",
            key=key_s,
        )


def _persist_portfolio_runtime_health(
    con,
    *,
    now_ms: int,
    degraded_reasons: List[Dict[str, Any]],
    orders_n: int,
    changed_symbols: List[str],
    execution_blocked: bool = False,
    execution_blocked_codes: List[str] | None = None,
) -> None:
    blocked_codes = [
        str(code)
        for code in list(execution_blocked_codes or [])
        if str(code or "").strip()
    ]
    payload = {
        "updated_ts_ms": int(now_ms),
        "degraded": bool(degraded_reasons),
        "degraded_reasons": list(degraded_reasons or []),
        "orders_n": int(orders_n),
        "changed_symbols": [str(symbol) for symbol in (changed_symbols or []) if str(symbol or "").strip()],
        "changed_symbols_n": int(len(changed_symbols or [])),
        "execution_blocked": bool(execution_blocked or blocked_codes),
        "execution_blocked_codes": blocked_codes,
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    _set_risk_state_inline(con, "portfolio_runtime_health", raw)
    try:
        _put_meta(con, "last_portfolio_runtime_health", raw)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RUNTIME_HEALTH_META_WRITE_FAILED",
            e,
            once_key="portfolio_runtime_health_meta_write",
        )


_PORTFOLIO_EXECUTION_BLOCKED_DEGRADED_CODES = frozenset(
    {
        "PORTFOLIO_STRATEGY_ALLOCATOR_FAILED",
        "PORTFOLIO_RISK_ENGINE_FAILED",
        "PORTFOLIO_RISK_GATE_FAILED",
        "PORTFOLIO_TOTAL_RISK_FAILED",
    }
)


def _portfolio_execution_blocked_codes(degraded_reasons: List[Dict[str, Any]]) -> List[str]:
    blocked_codes: list[str] = []
    for row in list(degraded_reasons or []):
        if not isinstance(row, dict):
            continue
        code = str((row or {}).get("code") or "").strip()
        if (
            code
            and code in _PORTFOLIO_EXECUTION_BLOCKED_DEGRADED_CODES
            and code not in blocked_codes
        ):
            blocked_codes.append(code)
    return blocked_codes


def _now_ms() -> int:
    return int(time.time() * 1000)


def _normalize_model_id(model_id: Any) -> str:
    mid = str(model_id or "").strip()
    return mid or "baseline"


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(x)))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float_failed",
            value_type=type(value).__name__,
        )
        fallback = float(default)
        return fallback
    return float(out) if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return int(default)
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_SAFE_INT_FAILED",
            e,
            once_key=f"safe_int:{type(value).__name__}:{str(value)[:64]}",
            value_type=type(value).__name__,
        )
        return int(default)


def _dict_str_any(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    out: Dict[str, Any] = {}
    for key, item in value.items():
        out[str(key)] = item
    return out


def _signed_weight(row: Optional[Dict[str, Any]]) -> float:
    row = row or {}
    w = _safe_float(row.get("weight", 0.0), 0.0)
    side = str(row.get("side", "") or "").upper().strip()
    if side == "SHORT":
        return -abs(float(w))
    if side == "LONG":
        return abs(float(w))
    if side == "FLAT":
        return 0.0
    return float(w)


def _corr_lookup_factory(con, lookback: int):
    from engine.strategy.risk import corr_from_prices

    cache: Dict[Tuple[str, str], float] = {}

    def _lookup(sym_a: str, sym_b: str) -> float:
        sa = str(sym_a or "").strip().upper()
        sb = str(sym_b or "").strip().upper()
        if not sa or not sb:
            return 0.0
        if sa == sb:
            return 1.0
        key: Tuple[str, str] = (sa, sb) if sa <= sb else (sb, sa)
        if key in cache:
            return float(cache[key])
        try:
            c = corr_from_prices(con, sa, sb, lookback=int(lookback))
            val = _clamp(_safe_float(c, 0.0), -1.0, 1.0) if c is not None else 0.0
        except Exception:
            val = 0.0
        cache[key] = float(val)
        return float(val)

    return _lookup


def _collect_position_items(desired: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for tgt in (desired or {}).values():
        sym = _desired_symbol(None, tgt)
        if not sym:
            continue
        sw = _signed_weight(tgt)
        if abs(float(sw)) <= 1e-12:
            continue
        items.append(
            {
                "model_id": _normalize_model_id((tgt or {}).get("model_id")),
                "symbol": sym,
                "signed_weight": float(sw),
                "abs_weight": abs(float(sw)),
            }
        )
    items.sort(key=lambda item: (item["model_id"], item["symbol"]))
    return items


def _ensure_reason_dict(target: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    row = target or {}
    reason = row.get("reason")
    if isinstance(reason, dict):
        return reason
    row["reason"] = {"raw": reason} if reason not in (None, "") else {}
    return row["reason"]


def _normalize_nonnegative_weights(weights: List[float]) -> List[float]:
    cleaned = [max(0.0, _safe_float(weight, 0.0)) for weight in (weights or [])]
    total = sum(cleaned)
    if total <= 1e-12:
        n = len(cleaned)
        return [1.0 / float(n) for _ in range(n)] if n > 0 else []
    return [float(weight) / float(total) for weight in cleaned]


def _sample_variance(xs: List[float]) -> Optional[float]:
    vals = [float(x) for x in (xs or []) if math.isfinite(_safe_float(x, float("nan")))]
    n = len(vals)
    if n < 2:
        return None
    mean = sum(vals) / float(n)
    var = sum((float(x) - float(mean)) * (float(x) - float(mean)) for x in vals) / float(max(1, n - 1))
    if not math.isfinite(var):
        return None
    return max(1e-8, float(var))


def _pairwise_correlation(xs: List[float], ys: List[float]) -> float:
    n = min(len(xs or []), len(ys or []))
    if n < 3:
        return 0.0
    xa = [float(v) for v in list(xs or [])[-n:]]
    ya = [float(v) for v in list(ys or [])[-n:]]
    mx = sum(xa) / float(n)
    my = sum(ya) / float(n)
    vx = sum((float(v) - float(mx)) * (float(v) - float(mx)) for v in xa)
    vy = sum((float(v) - float(my)) * (float(v) - float(my)) for v in ya)
    if vx <= 1e-12 or vy <= 1e-12:
        return 0.0
    cov = sum((float(xa[i]) - float(mx)) * (float(ya[i]) - float(my)) for i in range(n))
    return _clamp(float(cov) / float(math.sqrt(vx * vy)), -1.0, 1.0)


def _returns_to_series_list(returns: Any) -> List[List[float]]:
    if isinstance(returns, dict):
        return [list(series or []) for series in returns.values()]
    if isinstance(returns, (list, tuple)):
        return [list(series or []) for series in returns]
    return []


def compute_correlation_matrix(returns) -> List[List[float]]:
    """Build a dense asset correlation matrix from return series.

    Parameters
    ----------
    returns : Any
        Return-series container understood by ``_returns_to_series_list``.
        Values are expected to be unitless period returns.

    Returns
    -------
    list of list of float
        Symmetric correlation matrix with diagonal entries fixed at ``1.0``.
        Empty input returns an empty matrix.
    """
    series_list = _returns_to_series_list(returns)
    n = len(series_list)
    if n <= 0:
        return []
    corr_matrix: List[List[float]] = [[0.0 for _ in range(n)] for _ in range(n)]
    for i in range(n):
        corr_matrix[i][i] = 1.0
        for j in range(i + 1, n):
            corr = _pairwise_correlation(series_list[i], series_list[j])
            corr_matrix[i][j] = float(corr)
            corr_matrix[j][i] = float(corr)
    return corr_matrix


def hierarchical_clustering(corr_matrix) -> List[List[float]]:
    """Produce a deterministic single-linkage clustering from correlations.

    Parameters
    ----------
    corr_matrix : sequence of sequence of float
        Correlation matrix with values expected in the inclusive range
        ``[-1.0, 1.0]``.

    Returns
    -------
    list of list of float
        Linkage-style rows ``[left_id, right_id, distance, member_count]``.
        Distances are computed as ``sqrt(0.5 * (1 - corr))`` and are therefore
        unitless.

    Notes
    -----
    Tie-breaking is deterministic: equal-distance merges are ordered by the
    sorted pair of cluster identifiers so downstream quasi-diagonalization is
    stable across runs.
    """
    n = len(corr_matrix or [])
    if n <= 1:
        return []

    dist: List[List[float]] = [[0.0 for _ in range(n)] for _ in range(n)]
    for i in range(n):
        dist[i][i] = 0.0
        for j in range(i + 1, n):
            corr = 1.0 if i == j else _clamp(_safe_float((corr_matrix[i] or [0.0] * n)[j], 0.0), -1.0, 1.0)
            distance = math.sqrt(max(0.0, 0.5 * (1.0 - float(corr))))
            dist[i][j] = float(distance)
            dist[j][i] = float(distance)

    clusters: List[Dict[str, Any]] = [{"id": int(idx), "members": [int(idx)]} for idx in range(n)]
    linkage_matrix: List[List[float]] = []
    next_cluster_id = int(n)

    while len(clusters) > 1:
        best_pair: Optional[Tuple[int, int]] = None
        best_dist: Optional[float] = None
        best_ids: Optional[Tuple[int, int]] = None
        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                left = clusters[i]
                right = clusters[j]
                cur = min(float(dist[a][b]) for a in left["members"] for b in right["members"])
                pair_ids = (int(left["id"]), int(right["id"]))
                ordered_ids = pair_ids if pair_ids[0] <= pair_ids[1] else (pair_ids[1], pair_ids[0])
                if (
                    best_pair is None
                    or float(cur) < float(best_dist) - 1e-12
                    or (abs(float(cur) - float(best_dist or 0.0)) <= 1e-12 and ordered_ids < (best_ids or ordered_ids))
                ):
                    best_pair = (i, j)
                    best_dist = float(cur)
                    best_ids = ordered_ids

        if best_pair is None:
            break

        left = clusters[best_pair[0]]
        right = clusters[best_pair[1]]
        merged_members = list(left["members"]) + list(right["members"])
        linkage_matrix.append(
            [
                float(left["id"]),
                float(right["id"]),
                float(best_dist or 0.0),
                float(len(merged_members)),
            ]
        )
        merged = {"id": int(next_cluster_id), "members": merged_members}
        next_cluster_id += 1
        for idx in sorted(best_pair, reverse=True):
            clusters.pop(idx)
        clusters.append(merged)

    return linkage_matrix


def quasi_diagonalization(linkage_matrix) -> List[int]:
    """Recover a leaf ordering from the linkage matrix for HRP allocation.

    Parameters
    ----------
    linkage_matrix : sequence of sequence of float
        Linkage rows produced by :func:`hierarchical_clustering`.

    Returns
    -------
    list of int
        Asset index ordering used by :func:`recursive_bisection`. When the
        linkage matrix is empty, returns ``[0]`` to preserve existing behavior.

    Notes
    -----
    Ordering depends on linkage row order. Callers should pass the exact matrix
    emitted by the clustering step rather than a re-sorted variant.
    """
    rows = [list(row or []) for row in list(linkage_matrix or [])]
    if not rows:
        return [0]

    leaf_count = len(rows) + 1
    children: Dict[int, Tuple[int, int]] = {}
    for row_index, row in enumerate(rows):
        if len(row) < 2:
            continue
        children[int(leaf_count + row_index)] = (int(row[0]), int(row[1]))

    def _traverse(node_id: int) -> List[int]:
        if int(node_id) < int(leaf_count):
            return [int(node_id)]
        left_right = children.get(int(node_id))
        if not left_right:
            return []
        left, right = left_right
        return _traverse(int(left)) + _traverse(int(right))

    order = _traverse(int(leaf_count + len(rows) - 1))
    if not order:
        return list(range(int(leaf_count)))
    return [int(idx) for idx in order if 0 <= int(idx) < int(leaf_count)]


def _inverse_variance_weights(cov_matrix: List[List[float]], members: List[int]) -> List[float]:
    if not members:
        return []
    inv_diag: List[float] = []
    for idx in members:
        diag = 1.0
        if 0 <= int(idx) < len(cov_matrix or []):
            diag = max(1e-8, _safe_float((cov_matrix[int(idx)] or [1.0])[int(idx)], 1.0))
        inv_diag.append(1.0 / float(diag))
    return _normalize_nonnegative_weights(inv_diag)


def _cluster_variance(cov_matrix: List[List[float]], members: List[int]) -> float:
    if not members:
        return 0.0
    inv_weights = _inverse_variance_weights(cov_matrix, members)
    variance = 0.0
    for i_pos, i_idx in enumerate(members):
        for j_pos, j_idx in enumerate(members):
            variance += (
                float(inv_weights[i_pos])
                * _safe_float((cov_matrix[int(i_idx)] or [0.0] * len(cov_matrix))[int(j_idx)], 0.0)
                * float(inv_weights[j_pos])
            )
    return max(0.0, float(variance))


def recursive_bisection(weights, cov_matrix) -> List[float]:
    """Allocate HRP weights by recursively splitting the ordered leaf set.

    Parameters
    ----------
    weights : sequence of int
        Asset ordering, typically the output of
        :func:`quasi_diagonalization`.
    cov_matrix : sequence of sequence of float
        Covariance matrix aligned to the asset ordering. Values are variances
        and covariances in squared-return units.

    Returns
    -------
    list of float
        Non-negative portfolio weights aligned to ``cov_matrix`` indices and
        normalized to sum to approximately ``1.0``.

    Notes
    -----
    The function preserves the ordering dependency of hierarchical risk parity:
    different leaf orders can produce different allocations even with the same
    covariance matrix.
    """
    n = len(cov_matrix or [])
    if n <= 0:
        return []

    order = [int(idx) for idx in list(weights or []) if 0 <= int(idx) < int(n)]
    if not order:
        order = list(range(int(n)))

    out = [0.0 for _ in range(n)]
    for idx in order:
        out[int(idx)] = 1.0

    clusters: List[List[int]] = [list(order)]
    while clusters:
        cluster = clusters.pop(0)
        if len(cluster) <= 1:
            continue
        split = int(len(cluster) // 2)
        if split <= 0 or split >= len(cluster):
            continue

        left = list(cluster[:split])
        right = list(cluster[split:])
        left_var = _cluster_variance(cov_matrix, left)
        right_var = _cluster_variance(cov_matrix, right)
        denom = float(left_var + right_var)
        alpha = 0.5 if denom <= 1e-12 else 1.0 - (float(left_var) / float(denom))
        alpha = _clamp(float(alpha), 0.0, 1.0)

        for idx in left:
            out[int(idx)] = float(out[int(idx)]) * float(alpha)
        for idx in right:
            out[int(idx)] = float(out[int(idx)]) * float(1.0 - float(alpha))

        if len(left) > 1:
            clusters.append(left)
        if len(right) > 1:
            clusters.append(right)

    return _normalize_nonnegative_weights(out)


def _load_hrp_return_series(con, symbol: str, lookback: int) -> List[float]:
    if con is None:
        return []
    try:
        rows = con.execute(
            """
            SELECT price
            FROM prices
            WHERE symbol = ?
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(max(4, int(lookback) + 1))),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_HRP_RETURN_SERIES_LOAD_FAILED",
            e,
            once_key=f"portfolio_hrp_return_series:{str(symbol).upper().strip()}",
            symbol=str(symbol).upper().strip(),
            lookback=int(lookback or 0),
        )
        return []

    px = [float(r[0]) for r in rows or [] if r and r[0] is not None]
    px.reverse()
    if len(px) < 4:
        return []

    rets: List[float] = []
    for idx in range(1, len(px)):
        prev_px = float(px[idx - 1])
        cur_px = float(px[idx])
        if prev_px > 0.0 and cur_px > 0.0:
            rets.append(float(math.log(cur_px / prev_px)))
    return rets


def _build_covariance_matrix_from_returns(returns, corr_matrix: List[List[float]]) -> Tuple[List[List[float]], Dict[str, Any]]:
    series_list = _returns_to_series_list(returns)
    n = len(series_list)
    if n <= 0:
        return [], {"source": "empty", "usable_variances": 0}

    variances: List[Optional[float]] = [_sample_variance(series) for series in series_list]
    valid_variances = [float(v) for v in variances if v is not None]
    default_variance = float(sum(valid_variances) / float(len(valid_variances))) if valid_variances else 1.0
    resolved_variances = [float(v if v is not None else default_variance) for v in variances]

    source = "returns"
    if not valid_variances:
        source = "diagonal_fallback"
    elif len(valid_variances) < len(series_list):
        source = "mixed"

    cov_matrix: List[List[float]] = [[0.0 for _ in range(n)] for _ in range(n)]
    for i in range(n):
        cov_matrix[i][i] = max(1e-8, float(resolved_variances[i]))
        for j in range(i + 1, n):
            corr = _clamp(_safe_float((corr_matrix[i] or [0.0] * n)[j], 0.0), -1.0, 1.0)
            covariance = float(corr) * float(math.sqrt(max(1e-8, resolved_variances[i]) * max(1e-8, resolved_variances[j])))
            cov_matrix[i][j] = float(covariance)
            cov_matrix[j][i] = float(covariance)

    return cov_matrix, {"source": source, "usable_variances": int(len(valid_variances))}


def _apply_weight_caps(raw_weights: List[float], caps: List[float], target_total: float) -> List[float]:
    n = len(raw_weights or [])
    if n <= 0:
        return []

    target_left = max(0.0, float(target_total))
    normalized = _normalize_nonnegative_weights(list(raw_weights or []))
    out = [0.0 for _ in range(n)]
    remaining = {idx for idx in range(n)}
    safe_caps = [max(0.0, _safe_float(cap, 0.0)) for cap in caps]

    while remaining and target_left > 1e-12:
        remaining_total = sum(float(normalized[idx]) for idx in remaining)
        if remaining_total <= 1e-12:
            equal_share = float(target_left) / float(len(remaining))
            for idx in list(remaining):
                out[idx] += min(float(safe_caps[idx]), float(equal_share))
            break

        clipped: List[int] = []
        assigned = 0.0
        for idx in list(remaining):
            desired = float(target_left) * (float(normalized[idx]) / float(remaining_total))
            cap = float(safe_caps[idx])
            if desired >= cap - 1e-12:
                out[idx] += float(cap)
                assigned += float(cap)
                clipped.append(int(idx))

        if not clipped:
            for idx in list(remaining):
                desired = float(target_left) * (float(normalized[idx]) / float(remaining_total))
                out[idx] += min(float(safe_caps[idx]), float(desired))
            break

        target_left = max(0.0, float(target_left) - float(assigned))
        for idx in clipped:
            remaining.discard(int(idx))

    return [float(weight) for weight in out]


def _allocate_hrp_side(
    con,
    entries: List[Dict[str, Any]],
    *,
    lookback: int,
    side_budget: float,
) -> Tuple[List[float], Dict[str, Any]]:
    if not entries:
        return [], {"source": "empty", "order": []}

    if len(entries) == 1:
        return [max(0.0, float(side_budget))], {
            "source": "single_position",
            "order": [str((entries[0] or {}).get("symbol") or "")],
        }

    returns = [
        _load_hrp_return_series(con, str((entry or {}).get("symbol") or ""), int(lookback))
        for entry in entries
    ]
    corr_matrix = compute_correlation_matrix(returns)
    cov_matrix, cov_meta = _build_covariance_matrix_from_returns(returns, corr_matrix)
    linkage_matrix = hierarchical_clustering(corr_matrix)
    order = quasi_diagonalization(linkage_matrix)
    raw_weights = recursive_bisection(order, cov_matrix)
    if len(raw_weights) != len(entries):
        raw_weights = _normalize_nonnegative_weights([1.0 for _ in entries])

    caps = [
        max(
            _safe_float((entry or {}).get("weight"), 0.0),
            _safe_float((entry or {}).get("weight_cap"), _safe_float((entry or {}).get("weight"), 0.0)),
        )
        for entry in entries
    ]
    applied_weights = _apply_weight_caps(raw_weights, caps, float(side_budget))
    if float(sum(applied_weights)) <= 1e-12 and float(side_budget) > 0.0:
        applied_weights = _apply_weight_caps([1.0 for _ in entries], caps, float(side_budget))

    order_symbols = [
        str((entries[idx] or {}).get("symbol") or "")
        for idx in order
        if 0 <= int(idx) < len(entries)
    ]
    return applied_weights, {
        "source": str(cov_meta.get("source") or "unknown"),
        "order": order_symbols,
        "usable_variances": int(cov_meta.get("usable_variances") or 0),
    }


def _apply_hrp_allocation(
    con,
    desired: Dict[str, Dict[str, Any]],
    *,
    gross_cap: float,
    lookback: int,
) -> Dict[str, Dict[str, Any]]:
    if not desired or len(desired) < 2:
        return desired

    gross_before = 0.0
    side_entries: Dict[str, List[Dict[str, Any]]] = {"LONG": [], "SHORT": []}

    for desired_key, tgt in (desired or {}).items():
        sym = _desired_symbol(desired_key, tgt)
        if not sym:
            continue
        side = str((tgt or {}).get("side") or "").upper().strip()
        if side not in side_entries:
            continue
        weight = abs(_safe_float((tgt or {}).get("weight"), 0.0))
        if weight <= 0.0:
            continue
        weight_cap = max(
            float(weight),
            _safe_float((tgt or {}).get("weight_cap"), float(weight)),
        )
        side_entries[side].append(
            {
                "desired_key": desired_key,
                "symbol": sym,
                "weight": float(weight),
                "weight_cap": float(weight_cap),
            }
        )
        gross_before += float(weight)

    if gross_before <= 1e-12:
        return desired

    gross_scale = 1.0
    if float(gross_cap) > 0.0 and float(gross_before) > float(gross_cap):
        gross_scale = float(gross_cap) / float(gross_before)

    for side, entries in side_entries.items():
        if not entries:
            continue
        side_budget = float(sum(float((entry or {}).get("weight", 0.0) or 0.0) for entry in entries)) * float(gross_scale)
        side_weights, meta = _allocate_hrp_side(
            con,
            entries,
            lookback=int(lookback),
            side_budget=float(side_budget),
        )
        order_index = {
            str(symbol): int(idx)
            for idx, symbol in enumerate(list(meta.get("order") or []))
            if str(symbol or "").strip()
        }
        for idx, entry in enumerate(entries):
            desired_key = entry["desired_key"]
            if desired_key not in desired:
                continue
            new_weight = float(side_weights[idx]) if idx < len(side_weights) else 0.0
            desired[desired_key]["weight"] = max(0.0, float(new_weight))
            reason = _ensure_reason_dict(desired.get(desired_key))
            reason["allocation_mode"] = "hrp"
            reason["hrp_weight"] = float(new_weight)
            reason["hrp_side"] = str(side)
            reason["hrp_side_budget"] = float(side_budget)
            reason["hrp_covariance_source"] = str(meta.get("source") or "unknown")
            reason["hrp_usable_variances"] = int(meta.get("usable_variances") or 0)
            reason["hrp_leaf_order"] = int(order_index.get(str(entry.get("symbol") or ""), idx))
            reason["hrp_symbols"] = list(meta.get("order") or [])

    return desired


def _build_model_books(desired: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    books: Dict[str, Dict[str, float]] = {}
    for tgt in (desired or {}).values():
        sym = _desired_symbol(None, tgt)
        if not sym:
            continue
        sw = _signed_weight(tgt)
        if abs(float(sw)) <= 1e-12:
            continue
        model_id = _normalize_model_id((tgt or {}).get("model_id"))
        books.setdefault(model_id, {})
        books[model_id][sym] = float(books[model_id].get(sym, 0.0) + float(sw))
    return books


def _desired_symbol(item_key: Any, tgt: Optional[Dict[str, Any]]) -> str:
    if isinstance(tgt, dict):
        sym = str((tgt or {}).get("symbol") or "").strip().upper()
        if sym:
            return sym
    raw = str(item_key or "").strip().upper()
    if ":" in raw:
        _, _, tail = raw.partition(":")
        raw = tail.strip().upper()
    return raw


def _book_gross(book: Dict[str, float]) -> float:
    return float(sum(abs(_safe_float(v, 0.0)) for v in (book or {}).values()))


def _book_covariance(
    book_a: Dict[str, float],
    book_b: Dict[str, float],
    corr_lookup,
) -> float:
    cov = 0.0
    for sym_a, wa in (book_a or {}).items():
        for sym_b, wb in (book_b or {}).items():
            cov += float(wa) * float(wb) * float(corr_lookup(sym_a, sym_b))
    return float(cov)


def _book_same_direction_overlap(
    book_a: Dict[str, float],
    book_b: Dict[str, float],
    corr_lookup,
) -> float:
    overlap = 0.0
    for sym_a, wa in (book_a or {}).items():
        for sym_b, wb in (book_b or {}).items():
            if float(wa) * float(wb) <= 0.0:
                continue
            c = float(corr_lookup(sym_a, sym_b))
            if c <= 0.0:
                continue
            overlap += float(c) * abs(float(wa)) * abs(float(wb))
    return float(overlap)


def _build_portfolio_correlation_diagnostics(
    con,
    desired: Dict[str, Dict[str, Any]],
    *,
    lookback: int,
    top_n: Optional[int] = None,
) -> Dict[str, Any]:
    corr_lookup = _corr_lookup_factory(con, int(lookback))
    items = _collect_position_items(desired)
    gross = sum(item["abs_weight"] for item in items)
    net = sum(item["signed_weight"] for item in items)
    long_gross = sum(item["abs_weight"] for item in items if item["signed_weight"] > 0.0)
    short_gross = sum(item["abs_weight"] for item in items if item["signed_weight"] < 0.0)

    position_pairs: List[Dict[str, Any]] = []
    pos_abs_corr_num = 0.0
    pos_abs_corr_den = 0.0
    pos_max_abs_corr = 0.0
    same_direction_pair_long = 0.0
    same_direction_pair_short = 0.0

    for i in range(len(items)):
        left = items[i]
        for j in range(i + 1, len(items)):
            right = items[j]
            c = float(corr_lookup(left["symbol"], right["symbol"]))
            wprod = float(left["abs_weight"]) * float(right["abs_weight"])
            abs_corr = abs(float(c))
            pos_abs_corr_num += abs_corr * wprod
            pos_abs_corr_den += wprod
            pos_max_abs_corr = max(pos_max_abs_corr, abs_corr)

            same_direction = float(left["signed_weight"]) * float(right["signed_weight"]) > 0.0
            same_direction_risk = 0.0
            if same_direction and c > 0.0:
                same_direction_risk = float(c) * wprod
                if left["signed_weight"] > 0.0 and right["signed_weight"] > 0.0:
                    same_direction_pair_long += same_direction_risk
                elif left["signed_weight"] < 0.0 and right["signed_weight"] < 0.0:
                    same_direction_pair_short += same_direction_risk

            position_pairs.append(
                {
                    "model_id_a": left["model_id"],
                    "symbol_a": left["symbol"],
                    "model_id_b": right["model_id"],
                    "symbol_b": right["symbol"],
                    "corr": float(c),
                    "abs_corr": float(abs_corr),
                    "same_direction": bool(same_direction),
                    "signed_weight_a": float(left["signed_weight"]),
                    "signed_weight_b": float(right["signed_weight"]),
                    "abs_weight_product": float(wprod),
                    "same_direction_risk": float(same_direction_risk),
                }
            )

    books = _build_model_books(desired)
    model_ids = sorted(books.keys())
    model_pairs: List[Dict[str, Any]] = []
    model_abs_corr_num = 0.0
    model_abs_corr_den = 0.0
    model_max_abs_corr = 0.0
    model_scores: Dict[str, Dict[str, float]] = {}
    crowd_sum: Dict[str, float] = {mid: 0.0 for mid in model_ids}
    crowd_den: Dict[str, float] = {mid: 0.0 for mid in model_ids}
    abs_corr_sum: Dict[str, float] = {mid: 0.0 for mid in model_ids}
    abs_corr_den: Dict[str, float] = {mid: 0.0 for mid in model_ids}

    for i in range(len(model_ids)):
        mid_a = model_ids[i]
        book_a = books.get(mid_a) or {}
        gross_a = _book_gross(book_a)
        for j in range(i + 1, len(model_ids)):
            mid_b = model_ids[j]
            book_b = books.get(mid_b) or {}
            gross_b = _book_gross(book_b)
            var_a = _book_covariance(book_a, book_a, corr_lookup)
            var_b = _book_covariance(book_b, book_b, corr_lookup)
            cov_ab = _book_covariance(book_a, book_b, corr_lookup)
            if var_a > 1e-12 and var_b > 1e-12:
                corr_ab = _clamp(cov_ab / math.sqrt(var_a * var_b), -1.0, 1.0)
            else:
                corr_ab = 0.0
            weight_product = float(gross_a) * float(gross_b)
            same_overlap = _book_same_direction_overlap(book_a, book_b, corr_lookup)

            model_pairs.append(
                {
                    "model_id_a": mid_a,
                    "model_id_b": mid_b,
                    "corr": float(corr_ab),
                    "gross_a": float(gross_a),
                    "gross_b": float(gross_b),
                    "weight_product": float(weight_product),
                    "same_direction_overlap": float(same_overlap),
                }
            )

            abs_corr = abs(float(corr_ab))
            model_abs_corr_num += abs_corr * weight_product
            model_abs_corr_den += weight_product
            model_max_abs_corr = max(model_max_abs_corr, abs_corr)

            crowd_sum[mid_a] += max(0.0, float(corr_ab)) * float(gross_b)
            crowd_den[mid_a] += float(gross_b)
            crowd_sum[mid_b] += max(0.0, float(corr_ab)) * float(gross_a)
            crowd_den[mid_b] += float(gross_a)

            abs_corr_sum[mid_a] += abs_corr * float(gross_b)
            abs_corr_den[mid_a] += float(gross_b)
            abs_corr_sum[mid_b] += abs_corr * float(gross_a)
            abs_corr_den[mid_b] += float(gross_a)

    for mid in model_ids:
        gross_mid = _book_gross(books.get(mid) or {})
        avg_abs_corr = (
            float(abs_corr_sum[mid] / abs_corr_den[mid]) if abs_corr_den[mid] > 1e-12 else 0.0
        )
        crowd = float(crowd_sum[mid] / crowd_den[mid]) if crowd_den[mid] > 1e-12 else 0.0
        bonus = 1.0
        if len(model_ids) > 1:
            centered = 1.0 - (2.0 * float(crowd))
            bonus = 1.0 + float(PORTFOLIO_MODEL_DIVERSIFICATION_ALPHA) * float(centered)
            bonus = _clamp(
                bonus,
                float(PORTFOLIO_MODEL_DIVERSIFICATION_MIN),
                float(PORTFOLIO_MODEL_DIVERSIFICATION_MAX),
            )
        model_scores[mid] = {
            "gross": float(gross_mid),
            "avg_abs_corr": float(avg_abs_corr),
            "crowding": float(crowd),
            "diversification_bonus": float(bonus),
        }

    position_pairs.sort(
        key=lambda pair: (
            float(pair.get("same_direction_risk", 0.0)),
            float(pair.get("abs_corr", 0.0)) * float(pair.get("abs_weight_product", 0.0)),
        ),
        reverse=True,
    )
    model_pairs.sort(
        key=lambda pair: (
            abs(float(pair.get("corr", 0.0))) * float(pair.get("weight_product", 0.0)),
            float(pair.get("same_direction_overlap", 0.0)),
        ),
        reverse=True,
    )

    limit = None if top_n is None else max(0, int(top_n))
    if limit is not None and limit > 0:
        position_pairs_out = position_pairs[:limit]
        model_pairs_out = model_pairs[:limit]
    elif limit == 0:
        position_pairs_out = []
        model_pairs_out = []
    else:
        position_pairs_out = position_pairs
        model_pairs_out = model_pairs

    return {
        "ts_ms": int(_now_ms()),
        "position_summary": {
            "n_positions": int(len(items)),
            "gross": float(gross),
            "net": float(net),
            "long_gross": float(long_gross),
            "short_gross": float(short_gross),
            "weighted_avg_abs_corr": (
                float(pos_abs_corr_num / pos_abs_corr_den) if pos_abs_corr_den > 1e-12 else 0.0
            ),
            "max_abs_corr": float(pos_max_abs_corr),
            "same_direction_pair_long": float(same_direction_pair_long),
            "same_direction_pair_short": float(same_direction_pair_short),
            "same_direction_effective_long": float(
                long_gross + float(PORTFOLIO_EXPOSURE_NETTING_ALPHA) * same_direction_pair_long
            ),
            "same_direction_effective_short": float(
                short_gross + float(PORTFOLIO_EXPOSURE_NETTING_ALPHA) * same_direction_pair_short
            ),
            "same_direction_effective_total": float(
                gross
                + float(PORTFOLIO_EXPOSURE_NETTING_ALPHA)
                * (same_direction_pair_long + same_direction_pair_short)
            ),
        },
        "model_summary": {
            "n_models": int(len(model_ids)),
            "weighted_avg_abs_corr": (
                float(model_abs_corr_num / model_abs_corr_den) if model_abs_corr_den > 1e-12 else 0.0
            ),
            "max_abs_corr": float(model_max_abs_corr),
        },
        "model_scores": model_scores,
        "position_pairs": position_pairs_out,
        "model_pairs": model_pairs_out,
        "_all_position_pairs": position_pairs,
        "_all_model_pairs": model_pairs,
    }


def _persist_portfolio_correlation_diagnostics(
    con,
    diagnostics: Dict[str, Any],
    *,
    ts_ms: Optional[int] = None,
) -> None:
    snap_ts = int(ts_ms if ts_ms is not None else diagnostics.get("ts_ms") or _now_ms())
    cutoff_ts = int(
        snap_ts - max(1, int(PORTFOLIO_CORR_SNAPSHOT_RETENTION_DAYS)) * 24 * 60 * 60 * 1000
    )

    con.execute(
        "DELETE FROM portfolio_position_corr_snapshots WHERE ts_ms < ?",
        (int(cutoff_ts),),
    )
    con.execute(
        "DELETE FROM portfolio_model_corr_snapshots WHERE ts_ms < ?",
        (int(cutoff_ts),),
    )

    model_scores = diagnostics.get("model_scores") or {}
    for pair in diagnostics.get("_all_position_pairs") or diagnostics.get("position_pairs") or []:
        con.execute(
            """
            INSERT OR REPLACE INTO portfolio_position_corr_snapshots(
              ts_ms, model_id_a, symbol_a, model_id_b, symbol_b,
              corr, same_direction, signed_weight_a, signed_weight_b,
              abs_weight_product, same_direction_risk
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(snap_ts),
                str(pair.get("model_id_a") or ""),
                str(pair.get("symbol_a") or ""),
                str(pair.get("model_id_b") or ""),
                str(pair.get("symbol_b") or ""),
                float(pair.get("corr", 0.0) or 0.0),
                1 if bool(pair.get("same_direction")) else 0,
                float(pair.get("signed_weight_a", 0.0) or 0.0),
                float(pair.get("signed_weight_b", 0.0) or 0.0),
                float(pair.get("abs_weight_product", 0.0) or 0.0),
                float(pair.get("same_direction_risk", 0.0) or 0.0),
            ),
        )

    for pair in diagnostics.get("_all_model_pairs") or diagnostics.get("model_pairs") or []:
        score_a = float(
            ((model_scores.get(str(pair.get("model_id_a") or "")) or {}).get("diversification_bonus", 1.0))
            or 1.0
        )
        score_b = float(
            ((model_scores.get(str(pair.get("model_id_b") or "")) or {}).get("diversification_bonus", 1.0))
            or 1.0
        )
        con.execute(
            """
            INSERT OR REPLACE INTO portfolio_model_corr_snapshots(
              ts_ms, model_id_a, model_id_b, corr, gross_a, gross_b,
              weight_product, same_direction_overlap, diversification_score_a, diversification_score_b
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(snap_ts),
                str(pair.get("model_id_a") or ""),
                str(pair.get("model_id_b") or ""),
                float(pair.get("corr", 0.0) or 0.0),
                float(pair.get("gross_a", 0.0) or 0.0),
                float(pair.get("gross_b", 0.0) or 0.0),
                float(pair.get("weight_product", 0.0) or 0.0),
                float(pair.get("same_direction_overlap", 0.0) or 0.0),
                float(score_a),
                float(score_b),
            ),
        )

    summary = {
        "ts_ms": int(snap_ts),
        "position_summary": dict(diagnostics.get("position_summary") or {}),
        "model_summary": dict(diagnostics.get("model_summary") or {}),
        "model_scores": dict(model_scores or {}),
    }
    _put_meta(
        con,
        "last_portfolio_correlation_snapshot",
        json.dumps(summary, separators=(",", ":"), sort_keys=True),
    )


def _apply_model_diversification_scoring(con, desired: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "enabled": bool(PORTFOLIO_MODEL_DIVERSIFICATION),
        "lookback": int(PORTFOLIO_MODEL_DIVERSIFICATION_LOOKBACK),
    }
    if not PORTFOLIO_MODEL_DIVERSIFICATION or len(desired or {}) < 2:
        return desired, meta

    diagnostics = _build_portfolio_correlation_diagnostics(
        con,
        desired,
        lookback=int(PORTFOLIO_MODEL_DIVERSIFICATION_LOOKBACK),
        top_n=int(PORTFOLIO_DIAG_CORR_TOP_N),
    )
    model_scores = diagnostics.get("model_scores") or {}
    if not model_scores:
        return desired, meta

    applied = False
    for key in list((desired or {}).keys()):
        tgt = desired.get(key) or {}
        model_id = _normalize_model_id(tgt.get("model_id"))
        score = model_scores.get(model_id) or {}
        factor = float(score.get("diversification_bonus", 1.0) or 1.0)
        if abs(float(factor) - 1.0) <= 1e-9:
            continue
        desired[key]["weight"] = float(desired[key].get("weight", 0.0) or 0.0) * float(factor)
        desired[key].setdefault("reason", {})
        desired[key]["reason"]["model_diversification_bonus"] = float(factor)
        desired[key]["reason"]["model_avg_abs_corr"] = float(score.get("avg_abs_corr", 0.0) or 0.0)
        desired[key]["reason"]["model_crowding"] = float(score.get("crowding", 0.0) or 0.0)
        applied = True

    gross = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
    eff_cap = float(_eff_gross_cap())
    if gross > eff_cap and gross > 1e-12:
        scale = float(eff_cap) / float(gross)
        for key in list(desired.keys()):
            desired[key]["weight"] = float(desired[key].get("weight", 0.0) or 0.0) * float(scale)
            desired[key].setdefault("reason", {})
            desired[key]["reason"]["model_diversification_gross_scale"] = float(scale)
        meta["gross_scale"] = float(scale)

    meta["applied"] = bool(applied)
    meta["model_scores"] = model_scores
    meta["summary"] = {
        "model_summary": dict(diagnostics.get("model_summary") or {}),
    }
    return desired, meta


def _solve_same_direction_scale(gross: float, pair_term: float, limit: float) -> float:
    g = max(0.0, float(gross))
    p = max(0.0, float(pair_term)) * float(PORTFOLIO_EXPOSURE_NETTING_ALPHA)
    limit_cap = max(0.0, float(limit))
    if g <= 1e-12:
        return 1.0
    if (g + p) <= limit_cap + 1e-12:
        return 1.0
    if p <= 1e-12:
        return _clamp(limit_cap / g if g > 1e-12 else 0.0, 0.0, 1.0)
    disc = max(0.0, (g * g) + (4.0 * p * limit_cap))
    scale = (-g + math.sqrt(disc)) / (2.0 * p)
    return _clamp(scale, 0.0, 1.0)


def _apply_side_scale(
    desired: Dict[str, Dict[str, Any]],
    *,
    side: str,
    scale: float,
    reason_key: str,
    reason_value: float,
) -> None:
    target_side = str(side or "").upper().strip()
    sc = _clamp(float(scale), 0.0, 1.0)
    for key in list((desired or {}).keys()):
        tgt = desired.get(key) or {}
        if str(tgt.get("side", "")).upper().strip() != target_side:
            continue
        desired[key]["weight"] = float(desired[key].get("weight", 0.0) or 0.0) * float(sc)
        desired[key].setdefault("reason", {})
        desired[key]["reason"][reason_key] = float(reason_value)
        desired[key]["reason"]["same_direction_netting_side"] = str(target_side)
        desired[key]["reason"]["same_direction_netting_scale"] = float(sc)


def _apply_same_direction_exposure_netting(
    con,
    desired: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    limit = min(float(PORTFOLIO_EXPOSURE_NETTING_LIMIT), float(_eff_gross_cap()))
    meta: Dict[str, Any] = {
        "enabled": bool(PORTFOLIO_EXPOSURE_NETTING),
        "lookback": int(PORTFOLIO_EXPOSURE_NETTING_LOOKBACK),
        "limit": float(limit),
        "alpha": float(PORTFOLIO_EXPOSURE_NETTING_ALPHA),
    }
    if not PORTFOLIO_EXPOSURE_NETTING or len(desired or {}) < 2:
        return desired, meta

    diagnostics = _build_portfolio_correlation_diagnostics(
        con,
        desired,
        lookback=int(PORTFOLIO_EXPOSURE_NETTING_LOOKBACK),
        top_n=int(PORTFOLIO_DIAG_CORR_TOP_N),
    )
    summary = dict(diagnostics.get("position_summary") or {})
    meta["position_summary_pre"] = dict(summary)

    long_scale = _solve_same_direction_scale(
        float(summary.get("long_gross", 0.0) or 0.0),
        float(summary.get("same_direction_pair_long", 0.0) or 0.0),
        float(limit),
    )
    short_scale = _solve_same_direction_scale(
        float(summary.get("short_gross", 0.0) or 0.0),
        float(summary.get("same_direction_pair_short", 0.0) or 0.0),
        float(limit),
    )

    if long_scale < 1.0:
        _apply_side_scale(
            desired,
            side="LONG",
            scale=float(long_scale),
            reason_key="same_direction_netting_pre_long",
            reason_value=float(summary.get("same_direction_effective_long", 0.0) or 0.0),
        )
    if short_scale < 1.0:
        _apply_side_scale(
            desired,
            side="SHORT",
            scale=float(short_scale),
            reason_key="same_direction_netting_pre_short",
            reason_value=float(summary.get("same_direction_effective_short", 0.0) or 0.0),
        )

    meta["long_scale"] = float(long_scale)
    meta["short_scale"] = float(short_scale)
    meta["applied"] = bool(long_scale < 1.0 or short_scale < 1.0)

    diagnostics_post = _build_portfolio_correlation_diagnostics(
        con,
        desired,
        lookback=int(PORTFOLIO_EXPOSURE_NETTING_LOOKBACK),
        top_n=int(PORTFOLIO_DIAG_CORR_TOP_N),
    )
    meta["position_summary_post"] = dict(diagnostics_post.get("position_summary") or {})
    return desired, meta


def _apply_total_portfolio_risk_limit(
    con,
    desired: Dict[str, Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    meta: Dict[str, Any] = {
        "enabled": bool(float(PORTFOLIO_TOTAL_RISK_LIMIT) > 0.0),
        "limit": float(PORTFOLIO_TOTAL_RISK_LIMIT),
    }
    if float(PORTFOLIO_TOTAL_RISK_LIMIT) <= 0.0 or not desired:
        return desired, meta

    try:
        from engine.strategy.risk import portfolio_realized_vol

        total_risk = portfolio_realized_vol(con, desired, lookback=int(PORTFOLIO_CORR_LOOKBACK))
    except Exception:
        total_risk = None

    meta["total_risk_pre"] = float(total_risk or 0.0)
    if total_risk is None or float(total_risk) <= 1e-12:
        meta["scaled"] = False
        return desired, meta

    if float(total_risk) > float(PORTFOLIO_TOTAL_RISK_LIMIT):
        scale = float(PORTFOLIO_TOTAL_RISK_LIMIT) / float(total_risk)
        for key in list(desired.keys()):
            desired[key]["weight"] = float(desired[key].get("weight", 0.0) or 0.0) * float(scale)
            desired[key].setdefault("reason", {})
            desired[key]["reason"]["portfolio_total_risk_pre"] = float(total_risk)
            desired[key]["reason"]["portfolio_total_risk_limit"] = float(PORTFOLIO_TOTAL_RISK_LIMIT)
            desired[key]["reason"]["portfolio_total_risk_scale"] = float(scale)
        meta["scaled"] = True
        meta["scale"] = float(scale)
        meta["total_risk_post"] = float(PORTFOLIO_TOTAL_RISK_LIMIT)
    else:
        meta["scaled"] = False
        meta["total_risk_post"] = float(total_risk)
    return desired, meta


def _load_latest_portfolio_diagnostics(con, top_n: int) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    top_n = max(0, int(top_n))

    for meta_key, out_key in (
        ("last_portfolio_correlation_snapshot", "correlation"),
        ("last_model_diversification", "model_diversification"),
        ("last_exposure_netting", "exposure_netting"),
        ("last_total_portfolio_risk", "total_risk"),
    ):
        raw = _get_meta(con, meta_key)
        if not raw:
            continue
        try:
            out[out_key] = json.loads(raw)
        except Exception:
            out[out_key] = raw

    row = con.execute(
        "SELECT MAX(ts_ms) FROM portfolio_position_corr_snapshots"
    ).fetchone()
    pos_ts = int(row[0] or 0) if row and row[0] is not None else 0
    if pos_ts > 0 and top_n > 0:
        rows = con.execute(
            """
            SELECT model_id_a, symbol_a, model_id_b, symbol_b, corr, same_direction,
                   signed_weight_a, signed_weight_b, abs_weight_product, same_direction_risk
            FROM portfolio_position_corr_snapshots
            WHERE ts_ms=?
            ORDER BY same_direction_risk DESC, ABS(corr) DESC, abs_weight_product DESC
            LIMIT ?
            """,
            (int(pos_ts), int(top_n)),
        ).fetchall()
        out["position_pairs"] = [
            {
                "model_id_a": str(r[0]),
                "symbol_a": str(r[1]),
                "model_id_b": str(r[2]),
                "symbol_b": str(r[3]),
                "corr": float(r[4]),
                "same_direction": bool(int(r[5] or 0)),
                "signed_weight_a": float(r[6]),
                "signed_weight_b": float(r[7]),
                "abs_weight_product": float(r[8]),
                "same_direction_risk": float(r[9]),
            }
            for r in (rows or [])
        ]
        out["position_pairs_ts_ms"] = int(pos_ts)

    row = con.execute(
        "SELECT MAX(ts_ms) FROM portfolio_model_corr_snapshots"
    ).fetchone()
    model_ts = int(row[0] or 0) if row and row[0] is not None else 0
    if model_ts > 0 and top_n > 0:
        rows = con.execute(
            """
            SELECT model_id_a, model_id_b, corr, gross_a, gross_b,
                   weight_product, same_direction_overlap, diversification_score_a, diversification_score_b
            FROM portfolio_model_corr_snapshots
            WHERE ts_ms=?
            ORDER BY ABS(corr) DESC, same_direction_overlap DESC, weight_product DESC
            LIMIT ?
            """,
            (int(model_ts), int(top_n)),
        ).fetchall()
        out["model_pairs"] = [
            {
                "model_id_a": str(r[0]),
                "model_id_b": str(r[1]),
                "corr": float(r[2]),
                "gross_a": float(r[3]),
                "gross_b": float(r[4]),
                "weight_product": float(r[5]),
                "same_direction_overlap": float(r[6]),
                "diversification_score_a": float(r[7]),
                "diversification_score_b": float(r[8]),
            }
            for r in (rows or [])
        ]
        out["model_pairs_ts_ms"] = int(model_ts)

    if pos_ts > 0 or model_ts > 0:
        out["ts_ms"] = int(max(pos_ts, model_ts))
    return out


def _stdev(xs):
    xs = [float(x) for x in (xs or [])]
    n = len(xs)
    if n < 3:
        return None
    m = sum(xs) / n
    v = sum((x - m) * (x - m) for x in xs) / (n - 1)
    s = (v ** 0.5) if v > 0 else 0.0
    return float(s)


def _vix_stress(con, lookback: int = 180) -> dict:
    """
    Reads VIX from prices table where symbol='VIX'.
    Returns:
      level, z (zscore vs trailing window), change_1 (last - prev)
    Fail-soft: returns zeros if unavailable.
    """
    try:
        rows = con.execute(
            """
            SELECT price
            FROM prices
            WHERE symbol='VIX'
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(lookback),),
        ).fetchall()
        v = [float(r[0]) for r in (rows or []) if r and r[0] is not None]
        v.reverse()
        if len(v) < 5:
            return {"level": 0.0, "z": 0.0, "change_1": 0.0}

        level = float(v[-1])
        change_1 = float(level - float(v[-2]))

        # zscore vs trailing (use last up to 60 points if available)
        w = v[-60:] if len(v) >= 60 else v
        s = _stdev(w)
        if s is None or s <= 1e-12:
            z = 0.0
        else:
            m = sum(w) / len(w)
            z = (level - float(m)) / float(s)
            if z != z:  # NaN
                z = 0.0

        return {"level": float(level), "z": float(z), "change_1": float(change_1)}
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_VIX_STRESS_SNAPSHOT_FAILED", e, once_key="vix_stress_snapshot")
        snapshot = {"level": 0.0, "z": 0.0, "change_1": 0.0}
        return snapshot


def _stress_factor_from_vix_z(z: float) -> float:
    """
    Piecewise-linear compression:
      z <= TH        => 1.0
      z >= Z_AT_MIN  => MIN_FACTOR
      else linear in-between
    """
    try:
        z = float(z)
    except Exception:
        z = 0.0

    th = float(PORTFOLIO_STRESS_VIX_Z_TH)
    zmin = float(PORTFOLIO_STRESS_Z_AT_MIN)
    fmin = float(PORTFOLIO_STRESS_MIN_FACTOR)

    if z <= th:
        return 1.0
    if z >= zmin:
        return float(_clamp(fmin, 0.0, 1.0))

    # interpolate from 1.0 at th down to fmin at zmin
    t = (z - th) / max(1e-9, (zmin - th))
    f = 1.0 + (float(_clamp(fmin, 0.0, 1.0)) - 1.0) * float(_clamp(t, 0.0, 1.0))
    return float(_clamp(f, 0.0, 1.0))


def _symbol_cap(symbol: str) -> float:
    # symbol-specific cap if provided, else global cap
    if symbol in PORTFOLIO_SYMBOL_CAPS:
        try:
            return float(PORTFOLIO_SYMBOL_CAPS[symbol])
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_SYMBOL_CAP_FAILED",
                e,
                once_key=f"symbol_cap:{symbol}",
                symbol=str(symbol),
            )
            fallback_cap = float(PORTFOLIO_MAX_W_PER_SYMBOL)
            return fallback_cap
    return float(PORTFOLIO_MAX_W_PER_SYMBOL)


def _last_price_age_s(con, symbol: str, now_ms: int) -> Optional[float]:
    try:
        r = con.execute(
            "SELECT ts_ms FROM prices WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1",
            (str(symbol),),
        ).fetchone()
        if not r or r[0] is None:
            return None
        age_s = (int(now_ms) - int(r[0])) / 1000.0
        if not math.isfinite(age_s):
            return None
        return float(max(0.0, age_s))
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_LAST_PRICE_AGE_FAILED",
            e,
            once_key=f"last_price_age:{symbol}",
            symbol=str(symbol),
        )
        age_s = None
        return age_s


def _exec_realism_factor(con, symbol: str, now_ms: int) -> Tuple[float, Dict[str, float]]:
    """
    Returns (factor, meta). factor in [0,1].
    Hard block: stale prices beyond PORTFOLIO_EXEC_MAX_PRICE_AGE_S.
    Soft throttle: Market Stress Score above threshold.
    """
    meta: Dict[str, float] = {
        "staleness_sec": 0.0,
        "stress_score": 0.0,
    }

    f = 1.0

    # staleness (hard + soft)
    age_s = _last_price_age_s(con, symbol, int(now_ms))
    if age_s is None:
        age_s = 0.0
    meta["staleness_sec"] = float(age_s)

    max_age = float(PORTFOLIO_EXEC_MAX_PRICE_AGE_S)
    if max_age > 0 and age_s > max_age:
        return 0.0, meta
    if max_age > 0 and age_s > (0.5 * max_age):
        f *= float(PORTFOLIO_EXEC_STALE_HALF_FACTOR)

    # global stress (read-only)
    try:
        from engine.strategy.market_stress import get_market_stress_snapshot

        ms = get_market_stress_snapshot(con=con, ts_ms=int(now_ms)) or {}
        stress = float(ms.get("stress_score", 0.0))
        if not math.isfinite(stress):
            stress = 0.0
    except Exception:
        stress = 0.0
    meta["stress_score"] = float(stress)

    if stress >= float(PORTFOLIO_EXEC_STRESS_TH):
        f *= float(PORTFOLIO_EXEC_STRESS_FACTOR)

    if not math.isfinite(f):
        f = 1.0
    f = float(max(0.0, min(1.0, f)))
    return f, meta


def _execution_realism_factor(con, symbol: str, now_ms: int) -> Tuple[float, Dict[str, float]]:
    """
    Returns (factor, meta) where factor in [0,1].
    Fail-soft: if we can't compute anything, returns (1.0, meta with zeros).
    Hard block: stale prices beyond PORTFOLIO_EXEC_MAX_PRICE_AGE_S => factor=0.
    """
    meta: Dict[str, float] = {
        "staleness_sec": 0.0,
        "stress_vix_z_60": 0.0,
        "atr_pct": 0.0,
        "slippage_bps_est": 0.0,
    }

    f = 1.0

    # 1) Per-symbol staleness (hard safety + soft throttle)
    age_s = _last_price_age_s(con, symbol, int(now_ms))
    if age_s is None:
        # Unknown staleness => do not hard-block here (kill_switch may still block execution).
        age_s = 0.0
    meta["staleness_sec"] = float(age_s)

    max_age = float(PORTFOLIO_EXEC_MAX_PRICE_AGE_S)
    if max_age > 0 and age_s > max_age:
        return 0.0, meta
    if max_age > 0 and age_s > (0.5 * max_age):
        f *= float(PORTFOLIO_EXEC_STALE_HALF_FACTOR)

    # 2) Optional market stress + volatility proxies via tech_indicators (price-only)
    try:
        from engine.strategy.tech_indicators import compute_tech_features

        tf = compute_tech_features(str(symbol), int(now_ms)) or {}
    except Exception:
        tf = {}

    try:
        vix_z = float(tf.get("stress_vix_z_60", 0.0))
        if not math.isfinite(vix_z):
            vix_z = 0.0
    except Exception:
        vix_z = 0.0
    meta["stress_vix_z_60"] = float(vix_z)

    try:
        atr_pct = float(tf.get("atr_pct", 0.0))
        if not math.isfinite(atr_pct):
            atr_pct = 0.0
    except Exception:
        atr_pct = 0.0
    meta["atr_pct"] = float(atr_pct)

    # Stress throttle
    if vix_z > float(PORTFOLIO_EXEC_VIX_Z_TH):
        f *= float(PORTFOLIO_EXEC_VIX_FACTOR)

    # Volatility throttle: if atr_pct above threshold, scale down ~ (th/atr_pct)
    atr_th = float(PORTFOLIO_EXEC_ATR_PCT_TH)
    if atr_th > 0 and atr_pct > atr_th:
        f *= float(max(0.0, min(1.0, atr_th / max(1e-12, atr_pct))))

    # Slippage estimate (audit / explainability)
    try:
        slip = float(atr_pct) * 1e4 * float(PORTFOLIO_EXEC_ATR_SLIP_MULT)
        if not math.isfinite(slip):
            slip = 0.0
    except Exception:
        slip = 0.0
    meta["slippage_bps_est"] = float(max(0.0, slip))

    # Clamp
    if not math.isfinite(f):
        f = 1.0
    f = float(max(0.0, min(1.0, f)))
    return f, meta


# How strongly novelty boosts a signal:
# final_score = base_score * (1 + NOVELTY_ALPHA * novelty)
PORTFOLIO_NOVELTY_ALPHA = float(os.environ.get("PORTFOLIO_NOVELTY_ALPHA", "0.50"))
# Exploration controls: cap weight for symbols with low history
PORTFOLIO_EXPLORE_MIN_LABELS = int(os.environ.get("PORTFOLIO_EXPLORE_MIN_LABELS", "5"))
PORTFOLIO_EXPLORE_MAX_W = float(os.environ.get("PORTFOLIO_EXPLORE_MAX_W", "0.15"))
# ------------------------------------------------------
# Shadow Strategy Auto Promotion
# ------------------------------------------------------

SHADOW_PROMOTION_LOOKBACK = int(os.environ.get("SHADOW_PROMOTION_LOOKBACK", "200"))
SHADOW_PROMOTION_THRESHOLD = float(os.environ.get("SHADOW_PROMOTION_THRESHOLD", "1.10"))
SHADOW_PROMOTION_MIN_RUNS = int(os.environ.get("SHADOW_PROMOTION_MIN_RUNS", "30"))  # 5% cap for "new" symbols
# =========================
# SECTION 3 / ~200 lines
# =========================

def _novelty_from_explain(explain_json: str) -> float:
    try:
        x = json.loads(explain_json or "{}")
        meta = x.get("event_meta") if isinstance(x, dict) else None
        if not isinstance(meta, dict):
            return 0.0
        v = float(meta.get("novelty", 0.0))
        if v != v:
            return 0.0
        return max(0.0, min(1.0, v))
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_EXPLAIN_NOVELTY_FAILED", e, once_key="explain_novelty")
        novelty = 0.0
        return novelty


def _safe_json_obj(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not isinstance(raw, str):
        return {}
    try:
        parsed = json.loads(raw or "{}")
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_SAFE_JSON_OBJ_FAILED",
            e,
            once_key="safe_json_obj",
            raw_type=type(raw).__name__,
        )
        fallback_obj: Dict[str, Any] = {}
        return fallback_obj
    return dict(parsed) if isinstance(parsed, dict) else {}


def _coerce_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in ("1", "true", "yes", "y", "on", "enter", "trade", "buy", "sell", "long", "short"):
        return True
    if s in ("0", "false", "no", "n", "off", "hold", "flat", "skip", "none"):
        return False
    return None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_COERCE_FLOAT_FAILED",
            e,
            once_key="coerce_float_failed",
            value_type=type(value).__name__,
        )
        coerced = None
        return coerced
    return out if math.isfinite(out) else None


def _intent_container_candidates(explain: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(explain, dict):
        return []
    out: List[Dict[str, Any]] = []
    canonical = explain.get("model_intent")
    if is_canonical_model_intent(canonical):
        return [_dict_str_any(canonical)]
    keys = (
        "model_intent",
        "model_output",
        "portfolio_decision",
        "trade_decision",
        "decision",
        "signal",
        "strategy_output",
        "portfolio",
        "execution",
    )
    for key in keys:
        val = explain.get(key)
        if isinstance(val, dict):
            out.append(dict(val))
    direct_keys = {
        "should_trade",
        "trade",
        "action",
        "target_weight",
        "portfolio_weight",
        "position_size",
        "size_mult",
        "selection_score",
        "trade_score",
        "include_in_universe",
        "universe_score",
        "selected_features",
        "features_used",
    }
    if any(k in explain for k in direct_keys):
        out.append(dict(explain))
    return out


def _extract_model_intent_from_explain(explain_json: str) -> Dict[str, Any]:
    explain = _safe_json_obj(explain_json)
    canonical = explain.get("model_intent")
    if is_canonical_model_intent(canonical):
        return _dict_str_any(canonical)
    intent: Dict[str, Any] = {}

    for container in _intent_container_candidates(explain):
        if not isinstance(container, dict):
            continue

        for key in ("selection_score", "trade_score", "score", "prediction_strength", "priority", "rank_score"):
            val = _coerce_float(container.get(key))
            if val is not None:
                intent["score"] = float(val)
                break

        for key in ("target_weight", "portfolio_weight", "target_exposure", "notional_frac", "size", "position_size"):
            val = _coerce_float(container.get(key))
            if val is not None:
                intent["target_weight"] = float(val)
                break

        for key in ("size_mult", "size_factor", "allocation_multiplier", "weight_multiplier"):
            val = _coerce_float(container.get(key))
            if val is not None:
                intent["size_mult"] = float(val)
                break

        for key in ("confidence", "signal_confidence", "trade_confidence", "probability"):
            val = _coerce_float(container.get(key))
            if val is not None:
                intent["confidence"] = float(val)
                break

        for key in ("prediction_strength", "signal_strength", "strength"):
            val = _coerce_float(container.get(key))
            if val is not None:
                intent["prediction_strength"] = float(val)
                intent.setdefault("score", float(val))
                break

        for key in ("expected_z", "predicted_z", "signal_z"):
            val = _coerce_float(container.get(key))
            if val is not None:
                intent["expected_z"] = float(val)
                break

        for key in ("side", "direction", "action"):
            raw = container.get(key)
            if raw is None:
                continue
            side = str(raw).strip().upper()
            if side in ("BUY", "LONG"):
                intent["side"] = "LONG"
                break
            if side in ("SELL", "SHORT"):
                intent["side"] = "SHORT"
                break
            if side in ("FLAT", "HOLD", "SKIP", "NONE"):
                intent["side"] = "FLAT"
                break

        for key in ("should_trade", "trade", "enter", "allow_trade"):
            val = _coerce_bool(container.get(key))
            if val is not None:
                intent["should_trade"] = bool(val)
                break

        for key in ("timing", "entry_timing", "trade_timing", "when"):
            raw = container.get(key)
            if raw is None:
                continue
            timing = str(raw).strip().lower()
            if timing:
                intent["timing"] = timing
                break

        for key in ("selected_features", "features_used", "feature_ids", "feature_set"):
            raw = container.get(key)
            if isinstance(raw, list):
                feats = [str(v).strip() for v in raw if str(v or "").strip()]
                if feats:
                    intent["selected_features"] = feats
                    break

        for key in ("include_in_universe", "universe_include", "promote_symbol"):
            val = _coerce_bool(container.get(key))
            if val is not None:
                intent["include_in_universe"] = bool(val)
                break

        for key in ("universe_score", "universe_rank", "rank"):
            val = _coerce_float(container.get(key))
            if val is not None:
                intent["universe_score"] = float(val)
                break

    return intent


def _has_explicit_model_trade_intent(intent: Optional[Dict[str, Any]]) -> bool:
    intent = intent or {}
    return any(
        key in intent
        for key in ("should_trade", "target_weight", "score", "side", "timing", "selected_features")
    )


def _has_canonical_model_trade_intent(intent: Optional[Dict[str, Any]]) -> bool:
    return is_canonical_model_intent(intent) and _has_explicit_model_trade_intent(intent)


def _model_intent_allows_symbol(intent: Optional[Dict[str, Any]]) -> bool:
    intent = intent or {}
    if bool(intent.get("include_in_universe")):
        return True
    if _coerce_float(intent.get("universe_score")) is not None:
        return True
    if bool(intent.get("should_trade")):
        return True
    if _coerce_float(intent.get("target_weight")) is not None:
        return True
    return False


def _alert_effective_signal(a: Dict[str, Any]) -> Tuple[float, float]:
    intent = (a or {}).get("_model_intent")
    z = _coerce_float((intent or {}).get("expected_z"))
    c = _coerce_float((intent or {}).get("confidence"))
    if z is None:
        z = _coerce_float((a or {}).get("expected_z"))
    if c is None:
        c = _coerce_float((a or {}).get("confidence"))
    return float(z or 0.0), float(c or 0.0)


def _model_intent_trade_allowed(a: Dict[str, Any]) -> bool:
    intent = ((a or {}).get("_model_intent") or {})
    should_trade = _coerce_bool(intent.get("should_trade"))
    if should_trade is False:
        return False
    timing = str(intent.get("timing") or "").strip().lower()
    if timing in ("hold", "skip", "wait", "defer", "flat"):
        return False
    side = str(intent.get("side") or "").strip().upper()
    if side == "FLAT":
        return False
    return True


def _score_from_alert(z: float, conf: float, severity: str, explain_json: str) -> float:
    # core score: abs(z)*conf
    # small severity bump (already gated by rules)
    s = abs(float(z)) * float(conf)
    sev = (severity or "").upper()
    if sev == "CRIT":
        s *= 1.15
    elif sev == "HIGH":
        s *= 1.08

    novelty = _novelty_from_explain(explain_json)
    s *= (1.0 + float(PORTFOLIO_NOVELTY_ALPHA) * float(novelty))
    return float(s)


def _tradability_from_explain(explain_json: str) -> Dict[str, float]:
    try:
        ex = json.loads(explain_json or "{}")
        tr = ex.get("tradability") or {}
        return {
            "expected_ret_net": float(tr.get("expected_ret_net", 0.0)),
            "p_win": float(tr.get("p_win", 0.5)),
            "expected_dd": float(tr.get("expected_dd", 0.0)),
        }
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_TRADABILITY_PARSE_FAILED", e, once_key="tradability_from_explain")
        tradability = {
            "expected_ret_net": 0.0,
            "p_win": 0.5,
            "expected_dd": 0.0,
        }
        return tradability


def _black_litterman_reason_from_target(tgt: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    reason = dict((tgt or {}).get("reason") or {}) if isinstance((tgt or {}).get("reason"), dict) else {}
    payload = reason.get("black_litterman")
    return dict(payload or {}) if isinstance(payload, dict) else {}


def _expected_ret_net_from_target(tgt: Optional[Dict[str, Any]]) -> float:
    bl = _black_litterman_reason_from_target(tgt)
    for candidate in (
        (tgt or {}).get("adjusted_expected_ret_net"),
        bl.get("adjusted_expected_ret_net"),
    ):
        val = _coerce_float(candidate)
        if val is not None:
            return float(val)

    tr = _tradability_from_explain(str((tgt or {}).get("explain_json", "{}") or "{}"))
    return float(tr.get("expected_ret_net", 0.0) or 0.0)


def _black_litterman_prediction_payload(desired_key: str, tgt: Dict[str, Any]) -> Dict[str, Any]:
    explain = _safe_json_obj(str((tgt or {}).get("explain_json", "{}") or "{}"))
    tradability = dict(explain.get("tradability") or {}) if isinstance(explain.get("tradability"), dict) else {}
    model_intent = dict(explain.get("model_intent") or {}) if isinstance(explain.get("model_intent"), dict) else {}
    ensemble_output = dict(explain.get("ensemble_output") or {}) if isinstance(explain.get("ensemble_output"), dict) else {}
    reason = dict((tgt or {}).get("reason") or {}) if isinstance((tgt or {}).get("reason"), dict) else {}

    prediction = None
    for candidate in (
        tradability.get("expected_ret_net"),
        model_intent.get("expected_ret_net"),
        model_intent.get("expected_z"),
        (tgt or {}).get("expected_ret_net"),
        (tgt or {}).get("expected_z"),
        reason.get("expected_ret_net"),
        reason.get("expected_z"),
        ensemble_output.get("blended_prediction"),
    ):
        val = _coerce_float(candidate)
        if val is not None:
            prediction = float(val)
            break

    confidence = None
    for candidate in (
        model_intent.get("confidence"),
        reason.get("confidence"),
        ensemble_output.get("blended_confidence"),
        (tgt or {}).get("confidence"),
    ):
        val = _coerce_float(candidate)
        if val is not None:
            confidence = float(max(0.01, min(0.99, val)))
            break
    if confidence is None:
        confidence = float(max(0.01, min(0.99, BLACK_LITTERMAN_VIEW_CONFIDENCE)))

    uncertainty = None
    for candidate in (
        model_intent.get("uncertainty"),
        ensemble_output.get("uncertainty"),
    ):
        val = _coerce_float(candidate)
        if val is not None:
            uncertainty = float(max(1e-6, val))
            break
    if uncertainty is None:
        uncertainty = float(max(1e-6, 1.0 - confidence))

    payload: Dict[str, Any] = {
        "asset_key": str(desired_key),
        "symbol": str(_desired_symbol(desired_key, tgt) or ""),
        "confidence": float(confidence),
        "uncertainty": float(uncertainty),
    }
    if prediction is not None:
        payload["prediction"] = float(prediction)
    if ensemble_output:
        payload["ensemble_output"] = dict(ensemble_output)
    return payload


def _annotate_black_litterman_fallback(
    desired: Dict[str, Dict[str, Any]],
    *,
    fallback_reason: str,
) -> Dict[str, Dict[str, Any]]:
    for desired_key, tgt in list((desired or {}).items()):
        raw_ret = _tradability_from_explain(str((tgt or {}).get("explain_json", "{}") or "{}")).get("expected_ret_net", 0.0)
        tgt.setdefault("reason", {})
        if not isinstance(tgt["reason"], dict):
            tgt["reason"] = {"raw": tgt["reason"]}
        tgt["adjusted_expected_ret_net"] = float(raw_ret or 0.0)
        tgt["reason"]["black_litterman"] = {
            "enabled": True,
            "applied": False,
            "fallback_reason": str(fallback_reason),
            "raw_expected_ret_net": float(raw_ret or 0.0),
            "adjusted_expected_ret_net": float(raw_ret or 0.0),
            "tau": float(max(1e-6, BLACK_LITTERMAN_TAU)),
        }
    return desired


def _build_black_litterman_covariance(
    con,
    desired: Dict[str, Dict[str, Any]],
    asset_keys: List[str],
) -> Tuple[Optional[List[List[float]]], Dict[str, Any]]:
    from engine.strategy.risk import corr_from_prices, realized_vol_from_prices

    lookback = int(max(2, PORTFOLIO_CORR_LOOKBACK))
    symbols: List[str] = []
    vols: List[float] = []
    missing_symbols: List[str] = []

    for desired_key in list(asset_keys or []):
        tgt = dict((desired or {}).get(desired_key) or {})
        sym = str(_desired_symbol(desired_key, tgt) or "").upper().strip()
        if not sym:
            missing_symbols.append(str(desired_key))
            symbols.append("")
            vols.append(0.0)
            continue
        symbols.append(sym)
        try:
            vol = realized_vol_from_prices(con, sym, lookback=int(lookback))
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_BLACK_LITTERMAN_VOL_FAILED",
                e,
                once_key=f"portfolio_black_litterman_vol:{sym}",
                symbol=str(sym),
            )
            vol = None
        if vol is None or not math.isfinite(float(vol)) or float(vol) <= 0.0:
            missing_symbols.append(str(sym))
            vols.append(0.0)
        else:
            vols.append(float(vol))

    if missing_symbols:
        return None, {
            "fallback_reason": "missing_covariance",
            "missing_symbols": list(dict.fromkeys(missing_symbols)),
            "lookback": int(lookback),
        }

    size = len(asset_keys or [])
    cov = [[0.0 for _ in range(size)] for _ in range(size)]
    for i in range(size):
        cov[i][i] = float(vols[i]) * float(vols[i])

    for i in range(size):
        for j in range(i + 1, size):
            if symbols[i] == symbols[j]:
                corr = 1.0
            else:
                try:
                    corr = corr_from_prices(con, symbols[i], symbols[j], lookback=int(lookback))
                except Exception as e:
                    _warn_nonfatal(
                        "PORTFOLIO_BLACK_LITTERMAN_CORR_FAILED",
                        e,
                        once_key=f"portfolio_black_litterman_corr:{symbols[i]}:{symbols[j]}",
                        left_symbol=str(symbols[i]),
                        right_symbol=str(symbols[j]),
                    )
                    corr = None
            corr_value = _clamp(float(corr), -1.0, 1.0) if corr is not None else 0.0
            cov_ij = float(vols[i]) * float(vols[j]) * float(corr_value)
            cov[i][j] = float(cov_ij)
            cov[j][i] = float(cov_ij)

    return cov, {
        "lookback": int(lookback),
        "symbols": list(symbols),
        "vols": list(vols),
    }


def _apply_black_litterman_overlay(con, desired: Dict[str, Dict]) -> Dict[str, Dict]:
    if not desired or not BLACK_LITTERMAN_ENABLED:
        return desired

    model_predictions = {
        str(desired_key): _black_litterman_prediction_payload(str(desired_key), dict(tgt or {}))
        for desired_key, tgt in list((desired or {}).items())
    }
    views = build_view_matrix(model_predictions)
    asset_keys = [str(value) for value in list(views.get("assets") or [])]
    view_count = int(getattr(views.get("matrix"), "shape", (0, 0))[0])
    if not asset_keys or view_count <= 0:
        return _annotate_black_litterman_fallback(desired, fallback_reason="no_views")

    cov_matrix, cov_meta = _build_black_litterman_covariance(con, desired, asset_keys)
    if cov_matrix is None:
        return _annotate_black_litterman_fallback(
            desired,
            fallback_reason=str((cov_meta or {}).get("fallback_reason") or "missing_covariance"),
        )

    equilibrium = compute_equilibrium_returns(cov_matrix)
    posterior = black_litterman_posterior(
        cov_matrix,
        equilibrium,
        views,
        views.get("uncertainty"),
    )

    posterior_returns = posterior.get("posterior_returns")
    equilibrium_returns = posterior.get("equilibrium_returns")
    if posterior_returns is None or equilibrium_returns is None:
        return _annotate_black_litterman_fallback(desired, fallback_reason="posterior_unavailable")

    view_assets = [str(value) for value in list(posterior.get("view_assets") or [])]
    view_index = {asset_key: idx for idx, asset_key in enumerate(view_assets)}
    view_returns_raw = posterior.get("view_returns")
    view_confidence_raw = posterior.get("view_confidence")
    view_returns = list(view_returns_raw) if view_returns_raw is not None else []
    view_confidence = list(view_confidence_raw) if view_confidence_raw is not None else []
    uncertainty_matrix = posterior.get("uncertainty")

    for idx, desired_key in enumerate(asset_keys):
        tgt = desired.get(desired_key)
        if not isinstance(tgt, dict):
            continue
        raw_ret = _tradability_from_explain(str(tgt.get("explain_json", "{}") or "{}")).get("expected_ret_net", 0.0)
        adjusted_ret = float(posterior_returns[idx])
        equilibrium_ret = float(equilibrium_returns[idx])

        tgt.setdefault("reason", {})
        if not isinstance(tgt["reason"], dict):
            tgt["reason"] = {"raw": tgt["reason"]}
        tgt["adjusted_expected_ret_net"] = float(adjusted_ret)

        bl_reason: Dict[str, Any] = {
            "enabled": True,
            "applied": bool(posterior.get("applied", False)),
            "raw_expected_ret_net": float(raw_ret or 0.0),
            "adjusted_expected_ret_net": float(adjusted_ret),
            "equilibrium_return": float(equilibrium_ret),
            "tau": float(posterior.get("tau") or BLACK_LITTERMAN_TAU),
            "covariance_symbols": list((cov_meta or {}).get("symbols") or []),
            "covariance_lookback": int((cov_meta or {}).get("lookback") or PORTFOLIO_CORR_LOOKBACK),
        }
        if desired_key in view_index:
            v_idx = int(view_index[desired_key])
            if v_idx < len(view_returns):
                bl_reason["view_return"] = float(view_returns[v_idx])
            if v_idx < len(view_confidence):
                bl_reason["view_confidence"] = float(view_confidence[v_idx])
            try:
                if uncertainty_matrix is not None:
                    bl_reason["view_uncertainty"] = float(uncertainty_matrix[v_idx][v_idx])
            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_BLACK_LITTERMAN_VIEW_UNCERTAINTY_FAILED",
                    e,
                    once_key=f"portfolio_black_litterman_view_uncertainty:{desired_key}",
                    symbol=str(desired_key[0]),
                    horizon_s=int(desired_key[1]),
                    view_index=int(v_idx),
                )
        tgt["reason"]["black_litterman"] = bl_reason

    return desired


def _latest_price_ts_ms(con, symbol: str) -> Optional[int]:
    """
    Best-effort lookup of latest price timestamp for symbol.
    Assumes `prices(symbol, ts_ms, px)` exists (used elsewhere in your codebase).
    """
    try:
        row = con.execute(
            """
            SELECT ts_ms
            FROM prices
            WHERE symbol=?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(symbol),),
        ).fetchone()
        if not row:
            return None
        return int(row[0])
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_LATEST_PRICE_TS_FAILED",
            e,
            once_key=f"latest_price_ts:{symbol}",
            symbol=str(symbol),
        )
        latest_ts = None
        return latest_ts


def _is_price_fresh(con, symbol: str, now_ms: int) -> bool:
    ts = _latest_price_ts_ms(con, symbol)
    if ts is None:
        return False
    age_s = max(0.0, (int(now_ms) - int(ts)) / 1000.0)
    return age_s <= float(PORTFOLIO_MAX_PRICE_STALE_S)


def _desired_weight(score: float, symbol: str) -> float:
    # weight proportional to score, capped by symbol cap and gross cap later
    w = (float(score) / float(PORTFOLIO_SCORE_NORM)) * float(PORTFOLIO_GROSS_CAP)
    w = _clamp(w, 0.0, _symbol_cap(symbol))
    return float(w)


def _resolve_desired_weight(alert: Dict[str, Any], score: float, symbol: str) -> float:
    intent = ((alert or {}).get("_model_intent") or {})
    target_weight = _coerce_float(intent.get("target_weight"))
    size_mult = _coerce_float(intent.get("size_mult"))

    if target_weight is not None:
        w = abs(float(target_weight))
    else:
        w = _desired_weight(score, symbol)

    if size_mult is not None:
        w = float(w) * max(0.0, float(size_mult))

    w = _clamp(w, 0.0, _symbol_cap(symbol))
    return float(w)


def _strategy_candidate_limit(alerts: List[Dict], default_limit: int) -> int:
    default_n = max(1, int(default_limit or 1))
    canonical_count = 0
    for alert in alerts or []:
        intent = (alert or {}).get("_model_intent")
        if not _has_canonical_model_trade_intent(intent):
            continue
        if not _model_intent_trade_allowed(alert):
            continue
        canonical_count += 1

    if canonical_count <= 0:
        return int(default_n)

    explicit_cap = int(PORTFOLIO_MODEL_INTENT_MAX_POSITIONS)
    if explicit_cap > 0:
        return max(1, min(int(canonical_count), explicit_cap))
    return max(int(default_n), int(canonical_count))


def _merge_model_intent_reason(reason: Dict[str, Any], alert: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(reason or {})
    intent = ((alert or {}).get("_model_intent") or {})
    if not intent:
        return out
    out["model_intent"] = dict(intent)
    if intent.get("selected_features"):
        out["selected_features"] = list(intent.get("selected_features") or [])
    if intent.get("timing"):
        out["trade_timing"] = str(intent.get("timing"))
    if intent.get("target_weight") is not None:
        out["model_target_weight"] = _safe_float(intent.get("target_weight"))
    if intent.get("size_mult") is not None:
        out["model_size_mult"] = _safe_float(intent.get("size_mult"))
    if intent.get("score") is not None:
        out["model_score"] = _safe_float(intent.get("score"))
    if intent.get("prediction_strength") is not None:
        out["prediction_strength"] = _safe_float(intent.get("prediction_strength"))
    if intent.get("include_in_universe") is not None:
        out["model_universe_include"] = bool(intent.get("include_in_universe"))
    if intent.get("universe_score") is not None:
        out["model_universe_score"] = _safe_float(intent.get("universe_score"))
    return out


def _load_recent_alert_candidates(con, lookback_s: int) -> List[Dict]:
    from engine.runtime.state_cache import cache_get_or_load

    now_ms = _now_ms()
    cutoff_ms = int(now_ms) - int(lookback_s) * 1000
    cache_key = f"recent_candidates:{int(lookback_s)}"

    def _load() -> List[Dict]:
        # Dynamic universe filter: only consider ACTIVE + WATCH symbols
        try:
            allowed = set(get_active_symbols(con, limit=int(os.environ.get("PORTFOLIO_SYMBOL_LIMIT", "5000"))))
        except Exception:
            allowed = set()

        rows = con.execute(
            """
            SELECT id, ts_ms, prediction_id, symbol, horizon_s, expected_z, confidence, severity, event_title, explain_json
            FROM alerts
            WHERE ts_ms >= ?
            ORDER BY ts_ms DESC
            """,
            (int(cutoff_ms),),
        ).fetchall()

        out = []
        for r in rows or []:
            try:
                sym = str(r[3] or "").strip().upper()
                if not sym:
                    continue

                explain_json = str(r[9] or "{}")
                explain_obj = _safe_json_obj(explain_json)
                signal_payload = describe_signal_confidence(
                    expected_z=float(r[5] or 0.0),
                    confidence=float(r[6] or 0.0),
                    raw_confidence=((explain_obj.get("confidence_engine") or {}).get("raw_confidence") if isinstance(explain_obj.get("confidence_engine"), dict) else r[6]),
                    horizon_s=int(r[4] or 0),
                    calibration=((explain_obj.get("confidence_engine") or {}).get("calibration") if isinstance(explain_obj.get("confidence_engine"), dict) else None),
                    signal_ts_ms=int(r[1] or 0),
                    now_ms=int(now_ms),
                    apply_decay=True,
                )
                model_intent = _extract_model_intent_from_explain(explain_json)
                decay = dict(signal_payload.get("decay") or {})
                decay_mult = float(decay.get("multiplier") or 1.0)
                model_intent["confidence"] = float(signal_payload["confidence"])
                model_intent["probability"] = float(signal_payload["probability"])
                model_intent["uncertainty"] = float(signal_payload["uncertainty"])
                model_intent["prediction_strength"] = float(signal_payload["prediction_strength"])
                if model_intent.get("score") is not None:
                    model_intent["score"] = float(model_intent.get("score") or 0.0) * float(decay_mult)
                else:
                    model_intent["score"] = float(signal_payload["prediction_strength"])
                if model_intent.get("size_mult") is not None:
                    model_intent["size_mult"] = float(model_intent.get("size_mult") or 0.0) * float(decay_mult)
                else:
                    model_intent["size_mult"] = float(signal_payload["size_mult"])

                if allowed and sym not in allowed:
                    if not _model_intent_allows_symbol(model_intent):
                        continue
                    row = con.execute(
                        """
                        SELECT 1
                        FROM prices
                        WHERE symbol=?
                        ORDER BY ts_ms DESC
                        LIMIT 1
                        """,
                        (str(sym),),
                    ).fetchone()
                    if not row:
                        continue

                out.append(
                    {
                        "id": int(r[0]),
                        "ts_ms": int(r[1]),
                        "prediction_id": (_safe_int(r[2], 0) if r[2] not in (None, "") else None),
                        "symbol": sym,
                        "horizon_s": int(r[4]),
                        "expected_z": float(r[5]),
                        "confidence": float(signal_payload["confidence"]),
                        "probability": float(signal_payload["probability"]),
                        "uncertainty": float(signal_payload["uncertainty"]),
                        "raw_confidence": float(signal_payload["raw_confidence"]),
                        "prediction_strength": float(signal_payload["prediction_strength"]),
                        "signal_age_s": float(decay.get("signal_age_s") or 0.0),
                        "signal_decay_mult": float(decay.get("multiplier") or 1.0),
                        "severity": str(r[7] or ""),
                        "event_title": str(r[8] or ""),
                        "explain_json": explain_json,
                        "_model_intent": model_intent,
                    }
                )
            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_ALERT_CANDIDATE_ROW_FAILED",
                    e,
                    symbol=str(r[0] or "") if len(r) > 0 else "",
                )
                continue
        return out

    return cache_get_or_load("alerts", cache_key, _load, ttl_s=0.75)


def _alerts_support_lifecycle_tracking(con) -> bool:
    try:
        cols = {
            str(row[1]).strip().lower()
            for row in (con.execute("PRAGMA table_info(alerts)").fetchall() or [])
        }
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_ALERT_LIFECYCLE_SCHEMA_READ_FAILED", e, once_key="alert_lifecycle_schema_read")
        return False
    required = {
        "portfolio_first_seen_ts_ms",
        "portfolio_last_seen_ts_ms",
        "portfolio_consumed_ts_ms",
        "portfolio_expired_ts_ms",
        "portfolio_status",
    }
    return bool(required.issubset(cols))


def _invalidate_alert_candidate_cache() -> None:
    try:
        from engine.runtime.state_cache import cache_invalidate_namespace

        cache_invalidate_namespace("alerts")
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_ALERT_CACHE_INVALIDATE_FAILED", e, once_key="alert_cache_invalidate")


def _mark_alert_candidates_seen(con, alert_ids: List[Any], now_ms: int) -> None:
    if not _alerts_support_lifecycle_tracking(con):
        return
    ids = sorted({_safe_int(alert_id) for alert_id in (alert_ids or []) if _safe_int(alert_id) > 0})
    if not ids:
        return
    try:
        placeholders = ",".join("?" for _ in ids)
        con.execute(
            f"""
            UPDATE alerts
            SET portfolio_first_seen_ts_ms = CASE
                    WHEN COALESCE(portfolio_first_seen_ts_ms, 0) > 0 THEN portfolio_first_seen_ts_ms
                    ELSE ?
                END,
                portfolio_last_seen_ts_ms = ?,
                portfolio_status = CASE
                    WHEN COALESCE(portfolio_status, 'new') IN ('consumed', 'expired') THEN portfolio_status
                    ELSE 'seen'
                END
            WHERE id IN ({placeholders})
            """,
            (int(now_ms), int(now_ms), *ids),
        )
        _invalidate_alert_candidate_cache()
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_ALERT_SEEN_UPDATE_FAILED",
            e,
            once_key=None,
            alert_count=int(len(ids)),
        )


def _mark_alerts_consumed(con, alert_ids: List[Any], now_ms: int) -> None:
    if not _alerts_support_lifecycle_tracking(con):
        return
    ids = sorted({_safe_int(alert_id) for alert_id in (alert_ids or []) if _safe_int(alert_id) > 0})
    if not ids:
        return
    try:
        placeholders = ",".join("?" for _ in ids)
        con.execute(
            f"""
            UPDATE alerts
            SET portfolio_first_seen_ts_ms = CASE
                    WHEN COALESCE(portfolio_first_seen_ts_ms, 0) > 0 THEN portfolio_first_seen_ts_ms
                    ELSE ?
                END,
                portfolio_last_seen_ts_ms = ?,
                portfolio_consumed_ts_ms = CASE
                    WHEN COALESCE(portfolio_consumed_ts_ms, 0) > 0 THEN portfolio_consumed_ts_ms
                    ELSE ?
                END,
                portfolio_status = 'consumed'
            WHERE id IN ({placeholders})
            """,
            (int(now_ms), int(now_ms), int(now_ms), *ids),
        )
        _invalidate_alert_candidate_cache()
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_ALERT_CONSUMED_UPDATE_FAILED",
            e,
            once_key=None,
            alert_count=int(len(ids)),
        )


def _expire_stale_unconsumed_alerts(con, now_ms: int, lookback_s: int) -> int:
    if not _alerts_support_lifecycle_tracking(con):
        return 0
    cutoff_ms = int(now_ms) - max(0, int(lookback_s)) * 1000
    try:
        cur = con.execute(
            """
            UPDATE alerts
            SET portfolio_expired_ts_ms = CASE
                    WHEN COALESCE(portfolio_expired_ts_ms, 0) > 0 THEN portfolio_expired_ts_ms
                    ELSE ?
                END,
                portfolio_status = 'expired'
            WHERE ts_ms < ?
              AND COALESCE(portfolio_consumed_ts_ms, 0) <= 0
              AND COALESCE(portfolio_status, 'new') IN ('new', 'seen')
            """,
            (int(now_ms), int(cutoff_ms)),
        )
        updated = int(getattr(cur, "rowcount", 0) or 0)
        if updated > 0:
            _invalidate_alert_candidate_cache()
        return int(updated)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_ALERT_EXPIRE_FAILED",
            e,
            once_key=None,
            cutoff_ms=int(cutoff_ms),
        )
        return 0


def _selected_alert_ids_from_desired(desired: Dict[str, Dict[str, Any]]) -> List[int]:
    out = set()
    for tgt in (desired or {}).values():
        if not isinstance(tgt, dict):
            continue
        side = str(tgt.get("side") or "FLAT").upper().strip()
        weight = abs(_safe_float(tgt.get("weight", 0.0), 0.0))
        if side not in {"LONG", "SHORT"} or weight <= 0.0:
            continue
        alert_id = _safe_int(tgt.get("source_alert_id"), 0)
        if alert_id > 0:
            out.add(int(alert_id))
    return sorted(out)


def _pick_best_per_symbol(alerts: List[Dict]) -> Dict[str, Dict]:
    """
    Choose best candidate per symbol across horizons.
    Criteria: max score (abs(z)*conf with severity bump).
    """
    best = {}
    for a in alerts:
        if not _model_intent_trade_allowed(a):
            continue
        sym = a["symbol"]
        z, conf = _alert_effective_signal(a)
        model_intent = (a.get("_model_intent") or {})
        if conf < _eff_min_conf():
            continue
        explicit_trade_intent = _has_explicit_model_trade_intent(model_intent)
        if (not explicit_trade_intent) and abs(z) < _eff_min_abs_z():
            continue

        model_score = _coerce_float(model_intent.get("score"))
        if model_score is not None:
            base_score = float(model_score)
        else:
            # Base score from signal strength
            base_score = _score_from_alert(z, conf, str(a.get("severity") or ""), str(a.get("explain_json") or "{}"))

        # Tradability adjustment (from explain_json)
        tr = _tradability_from_explain(a.get("explain_json", "{}"))
        net = float(tr.get("expected_ret_net", 0.0))
        pwin = float(tr.get("p_win", 0.5))
        dd = float(tr.get("expected_dd", 0.0))

        # Penalize negative expectancy, reward positive
        tradability_mult = 1.0
        if net < 0.0:
            tradability_mult *= 0.5
        else:
            tradability_mult *= (1.0 + min(0.5, net * 10.0))

        # Modest p(win) influence (kept conservative)
        tradability_mult *= (0.75 + 0.5 * max(0.0, min(1.0, pwin)))

        # Drawdown penalty (soft)
        tradability_mult *= (1.0 / (1.0 + dd * 10.0))

        score = base_score * tradability_mult
        cur = best.get(sym)
        if (cur is None) or (score > float(cur.get("_score", 0.0))):
            b = dict(a)
            b["expected_z"] = float(z)
            b["confidence"] = float(conf)
            b["_score"] = float(score)
            best[sym] = b
    return best


def _apply_temporal_dampener(con, desired: Dict[str, Dict], now_ms: int) -> Dict[str, Dict]:
    """
    If a symbol has too many recent alerts in TD window, scale its weight down.
    Uses alerts table (already exists).
    """
    from engine.runtime.state_cache import cache_get_or_load

    if not desired:
        return desired

    cutoff = int(now_ms) - int(PORTFOLIO_TD_WINDOW_S) * 1000
    for desired_key in list(desired.keys()):
        sym = _desired_symbol(desired_key, desired.get(desired_key))
        if not sym:
            continue
        try:
            n = cache_get_or_load(
                "alerts",
                f"td_count:{str(sym)}:{int(cutoff)}:{int(PORTFOLIO_TD_WINDOW_S)}",
                lambda: (
                    lambda row: int(row[0] or 0) if row else 0
                )(
                    con.execute(
                        """
                        SELECT COUNT(1)
                        FROM alerts
                        WHERE symbol=? AND ts_ms >= ?
                        """,
                        (str(sym), int(cutoff)),
                    ).fetchone()
                ),
                ttl_s=0.75,
            )
        except Exception:
            n = 0

        if n > int(PORTFOLIO_TD_MAX_SIGNALS):
            try:
                desired[desired_key]["weight"] = float(desired[desired_key].get("weight", 0.0) or 0.0) * float(PORTFOLIO_TD_SCALE)
                desired[desired_key].setdefault("reason", {})
                desired[desired_key]["reason"]["temporal_dampener"] = True
                desired[desired_key]["reason"]["td_n"] = int(n)
                desired[desired_key]["reason"]["td_window_s"] = int(PORTFOLIO_TD_WINDOW_S)
                desired[desired_key]["reason"]["td_scale"] = float(PORTFOLIO_TD_SCALE)
            except Exception:
                _warn_nonfatal("PORTFOLIO_REBALANCE_COOLDOWN_PARSE_FAILED", Exception("rebalance cooldown parse failed"), once_key="rebalance_cooldown_parse")

    # renormalize gross after dampener
    grossT = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
    if grossT > float(PORTFOLIO_GROSS_CAP) and grossT > 1e-9:
        scaleT = float(PORTFOLIO_GROSS_CAP) / float(grossT)
        for desired_key in list(desired.keys()):
            nw = float(desired[desired_key].get("weight", 0.0) or 0.0) * float(scaleT)
            desired[desired_key]["weight"] = nw if math.isfinite(nw) else 0.0

    return desired


def _load_avg_slippage_bps_by_symbol(con, limit_metrics: int) -> Dict[str, float]:
    """
    Reads execution_metrics to compute avg slippage bps per symbol.
    Uses latest available metrics (across timestamps).
    """
    from engine.runtime.state_cache import cache_get_or_load

    def _load() -> Dict[str, float]:
        out: Dict[str, float] = {}
        try:
            rows = con.execute(
                """
                SELECT symbol, AVG(COALESCE(slippage_bps,0.0)) AS avg_slip
                FROM (
                  SELECT symbol, slippage_bps
                  FROM execution_metrics
                  ORDER BY ts_ms DESC
                  LIMIT ?
                )
                GROUP BY symbol
                """,
                (int(max(1, min(100000, int(limit_metrics)))),),
            ).fetchall()

            for sym, avg_slip in rows or []:
                try:
                    s = str(sym).upper().strip()
                    if not s:
                        continue
                    out[s] = float(avg_slip or 0.0)
                except Exception as e:
                    _warn_nonfatal(
                        "PORTFOLIO_AVG_SLIPPAGE_ROW_FAILED",
                        e,
                        symbol=str(sym or ""),
                    )
                    continue
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_AVG_SLIPPAGE_LOAD_FAILED", e, once_key="avg_slippage_load")
            slippage_map: Dict[str, float] = {}
            return slippage_map
        return out

    return cache_get_or_load(
        "portfolio_slippage",
        f"avg_slippage:{int(limit_metrics)}",
        _load,
        ttl_s=2.0,
    )


def _extract_model_identity_from_explain(explain_json: str) -> Dict[str, Any]:
    try:
        ex = json.loads(explain_json or "{}")
    except Exception:
        ex = {}
    if not isinstance(ex, dict):
        ex = {}

    out: Dict[str, Any] = {}

    for key in ("model_id", "agent_id"):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            out["model_id"] = _normalize_model_id(val)
            break

    for key in ("model_name", "strategy_name"):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            out["model_name"] = str(val).strip()
            break

    model_obj = ex.get("model")
    if isinstance(model_obj, dict):
        if not out.get("model_id"):
            for key in ("model_id", "id", "agent_id"):
                val = model_obj.get(key)
                if isinstance(val, str) and val.strip():
                    out["model_id"] = _normalize_model_id(val)
                    break
        if not out.get("model_name"):
            for key in ("model_name", "name", "id"):
                val = model_obj.get(key)
                if isinstance(val, str) and val.strip():
                    out["model_name"] = str(val).strip()
                    break
        for key in ("model_kind", "kind", "type"):
            val = model_obj.get(key)
            if isinstance(val, str) and val.strip():
                out["model_kind"] = str(val).strip()
                break
        for key in ("model_ts_ms", "ts_ms", "trained_ts_ms"):
            val = model_obj.get(key)
            if val is not None:
                try:
                    out["model_ts_ms"] = int(val)
                    break
                except Exception as e:
                    _warn_nonfatal("PORTFOLIO_MODEL_IDENTITY_PARSE_FAILED", e, once_key="model_identity_ts_parse")
        for key in ("model_version", "version"):
            val = model_obj.get(key)
            if isinstance(val, str) and val.strip():
                out["model_version"] = str(val).strip()
                break

    if ex.get("model_kind") is not None:
        try:
            out["model_kind"] = str(ex.get("model_kind")).strip()
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_MODEL_IDENTITY_PARSE_FAILED", e, once_key="model_identity_kind_parse")
    if ex.get("model_ts_ms") is not None:
        try:
            out["model_ts_ms"] = _safe_int(ex.get("model_ts_ms"))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_MODEL_IDENTITY_PARSE_FAILED", e, once_key="model_identity_ex_ts_parse")
    if ex.get("model_version") is not None:
        try:
            out["model_version"] = str(ex.get("model_version")).strip()
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_MODEL_IDENTITY_PARSE_FAILED", e, once_key="model_identity_version_parse")

    for key in ("regime", "current_regime", "regime_label"):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            out["regime"] = str(val).strip()
            break

    out["model_id"] = _normalize_model_id(out.get("model_id"))

    return out


def _extract_horizon_s_from_explain_json(explain_json: str) -> int:
    ex = _safe_json_obj(explain_json)
    candidates = [
        ex.get("horizon_s"),
        ex.get("horizon"),
        ((ex.get("model_intent") or {}).get("horizon_s") if isinstance(ex.get("model_intent"), dict) else None),
        ((ex.get("signal") or {}).get("horizon_s") if isinstance(ex.get("signal"), dict) else None),
    ]
    for val in candidates:
        if val in (None, ""):
            continue
        try:
            return int(val)
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_HORIZON_PARSE_FAILED",
                e,
                once_key="horizon_parse_failed",
                value=repr(val)[:120],
            )
            continue
    return 0


def _build_recent_alert_meta_index(alerts: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    out: Dict[int, Dict[str, Any]] = {}
    for alert in alerts or []:
        try:
            alert_id = _safe_int(alert.get("id"))
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_ALERT_ID_PARSE_FAILED",
                e,
                once_key="alert_id_parse_failed",
                raw_id=repr((alert or {}).get("id"))[:120],
            )
            continue
        explain_json = str(alert.get("explain_json") or "{}")
        model_identity = _extract_model_identity_from_explain(explain_json)
        out[int(alert_id)] = {
            "horizon_s": int(alert.get("horizon_s") or _extract_horizon_s_from_explain_json(explain_json) or 0),
            "regime": str(model_identity.get("regime") or "global").strip() or "global",
            "model_name": str(model_identity.get("model_name") or "").strip(),
            "model_id": _normalize_model_id(model_identity.get("model_id")),
            "prediction_id": (_safe_int(alert.get("prediction_id"), 0) if alert.get("prediction_id") not in (None, "") else None),
        }
    return out


def _load_cached_competition_capital_plan(con, now_ms: int) -> Dict[str, Any]:
    try:
        row = con.execute(
            "SELECT value FROM runtime_meta WHERE key=?",
            ("competition_capital_plan",),
        ).fetchone()
    except Exception:
        row = None
    if not row or row[0] in (None, ""):
        return {}
    try:
        plan = json.loads(str(row[0]))
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_COMPETITION_CAPITAL_PLAN_PARSE_FAILED",
            e,
            once_key="competition_capital_plan_parse_failed",
        )
        return {}
    if not isinstance(plan, dict):
        return {}
    updated_ts_ms = int(plan.get("updated_ts_ms") or 0) if plan.get("updated_ts_ms") not in (None, "") else 0
    age_ms = max(0, int(now_ms) - int(updated_ts_ms)) if updated_ts_ms > 0 else int(COMPETITION_CAPITAL_PLAN_MAX_AGE_MS) + 1
    plan["updated_ts_ms"] = int(updated_ts_ms)
    plan["age_ms"] = int(age_ms)
    plan["fresh"] = bool(updated_ts_ms > 0 and age_ms <= int(COMPETITION_CAPITAL_PLAN_MAX_AGE_MS))
    return plan


def _competition_policy_from_cached_plan(
    capital_plan: Dict[str, Any],
    *,
    symbol: str,
    horizon_s: int,
    model_name: str,
    regime: str,
) -> Dict[str, Any]:
    symbol_u = str(symbol or "").upper().strip()
    candidate_name = str(model_name or "").strip()
    reg = str(regime or "global").strip() or "global"
    if not candidate_name:
        return {"allowed": False, "blocked": True, "reason": "model_identity_missing"}
    if not bool((capital_plan or {}).get("fresh")):
        return {
            "allowed": False,
            "blocked": True,
            "reason": "competition_plan_stale",
            "capital_plan_updated_ts_ms": int((capital_plan or {}).get("updated_ts_ms") or 0),
            "capital_plan_age_ms": int((capital_plan or {}).get("age_ms") or 0),
            "capital_plan_fresh": False,
        }

    allocations = (capital_plan or {}).get("allocations")
    if not isinstance(allocations, dict):
        allocations = {}
    group_key = "|".join([symbol_u, str(int(horizon_s or 0)), reg])
    group = allocations.get(group_key) or (allocations.get("|".join([symbol_u, "0", reg])) if int(horizon_s or 0) != 0 else None)
    group = dict(group or {})
    ranked_models = list(group.get("models") or []) if isinstance(group, dict) else []
    allocation_fraction = 0.0
    effective_allocation_fraction = 0.0
    model_risk_limit_multiplier = float(group.get("risk_limit_multiplier", 1.0) or 1.0)
    for row in ranked_models:
        if str((row or {}).get("model_name") or "").strip() != candidate_name:
            continue
        allocation_fraction = float((row or {}).get("allocation_fraction") or 0.0)
        effective_allocation_fraction = float(
            (row or {}).get("effective_allocation_fraction")
            or allocation_fraction
            or 0.0
        )
        model_risk_limit_multiplier = float(
            (row or {}).get("model_risk_limit_multiplier")
            or model_risk_limit_multiplier
            or 1.0
        )
        break

    model_budget_fraction = 0.0
    for alloc in allocations.values():
        if not isinstance(alloc, dict):
            continue
        for row in list(alloc.get("models") or []):
            if str((row or {}).get("model_name") or "").strip() != candidate_name:
                continue
            model_budget_fraction += float(
                (row or {}).get("effective_allocation_fraction")
                or (row or {}).get("allocation_fraction")
                or 0.0
            )
            break

    group_budget_fraction = float(group.get("group_budget_fraction") or 0.0)
    if group_budget_fraction <= 0.0 and ranked_models:
        group_budget_fraction = sum(
            max(
                0.0,
                float(
                    (row or {}).get("effective_allocation_fraction")
                    or (row or {}).get("allocation_fraction")
                    or 0.0
                ),
            )
            for row in ranked_models
        )

    policy = {
        "allowed": True,
        "blocked": False,
        "reason": "",
        "group_key": str(group_key),
        "champion_model_name": str(group.get("champion_model_name") or ""),
        "allocation_strategy": str(group.get("allocation_strategy") or (capital_plan or {}).get("allocation_strategy") or "proportional"),
        "allocation_fraction": float(max(0.0, allocation_fraction)),
        "effective_allocation_fraction": float(max(0.0, effective_allocation_fraction)),
        "capital_multiplier": float(max(0.0, effective_allocation_fraction)),
        "model_weight": float(max(0.0, allocation_fraction)),
        "group_budget_fraction": float(max(0.0, min(1.0, group_budget_fraction))),
        "model_budget_fraction": float(max(0.0, min(1.0, model_budget_fraction))),
        "risk_limit_multiplier": float(
            min(
                float(group.get("risk_limit_multiplier") or 1.0),
                max(0.0, float(model_risk_limit_multiplier)),
            )
        ),
        "group_risk_limit_multiplier": float(group.get("risk_limit_multiplier") or 1.0),
        "model_risk_limit_multiplier": float(max(0.0, model_risk_limit_multiplier)),
        "regime": str(group.get("regime") or reg),
        "horizon_s": int(group.get("horizon_s") or horizon_s or 0),
        "capital_plan_updated_ts_ms": int((capital_plan or {}).get("updated_ts_ms") or 0),
        "capital_plan_age_ms": int((capital_plan or {}).get("age_ms") or 0),
        "capital_plan_fresh": bool((capital_plan or {}).get("fresh")),
    }
    if not ranked_models:
        policy["allowed"] = False
        policy["blocked"] = True
        policy["reason"] = "no_group_allocation"
    elif allocation_fraction <= 0.0:
        policy["allowed"] = False
        policy["blocked"] = True
        policy["reason"] = "model_not_allocated"
    elif effective_allocation_fraction <= 0.0:
        policy["allowed"] = False
        policy["blocked"] = True
        policy["reason"] = "model_effective_capital_zero"
    return policy


def _apply_impact_aware_sizing(con, desired: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Scales weights down for symbols with poor realized slippage.
    factor = clamp(1 - (abs(slip_bps)/IMPACT_BAD_BPS - 1), floor..1)
    """
    if not desired or not PORTFOLIO_IMPACT_SIZING:
        return desired

    slip = _load_avg_slippage_bps_by_symbol(con, int(PORTFOLIO_IMPACT_LOOKBACK_METRICS))
    if not slip:
        return desired

    bad = float(PORTFOLIO_IMPACT_BAD_BPS)
    floor = float(PORTFOLIO_IMPACT_FLOOR)

    for desired_key in list(desired.keys()):
        s = _desired_symbol(desired_key, desired.get(desired_key))
        if not s:
            continue
        sbps = float(slip.get(s, 0.0))
        a = abs(sbps)
        if bad <= 1e-9:
            continue
        if a <= bad:
            # no penalty
            desired[desired_key].setdefault("reason", {})
            desired[desired_key]["reason"]["impact_slip_bps"] = float(sbps)
            desired[desired_key]["reason"]["impact_factor"] = 1.0
            continue

        # if slippage is 2x bad, factor ~ 0.0 -> floored
        raw = 1.0 - ((a / bad) - 1.0)
        f = max(float(floor), min(1.0, float(raw)))

        try:
            desired[desired_key]["weight"] = float(desired[desired_key].get("weight", 0.0) or 0.0) * float(f)
            desired[desired_key].setdefault("reason", {})
            desired[desired_key]["reason"]["impact_slip_bps"] = float(sbps)
            desired[desired_key]["reason"]["impact_factor"] = float(f)
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_IMPACT_SIZING_REASON_FAILED", e, once_key="impact_sizing_reason")

    gross = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
    eff_cap = float(_eff_gross_cap())
    if gross > float(eff_cap) and gross > 1e-9:
        sc = float(eff_cap) / float(gross)
        for desired_key in list(desired.keys()):
            try:
                nw = float(desired[desired_key].get("weight", 0.0) or 0.0) * float(sc)
                desired[desired_key]["weight"] = float(nw) if math.isfinite(nw) else 0.0
            except Exception:
                desired[desired_key]["weight"] = 0.0

    return desired
# =========================
# SECTION 4 / ~200 lines
# =========================

def _optimize_capital_allocation(con, desired: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Reweights desired weights using:
      utility ~ (expected_ret_net^alpha) / (expected_dd^beta + eps)
    Then scales each symbol relative to its original weight and clamps factor.
    """
    if not desired or not PORTFOLIO_ALLOC_OPT:
        return desired

    alpha = float(PORTFOLIO_ALLOC_ALPHA)
    beta = float(PORTFOLIO_ALLOC_BETA)
    fmin = float(PORTFOLIO_ALLOC_FLOOR)
    fmax = float(PORTFOLIO_ALLOC_CEIL)

    # compute utilities from tradability
    util = {}
    now_ms = _now_ms()

    for desired_key, tgt in desired.items():
        sym = _desired_symbol(desired_key, tgt)
        if not sym:
            continue
        tr = _tradability_from_explain(tgt.get("explain_json", "{}"))
        net = float(_expected_ret_net_from_target(tgt))
        dd = float(tr.get("expected_dd", 0.0) or 0.0)
        dd = max(0.0, min(1.0, dd))

        # conservative: ignore negative expectancy in optimizer
        netp = max(0.0, net)

        # ----------------------------
        # Execution Regime Alpha Boost
        # ----------------------------
        regime_mult = 1.0

        if PORTFOLIO_USE_EXEC_REGIME:
            try:
                skew_z = float(_get_factor_feature_asof(con, "options.skew_25d_z", int(now_ms)))
                flow_z = float(_get_factor_feature_asof(con, "flows.index_constituent_imbalance_z", int(now_ms)))
            except Exception:
                skew_z = 0.0
                flow_z = 0.0

            stress_mag = max(
                0.0,
                max(
                    abs(skew_z) - float(PORTFOLIO_EXEC_SKEW_Z_TH),
                    abs(flow_z) - float(PORTFOLIO_EXEC_FLOW_Z_TH),
                ),
            )

            if stress_mag > 0.0:
                regime_mult *= float(
                    _clamp(
                        1.0 - (stress_mag * float(PORTFOLIO_EXEC_STRESS_SIZE_REDUCTION)),
                        float(PORTFOLIO_EXEC_REGIME_FLOOR),
                        1.0,
                    )
                )

            # earnings proximity penalty
            try:
                row_e = con.execute(
                    """
                    SELECT earnings_date
                    FROM earnings_calendar
                    WHERE symbol=?
                    ORDER BY ABS(julianday(earnings_date) - julianday(date('now'))) ASC
                    LIMIT 1
                    """,
                    (str(sym),),
                ).fetchone()

                if row_e and row_e[0]:
                    jd = con.execute(
                        "SELECT (julianday(?) - julianday(date('now')))",
                        (str(row_e[0]),),
                    ).fetchone()
                    if jd and jd[0] is not None:
                        days = float(jd[0])
                        decay = math.exp(-abs(days) / 5.0)
                        earnings_pen = float(
                            _clamp(
                                1.0 - (decay * float(PORTFOLIO_EXEC_EARNINGS_SIZE_REDUCTION)),
                                float(PORTFOLIO_EXEC_REGIME_FLOOR),
                                1.0,
                            )
                        )
                        regime_mult *= earnings_pen
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_EFFICIENCY_EARNINGS_PENALTY_FAILED", e, once_key="efficiency_earnings_penalty")

        u = ((netp ** max(0.0, alpha)) / ((dd ** max(0.0, beta)) + 1e-6)) * float(regime_mult)

        util[str(desired_key)] = float(u)

        desired[desired_key].setdefault("reason", {})
        desired[desired_key]["reason"]["optimizer_regime_mult"] = float(regime_mult)

    # if all utilities are zero, do nothing
    tot_u = sum(float(u) for u in util.values())
    if tot_u <= 1e-12:
        return desired

    # apply multiplicative factor relative to original weights
    for desired_key in list(desired.keys()):
        u = float(util.get(desired_key, 0.0))
        # normalized utility share
        share = u / tot_u if tot_u > 0 else 0.0

        w0 = float(desired[desired_key].get("weight", 0.0) or 0.0)
        if w0 <= 0:
            continue

        # target weight proportional to share, but keep within factor bounds vs original
        wt = float(PORTFOLIO_GROSS_CAP) * float(share)
        factor = wt / max(1e-9, w0)
        factor = max(fmin, min(fmax, float(factor)))

        desired[desired_key]["weight"] = float(w0) * float(factor)
        desired[desired_key].setdefault("reason", {})
        desired[desired_key]["reason"]["alloc_util"] = float(u)
        desired[desired_key]["reason"]["alloc_share"] = float(share)
        desired[desired_key]["reason"]["alloc_factor"] = float(factor)

    # renormalize gross
    gross = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
    eff_cap = float(_eff_gross_cap())
    if gross > float(eff_cap) and gross > 1e-9:
        sc = float(eff_cap) / float(gross)
        for desired_key in list(desired.keys()):
            try:
                nw = float(desired[desired_key].get("weight", 0.0) or 0.0) * float(sc)
                desired[desired_key]["weight"] = float(nw) if math.isfinite(nw) else 0.0
            except Exception:
                desired[desired_key]["weight"] = 0.0

    return desired


def _resolve_existing_allocation_mode() -> str:
    if PORTFOLIO_CORR_OPT:
        return "corr_opt"
    if PORTFOLIO_CORR_PRUNE:
        return "corr_prune"
    return "legacy"


def _resolve_allocation_mode() -> str:
    mode = str(PORTFOLIO_ALLOCATION_MODE or "").strip().lower()
    if mode in {"", "existing_mode"}:
        return _resolve_existing_allocation_mode()
    if mode in {"hrp", "corr_opt", "corr_prune", "legacy"}:
        return str(mode)
    return _resolve_existing_allocation_mode()


def _apply_existing_allocation_mode(
    con,
    desired: Dict[str, Dict],
    allocation_mode: Optional[str] = None,
) -> Dict[str, Dict]:
    mode = str(allocation_mode or _resolve_existing_allocation_mode()).strip().lower()

    if mode == "corr_opt":
        try:
            gamma_eff = float(PORTFOLIO_CORR_OPT_GAMMA_BASE)
            regime_name = "MID"

            try:
                from engine.strategy.regime_size import regime_capital_scale

                _rs = regime_capital_scale(con=con, anchor=str(PORTFOLIO_REGIME_ANCHOR))
                regime_name = str((_rs or {}).get("regime") or "MID").upper()
            except Exception:
                regime_name = "MID"

            if regime_name == "LOW":
                gamma_eff *= float(PORTFOLIO_CORR_OPT_GAMMA_LOW)
            elif regime_name == "HIGH":
                gamma_eff *= float(PORTFOLIO_CORR_OPT_GAMMA_HIGH)
            else:
                gamma_eff *= float(PORTFOLIO_CORR_OPT_GAMMA_MID)

            from engine.strategy.corr_opt import corr_aware_optimize_desired

            desired = corr_aware_optimize_desired(
                con,
                desired,
                gross_cap=float(_eff_gross_cap()),
                lookback=int(PORTFOLIO_CORR_LOOKBACK),
                corr_max=float(PORTFOLIO_CORR_MAX),
                gamma=float(gamma_eff),
                ridge=float(PORTFOLIO_CORR_OPT_RIDGE),
                iters=int(PORTFOLIO_CORR_OPT_ITERS),
            )

            for sym in list(desired.keys()):
                desired[sym].setdefault("reason", {})
                desired[sym]["reason"]["corr_opt_gamma_eff"] = float(gamma_eff)
                desired[sym]["reason"]["corr_opt_regime"] = str(regime_name)

        except Exception as e:
            _warn_nonfatal("PORTFOLIO_CORR_OPTIMIZER_FAILED", e, once_key="corr_optimizer")
        return desired

    if mode == "corr_prune":
        try:
            from engine.strategy.risk import corr_from_prices

            kept = []
            items = sorted(
                desired.items(),
                key=lambda kv: abs(float(kv[1].get("weight", 0.0))),
                reverse=True,
            )

            for sym, tgt in items:
                ok = True
                for k in kept:
                    c = corr_from_prices(con, sym, k, lookback=PORTFOLIO_CORR_LOOKBACK)
                    if c is not None and abs(float(c)) >= float(PORTFOLIO_CORR_MAX):
                        ok = False
                        break
                if ok:
                    kept.append(sym)
            desired = {s: desired[s] for s in kept if s in desired}
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_CORR_PRUNE_FAILED", e, once_key="corr_prune")
    return desired


def _apply_allocation_mode(con, desired: Dict[str, Dict]) -> Dict[str, Dict]:
    allocation_mode = _resolve_allocation_mode()
    if len(desired) <= 1:
        return desired

    if allocation_mode == "hrp":
        try:
            return _apply_hrp_allocation(
                con,
                desired,
                gross_cap=float(_eff_gross_cap()),
                lookback=int(PORTFOLIO_CORR_LOOKBACK),
            )
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_HRP_ALLOCATOR_FAILED", e, once_key="portfolio_hrp_allocator")
            fallback_mode = _resolve_existing_allocation_mode()
            desired = _apply_existing_allocation_mode(con, desired, allocation_mode=fallback_mode)
            for sym in list((desired or {}).keys()):
                reason = _ensure_reason_dict(desired.get(sym))
                reason["allocation_mode_requested"] = "hrp"
                reason["allocation_mode_fallback"] = str(fallback_mode)
                reason["hrp_error"] = str(e)
            return desired

    return _apply_existing_allocation_mode(con, desired, allocation_mode=allocation_mode)


def _load_strategies_by_stage(con, stage: str) -> List[str]:
    """
    Returns all strategies currently in the requested stage.
    """
    try:
        rows = con.execute(
            """
            SELECT strategy_name
            FROM strategy_registry
            WHERE stage=?
            ORDER BY strategy_name ASC
            """,
            (str(stage),),
        ).fetchall()
        return [str(r[0]) for r in rows or [] if r and r[0]]
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_LOAD_STRATEGIES_BY_STAGE_FAILED",
            e,
            once_key=f"load_strategies_by_stage:{stage}",
            stage=str(stage),
        )
        strategies: List[str] = []
        return strategies


def _load_live_strategies(con) -> List[str]:
    rows = _load_strategies_by_stage(con, "live")
    if rows:
        return rows

    try:
        fallback = str(choose_strategy_name(con, _now_ms()) or "").strip().lower()
    except Exception:
        fallback = ""

    if fallback:
        return [fallback]

    return ["baseline"]


def _load_shadow_strategies(con) -> List[str]:
    return _load_strategies_by_stage(con, "shadow")

def _auto_promote_shadow_strategies(con):
    """
    Promote shadow strategies if they outperform live strategies.
    """

    shadow_perf = _load_shadow_performance(con)
    live_perf = _load_live_strategy_performance(con)

    if not shadow_perf:
        return

    best_live = max(live_perf.values()) if live_perf else 0.0

    for strat, score in shadow_perf.items():

        if score <= best_live * SHADOW_PROMOTION_THRESHOLD:
            continue

        row = con.execute(
            """
            SELECT COUNT(1)
            FROM strategy_shadow_runs
            WHERE strategy_name=?
            """,
            (strat,),
        ).fetchone()

        if not row or int(row[0]) < SHADOW_PROMOTION_MIN_RUNS:
            continue

        con.execute(
            """
            UPDATE strategy_registry
            SET stage='live'
            WHERE strategy_name=?
              AND COALESCE(stage,'') <> 'live'
            """,
            (strat,),
        )

        con.execute(
            """
            INSERT INTO strategy_promotion_log(ts_ms,strategy_name,reason)
            VALUES(?,?,?)
            """,
            (
                _now_ms(),
                strat,
                json.dumps({
                    "reason":"shadow_outperformance",
                    "shadow_score":score,
                    "best_live":best_live
                })
            )
        )


def _score_shadow_targets(targets: Dict[str, Dict]) -> Dict[str, Any]:
    gross = 0.0
    max_abs_w = 0.0
    n_positions = 0

    scores: List[float] = []
    confs: List[float] = []
    abs_zs: List[float] = []

    for _, tgt in (targets or {}).items():
        try:
            w = abs(float((tgt or {}).get("weight", 0.0) or 0.0))
            if w <= 0.0:
                continue

            reason = (tgt or {}).get("reason") or {}
            score = float(reason.get("score", 0.0) or 0.0)
            conf = float(reason.get("confidence", 0.0) or 0.0)
            expected_z = abs(float(reason.get("expected_z", 0.0) or 0.0))

            gross += float(w)
            max_abs_w = max(float(max_abs_w), float(w))
            n_positions += 1

            scores.append(float(score))
            confs.append(float(conf))
            abs_zs.append(float(expected_z))
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_SHADOW_PROXY_ROW_FAILED",
                e,
                strategy_name=str((tgt or {}).get("strategy_name") or ""),
            )
            continue

    avg_score = (sum(scores) / float(len(scores))) if scores else 0.0
    avg_conf = (sum(confs) / float(len(confs))) if confs else 0.0
    avg_abs_z = (sum(abs_zs) / float(len(abs_zs))) if abs_zs else 0.0

    concentration = (float(max_abs_w) / float(gross)) if gross > 1e-9 else 1.0
    proxy_score = float(avg_score) * (0.50 + 0.50 * float(avg_conf))
    proxy_score *= max(0.25, 1.0 - (0.35 * float(concentration)))

    return {
        "proxy_score": float(proxy_score),
        "gross_target": float(gross),
        "max_abs_weight": float(max_abs_w),
        "concentration": float(concentration),
        "n_positions": int(n_positions),
        "avg_score": float(avg_score),
        "avg_conf": float(avg_conf),
        "avg_abs_z": float(avg_abs_z),
    }

def _load_shadow_performance(con) -> Dict[str, float]:
    """
    Compute average proxy_score for each shadow strategy
    """
    cutoff_ms = int(time.time() * 1000) - int(SHADOW_PROMOTION_LOOKBACK * 1000)
    rows = con.execute(
        """
        SELECT strategy_name, AVG(CAST(json_extract(metrics_json,'$.proxy_score') AS REAL))
        FROM strategy_shadow_runs
        WHERE ts_ms > ?
        GROUP BY strategy_name
        """,
        (int(cutoff_ms),),
    ).fetchall()

    out = {}
    for r in rows or []:
        try:
            out[str(r[0])] = float(r[1])
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_SHADOW_PERFORMANCE_ROW_FAILED",
                e,
                strategy_name=str(r[0] or "") if len(r) > 0 else "",
            )
            continue
    return out

def _record_shadow_strategy_run(
    con,
    *,
    strategy_name: str,
    targets: Dict[str, Dict],
    metrics: Dict[str, Any],
) -> None:
    con.execute(
        """
        INSERT INTO strategy_shadow_runs(
          ts_ms, strategy_name, desired_json, metrics_json
        )
        VALUES (?,?,?,?)
        """,
        (
            int(_now_ms()),
            str(strategy_name),
            json.dumps(targets or {}, separators=(",", ":"), sort_keys=True),
            json.dumps(metrics or {}, separators=(",", ":"), sort_keys=True),
        ),
    )


def _load_strategy_efficiency(con) -> Dict[str, Dict[str, float]]:
    """
    Loads latest capital-efficiency aggregates from strategy_metrics (window_days=0).
    Returns:
        {strategy_name: {
            "efficiency_score": float,
            "return_per_risk_unit": float,
            "drawdown_contribution": float
        }}
    """
    out: Dict[str, Dict[str, float]] = {}
    try:
        rows = con.execute(
            """
            SELECT strategy_name, metrics_json
            FROM strategy_metrics
            WHERE window_days=0
            """
        ).fetchall()

        for name, mj in rows or []:
            try:
                m = json.loads(mj or "{}")
                out[str(name)] = {
                    "efficiency_score": float(m.get("efficiency_score", 0.0) or 0.0),
                    "return_per_risk_unit": float(m.get("return_per_risk_unit", 0.0) or 0.0),
                    "drawdown_contribution": float(m.get("drawdown_contribution", 0.0) or 0.0),
                }
            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_STRATEGY_EFFICIENCY_ROW_FAILED",
                    e,
                    strategy_name=str(name or ""),
                )
                continue
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_STRATEGY_EFFICIENCY_LOAD_FAILED", e, once_key="strategy_efficiency_load")
    return out

def _load_live_strategy_performance(con) -> Dict[str, float]:
    """
    Live strategy performance proxy
    """
    rows = con.execute(
        """
        SELECT strategy_name,
               AVG(CAST(json_extract(metrics_json,'$.return_per_risk_unit') AS REAL))
        FROM strategy_metrics
        WHERE window_days=0
        GROUP BY strategy_name
        """
    ).fetchall()

    out = {}
    for r in rows or []:
        try:
            out[str(r[0])] = float(r[1] or 0.0)
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_LIVE_STRATEGY_PERFORMANCE_ROW_FAILED",
                e,
                strategy_name=str(r[0] or "") if len(r) > 0 else "",
            )
            continue
    return out




def _load_state(con) -> Dict[str, Dict]:
    rows = con.execute(
        """
        SELECT model_id, symbol, side, weight, opened_ts_ms, updated_ts_ms, source_alert_id, explain_json
        FROM portfolio_state
        """
    ).fetchall()

    out = {}
    for r in rows or []:
        out[f"{_normalize_model_id(r[0])}:{str(r[1])}"] = {
            "model_id": _normalize_model_id(r[0]),
            "symbol": str(r[1]),
            "side": str(r[2]),
            "weight": float(r[3]),
            "opened_ts_ms": int(r[4]),
            "updated_ts_ms": int(r[5]),
            "source_alert_id": (int(r[6]) if r[6] is not None else None),
            "explain_json": str(r[7] or "{}"),
        }
    return out


def _write_state_row(
    con,
    model_id: str,
    sym: str,
    side: str,
    weight: float,
    opened_ts_ms: int,
    updated_ts_ms: int,
    source_alert_id: Optional[int],
    explain_json: str,
) -> None:
    from engine.runtime.state_cache import cache_invalidate_namespace

    con.execute(
        """
        INSERT INTO portfolio_state(model_id, symbol, side, weight, opened_ts_ms, updated_ts_ms, source_alert_id, explain_json)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(model_id, symbol) DO UPDATE SET
          side=excluded.side,
          weight=excluded.weight,
          opened_ts_ms=excluded.opened_ts_ms,
          updated_ts_ms=excluded.updated_ts_ms,
          source_alert_id=excluded.source_alert_id,
          explain_json=excluded.explain_json
        """,
        (
            _normalize_model_id(model_id),
            str(sym),
            str(side),
            float(weight),
            int(opened_ts_ms),
            int(updated_ts_ms),
            int(source_alert_id) if source_alert_id is not None else None,
            str(explain_json or "{}"),
        ),
    )
    cache_invalidate_namespace("portfolio_state")
    cache_invalidate_namespace("portfolio_snapshot")


def _portfolio_flip_lambda() -> float:
    raw = os.environ.get("TS_PORTFOLIO_FLIP_LAMBDA", "0.001")
    try:
        value = float(raw)
    except Exception:
        return 0.001
    if not math.isfinite(value):
        return 0.001
    return max(0.0, float(value))


def _side_signed_weight(side: Any, weight: Any) -> float:
    side_s = str(side or "").strip().upper()
    magnitude = abs(float(_safe_float(weight, 0.0)))
    if magnitude <= 1e-12 or side_s in {"", "FLAT", "NONE"}:
        return 0.0
    if side_s == "SHORT":
        return -magnitude
    if side_s == "LONG":
        return magnitude
    return float(_safe_float(weight, 0.0))


def _apply_flip_flop_penalty(
    con,
    desired: Dict[str, Dict],
    state: Dict[str, Dict],
) -> Tuple[Dict[str, Dict], Dict[str, Any]]:
    lambda_flip = _portfolio_flip_lambda()
    normalized_state: Dict[str, Dict] = {}
    for prev in (state or {}).values():
        if not isinstance(prev, dict):
            continue
        prev_key = f"{_normalize_model_id(prev.get('model_id'))}:{str(prev.get('symbol') or '').strip().upper()}"
        normalized_state[prev_key] = prev

    flips: List[Dict[str, Any]] = []
    total_delta = 0.0
    for item_key, tgt in list((desired or {}).items()):
        if not isinstance(tgt, dict):
            continue
        symbol = _desired_symbol(item_key, tgt)
        if not symbol:
            continue
        model_id = _normalize_model_id(tgt.get("model_id"))
        prev = normalized_state.get(f"{model_id}:{str(symbol).strip().upper()}")
        if not prev:
            continue
        prev_signed = _side_signed_weight(prev.get("side"), prev.get("weight"))
        target_signed = _side_signed_weight(tgt.get("side"), tgt.get("weight"))
        if prev_signed * target_signed >= 0.0:
            continue
        delta_weight = abs(float(target_signed) - float(prev_signed))
        penalty = float(lambda_flip) * float(delta_weight)
        total_delta += float(delta_weight)
        detail = {
            "model_id": str(model_id),
            "symbol": str(symbol),
            "prev_side": str(prev.get("side") or ""),
            "target_side": str(tgt.get("side") or ""),
            "prev_weight": float(prev_signed),
            "target_weight": float(target_signed),
            "delta_weight": float(delta_weight),
            "lambda_flip": float(lambda_flip),
            "penalty": float(penalty),
        }
        flips.append(detail)
        reason = _ensure_reason_dict(tgt)
        reason["flip_flop_penalty"] = dict(detail)

    meta = {
        "enabled": True,
        "lambda_flip": float(lambda_flip),
        "flip_count": int(len(flips)),
        "turnover": float(total_delta),
        "penalty": float(lambda_flip) * float(total_delta),
        "flips": flips,
    }
    _put_meta(con, "last_flip_flop_penalty", json.dumps(meta, separators=(",", ":"), sort_keys=True))
    if flips:
        LOGGER.warning(
            "portfolio_flip_flop_penalty flip_count=%s penalty=%s lambda_flip=%s",
            len(flips),
            meta["penalty"],
            lambda_flip,
        )
    return desired, meta
# =========================
# SECTION 5 / ~200 lines
# =========================

def _apply_capital_at_risk_gate(desired: Dict[str, Dict]) -> Tuple[Dict[str, Dict], Dict]:
    """
    Enforce portfolio and per-symbol tail-risk budgets using expected_dd from tradability.
    risk_i = |w_i| * expected_dd_i
    """
    meta = {
        "car_enabled": True,
        "car_max": float(PORTFOLIO_CAR_MAX),
        "car_max_per_symbol": float(PORTFOLIO_CAR_MAX_PER_SYMBOL),
    }
    if not desired:
        meta["car_scaled"] = False
        return desired, meta

    # Compute per-symbol expected_dd (fallback to 0)
    risks = {}
    total_risk = 0.0

    for desired_key, tgt in desired.items():
        sym = _desired_symbol(desired_key, tgt)
        if not sym:
            continue
        w = abs(float(tgt.get("weight", 0.0) or 0.0))
        tr = _tradability_from_explain(tgt.get("explain_json", "{}"))
        dd = float(tr.get("expected_dd", 0.0) or 0.0)
        dd = max(0.0, min(1.0, dd))

        # Per-symbol cap first
        max_w = None
        if float(PORTFOLIO_CAR_MAX_PER_SYMBOL) > 0.0:
            denom = dd if dd > 0 else 1.0
            max_w = float(PORTFOLIO_CAR_MAX_PER_SYMBOL) / max(1e-9, denom)

        if max_w is not None and w > max_w:
            new_w = float(max_w)
            if str(tgt.get("side")).upper() == "SHORT":
                tgt["weight"] = -float(new_w)
            else:
                tgt["weight"] = float(new_w)

            tgt.setdefault("reason", {})
            tgt["reason"]["car_symbol_cap"] = True
            tgt["reason"]["car_expected_dd"] = float(dd)
            tgt["reason"]["car_symbol_max_w"] = float(max_w)

        w2 = abs(float(tgt.get("weight", 0.0) or 0.0))
        r = w2 * dd
        risks[str(desired_key)] = {"symbol": str(sym), "w": w2, "expected_dd": dd, "risk": r}
        total_risk += float(r)

    meta["car_total_risk_before"] = float(total_risk)

    # Portfolio cap via scaling if needed
    if float(PORTFOLIO_CAR_MAX) > 0.0 and total_risk > float(PORTFOLIO_CAR_MAX) and total_risk > 1e-9:
        scale = float(PORTFOLIO_CAR_MAX) / float(total_risk)
        for sym in list(desired.keys()):
            try:
                desired[sym]["weight"] = float(desired[sym].get("weight", 0.0) or 0.0) * float(scale)
                desired[sym].setdefault("reason", {})
                desired[sym]["reason"]["car_scale"] = float(scale)
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_CAPITAL_AT_RISK_REASON_FAILED", e, once_key="capital_at_risk_reason")
        meta["car_scaled"] = True
        meta["car_scale"] = float(scale)
    else:
        meta["car_scaled"] = False

    # Renormalize gross after CAR scaling (safety)
    grossC = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
    if grossC > float(PORTFOLIO_GROSS_CAP) and grossC > 1e-9:
        scaleC = float(PORTFOLIO_GROSS_CAP) / float(grossC)
        for sym in list(desired.keys()):
            w0 = float(desired[sym].get("weight", 0.0) or 0.0)
            sign = -1.0 if w0 < 0 else 1.0
            desired[sym]["weight"] = sign * abs(float(w0) * float(scaleC))
        meta["gross_renorm_after_car"] = True
        meta["gross_scale_after_car"] = float(scaleC)

    meta["car_by_symbol"] = risks
    return desired, meta


def _emit_order(
    con,
    sym: str,
    action: str,
    from_side: str,
    to_side: str,
    from_w: float,
    to_w: float,
    source_alert_id: Optional[int],
    prediction_id: Optional[int] | Dict | None = None,
    explain: Optional[Dict] = None,
) -> None:
    from engine.runtime.state_cache import cache_invalidate_namespace

    ts_ms = _now_ms()

    if explain is None and isinstance(prediction_id, dict):
        explain = prediction_id
        prediction_id = None

    ex = dict(explain or {})
    ex.setdefault("execution", {})
    ex["execution"]["intent_only"] = True
    ex["execution"]["component"] = "portfolio"

    reason_data = ex.get("reason")
    reason_obj = dict(reason_data) if isinstance(reason_data, dict) else {}
    strategy_name = str(reason_obj.get("strategy") or "").strip()
    strategy_alloc = reason_obj.get("strategy_alloc")
    strategy_alloc_detail = reason_obj.get("strategy_alloc_detail")
    model_identity = {}
    try:
        model_identity = _extract_model_identity_from_explain(json.dumps(ex, ensure_ascii=False))
    except Exception:
        model_identity = {}

    if strategy_name:
        ex.setdefault("strategy", {})
        if not isinstance(ex.get("strategy"), dict):
            ex["strategy"] = {}
        ex["strategy"]["name"] = str(strategy_name)

    if isinstance(model_identity, dict) and model_identity:
        ex["model_id"] = _normalize_model_id(model_identity.get("model_id"))
        if model_identity.get("model_name"):
            ex["model_name"] = str(model_identity.get("model_name"))
        if model_identity.get("model_kind"):
            ex["model_kind"] = str(model_identity.get("model_kind"))
        if model_identity.get("model_ts_ms") is not None:
            try:
                ex["model_ts_ms"] = _safe_int(model_identity.get("model_ts_ms"))
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_CORR_OPTIMIZER_FAILED", e, once_key="corr_optimizer")
        if model_identity.get("model_version"):
            ex["model_version"] = str(model_identity.get("model_version"))
        if model_identity.get("regime"):
            ex["regime"] = str(model_identity.get("regime"))

    if isinstance(strategy_alloc, dict) and strategy_alloc:
        ex["execution"]["strategy_alloc"] = dict(strategy_alloc)

    if isinstance(strategy_alloc_detail, dict) and strategy_alloc_detail:
        ex["execution"]["strategy_alloc_detail"] = dict(strategy_alloc_detail)
    resolved_prediction_id = _resolve_portfolio_order_prediction_id(
        con,
        source_alert_id=source_alert_id,
        prediction_id=prediction_id,
    )
    if resolved_prediction_id is not None:
        ex["prediction_id"] = int(resolved_prediction_id)
    else:
        ex.pop("prediction_id", None)

    con.execute(
        """
        INSERT INTO portfolio_orders(
          ts_ms, model_id, symbol, action, from_side, to_side, from_weight, to_weight, delta_weight, source_alert_id, prediction_id, explain_json
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            int(ts_ms),
            _normalize_model_id(ex.get("model_id")),
            str(sym),
            str(action),
            str(from_side),
            str(to_side),
            float(from_w),
            float(to_w),
            float(to_w - from_w),
            int(source_alert_id) if source_alert_id is not None else None,
            resolved_prediction_id,
            json.dumps(ex, ensure_ascii=False),
        ),
    )
    cache_invalidate_namespace("portfolio_orders")
    cache_invalidate_namespace("portfolio_snapshot")


# Pyright hits an internal complexity ceiling on this orchestration function.
# The diagnostic is not pointing to one incorrect expression, so keep the
# warning local instead of weakening repo-wide analysis.
def compute_rebalance() -> Dict:  # pyright: ignore[reportGeneralTypeIssues]
    """
    Main entry point for portfolio rebalance.
    """
    init_db()
    init_portfolio_db()
    con = connect()
    degraded_reasons: list[dict[str, Any]] = []

    def _record_degraded_phase(phase: str, code: str, error: BaseException | str | None = None) -> None:
        item: dict[str, Any] = {
            "phase": str(phase or "").strip(),
            "code": str(code or "").strip(),
        }
        if error is not None:
            if isinstance(error, BaseException):
                item["error"] = f"{type(error).__name__}: {error}"
            else:
                item["error"] = str(error)
        degraded_reasons.append(item)

    try:
        # cooldown guard
        last = _get_meta(con, "last_rebalance_ts_ms")
        now_ms = _now_ms()

        last_exec = _get_meta(con, "last_rebalance_exec_id")
        if last_exec == str(now_ms):
            return {"ok": False, "error": "duplicate rebalance execution"}

        try:
            if not bool(getattr(con, "in_transaction", False)):
                con.begin_managed_write()
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_TRANSACTION_BEGIN_FAILED", e, once_key="transaction_begin")
            raise

        _set_meta(con, "last_rebalance_exec_id", str(now_ms))

        if last:
            try:
                last_ms = int(last)
                eff_cd = int(_eff_rebalance_cooldown_s())
                if (now_ms - last_ms) < int(eff_cd) * 1000:
                    return {
                        "ok": False,
                        "error": "rebalance cooldown active",
                        "cooldown_s": int(eff_cd),
                    }
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_REBALANCE_COOLDOWN_PARSE_FAILED", e, once_key="rebalance_cooldown_parse")

        # capital guard (hard stop) — portfolio is intent-only, but still should not churn state when halted
        try:
            from engine.strategy.capital_guard import trading_allowed

            if not trading_allowed(con):
                return {"ok": False, "error": "trading halted by capital guard"}
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_CAPITAL_GUARD_CHECK_FAILED", e, once_key="capital_guard_check")
            error_payload = {"ok": False, "error": f"capital_guard check failed: {e}"}
            return error_payload

        # read alerts + state
        alerts = _load_recent_alert_candidates(con, PORTFOLIO_LOOKBACK_S)
        _mark_alert_candidates_seen(
            con,
            [dict(alert or {}).get("id") for alert in (alerts or [])],
            int(now_ms),
        )
        alert_meta_index = _build_recent_alert_meta_index(alerts)
        state = _load_state(con)

        try:
            _auto_promote_shadow_strategies(con)
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_AUTO_PROMOTION_FAILED", e, once_key="auto_promote_shadow_strategies")

        # ------------------------------------------------------
        # Multi-Strategy Capital Competition
        # ------------------------------------------------------
        live_strategies = _load_live_strategies(con)
        shadow_strategies = _load_shadow_strategies(con)

        # ------------------------------------------------------
        # Strategy Allocator (Meta Capital Engine)
        # - rolling perf scoring
        # - drawdown-aware scaling
        # - correlation-adjusted allocation
        # - dynamic redistribution
        # - config-driven risk budgets
        # ------------------------------------------------------
        alloc_map: Dict[str, float] = {}
        alloc_detail: Dict[str, Any] = {}
        alloc_regime: Dict[str, Any] = {}
        alloc_regime_conf: float = 0.0
        alloc_reason: Dict[str, Any] = {}
        alloc_alpha_runtime: Dict[str, Any] = {}
        alloc_portfolio_target_gross: float = 1.0
        competition_policy_cache: Dict[Tuple[str, int, str, str], Dict[str, Any]] = {}

        try:
            from engine.runtime.strategy_allocator import compute_and_persist_strategy_allocations

            alloc_res = compute_and_persist_strategy_allocations(con, now_ms=int(now_ms)) or {}
            alloc_map = dict(alloc_res.get("allocations") or {})
            alloc_detail = dict(alloc_res.get("details") or {})
            alloc_regime = dict(alloc_res.get("regime") or {})
            alloc_regime_conf = float(alloc_res.get("regime_confidence", 0.0) or 0.0)
            alloc_reason = dict(alloc_res.get("reason") or {})
            alloc_alpha_runtime = dict(alloc_res.get("alpha_decay_runtime") or {})
            alloc_portfolio_target_gross = float(alloc_res.get("portfolio_target_gross", 1.0) or 1.0)
        except Exception as e:
            alloc_map = {}
            alloc_detail = {}
            alloc_regime = {}
            alloc_regime_conf = 0.0
            alloc_reason = {}
            alloc_alpha_runtime = {}
            alloc_portfolio_target_gross = 1.0
            _record_degraded_phase("strategy_allocator", "PORTFOLIO_STRATEGY_ALLOCATOR_FAILED", e)
            _warn_nonfatal("PORTFOLIO_STRATEGY_ALLOCATOR_FAILED", Exception("strategy allocator failed"), once_key="strategy_allocator")

        competition_capital_plan = _load_cached_competition_capital_plan(con, int(now_ms))

        try:
            shadow_perf = _load_shadow_performance(con)
            for s in shadow_perf:
                if s not in alloc_map:
                    alloc_map[s] = 0.02
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_SHADOW_PERFORMANCE_LOAD_FAILED", e, once_key="shadow_performance_load")

        # Back-compat fallback: use stored efficiency_score if allocator has no output
        eff_map = _load_strategy_efficiency(con)

        strategy_targets: Dict[str, Dict] = {}
        total_share = 0.0

        strat = None  # preserve original "last loaded strat" behavior for later get_regime_profile usage

        ordered_strategies: List[Tuple[str, str]] = []
        seen_strategies = set()

        for sname in live_strategies:
            ss = str(sname)
            if ss and ss not in seen_strategies:
                ordered_strategies.append((ss, "live"))
                seen_strategies.add(ss)

        for sname in shadow_strategies:
            ss = str(sname)
            if ss and ss not in seen_strategies:
                ordered_strategies.append((ss, "shadow"))
                seen_strategies.add(ss)

        for sname, stage in ordered_strategies:
            try:
                strat = load_strategy_module(str(sname))
                d = strat.build_desired(alerts=alerts, now_ms=int(now_ms)) or {}

                if alloc_map:
                    share_hint = float(alloc_map.get(str(sname), 0.0) or 0.0)
                else:
                    share_hint = float((eff_map.get(str(sname)) or {}).get("efficiency_score", 0.0) or 0.0)
                    share_hint = max(0.0, share_hint)

                if str(stage) == "shadow":
                    shadow_metrics = _score_shadow_targets(d)
                    shadow_metrics["allocator_share_hint"] = float(share_hint)
                    shadow_metrics["stage"] = "shadow"
                    if alloc_detail and str(sname) in alloc_detail:
                        shadow_metrics["allocator_detail"] = alloc_detail.get(str(sname))
                    _record_shadow_strategy_run(
                        con,
                        strategy_name=str(sname),
                        targets=d,
                        metrics=shadow_metrics,
                    )
                    continue

                strategy_targets[str(sname)] = d
                total_share += float(max(0.0, share_hint))
            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_STRATEGY_TARGET_ROW_FAILED",
                    e,
                    strategy_name=str(sname),
                )
                continue

        # If no allocation available, equal weight fallback
        if total_share <= 1e-9:
            total_share = float(len(strategy_targets) or 1)

        # Merge with allocator-weighted capital share
        desired: Dict[str, Dict] = {}

        for sname, targets in strategy_targets.items():
            if alloc_map:
                share_raw = float(alloc_map.get(str(sname), 0.0) or 0.0)
            else:
                share_raw = float((eff_map.get(str(sname)) or {}).get("efficiency_score", 0.0) or 0.0)
                share_raw = max(0.0, share_raw)

            share = float(share_raw) / float(total_share) if float(total_share) > 0 else 0.0

            # ------------------------------------------------------
            # Global Risk Envelope (Top-Down Capital Throttle)
            # ------------------------------------------------------
            global_scale = 1.0
            try:
                from engine.runtime.global_risk_envelope import compute_global_risk_envelope
                _g = compute_global_risk_envelope(con, now_ms=int(now_ms)) or {}
                global_scale = float(_g.get("global_scale", 1.0) or 1.0)
            except Exception:
                global_scale = 1.0

            allocator_portfolio_scale = max(0.0, float(alloc_portfolio_target_gross))
            alpha_runtime_scale = 1.0
            try:
                alpha_runtime_scale = float((alloc_alpha_runtime or {}).get("min_throttle_mult", 1.0) or 1.0)
            except Exception:
                alpha_runtime_scale = 1.0
            alpha_runtime_scale = max(0.0, min(1.0, float(alpha_runtime_scale)))

            share = (
                float(share)
                * float(global_scale)
                * float(allocator_portfolio_scale)
                * float(alpha_runtime_scale)
            )

            for sym, tgt in (targets or {}).items():
                try:
                    w = float((tgt or {}).get("weight", 0.0) or 0.0)
                    w = float(w) * float(share)

                    # ---------------------------------
                    # REGIME EXECUTION ADJUSTMENT
                    # ---------------------------------
                    try:
                        from engine.strategy.regime_stack import compute_regime_vector

                        reg = compute_regime_vector(symbol=sym, ts_ms=int(now_ms), con=con) or {}

                        compat = 1.0

                        vol_cluster = float((reg.get("micro") or {}).get("vol_clustered", 0.0))
                        thin_liq = float((reg.get("micro") or {}).get("liquidity_thin", 0.0))
                        dd_shift = float((reg.get("macro") or {}).get("drawdown_shift", 0.0))

                        stress = max(vol_cluster, thin_liq, dd_shift)

                        compat = max(0.25, 1.0 - float(stress) * 0.5)

                        w = float(w) * float(compat)

                        try:
                            tgt.setdefault("reason", {})
                            tgt["reason"]["exec_regime_compat"] = float(compat)
                            tgt["reason"]["exec_regime_stress"] = float(stress)
                        except Exception as e:
                            _warn_nonfatal("PORTFOLIO_EXEC_REGIME_REASON_FAILED", e, once_key="exec_regime_reason")

                    except Exception as e:
                        _warn_nonfatal("PORTFOLIO_EXEC_REGIME_VECTOR_FAILED", e, once_key="exec_regime_vector")

                    exj = str((tgt or {}).get("explain_json", "{}") or "{}")
                    model_identity = _extract_model_identity_from_explain(exj)
                    source_alert_id = (tgt or {}).get("source_alert_id")
                    alert_meta = {}
                    if source_alert_id is not None:
                        try:
                            alert_meta = dict(alert_meta_index.get(int(source_alert_id)) or {})
                        except Exception:
                            alert_meta = {}
                    model_id = _normalize_model_id(
                        (tgt or {}).get("model_id") or model_identity.get("model_id")
                    )
                    model_name = str(
                        (tgt or {}).get("model_name")
                        or model_identity.get("model_name")
                        or alert_meta.get("model_name")
                        or ""
                    ).strip()
                    regime_name = str(
                        (tgt or {}).get("regime")
                        or model_identity.get("regime")
                        or alert_meta.get("regime")
                        or "global"
                    ).strip() or "global"
                    horizon_s = int(
                        (tgt or {}).get("horizon_s")
                        or alert_meta.get("horizon_s")
                        or _extract_horizon_s_from_explain_json(exj)
                        or 0
                    )
                    competition_policy: Dict[str, Any] = {}
                    competition_reason_code = ""
                    if model_name:
                        try:
                            competition_key = (
                                str(sym).upper().strip(),
                                int(horizon_s),
                                str(model_name),
                                str(regime_name),
                            )
                            if competition_key not in competition_policy_cache:
                                competition_policy_cache[competition_key] = dict(
                                    _competition_policy_from_cached_plan(
                                        competition_capital_plan,
                                        symbol=str(sym).upper().strip(),
                                        horizon_s=int(horizon_s),
                                        model_name=str(model_name),
                                        regime=str(regime_name),
                                    )
                                    or {}
                                )
                            competition_policy = dict(competition_policy_cache.get(competition_key) or {})
                        except Exception as e:
                            competition_policy = {}
                            _warn_nonfatal(
                                "PORTFOLIO_COMPETITION_POLICY_LOAD_FAILED",
                                e,
                                once_key=f"competition_policy:{sym}:{model_name}:{horizon_s}:{regime_name}",
                            )

                    if model_name and competition_policy:
                        policy_reason = str((competition_policy or {}).get("reason") or "")
                        competition_cap_mult = float(
                            (competition_policy or {}).get("capital_multiplier")
                            or (competition_policy or {}).get("effective_allocation_fraction")
                            or (competition_policy or {}).get("model_weight")
                            or 0.0
                        )
                        if bool((competition_policy or {}).get("blocked")) and policy_reason in {
                            "competition_plan_stale",
                            "no_group_allocation",
                        }:
                            competition_reason_code = policy_reason
                        elif bool((competition_policy or {}).get("blocked")) or competition_cap_mult <= 0.0:
                            w = 0.0
                            competition_reason_code = str(
                                policy_reason or "competition_blocked"
                            )
                        else:
                            w = float(w) * float(competition_cap_mult)
                            competition_reason_code = "competition_capital_applied"
                    elif model_name:
                        competition_reason_code = "competition_policy_missing"
                    else:
                        competition_reason_code = "model_identity_missing"
                    desired_key = f"{model_id}:{str(sym).upper().strip()}"

                    cur = desired.get(desired_key)
                    if cur is None:
                        desired[desired_key] = dict(tgt)
                        desired[desired_key]["model_id"] = str(model_id)
                        desired[desired_key]["model_name"] = str(model_name)
                        desired[desired_key]["regime"] = str(regime_name)
                        desired[desired_key]["horizon_s"] = int(horizon_s)
                        desired[desired_key]["competition_policy"] = dict(competition_policy or {})
                        desired[desired_key]["symbol"] = str(sym).upper().strip()
                        desired[desired_key]["weight"] = float(w)
                        desired[desired_key]["source_alert_id"] = tgt.get("source_alert_id")
                        desired[desired_key]["prediction_id"] = (
                            tgt.get("prediction_id")
                            if tgt.get("prediction_id") not in (None, "")
                            else alert_meta.get("prediction_id")
                        )
                        desired[desired_key]["explain_json"] = exj
                        desired[desired_key].setdefault("reason", {})
                        desired[desired_key]["reason"]["strategy"] = str(sname)
                        desired[desired_key]["reason"]["strategy_share"] = float(share)
                        desired[desired_key]["reason"]["strategy_alloc"] = {str(sname): float(share)}
                        desired[desired_key]["reason"]["allocator_regime"] = dict(alloc_regime.get("regimes") or {})
                        desired[desired_key]["reason"]["allocator_regime_confidence"] = float(alloc_regime_conf)
                        desired[desired_key]["reason"]["allocator_portfolio_target_gross"] = float(alloc_portfolio_target_gross)
                        desired[desired_key]["reason"]["allocator_alpha_decay_runtime"] = dict(alloc_alpha_runtime)
                        desired[desired_key]["reason"]["allocator_reason"] = dict(alloc_reason)
                        desired[desired_key]["reason"]["score"] = float((tgt.get("reason") or {}).get("score", 0.0) or 0.0)
                        desired[desired_key]["reason"]["confidence"] = float((tgt.get("reason") or {}).get("confidence", 0.0) or 0.0)
                        desired[desired_key]["reason"]["expected_z"] = float((tgt.get("reason") or {}).get("expected_z", 0.0) or 0.0)
                        desired[desired_key]["reason"]["competition"] = {
                            "policy": dict(competition_policy or {}),
                            "reason_code": str(competition_reason_code),
                            "capital_applied_upstream": bool(competition_reason_code == "competition_capital_applied"),
                            "model_name": str(model_name),
                            "regime": str(regime_name),
                            "horizon_s": int(horizon_s),
                        }
                        if alloc_detail and str(sname) in alloc_detail:
                            desired[desired_key]["reason"]["strategy_alloc_detail"] = alloc_detail.get(str(sname))
                    else:
                        # combine weights from multiple strategies
                        cur_w = float(cur.get("weight", 0.0) or 0.0)
                        desired[desired_key]["weight"] = float(cur_w + w)
                        if model_name and not str(desired[desired_key].get("model_name") or "").strip():
                            desired[desired_key]["model_name"] = str(model_name)
                        if not desired[desired_key].get("horizon_s"):
                            desired[desired_key]["horizon_s"] = int(horizon_s)
                        if desired[desired_key].get("prediction_id") in (None, "") and (
                            tgt.get("prediction_id") not in (None, "") or alert_meta.get("prediction_id") not in (None, "")
                        ):
                            desired[desired_key]["prediction_id"] = (
                                tgt.get("prediction_id")
                                if tgt.get("prediction_id") not in (None, "")
                                else alert_meta.get("prediction_id")
                            )
                        if not str(desired[desired_key].get("regime") or "").strip():
                            desired[desired_key]["regime"] = str(regime_name)
                        if competition_policy and not isinstance(desired[desired_key].get("competition_policy"), dict):
                            desired[desired_key]["competition_policy"] = dict(competition_policy or {})
                        desired[desired_key].setdefault("reason", {})
                        desired[desired_key]["reason"]["multi_strategy"] = True
                        if isinstance(desired[desired_key]["reason"], dict):
                            desired[desired_key]["reason"]["competition"] = {
                                "policy": dict(competition_policy or desired[desired_key].get("competition_policy") or {}),
                                "reason_code": str(competition_reason_code),
                                "capital_applied_upstream": bool(competition_reason_code == "competition_capital_applied"),
                                "model_name": str(model_name or desired[desired_key].get("model_name") or ""),
                                "regime": str(regime_name or desired[desired_key].get("regime") or "global"),
                                "horizon_s": int(horizon_s or desired[desired_key].get("horizon_s") or 0),
                            }

                    if desired[desired_key].get("source_alert_id") is None:
                        desired[desired_key]["source_alert_id"] = tgt.get("source_alert_id")

                    try:
                        sa = desired[desired_key]["reason"].get("strategy_alloc")
                        if not isinstance(sa, dict):
                            sa = {}
                        sa[str(sname)] = float(share)
                        desired[desired_key]["reason"]["strategy_alloc"] = sa
                        desired[desired_key]["reason"]["allocator_portfolio_target_gross"] = float(alloc_portfolio_target_gross)
                        desired[desired_key]["reason"]["allocator_alpha_decay_runtime"] = dict(alloc_alpha_runtime)
                    except Exception as e:
                        _warn_nonfatal("PORTFOLIO_STRATEGY_ALLOC_REASON_FAILED", e, once_key="strategy_alloc_reason")
                except Exception as e:
                    _warn_nonfatal(
                        "PORTFOLIO_DESIRED_STRATEGY_ALLOC_FAILED",
                        e,
                        strategy_name=str(sname),
                    )
                    continue

        # defensive normalization (strategy modules are pluggable)
        norm: Dict[str, Dict] = {}
        for desired_key, tgt in (desired or {}).items():
            try:
                s = _desired_symbol(desired_key, tgt)
                if not s:
                    continue
                side = str((tgt or {}).get("side", "FLAT")).upper()
                if side not in ("LONG", "SHORT", "FLAT"):
                    side = "FLAT"

                w = float((tgt or {}).get("weight", 0.0))
                if not (w == w):  # NaN
                    w = 0.0

                # if FLAT, force weight=0
                if side == "FLAT":
                    w = 0.0
                    side = "FLAT"

                # apply per-symbol cap
                w = _clamp(abs(w), 0.0, _symbol_cap(s))

                if side == "SHORT":
                    w = -float(w)

                # ------------------------------------------------------
                # Execution Regime Sizing (feature-aware sizing)
                # ------------------------------------------------------
                if PORTFOLIO_USE_EXEC_REGIME and abs(float(w)) > 0.0:
                    _sgn = -1.0 if float(w) < 0 else 1.0
                    _mag = abs(float(w))

                    try:
                        skew_z = float(_get_factor_feature_asof(con, "options.skew_25d_z", int(now_ms)))
                        flow_z = float(_get_factor_feature_asof(con, "flows.index_constituent_imbalance_z", int(now_ms)))
                    except Exception:
                        skew_z = 0.0
                        flow_z = 0.0
# =========================
# SECTION 6 / ~200 lines
# =========================

                    stress_mag = max(
                        0.0,
                        max(
                            abs(skew_z) - float(PORTFOLIO_EXEC_SKEW_Z_TH),
                            abs(flow_z) - float(PORTFOLIO_EXEC_FLOW_Z_TH),
                        ),
                    )

                    stress_mult = 1.0
                    if stress_mag > 0.0:
                        stress_mult = float(
                            _clamp(
                                1.0 - (stress_mag * float(PORTFOLIO_EXEC_STRESS_SIZE_REDUCTION)),
                                float(PORTFOLIO_EXEC_REGIME_FLOOR),
                                1.0,
                            )
                        )

                    # earnings proximity (nearest date)
                    earnings_mult = 1.0
                    try:
                        row_e = con.execute(
                            """
                            SELECT earnings_date
                            FROM earnings_calendar
                            WHERE symbol=?
                            ORDER BY ABS(julianday(earnings_date) - julianday(date('now'))) ASC
                            LIMIT 1
                            """,
                            (str(s),),
                        ).fetchone()

                        if row_e and row_e[0]:
                            jd = con.execute(
                                "SELECT (julianday(?) - julianday(date('now')))",
                                (str(row_e[0]),),
                            ).fetchone()
                            if jd and jd[0] is not None:
                                days = float(jd[0])
                                decay = math.exp(-abs(days) / 5.0)
                                earnings_mult = float(
                                    _clamp(
                                        1.0 - (decay * float(PORTFOLIO_EXEC_EARNINGS_SIZE_REDUCTION)),
                                        float(PORTFOLIO_EXEC_REGIME_FLOOR),
                                        1.0,
                                    )
                                )
                    except Exception:
                        earnings_mult = 1.0

                    _mag = float(_mag) * float(stress_mult) * float(earnings_mult)
                    w = float(_sgn) * float(_mag)

                # preserve fields expected downstream
                reason = (tgt or {}).get("reason", {})
                if not isinstance(reason, dict):
                    reason = {"raw": reason}

                try:
                    reason["confidence"] = float(tgt.get("confidence", reason.get("confidence", 0.0)))
                except Exception:
                    reason["confidence"] = 0.0

                exj = (tgt or {}).get("explain_json", "{}")
                if exj is None:
                    exj = "{}"
                exj = str(exj)

                src_id = tgt.get("source_alert_id") if isinstance(tgt, dict) else None
                try:
                    src_id = int(src_id) if src_id is not None else None
                except Exception:
                    src_id = None
                prediction_id = tgt.get("prediction_id") if isinstance(tgt, dict) else None
                try:
                    prediction_id = int(prediction_id) if prediction_id not in (None, "") else None
                except Exception:
                    prediction_id = None
                prediction_id = _validated_prediction_id(con, prediction_id)

                strategy_name_for_symbol = str(reason.get("strategy") or "").strip().lower()
                strategy_module_for_symbol = None
                if strategy_name_for_symbol:
                    try:
                        strategy_module_for_symbol = load_strategy_module(strategy_name_for_symbol)
                    except Exception:
                        strategy_module_for_symbol = None

                # --- Regime Vector Injection ---
                try:
                    from engine.strategy.regime_stack import compute_regime_vector, regime_compatibility

                    regime_vector = compute_regime_vector(symbol=s, ts_ms=int(now_ms), con=con)

                    try:
                        regime_profile = (
                            getattr(strategy_module_for_symbol, "get_regime_profile", lambda: {})()
                            if strategy_module_for_symbol
                            else {}
                        )
                    except Exception:
                        regime_profile = {}

                    compat = float(regime_compatibility(regime_profile, regime_vector))
                    compat = max(0.0, min(1.0, float(compat)))

                    regime_conf = float(((regime_vector.get("confidence") or {}).get("overall", 1.0)) or 1.0)
                    regime_conf = max(0.0, min(1.0, float(regime_conf)))

                    regime_exec_scale = max(0.25, float(compat) * (0.50 + 0.50 * float(regime_conf)))
                    w = float(w) * float(regime_exec_scale)

                    try:
                        reason["regime_signals"] = dict(regime_vector.get("regimes") or {})
                        reason["regime_confidence"] = float(regime_conf)
                        reason["regime_exec_scale"] = float(regime_exec_scale)
                    except Exception as e:
                        _warn_nonfatal("PORTFOLIO_REGIME_VECTOR_REASON_FAILED", e, once_key="regime_vector_reason")
                except Exception:
                    regime_vector = {}
                    compat = 1.0
                    regime_conf = 1.0
                    regime_exec_scale = 1.0

                model_identity = _extract_model_identity_from_explain(exj)
                model_id = _normalize_model_id((tgt or {}).get("model_id") or model_identity.get("model_id"))
                state_key = f"{model_id}:{s}"

                norm[state_key] = {
                    "model_id": model_id,
                    "side": side,
                    "weight": float(w),
                    "source_alert_id": src_id,
                    "prediction_id": prediction_id,
                    "reason": reason,
                    "explain_json": exj,
                    **model_identity,
                    "symbol": s,
                    "regime_vector": regime_vector,
                    "regime_compatibility": compat,
                    "regime_confidence": regime_conf,
                    "regime_exec_scale": regime_exec_scale,
                    "regime_signals": dict(regime_vector.get("regimes") or {}),
                }

            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_NORMALIZE_DESIRED_ROW_FAILED",
                    e,
                    desired_key=str(desired_key),
                )
                continue

        desired = norm

        # renormalize gross
        grossE = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
        eff_cap = float(_eff_gross_cap())
        if grossE > float(eff_cap) and grossE > 1e-9:
            scaleE = float(eff_cap) / float(grossE)
            for sym in list(desired.keys()):
                desired[sym]["weight"] = float(desired[sym]["weight"]) * float(scaleE)

        # ---            -- ------------------------------------------------------
        # Auto blacklist enforcement (skip symbols temporarily banned)
        # ---            -- ------------------------------------------------------
        try:
            for desired_key in list(desired.keys()):
                sym = _desired_symbol(desired_key, desired.get(desired_key))
                if sym and is_blacklisted(con, sym, now_ms=int(now_ms)):
                    desired.pop(desired_key, None)
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_BLACKLIST_FILTER_FAILED", e, once_key="blacklist_filter")

        # ---            -- ------------------------------------------------------
        # Exploration cap: if symbol has few realized labels, cap weight
        # ---            -- ------------------------------------------------------
        try:
            for desired_key in list(desired.keys()):
                sym = _desired_symbol(desired_key, desired.get(desired_key))
                if not sym:
                    continue
                row = con.execute(
                    "SELECT COUNT(1) FROM labels WHERE symbol=?",
                    (str(sym),),
                ).fetchone()
                nlab = int(row[0] or 0) if row else 0

                if nlab < int(PORTFOLIO_EXPLORE_MIN_LABELS):
                    # Cap exposure for exploration symbols without forcing tiny/no-trade weights
                    w0 = float(desired[desired_key].get("weight", 0.0) or 0.0)
                    sgn = -1.0 if w0 < 0 else 1.0
                    floor_abs = min(float(PORTFOLIO_EXPLORE_MAX_W), max(0.0, abs(w0)))
                    wabs = float(_clamp(abs(w0), floor_abs, float(PORTFOLIO_EXPLORE_MAX_W)))
                    desired[desired_key]["weight"] = float(sgn * wabs)
                    # Tag for explainability/debugging
                    try:
                        r = desired[desired_key].get("reason") or {}
                        if isinstance(r, dict):
                            r["explore_cap"] = True
                            r["labels_n"] = int(nlab)
                            desired[desired_key]["reason"] = r
                    except Exception as e:
                        _warn_nonfatal("PORTFOLIO_EXPLORE_CAP_REASON_FAILED", e, once_key="explore_cap_reason")
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_EXPLORE_CAP_FAILED", e, once_key="explore_cap")

        # ---            -- ------------------------------------------------------
        # Optional expected-return blending (Black-Litterman)
        # ------------------------------------------------------
        try:
            desired = _apply_black_litterman_overlay(con, desired)
        except Exception as e:
            _record_degraded_phase("black_litterman", "PORTFOLIO_BLACK_LITTERMAN_FAILED", e)
            _warn_nonfatal("PORTFOLIO_BLACK_LITTERMAN_FAILED", e, once_key="portfolio_black_litterman")

        # ---            -- ------------------------------------------------------
        # Capital allocation optimizer (return vs drawdown utility)
        # ---            -- ------------------------------------------------------
        try:
            desired = _optimize_capital_allocation(con, desired)
        except Exception as e:
            _record_degraded_phase("capital_allocation_opt", "PORTFOLIO_CAPITAL_ALLOCATION_OPT_FAILED", e)
            _warn_nonfatal("PORTFOLIO_CAPITAL_ALLOCATION_OPT_FAILED", e, once_key="capital_allocation_opt")

        # ---            -- ------------------------------------------------------
        # Impact-aware sizing (penalize symbols with bad realized slippage)
        # ---            -- ------------------------------------------------------
        try:
            desired = _apply_impact_aware_sizing(con, desired)
        except Exception as e:
            _record_degraded_phase("impact_sizing", "PORTFOLIO_IMPACT_SIZING_FAILED", e)
            _warn_nonfatal("PORTFOLIO_IMPACT_SIZING_FAILED", e, once_key="impact_sizing")

        # Reward models whose books are less correlated with the rest of the portfolio.
        try:
            desired, _model_div = _apply_model_diversification_scoring(con, desired)
            try:
                _put_meta(
                    con,
                    "last_model_diversification",
                    json.dumps(_model_div or {}, separators=(",", ":"), sort_keys=True),
                )
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_MODEL_DIVERSIFICATION_META_FAILED", e, once_key="model_diversification_meta")
        except Exception as e:
            _record_degraded_phase("model_diversification", "PORTFOLIO_MODEL_DIVERSIFICATION_FAILED", e)
            _warn_nonfatal("PORTFOLIO_MODEL_DIVERSIFICATION_FAILED", e, once_key="model_diversification")
            _model_div = None

        # hard cap max positions (keep largest abs weights) (dynamic under preserve)
        eff_max_pos = int(_eff_max_positions())
        if eff_max_pos >= 0 and len(desired) > int(eff_max_pos):
            items = sorted(
                desired.items(),
                key=lambda kv: abs(float((kv[1] or {}).get("weight", 0.0))),
                reverse=True,
            )
            desired = dict(items[: int(eff_max_pos)])

        # ---            -- ------------------------------------------------------
        # Allocation mode is explicit when requested, otherwise preserves the
        # existing corr-opt -> prune -> legacy fallback order.
        # ---            -- ------------------------------------------------------
        desired = _apply_allocation_mode(con, desired)

        # safety: enforce portfolio gross cap even if strategy already normalized
        gross = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
        eff_cap = float(_eff_gross_cap())
        if gross > float(eff_cap) and gross > 1e-9:
            scale = float(eff_cap) / float(gross)
            for sym in list(desired.keys()):
                w0 = float(desired[sym].get("weight", 0.0) or 0.0)
                sign = -1.0 if w0 < 0 else 1.0
                desired[sym]["weight"] = sign * abs(float(w0) * float(scale))

        # ---            -- ------------------------------------------------------
        # A) VOL TARGETING (default-on): inverse-vol symbol sizing + portfolio vol target
        # ---            -- ------------------------------------------------------
        try:
            from engine.strategy.risk import (
                PORTFOLIO_USE_VOL_TARGET,
                TARGET_VOL,
                portfolio_realized_vol,
                portfolio_vol_target_scale,
                realized_vol_from_prices,
                symbol_vol_scale,
                vol_scale_weight,
            )

            if PORTFOLIO_USE_VOL_TARGET:
                for desired_key in list(desired.keys()):
                    sym = _desired_symbol(desired_key, desired.get(desired_key))
                    if not sym:
                        continue
                    vol = realized_vol_from_prices(con, sym)
                    if vol is None:
                        continue

                    sym_scale = float(symbol_vol_scale(vol))
                    desired[desired_key]["weight"] = float(vol_scale_weight(desired[desired_key]["weight"], vol))
                    desired[desired_key].setdefault("reason", {})
                    desired[desired_key]["reason"]["realized_vol"] = float(vol)
                    desired[desired_key]["reason"]["symbol_vol_scale"] = float(sym_scale)
                    desired[desired_key]["reason"]["vol_target_symbol_scaled"] = 1

                vol_scale, pre_portfolio_vol = portfolio_vol_target_scale(con, desired)

                if abs(float(vol_scale) - 1.0) > 1e-12:
                    for sym in list(desired.keys()):
                        desired[sym]["weight"] = float(desired[sym].get("weight", 0.0) or 0.0) * float(vol_scale)
                        desired[sym].setdefault("reason", {})
                        desired[sym]["reason"]["portfolio_vol_target"] = float(TARGET_VOL)
                        desired[sym]["reason"]["portfolio_vol_pre_scale"] = (
                            float(pre_portfolio_vol) if pre_portfolio_vol is not None else None
                        )
                        desired[sym]["reason"]["portfolio_vol_scale"] = float(vol_scale)

                # renormalize gross after vol targeting
                gross2 = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
                if gross2 > float(PORTFOLIO_GROSS_CAP) and gross2 > 1e-9:
                    scale2 = float(PORTFOLIO_GROSS_CAP) / float(gross2)
                    for sym in list(desired.keys()):
                        w0 = float(desired[sym].get("weight", 0.0) or 0.0)
                        sign = -1.0 if w0 < 0 else 1.0
                        desired[sym]["weight"] = sign * abs(float(w0) * float(scale2))
                        desired[sym].setdefault("reason", {})
                        desired[sym]["reason"]["portfolio_gross_cap_after_vol_target"] = float(PORTFOLIO_GROSS_CAP)
                        desired[sym]["reason"]["portfolio_gross_scale_after_vol_target"] = float(scale2)

                post_portfolio_vol = portfolio_realized_vol(con, desired)
                for sym in list(desired.keys()):
                    desired[sym].setdefault("reason", {})
                    desired[sym]["reason"]["portfolio_vol_post_target"] = (
                        float(post_portfolio_vol) if post_portfolio_vol is not None else None
                    )

                try:
                    _set_risk_state_inline(con, "portfolio_vol_target_enabled", "1")
                    _set_risk_state_inline(con, "portfolio_target_vol", str(float(TARGET_VOL)))
                    _set_risk_state_inline(
                        con,
                        "portfolio_realized_vol_pre_target",
                        "" if pre_portfolio_vol is None else str(float(pre_portfolio_vol)),
                    )
                    _set_risk_state_inline(
                        con,
                        "portfolio_realized_vol_post_target",
                        "" if post_portfolio_vol is None else str(float(post_portfolio_vol)),
                    )
                    _set_risk_state_inline(con, "portfolio_vol_target_scale", str(float(vol_scale)))
                    _set_risk_state_inline(con, "portfolio_vol_target_ts_ms", str(int(_now_ms())))
                except Exception as e:
                    _warn_nonfatal("PORTFOLIO_VOL_TARGET_STATE_WRITE_FAILED", e, once_key="vol_target_state_write")
            else:
                try:
                    _set_risk_state_inline(con, "portfolio_vol_target_enabled", "0")
                    _set_risk_state_inline(con, "portfolio_target_vol", str(float(TARGET_VOL)))
                    _set_risk_state_inline(con, "portfolio_realized_vol_pre_target", "")
                    _set_risk_state_inline(con, "portfolio_realized_vol_post_target", "")
                    _set_risk_state_inline(con, "portfolio_vol_target_scale", "1.0")
                    _set_risk_state_inline(con, "portfolio_vol_target_ts_ms", str(int(_now_ms())))
                except Exception as e:
                    _warn_nonfatal("PORTFOLIO_VOL_TARGET_STATE_WRITE_FAILED", e, once_key="vol_target_state_write")
        except Exception as e:
            _record_degraded_phase("vol_target", "PORTFOLIO_VOL_TARGET_FAILED", e)
            _warn_nonfatal("PORTFOLIO_VOL_TARGET_FAILED", e, once_key="vol_target")
# =========================
# SECTION 7 / ~200 lines
# =========================

        # ---            -- ------------------------------------------------------
        # B) STRESS / REGIME GATE (opt-in): compress exposure under stress
        # Now incorporates:
        #   • VIX stress
        #   • Options skew z-score
        #   • Index constituent flow imbalance z-score
        # ---            -- ------------------------------------------------------
        try:
            if PORTFOLIO_USE_STRESS_GATE and desired:
                # --- VIX baseline ---
                st = _vix_stress(con)
                vix_z = float(st.get("z", 0.0))
                f_vix = float(_stress_factor_from_vix_z(vix_z))

                # --- Skew + Flow regime factors (latest as-of now_ms) ---
                skew_z = 0.0
                flow_z = 0.0

                try:
                    row = con.execute(
                        """
                        SELECT value
                        FROM factor_features
                        WHERE feature_id='options.skew_25d_z'
                        ORDER BY asof_ts DESC
                        LIMIT 1
                        """
                    ).fetchone()
                    if row:
                        skew_z = float(row[0] or 0.0)
                except Exception:
                    skew_z = 0.0

                try:
                    row = con.execute(
                        """
                        SELECT value
                        FROM factor_features
                        WHERE feature_id='flows.index_constituent_imbalance_z'
                        ORDER BY asof_ts DESC
                        LIMIT 1
                        """
                    ).fetchone()
                    if row:
                        flow_z = float(row[0] or 0.0)
                except Exception:
                    flow_z = 0.0

                # --- Stress magnitude beyond thresholds ---
                skew_excess = max(0.0, abs(skew_z) - 1.5)
                flow_excess = max(0.0, abs(flow_z) - 2.0)

                stress_mag = max(skew_excess, flow_excess)

                # compression factor (bounded)
                f_struct = 1.0
                if stress_mag > 0.0:
                    f_struct = max(0.20, 1.0 - (0.35 * stress_mag))

                # combined factor
                f_total = float(min(f_vix, f_struct))

                if f_total < 1.0:
                    for sym in list(desired.keys()):
                        desired[sym]["weight"] = float(desired[sym]["weight"]) * float(f_total)
                        desired[sym].setdefault("reason", {})
                        desired[sym]["reason"]["stress_gate_factor"] = float(f_total)
                        desired[sym]["reason"]["stress_vix_z"] = float(vix_z)
                        desired[sym]["reason"]["stress_skew_z"] = float(skew_z)
                        desired[sym]["reason"]["stress_flow_z"] = float(flow_z)

                # renormalize gross after compression
                gross_s = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
                eff_cap = float(_eff_gross_cap())
                if gross_s > float(eff_cap) and gross_s > 1e-9:
                    scale_s = float(eff_cap) / float(gross_s)
                    for sym in list(desired.keys()):
                        w0 = float(desired[sym].get("weight", 0.0) or 0.0)
                        sign = -1.0 if w0 < 0 else 1.0
                        desired[sym]["weight"] = sign * abs(float(w0) * float(scale_s))

        except Exception as e:
            _record_degraded_phase("stress_gate", "PORTFOLIO_EXEC_REGIME_SIZING_FAILED", e)
            _warn_nonfatal("PORTFOLIO_EXEC_REGIME_SIZING_FAILED", e, once_key="exec_regime_sizing")

        # ---            -- ------------------------------------------------------
        # B2) SOCIAL GATE (opt-in): block/downsizing under manipulation risk
        # ---            -- ------------------------------------------------------
        try:
            if PORTFOLIO_USE_SOCIAL_GATE and desired:
                from engine.strategy.social_risk import social_gate_for_symbol

                for desired_key in list(desired.keys()):
                    sym = _desired_symbol(desired_key, desired.get(desired_key))
                    if not sym:
                        continue
                    g = social_gate_for_symbol(
                        con,
                        str(sym),
                        int(now_ms),
                        bucket_sec=int(PORTFOLIO_SOCIAL_BUCKET_SEC),
                        manip_block_th=float(PORTFOLIO_SOCIAL_MANIP_BLOCK_TH),
                        shock_th=float(PORTFOLIO_SOCIAL_ATTEN_SHOCK_TH),
                        shock_factor=float(PORTFOLIO_SOCIAL_SHOCK_FACTOR),
                    ) or {}

                    if g.get("block"):
                        desired[desired_key]["weight"] = 0.0
                        desired[desired_key]["side"] = "FLAT"
                        desired[desired_key].setdefault("reason", {})
                        desired[desired_key]["reason"]["social_gate_block"] = 1
                        desired[desired_key]["reason"]["social_manip_risk"] = float(g.get("manip_risk", 0.0))
                        desired[desired_key]["reason"]["social_attention_shock"] = float(g.get("attention_shock", 0.0))
                        desired[desired_key]["reason"]["social_promo_likelihood"] = float(g.get("promo_likelihood_mean", 0.0))
                        continue

                    f = float(g.get("factor", 1.0))
                    if f < 1.0:
                        desired[desired_key]["weight"] = float(desired[desired_key]["weight"]) * f
                        desired[desired_key].setdefault("reason", {})
                        desired[desired_key]["reason"]["social_gate_factor"] = float(f)
                        desired[desired_key]["reason"]["social_manip_risk"] = float(g.get("manip_risk", 0.0))
                        desired[desired_key]["reason"]["social_attention_shock"] = float(g.get("attention_shock", 0.0))
                        desired[desired_key]["reason"]["social_promo_likelihood"] = float(g.get("promo_likelihood_mean", 0.0))

                # renormalize gross after social compression (still respect gross cap)
                gross_soc = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
                eff_cap = float(_eff_gross_cap())
                if gross_soc > float(eff_cap) and gross_soc > 1e-9:
                    scale_soc = float(eff_cap) / float(gross_soc)
                    for sym in list(desired.keys()):
                        w0 = float(desired[sym].get("weight", 0.0) or 0.0)
                        sign = -1.0 if w0 < 0 else 1.0
                        desired[sym]["weight"] = sign * abs(float(w0) * float(scale_soc))
        except Exception as e:
            _record_degraded_phase("social_gate", "PORTFOLIO_SOCIAL_GATE_FAILED", e)
            _warn_nonfatal("PORTFOLIO_SOCIAL_GATE_FAILED", e, once_key="social_gate")

        # ---            -- ------------------------------------------------------
        # C) VOL-OF-VOL GATE (opt-in): per-symbol compression in unstable regimes
        # Uses price-only proxy from engine.tech_indicators (if present).
        # ---            -- ------------------------------------------------------
        try:
            if PORTFOLIO_USE_VOV_GATE and desired:
                try:
                    from engine.strategy.tech_indicators import compute_tech_features
                except Exception:
                    compute_tech_features = None

                if compute_tech_features:
                    now_ms2 = _now_ms()
                    a = float(PORTFOLIO_VOV_ALPHA)
                    v_lo = float(PORTFOLIO_VOV_FLOOR)
                    v_hi = float(PORTFOLIO_VOV_CEIL)
                    span = max(1e-12, (v_hi - v_lo))

                    for desired_key in list(desired.keys()):
                        sym = _desired_symbol(desired_key, desired.get(desired_key))
                        if not sym:
                            continue
                        tf = compute_tech_features(str(sym), int(now_ms2)) or {}
                        vv = float(tf.get("vol_of_vol", 0.0))

                        # normalize vv into [0,1] then apply penalty
                        x = (vv - v_lo) / span
                        x = float(_clamp(x, 0.0, 1.0))

                        # factor = 1/(1 + alpha*x)
                        f = float(1.0 / (1.0 + a * x))
                        desired[desired_key]["weight"] = float(desired[desired_key]["weight"]) * float(f)

                        desired[desired_key].setdefault("reason", {})
                        desired[desired_key]["reason"]["vov_gate_factor"] = float(f)
                        desired[desired_key]["reason"]["vov_value"] = float(vv)

                    # renormalize gross after vov compression
                    gross_v = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
                    eff_cap = float(_eff_gross_cap())
                    if gross_v > float(eff_cap) and gross_v > 1e-9:
                        scale_v = float(eff_cap) / float(gross_v)
                        for sym in list(desired.keys()):
                            desired[sym]["weight"] = float(desired[sym]["weight"]) * float(scale_v)
        except Exception as e:
            _record_degraded_phase("vol_of_vol_gate", "PORTFOLIO_VOL_OF_VOL_GATE_FAILED", e)
            _warn_nonfatal("PORTFOLIO_VOL_OF_VOL_GATE_FAILED", e, once_key="vol_of_vol_gate")
# =========================
# SECTION 8 / ~200 lines
# =========================

        # ---            -- ------------------------------------------------------
        # Phase 5.2: POSITION SIZE POLICY (confidence -> factor)
        # (must happen BEFORE orders are emitted)
        # ---            -- ------------------------------------------------------
        try:
            from engine.strategy.size_policy import load_latest_size_policy, size_factor
            from engine.strategy.drawdown_state import get_current_drawdown

            pol = load_latest_size_policy(con)
            if pol:
                dd = float(get_current_drawdown(con))
                for sym in list(desired.keys()):
                    try:
                        conf = float((desired[sym].get("reason") or {}).get("confidence", 0.0))
                    except Exception:
                        conf = 0.0
                    f = float(size_factor(pol, conf, drawdown=dd))

                    desired[sym]["weight"] = float(desired[sym]["weight"]) * f

                    # annotate for auditability
                    desired[sym].setdefault("reason", {})
                    desired[sym]["reason"]["size_factor"] = float(f)
                    desired[sym]["reason"]["size_policy_ts_ms"] = int(pol.get("ts_ms", 0))
                    desired[sym]["reason"]["drawdown_for_sizing"] = float(dd)
        except Exception as e:
            _record_degraded_phase("size_policy", "PORTFOLIO_SIZE_POLICY_FAILED", e)
            _warn_nonfatal("PORTFOLIO_SIZE_POLICY_FAILED", e, once_key="size_policy")

        # ---            -- ------------------------------------------------------
        # Phase 5.3: EXECUTION REALISM (opt-in)
        # - blocks/downsizes intents when symbol prices are stale
        # - downsizes under elevated stress (VIX z) if VIX is present
        # - downsizes under high ATR% (volatility proxy)
        # ---            -- ------------------------------------------------------
        if PORTFOLIO_USE_EXEC_REALISM:
            for desired_key in list(desired.keys()):
                sym = _desired_symbol(desired_key, desired.get(desired_key))
                if not sym:
                    continue
                try:
                    ef, meta = _execution_realism_factor(con, sym, int(now_ms))
                except Exception:
                    ef, meta = 1.0, {
                        "staleness_sec": 0.0,
                        "stress_vix_z_60": 0.0,
                        "atr_pct": 0.0,
                        "slippage_bps_est": 0.0,
                    }

                desired[desired_key]["weight"] = float(desired[desired_key].get("weight", 0.0)) * float(ef)

                # annotate for auditability/explainability
                desired[desired_key].setdefault("reason", {})
                desired[desired_key]["reason"]["exec_realism_factor"] = float(ef)
                desired[desired_key]["reason"]["exec_staleness_sec"] = float(meta.get("staleness_sec", 0.0))
                desired[desired_key]["reason"]["exec_stress_vix_z_60"] = float(meta.get("stress_vix_z_60", 0.0))
                desired[desired_key]["reason"]["exec_atr_pct"] = float(meta.get("atr_pct", 0.0))
                desired[desired_key]["reason"]["exec_slippage_bps_est"] = float(meta.get("slippage_bps_est", 0.0))

        # renormalize gross after size policy scaling
        gross3 = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
        eff_cap = float(_eff_gross_cap())
        if gross3 > float(eff_cap) and gross3 > 1e-9:
            scale3 = float(eff_cap) / float(gross3)
            for sym in list(desired.keys()):
                desired[sym]["weight"] = float(desired[sym]["weight"]) * float(scale3)
        # ---            -- ------------------------------------------------------
        # B2) CAPITAL PRESERVATION MODE: compress gross exposure + annotate reasons
        # ---            -- ------------------------------------------------------
        try:
            if _capital_mode() == "preserve" and desired:
                cap = float(_eff_gross_cap())
                gross0 = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
                if gross0 > cap and gross0 > 1e-9:
                    scale_cap = float(cap) / float(gross0)
                    for sym in list(desired.keys()):
                        desired[sym]["weight"] = float(desired[sym]["weight"]) * float(scale_cap)
                        desired[sym].setdefault("reason", {})
                        desired[sym]["reason"]["capital_mode"] = "preserve"
                        desired[sym]["reason"]["capital_preserve_gross_cap"] = float(cap)
                        desired[sym]["reason"]["capital_preserve_scale"] = float(scale_cap)
        except Exception as e:
            _record_degraded_phase("capital_preserve_scaling", "PORTFOLIO_CAPITAL_PRESERVE_SCALING_FAILED", e)
            _warn_nonfatal("PORTFOLIO_CAPITAL_PRESERVE_SCALING_FAILED", e, once_key="capital_preserve_scaling")

        # ---            -- ------------------------------------------------------
        # Phase 6: REGIME-ADAPTIVE CAPITAL SCALING (base * confidence * VIX * drawdown)
        # ---            -- ------------------------------------------------------
        try:
            from engine.strategy.regime_size import regime_capital_scale

            _rs = regime_capital_scale(con=con, anchor=str(PORTFOLIO_REGIME_ANCHOR))
            mult = float((_rs or {}).get("final_mult", 1.0))

            # persist last scaling decision (auditability)
            try:
                _put_meta(
                    con,
                    "last_regime_scaling",
                    json.dumps(_rs or {}, separators=(",", ":"), sort_keys=True),
                )
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_REGIME_SCALING_META_FAILED", e, once_key="regime_scaling_meta")

            if mult != 1.0:
                for sym in list(desired.keys()):
                    try:
                        desired[sym]["weight"] = float(desired[sym].get("weight", 0.0) or 0.0) * float(mult)
                        desired[sym].setdefault("reason", {})
                        desired[sym]["reason"]["regime_anchor"] = str(
                            (_rs or {}).get("anchor") or str(PORTFOLIO_REGIME_ANCHOR)
                        )
                        desired[sym]["reason"]["regime"] = str((_rs or {}).get("regime") or "")
                        desired[sym]["reason"]["regime_base_mult"] = float((_rs or {}).get("base_mult", 1.0))
                        desired[sym]["reason"]["regime_conf"] = float((_rs or {}).get("conf", 1.0))
                        desired[sym]["reason"]["regime_conf_mult"] = float((_rs or {}).get("conf_mult", 1.0))
                        desired[sym]["reason"]["regime_vix_z"] = (_rs or {}).get("vix_z", None)
                        desired[sym]["reason"]["regime_vix_mult"] = float((_rs or {}).get("vix_mult", 1.0))
                        desired[sym]["reason"]["regime_dd"] = (_rs or {}).get("dd", None)
                        desired[sym]["reason"]["regime_dd_mult"] = float((_rs or {}).get("dd_mult", 1.0))
                        desired[sym]["reason"]["regime_final_mult"] = float((_rs or {}).get("final_mult", 1.0))
                    except Exception:
                        _warn_nonfatal("PORTFOLIO_REGIME_SCALING_REASON_FAILED", Exception("regime scaling reason failed"), once_key="regime_scaling_reason")

                # renormalize gross after regime scaling
                grossR = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
                eff_cap = float(_eff_gross_cap())
                if grossR > float(eff_cap) and grossR > 1e-9:
                    scaleR = float(eff_cap) / float(grossR)
                    for sym in list(desired.keys()):
                        desired[sym]["weight"] = float(desired[sym]["weight"]) * float(scaleR)
        except Exception as e:
            _record_degraded_phase("regime_scaling", "PORTFOLIO_REGIME_SCALING_FAILED", e)
            _warn_nonfatal("PORTFOLIO_REGIME_SCALING_FAILED", e, once_key="regime_scaling")

        # ---            -- ------------------------------------------------------
        # Allocation risk overlays: crowding, concentration, execution capacity
        # ------------------------------------------------------
        try:
            from engine.strategy.allocation_risk_overlay import apply_allocation_risk_overlays

            desired, _overlay = apply_allocation_risk_overlays(
                con,
                desired,
                gross_cap=float(_eff_gross_cap()),
                now_ms=int(now_ms),
            )
            try:
                _put_meta(
                    con,
                    "last_allocation_risk_overlay",
                    json.dumps(_overlay or {}, separators=(",", ":"), sort_keys=True),
                )
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_ALLOCATION_OVERLAY_META_FAILED", e, once_key="allocation_overlay_meta")
        except Exception as e:
            _record_degraded_phase("allocation_overlay", "PORTFOLIO_ALLOCATION_OVERLAY_FAILED", e)
            _warn_nonfatal("PORTFOLIO_ALLOCATION_OVERLAY_FAILED", e, once_key="allocation_overlay")
            _overlay = None

        # ---            -- ------------------------------------------------------
        # Phase 1.5: PORTFOLIO RISK ENGINE (institutional exposure / vol / corr / budgets)
        # ---            -- ------------------------------------------------------
        try:
            desired, _risk_engine = apply_portfolio_risk_engine(con, desired, state, now_ms=int(now_ms))
            try:
                request_monte_carlo_refresh(desired)
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_MONTE_CARLO_REFRESH_FAILED", e, once_key="monte_carlo_refresh")
            try:
                _put_meta(
                    con,
                    "last_portfolio_risk_engine",
                    json.dumps(_risk_engine or {}, separators=(",", ":"), sort_keys=True),
                )
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_ENGINE_META_FAILED", e, once_key="risk_engine_meta")
        except Exception as e:
            _record_degraded_phase("risk_engine", "PORTFOLIO_RISK_ENGINE_FAILED", e)
            _warn_nonfatal("PORTFOLIO_RISK_ENGINE_FAILED", e, once_key="risk_engine")
            _risk_engine = None

        # ---            -- ------------------------------------------------------
        # Phase 2: PORTFOLIO HARD RISK GATE (net / turnover / dd add-block)
        # ---            -- ------------------------------------------------------
        try:
            desired, _gate = apply_portfolio_risk_gate(con, desired, state, now_ms=int(now_ms))
            try:
                _put_meta(
                    con,
                    "last_risk_gate",
                    json.dumps(_gate or {}, separators=(",", ":"), sort_keys=True),
                )
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_GATE_META_FAILED", e, once_key="risk_gate_meta")
        except Exception as e:
            _record_degraded_phase("risk_gate", "PORTFOLIO_RISK_GATE_FAILED", e)
            _warn_nonfatal("PORTFOLIO_RISK_GATE_FAILED", e, once_key="risk_gate")
            _gate = None

        # ---            -- ------------------------------------------------------
        # Burst control: temporal clustering dampener
        # ---            -- ------------------------------------------------------
        try:
            desired = _apply_temporal_dampener(con, desired, now_ms=int(now_ms))
        except Exception as e:
            _record_degraded_phase("temporal_dampener", "PORTFOLIO_TEMPORAL_DAMPENER_FAILED", e)
            _warn_nonfatal("PORTFOLIO_TEMPORAL_DAMPENER_FAILED", e, once_key="temporal_dampener")

        # ---            -- ------------------------------------------------------
        # Capital-at-Risk gate (tail-risk budget)
        # ---            -- ------------------------------------------------------
        try:
            desired, _car = _apply_capital_at_risk_gate(desired)
            try:
                _put_meta(
                    con,
                    "last_capital_at_risk",
                    json.dumps(_car or {}, separators=(",", ":"), sort_keys=True),
                )
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_CAPITAL_AT_RISK_META_FAILED", e, once_key="capital_at_risk_meta")
        except Exception as e:
            _record_degraded_phase("capital_at_risk", "PORTFOLIO_CAPITAL_AT_RISK_FAILED", e)
            _warn_nonfatal("PORTFOLIO_CAPITAL_AT_RISK_FAILED", e, once_key="capital_at_risk")

        # Prevent crowded same-direction books from surviving the earlier symbol-level sizing layers.
        try:
            desired, _netting = _apply_same_direction_exposure_netting(con, desired)
            try:
                _put_meta(
                    con,
                    "last_exposure_netting",
                    json.dumps(_netting or {}, separators=(",", ":"), sort_keys=True),
                )
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_EXPOSURE_NETTING_META_FAILED", e, once_key="exposure_netting_meta")
        except Exception as e:
            _record_degraded_phase("exposure_netting", "PORTFOLIO_EXPOSURE_NETTING_FAILED", e)
            _warn_nonfatal("PORTFOLIO_EXPOSURE_NETTING_FAILED", e, once_key="exposure_netting")
            _netting = None

        # Final hard portfolio-risk clamp after all other portfolio transforms.
        try:
            desired, _total_risk = _apply_total_portfolio_risk_limit(con, desired)
            try:
                _put_meta(
                    con,
                    "last_total_portfolio_risk",
                    json.dumps(_total_risk or {}, separators=(",", ":"), sort_keys=True),
                )
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_TOTAL_RISK_META_FAILED", e, once_key="total_risk_meta")
        except Exception as e:
            _record_degraded_phase("total_risk", "PORTFOLIO_TOTAL_RISK_FAILED", e)
            _warn_nonfatal("PORTFOLIO_TOTAL_RISK_FAILED", e, once_key="total_risk")
            _total_risk = None

        _flip_penalty = {}
        try:
            desired, _flip_penalty = _apply_flip_flop_penalty(con, desired, state)
        except Exception as e:
            _record_degraded_phase("flip_flop_penalty", "PORTFOLIO_FLIP_FLOP_PENALTY_FAILED", e)
            _warn_nonfatal("PORTFOLIO_FLIP_FLOP_PENALTY_FAILED", e, once_key="flip_flop_penalty")
            _flip_penalty = {}

        portfolio_diag = {}
        try:
            portfolio_diag = _build_portfolio_correlation_diagnostics(
                con,
                desired,
                lookback=int(PORTFOLIO_CORR_LOOKBACK),
                top_n=int(PORTFOLIO_DIAG_CORR_TOP_N),
            )
            _persist_portfolio_correlation_diagnostics(con, portfolio_diag, ts_ms=int(now_ms))
        except Exception as e:
            _record_degraded_phase("correlation_diagnostics", "PORTFOLIO_CORRELATION_DIAGNOSTICS_FAILED", e)
            portfolio_diag = {}

        execution_blocked_codes = _portfolio_execution_blocked_codes(degraded_reasons)
        execution_blocked = bool(execution_blocked_codes)
        if execution_blocked:
            _expire_stale_unconsumed_alerts(con, int(now_ms), int(PORTFOLIO_LOOKBACK_S))
            _set_meta(con, "last_strategy_name", "multi_strategy")
            _set_meta(con, "last_rebalance_ts_ms", str(now_ms))
            _persist_portfolio_runtime_health(
                con,
                now_ms=int(now_ms),
                degraded_reasons=list(degraded_reasons),
                orders_n=0,
                changed_symbols=[],
                execution_blocked=True,
                execution_blocked_codes=list(execution_blocked_codes),
            )
            con.commit()
            return {
                "ok": True,
                "strategy": "multi_strategy",
                "changed": [],
                "orders_n": 0,
                "selected": [str((v or {}).get("symbol") or k) for k, v in desired.items()],
                "execution_blocked": True,
                "execution_blocked_codes": list(execution_blocked_codes),
                "portfolio_diagnostics": {
                    "degraded": bool(degraded_reasons),
                    "degraded_reasons": list(degraded_reasons),
                    "execution_blocked": True,
                    "execution_blocked_codes": list(execution_blocked_codes),
                    "position_summary": dict((portfolio_diag or {}).get("position_summary") or {}),
                    "model_summary": dict((portfolio_diag or {}).get("model_summary") or {}),
                    "flip_flop_penalty": dict(_flip_penalty or {}),
                },
            }

        _mark_alerts_consumed(con, _selected_alert_ids_from_desired(desired), int(now_ms))
        _expire_stale_unconsumed_alerts(con, int(now_ms), int(PORTFOLIO_LOOKBACK_S))

        orders_n = 0
        changed = []
# =========================
# SECTION 9 / ~200 lines
# =========================

        # 1) handle symbols in desired set
        for state_key, tgt in desired.items():
            sym = str(tgt.get("symbol") or "").strip()
            model_id = _normalize_model_id(tgt.get("model_id"))
            cur = state.get(state_key)
            to_side = str(tgt.get("side", "FLAT")).upper()
            raw_to_w = float(tgt.get("weight", 0.0) or 0.0)
            to_w = abs(float(raw_to_w)) if to_side in ("LONG", "SHORT") else 0.0
            source_alert_id = tgt.get("source_alert_id")
            prediction_id = tgt.get("prediction_id")

            explain = {
                "strategy": {
                    "name": "multi_strategy",
                    "min_conf": PORTFOLIO_MIN_CONF,
                    "min_abs_z": PORTFOLIO_MIN_ABS_Z,
                    "max_positions": PORTFOLIO_MAX_POSITIONS,
                    "gross_cap": PORTFOLIO_GROSS_CAP,
                    "max_w_per_symbol": PORTFOLIO_MAX_W_PER_SYMBOL,
                    "score_norm": PORTFOLIO_SCORE_NORM,
                    "min_hold_s": PORTFOLIO_MIN_HOLD_S,
                    "lookback_s": PORTFOLIO_LOOKBACK_S,
                },
                    "selector": {
                        "mode": "live_registry_only",
                        "strategy_name": "multi_strategy",
                    },
                    "signal": tgt.get("reason") or {},
                    "tradability": _tradability_from_explain(tgt.get("explain_json", "{}")),
                    "model_id": model_id,
                }
            model_identity = _extract_model_identity_from_explain(tgt.get("explain_json", "{}"))
            if str(tgt.get("model_name") or model_identity.get("model_name") or "").strip():
                explain["model_name"] = str(tgt.get("model_name") or model_identity.get("model_name") or "").strip()
            if str(tgt.get("regime") or model_identity.get("regime") or "").strip():
                explain["regime"] = str(tgt.get("regime") or model_identity.get("regime") or "").strip()
            if int(tgt.get("horizon_s") or 0) > 0:
                explain["horizon_s"] = int(tgt.get("horizon_s") or 0)
            if model_identity.get("model_kind"):
                explain["model_kind"] = str(model_identity.get("model_kind"))
            if model_identity.get("model_ts_ms") is not None:
                explain["model_ts_ms"] = int(model_identity.get("model_ts_ms"))
            if model_identity.get("model_version"):
                explain["model_version"] = str(model_identity.get("model_version"))

            # Empty/flat targets should not create synthetic OPEN rows.
            if not cur and (to_side == "FLAT" or to_w <= 0.0):
                continue

            if not cur:
                # open new
                _write_state_row(con, model_id, sym, to_side, abs(float(to_w)), now_ms, now_ms, source_alert_id, tgt.get("explain_json", "{}"))
                _emit_order(con, sym, "OPEN", "FLAT", to_side, 0.0, abs(float(to_w)), source_alert_id, prediction_id, explain)
                orders_n += 1
                changed.append(sym)
                continue

            from_side = str(cur["side"])
            from_w = abs(float(cur.get("weight", 0.0) or 0.0))
            opened_ts = int(cur["opened_ts_ms"])
            age_s = max(0.0, (now_ms - opened_ts) / 1000.0)

            # reversal hold guard
            if from_side in ("LONG", "SHORT") and to_side != from_side and age_s < float(PORTFOLIO_MIN_HOLD_S):
                # HOLD (no change)
                explain["hold_reason"] = f"min_hold not met (age_s={age_s:.1f} < {PORTFOLIO_MIN_HOLD_S})"
                _emit_order(con, sym, "HOLD", from_side, from_side, from_w, from_w, source_alert_id, prediction_id, explain)
                orders_n += 1
                continue

            # compute action
            if from_side == "FLAT" and to_side != "FLAT" and to_w > 0:
                _write_state_row(con, model_id, sym, to_side, abs(float(to_w)), now_ms, now_ms, source_alert_id, tgt.get("explain_json", "{}"))
                _emit_order(con, sym, "OPEN", "FLAT", to_side, 0.0, abs(float(to_w)), source_alert_id, prediction_id, explain)
                orders_n += 1
                changed.append(sym)
                continue

            if from_side in ("LONG", "SHORT") and to_side == from_side:
                if abs(to_w - from_w) < 1e-6:
                    _emit_order(con, sym, "HOLD", from_side, from_side, from_w, from_w, source_alert_id, prediction_id, explain)
                    orders_n += 1
                elif to_w > from_w:
                    _write_state_row(con, model_id, sym, from_side, abs(float(to_w)), opened_ts, now_ms, source_alert_id, tgt["explain_json"])
                    _emit_order(con, sym, "INCREASE", from_side, from_side, abs(float(from_w)), abs(float(to_w)), source_alert_id, prediction_id, explain)
                    orders_n += 1
                    changed.append(sym)
                else:
                    _write_state_row(con, model_id, sym, from_side, abs(float(to_w)), opened_ts, now_ms, source_alert_id, tgt["explain_json"])
                    _emit_order(con, sym, "DECREASE", from_side, from_side, abs(float(from_w)), abs(float(to_w)), source_alert_id, prediction_id, explain)
                    orders_n += 1
                    changed.append(sym)
                continue

            if from_side in ("LONG", "SHORT") and to_side != from_side:
                # reverse
                _write_state_row(con, model_id, sym, to_side, abs(float(to_w)), now_ms, now_ms, source_alert_id, tgt.get("explain_json", "{}"))
                _emit_order(con, sym, "REVERSE", from_side, to_side, abs(float(from_w)), abs(float(to_w)), source_alert_id, prediction_id, explain)
                orders_n += 1
                changed.append(sym)
                continue

            # fallback: hold
            _emit_order(con, sym, "HOLD", from_side, from_side, from_w, from_w, source_alert_id, prediction_id, explain)
            orders_n += 1

        # 2) close symbols not desired anymore (but only if currently open)
        for state_key, cur in state.items():
            if state_key in desired:
                continue
            sym = str(cur.get("symbol") or "").strip()
            model_id = _normalize_model_id(cur.get("model_id"))
            from_side = str(cur["side"])
            from_w = abs(float(cur.get("weight", 0.0) or 0.0))
            opened_ts = int(cur["opened_ts_ms"])
            age_s = max(0.0, (now_ms - opened_ts) / 1000.0)

            if from_side in ("LONG", "SHORT") and from_w > 0:
                # optional: min hold before closing too
                if age_s < float(PORTFOLIO_MIN_HOLD_S):
                    explain = {
                        "model_id": model_id,
                        "hold_reason": f"min_hold not met for close (age_s={age_s:.1f} < {PORTFOLIO_MIN_HOLD_S})"
                    }
                    _emit_order(con, sym, "HOLD", from_side, from_side, from_w, from_w, None, None, explain)
                    orders_n += 1
                    continue

                _write_state_row(con, model_id, sym, "FLAT", 0.0, now_ms, now_ms, None, "{}")
                _emit_order(con, sym, "CLOSE", from_side, "FLAT", from_w, 0.0, None, None, {"model_id": model_id, "reason": "no longer selected"})
                orders_n += 1
                changed.append(sym)

        # (size policy applied earlier, before order emission)
        # ---            -- ------------------------------------------------------
        # Update live drawdown meta (equity proxy from weights)
        # ---            -- ------------------------------------------------------
        try:
            # Simple proxy: peak gross vs current gross
            gross_now = sum(abs(float((v or {}).get("weight", 0.0) or 0.0)) for v in desired.values())
            peak_raw = _get_meta(con, "peak_gross_weight")
            peak = float(peak_raw) if peak_raw is not None else gross_now
            if gross_now > peak:
                peak = gross_now
            drawdown = (peak - gross_now) if peak > 1e-9 else 0.0

            _set_meta(con, "peak_gross_weight", str(float(peak)))
            _set_meta(con, "last_drawdown", str(float(drawdown)))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_DRAWDOWN_META_FAILED", e, once_key="drawdown_meta")

        # update live drawdown meta (from broker if available)
        try:
            from engine.execution.broker_sim import broker_snapshot

            snap = broker_snapshot(limit_fills=0)
            if snap and snap.get("ok"):
                dd = snap.get("account", {}).get("drawdown")
                if dd is not None:
                    _set_meta(con, "live_drawdown", str(float(dd)))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_LIVE_DRAWDOWN_SYNC_FAILED", e, once_key="live_drawdown_sync")

        _set_meta(con, "last_strategy_name", "multi_strategy")
        _set_meta(con, "last_rebalance_ts_ms", str(now_ms))
        _persist_portfolio_runtime_health(
            con,
            now_ms=int(now_ms),
            degraded_reasons=list(degraded_reasons),
            orders_n=int(orders_n),
            changed_symbols=list(changed),
            execution_blocked=False,
            execution_blocked_codes=[],
        )
        con.commit()

        return {
            "ok": True,
            "strategy": "multi_strategy",
            "changed": changed,
            "orders_n": int(orders_n),
            "selected": [str((v or {}).get("symbol") or k) for k, v in desired.items()],
            "execution_blocked": False,
            "execution_blocked_codes": [],
            "portfolio_diagnostics": {
                "degraded": bool(degraded_reasons),
                "degraded_reasons": list(degraded_reasons),
                "execution_blocked": False,
                "execution_blocked_codes": [],
                "position_summary": dict((portfolio_diag or {}).get("position_summary") or {}),
                "model_summary": dict((portfolio_diag or {}).get("model_summary") or {}),
                "flip_flop_penalty": dict(_flip_penalty or {}),
            },
        }

    except Exception as e:
        _warn_nonfatal("PORTFOLIO_REBALANCE_FAILED", e, once_key="portfolio_rebalance_failed")
        try:
            con.rollback()
        except Exception as rollback_err:
            _warn_nonfatal("PORTFOLIO_TRANSACTION_ROLLBACK_FAILED", rollback_err, once_key="transaction_rollback")
        return {"ok": False, "error": str(e)}
    finally:
        con.close()
# =========================
# SECTION 10 / remainder
# =========================

def get_portfolio_snapshot(limit_orders: int = 50) -> Dict:
    """Return the current portfolio state, recent rebalance orders, and diagnostics.

    Parameters
    ----------
    limit_orders : int, default=50
        Maximum number of recent orders to include. Values are clamped to the
        inclusive range ``[1, 500]``.

    Returns
    -------
    dict
        Mapping with ``ok``, ``state``, ``orders``, and ``diagnostics``.
        ``state`` rows contain model/symbol holdings with weights expressed as
        unit fractions, and ``orders`` rows contain recent rebalance actions
        with timestamps in epoch milliseconds.

    Side Effects
    ------------
    Ensures portfolio tables exist before reading and serves the result through
    the runtime state cache with a ``0.75`` second TTL.
    """
    from engine.runtime.state_cache import cache_get_or_load

    init_portfolio_db()
    limit_n = int(max(1, min(500, int(limit_orders))))

    def _load() -> Dict:
        con = connect()
        try:
            state = con.execute(
                """
                SELECT model_id, symbol, side, weight, opened_ts_ms, updated_ts_ms, source_alert_id, explain_json
                FROM portfolio_state
                ORDER BY model_id, symbol
                """
            ).fetchall()

            orders = con.execute(
                """
                SELECT ts_ms, model_id, symbol, action, from_side, to_side, from_weight, to_weight, delta_weight, source_alert_id, prediction_id, explain_json
                FROM portfolio_orders
                ORDER BY ts_ms DESC, id DESC
                LIMIT ?
                """,
                (limit_n,),
            ).fetchall()

            out_state = []
            for r in state or []:
                out_state.append(
                    {
                        "model_id": _normalize_model_id(r[0]),
                        "symbol": r[1],
                        "side": r[2],
                        "weight": float(r[3]),
                        "opened_ts_ms": int(r[4]),
                        "updated_ts_ms": int(r[5]),
                        "source_alert_id": (int(r[6]) if r[6] is not None else None),
                        "explain_json": str(r[7] or "{}"),
                    }
                )

            out_orders = []
            for r in orders or []:
                out_orders.append(
                    {
                        "ts_ms": int(r[0]),
                        "model_id": _normalize_model_id(r[1]),
                        "symbol": str(r[2]),
                        "action": str(r[3]),
                        "from_side": str(r[4]),
                        "to_side": str(r[5]),
                        "from_weight": float(r[6]),
                        "to_weight": float(r[7]),
                        "delta_weight": float(r[8]),
                        "source_alert_id": (int(r[9]) if r[9] is not None else None),
                        "prediction_id": (int(r[10]) if r[10] is not None else None),
                        "explain_json": str(r[11] or "{}"),
                    }
                )

            diagnostics = {}
            try:
                diagnostics = _load_latest_portfolio_diagnostics(
                    con,
                    top_n=int(PORTFOLIO_DIAG_CORR_TOP_N),
                )
            except Exception:
                diagnostics = {}

            return {"ok": True, "state": out_state, "orders": out_orders, "diagnostics": diagnostics}
        finally:
            con.close()

    return cache_get_or_load("portfolio_snapshot", f"strategy_portfolio:{limit_n}", _load, ttl_s=0.75)
