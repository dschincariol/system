"""
FILE: broker_sim.py

Execution subsystem module for `broker_sim`.
"""

"""
Broker simulator (paper execution).

Consumes portfolio_orders (intents) and writes:
- broker_account (cash/equity baseline)
- broker_positions (qty, avg_px)
- broker_fills (fills history)
- broker_meta (cursor for last applied portfolio_orders id)

Assumptions:
- Uses latest price <= fill_ts for each symbol (from prices table).
- Converts target weights into target qty: qty = target_weight * equity / px
- SHORT => negative qty

Broker realism knobs:
- Spread + slippage in execution price
- Fees (bps of notional)
- Chunking + per-chunk latency
- Max trade notional cap per apply pass (% of equity)
"""

import json
import os
import time
import math
import hashlib
import logging
import threading
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
from engine.runtime import dbapi_compat as dbapi
from engine.execution.crypto_costs import (
    fee_bps as crypto_fee_bps,
    funding_carry_bps as crypto_funding_carry_bps,
    is_crypto_asset_class,
    spread_bps as crypto_spread_bps,
)
from engine.execution.fx_costs import is_fx_asset_class, pip_spread_bps, swap_bps, weekend_gap_bps
from engine.runtime.failure_diagnostics import log_failure
from engine.execution.cost_models.almgren_chriss import AlmgrenChrissCost
from engine.runtime.storage import connect, connect_rw_direct, run_write_txn
from engine.execution.almgren_chriss import estimate_almgren_chriss_costs  # noqa: F401 - compatibility patch target covered by tests/test_broker_order_idempotency_regressions.py.
from engine.execution.deployable_capital import compute_deployable_equity
from engine.execution.execution_liquidity_model import get_execution_liquidity_snapshot
from engine.execution.lob_simulation import build_reactive_lob_simulation
from engine.execution.share_rounding import round_equity_qty
from engine.execution.order_idempotency import (
    claim_order_submission,
    mark_order_submission_submitted,
)

# -----------------------------
# Small numeric guards
# -----------------------------
def _is_finite(x) -> bool:
    try:
        return math.isfinite(float(x))
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_is_finite_failed",
            "BROKER_SIM_IS_FINITE_FAILED",
            e,
            warn_key="is_finite_failed",
            value_type=type(x).__name__,
        )
        finite = False
        return finite


def _safe_f(x, default: float = 0.0) -> float:
    if x is None:
        return float(default)
    if isinstance(x, str) and not x.strip():
        return float(default)
    try:
        v = float(x)
        return v if math.isfinite(v) else float(default)
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_safe_float_failed",
            "BROKER_SIM_SAFE_FLOAT_FAILED",
            e,
            warn_key="safe_float_failed",
            value_type=type(x).__name__,
        )
        fallback = float(default)
        return fallback


def _safe_i(x, default: int = 0) -> int:
    if x is None:
        return int(default)
    if isinstance(x, str) and not x.strip():
        return int(default)
    try:
        v = int(x)
        return v
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_safe_int_failed",
            "BROKER_SIM_SAFE_INT_FAILED",
            e,
            warn_key="safe_int_failed",
            value_type=type(x).__name__,
        )
        fallback = int(default)
        return fallback


def _share_rounding_asset_class(symbol: str) -> str:
    try:
        from engine.data.asset_map import asset_class_for_symbol

        return str(asset_class_for_symbol(symbol) or "UNKNOWN").upper().strip() or "UNKNOWN"
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_share_rounding_asset_class_failed",
            "BROKER_SIM_SHARE_ROUNDING_ASSET_CLASS_FAILED",
            e,
            warn_key=f"share_rounding_asset_class:{symbol}",
            symbol=str(symbol),
        )
        return "UNKNOWN"


def _begin_managed_write(con: Any) -> None:
    begin_write = getattr(con, "begin_managed_write", None)
    if callable(begin_write):
        begin_write()


def _prime_broker_order_state_after_commit(
    con: Any,
    *,
    source_order_id: int,
    symbol: str,
    state: str,
    created_ts_ms: int,
    updated_ts_ms: int,
    ttl_ms: int | None,
    meta: Dict[str, Any] | None = None,
) -> None:
    try:
        from engine.cache.wrappers.broker_order_state import prime_broker_order_state

        payload = {
            "source_order_id": int(source_order_id or 0),
            "symbol": str(symbol or "").upper().strip(),
            "state": str(state or ""),
            "created_ts_ms": int(created_ts_ms or 0),
            "updated_ts_ms": int(updated_ts_ms or 0),
            "ttl_ms": (int(ttl_ms) if ttl_ms is not None else None),
            "meta": dict(meta or {}),
        }
        register = getattr(con, "register_after_commit", None)
        if callable(register) and bool(getattr(con, "in_transaction", False)):
            register(lambda: prime_broker_order_state(payload))
        elif not bool(getattr(con, "in_transaction", False)):
            prime_broker_order_state(payload)
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_broker_order_state_cache_prime_failed",
            "BROKER_SIM_BROKER_ORDER_STATE_CACHE_PRIME_FAILED",
            e,
            warn_key=f"broker_order_state_cache_prime:{symbol}:{source_order_id}:{state}",
            symbol=str(symbol),
            source_order_id=int(source_order_id or 0),
            state=str(state or ""),
        )


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        v = float(x)
    except Exception:
        v = float(lo)
    return float(max(float(lo), min(float(hi), v)))


def _u01(seed: str) -> float:
    """
    Deterministic pseudo-random in [0,1) from a stable seed string.
    (No global RNG; reproducible in audits.)
    """
    try:
        h = hashlib.sha256(str(seed).encode("utf-8")).hexdigest()
        # 12 hex chars ~ 48 bits
        n = int(h[:12], 16)
        return (n % 10_000_000) / 10_000_000.0
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_u01_failed",
            "BROKER_SIM_U01_FAILED",
            e,
            warn_key="u01_failed",
        )
        value = 0.0
        return value


# -----------------------------
# Execution ROI conditioning (no sentiment, no LLM)
# -----------------------------

_EARNINGS_HALF_LIFE_DAYS = float(os.environ.get("EARNINGS_HALF_LIFE_DAYS", "5.0"))
_EXEC_SKEW_Z_THRESH = float(os.environ.get("EXEC_SKEW_Z_THRESH", "1.5"))
_EXEC_FLOW_Z_THRESH = float(os.environ.get("EXEC_FLOW_Z_THRESH", "2.0"))
_EXEC_EARNINGS_SIZE_MAX_REDUCTION = float(os.environ.get("EXEC_EARNINGS_SIZE_MAX_REDUCTION", "0.55"))
_EXEC_STRESS_SIZE_MAX_REDUCTION = float(os.environ.get("EXEC_STRESS_SIZE_MAX_REDUCTION", "0.35"))
_EXEC_EARNINGS_SLIP_ADD_BPS = float(os.environ.get("EXEC_EARNINGS_SLIP_ADD_BPS", "0.75"))
_EXEC_STRESS_SLIP_ADD_BPS = float(os.environ.get("EXEC_STRESS_SLIP_ADD_BPS", "0.75"))
_EXEC_STRESS_LATENCY_MULT_MAX = float(os.environ.get("EXEC_STRESS_LATENCY_MULT_MAX", "2.0"))
EXEC_TOTAL_EXPOSURE_CAP = float(
    os.environ.get(
        "EXEC_PORTFOLIO_TOTAL_EXPOSURE_CAP",
        os.environ.get("PORTFOLIO_RISK_MAX_GROSS", os.environ.get("PORTFOLIO_GROSS_CAP", "1.00")),
    )
)

LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: Optional[str] = None, **extra: Any) -> None:
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


def _is_closed_connection_error(error: BaseException) -> bool:
    text = str(error or "").strip().lower()
    return any(
        marker in text
        for marker in (
            "cannot operate on a closed database",
            "closed database",
            "connection already closed",
            "connection is closed",
            "connection closed",
            "closed connection",
        )
    )


def _connection_marked_closed(con: Any) -> bool:
    if con is None:
        return True
    missing = object()
    for attr in ("_closed", "closed"):
        try:
            value = getattr(con, attr, missing)
        except Exception as e:
            _warn_nonfatal(
                "broker_sim_connection_state_attr_failed",
                "BROKER_SIM_CONNECTION_STATE_ATTR_FAILED",
                e,
                warn_key=f"connection_state_attr:{attr}",
                attr=str(attr),
            )
            continue
        if value is missing:
            continue
        if isinstance(value, bool):
            return bool(value)
        if value is not None and not callable(value):
            try:
                return bool(value)
            except Exception as e:
                _warn_nonfatal(
                    "broker_sim_connection_state_coerce_failed",
                    "BROKER_SIM_CONNECTION_STATE_COERCE_FAILED",
                    e,
                    warn_key=f"connection_state_coerce:{attr}",
                    attr=str(attr),
                )
                continue
    try:
        raw = getattr(con, "raw", None)
    except Exception:
        raw = None
    if raw is not None:
        for attr in ("closed", "_closed"):
            try:
                value = getattr(raw, attr, missing)
                if value is not missing and bool(value):
                    return True
            except Exception as e:
                _warn_nonfatal(
                    "broker_sim_raw_connection_state_failed",
                    "BROKER_SIM_RAW_CONNECTION_STATE_FAILED",
                    e,
                    warn_key=f"raw_connection_state:{attr}",
                    attr=str(attr),
                )
                continue
    return False


def _safe_commit_connection(con: Any, *, context: str, once_key: str) -> bool:
    connection = con
    if _connection_marked_closed(connection):
        _warn_nonfatal(
            "broker_sim_commit_skipped_closed_connection",
            "BROKER_SIM_COMMIT_SKIPPED_CLOSED_CONNECTION",
            RuntimeError("connection already closed before commit"),
            warn_key=f"{once_key}:closed",
            context=str(context),
        )
        return False
    try:
        connection.commit()
        return True
    except Exception as e:
        if _is_closed_connection_error(e):
            _warn_nonfatal(
                "broker_sim_commit_skipped_closed_connection",
                "BROKER_SIM_COMMIT_SKIPPED_CLOSED_CONNECTION",
                RuntimeError("connection already closed before commit"),
                warn_key=f"{once_key}:closed",
                context=str(context),
            )
            return False
        _warn_nonfatal(
            "broker_sim_commit_failed",
            "BROKER_SIM_COMMIT_FAILED",
            e,
            warn_key=once_key,
            context=str(context),
        )
        return False


def _safe_close_connection(con: Any, *, context: str, once_key: str) -> bool:
    connection = con
    if connection is None:
        return False
    if _connection_marked_closed(connection):
        _warn_nonfatal(
            "broker_sim_connection_close_skipped_closed",
            "BROKER_SIM_CONNECTION_CLOSE_SKIPPED_CLOSED",
            RuntimeError("connection already closed before close"),
            warn_key=f"{once_key}:closed",
            context=str(context),
        )
        return False
    try:
        connection.close()
        return True
    except Exception as e:
        if _is_closed_connection_error(e):
            _warn_nonfatal(
                "broker_sim_connection_close_skipped_closed",
                "BROKER_SIM_CONNECTION_CLOSE_SKIPPED_CLOSED",
                RuntimeError("connection already closed before close"),
                warn_key=f"{once_key}:closed",
                context=str(context),
            )
            return False
        _warn_nonfatal(
            "broker_sim_connection_close_failed",
            "BROKER_SIM_CONNECTION_CLOSE_FAILED",
            e,
            warn_key=once_key,
            context=str(context),
        )
        return False

EXEC_SYMBOL_CONCENTRATION_CAP = float(
    os.environ.get(
        "EXEC_PORTFOLIO_SYMBOL_CONCENTRATION_CAP",
        os.environ.get("PORTFOLIO_RISK_MAX_SYMBOL_GROSS", os.environ.get("KILL_SWITCH_CONCENTRATION_MAX_SINGLE", "0.35")),
    )
)
EXEC_DIRECTION_CONCENTRATION_CAP = float(
    os.environ.get(
        "EXEC_PORTFOLIO_DIRECTION_CONCENTRATION_CAP",
        os.environ.get("PORTFOLIO_RISK_MAX_NET", "0.60"),
    )
)


def _clamp01(x: float) -> float:
    return _clamp(float(x), 0.0, 1.0)


def _get_factor_feature_asof(con, feature_id: str, ts_ms: int) -> float:
    try:
        row = con.execute(
            """
            SELECT value
            FROM factor_features
            WHERE feature_id=?
              AND asof_ts <= ?
              AND effective_ts <= ?
            ORDER BY asof_ts DESC, effective_ts DESC
            LIMIT 1
            """,
            (str(feature_id), int(ts_ms), int(ts_ms)),
        ).fetchone()
        if not row:
            return 0.0
        return _safe_f(row[0], 0.0)
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_factor_feature_asof_failed",
            "BROKER_SIM_FACTOR_FEATURE_ASOF_FAILED",
            e,
            warn_key=f"factor_feature_asof:{feature_id}",
            feature_id=str(feature_id),
        )
        value = 0.0
        return value


def _ymd_from_ts_ms(ts_ms: int) -> str:
    try:
        dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_ymd_from_ts_failed",
            "BROKER_SIM_YMD_FROM_TS_FAILED",
            e,
            warn_key="ymd_from_ts_failed",
            ts_ms=int(ts_ms or 0),
        )
        fallback = time.strftime("%Y-%m-%d", time.gmtime(int(ts_ms) / 1000.0))
        return fallback


def _earnings_proximity_decay(con, symbol: str, ts_ms: int) -> float:
    """
    Returns [0,1]. 1.0 = very near earnings date, 0.0 = far.
    Uses nearest earnings_calendar row by date distance.
    This is only a realism conditioning input for simulated execution.
    """
    sym = str(symbol or "").strip().upper()
    if not sym:
        return 0.0

    try:
        if not _table_exists(con, "earnings_calendar"):
            return 0.0
        today = _ymd_from_ts_ms(int(ts_ms))
        if dbapi.is_sqlite_connection(con):
            row = con.execute(
                """
                SELECT earnings_date
                FROM earnings_calendar
                WHERE symbol=?
                ORDER BY ABS(julianday(earnings_date) - julianday(?)) ASC
                LIMIT 1
                """,
                (sym, str(today)),
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT earnings_date
                FROM earnings_calendar
                WHERE symbol=?
                ORDER BY ABS(earnings_date::date - ?::date) ASC
                LIMIT 1
                """,
                (sym, str(today)),
            ).fetchone()
        if not row:
            return 0.0

        ed = str(row[0] or "").strip()
        if not ed:
            return 0.0

        if dbapi.is_sqlite_connection(con):
            jd = con.execute(
                "SELECT julianday(?) - julianday(?)",
                (str(ed), str(today)),
            ).fetchone()
        else:
            jd = con.execute(
                "SELECT (?::date - ?::date)",
                (str(ed), str(today)),
            ).fetchone()
        if not jd or jd[0] is None:
            return 0.0

        days = float(jd[0])
        hl = max(0.5, float(_EARNINGS_HALF_LIFE_DAYS))
        return _clamp01(math.exp(-abs(days) / hl))
    except dbapi.OperationalError as e:
        if "no such table" in str(e).lower() and "earnings_calendar" in str(e).lower():
            if f"earnings_calendar_missing:{sym}" not in _WARNED_NONFATAL_KEYS:
                _WARNED_NONFATAL_KEYS.add(f"earnings_calendar_missing:{sym}")
                log_failure(
                    LOGGER,
                    event="broker_sim_earnings_calendar_missing",
                    code="BROKER_SIM_EARNINGS_CALENDAR_MISSING",
                    message="broker_sim_earnings_calendar_missing",
                    error=e,
                    level=logging.WARNING,
                    component=__name__,
                    extra={"symbol": str(sym)},
                    persist=False,
                )
            return 0.0
        _warn_nonfatal(
            "broker_sim_earnings_proximity_decay_failed",
            "BROKER_SIM_EARNINGS_PROXIMITY_DECAY_FAILED",
            e,
            warn_key=f"earnings_proximity_decay:{sym}",
            symbol=str(sym),
        )
        decay = 0.0
        return decay
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_earnings_proximity_decay_failed",
            "BROKER_SIM_EARNINGS_PROXIMITY_DECAY_FAILED",
            e,
            warn_key=f"earnings_proximity_decay:{sym}",
            symbol=str(sym),
        )
        decay = 0.0
        return decay


# -----------------------------
# Broker realism knobs (env)
# These are intentionally operational knobs so paper execution realism can be
# tuned without changing broker_sim logic.
# -----------------------------
BROKER_SPREAD_BPS = float(os.environ.get("BROKER_SPREAD_BPS", "2.0"))  # total spread (bps)
BROKER_SLIPPAGE_BPS = float(os.environ.get("BROKER_SLIPPAGE_BPS", "1.0"))  # extra slippage (bps)
BROKER_FEE_BPS = float(os.environ.get("BROKER_FEE_BPS", "0.5"))  # commission/fees (bps of notional)
BROKER_MAX_TRADE_PCT_EQUITY = float(os.environ.get("BROKER_MAX_TRADE_PCT_EQUITY", "0.35"))  # cap per apply pass
BROKER_CHUNK_PCT = float(os.environ.get("BROKER_CHUNK_PCT", "0.33"))  # split into chunks
BROKER_LATENCY_MS = int(os.environ.get("BROKER_LATENCY_MS", "120"))  # per chunk latency
BROKER_OPTION_MAX_QUOTE_AGE_MS = int(os.environ.get("BROKER_OPTION_MAX_QUOTE_AGE_MS", os.environ.get("BROKER_MAX_PRICE_AGE_MS", "300000")))
OPTIONS_SIM_MARGIN_UNDERLYING_FRACTION = float(os.environ.get("OPTIONS_SIM_MARGIN_UNDERLYING_FRACTION", "0.20"))

# Starting capital / cash baseline (additive; preserves existing behavior if not set)
BROKER_START_CASH = float(os.environ.get("BROKER_START_CASH", "0.0"))
BROKER_START_EQUITY = float(os.environ.get("BROKER_START_EQUITY", "0.0"))  # optional override; usually = cash

# If 0: do not allow cash to go negative (no margin). If 1: allow margin/short proceeds to fund buys.
BROKER_ALLOW_MARGIN = os.environ.get("BROKER_ALLOW_MARGIN", "1") == "1"

# Size-based slippage (impact proxy). 0 disables (keeps constant slippage).
# Applied as: slip_bps = base_slip_bps * (1 + impact_alpha * (notional / equity))
BROKER_IMPACT_ALPHA = float(os.environ.get("BROKER_IMPACT_ALPHA", "1.5"))

# Optional wall-clock latency simulation (default off to preserve throughput)
BROKER_LATENCY_SLEEP = os.environ.get("BROKER_LATENCY_SLEEP", "0") == "1"

SCHEMA = """
CREATE TABLE IF NOT EXISTS broker_account (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  cash REAL NOT NULL,
  equity REAL NOT NULL,
  updated_ts_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS broker_positions (
  symbol TEXT PRIMARY KEY,
  qty REAL NOT NULL,
  avg_px REAL NOT NULL,
  updated_ts_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS broker_fills (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  qty REAL NOT NULL,
  px REAL NOT NULL,
  source_order_id INTEGER,
  source TEXT NOT NULL DEFAULT 'sim',
  book_key TEXT,
  contract_multiplier REAL,
  option_quote_source TEXT,
  option_margin_debit REAL,
  note TEXT,
  explain_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_broker_fills_ts ON broker_fills(ts_ms);

CREATE TABLE IF NOT EXISTS broker_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS broker_order_state (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_order_id INTEGER,
  symbol TEXT NOT NULL,
  state TEXT NOT NULL,
  created_ts_ms INTEGER NOT NULL,
  updated_ts_ms INTEGER NOT NULL,
  ttl_ms INTEGER,
  meta_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_broker_order_state_symbol ON broker_order_state(symbol);

CREATE TABLE IF NOT EXISTS broker_shadow_account (
  book_key TEXT PRIMARY KEY,
  cash REAL NOT NULL,
  equity REAL NOT NULL,
  updated_ts_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS broker_shadow_positions (
  book_key TEXT NOT NULL,
  symbol TEXT NOT NULL,
  qty REAL NOT NULL,
  avg_px REAL NOT NULL,
  updated_ts_ms INTEGER NOT NULL,
  PRIMARY KEY (book_key, symbol)
);

CREATE TABLE IF NOT EXISTS broker_shadow_meta (
  book_key TEXT NOT NULL,
  key TEXT NOT NULL,
  value TEXT NOT NULL,
  PRIMARY KEY (book_key, key)
);

CREATE TABLE IF NOT EXISTS broker_shadow_order_state (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  book_key TEXT NOT NULL,
  source_order_id INTEGER,
  symbol TEXT NOT NULL,
  state TEXT NOT NULL,
  created_ts_ms INTEGER NOT NULL,
  updated_ts_ms INTEGER NOT NULL,
  ttl_ms INTEGER,
  meta_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_broker_shadow_positions_book ON broker_shadow_positions(book_key, symbol);
CREATE INDEX IF NOT EXISTS idx_broker_shadow_order_state_book ON broker_shadow_order_state(book_key, symbol);
"""
_BROKER_SCHEMA_TABLES = (
    "broker_account",
    "broker_positions",
    "broker_fills",
    "broker_meta",
    "broker_order_state",
    "broker_shadow_account",
    "broker_shadow_positions",
    "broker_shadow_meta",
    "broker_shadow_order_state",
)
_BROKER_SCHEMA_INDEXES = (
    "idx_broker_fills_ts",
    "idx_broker_fills_source_book_ts",
    "idx_broker_order_state_symbol",
    "idx_broker_shadow_positions_book",
    "idx_broker_shadow_order_state_book",
)
_BROKER_FILLS_DEDUP_INDEX = "uq_broker_fills_src"
_BROKER_FILLS_DEDUP_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_broker_fills_src
  ON broker_fills(source, COALESCE(book_key, ''), symbol, ts_ms, source_order_id)
  WHERE source_order_id IS NOT NULL
"""
_BROKER_DB_INIT_LOCK = threading.Lock()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _book_key(book_key: Optional[str]) -> Optional[str]:
    bk = str(book_key or "").strip()
    return bk or None


def _is_shadow_book(book_key: Optional[str]) -> bool:
    return _book_key(book_key) is not None


def _table_exists(con, table_name: str) -> bool:
    if dbapi.is_sqlite_connection(con):
        row = con.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type='table' AND name=?
            LIMIT 1
            """,
            (str(table_name),),
        ).fetchone()
        return bool(row)
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


def _index_exists(con, index_name: str) -> bool:
    if dbapi.is_sqlite_connection(con):
        row = con.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type='index' AND name=?
            LIMIT 1
            """,
            (str(index_name),),
        ).fetchone()
        return bool(row)
    row = con.execute(
        """
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = ANY (current_schemas(false))
          AND indexname=?
        LIMIT 1
        """,
        (str(index_name),),
    ).fetchone()
    return bool(row)


def _broker_account_columns(con) -> set[str]:
    try:
        return {
            str(row[1] or "").strip()
            for row in (con.execute("PRAGMA table_info(broker_account)").fetchall() or [])
        }
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_broker_account_columns_failed",
            "BROKER_SIM_BROKER_ACCOUNT_COLUMNS_FAILED",
            e,
            warn_key="broker_account_columns_failed",
        )
        columns: set[str] = set()
        return columns


def _broker_account_uses_singleton_id(con) -> bool:
    return "id" in _broker_account_columns(con)


def _broker_positions_columns(con) -> set[str]:
    try:
        return {
            str(row[1] or "").strip()
            for row in (con.execute("PRAGMA table_info(broker_positions)").fetchall() or [])
        }
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_broker_positions_columns_failed",
            "BROKER_SIM_BROKER_POSITIONS_COLUMNS_FAILED",
            e,
            warn_key="broker_positions_columns_failed",
        )
        columns: set[str] = set()
        return columns


def _broker_positions_use_timeseries(con) -> bool:
    return "ts_ms" in _broker_positions_columns(con)


def _broker_fills_columns(con) -> set[str]:
    try:
        return {
            str(row[1] or "").strip()
            for row in (con.execute("PRAGMA table_info(broker_fills)").fetchall() or [])
        }
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_broker_fills_columns_failed",
            "BROKER_SIM_BROKER_FILLS_COLUMNS_FAILED",
            e,
            warn_key="broker_fills_columns_failed",
        )
        return set()


def _ensure_broker_fill_provenance_columns(con) -> None:
    columns = _broker_fills_columns(con)
    if "source" not in columns:
        con.execute("ALTER TABLE broker_fills ADD COLUMN source TEXT NOT NULL DEFAULT 'sim'")
    if "book_key" not in columns:
        con.execute("ALTER TABLE broker_fills ADD COLUMN book_key TEXT")
    if "contract_multiplier" not in columns:
        con.execute("ALTER TABLE broker_fills ADD COLUMN contract_multiplier REAL")
    if "option_quote_source" not in columns:
        con.execute("ALTER TABLE broker_fills ADD COLUMN option_quote_source TEXT")
    if "option_margin_debit" not in columns:
        con.execute("ALTER TABLE broker_fills ADD COLUMN option_margin_debit REAL")
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_broker_fills_source_book_ts
          ON broker_fills(source, book_key, symbol, ts_ms)
        """
    )


def _broker_fill_dedup_conflict_row(con):
    try:
        return con.execute(
            """
            SELECT source, COALESCE(book_key, ''), symbol, ts_ms, source_order_id, COUNT(*) AS duplicate_count
            FROM broker_fills
            WHERE source_order_id IS NOT NULL
            GROUP BY source, COALESCE(book_key, ''), symbol, ts_ms, source_order_id
            HAVING COUNT(*) > 1
            LIMIT 1
            """
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_fill_dedup_conflict_probe_failed",
            "BROKER_SIM_FILL_DEDUP_CONFLICT_PROBE_FAILED",
            e,
            warn_key="broker_fill_dedup_conflict_probe_failed",
        )
        return None


def _warn_broker_fill_dedup_index_skipped(row) -> None:
    duplicate_count = None
    try:
        duplicate_count = int(row[5]) if row and row[5] is not None else None
    except Exception:
        duplicate_count = None
    _warn_nonfatal(
        "broker_sim_fill_dedup_index_skipped_existing_duplicates",
        "BROKER_SIM_FILL_DEDUP_INDEX_SKIPPED_EXISTING_DUPLICATES",
        RuntimeError("broker_fills contains duplicate sourced fill identities"),
        warn_key="broker_fill_dedup_index_skipped_existing_duplicates",
        source=(str(row[0]) if row and row[0] is not None else None),
        book_key=(str(row[1]) if row and row[1] is not None else None),
        symbol=(str(row[2]) if row and row[2] is not None else None),
        ts_ms=(int(row[3]) if row and row[3] is not None else None),
        source_order_id=(int(row[4]) if row and row[4] is not None else None),
        duplicate_count=duplicate_count,
    )


def _broker_fill_dedup_index_ready(con) -> bool:
    if _index_exists(con, _BROKER_FILLS_DEDUP_INDEX):
        return True
    if not _table_exists(con, "broker_fills"):
        return False
    row = _broker_fill_dedup_conflict_row(con)
    if row:
        _warn_broker_fill_dedup_index_skipped(row)
        return True
    return False


def _ensure_broker_fill_dedup_index(con) -> None:
    if _index_exists(con, _BROKER_FILLS_DEDUP_INDEX):
        return
    row = _broker_fill_dedup_conflict_row(con)
    if row:
        _warn_broker_fill_dedup_index_skipped(row)
        return
    try:
        con.execute(_BROKER_FILLS_DEDUP_INDEX_SQL)
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_fill_dedup_index_create_failed",
            "BROKER_SIM_FILL_DEDUP_INDEX_CREATE_FAILED",
            e,
            warn_key="broker_fill_dedup_index_create_failed",
        )


def _account_snapshot_value_missing(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def _coerce_snapshot_float(value: Any) -> Optional[float]:
    if _account_snapshot_value_missing(value):
        return None
    try:
        out = float(value)
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_account_snapshot_float_parse_failed",
            "BROKER_SIM_ACCOUNT_SNAPSHOT_FLOAT_PARSE_FAILED",
            e,
            warn_key=f"account_snapshot_float_parse:{type(value).__name__}",
            value_type=type(value).__name__,
        )
        return None
    return float(out) if math.isfinite(out) else None


def _coerce_snapshot_int(value: Any) -> Optional[int]:
    if _account_snapshot_value_missing(value):
        return None
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_account_snapshot_int_parse_failed",
            "BROKER_SIM_ACCOUNT_SNAPSHOT_INT_PARSE_FAILED",
            e,
            warn_key=f"account_snapshot_int_parse:{type(value).__name__}",
            value_type=type(value).__name__,
        )
        return None


def _account_snapshot_needs_repair(
    cash_raw: Any,
    equity_raw: Any,
    updated_ts_raw: Any,
    snapshot: Dict[str, Any],
) -> bool:
    cash = _coerce_snapshot_float(cash_raw)
    equity = _coerce_snapshot_float(equity_raw)
    updated_ts_ms = _coerce_snapshot_int(updated_ts_raw)
    if cash is None or equity is None or updated_ts_ms is None:
        return True
    try:
        if abs(float(cash) - float(snapshot.get("cash") or 0.0)) > 1e-9:
            return True
        if abs(float(equity) - float(snapshot.get("equity") or 0.0)) > 1e-9:
            return True
        if int(updated_ts_ms) != int(snapshot.get("updated_ts_ms") or 0):
            return True
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_account_snapshot_compare_failed",
            "BROKER_SIM_ACCOUNT_SNAPSHOT_COMPARE_FAILED",
            e,
            warn_key="account_snapshot_compare_failed",
        )
        return True
    return False


def _repair_account_snapshot_if_needed(
    con,
    snapshot: Dict[str, Any],
    *,
    cash_raw: Any,
    equity_raw: Any,
    updated_ts_raw: Any,
    scope: str,
    book_key: Optional[str] = None,
) -> None:
    if not _account_snapshot_needs_repair(cash_raw, equity_raw, updated_ts_raw, snapshot):
        return
    try:
        _write_account(
            con,
            cash=float(snapshot.get("cash") or 0.0),
            equity=float(snapshot.get("equity") or 0.0),
            ts_ms=int(snapshot.get("updated_ts_ms") or _now_ms()),
            book_key=book_key,
        )
    except Exception as e:
        error: BaseException
        if _is_closed_connection_error(e):
            error = RuntimeError("connection already closed before account snapshot repair")
        else:
            error = e
        _warn_nonfatal(
            "broker_sim_account_snapshot_repair_failed",
            "BROKER_SIM_ACCOUNT_SNAPSHOT_REPAIR_FAILED",
            error,
            warn_key=f"account_snapshot_repair_failed:{scope}",
            scope=str(scope),
            book_key=str(_book_key(book_key) or ""),
        )


def _default_account_snapshot() -> Dict[str, Any]:
    cash = float(BROKER_START_CASH or 0.0)
    equity = float(BROKER_START_EQUITY or 0.0) if float(BROKER_START_EQUITY or 0.0) > 0.0 else float(cash)
    if cash == 0.0 and equity == 0.0:
        equity = 1.0
    return {"cash": float(cash), "equity": float(equity), "updated_ts_ms": 0}


def _normalize_account_snapshot(
    cash_raw: Any,
    equity_raw: Any,
    updated_ts_raw: Any,
    *,
    scope: str,
) -> Dict[str, Any]:
    invalid_snapshot = any(_account_snapshot_value_missing(value) for value in (cash_raw, equity_raw, updated_ts_raw))
    cash = _safe_f(cash_raw, float(BROKER_START_CASH or 0.0))
    equity_default = (
        float(BROKER_START_EQUITY or 0.0)
        if float(BROKER_START_EQUITY or 0.0) > 0.0
        else float(cash)
    )
    equity = _safe_f(equity_raw, equity_default)
    updated_ts_default = _now_ms() if _account_snapshot_value_missing(updated_ts_raw) else 0
    updated_ts_ms = _safe_i(updated_ts_raw, int(updated_ts_default))
    if invalid_snapshot and int(updated_ts_ms or 0) <= 0:
        updated_ts_ms = _now_ms()

    if cash == 0.0 and equity == 0.0:
        if float(BROKER_START_EQUITY or 0.0) > 0.0:
            equity = float(BROKER_START_EQUITY or 0.0)
        elif float(BROKER_START_CASH or 0.0) > 0.0:
            cash = float(BROKER_START_CASH or 0.0)
            equity = float(cash)
        else:
            equity = 1.0
    elif equity == 0.0 and cash > 0.0:
        equity = float(cash)

    if invalid_snapshot:
        _warn_nonfatal(
            "broker_sim_account_snapshot_invalid",
            "BROKER_SIM_ACCOUNT_SNAPSHOT_INVALID",
            RuntimeError("broker account snapshot contained null or blank values"),
            warn_key=f"account_snapshot_invalid:{scope}",
            scope=str(scope),
            cash_raw=repr(cash_raw),
            equity_raw=repr(equity_raw),
            updated_ts_raw=repr(updated_ts_raw),
        )

    return {
        "cash": float(cash),
        "equity": float(equity),
        "updated_ts_ms": int(updated_ts_ms),
    }


def _broker_account_seeded(con) -> bool:
    # The simulator owns its own broker_* tables so paper execution can run
    # independently of any live broker integration.
    if _broker_account_uses_singleton_id(con):
        row = con.execute("SELECT cash, equity FROM broker_account WHERE id=1").fetchone()
    else:
        row = con.execute(
            """
            SELECT cash, equity
            FROM broker_account
            ORDER BY COALESCE(updated_ts_ms, ts_ms, 0) DESC, ts_ms DESC
            LIMIT 1
            """
        ).fetchone()
    return bool(row)


def _broker_schema_ready(con) -> bool:
    fill_columns = _broker_fills_columns(con)
    tables_ready = all(_table_exists(con, table_name) for table_name in _BROKER_SCHEMA_TABLES)
    return (
        tables_ready
        and all(_index_exists(con, index_name) for index_name in _BROKER_SCHEMA_INDEXES)
        and {
            "source",
            "book_key",
            "contract_multiplier",
            "option_quote_source",
            "option_margin_debit",
        }.issubset(fill_columns)
        and _broker_fill_dedup_index_ready(con)
        and _broker_account_seeded(con)
    )


def _ensure_tables(con):
    con.executescript(SCHEMA)
    _ensure_broker_fill_provenance_columns(con)
    _ensure_broker_fill_dedup_index(con)
    if not _broker_account_seeded(con):
        ts = _now_ms()
        # Preserve legacy defaults unless user explicitly sets env
        cash0 = float(BROKER_START_CASH or 0.0)

        # If BROKER_START_EQUITY not set, default equity to cash0 (mark-to-market will update later)
        eq0 = float(BROKER_START_EQUITY) if float(BROKER_START_EQUITY or 0.0) > 0.0 else float(cash0)

        # Legacy behavior was (cash=0, equity=1). Keep that only when both are unset/zero.
        if cash0 == 0.0 and eq0 == 0.0:
            cash0 = 0.0
            eq0 = 1.0

        if _broker_account_uses_singleton_id(con):
            con.execute(
                "INSERT INTO broker_account(id, cash, equity, updated_ts_ms) VALUES(1, ?, ?, ?)",
                (float(cash0), float(eq0), int(ts)),
            )
        else:
            con.execute(
                """
                INSERT INTO broker_account(
                    ts_ms, updated_ts_ms, broker, account_id, equity, cash,
                    buying_power, maintenance_margin, day_pnl, unrealized_pnl,
                    realized_pnl, currency, extra_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(ts),
                    int(ts),
                    "sim",
                    "paper",
                    float(eq0),
                    float(cash0),
                    float(cash0),
                    None,
                    None,
                    None,
                    None,
                    "USD",
                    None,
                ),
            )


def _seed_shadow_account(con, book_key: str) -> None:
    row = con.execute(
        "SELECT cash, equity FROM broker_shadow_account WHERE book_key=?",
        (str(book_key),),
    ).fetchone()
    if row:
        return
    base = _read_account(con)
    cash0 = float(base.get("cash") or 0.0)
    eq0 = float(base.get("equity") or 0.0)
    if cash0 == 0.0 and eq0 == 0.0:
        cash0 = float(BROKER_START_CASH or 0.0)
        eq0 = float(BROKER_START_EQUITY or 0.0) if float(BROKER_START_EQUITY or 0.0) > 0.0 else float(cash0)
    if cash0 == 0.0 and eq0 == 0.0:
        eq0 = 1.0
    con.execute(
        """
        INSERT INTO broker_shadow_account(book_key, cash, equity, updated_ts_ms)
        VALUES(?,?,?,?)
        """,
        (str(book_key), float(cash0), float(eq0), int(_now_ms())),
    )


def init_broker_db(*, use_write_txn: bool = True):
    con = None
    try:
        try:
            con = connect(readonly=True)
        except Exception:
            con = None
        if con is not None and _broker_schema_ready(con):
            return
    finally:
        if con is not None:
            _safe_close_connection(con, context="init_broker_db_schema_probe", once_key="init_broker_db_schema_probe_close")
    with _BROKER_DB_INIT_LOCK:
        con = None
        try:
            try:
                con = connect(readonly=True)
            except Exception:
                con = None
            if con is not None and _broker_schema_ready(con):
                return
        finally:
            if con is not None:
                _safe_close_connection(con, context="init_broker_db_locked_schema_probe", once_key="init_broker_db_locked_schema_probe_close")

        if use_write_txn:
            run_write_txn(
                _ensure_tables,
                table="broker_account",
                operation="init_broker_db",
                direct=True,
            )
            return

        con = connect_rw_direct()
        try:
            _ensure_tables(con)
            con.commit()
        except Exception:
            try:
                con.rollback()
            except Exception as e:
                _warn_nonfatal(
                    "broker_sim_init_rollback_failed",
                    "BROKER_SIM_INIT_ROLLBACK_FAILED",
                    e,
                    warn_key="init_rollback_failed",
                )
            raise
        finally:
            _safe_close_connection(con, context="init_broker_db_direct_write", once_key="init_broker_db_direct_write_close")


def _read_account(con, book_key: Optional[str] = None) -> dict:
    if _is_shadow_book(book_key):
        _seed_shadow_account(con, str(_book_key(book_key)))
        r = con.execute(
            """
            SELECT cash, equity, updated_ts_ms
            FROM broker_shadow_account
            WHERE book_key=?
            """,
            (str(_book_key(book_key)),),
        ).fetchone()
        if not r:
            return _default_account_snapshot()
        snapshot = _normalize_account_snapshot(
            r[0],
            r[1],
            r[2],
            scope=f"broker_shadow_account:{str(_book_key(book_key))}",
        )
        _repair_account_snapshot_if_needed(
            con,
            snapshot,
            cash_raw=r[0],
            equity_raw=r[1],
            updated_ts_raw=r[2],
            scope=f"broker_shadow_account:{str(_book_key(book_key))}",
            book_key=str(_book_key(book_key)),
        )
        return snapshot
    if _broker_account_uses_singleton_id(con):
        r = con.execute("SELECT cash, equity, updated_ts_ms FROM broker_account WHERE id=1").fetchone()
    else:
        r = con.execute(
            """
            SELECT cash, equity, COALESCE(updated_ts_ms, ts_ms) AS updated_ts_ms
            FROM broker_account
            ORDER BY COALESCE(updated_ts_ms, ts_ms, 0) DESC, ts_ms DESC
            LIMIT 1
            """
        ).fetchone()
    if not r:
        return _default_account_snapshot()
    snapshot = _normalize_account_snapshot(
        r[0],
        r[1],
        r[2],
        scope="broker_account",
    )
    _repair_account_snapshot_if_needed(
        con,
        snapshot,
        cash_raw=r[0],
        equity_raw=r[1],
        updated_ts_raw=r[2],
        scope="broker_account",
    )
    return snapshot


def _write_account(con, cash: float, equity: float, ts_ms: int, book_key: Optional[str] = None):
    if _is_shadow_book(book_key):
        con.execute(
            """
            INSERT INTO broker_shadow_account(book_key, cash, equity, updated_ts_ms)
            VALUES(?,?,?,?)
            ON CONFLICT(book_key) DO UPDATE SET
              cash=excluded.cash,
              equity=excluded.equity,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (str(_book_key(book_key)), float(cash), float(equity), int(ts_ms)),
        )
        return
    if _broker_account_uses_singleton_id(con):
        con.execute(
            "UPDATE broker_account SET cash=?, equity=?, updated_ts_ms=? WHERE id=1",
            (float(cash), float(equity), int(ts_ms)),
        )
    else:
        con.execute(
            """
            INSERT INTO broker_account(
                ts_ms, updated_ts_ms, broker, account_id, equity, cash,
                buying_power, maintenance_margin, day_pnl, unrealized_pnl,
                realized_pnl, currency, extra_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ts_ms) DO UPDATE SET
              updated_ts_ms=excluded.updated_ts_ms,
              broker=excluded.broker,
              account_id=excluded.account_id,
              equity=excluded.equity,
              cash=excluded.cash,
              buying_power=excluded.buying_power,
              maintenance_margin=excluded.maintenance_margin,
              day_pnl=excluded.day_pnl,
              unrealized_pnl=excluded.unrealized_pnl,
              realized_pnl=excluded.realized_pnl,
              currency=excluded.currency,
              extra_json=excluded.extra_json
            """,
            (
                int(ts_ms),
                int(ts_ms),
                "sim",
                "paper",
                float(equity),
                float(cash),
                float(cash),
                None,
                None,
                None,
                None,
                "USD",
                None,
            ),
        )


def _is_transient_write_error(error: BaseException) -> bool:
    return dbapi.is_transient_write_error(error)


def _persist_account_snapshot(
    cash: float,
    equity: float,
    ts_ms: int,
    book_key: Optional[str] = None,
    con: Optional[Any] = None,
) -> None:
    def _txn(tx_con) -> None:
        _write_account(
            tx_con,
            cash=float(cash),
            equity=float(equity),
            ts_ms=int(ts_ms),
            book_key=book_key,
        )

    if con is not None:
        _txn(con)
        return

    run_write_txn(
        _txn,
        table=("broker_shadow_account" if _is_shadow_book(book_key) else "broker_account"),
        operation="persist_broker_account_snapshot",
        direct=True,
        maintenance=False,
    )


def _get_meta(con, key: str, book_key: Optional[str] = None):
    if _is_shadow_book(book_key):
        r = con.execute(
            "SELECT value FROM broker_shadow_meta WHERE book_key=? AND key=?",
            (str(_book_key(book_key)), str(key)),
        ).fetchone()
        return str(r[0]) if r and r[0] is not None else None
    r = con.execute("SELECT value FROM broker_meta WHERE key=?", (str(key),)).fetchone()
    return str(r[0]) if r and r[0] is not None else None


def _set_meta(con, key: str, value: str, book_key: Optional[str] = None):
    if _is_shadow_book(book_key):
        con.execute(
            """
            INSERT INTO broker_shadow_meta(book_key, key, value) VALUES(?,?,?)
            ON CONFLICT(book_key,key) DO UPDATE SET value=excluded.value
            """,
            (str(_book_key(book_key)), str(key), str(value)),
        )
        return
    con.execute(
        """
        INSERT INTO broker_meta(key,value) VALUES(?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (str(key), str(value)),
    )


def _get_price_at_or_before(con, symbol: str, ts_ms: int):
    r = con.execute(
        """
        SELECT price, ts_ms
        FROM prices
        WHERE symbol = ? AND ts_ms <= ?
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
        (str(symbol), int(ts_ms)),
    ).fetchone()
    if not r:
        # legacy fallback (older schema) - keep behavior
        r = con.execute(
            "SELECT px, ts_ms FROM prices WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1",
            (str(symbol),),
        ).fetchone()
    if not r:
        return None, None
    try:
        px, px_ts = float(r[0]), int(r[1])
        # Fail if price is too stale (default 5 minutes)
        max_age_ms = int(os.environ.get("BROKER_MAX_PRICE_AGE_MS", "300000"))
        if (ts_ms - px_ts) > max_age_ms:
            return None, None
        return px, px_ts

    except Exception as e:
        _warn_nonfatal(
            "broker_sim_get_price_at_or_before_failed",
            "BROKER_SIM_GET_PRICE_AT_OR_BEFORE_FAILED",
            e,
            warn_key=f"get_price_at_or_before:{symbol}",
            symbol=str(symbol),
        )
        price_result = (None, None)
        return price_result


@lru_cache(maxsize=4096)
def _option_contract_meta(symbol: str):
    try:
        from engine.data.options_instrument import parse_option_symbol

        meta = parse_option_symbol(symbol)
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_option_instrument_import_failed",
            "BROKER_SIM_OPTION_INSTRUMENT_IMPORT_FAILED",
            e,
            warn_key="broker_sim_option_instrument_import_failed",
        )
        return None
    if meta is None:
        return None
    try:
        multiplier = float(getattr(meta, "multiplier"))
        contract = str(getattr(meta, "occ_symbol"))
        underlying = str(getattr(meta, "underlying"))
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_option_contract_meta_invalid",
            "BROKER_SIM_OPTION_CONTRACT_META_INVALID",
            e,
            warn_key=f"broker_sim_option_contract_meta_invalid:{symbol}",
            symbol=str(symbol),
        )
        return None
    if multiplier <= 0.0 or not contract.strip() or not underlying.strip():
        return None
    return meta


def _is_option_symbol(symbol: str) -> bool:
    return _option_contract_meta(str(symbol or "").upper().strip()) is not None


def _option_contract_key(meta) -> Optional[str]:
    try:
        contract = str(getattr(meta, "occ_symbol") or "").upper().strip()
    except Exception:
        contract = ""
    return contract or None


def _option_multiplier(meta) -> Optional[float]:
    try:
        multiplier = float(getattr(meta, "multiplier"))
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_option_multiplier_parse_failed",
            "BROKER_SIM_OPTION_MULTIPLIER_PARSE_FAILED",
            e,
            warn_key="broker_sim_option_multiplier_parse_failed",
        )
        return None
    if multiplier <= 0.0 or not math.isfinite(multiplier):
        return None
    return float(multiplier)


def _get_option_quote_at_or_before(con, contract: str, ts_ms: int):
    contract_key = str(contract or "").upper().strip()
    if not contract_key:
        return None, None, None, None
    try:
        row = con.execute(
            """
            SELECT bid, ask, ts_ms
            FROM options_chain_v2
            WHERE contract = ? AND ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (contract_key, int(ts_ms)),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_option_quote_lookup_failed",
            "BROKER_SIM_OPTION_QUOTE_LOOKUP_FAILED",
            e,
            warn_key=f"broker_sim_option_quote_lookup_failed:{contract_key}",
            contract=str(contract_key),
        )
        return None, None, None, None
    if not row:
        return None, None, None, None
    try:
        bid = float(row[0] or 0.0)
        ask = float(row[1] or 0.0)
        quote_ts = int(row[2])
        if int(ts_ms) - int(quote_ts) > int(BROKER_OPTION_MAX_QUOTE_AGE_MS):
            return None, None, None, None
        if bid <= 0.0 or ask <= 0.0:
            return None, None, None, None
        mid = (float(bid) + float(ask)) / 2.0
        if mid <= 0.0:
            return None, None, None, None
        return float(bid), float(ask), float(mid), int(quote_ts)
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_option_quote_parse_failed",
            "BROKER_SIM_OPTION_QUOTE_PARSE_FAILED",
            e,
            warn_key=f"broker_sim_option_quote_parse_failed:{contract_key}",
            contract=str(contract_key),
        )
        return None, None, None, None


def _option_spread_bps(bid: float, ask: float, mid: float) -> float:
    mid_f = float(mid or 0.0)
    if mid_f <= 0.0:
        return float(BROKER_SPREAD_BPS)
    return max(0.0, ((float(ask) - float(bid)) / mid_f) * 10000.0)


def _option_short_margin(meta, qty: float, mid: float, underlying_px: float) -> float:
    """Reference-grade short-option margin proxy for paper simulation only."""

    multiplier = _option_multiplier(meta)
    if multiplier is None:
        return 0.0
    contracts = abs(float(qty or 0.0))
    if contracts <= 0.0:
        return 0.0
    fraction = max(0.0, _safe_f(OPTIONS_SIM_MARGIN_UNDERLYING_FRACTION, 0.20))
    return float(contracts) * float(multiplier) * (float(mid) + float(fraction) * max(0.0, float(underlying_px or 0.0)))


def _option_underlying_px(con, meta, ts_ms: int) -> Optional[float]:
    try:
        underlying = str(getattr(meta, "underlying") or "").upper().strip()
    except Exception:
        underlying = ""
    if not underlying:
        return None
    px, _ = _get_price_at_or_before(con, underlying, int(ts_ms))
    if px is None or float(px) <= 0.0:
        return None
    return float(px)


def _read_position(con, symbol: str, book_key: Optional[str] = None):
    if _is_shadow_book(book_key):
        r = con.execute(
            "SELECT qty, avg_px FROM broker_shadow_positions WHERE book_key=? AND symbol=?",
            (str(_book_key(book_key)), str(symbol)),
        ).fetchone()
        if not r:
            return 0.0, 0.0
        return float(r[0]), float(r[1])
    if _broker_positions_use_timeseries(con):
        r = con.execute(
            """
            SELECT qty, avg_px
            FROM broker_positions
            WHERE symbol=?
            ORDER BY COALESCE(updated_ts_ms, ts_ms, 0) DESC, ts_ms DESC
            LIMIT 1
            """,
            (str(symbol),),
        ).fetchone()
    else:
        r = con.execute(
            "SELECT qty, avg_px FROM broker_positions WHERE symbol=?",
            (str(symbol),),
        ).fetchone()
    if not r:
        return 0.0, 0.0
    return float(r[0]), float(r[1])


def _write_position(con, symbol: str, qty: float, avg_px: float, ts_ms: int, book_key: Optional[str] = None):
    if _is_shadow_book(book_key):
        con.execute(
            """
            INSERT INTO broker_shadow_positions(book_key, symbol, qty, avg_px, updated_ts_ms)
            VALUES(?,?,?,?,?)
            ON CONFLICT(book_key, symbol) DO UPDATE SET
              qty=excluded.qty,
              avg_px=excluded.avg_px,
              updated_ts_ms=excluded.updated_ts_ms
            """,
            (str(_book_key(book_key)), str(symbol), float(qty), float(avg_px), int(ts_ms)),
        )
        return
    if _broker_positions_use_timeseries(con):
        side = "LONG" if float(qty) > 0 else ("SHORT" if float(qty) < 0 else "FLAT")
        market_px = float(avg_px) if abs(float(qty)) > 0.0 else 0.0
        market_value = float(qty) * float(market_px)
        con.execute(
            """
            INSERT INTO broker_positions(
                ts_ms, symbol, qty, avg_px, market_px, market_value,
                unrealized_pnl, realized_pnl, side, updated_ts_ms, extra_json
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, ts_ms) DO UPDATE SET
              qty=excluded.qty,
              avg_px=excluded.avg_px,
              market_px=excluded.market_px,
              market_value=excluded.market_value,
              unrealized_pnl=excluded.unrealized_pnl,
              realized_pnl=excluded.realized_pnl,
              side=excluded.side,
              updated_ts_ms=excluded.updated_ts_ms,
              extra_json=excluded.extra_json
            """,
            (
                int(ts_ms),
                str(symbol),
                float(qty),
                float(avg_px),
                float(market_px),
                float(market_value),
                None,
                None,
                side,
                int(ts_ms),
                None,
            ),
        )
        return
    con.execute(
        """
        INSERT INTO broker_positions(symbol, qty, avg_px, updated_ts_ms)
        VALUES(?,?,?,?)
        ON CONFLICT(symbol) DO UPDATE SET
          qty=excluded.qty,
          avg_px=excluded.avg_px,
          updated_ts_ms=excluded.updated_ts_ms
        """,
        (str(symbol), float(qty), float(avg_px), int(ts_ms)),
    )

def _exec_px(
    mid_px: float,
    side: str,
    trade_notional: float = 0.0,
    equity: float = 0.0,
    slip_bps_override: Optional[float] = None,
    spread_bps_override: Optional[float] = None,
) -> float:
    """
    Execution price with:
      - spread (half spread added/subtracted)
      - slippage (bps), optionally size-aware using an impact proxy

    Optional per-call overrides:
      - slip_bps_override
      - spread_bps_override
    """
    mid_px = _safe_f(mid_px, 0.0)
    if mid_px <= 0.0:
        return 0.0

    # clamp knobs to sane ranges
    spread_bps = max(0.0, _safe_f(spread_bps_override if spread_bps_override is not None else BROKER_SPREAD_BPS, 0.0))
    slip_bps = max(0.0, _safe_f(slip_bps_override if slip_bps_override is not None else BROKER_SLIPPAGE_BPS, 0.0))
    fee_bps = max(0.0, _safe_f(BROKER_FEE_BPS, 0.0))  # not used here, but kept consistent
    _ = fee_bps

    half_spread = (spread_bps / 10000.0) * mid_px / 2.0
    base_slip = (slip_bps / 10000.0) * mid_px

    # size-aware slippage (impact proxy): increases with notional/equity
    slip = base_slip
    try:
        eq = _safe_f(equity, 0.0)
        tn = abs(_safe_f(trade_notional, 0.0))
        impact_alpha = max(0.0, _safe_f(BROKER_IMPACT_ALPHA, 0.0))
        if eq > 1e-9 and impact_alpha > 0.0 and tn > 0.0:
            slip = base_slip * (1.0 + impact_alpha * (tn / eq))
    except Exception:
        slip = base_slip

    s = str(side or "").upper()
    if s == "BUY":
        return max(0.0, mid_px + half_spread + slip)
    # SELL
    return max(0.0, mid_px - half_spread - slip)


def _impact_px_from_bps(mid_px: float, impact_bps: float) -> float:
    px_f = _safe_f(mid_px, 0.0)
    if px_f <= 0.0:
        return 0.0
    return max(0.0, (max(0.0, _safe_f(impact_bps, 0.0)) / 10000.0) * float(px_f))


def _estimate_optional_cost_model(
    cost_model: Any,
    *,
    symbol: str,
    qty: float,
    px: float,
    side: str,
    liquidity_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "ok": False,
        "enabled": bool(cost_model is not None),
        "model": "",
        "symbol": str(symbol or "").upper().strip(),
        "side": str(side or "").upper().strip(),
        "qty_abs": float(abs(_safe_f(qty, 0.0))),
        "px": float(_safe_f(px, 0.0)),
        "execution_cost_bps": 0.0,
    }
    if cost_model is None:
        return result

    px_f = _safe_f(px, 0.0)
    qty_abs = abs(_safe_f(qty, 0.0))
    if px_f <= 0.0 or qty_abs <= 0.0:
        return result

    snapshot = dict(liquidity_snapshot or {})
    notional = float(qty_abs) * float(px_f)
    adv_raw = _safe_f(
        snapshot.get("rolling_adv_notional")
        or snapshot.get("adv_notional")
        or snapshot.get("rolling_adv")
        or 0.0,
        0.0,
    )
    adv = float(adv_raw)
    if adv > 0.0 and adv < max(1.0, qty_abs * 100.0):
        adv = adv * float(px_f)
    if adv <= 0.0:
        adv = max(float(notional) * 20.0, 1.0)

    sigma_daily = _safe_f(
        snapshot.get("sigma_daily")
        or snapshot.get("daily_volatility")
        or snapshot.get("daily_vol")
        or 0.0,
        0.0,
    )
    if 0.0 < sigma_daily <= 1.0:
        sigma_daily = sigma_daily * 10000.0
    if sigma_daily <= 0.0:
        sigma_daily = _safe_f(snapshot.get("intraday_vol_bps"), 0.0)
    if sigma_daily <= 0.0:
        sigma_daily = 200.0

    participation = _safe_f(
        snapshot.get("live_participation_rate")
        or snapshot.get("adv_participation")
        or 0.0,
        0.0,
    )
    if participation <= 0.0:
        participation = min(1.0, max(0.0, float(notional) / float(max(adv, 1e-12))))

    half_spread_bps = max(0.0, _safe_f(snapshot.get("true_spread_bps"), BROKER_SPREAD_BPS) * 0.5)
    try:
        cost_bps = float(
            cost_model.cost_bps(
                notional=float(notional),
                adv=float(adv),
                sigma_daily=float(sigma_daily),
                participation=float(participation),
                half_spread_bps=float(half_spread_bps),
                asset_class=str(snapshot.get("asset_class") or "US_EQUITY"),
            )
        )
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_cost_model_failed",
            "BROKER_SIM_COST_MODEL_FAILED",
            e,
            warn_key=f"broker_sim_cost_model_failed:{symbol}",
            symbol=str(symbol),
        )
        return result

    result.update(
        {
            "ok": True,
            "enabled": True,
            "model": type(cost_model).__name__,
            "execution_cost_bps": float(max(0.0, cost_bps)),
            "notional": float(notional),
            "adv": float(adv),
            "sigma_daily": float(sigma_daily),
            "participation_rate": float(participation),
            "true_spread_bps": float(half_spread_bps * 2.0),
            "liquidity_snapshot": snapshot,
        }
    )
    return result


def _fee(notional: float) -> float:
    return abs(float(notional or 0.0)) * (BROKER_FEE_BPS / 10000.0)


def _offline_fx_crosses_weekend(cfg: Dict[str, Any]) -> bool:
    if "crosses_weekend" in cfg:
        return bool(cfg.get("crosses_weekend"))
    start = cfg.get("entry_ts_ms", cfg.get("start_ts_ms", cfg.get("ts_ms")))
    end = cfg.get("exit_ts_ms", cfg.get("end_ts_ms", cfg.get("next_ts_ms")))
    if start in (None, "") or end in (None, ""):
        return False
    try:
        from engine.data.prices.fx_clock import fx_window_spans_closed_gap

        return bool(fx_window_spans_closed_gap(int(float(start)), int(float(end))))
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_offline_fx_weekend_gap_check_failed",
            "BROKER_SIM_OFFLINE_FX_WEEKEND_GAP_CHECK_FAILED",
            e,
            warn_key="offline_fx_crosses_weekend",
        )
        return False


def _offline_ac_cost_components(
    turnover: float,
    *,
    cost_config: Optional[Dict[str, Any]] = None,
    cost_model: Optional[Any] = None,
) -> Dict[str, float]:
    cfg = dict(cost_config or {})
    turnover_f = max(0.0, _safe_f(turnover, 0.0))
    if turnover_f <= 1e-12 or not bool(cfg.get("enabled", True)):
        return {
            "turnover": float(turnover_f),
            "commission_bps": 0.0,
            "half_spread_bps": 0.0,
            "temporary_impact_bps": 0.0,
            "total_cost_bps": 0.0,
            "cost_return": 0.0,
        }

    model = cost_model if cost_model is not None else AlmgrenChrissCost()
    base_notional = max(0.0, _safe_f(cfg.get("notional"), 100_000.0))
    asset_class = str(cfg.get("asset_class") or "US_EQUITY")
    is_fx = is_fx_asset_class(asset_class)
    is_crypto = is_crypto_asset_class(asset_class)
    symbol = str(cfg.get("symbol") or cfg.get("pair") or "EUR_USD")
    fx_full_spread_bps = 0.0
    crypto_full_spread_bps = 0.0
    if is_fx:
        fx_full_spread_bps = max(0.0, float(pip_spread_bps(symbol, half=False)))
        if "half_spread_bps" in cfg and cfg.get("half_spread_bps") not in (None, ""):
            half_spread_bps = max(0.0, _safe_f(cfg.get("half_spread_bps"), fx_full_spread_bps / 2.0))
        else:
            half_spread_bps = max(0.0, float(pip_spread_bps(symbol, half=True)))
    elif is_crypto:
        crypto_symbol = str(cfg.get("symbol") or cfg.get("pair") or "BTC")
        crypto_full_spread_bps = max(0.0, float(crypto_spread_bps(crypto_symbol, half=False)))
        if "half_spread_bps" in cfg and cfg.get("half_spread_bps") not in (None, ""):
            half_spread_bps = max(0.0, _safe_f(cfg.get("half_spread_bps"), crypto_full_spread_bps / 2.0))
        else:
            half_spread_bps = max(0.0, float(crypto_spread_bps(crypto_symbol, half=True)))
    else:
        half_spread_bps = max(0.0, _safe_f(cfg.get("half_spread_bps"), 1.0))
    components = model.components_bps(
        notional=float(base_notional) * float(turnover_f),
        adv=max(1e-12, _safe_f(cfg.get("adv"), 10_000_000.0)),
        sigma_daily=max(0.0, _safe_f(cfg.get("sigma_daily"), 200.0)),
        participation=max(0.0, min(1.0, _safe_f(cfg.get("participation"), 0.10))),
        half_spread_bps=0.0,
        asset_class=asset_class,
    )
    if is_crypto:
        crypto_symbol = str(cfg.get("symbol") or cfg.get("pair") or "BTC")
        default_fee_bps = float(
            crypto_fee_bps(
                crypto_symbol,
                taker=str(cfg.get("liquidity") or "taker").lower().strip() != "maker",
            )
        )
        commission_bps = max(0.0, _safe_f(cfg.get("commission_bps"), default_fee_bps))
    else:
        commission_bps = max(0.0, _safe_f(cfg.get("commission_bps"), float(BROKER_FEE_BPS)))
    temporary_bps = max(0.0, _safe_f(components.get("temporary_impact_bps"), 0.0))
    swap_carry_bps = 0.0
    weekend_gap_cost_bps = 0.0
    crypto_funding_bps = 0.0
    if is_fx:
        swap_carry_bps = float(
            swap_bps(
                symbol,
                _safe_f(cfg.get("side_sign"), _safe_f(cfg.get("direction"), 1.0)),
                int(max(0.0, _safe_f(cfg.get("nights"), 1.0))),
            )
        )
        weekend_gap_cost_bps = float(weekend_gap_bps(symbol, crosses_weekend=_offline_fx_crosses_weekend(cfg)))
    if is_crypto:
        crypto_funding_bps = float(
            crypto_funding_carry_bps(
                str(cfg.get("symbol") or cfg.get("pair") or "BTC"),
                _safe_f(cfg.get("side_sign"), _safe_f(cfg.get("direction"), 1.0)),
                int(max(0.0, _safe_f(cfg.get("nights"), 1.0))),
            )
        )
    total_bps = max(
        0.0,
        float(commission_bps + half_spread_bps + temporary_bps + swap_carry_bps + weekend_gap_cost_bps + crypto_funding_bps),
    )
    result = {
        "turnover": float(turnover_f),
        "commission_bps": float(commission_bps),
        "half_spread_bps": float(half_spread_bps),
        "temporary_impact_bps": float(temporary_bps),
        "total_cost_bps": float(total_bps),
        "cost_return": float(turnover_f * total_bps / 10000.0),
    }
    if is_fx:
        result.update(
            {
                "fx_pip_spread_bps": float(fx_full_spread_bps),
                "swap_carry_bps": float(swap_carry_bps),
                "weekend_gap_bps": float(weekend_gap_cost_bps),
            }
        )
    if is_crypto:
        result.update(
            {
                "crypto_spread_bps": float(crypto_full_spread_bps),
                "crypto_fee_bps": float(commission_bps),
                "funding_carry_bps": float(crypto_funding_bps),
            }
        )
    return result


def simulate_weight_order_batch(
    *,
    orders: List[Dict[str, Any]],
    realized_returns_by_symbol: Dict[str, List[float]],
    previous_weights: Optional[Dict[str, float]] = None,
    cost_config: Optional[Dict[str, Any]] = None,
    cost_model: Optional[Any] = None,
) -> Dict[str, Any]:
    """Deterministic broker-sim replay for offline target-weight batches."""
    prior = {str(k).upper().strip(): _safe_f(v, 0.0) for k, v in dict(previous_weights or {}).items()}
    weights: Dict[str, float] = {}
    for order in list(orders or []):
        symbol = str((order or {}).get("symbol") or "").upper().strip()
        if not symbol:
            continue
        raw_weight = abs(_safe_f((order or {}).get("to_weight"), _safe_f((order or {}).get("qty"), 0.0)))
        side = str((order or {}).get("side") or "").upper().strip()
        weights[symbol] = float(raw_weight if side != "SELL" else -raw_weight)

    turnover = sum(
        abs(float(weights.get(symbol, 0.0)) - float(prior.get(symbol, 0.0)))
        for symbol in sorted(set(weights.keys()) | set(prior.keys()))
    )
    gross_return = 0.0
    for symbol, weight in weights.items():
        realized_values = [
            _safe_f(value, 0.0)
            for value in list((realized_returns_by_symbol or {}).get(str(symbol).upper().strip(), []) or [])
        ]
        if not realized_values:
            continue
        gross_return += float(weight) * float(sum(realized_values) / float(len(realized_values)))
    costs = _offline_ac_cost_components(turnover, cost_config=cost_config, cost_model=cost_model)
    cost_return = float(costs.get("cost_return") or 0.0)
    return {
        "ok": True,
        "weights": dict(weights),
        "gross_return": float(gross_return),
        "cost_return": float(cost_return),
        "net_return": float(gross_return - cost_return),
        "turnover": float(turnover),
        "costs": dict(costs),
    }


def _broker_fill_duplicate_exists(
    con,
    *,
    ts_ms: int,
    source_order_id,
    symbol: str,
    source: str,
    book_key: Optional[str],
    columns: set[str],
) -> bool:
    if source_order_id is None:
        return False
    try:
        if {"source", "book_key"}.issubset(columns):
            row = con.execute(
                """
                SELECT 1
                FROM broker_fills
                WHERE source_order_id=?
                  AND source=?
                  AND COALESCE(book_key, '')=?
                  AND symbol=?
                  AND ts_ms=?
                LIMIT 1
                """,
                (
                    source_order_id,
                    str(source),
                    str(_book_key(book_key) or ""),
                    str(symbol),
                    int(ts_ms),
                ),
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT 1
                FROM broker_fills
                WHERE source_order_id=? AND symbol=? AND ts_ms=?
                LIMIT 1
                """,
                (source_order_id, str(symbol), int(ts_ms)),
            ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_fill_duplicate_probe_failed",
            "BROKER_SIM_FILL_DUPLICATE_PROBE_FAILED",
            e,
            warn_key=f"broker_fill_duplicate_probe_failed:{symbol}",
            symbol=str(symbol),
            source_order_id=(int(source_order_id) if source_order_id is not None else None),
        )
        return False


def _warn_broker_fill_deduped(
    *,
    ts_ms: int,
    source_order_id,
    symbol: str,
    source: str,
    book_key: Optional[str],
) -> None:
    _warn_nonfatal(
        "broker_sim_fill_duplicate_ignored",
        "BROKER_SIM_FILL_DUPLICATE_IGNORED",
        RuntimeError("duplicate sourced broker fill ignored"),
        warn_key=f"broker_fill_duplicate_ignored:{source}:{_book_key(book_key) or ''}:{symbol}:{ts_ms}:{source_order_id}",
        source=str(source),
        book_key=str(_book_key(book_key) or ""),
        symbol=str(symbol),
        ts_ms=int(ts_ms),
        source_order_id=(int(source_order_id) if source_order_id is not None else None),
    )


def _write_fill(
    con,
    ts_ms: int,
    source_order_id,
    symbol: str,
    qty: float,
    px: float,
    fees: Optional[float] = None,
    note: str = "",
    explain_json: Optional[str] = None,
    client_order_id: Optional[str] = None,
    book_key: Optional[str] = None,
    source: Optional[str] = None,
    contract_multiplier: Optional[float] = None,
    option_quote_source: Optional[str] = None,
    option_margin_debit: Optional[float] = None,
) -> bool:
    from engine.runtime.state_cache import cache_invalidate_namespace

    fill_book_key = _book_key(book_key)
    fill_source = str(source or ("shadow" if fill_book_key else "sim")).strip() or "sim"
    columns = _broker_fills_columns(con)
    dedup_source_fill = source_order_id is not None
    if dedup_source_fill and _broker_fill_duplicate_exists(
        con,
        ts_ms=int(ts_ms),
        source_order_id=source_order_id,
        symbol=str(symbol),
        source=str(fill_source),
        book_key=fill_book_key,
        columns=columns,
    ):
        _warn_broker_fill_deduped(
            ts_ms=int(ts_ms),
            source_order_id=source_order_id,
            symbol=str(symbol),
            source=str(fill_source),
            book_key=fill_book_key,
        )
        return False
    insert_prefix = "INSERT OR IGNORE" if dedup_source_fill else "INSERT"
    insert_cur = None
    if {"source", "book_key", "contract_multiplier", "option_quote_source", "option_margin_debit"}.issubset(columns):
        insert_cur = con.execute(
            f"""
            {insert_prefix} INTO broker_fills(
              ts_ms, symbol, qty, px, source_order_id, source, book_key,
              contract_multiplier, option_quote_source, option_margin_debit,
              note, explain_json
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(ts_ms),
                str(symbol),
                float(qty),
                float(px),
                source_order_id,
                str(fill_source),
                fill_book_key,
                (float(contract_multiplier) if contract_multiplier is not None else None),
                (str(option_quote_source) if option_quote_source is not None else None),
                (float(option_margin_debit) if option_margin_debit is not None else None),
                str(note or ""),
                explain_json,
            ),
        )
    elif {"source", "book_key"}.issubset(columns):
        insert_cur = con.execute(
            f"""
            {insert_prefix} INTO broker_fills(ts_ms, symbol, qty, px, source_order_id, source, book_key, note, explain_json)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                int(ts_ms),
                str(symbol),
                float(qty),
                float(px),
                source_order_id,
                str(fill_source),
                fill_book_key,
                str(note or ""),
                explain_json,
            ),
        )
    else:
        insert_cur = con.execute(
            f"""
            {insert_prefix} INTO broker_fills(ts_ms, symbol, qty, px, source_order_id, note, explain_json)
            VALUES(?,?,?,?,?,?,?)
            """,
            (int(ts_ms), str(symbol), float(qty), float(px), source_order_id, str(note or ""), explain_json),
        )
    inserted = int(getattr(insert_cur, "rowcount", 0) or 0) > 0
    if dedup_source_fill and not inserted:
        _warn_broker_fill_deduped(
            ts_ms=int(ts_ms),
            source_order_id=source_order_id,
            symbol=str(symbol),
            source=str(fill_source),
            book_key=fill_book_key,
        )
        return False
    cache_invalidate_namespace("api_read", prefix="execution_stats")
    cache_invalidate_namespace("api_read", prefix="execution_metrics")

    if fill_book_key:
        return True

    # --- execution ledger mirror (for slippage + pnl attribution parity) ---
    try:
        from engine.execution.execution_ledger import log_fill

        raw_payload: Dict[str, Any] = {
            "note": note,
            "symbol": str(symbol),
            "broker": "sim",
            "broker_fill_source": str(fill_source),
        }
        if explain_json:
            raw_payload["explain_json"] = explain_json
            try:
                explain = json.loads(explain_json or "{}")
                if isinstance(explain, dict):
                    if explain.get("mid_px") is not None:
                        raw_payload["mid_px"] = _safe_f(explain.get("mid_px"))
                    if explain.get("spread_bps") is not None:
                        raw_payload["spread_bps"] = _safe_f(explain.get("spread_bps"))
                    if explain.get("expected_price") is not None:
                        raw_payload["expected_px"] = _safe_f(explain.get("expected_price"))
                    if explain.get("latency_ms") is not None:
                        raw_payload["latency_ms"] = _safe_i(explain.get("latency_ms"))
            except Exception as e:
                _warn_nonfatal(
                    "broker_sim_fill_explain_parse_failed",
                    "BROKER_SIM_FILL_EXPLAIN_PARSE_FAILED",
                    e,
                    warn_key="broker_sim_fill_explain_parse_failed",
                    symbol=str(symbol),
                    source_order_id=(int(source_order_id) if source_order_id is not None else None),
                )

        log_fill(
            client_order_id=str(client_order_id or f"sim_{int(source_order_id) if source_order_id is not None else 'override'}_{symbol}"),
            fill_ts_ms=int(ts_ms),
            fill_qty=float(qty),
            fill_px=float(px),
            fees=(float(fees) if fees is not None else None),
            liquidity="sim",
            raw=raw_payload,
            con=con,
        )
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_fill_log_write_failed",
            "BROKER_SIM_FILL_LOG_WRITE_FAILED",
            e,
            warn_key=f"broker_sim_fill_log_write_failed:{symbol}",
            symbol=str(symbol),
            source_order_id=(int(source_order_id) if source_order_id is not None else None),
        )
    return True


def _mark_to_market(
    con,
    ts_ms: int,
    book_key: Optional[str] = None,
    *,
    persist: bool = True,
    best_effort: bool = False,
    persist_con: Optional[Any] = None,
):
    acct = _read_account(con, book_key=book_key)
    cash = float(acct.get("cash") or 0.0)
    eq = cash

    if _is_shadow_book(book_key):
        rows = con.execute(
            "SELECT symbol, qty FROM broker_shadow_positions WHERE book_key=?",
            (str(_book_key(book_key)),),
        ).fetchall()
    else:
        rows = con.execute("SELECT symbol, qty FROM broker_positions").fetchall()
    for sym, qty in rows or []:
        sym_text = str(sym)
        meta = _option_contract_meta(sym_text)
        if meta is not None:
            multiplier = _option_multiplier(meta)
            contract = _option_contract_key(meta)
            if multiplier is None or not contract:
                continue
            _bid, _ask, option_mid, _quote_ts = _get_option_quote_at_or_before(con, contract, int(ts_ms))
            if option_mid is None:
                continue
            eq += float(qty) * float(option_mid) * float(multiplier)
            continue
        px, _ = _get_price_at_or_before(con, sym_text, int(ts_ms))
        if px is None:
            continue
        eq += float(qty) * float(px)

    # Safety: prevent runaway negative equity from poisoning downstream sizing
    if not (eq > -1e12):
        eq = -1e12

    snapshot = {
        "cash": float(cash),
        "equity": float(eq),
        "updated_ts_ms": int(ts_ms),
    }
    if not persist:
        snapshot["storage_status"] = "skipped"
        return snapshot

    try:
        _persist_account_snapshot(
            cash=float(cash),
            equity=float(eq),
            ts_ms=int(ts_ms),
            book_key=book_key,
            con=persist_con,
        )
        snapshot["storage_status"] = "persisted"
    except Exception as e:
        if bool(best_effort) and _is_transient_write_error(e):
            _warn_nonfatal(
                "broker_sim_mark_to_market_persist_deferred",
                "BROKER_SIM_MARK_TO_MARKET_PERSIST_DEFERRED",
                e,
                warn_key="broker_sim_mark_to_market_persist_deferred",
                ts_ms=int(ts_ms),
                book_key=str(_book_key(book_key) or ""),
            )
            snapshot["storage_status"] = "best_effort_deferred_lock_contention"
            return snapshot
        raise
    return snapshot


def _position_qty_map(con, book_key: Optional[str] = None) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if _is_shadow_book(book_key):
        rows = con.execute(
            "SELECT symbol, qty FROM broker_shadow_positions WHERE book_key=?",
            (str(_book_key(book_key)),),
        ).fetchall()
    else:
        rows = con.execute("SELECT symbol, qty FROM broker_positions").fetchall()

    for sym, qty in rows or []:
        sym_u = str(sym or "").upper().strip()
        if not sym_u:
            continue
        out[sym_u] = float(qty or 0.0)
    return out


def _position_price_map(con, positions: Dict[str, float], ts_ms: int) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for sym in (positions or {}).keys():
        sym_u = str(sym or "").upper().strip()
        if not sym_u:
            continue
        meta = _option_contract_meta(sym_u)
        if meta is not None:
            multiplier = _option_multiplier(meta)
            contract = _option_contract_key(meta)
            if multiplier is None or not contract:
                continue
            _bid, _ask, option_mid, _quote_ts = _get_option_quote_at_or_before(con, contract, int(ts_ms))
            if option_mid is not None and float(option_mid) > 0.0:
                out[sym_u] = float(option_mid) * float(multiplier)
            continue
        px, _ = _get_price_at_or_before(con, sym_u, int(ts_ms))
        if px is not None and float(px) > 0.0:
            out[sym_u] = float(px)
    return out


def _options_lifecycle_enabled() -> bool:
    return str(os.environ.get("OPTIONS_LIFECYCLE_ENABLED", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def _lifecycle_position_rows(con, book_key: Optional[str] = None) -> List[Tuple[str, float, float]]:
    if _is_shadow_book(book_key):
        rows = con.execute(
            """
            SELECT symbol, qty, avg_px
            FROM broker_shadow_positions
            WHERE book_key=?
            ORDER BY symbol
            """,
            (str(_book_key(book_key)),),
        ).fetchall()
    elif _broker_positions_use_timeseries(con):
        rows = con.execute(
            """
            SELECT p.symbol, p.qty, p.avg_px
            FROM broker_positions p
            JOIN (
              SELECT symbol, MAX(COALESCE(updated_ts_ms, ts_ms, 0)) AS latest_ts
              FROM broker_positions
              GROUP BY symbol
            ) latest
              ON latest.symbol=p.symbol
             AND latest.latest_ts=COALESCE(p.updated_ts_ms, p.ts_ms, 0)
            ORDER BY p.symbol
            """
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT symbol, qty, avg_px
            FROM broker_positions
            ORDER BY symbol
            """
        ).fetchall()

    out: List[Tuple[str, float, float]] = []
    for symbol, qty, avg_px in rows or []:
        sym = str(symbol or "").upper().strip()
        qty_f = _safe_f(qty, 0.0)
        if not sym or abs(float(qty_f)) <= 1e-12:
            continue
        if _option_contract_meta(sym) is None:
            continue
        out.append((sym, float(qty_f), _safe_f(avg_px, 0.0)))
    return out


def _lifecycle_underlying_prices(con, positions: List[Tuple[str, float, float]], now_ms: int) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for symbol, _qty, _avg_px in positions or []:
        meta = _option_contract_meta(str(symbol))
        if meta is None:
            continue
        try:
            underlying = str(getattr(meta, "underlying") or "").upper().strip()
        except Exception:
            underlying = ""
        if not underlying or underlying in out:
            continue
        px, px_ts = _get_price_at_or_before(con, underlying, int(now_ms))
        if px is None or float(px) <= 0.0:
            continue
        out[underlying] = {"price": float(px), "ts_ms": int(px_ts) if px_ts is not None else None}
    return out


def _lifecycle_quote_mid(con, symbol: str, now_ms: int) -> Tuple[Optional[float], Optional[str], Optional[float]]:
    meta = _option_contract_meta(str(symbol))
    if meta is None:
        return None, None, None
    multiplier = _option_multiplier(meta)
    contract = _option_contract_key(meta)
    if multiplier is None or not contract:
        return None, None, None
    _bid, _ask, mid, quote_ts = _get_option_quote_at_or_before(con, str(contract), int(now_ms))
    if mid is None or float(mid) < 0.0:
        return None, None, None
    return float(mid), f"options_chain_v2:{contract}:{int(quote_ts)}", float(multiplier)


def _lifecycle_close_price(con, event: Any, now_ms: int) -> Tuple[Optional[float], str]:
    event_type = str(getattr(event, "event_type", "") or "")
    if event_type in {"EXERCISE", "ASSIGN", "CASH_SETTLE"}:
        return max(0.0, _safe_f(getattr(event, "intrinsic_per_contract", 0.0), 0.0)), "options_lifecycle:intrinsic"
    if event_type in {"EXPIRE_WORTHLESS", "PIN_RISK"}:
        return 0.0, "options_lifecycle:zero_settlement"
    if event_type in {"DTE_AUTOCLOSE", "DTE_ROLL"}:
        mid, quote_source, _multiplier = _lifecycle_quote_mid(con, str(getattr(event, "symbol", "")), int(now_ms))
        if mid is None:
            return None, "options_lifecycle:missing_quote"
        return float(mid), str(quote_source or "options_lifecycle:quote")
    return None, "options_lifecycle:unsupported_event"


def _avg_after_lifecycle_fill(cur_qty: float, cur_avg: float, fill_qty: float, fill_px: float) -> float:
    new_qty = float(cur_qty) + float(fill_qty)
    if abs(new_qty) <= 1e-12:
        return 0.0
    if (
        abs(float(cur_qty)) <= 1e-12
        or (float(cur_qty) > 0.0 and float(fill_qty) > 0.0)
        or (float(cur_qty) < 0.0 and float(fill_qty) < 0.0)
    ):
        denom = abs(float(cur_qty)) + abs(float(fill_qty))
        if denom <= 1e-12:
            return float(fill_px)
        return ((abs(float(cur_qty)) * float(cur_avg)) + (abs(float(fill_qty)) * float(fill_px))) / denom
    return float(cur_avg) if (float(cur_qty) * float(new_qty)) > 0.0 else float(fill_px)


def _lifecycle_event_payload(event: Any, *, cash_delta: float, close_px: float) -> str:
    if hasattr(event, "to_dict"):
        try:
            payload = dict(event.to_dict())
        except Exception:
            payload = {}
    else:
        payload = {}
    payload["cash_delta"] = float(cash_delta)
    payload["settlement_px"] = float(close_px)
    payload["settlement_model"] = "broker_sim_cash_settled_reference"
    payload["live_order_authority"] = False
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _apply_lifecycle_close(con, event: Any, *, cash: float, now_ms: int, book_key: Optional[str]) -> Tuple[float, bool, str]:
    symbol = str(getattr(event, "symbol", "") or "").upper().strip()
    qty = _safe_f(getattr(event, "qty", 0.0), 0.0)
    multiplier = _safe_f(getattr(event, "multiplier", 0.0), 0.0)
    if not symbol or abs(float(qty)) <= 1e-12 or multiplier <= 0.0:
        return float(cash), False, "invalid_event"

    close_px, quote_source = _lifecycle_close_price(con, event, int(now_ms))
    if close_px is None:
        return float(cash), False, str(quote_source)

    fill_qty = -float(qty)
    cash_delta = -float(fill_qty) * float(close_px) * float(multiplier)
    _write_position(con, symbol, qty=0.0, avg_px=0.0, ts_ms=int(now_ms), book_key=book_key)
    _write_fill(
        con,
        ts_ms=int(now_ms),
        source_order_id=None,
        symbol=symbol,
        qty=float(fill_qty),
        px=float(close_px),
        fees=0.0,
        note=f"options_lifecycle:{str(getattr(event, 'event_type', '') or 'UNKNOWN')}",
        explain_json=_lifecycle_event_payload(event, cash_delta=float(cash_delta), close_px=float(close_px)),
        client_order_id=f"options_lifecycle:{symbol}:{int(now_ms)}",
        book_key=book_key,
        source=("shadow" if _is_shadow_book(book_key) else "sim"),
        contract_multiplier=float(multiplier),
        option_quote_source=str(quote_source),
        option_margin_debit=None,
    )
    return float(cash) + float(cash_delta), True, "applied"


def _apply_lifecycle_roll_open(con, event: Any, *, cash: float, now_ms: int, book_key: Optional[str]) -> Tuple[float, bool, str]:
    target_symbol = str(getattr(event, "target_symbol", "") or "").upper().strip()
    qty = _safe_f(getattr(event, "qty", 0.0), 0.0)
    if not target_symbol or abs(float(qty)) <= 1e-12:
        return float(cash), False, "roll_target_unavailable"

    mid, quote_source, multiplier = _lifecycle_quote_mid(con, target_symbol, int(now_ms))
    if mid is None or multiplier is None:
        return float(cash), False, "roll_target_quote_unavailable"

    cur_qty, cur_avg = _read_position(con, target_symbol, book_key=book_key)
    new_qty = float(cur_qty) + float(qty)
    new_avg = _avg_after_lifecycle_fill(float(cur_qty), float(cur_avg), float(qty), float(mid))
    cash_delta = -float(qty) * float(mid) * float(multiplier)

    _write_position(con, target_symbol, qty=float(new_qty), avg_px=float(new_avg), ts_ms=int(now_ms), book_key=book_key)
    _write_fill(
        con,
        ts_ms=int(now_ms),
        source_order_id=None,
        symbol=target_symbol,
        qty=float(qty),
        px=float(mid),
        fees=0.0,
        note="options_lifecycle:DTE_ROLL_OPEN",
        explain_json=json.dumps(
            {
                "source_event": (event.to_dict() if hasattr(event, "to_dict") else {}),
                "cash_delta": float(cash_delta),
                "settlement_px": float(mid),
                "settlement_model": "broker_sim_roll_target_open",
                "live_order_authority": False,
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
        client_order_id=f"options_lifecycle_roll:{target_symbol}:{int(now_ms)}",
        book_key=book_key,
        source=("shadow" if _is_shadow_book(book_key) else "sim"),
        contract_multiplier=float(multiplier),
        option_quote_source=str(quote_source or ""),
        option_margin_debit=None,
    )
    return float(cash) + float(cash_delta), True, "applied"


def apply_option_lifecycle(con, *, book_key: Optional[str] = None, now_ms: Optional[int] = None) -> Dict[str, Any]:
    """Apply opt-in shadow option lifecycle events through broker-sim helpers."""

    if not _options_lifecycle_enabled():
        return {"ok": True, "processed": 0, "skipped_disabled": True}

    ts_ms = int(now_ms if now_ms is not None else _now_ms())
    summary: Dict[str, Any] = {
        "ok": True,
        "processed": 0,
        "fills_written": 0,
        "skipped_disabled": False,
        "book_key": _book_key(book_key),
        "events": [],
        "skipped": [],
    }

    try:
        _ensure_tables(con)
        from engine.execution.options_lifecycle import plan_option_lifecycle_events

        positions = _lifecycle_position_rows(con, book_key=book_key)
        if not positions:
            return summary
        underlying_prices = _lifecycle_underlying_prices(con, positions, int(ts_ms))
        events = plan_option_lifecycle_events(
            list(positions),
            underlying_prices=underlying_prices,
            now_ms=int(ts_ms),
            metadata_for=lambda symbol: _option_contract_meta(str(symbol)),
            env=os.environ,
        )
        summary["planned"] = int(len(events))
        if not events:
            return summary

        account = _read_account(con, book_key=book_key)
        cash = float(account.get("cash") or 0.0)
        for event in events:
            event_type = str(getattr(event, "event_type", "") or "")
            cash, applied, reason = _apply_lifecycle_close(con, event, cash=float(cash), now_ms=int(ts_ms), book_key=book_key)
            if not applied:
                summary["skipped"].append({"symbol": str(getattr(event, "symbol", "")), "event_type": event_type, "reason": reason})
                continue
            summary["processed"] = int(summary.get("processed", 0) or 0) + 1
            summary["fills_written"] = int(summary.get("fills_written", 0) or 0) + 1
            summary["events"].append(event.to_dict() if hasattr(event, "to_dict") else {"event_type": event_type})

            if event_type == "DTE_ROLL" and str(getattr(event, "target_symbol", "") or "").strip():
                cash, opened, open_reason = _apply_lifecycle_roll_open(
                    con,
                    event,
                    cash=float(cash),
                    now_ms=int(ts_ms),
                    book_key=book_key,
                )
                if opened:
                    summary["fills_written"] = int(summary.get("fills_written", 0) or 0) + 1
                    summary.setdefault("roll_opens", []).append(
                        {
                            "source_symbol": str(getattr(event, "symbol", "")),
                            "target_symbol": str(getattr(event, "target_symbol", "")),
                        }
                    )
                else:
                    summary["skipped"].append(
                        {
                            "symbol": str(getattr(event, "symbol", "")),
                            "event_type": "DTE_ROLL_OPEN",
                            "reason": open_reason,
                        }
                    )

        if int(summary.get("processed", 0) or 0) <= 0:
            return summary

        _write_account(
            con,
            cash=float(cash),
            equity=float(_read_account(con, book_key=book_key).get("equity", cash) or cash),
            ts_ms=int(ts_ms),
            book_key=book_key,
        )
        _safe_commit_connection(con, context="options_lifecycle", once_key="options_lifecycle_commit_account")
        summary["account"] = _mark_to_market(con, int(ts_ms), book_key=book_key)
        _safe_commit_connection(con, context="options_lifecycle", once_key="options_lifecycle_commit_mtm")
        return summary
    except Exception as e:
        try:
            con.rollback()
        except Exception as rollback_error:
            _warn_nonfatal(
                "broker_sim_options_lifecycle_rollback_failed",
                "BROKER_SIM_OPTIONS_LIFECYCLE_ROLLBACK_FAILED",
                rollback_error,
                warn_key="broker_sim_options_lifecycle_rollback_failed",
                book_key=str(_book_key(book_key) or ""),
            )
        _warn_nonfatal(
            "broker_sim_options_lifecycle_apply_failed",
            "BROKER_SIM_OPTIONS_LIFECYCLE_APPLY_FAILED",
            e,
            warn_key="broker_sim_options_lifecycle_apply_failed",
            book_key=str(_book_key(book_key) or ""),
        )
        summary["ok"] = False
        summary["error"] = str(e)
        return summary


def _book_exposure_notional(
    positions: Dict[str, float],
    price_map: Dict[str, float],
) -> Tuple[float, float]:
    gross = 0.0
    net = 0.0
    for sym, qty in (positions or {}).items():
        px = float(price_map.get(str(sym or "").upper().strip()) or 0.0)
        if px <= 0.0:
            continue
        signed = float(qty or 0.0) * float(px)
        gross += abs(float(signed))
        net += float(signed)
    return float(gross), float(net)


def _max_scale_for_metric(metric_fn, cap: float) -> float:
    eps = 1e-9
    cap_f = float(cap)
    current = float(metric_fn(0.0))
    projected = float(metric_fn(1.0))

    if projected <= cap_f + eps:
        return 1.0
    if projected <= current + eps:
        return 1.0
    if current >= cap_f - eps:
        return 0.0

    lo = 0.0
    hi = 1.0
    for _ in range(48):
        mid = (lo + hi) / 2.0
        if float(metric_fn(mid)) <= cap_f + eps:
            lo = mid
        else:
            hi = mid
    return float(max(0.0, min(1.0, lo)))


def _apply_execution_risk_caps(
    *,
    positions: Dict[str, float],
    price_map: Dict[str, float],
    symbol: str,
    current_qty: float,
    delta_qty: float,
    px: float,
    equity: float,
) -> Tuple[float, Dict[str, Any]]:
    sym = str(symbol or "").upper().strip()
    px_f = float(px or 0.0)
    eq_f = float(equity or 0.0)
    delta_f = float(delta_qty or 0.0)
    cur_qty_f = float(current_qty or 0.0)

    if (not sym) or px_f <= 0.0 or eq_f <= 0.0 or abs(delta_f) <= 1e-9:
        return delta_f, {"applied": False, "scale": 1.0}

    prices_local = dict(price_map or {})
    prices_local[sym] = float(px_f)

    gross_cur, net_cur = _book_exposure_notional(positions or {}, prices_local)
    cur_sym_notional = float(cur_qty_f) * float(px_f)
    delta_notional = float(delta_f) * float(px_f)
    other_gross = max(0.0, float(gross_cur) - abs(float(cur_sym_notional)))

    total_cap = max(0.0, float(EXEC_TOTAL_EXPOSURE_CAP)) * float(eq_f)
    symbol_cap = max(0.0, float(EXEC_SYMBOL_CONCENTRATION_CAP)) * float(eq_f)
    direction_cap = max(0.0, float(EXEC_DIRECTION_CONCENTRATION_CAP)) * float(eq_f)

    total_scale = _max_scale_for_metric(
        lambda s: float(other_gross) + abs(float(cur_sym_notional) + (float(s) * float(delta_notional))),
        total_cap,
    )
    symbol_scale = _max_scale_for_metric(
        lambda s: abs(float(cur_sym_notional) + (float(s) * float(delta_notional))),
        symbol_cap,
    )
    direction_scale = _max_scale_for_metric(
        lambda s: abs(float(net_cur) + (float(s) * float(delta_notional))),
        direction_cap,
    )

    scale = max(0.0, min(1.0, float(total_scale), float(symbol_scale), float(direction_scale)))
    scaled_delta = float(delta_f) * float(scale)
    projected_sym_notional = float(cur_sym_notional) + float(scaled_delta) * float(px_f)
    projected_total_gross = float(other_gross) + abs(float(projected_sym_notional))
    projected_net = float(net_cur) + (float(scaled_delta) * float(px_f))

    audit = {
        "applied": True,
        "scale": float(scale),
        "scaled": bool(scale < 0.999999),
        "caps": {
            "total_exposure_cap": float(total_cap),
            "symbol_concentration_cap": float(symbol_cap),
            "direction_concentration_cap": float(direction_cap),
        },
        "factors": {
            "total_exposure": float(total_scale),
            "symbol_concentration": float(symbol_scale),
            "direction_concentration": float(direction_scale),
        },
        "pre": {
            "gross_notional": float(gross_cur),
            "net_notional": float(net_cur),
            "symbol_notional": float(cur_sym_notional),
            "delta_notional": float(delta_notional),
        },
        "post": {
            "gross_notional": float(projected_total_gross),
            "net_notional": float(projected_net),
            "symbol_notional": float(projected_sym_notional),
            "delta_notional": float(scaled_delta * float(px_f)),
        },
    }
    return float(scaled_delta), audit


def _equity(con, ts_ms: int, book_key: Optional[str] = None) -> float:
    # capital-aware: prefer stored equity; if missing/zero, mark-to-market
    acct = _read_account(con, book_key=book_key)
    eq = float(acct.get("equity") or 0.0)
    if eq <= 0.0:
        acct = _mark_to_market(con, ts_ms, book_key=book_key)
        eq = float(acct.get("equity") or 0.0)
    return float(eq)


def _broker_sim_phase_load_orders(
    con,
    *,
    override_orders: Optional[List[dict]],
    override_order_id: Optional[int],
    override_ts_ms: Optional[int],
    now_ms: int,
) -> Dict[str, Any]:
    if override_orders is not None:
        return {
            "orders": list(override_orders or []),
            "order_id": (int(override_order_id) if override_order_id is not None else None),
            "ts_ms": (int(override_ts_ms) if override_ts_ms is not None else int(now_ms)),
            "override": True,
        }

    # Read the latest *row-per-order* portfolio_orders batch (no orders_json dependency).
    from engine.strategy.portfolio_execution_intents import load_latest_execution_intents

    batch = load_latest_execution_intents(con)
    return {
        "orders": list(batch.get("intents") or []),
        "order_id": batch.get("batch_id"),
        "ts_ms": int(batch.get("batch_ts_ms") or now_ms),
        "override": False,
    }


def _broker_sim_phase_validate_gate(
    con,
    *,
    loaded: Dict[str, Any],
    dry_run: bool,
    now_ms: int,
    book_key: Optional[str],
    ale_meta: Dict[str, Any],
) -> Dict[str, Any]:
    orders = list(loaded.get("orders") or [])
    order_id = loaded.get("order_id")

    if (not bool(loaded.get("override"))) and not orders:
        acct = _mark_to_market(con, int(now_ms), book_key=book_key, best_effort=True)
        return {
            "continue": False,
            "summary": {"ok": True, "status": "no_orders", "broker": "sim", "account": acct},
        }

    if dry_run:
        return {
            "continue": False,
            "summary": {
                "ok": True,
                "status": "dry_run_preview",
                "broker": "sim",
                "order_id": (int(order_id) if order_id is not None else None),
                "orders": orders,
                "ale": dict(ale_meta),
                "account": _read_account(con, book_key=book_key),
            },
        }

    if order_id is not None:
        last_applied = _get_meta(con, "last_portfolio_orders_id", book_key=book_key)
        if last_applied is not None:
            try:
                if int(last_applied) >= int(order_id):
                    acct = _mark_to_market(con, int(now_ms), book_key=book_key, best_effort=True)
                    return {
                        "continue": False,
                        "summary": {
                            "ok": True,
                            "status": "already_applied",
                            "broker": "sim",
                            "order_id": int(order_id),
                            "account": acct,
                        },
                    }
            except Exception as e:
                _warn_nonfatal(
                    "broker_sim_last_applied_order_guard_failed",
                    "BROKER_SIM_LAST_APPLIED_ORDER_GUARD_FAILED",
                    e,
                    warn_key="broker_sim_last_applied_order_guard_failed",
                    order_id=order_id,
                    last_applied=last_applied,
                    book_key=str(book_key or ""),
                )

    return {"continue": True, "orders": orders}


def _broker_sim_phase_size_cap(
    con,
    *,
    ts_ms: int,
    now_ms: int,
    book_key: Optional[str],
) -> Dict[str, Any]:
    acct = _read_account(con, book_key=book_key)
    cash = float(acct.get("cash") or 0.0)
    equity = float(_equity(con, int(ts_ms), book_key=book_key) or 0.0)
    position_qty_map = _position_qty_map(con, book_key=book_key)
    position_price_map = _position_price_map(con, position_qty_map, int(ts_ms))

    # Conservative deployable base allows testing leverage constraints even in sim.
    equity = float(
        compute_deployable_equity(
            {"equity": float(equity), "cash": float(cash), "buying_power": float(equity)},
            default_equity=float(equity),
        )
        or 0.0
    )

    if not _is_finite(equity) or equity <= 0.0:
        acct0 = _read_account(con, book_key=book_key)
        fallback_equity = max(
            _safe_f(acct0.get("equity"), 0.0),
            _safe_f(acct0.get("cash"), 0.0),
            _safe_f(BROKER_START_EQUITY, 0.0),
            _safe_f(BROKER_START_CASH, 0.0),
            1.0,
        )
        equity = float(fallback_equity)
        _write_account(
            con,
            cash=float(max(_safe_f(acct0.get("cash"), 0.0), _safe_f(BROKER_START_CASH, 0.0))),
            equity=float(equity),
            ts_ms=int(now_ms),
            book_key=book_key,
        )
        _safe_commit_connection(con, context="fallback_write_account", once_key="fallback_write_account_commit")

    base_max_notional_budget = max(0.0, float(equity) * float(BROKER_MAX_TRADE_PCT_EQUITY))

    skew_z = _get_factor_feature_asof(con, "options.skew_25d_z", int(ts_ms))
    flow_z = _get_factor_feature_asof(con, "flows.index_constituent_imbalance_z", int(ts_ms))

    stress_mag = max(
        0.0,
        max(
            abs(float(skew_z)) - float(_EXEC_SKEW_Z_THRESH),
            abs(float(flow_z)) - float(_EXEC_FLOW_Z_THRESH),
        ),
    )

    stress_size_mult = 1.0
    if stress_mag > 0.0:
        stress_size_mult = float(_clamp(1.0 - (stress_mag * float(_EXEC_STRESS_SIZE_MAX_REDUCTION)), 0.20, 1.0))

    stress_slip_add_bps = 0.0
    if stress_mag > 0.0:
        stress_slip_add_bps = float(_clamp(stress_mag * float(_EXEC_STRESS_SLIP_ADD_BPS), 0.0, 5.0))

    stress_latency_mult = 1.0
    if abs(float(flow_z)) > float(_EXEC_FLOW_Z_THRESH):
        stress_latency_mult = float(
            _clamp(
                1.0 + 0.25 * (abs(float(flow_z)) - float(_EXEC_FLOW_Z_THRESH)),
                1.0,
                float(_EXEC_STRESS_LATENCY_MULT_MAX),
            )
        )

    max_notional_budget = max(0.0, float(base_max_notional_budget) * float(stress_size_mult))

    return {
        "acct": acct,
        "cash": float(cash),
        "equity": float(equity),
        "position_qty_map": position_qty_map,
        "position_price_map": position_price_map,
        "base_max_notional_budget": float(base_max_notional_budget),
        "max_notional_budget": float(max_notional_budget),
        "chunk_cap_notional": max(1e-9, float(max_notional_budget) * float(BROKER_CHUNK_PCT or 0.33)),
        "skew_z": float(skew_z),
        "flow_z": float(flow_z),
        "stress_size_mult": float(stress_size_mult),
        "stress_slip_add_bps": float(stress_slip_add_bps),
        "stress_latency_mult": float(stress_latency_mult),
    }


def _broker_sim_phase_log_ledger_effects(
    con,
    *,
    order: Dict[str, Any],
    symbol: str,
    delta: float,
    px_mid: float,
    ts_ms: int,
    order_id: Optional[int],
    order_uid: str,
    client_order_id: str,
    risk_cap_audit: Dict[str, Any],
    book_key: Optional[str],
) -> None:
    if _is_shadow_book(book_key):
        return

    try:
        from engine.execution.execution_ledger import log_submit

        _extra = dict(order or {})
        try:
            ex = _extra.get("explain") or {}
            if isinstance(ex, dict):
                strat = (ex.get("strategy") or {}) if isinstance(ex.get("strategy"), dict) else {}
                if strat.get("name"):
                    _extra["strategy_name"] = str(strat.get("name"))
        except Exception as e:
            _warn_nonfatal(
                "broker_sim_strategy_name_extract_failed",
                "BROKER_SIM_STRATEGY_NAME_EXTRACT_FAILED",
                e,
                warn_key=f"broker_sim_strategy_name_extract_failed:{symbol}",
                symbol=str(symbol),
            )

        _extra["order_uid"] = str(order_uid)
        _extra["idempotency_status"] = "submitted"
        _extra["portfolio_risk_caps"] = dict(risk_cap_audit or {})

        submit_ts_ms = int(ts_ms)
        if not bool(getattr(con, "in_transaction", False)):
            _begin_managed_write(con)

        log_submit(
            client_order_id=str(client_order_id),
            broker="sim",
            symbol=str(symbol),
            qty=float(delta),
            submit_ts_ms=int(submit_ts_ms),
            ref_px=float(px_mid),
            broker_order_id=None,
            portfolio_orders_id=(int(order_id) if order_id is not None else None),
            source_alert_id=(int(order.get("source_alert_id")) if order.get("source_alert_id") is not None else None),
            extra=_extra,
            order_uid=str(order_uid),
            idempotency_status="submitted",
            con=con,
        )

        mark_order_submission_submitted(
            con=con,
            order_uid=str(order_uid),
            client_order_id=str(client_order_id),
            broker_order_id=None,
            submit_ts_ms=int(submit_ts_ms),
        )
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_execution_ledger_submit_failed",
            "BROKER_SIM_EXECUTION_LEDGER_SUBMIT_FAILED",
            e,
            warn_key=f"broker_sim_execution_ledger_submit_failed:{symbol}:{order_id}",
            symbol=str(symbol),
            order_id=(int(order_id) if order_id is not None else None),
        )


def _broker_sim_phase_cleanup_duplicate_pending_order_state(
    con,
    *,
    order: Dict[str, Any],
    symbol: str,
    book_key: Optional[str],
) -> None:
    source_order_id = int(order.get("source_order_id") or 0)
    try:
        if _is_shadow_book(book_key):
            con.execute(
                """
                DELETE FROM broker_shadow_order_state
                WHERE id IN (
                  SELECT id
                  FROM broker_shadow_order_state
                  WHERE book_key=? AND source_order_id=? AND symbol=? AND state='PENDING'
                  ORDER BY id DESC
                  LIMIT 1
                )
                """,
                (str(_book_key(book_key)), int(source_order_id), str(symbol)),
            )
            return
        con.execute(
            """
            DELETE FROM broker_order_state
            WHERE id IN (
              SELECT id
              FROM broker_order_state
              WHERE source_order_id=? AND symbol=? AND state='PENDING'
              ORDER BY id DESC
              LIMIT 1
            )
            """,
            (int(source_order_id), str(symbol)),
        )
    except Exception as e:
        _warn_nonfatal(
            "broker_sim_duplicate_pending_order_state_cleanup_failed",
            "BROKER_SIM_DUPLICATE_PENDING_ORDER_STATE_CLEANUP_FAILED",
            e,
            warn_key=f"duplicate_pending_order_state_cleanup:{symbol}:{source_order_id}",
            symbol=str(symbol),
            source_order_id=int(source_order_id),
            book_key=str(_book_key(book_key) or ""),
        )


def _broker_sim_phase_persist_fill_effects(
    con,
    *,
    order: Dict[str, Any],
    symbol: str,
    order_id: Optional[int],
    ts_ms: int,
    fill_ts: int,
    book_key: Optional[str],
    qty_cap: float,
    px_mid_use: float,
    px_exec: float,
    new_qty: float,
    new_avg: float,
    fee: float,
    notional: float,
    explain: Dict[str, Any],
    exec_spread_bps: float,
    chunk_lob_slip_bps: float,
    lob_adverse_bps: float,
    lob_impact_bps: float,
    almgren_chriss: Dict[str, Any],
    client_order_id: str,
    order_ttl_ms: int,
    state_meta: Dict[str, Any],
    contract_multiplier: Optional[float] = None,
    option_quote_source: Optional[str] = None,
    option_margin_debit: Optional[float] = None,
) -> bool:
    fill_inserted = _write_fill(
        con,
        ts_ms=int(fill_ts),
        source_order_id=(int(order.get("source_order_id")) if order.get("source_order_id") is not None else order_id),
        symbol=symbol,
        qty=float(qty_cap),
        px=float(px_exec),
        fees=float(fee),
        note=(
            f"spread_bps={float(exec_spread_bps):.4f} "
            f"slippage_bps={float(chunk_lob_slip_bps):.4f} "
            f"lob_adv_bps={float(lob_adverse_bps):.4f} "
            f"lob_impact_bps={float(lob_impact_bps):.4f} "
            f"ac_bps={float(almgren_chriss.get('execution_cost_bps') or 0.0):.4f} "
            f"fee_bps={BROKER_FEE_BPS}"
            + (" option_sim_margin_reference" if option_margin_debit is not None else "")
        ),
        explain_json=json.dumps(explain),
        client_order_id=client_order_id,
        book_key=book_key,
        source=("shadow" if _is_shadow_book(book_key) else "sim"),
        contract_multiplier=contract_multiplier,
        option_quote_source=option_quote_source,
        option_margin_debit=option_margin_debit,
    )
    if not fill_inserted:
        _broker_sim_phase_cleanup_duplicate_pending_order_state(
            con,
            order=order,
            symbol=symbol,
            book_key=book_key,
        )
        return False

    _write_position(con, symbol, qty=float(new_qty), avg_px=float(new_avg), ts_ms=int(fill_ts), book_key=book_key)

    if not _is_shadow_book(book_key):
        try:
            label_extra = dict(explain)
            label_extra["placeholder_exec_label"] = True
            label_extra["placeholder_reason"] = "entry_fill_only"
            con.execute(
                """
                INSERT OR REPLACE INTO labels_exec (
                  event_id,
                  symbol,
                  horizon_s,
                  ts_ms,
                  source,
                  realized,
                  side,
                  gross_ret,
                  net_ret,
                  mid_in,
                  mid_out,
                  spread_in,
                  fees_bps,
                  slippage_bps,
                  spread_bps,
                  total_cost_bps,
                  extra_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(order.get("event_id") or 0),
                    symbol,
                    int(order.get("horizon_s") or 0),
                    int(fill_ts),
                    "broker_sim_placeholder",
                    0,
                    1 if qty_cap > 0 else -1,
                    0.0,
                    0.0,
                    float(px_mid_use),
                    float(px_exec),
                    float(exec_spread_bps),
                    float(BROKER_FEE_BPS),
                    float(chunk_lob_slip_bps),
                    float(exec_spread_bps),
                    float(
                        BROKER_FEE_BPS
                        + float(chunk_lob_slip_bps)
                        + float(exec_spread_bps)
                        + float(almgren_chriss.get("execution_cost_bps") or 0.0)
                    ),
                    json.dumps(label_extra, separators=(",", ":"), sort_keys=True),
                ),
            )
        except Exception as e:
            _warn_nonfatal(
                "broker_sim_placeholder_exec_label_write_failed",
                "BROKER_SIM_PLACEHOLDER_EXEC_LABEL_WRITE_FAILED",
                e,
                warn_key=f"broker_sim_placeholder_exec_label_write_failed:{symbol}",
                symbol=str(symbol),
                source_order_id=int(order.get("source_order_id") or 0),
            )

    if _is_shadow_book(book_key):
        con.execute(
            """
            UPDATE broker_shadow_order_state
            SET state=?, updated_ts_ms=?
            WHERE book_key=? AND source_order_id=? AND symbol=? AND state='PENDING'
            """,
            ("FILLED", _now_ms(), str(_book_key(book_key)), int(order.get("source_order_id") or 0), symbol),
        )
        return True

    filled_ts_ms = _now_ms()
    con.execute(
        """
        UPDATE broker_order_state
        SET state=?, updated_ts_ms=?
        WHERE source_order_id=? AND symbol=? AND state='PENDING'
        """,
        ("FILLED", filled_ts_ms, int(order.get("source_order_id") or 0), symbol),
    )
    _prime_broker_order_state_after_commit(
        con,
        source_order_id=int(order.get("source_order_id") or 0),
        symbol=str(symbol),
        state="FILLED",
        created_ts_ms=int(ts_ms),
        updated_ts_ms=int(filled_ts_ms),
        ttl_ms=(int(order_ttl_ms) if order_ttl_ms is not None else None),
        meta=state_meta,
    )
    return True


def _broker_sim_phase_persist_account_positions(
    con,
    *,
    cash: float,
    now_ms: int,
    book_key: Optional[str],
) -> Dict[str, Any]:
    cash = _safe_f(cash, 0.0)

    if _broker_account_uses_singleton_id(con):
        if _is_shadow_book(book_key):
            _write_account(
                con,
                float(cash),
                float(_read_account(con, book_key=book_key).get("equity", 1.0)),
                int(now_ms),
                book_key=book_key,
            )
        else:
            con.execute(
                "UPDATE broker_account SET cash=?, updated_ts_ms=? WHERE id=1",
                (float(cash), int(now_ms)),
            )
    else:
        acct_now = _read_account(con, book_key=book_key)
        _write_account(
            con,
            float(cash),
            float(acct_now.get("equity", 1.0)),
            int(now_ms),
            book_key=book_key,
        )

    return _mark_to_market(con, int(now_ms), book_key=book_key, persist_con=con)


def _broker_sim_phase_return_summary(
    *,
    book_key: Optional[str],
    order_id: Optional[int],
    wrote_fills: bool,
    fills_written: int,
    fills_deduped: int,
    account: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "ok": True,
        "broker": "sim",
        "book_key": _book_key(book_key),
        "status": "applied" if wrote_fills else "no_changes",
        "order_id": (int(order_id) if order_id is not None else None),
        "fills_written": int(fills_written),
        "fills_deduped": int(fills_deduped),
        "account": account,
    }


def apply_new_portfolio_orders(
    max_rows: int = 500,
    dry_run: bool = False,
    override_orders: Optional[List[dict]] = None,
    override_order_id: Optional[int] = None,
    override_ts_ms: Optional[int] = None,
    book_key: Optional[str] = None,
    cost_model: Optional[Any] = None,
) -> dict:
    """

    Apply latest portfolio_orders (targets) into broker_positions with realism:
      - spread + slippage via _exec_px
      - fees via _fee
      - max notional cap per apply pass (equity * BROKER_MAX_TRADE_PCT_EQUITY)
      - chunking with latency timestamps
      - cash debits/credits and mark-to-market equity
    """
    init_broker_db()
    con = connect()
    try:
        now_ms = _now_ms()
        # Phase 1: load orders.
        loaded = _broker_sim_phase_load_orders(
            con,
            override_orders=override_orders,
            override_order_id=override_order_id,
            override_ts_ms=override_ts_ms,
            now_ms=int(now_ms),
        )
        orders = list(loaded.get("orders") or [])
        order_id = loaded.get("order_id")
        ts_ms = int(loaded.get("ts_ms") or now_ms)
        ale_meta = {"ok": True, "note": "ale_applied_upstream_or_ttl_guard_local"}

        # Phase 2: validate/gate batch-level execution.
        gate = _broker_sim_phase_validate_gate(
            con,
            loaded=loaded,
            dry_run=bool(dry_run),
            now_ms=int(now_ms),
            book_key=book_key,
            ale_meta=ale_meta,
        )
        if not bool(gate.get("continue")):
            return dict(gate.get("summary") or {})

        orders = list(gate.get("orders") or [])
        # Phase 3: size/cap using current account, position, and stress context.
        sizing = _broker_sim_phase_size_cap(
            con,
            ts_ms=int(ts_ms),
            now_ms=int(now_ms),
            book_key=book_key,
        )
        cash = float(sizing.get("cash") or 0.0)
        equity = float(sizing.get("equity") or 0.0)
        position_qty_map = dict(sizing.get("position_qty_map") or {})
        position_price_map = dict(sizing.get("position_price_map") or {})
        skew_z = float(sizing.get("skew_z") or 0.0)
        flow_z = float(sizing.get("flow_z") or 0.0)
        stress_size_mult = float(sizing.get("stress_size_mult") or 1.0)
        stress_slip_add_bps = float(sizing.get("stress_slip_add_bps") or 0.0)
        stress_latency_mult = float(sizing.get("stress_latency_mult") or 1.0)
        max_notional_budget = float(sizing.get("max_notional_budget") or 0.0)
        chunk_cap_notional = float(sizing.get("chunk_cap_notional") or 1e-9)

        wrote_fills = False
        fills_written = 0
        fills_deduped = 0
        submitted_count = min(len(orders or []), int(max_rows))
        share_rounding_skipped: List[Dict[str, Any]] = []

        _begin_managed_write(con)

        # Phase 4: simulate fills. Per-fill persistence and ledger effects are
        # delegated to named phase helpers so the write path stays auditable.
        for o in (orders or [])[: int(max_rows)]:
            symbol = str(o.get("symbol") or "").strip()
            order_ttl_ms = int(o.get("alpha_ttl_ms") or 0)
            if not symbol:
                continue

            # Kill switch (global/symbol) is enforced here as a last line of defense
            try:
                from engine.execution.kill_switch import execution_allowed

                allow, _, _ = execution_allowed(con=con, symbol=symbol, regime=None)
                if not allow:
                    continue
            except Exception as e:
                _warn_nonfatal(
                    "broker_sim_kill_switch_enforcement_failed",
                    "BROKER_SIM_KILL_SWITCH_ENFORCEMENT_FAILED",
                    e,
                    warn_key=f"kill_switch_enforcement:{symbol}",
                    symbol=str(symbol),
                )
                continue

            # ------------------------------------------------------------
            # PHASE 4: EPE policy extraction + regime-adaptive microstructure
            # ------------------------------------------------------------
            _epe_ov_raw = o.get("epe_broker_sim_overrides")
            _epe_ov = dict(_epe_ov_raw) if isinstance(_epe_ov_raw, dict) else {}

            # base knobs (may be overridden per order)
            try:
                _lat_ms = _safe_i(_epe_ov.get("latency_ms")) if _epe_ov.get("latency_ms") is not None else None
            except Exception:
                _lat_ms = None
            try:
                _chunk_pct = _safe_f(_epe_ov.get("chunk_pct")) if _epe_ov.get("chunk_pct") is not None else None
            except Exception:
                _chunk_pct = None
            try:
                _extra_slip = _safe_f(_epe_ov.get("extra_slippage_bps")) if _epe_ov.get("extra_slippage_bps") is not None else 0.0
            except Exception:
                _extra_slip = 0.0

            # EPE policy fields (optional)
            order_type = str(
                o.get("order_type")
                or o.get("epe_order_type")
                or "MARKET"
            ).upper().strip()

            aggressiveness = str(
                o.get("aggressiveness")
                or o.get("epe_aggressiveness")
                or "NEUTRAL"
            ).upper().strip()

            try:
                max_reprice_attempts = int(o.get("max_reprice_attempts") or o.get("epe_max_reprice_attempts") or 0)
            except Exception:
                max_reprice_attempts = 0

            # regime/volatility hints (optional)
            regime = str(o.get("regime") or o.get("epe_regime") or "").upper().strip()
            try:
                volatility = float(o.get("volatility") or o.get("epe_volatility") or 0.0)
            except Exception:
                volatility = 0.0

            # base locals
            local_latency_ms = int(_lat_ms) if (_lat_ms is not None and int(_lat_ms) > 0) else int(BROKER_LATENCY_MS)
            local_chunk_pct = float(_chunk_pct) if (_chunk_pct is not None and 0.01 <= float(_chunk_pct) <= 1.0) else float(BROKER_CHUNK_PCT)

            # global stress conditioning (execution-only)
            try:
                local_latency_ms = int(max(1, int(float(local_latency_ms) * float(stress_latency_mult))))
            except Exception as e:
                _warn_nonfatal(
                    "broker_sim_stress_latency_adjust_failed",
                    "BROKER_SIM_STRESS_LATENCY_ADJUST_FAILED",
                    e,
                    warn_key="broker_sim_stress_latency_adjust_failed",
                    symbol=str(symbol),
                    stress_latency_mult=float(stress_latency_mult),
                )
            try:
                if abs(float(skew_z)) > float(_EXEC_SKEW_Z_THRESH) or abs(float(flow_z)) > float(_EXEC_FLOW_Z_THRESH):
                    local_chunk_pct = float(_clamp(local_chunk_pct * 0.85, 0.05, 1.0))
            except Exception as e:
                _warn_nonfatal(
                    "broker_sim_stress_chunk_adjust_failed",
                    "BROKER_SIM_STRESS_CHUNK_ADJUST_FAILED",
                    e,
                    warn_key="broker_sim_stress_chunk_adjust_failed",
                    symbol=str(symbol),
                    skew_z=float(skew_z),
                    flow_z=float(flow_z),
                )

            # regime-adaptive tweaks (deterministic; auditable)
            # - higher vol => smaller chunks + more latency (slower fill) + more slippage
            # - "ILLQ"/"LOW_LIQ"/"WIDE" => more slippage + smaller chunks
            vol = max(0.0, float(volatility))
            if vol >= 0.03:
                local_chunk_pct = float(_clamp(local_chunk_pct * 0.60, 0.05, 1.0))
                local_latency_ms = int(max(local_latency_ms, int(BROKER_LATENCY_MS * 2)))
                _extra_slip = float(_extra_slip) + 0.50
            elif vol >= 0.015:
                local_chunk_pct = float(_clamp(local_chunk_pct * 0.80, 0.05, 1.0))
                _extra_slip = float(_extra_slip) + 0.25

            if regime in ("ILLQ", "LOW_LIQ", "WIDE", "WIDE_SPREAD", "THIN"):
                local_chunk_pct = float(_clamp(local_chunk_pct * 0.70, 0.05, 1.0))
                _extra_slip = float(_extra_slip) + 0.75

            # aggressiveness affects effective slippage (more aggressive => more slippage)
            aggr_slip_bps = 0.0
            if aggressiveness == "PASSIVE":
                aggr_slip_bps = -0.25
            elif aggressiveness == "AGGRESSIVE":
                aggr_slip_bps = 0.50

            local_slip_bps = float(BROKER_SLIPPAGE_BPS) + float(_extra_slip) + float(aggr_slip_bps) + float(stress_slip_add_bps)

            # track limit reprice attempts across chunks
            attempts_left = int(max(0, max_reprice_attempts))
            order_type_eff = str(order_type)


            to_side = str(o.get("to_side") or "FLAT").upper()

            raw_qty = _safe_f(o.get("qty"), 0.0)
            has_explicit_qty = _is_finite(raw_qty) and abs(float(raw_qty)) > 0.0

            to_w = _safe_f(o.get("to_weight"), 0.0)
            if (not has_explicit_qty) and (not _is_finite(to_w)):
                continue

            option_meta = _option_contract_meta(str(symbol))
            option_contract = _option_contract_key(option_meta) if option_meta is not None else None
            option_multiplier = _option_multiplier(option_meta) if option_meta is not None else None
            option_bid = None
            option_ask = None
            option_quote_ts = None
            option_underlying_px = None
            option_quote_source = None
            option_exec_spread_bps = None
            if option_meta is not None:
                if option_multiplier is None or not option_contract:
                    continue
                option_bid, option_ask, px_mid, option_quote_ts = _get_option_quote_at_or_before(con, option_contract, ts_ms)
                if px_mid is None or float(px_mid) <= 0.0:
                    continue
                option_underlying_px = _option_underlying_px(con, option_meta, int(ts_ms))
                option_quote_source = f"options_chain_v2:{option_contract}:{int(option_quote_ts)}"
                option_exec_spread_bps = _option_spread_bps(float(option_bid), float(option_ask), float(px_mid))
                sizing_px = float(px_mid) * float(option_multiplier)
            else:
                px_mid, _ = _get_price_at_or_before(con, symbol, ts_ms)
                if px_mid is None or float(px_mid) <= 0.0:
                    continue
                sizing_px = float(px_mid)

            cur_qty, cur_avg = _read_position(con, symbol, book_key=book_key)

            share_rounding_audit: Optional[Dict[str, Any]] = None
            if has_explicit_qty:
                delta = float(round(raw_qty)) if option_meta is not None else float(raw_qty)
                target_qty = float(cur_qty) + float(delta)
            else:
                if option_meta is not None:
                    target_qty = (to_w * equity) / float(sizing_px)
                else:
                    # NO-GO-pending-owner: FX weight-to-lots conversion is deliberately
                    # unowned here. This generic seam remains target_weight * equity / px.
                    target_qty = (to_w * equity) / float(px_mid)
                if to_side == "SHORT":
                    target_qty = -abs(target_qty)
                elif to_side == "LONG":
                    target_qty = abs(target_qty)
                else:
                    target_qty = 0.0
                if option_meta is not None:
                    target_qty = float(round(target_qty))
                delta = float(target_qty) - float(cur_qty)

            if abs(delta) < 1e-9:
                if (
                    share_rounding_audit is not None
                    and bool(share_rounding_audit.get("enabled"))
                    and bool(share_rounding_audit.get("eligible"))
                    and bool(share_rounding_audit.get("changed"))
                ):
                    share_rounding_skipped.append(
                        {
                            "symbol": str(symbol),
                            "reason": str(share_rounding_audit.get("reason") or "share_rounding_zero_delta"),
                            "share_rounding": dict(share_rounding_audit),
                        }
                    )
                continue

            delta, risk_cap_audit = _apply_execution_risk_caps(
                positions=position_qty_map,
                price_map=position_price_map,
                symbol=symbol,
                current_qty=cur_qty,
                delta_qty=delta,
                px=float(sizing_px),
                equity=float(equity),
            )
            if option_meta is None:
                # EQ-07 owns equity share rounding only. Mirror the live adapters
                # by rounding the broker order delta, not the absolute target.
                # FX weight-to-lots conversion remains the unowned FX-06 seam and
                # passes through via round_equity_qty's asset-class guard.
                delta, share_rounding_audit = round_equity_qty(
                    float(delta),
                    float(sizing_px),
                    broker="sim",
                    asset_class=_share_rounding_asset_class(symbol),
                )
            if abs(delta) < 1e-9:
                position_qty_map[str(symbol).upper().strip()] = float(cur_qty)
                if (
                    share_rounding_audit is not None
                    and bool(share_rounding_audit.get("enabled"))
                    and bool(share_rounding_audit.get("eligible"))
                    and bool(share_rounding_audit.get("changed"))
                ):
                    share_rounding_skipped.append(
                        {
                            "symbol": str(symbol),
                            "reason": str(share_rounding_audit.get("reason") or "share_rounding_zero_delta"),
                            "share_rounding": dict(share_rounding_audit),
                        }
                    )
                continue

            client_order_id = None
            order_uid = ""
            try:
                guard = claim_order_submission(
                    con=con,
                    broker="sim",
                    portfolio_orders_id=(int(order_id) if order_id is not None else None),
                    portfolio_ts_ms=int(ts_ms),
                    order=o,
                )
            except Exception as e:
                _warn_nonfatal(
                    "broker_sim_order_idempotency_claim_failed",
                    "BROKER_SIM_ORDER_IDEMPOTENCY_CLAIM_FAILED",
                    e,
                    warn_key=f"broker_sim_order_idempotency_claim_failed:{symbol}:{order_id}",
                    symbol=str(symbol),
                    order_id=(int(order_id) if order_id is not None else None),
                )
                continue

            if not bool(guard.get("ok")):
                return {
                    "ok": False,
                    "status": str(guard.get("status") or "order_idempotency_claim_failed"),
                    "broker": "sim",
                    "stop_failover": True,
                    "detail": "order_idempotency_claim_failed",
                    "order_uid": str(guard.get("order_uid") or ""),
                    "client_order_id": str(guard.get("client_order_id") or ""),
                    "symbol": str(symbol),
                    "submitted_n": int(submitted_count),
                }
            if bool(guard.get("duplicate")):
                continue

            client_order_id = str(guard.get("client_order_id") or "")
            order_uid = str(guard.get("order_uid") or "")
            record_share_rounding = bool(
                share_rounding_audit is not None
                and share_rounding_audit.get("enabled")
                and share_rounding_audit.get("eligible")
            )
            order_for_audit = dict(o or {})
            if record_share_rounding and share_rounding_audit is not None:
                order_for_audit["share_rounding"] = dict(share_rounding_audit)
            state_meta = dict(order_for_audit or {})
            state_meta["order_uid"] = str(order_uid)
            state_meta["client_order_id"] = str(client_order_id)

            if _is_shadow_book(book_key):
                con.execute(
                    """
                    INSERT INTO broker_shadow_order_state(
                        book_key, source_order_id, symbol, state, created_ts_ms, updated_ts_ms, ttl_ms, meta_json
                    )
                    VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        str(_book_key(book_key)),
                        int(o.get("source_order_id") or 0),
                        symbol,
                        "PENDING",
                        int(ts_ms),
                        int(ts_ms),
                        order_ttl_ms,
                        json.dumps(state_meta),
                    ),
                )
            else:
                con.execute(
                    """
                    INSERT INTO broker_order_state(
                        source_order_id, symbol, state, created_ts_ms, updated_ts_ms, ttl_ms, meta_json
                    )
                    VALUES(?,?,?,?,?,?,?)
                    """,
                    (
                        int(o.get("source_order_id") or 0),
                        symbol,
                        "PENDING",
                        int(ts_ms),
                        int(ts_ms),
                        order_ttl_ms,
                        json.dumps(state_meta),
                    ),
                )
                _prime_broker_order_state_after_commit(
                    con,
                    source_order_id=int(o.get("source_order_id") or 0),
                    symbol=str(symbol),
                    state="PENDING",
                    created_ts_ms=int(ts_ms),
                    updated_ts_ms=int(ts_ms),
                    ttl_ms=(int(order_ttl_ms) if order_ttl_ms is not None else None),
                    meta=state_meta,
                )

            _broker_sim_phase_log_ledger_effects(
                con,
                order=order_for_audit,
                symbol=str(symbol),
                delta=float(delta),
                px_mid=float(px_mid),
                ts_ms=int(ts_ms),
                order_id=(int(order_id) if order_id is not None else None),
                order_uid=str(order_uid),
                client_order_id=str(client_order_id),
                risk_cap_audit=dict(risk_cap_audit or {}),
                book_key=book_key,
            )

            remaining = float(delta)
            position_qty_map[str(symbol).upper().strip()] = float(cur_qty) + float(delta)
            position_price_map[str(symbol).upper().strip()] = float(sizing_px)
            chunk_idx = 0

            # per-order chunk cap (regime/vol adjusted) + earnings proximity conditioning
            earnings_decay = _earnings_proximity_decay(con, symbol, int(ts_ms))
            # size reduction near earnings (bounded)
            earnings_size_mult = float(_clamp(1.0 - (float(earnings_decay) * float(_EXEC_EARNINGS_SIZE_MAX_REDUCTION)), 0.20, 1.0))
            local_max_notional_budget = max(0.0, float(max_notional_budget) * float(earnings_size_mult))
            chunk_cap_notional = max(1e-9, float(local_max_notional_budget) * float(local_chunk_pct or 0.33))

            # slippage add near earnings (execution-only)
            local_slip_bps = float(local_slip_bps) + float(_clamp(float(earnings_decay) * float(_EXEC_EARNINGS_SLIP_ADD_BPS), 0.0, 5.0))

            while abs(remaining) > 1e-9:

                # TTL enforcement
                if order_ttl_ms and (_now_ms() - ts_ms) > order_ttl_ms:
                    if _is_shadow_book(book_key):
                        con.execute(
                            """
                            UPDATE broker_shadow_order_state
                            SET state=?, updated_ts_ms=?
                            WHERE book_key=? AND source_order_id=? AND symbol=? AND state='PENDING'
                            """,
                            ("EXPIRED", _now_ms(), str(_book_key(book_key)), int(o.get("source_order_id") or 0), symbol),
                        )
                    else:
                        expired_ts_ms = _now_ms()
                        con.execute(
                            """
                            UPDATE broker_order_state
                            SET state=?, updated_ts_ms=?
                            WHERE source_order_id=? AND symbol=? AND state='PENDING'
                            """,
                            ("EXPIRED", expired_ts_ms, int(o.get("source_order_id") or 0), symbol),
                        )
                        _prime_broker_order_state_after_commit(
                            con,
                            source_order_id=int(o.get("source_order_id") or 0),
                            symbol=str(symbol),
                            state="EXPIRED",
                            created_ts_ms=int(ts_ms),
                            updated_ts_ms=int(expired_ts_ms),
                            ttl_ms=(int(order_ttl_ms) if order_ttl_ms is not None else None),
                            meta=state_meta,
                        )
                    break
                if max_notional_budget <= 0.0 or local_max_notional_budget <= 0.0:
                    break

                chunk_side = "BUY" if remaining > 0 else "SELL"

                # Use price at this chunk's simulated fill time (latency-aware), not the parent ts_ms
                fill_ts = int(int(ts_ms) + (int(chunk_idx) * int(local_latency_ms)))

                if option_meta is not None:
                    option_bid, option_ask, px_mid_chunk, option_quote_ts = _get_option_quote_at_or_before(
                        con,
                        str(option_contract),
                        int(fill_ts),
                    )
                    if px_mid_chunk is None or float(px_mid_chunk) <= 0.0:
                        break
                    px_mid_use = float(px_mid_chunk)
                    option_quote_source = f"options_chain_v2:{option_contract}:{int(option_quote_ts)}"
                    option_exec_spread_bps = _option_spread_bps(float(option_bid), float(option_ask), float(px_mid_use))
                    if option_underlying_px is None:
                        option_underlying_px = _option_underlying_px(con, option_meta, int(fill_ts))
                else:
                    px_mid_chunk, _ = _get_price_at_or_before(con, symbol, int(fill_ts))
                    px_mid_use = px_mid_chunk if (px_mid_chunk is not None and float(px_mid_chunk) > 0.0) else px_mid

                # ------------------------------------------------------------
                # PHASE 4: Order type + aggressiveness shaping (MARKET vs LIMIT)
                # - MARKET: uses _exec_px with (possibly adjusted) slippage
                # - LIMIT: improved price but partial fills; cancel/replace escalates to MARKET
                # ------------------------------------------------------------
                px_exec = 0.0
                trade_multiplier = float(option_multiplier) if option_multiplier is not None else 1.0

                # provisional px for sizing (MARKET-like baseline)
                px_mkt = _exec_px(
                    px_mid_use,
                    chunk_side,
                    trade_notional=0.0,
                    equity=equity,
                    slip_bps_override=float(local_slip_bps),
                    spread_bps_override=(float(option_exec_spread_bps) if option_exec_spread_bps is not None else None),
                )
                if px_mkt <= 0.0:
                    break

                # cap by remaining and notional budget using provisional px
                effective_budget = float(min(float(local_max_notional_budget), float(max_notional_budget)))

                remaining_notional = abs(remaining) * px_mkt * float(trade_multiplier)
                if remaining_notional > effective_budget:
                    qty_cap = (effective_budget / (px_mkt * float(trade_multiplier))) * (1.0 if remaining > 0 else -1.0)
                else:
                    qty_cap = remaining

                # chunk cap using provisional px
                if abs(qty_cap) * px_mkt * float(trade_multiplier) > chunk_cap_notional:
                    qty_cap = (chunk_cap_notional / (px_mkt * float(trade_multiplier))) * (1.0 if remaining > 0 else -1.0)

                if option_meta is not None:
                    qty_cap = float(round(qty_cap))
                    if abs(qty_cap) > abs(remaining):
                        qty_cap = float(remaining)

                if abs(qty_cap) < 1e-9:
                    break

                liquidity_snapshot: Dict[str, Any] = {}
                exec_spread_bps = float(BROKER_SPREAD_BPS)
                try:
                    liquidity_snapshot = dict(
                        get_execution_liquidity_snapshot(
                            symbol=str(symbol),
                            qty=float(abs(qty_cap)),
                            px=float(px_mid_use),
                            ts_ms=int(fill_ts),
                        )
                        or {}
                    )
                    exec_spread_bps = max(
                        0.0,
                        _safe_f(liquidity_snapshot.get("true_spread_bps"), float(BROKER_SPREAD_BPS)),
                    )
                except Exception as e:
                    liquidity_snapshot = {}
                    exec_spread_bps = float(BROKER_SPREAD_BPS)
                    _warn_nonfatal(
                        "broker_sim_liquidity_snapshot_failed",
                        "BROKER_SIM_LIQUIDITY_SNAPSHOT_FAILED",
                        e,
                        warn_key=f"broker_sim_liquidity_snapshot_failed:{symbol}",
                        symbol=str(symbol),
                    )
                if option_meta is not None and option_exec_spread_bps is not None:
                    exec_spread_bps = float(option_exec_spread_bps)

                lob_simulation = build_reactive_lob_simulation(
                    con,
                    symbol=str(symbol),
                    side=str(chunk_side),
                    qty=float(abs(qty_cap)),
                    mid_px=float(px_mid_use),
                    order_type=str(order_type_eff),
                    aggressiveness=str(aggressiveness),
                    ts_ms=int(fill_ts),
                    latency_ms=int(local_latency_ms),
                    liquidity_snapshot=liquidity_snapshot,
                )
                lob_applied = bool(lob_simulation.get("applied"))
                lob_adverse_bps = (
                    max(0.0, _safe_f(lob_simulation.get("adverse_selection_bps"), 0.0))
                    if lob_applied
                    else 0.0
                )
                lob_impact_bps = (
                    max(0.0, _safe_f(lob_simulation.get("market_impact_bps"), 0.0))
                    if lob_applied
                    else 0.0
                )
                lob_sweep_bps = (
                    max(0.0, _safe_f(lob_simulation.get("sweep_bps"), 0.0))
                    if lob_applied
                    else 0.0
                )
                chunk_lob_slip_bps = float(local_slip_bps) + float(lob_adverse_bps) + float(lob_impact_bps) + float(lob_sweep_bps)

                # Choose effective order type for this chunk
                if order_type_eff == "LIMIT":
                    # LIMIT improves price relative to market baseline.
                    # PASSIVE => better price, lower fill; AGGRESSIVE => closer to market, higher fill.
                    improve = 0.5
                    if aggressiveness == "PASSIVE":
                        improve = 1.0
                    elif aggressiveness == "AGGRESSIVE":
                        improve = 0.15

                    # allow cancel/replace: each attempt reduces improvement (more aggressive repricing)
                    if attempts_left > 0:
                        step = min(max_reprice_attempts, max(0, max_reprice_attempts - attempts_left))
                        improve = float(_clamp(improve - 0.25 * float(step), 0.0, 1.0))

                    half_spread = (float(exec_spread_bps) / 10000.0) * float(px_mid_use) / 2.0
                    if chunk_side == "BUY":
                        px_exec = max(0.0, float(px_mid_use) - (half_spread * float(improve)))
                    else:
                        px_exec = max(0.0, float(px_mid_use) + (half_spread * float(improve)))

                    # deterministic partial fill model
                    base_fill = 0.70
                    if aggressiveness == "PASSIVE":
                        base_fill = 0.45
                    elif aggressiveness == "AGGRESSIVE":
                        base_fill = 0.95

                    # higher vol / illiq => lower fill
                    fill_penalty = float(_clamp(vol * 6.0, 0.0, 0.50))
                    fill_frac = float(_clamp(base_fill - fill_penalty, 0.20, 1.0))

                    # deterministic per-chunk variation (auditable, reproducible)
                    u = _u01(f"{order_id}|{symbol}|{chunk_idx}|{fill_ts}|{order_type_eff}|{aggressiveness}")
                    jitter = float(_clamp((u - 0.5) * 0.10, -0.05, 0.05))
                    fill_frac = float(_clamp(fill_frac + jitter, 0.20, 1.0))
                    if lob_applied:
                        fill_frac = float(fill_frac) * float(
                            _clamp(_safe_f(lob_simulation.get("fill_probability_mult"), 1.0), 0.05, 1.0)
                        )
                        fill_frac = min(
                            float(fill_frac),
                            float(_clamp(_safe_f(lob_simulation.get("partial_fill_cap"), 1.0), 0.05, 1.0)),
                        )
                        if bool(lob_simulation.get("spread_crossed")):
                            fill_frac = max(float(fill_frac), 0.95)
                        fill_frac = float(_clamp(fill_frac, 0.05, 1.0))

                    # apply partial fill
                    qty_cap = float(qty_cap) * float(fill_frac)
                    if option_meta is not None:
                        qty_cap = float(round(qty_cap))
                        if abs(qty_cap) > abs(remaining):
                            qty_cap = float(remaining)

                    if abs(qty_cap) < 1e-9:
                        # no fill at this price => cancel/replace attempt
                        if attempts_left > 0:
                            attempts_left -= 1
                            chunk_idx += 1
                            # simulate time passing, but keep remaining unchanged
                            continue
                        # escalate remainder to MARKET
                        order_type_eff = "MARKET"
                        continue

                    # if we didn't fill the whole remainder, burn an attempt (cancel/replace)
                    if abs(qty_cap) < abs(remaining) and attempts_left > 0:
                        attempts_left -= 1
                        if attempts_left <= 0:
                            # no more reprices => escalate to MARKET for remaining
                            order_type_eff = "MARKET"

                else:
                    # MARKET path: recompute with size-aware slippage
                    almgren_chriss = _estimate_optional_cost_model(
                        cost_model,
                        symbol=str(symbol),
                        qty=float(abs(qty_cap)),
                        px=float(px_mid_use),
                        side=str(chunk_side),
                        liquidity_snapshot=liquidity_snapshot,
                    )
                    ac_exec_bps = float(almgren_chriss.get("execution_cost_bps") or 0.0)
                    notional_est = float(qty_cap) * float(px_mid_use) * float(trade_multiplier)
                    px_exec = _exec_px(
                        px_mid_use,
                        chunk_side,
                        trade_notional=notional_est,
                        equity=equity,
                        slip_bps_override=float(chunk_lob_slip_bps) + float(ac_exec_bps),
                        spread_bps_override=float(exec_spread_bps),
                    )
                if order_type_eff == "LIMIT":
                    almgren_chriss = _estimate_optional_cost_model(
                        cost_model,
                        symbol=str(symbol),
                        qty=float(abs(qty_cap)),
                        px=float(px_mid_use),
                        side=str(chunk_side),
                        liquidity_snapshot=liquidity_snapshot,
                    )
                    ac_exec_bps = float(almgren_chriss.get("execution_cost_bps") or 0.0)
                    impact_px = _impact_px_from_bps(
                        float(px_mid_use),
                        float(ac_exec_bps) + float(lob_adverse_bps) + float(lob_impact_bps),
                    )
                    if chunk_side == "BUY":
                        px_exec = max(0.0, float(px_exec) + float(impact_px))
                    else:
                        px_exec = max(0.0, float(px_exec) - float(impact_px))

                if px_exec <= 0.0:
                    break

                # execute
                notional = float(qty_cap) * float(px_exec) * float(trade_multiplier)
                fee = _fee(notional)
                option_margin_debit = None

                # Optional no-margin mode: prevent cash from going negative on buys
                if (not BROKER_ALLOW_MARGIN) and (notional > 0.0):
                    max_afford = max(0.0, float(cash) - float(fee))
                    if max_afford <= 0.0:
                        break
                    max_qty = max_afford / (float(px_exec) * float(trade_multiplier))
                    if max_qty < abs(float(qty_cap)):
                        qty_cap = (max_qty * (1.0 if qty_cap > 0 else -1.0))
                        if option_meta is not None:
                            qty_cap = math.floor(abs(float(max_qty))) * (1.0 if qty_cap > 0 else -1.0)
                        if abs(qty_cap) < 1e-9:
                            break
                        notional = float(qty_cap) * float(px_exec) * float(trade_multiplier)
                        fee = _fee(notional)
                if option_meta is not None and qty_cap < 0.0:
                    if option_underlying_px is None:
                        break
                    option_margin_debit = _option_short_margin(
                        option_meta,
                        qty=float(qty_cap),
                        mid=float(px_mid_use),
                        underlying_px=float(option_underlying_px),
                    )
                    if (not BROKER_ALLOW_MARGIN) and option_margin_debit is not None and option_margin_debit > 0.0:
                        per_contract_margin = float(option_margin_debit) / max(1.0, abs(float(qty_cap)))
                        max_afford = max(0.0, float(cash) - float(fee))
                        max_qty = math.floor(max_afford / float(per_contract_margin)) if per_contract_margin > 0.0 else 0
                        if max_qty < abs(float(qty_cap)):
                            qty_cap = -float(max_qty)
                            if abs(qty_cap) < 1e-9:
                                break
                            notional = float(qty_cap) * float(px_exec) * float(trade_multiplier)
                            fee = _fee(notional)
                            option_margin_debit = _option_short_margin(
                                option_meta,
                                qty=float(qty_cap),
                                mid=float(px_mid_use),
                                underlying_px=float(option_underlying_px),
                            )

                # cash: BUY spends (negative), SELL receives (positive). fees always reduce cash.
                cash_after_fill = float(cash) + float(-notional - fee)
                if option_margin_debit is not None:
                    cash_after_fill -= float(option_margin_debit)

                new_qty = float(cur_qty) + float(qty_cap)

                # avg_px update:
                if (
                    float(cur_qty) == 0.0
                    or (float(cur_qty) > 0 and float(qty_cap) > 0)
                    or (float(cur_qty) < 0 and float(qty_cap) < 0)
                ):
                    old_notional_abs = abs(float(cur_qty)) * float(cur_avg)
                    add_notional_abs = abs(float(qty_cap)) * float(px_exec)
                    denom = abs(float(cur_qty)) + abs(float(qty_cap))
                    new_avg = (old_notional_abs + add_notional_abs) / denom if denom > 1e-12 else float(px_exec)
                else:
                    if (float(cur_qty) > 0 and new_qty > 0) or (float(cur_qty) < 0 and new_qty < 0):
                        new_avg = float(cur_avg)
                    elif abs(new_qty) < 1e-12:
                        new_avg = 0.0
                    else:
                        new_avg = float(px_exec)

                fill_ts = int(fill_ts)

                explain = {
                    "mid_px": float(px_mid_use),
                    "exec_px": float(px_exec),
                    "side": str(chunk_side),

                    "order_type": str(order_type_eff),
                    "aggressiveness": str(aggressiveness),
                    "regime": str(regime),
                    "market_regime": str(o.get("market_regime") or o.get("market_regime_label") or "mean_reversion"),
                    "market_regime_snapshot": (
                        dict(o.get("market_regime_snapshot") or {})
                        if isinstance(o.get("market_regime_snapshot"), dict)
                        else None
                    ),
                    "volatility": float(vol),

                    # --- execution ROI conditioning ---
                    "exec_stress": {
                        "skew_z": float(skew_z),
                        "flow_z": float(flow_z),
                        "stress_size_mult": float(stress_size_mult),
                        "stress_slip_add_bps": float(stress_slip_add_bps),
                        "stress_latency_mult": float(stress_latency_mult),
                        "earnings_decay": float(earnings_decay),
                    },

                    "spread_bps": float(exec_spread_bps),
                    "slippage_bps": float(chunk_lob_slip_bps),
                    "base_slippage_bps": float(local_slip_bps),
                    "lob_simulation": dict(lob_simulation or {}),
                    "almgren_chriss": dict(almgren_chriss or {}),
                    "almgren_chriss_execution_cost_bps": float(almgren_chriss.get("execution_cost_bps") or 0.0),
                    "expected_price": float(px_mid_use),
                    "fill_price": float(px_exec),
                    "slippage": float(px_exec) - float(px_mid_use),
                    "impact_alpha": float(BROKER_IMPACT_ALPHA),
                    "fee_bps": float(BROKER_FEE_BPS),

                    "qty": float(qty_cap),
                    "notional": float(notional),
                    "fee": float(fee),
                    "equity_ref": float(equity),

                    "latency_ms": int(local_latency_ms),
                    "chunk_pct": float(local_chunk_pct),

                    "max_reprice_attempts": int(max_reprice_attempts),
                    "attempts_left": int(attempts_left),

                    "chunk_idx": int(chunk_idx),
                }
                if record_share_rounding and share_rounding_audit is not None:
                    explain["share_rounding"] = dict(share_rounding_audit)
                if liquidity_snapshot:
                    explain["liquidity_snapshot"] = dict(liquidity_snapshot)
                if option_meta is not None:
                    explain["option"] = {
                        "contract": str(option_contract),
                        "underlying": str(getattr(option_meta, "underlying", "")),
                        "contract_multiplier": float(option_multiplier),
                        "quote_source": str(option_quote_source or ""),
                        "quote_ts_ms": int(option_quote_ts) if option_quote_ts is not None else None,
                        "bid": float(option_bid) if option_bid is not None else None,
                        "ask": float(option_ask) if option_ask is not None else None,
                        "mid": float(px_mid_use),
                        "notional_multiplier_applied": True,
                        "option_margin_debit": (
                            float(option_margin_debit) if option_margin_debit is not None else None
                        ),
                        "margin_model": (
                            "option_sim_margin_reference" if option_margin_debit is not None else None
                        ),
                    }
                if _is_shadow_book(book_key):
                    explain["shadow_book_key"] = str(_book_key(book_key))
                    explain["shadow_model_id"] = str(o.get("model_id") or "")

                fill_inserted = _broker_sim_phase_persist_fill_effects(
                    con,
                    order=order_for_audit,
                    symbol=symbol,
                    order_id=(int(order_id) if order_id is not None else None),
                    ts_ms=int(ts_ms),
                    fill_ts=int(fill_ts),
                    book_key=book_key,
                    qty_cap=float(qty_cap),
                    px_mid_use=float(px_mid_use),
                    px_exec=float(px_exec),
                    new_qty=float(new_qty),
                    new_avg=float(new_avg),
                    fee=float(fee),
                    notional=float(notional),
                    explain=explain,
                    exec_spread_bps=float(exec_spread_bps),
                    chunk_lob_slip_bps=float(chunk_lob_slip_bps),
                    lob_adverse_bps=float(lob_adverse_bps),
                    lob_impact_bps=float(lob_impact_bps),
                    almgren_chriss=dict(almgren_chriss or {}),
                    client_order_id=str(client_order_id),
                    order_ttl_ms=int(order_ttl_ms),
                    state_meta=state_meta,
                    contract_multiplier=(float(option_multiplier) if option_multiplier is not None else None),
                    option_quote_source=(str(option_quote_source) if option_quote_source is not None else None),
                    option_margin_debit=(
                        float(option_margin_debit) if option_margin_debit is not None else None
                    ),
                )

                if fill_inserted:
                    cash = float(cash_after_fill)
                    wrote_fills = True
                    fills_written += 1
                else:
                    fills_deduped += 1
                chunk_idx += 1

                # Optional wall-clock latency simulation (default off)
                if BROKER_LATENCY_SLEEP and int(local_latency_ms) > 0:
                    try:
                        time.sleep(max(0.0, int(local_latency_ms) / 1000.0))
                    except Exception as e:
                        _warn_nonfatal(
                            "broker_sim_latency_sleep_failed",
                            "BROKER_SIM_LATENCY_SLEEP_FAILED",
                            e,
                            warn_key="broker_sim_latency_sleep_failed",
                            latency_ms=int(local_latency_ms),
                        )

                # update loop state
                if fill_inserted:
                    cur_qty = float(new_qty)
                    cur_avg = float(new_avg)
                remaining -= float(qty_cap)

                max_notional_budget = max(0.0, float(max_notional_budget) - abs(float(notional)))

        # Phase 5: persist account/positions and mark to market.
        acct2 = _broker_sim_phase_persist_account_positions(
            con,
            cash=float(cash),
            now_ms=int(now_ms),
            book_key=book_key,
        )

        # mark orders applied (idempotency)
        if order_id is not None:
            _set_meta(con, "last_portfolio_orders_id", str(order_id), book_key=book_key)
        _safe_commit_connection(con, context="apply_new_portfolio_orders", once_key="apply_orders_commit")

        lifecycle_summary = None
        if _options_lifecycle_enabled():
            lifecycle_summary = apply_option_lifecycle(con, book_key=book_key, now_ms=int(now_ms))
            if isinstance(lifecycle_summary, dict) and lifecycle_summary.get("account"):
                acct2 = dict(lifecycle_summary.get("account") or acct2)

        # Phase 6: return summary.
        summary = _broker_sim_phase_return_summary(
            book_key=book_key,
            order_id=(int(order_id) if order_id is not None else None),
            wrote_fills=bool(wrote_fills),
            fills_written=int(fills_written),
            fills_deduped=int(fills_deduped),
            account=acct2,
        )
        if lifecycle_summary is not None:
            summary["options_lifecycle"] = dict(lifecycle_summary)
        if share_rounding_skipped:
            summary["share_rounding_skipped"] = list(share_rounding_skipped)
        return summary
    except Exception:
        try:
            con.rollback()
        except Exception as rollback_error:
            _warn_nonfatal(
                "broker_sim_apply_orders_rollback_failed",
                "BROKER_SIM_APPLY_ORDERS_ROLLBACK_FAILED",
                rollback_error,
                warn_key="apply_orders_rollback_failed",
            )
        raise
    finally:
        _safe_close_connection(con, context="apply_new_portfolio_orders", once_key="apply_orders_close")


def broker_equity_at(ts_ms: int, include_prices: bool = False) -> dict:
    """
    Mark-to-market broker equity at an arbitrary timestamp WITHOUT mutating broker_account.

    Returns:
      {
        ok, ts_ms, cash, equity,
        positions: [{symbol, qty, px, px_ts_ms, notional}],
        missing_prices: [symbol...]
      }
    """
    init_broker_db()
    con = connect()
    try:
        acct = _read_account(con)
        cash = float(acct.get("cash") or 0.0)

        eq = float(cash)
        out_positions = []
        missing = []

        rows = con.execute("SELECT symbol, qty FROM broker_positions ORDER BY symbol").fetchall()
        for sym, qty in rows or []:
            sym = str(sym)
            qty = float(qty or 0.0)

            meta = _option_contract_meta(sym)
            multiplier = _option_multiplier(meta) if meta is not None else None
            contract = _option_contract_key(meta) if meta is not None else None
            if meta is not None:
                if multiplier is None or not contract:
                    missing.append(sym)
                    continue
                _bid, _ask, px, px_ts = _get_option_quote_at_or_before(con, contract, int(ts_ms))
                if px is None or float(px) <= 0.0:
                    missing.append(sym)
                    continue
                notional = float(qty) * float(px) * float(multiplier)
                eq += notional

                if include_prices:
                    out_positions.append(
                        {
                            "symbol": sym,
                            "qty": float(qty),
                            "px": float(px),
                            "px_ts_ms": (int(px_ts) if px_ts is not None else None),
                            "notional": float(notional),
                            "contract_multiplier": float(multiplier),
                            "option_quote_source": f"options_chain_v2:{contract}:{int(px_ts)}",
                        }
                    )
                continue

            px, px_ts = _get_price_at_or_before(con, sym, int(ts_ms))
            if px is None or float(px) <= 0.0:
                missing.append(sym)
                continue

            notional = float(qty) * float(px)
            eq += notional

            if include_prices:
                out_positions.append(
                    {
                        "symbol": sym,
                        "qty": float(qty),
                        "px": float(px),
                        "px_ts_ms": (int(px_ts) if px_ts is not None else None),
                        "notional": float(notional),
                    }
                )

        return {
            "ok": True,
            "ts_ms": int(ts_ms),
            "cash": float(cash),
            "equity": float(eq),
            "positions": out_positions,
            "missing_prices": missing,
        }
    finally:
        _safe_close_connection(con, context="broker_equity_at", once_key="broker_equity_at_close")


def broker_snapshot(limit_fills: int = 50):

    init_broker_db()
    con = connect()
    try:
        acct = _read_account(con)
        cash = float(acct.get("cash", 0.0))
        equity = float(acct.get("equity", 1.0))
        upd = int(acct.get("updated_ts_ms", 0))

        pos = con.execute(
            "SELECT symbol, qty, avg_px, updated_ts_ms FROM broker_positions ORDER BY symbol"
        ).fetchall()

        fills = con.execute(
            """
            SELECT ts_ms, symbol, qty, px, source_order_id, note
            FROM broker_fills
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (int(max(1, min(500, int(limit_fills)))),),
        ).fetchall()

        return {
            "ok": True,
            "account": {"cash": float(cash), "equity": float(equity), "updated_ts_ms": int(upd)},
            "positions": [
                {"symbol": r[0], "qty": float(r[1]), "avg_px": float(r[2]), "updated_ts_ms": int(r[3])}
                for r in (pos or [])
            ],
            "fills": [
                {
                    "ts_ms": int(r[0]),
                    "symbol": r[1],
                    "qty": float(r[2]),
                    "px": float(r[3]),
                    "order_id": (int(r[4]) if r[4] is not None else None),
                    "note": r[5],
                }
                for r in (fills or [])
            ],
        }
    finally:
        _safe_close_connection(con, context="broker_snapshot", once_key="broker_snapshot_close")


def main():
    res = apply_new_portfolio_orders()
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
