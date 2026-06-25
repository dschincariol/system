"""
FILE: portfolio_backtest.py

Backtest harness for the portfolio construction layer. It walks alerts through
time, rebuilds target portfolios, and scores them against realized outcomes
while persisting both run metadata and step-by-step points.
"""

import json
import os
import time
import math
import statistics
import logging
from typing import Any

from engine.data.universe_pit import filter_symbols_for_snapshot
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, run_write_txn
from engine.execution.almgren_chriss import estimate_almgren_chriss_costs
from engine.execution.execution_costs import estimate_cost_bps
from engine.execution.execution_liquidity_model import get_execution_liquidity_snapshot
from engine.risk.futures_margin import contract_notional, weight_to_contracts
from engine.strategy.regime_stack import compute_regime_vector, regime_model_version
from engine.execution.execution_policy_engine import apply_execution_policy
from engine.strategy.portfolio import (
    init_portfolio_db,
    PORTFOLIO_LOOKBACK_S,
    PORTFOLIO_MIN_CONF,
    PORTFOLIO_MIN_ABS_Z,
    PORTFOLIO_MAX_POSITIONS,
    PORTFOLIO_GROSS_CAP,
    PORTFOLIO_MAX_W_PER_SYMBOL,
)

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [portfolio_backtest] %(message)s",
)
LOG = get_logger("engine.strategy.portfolio_backtest")
PORTFOLIO_BACKTEST_USE_EXEC_COSTS = os.environ.get("PORTFOLIO_BACKTEST_USE_EXEC_COSTS", "0") == "1"
PORTFOLIO_BACKTEST_FEE_BPS = float(os.environ.get("BROKER_FEE_BPS", "0.5"))
PORTFOLIO_BACKTEST_SLIPPAGE_BPS = float(os.environ.get("BROKER_SLIPPAGE_BPS", "1.0"))
PORTFOLIO_BACKTEST_SPREAD_BPS = float(os.environ.get("BROKER_SPREAD_BPS", "2.0"))
PORTFOLIO_BACKTEST_FUTURES_SLIPPAGE_TICKS = float(os.environ.get("PORTFOLIO_BACKTEST_FUTURES_SLIPPAGE_TICKS", "1.0"))
PORTFOLIO_BACKTEST_FUTURES_ROLL_TICKS = float(os.environ.get("PORTFOLIO_BACKTEST_FUTURES_ROLL_TICKS", "2.0"))
PORTFOLIO_BACKTEST_FUTURES_ROLL_WINDOW_MS = int(os.environ.get("PORTFOLIO_BACKTEST_FUTURES_ROLL_WINDOW_MS", str(24 * 60 * 60 * 1000)))


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    log_failure(
        LOG,
        event="portfolio_backtest_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.portfolio_backtest",
        extra=extra or None,
        persist=False,
    )


def _require_int(value: int | None, *, field: str) -> int:
    if value is None:
        raise RuntimeError(f"missing_required_{field}")
    return int(value)

SCHEMA = """
CREATE TABLE IF NOT EXISTS portfolio_bt_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  start_ts_ms INTEGER NOT NULL,
  end_ts_ms INTEGER NOT NULL,
  metrics_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_bt_points (
  run_id INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL,
  ret REAL NOT NULL,
  equity REAL NOT NULL,
  drawdown REAL NOT NULL,
  exec_cost REAL DEFAULT 0.0,
  slippage REAL DEFAULT 0.0,
  fees REAL DEFAULT 0.0,
  detail_json TEXT,
  PRIMARY KEY (run_id, ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_portfolio_bt_points_run ON portfolio_bt_points(run_id, ts_ms);
CREATE INDEX IF NOT EXISTS idx_portfolio_bt_points_ts  ON portfolio_bt_points(ts_ms);

CREATE TABLE IF NOT EXISTS strategy_metrics (
  strategy_name TEXT NOT NULL,
  window_days INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL,
  start_ts_ms INTEGER NOT NULL,
  end_ts_ms INTEGER NOT NULL,
  metrics_json TEXT NOT NULL,
  PRIMARY KEY (strategy_name, window_days)
);

CREATE TABLE IF NOT EXISTS rl_shadow_eval (
  policy_name TEXT NOT NULL,
  run_id INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL,
  baseline_metrics_json TEXT NOT NULL,
  policy_metrics_json TEXT NOT NULL,
  delta_metrics_json TEXT NOT NULL,
  PRIMARY KEY (policy_name, run_id)
);

CREATE TABLE IF NOT EXISTS rl_shadow_actions (
  run_id INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL,
  step_idx INTEGER NOT NULL,
  state_json TEXT NOT NULL,
  baseline_action_json TEXT NOT NULL,
  rl_action_json TEXT NOT NULL,
  step_ret REAL NOT NULL,
  equity REAL NOT NULL,
  drawdown REAL NOT NULL,
  turnover REAL NOT NULL,
  reward REAL NOT NULL,
  PRIMARY KEY (run_id, ts_ms)
);

CREATE TABLE IF NOT EXISTS rl_policies (
  policy_name TEXT PRIMARY KEY,
  ts_ms INTEGER NOT NULL,
  params_json TEXT NOT NULL,
  metrics_json TEXT NOT NULL
);
"""
_PORTFOLIO_BACKTEST_SCHEMA_TABLES = (
    "portfolio_bt_runs",
    "portfolio_bt_points",
    "strategy_metrics",
    "rl_shadow_eval",
    "rl_shadow_actions",
    "rl_policies",
)
_PORTFOLIO_BACKTEST_SCHEMA_INDEXES = (
    "idx_portfolio_bt_points_run",
    "idx_portfolio_bt_points_ts",
)

# -------------            -- ------------------------------------------------------
# Small numeric guards
# -------------            -- ------------------------------------------------------

def _cost_bps_from_trade(trade: dict, px_in: float, px_out: float, side: int) -> dict:
    """
    Best-effort execution cost decomposition in bps.
    Returns dict with:
      fees_bps, slippage_bps, spread_bps, total_cost_bps, spread_in
    All fields are floats (>=0 where applicable).
    """
    try:
        pin = float(px_in)
        sgn = float(side) if int(side) != 0 else 1.0
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_COST_INPUT_PARSE_FAILED",
            e,
            px_in=px_in,
            px_out=px_out,
            side=side,
        )
        return {"fees_bps": 0.0, "slippage_bps": 0.0, "spread_bps": 0.0, "total_cost_bps": 0.0, "spread_in": None}

    if pin <= 1e-12:
        return {"fees_bps": 0.0, "slippage_bps": 0.0, "spread_bps": 0.0, "total_cost_bps": 0.0, "spread_in": None}

    # Fees: accept a few possible keys
    fees_total = 0.0
    try:
        fees_total = float(trade.get("fees_total") or trade.get("fees") or 0.0)
    except Exception:
        fees_total = 0.0

    traded_notional = 0.0
    try:
        traded_notional = abs(float(trade.get("notional") or 0.0))
    except Exception:
        traded_notional = 0.0
    if traded_notional <= 0.0:
        try:
            qty = abs(
                float(
                    trade.get("qty")
                    or trade.get("filled_qty")
                    or trade.get("size")
                    or trade.get("shares")
                    or 0.0
                )
            )
        except Exception:
            qty = 0.0
        traded_notional = float(qty) * float(pin)

    fees_bps = 0.0
    if traded_notional > 1e-12:
        try:
            fees_bps = float(fees_total) / float(traded_notional) * 10000.0
            if fees_bps != fees_bps or fees_bps < 0:
                fees_bps = 0.0
        except Exception:
            fees_bps = 0.0

    # Slippage: if trade provides a ref price, compare fill to ref in sign-aware bps.
    slippage_bps = 0.0
    ref_px = None
    try:
        ref_px = trade.get("ref_px")
        if ref_px is None:
            ref_px = trade.get("mid_in")
        if ref_px is not None:
            ref_px = float(ref_px)
    except Exception:
        ref_px = None

    if ref_px is not None and ref_px > 1e-12:
        try:
            # buy worse if fill > ref; sell worse if fill < ref -> sign by side
            slippage_bps = ((float(pin) - float(ref_px)) / float(ref_px)) * 10000.0 * float(sgn)
            # cost should be positive "worse"; flip sign if needed
            slippage_bps = -float(slippage_bps)
            if slippage_bps != slippage_bps:
                slippage_bps = 0.0
        except Exception:
            slippage_bps = 0.0

    # Spread: if trade provides spread_in or bid/ask, compute.
    spread_in = None
    spread_bps = 0.0
    try:
        si = trade.get("spread_in")
        if si is None:
            bid = trade.get("bid_in")
            ask = trade.get("ask_in")
            if bid is not None and ask is not None:
                si = float(ask) - float(bid)
        if si is not None:
            spread_in = float(si)
    except Exception:
        spread_in = None

    if spread_in is not None and pin > 1e-12:
        try:
            spread_bps = float(spread_in) / float(pin) * 10000.0
            if spread_bps != spread_bps or spread_bps < 0:
                spread_bps = 0.0
        except Exception:
            spread_bps = 0.0

    total_cost_bps = float(max(0.0, fees_bps)) + float(max(0.0, slippage_bps)) + float(max(0.0, spread_bps))

    return {
        "fees_bps": float(max(0.0, fees_bps)),
        "slippage_bps": float(max(0.0, slippage_bps)),
        "spread_bps": float(max(0.0, spread_bps)),
        "total_cost_bps": float(max(0.0, total_cost_bps)),
        "spread_in": (float(spread_in) if spread_in is not None else None),
    }


def _price_at_or_before(con, symbol: str, ts_ms: int) -> float | None:
    try:
        row = con.execute(
            """
            SELECT COALESCE(price, px)
            FROM prices
            WHERE symbol=? AND ts_ms<=?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(symbol).upper().strip(), int(ts_ms)),
        ).fetchone()
        if row and row[0] is not None:
            px = _safe_f(row[0], 0.0)
            return float(px) if px > 0.0 else None
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_PRICE_LOOKUP_FAILED",
            e,
            symbol=str(symbol).upper().strip(),
            ts_ms=int(ts_ms),
        )
    return None


def _futures_metadata(con, symbol: str) -> dict[str, Any] | None:
    try:
        from engine.data.universe import get_instrument_metadata

        raw = get_instrument_metadata(con, symbol)
        if isinstance(raw, dict) and str(raw.get("asset_class") or "").upper().strip() == "FUTURES":
            return dict(raw)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_FUTURES_METADATA_LOOKUP_FAILED",
            e,
            symbol=str(symbol),
        )
    try:
        from engine.data.futures_instrument import parse_futures_symbol

        parsed = parse_futures_symbol(symbol)
        if parsed is None:
            return None
        return dict(parsed.to_dict())
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_FUTURES_SYMBOL_PARSE_FAILED",
            e,
            symbol=str(symbol),
        )
        return None


def _futures_roll_day(con, root: str, ts_ms: int) -> bool:
    if not root:
        return False
    window = max(0, int(PORTFOLIO_BACKTEST_FUTURES_ROLL_WINDOW_MS))
    start = int(ts_ms) - int(window // 2)
    end = int(ts_ms) + int(window // 2)
    try:
        row = con.execute(
            """
            SELECT 1
            FROM futures_roll_calendar
            WHERE root=? AND roll_ts_ms BETWEEN ? AND ?
            LIMIT 1
            """,
            (str(root).upper().strip(), int(start), int(end)),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_FUTURES_ROLL_DAY_LOOKUP_FAILED",
            e,
            root=str(root),
        )
        return False


def futures_point_value_pnl(
    *,
    contracts: int | float,
    multiplier: float,
    entry_px: float,
    exit_px: float,
    side: int | float = 1,
) -> float:
    side_sign = 1.0 if float(side or 0.0) >= 0.0 else -1.0
    return float(float(contracts or 0.0) * float(multiplier or 0.0) * (float(exit_px or 0.0) - float(entry_px or 0.0)) * side_sign)


def _estimate_weight_delta_trade_cost(con, symbol: str, delta_weight: float, equity: float, ts_ms: int) -> dict[str, Any]:
    out: dict[str, Any] = {
        "symbol": str(symbol).upper().strip(),
        "delta_weight": float(delta_weight),
        "exec_cost": 0.0,
        "slippage": 0.0,
        "fees": 0.0,
        "qty": 0.0,
        "notional": 0.0,
        "cost_bps": {},
        "almgren_chriss": {},
        "status": "disabled",
    }
    if not PORTFOLIO_BACKTEST_USE_EXEC_COSTS:
        return out

    px = _price_at_or_before(con, str(symbol), int(ts_ms))
    if px is None or px <= 0.0:
        out["status"] = "missing_price"
        return out

    futures_meta = _futures_metadata(con, str(symbol))
    is_futures = isinstance(futures_meta, dict)
    contract_multiplier = 1.0
    tick_size = 0.0
    tick_value = 0.0
    contracts = 0
    roll_cost = 0.0
    tick_slippage_cost = 0.0
    roll_cost_bps = 0.0

    if is_futures:
        contract_multiplier = _safe_f(futures_meta.get("multiplier", futures_meta.get("fut_multiplier")), 1.0)
        tick_size = _safe_f(futures_meta.get("tick_size", futures_meta.get("fut_tick_size")), 0.0)
        tick_value = _safe_f(futures_meta.get("tick_value", futures_meta.get("fut_tick_value")), 0.0)
        contracts = weight_to_contracts(float(delta_weight), float(equity), float(contract_multiplier), float(px))
        if contracts == 0:
            out["status"] = "no_contracts"
            out["contract_multiplier"] = float(contract_multiplier)
            out["px"] = float(px)
            return out
        qty = float(abs(contracts))
        side = 1 if int(contracts) >= 0 else -1
        notional = contract_notional(qty, float(px), float(contract_multiplier))
        if notional <= 0.0:
            out["status"] = "no_notional"
            return out
        tick_slippage_cost = float(qty) * max(0.0, float(tick_value)) * max(0.0, float(PORTFOLIO_BACKTEST_FUTURES_SLIPPAGE_TICKS))
        root = str(futures_meta.get("root") or futures_meta.get("fut_root") or "").upper().strip()
        if _futures_roll_day(con, root, int(ts_ms)):
            roll_cost = float(qty) * max(0.0, float(tick_value)) * max(0.0, float(PORTFOLIO_BACKTEST_FUTURES_ROLL_TICKS)) * 2.0
        roll_cost_bps = (float(roll_cost) / float(notional) * 10000.0) if notional > 0.0 else 0.0
    else:
        notional = abs(float(delta_weight)) * max(0.0, float(equity))
        if notional <= 0.0:
            out["status"] = "no_notional"
            return out

        qty = float(notional) / float(px)
        side = 1 if float(delta_weight) >= 0.0 else -1
    try:
        liquidity_snapshot = dict(
            get_execution_liquidity_snapshot(
                str(symbol),
                qty=float(qty),
                px=float(px),
                ts_ms=int(ts_ms),
            )
            or {}
        )
    except Exception as e:
        liquidity_snapshot = {}
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_LIQUIDITY_SNAPSHOT_FAILED",
            e,
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )

    spread_cost_bps = float(PORTFOLIO_BACKTEST_SPREAD_BPS) * 0.5
    if liquidity_snapshot:
        spread_cost_bps = max(
            0.0,
            float(liquidity_snapshot.get("true_spread_bps") or 0.0) * 0.5,
        )
    ac_costs = estimate_almgren_chriss_costs(
        symbol=str(symbol),
        qty=float(qty),
        px=float(px),
        side=int(side),
        ts_ms=int(ts_ms),
        liquidity_snapshot=liquidity_snapshot,
        contract_multiplier=float(contract_multiplier),
    )
    futures_slippage_bps = (float(tick_slippage_cost) / float(notional) * 10000.0) if is_futures and notional > 0.0 else None
    cost_bps = estimate_cost_bps(
        px=float(px),
        bid=None,
        ask=None,
        side=int(side),
        fees_bps=float(PORTFOLIO_BACKTEST_FEE_BPS),
        slippage_bps=float(futures_slippage_bps if futures_slippage_bps is not None else PORTFOLIO_BACKTEST_SLIPPAGE_BPS),
        spread_bps_override=float(spread_cost_bps),
        extra_cost_bps=float(ac_costs.get("execution_cost_bps") or 0.0) + float(roll_cost_bps),
        contract_multiplier=(float(contract_multiplier) if is_futures else None),
        tick_size=(float(tick_size) if is_futures else None),
        tick_value=(float(tick_value) if is_futures else None),
    )
    fees_cost = float(notional) * (float(cost_bps.get("fees_bps") or 0.0) / 10000.0)
    slippage_cost = float(notional) * (
        (
            float(cost_bps.get("slippage_bps") or 0.0)
            + float(cost_bps.get("spread_bps") or 0.0)
            + float(cost_bps.get("extra_cost_bps") or 0.0)
        )
        / 10000.0
    )
    total_cost = float(notional) * (float(cost_bps.get("total_cost_bps") or 0.0) / 10000.0)
    out.update(
        {
            "exec_cost": float(total_cost),
            "slippage": float(slippage_cost),
            "fees": float(fees_cost),
            "qty": float(qty),
            "notional": float(notional),
            "px": float(px),
            "liquidity_snapshot": dict(liquidity_snapshot or {}),
            "cost_bps": dict(cost_bps or {}),
            "almgren_chriss": dict(ac_costs or {}),
            "status": ("estimated_futures" if is_futures else "estimated"),
        }
    )
    if is_futures:
        out.update(
            {
                "asset_class": "FUTURES",
                "contracts": int(contracts),
                "contract_multiplier": float(contract_multiplier),
                "tick_size": float(tick_size),
                "tick_value": float(tick_value),
                "tick_slippage_cost": float(tick_slippage_cost),
                "roll_cost": float(roll_cost),
                "roll_cost_bps": float(roll_cost_bps),
                "point_value_notional": float(notional),
            }
        )
    return out


def _estimate_transition_trade_costs(con, prev_positions, cur_positions, equity: float, ts_ms: int) -> dict[str, Any]:
    summary = {
        "exec_cost": 0.0,
        "slippage": 0.0,
        "fees": 0.0,
        "trade_costs": [],
        "enabled": bool(PORTFOLIO_BACKTEST_USE_EXEC_COSTS),
    }
    if not PORTFOLIO_BACKTEST_USE_EXEC_COSTS:
        return summary

    prev_map = _posmap_signed(prev_positions)
    cur_map = _posmap_signed(cur_positions)
    for symbol in sorted(set(prev_map) | set(cur_map)):
        delta_weight = float(cur_map.get(symbol, 0.0) or 0.0) - float(prev_map.get(symbol, 0.0) or 0.0)
        if abs(delta_weight) <= 1e-9:
            continue
        trade_cost = _estimate_weight_delta_trade_cost(con, str(symbol), float(delta_weight), float(equity), int(ts_ms))
        summary["trade_costs"].append(trade_cost)
        summary["exec_cost"] += float(trade_cost.get("exec_cost") or 0.0)
        summary["slippage"] += float(trade_cost.get("slippage") or 0.0)
        summary["fees"] += float(trade_cost.get("fees") or 0.0)
    return summary


def _now_ms():
    return int(time.time() * 1000)

def _safe_f(x, d=0.0):
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_SAFE_FLOAT_FAILED",
            e,
            warn_key="portfolio_backtest_safe_float_failed",
            value=x,
            default=d,
        )
        return d

# RL env (must be after _safe_f)
RL_TRAIN = os.environ.get("RL_TRAIN", "0") == "1"
RL_POLICY_NAME = os.environ.get("RL_POLICY_NAME", "conf_threshold_v1")
RL_GRID_MIN_CONF = _safe_f(os.environ.get("RL_GRID_MIN_CONF", "0.50"), 0.50)
RL_GRID_MAX_CONF = _safe_f(os.environ.get("RL_GRID_MAX_CONF", "0.90"), 0.90)
RL_GRID_STEP_CONF = _safe_f(os.environ.get("RL_GRID_STEP_CONF", "0.02"), 0.02)
RL_LAMBDA_DD = _safe_f(os.environ.get("RL_LAMBDA_DD", "0.5"), 0.5)
RL_LAMBDA_TURN = _safe_f(os.environ.get("RL_LAMBDA_TURN", "0.1"), 0.1)

def _safe_mean(xs):
    xs = list(xs or [])
    return float(statistics.mean(xs)) if xs else 0.0

def _safe_stdev(xs):
    xs = list(xs or [])
    if len(xs) < 2:
        return 0.0
    try:
        return float(statistics.stdev(xs))
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_SAFE_STDEV_FAILED",
            e,
            warn_key="portfolio_backtest_safe_stdev_failed",
            sample_size=len(xs),
        )
        return 0.0

def _safe_i(x, d=0):
    try:
        return int(x)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_SAFE_INT_FAILED",
            e,
            warn_key="portfolio_backtest_safe_int_failed",
            value=x,
            default=d,
        )
        return int(d)

def _is_finite(x):
    try:
        return math.isfinite(float(x))
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_IS_FINITE_FAILED",
            e,
            warn_key="portfolio_backtest_is_finite_failed",
            value=x,
        )
        return False

def _clamp(x, lo, hi):
    return max(float(lo), min(float(hi), float(x)))

def _slice_curve_window(curve, end_ts_ms, window_days):
    if not curve or window_days <= 0:
        return curve
    cutoff = int(end_ts_ms) - int(window_days) * 86400 * 1000
    return [p for p in curve if int(p[0]) >= cutoff]

def _risk_metrics_from_curve(curve, total_return, max_drawdown):
    rets = [float(p[1]) for p in (curve or [])]
    n = len(rets)
    mu = _safe_mean(rets)
    vol = _safe_stdev(rets)
    sharpe = (mu / vol) * math.sqrt(n) if vol > 1e-12 and n > 1 else 0.0
    downside = [r for r in rets if r < 0.0]
    dvol = _safe_stdev(downside)
    sortino = (mu / dvol) * math.sqrt(n) if dvol > 1e-12 and n > 1 else 0.0
    calmar = (total_return / abs(max_drawdown)) if abs(max_drawdown) > 1e-12 else 0.0
    return {
        "ret_mean": mu,
        "ret_volatility": vol,
        "downside_volatility": dvol,
        "sharpe_simple": sharpe,
        "sortino_simple": sortino,
        "calmar_simple": calmar,
        "n_returns": n,
    }

# -------------            -- ------------------------------------------------------
# DB init
# -------------            -- ------------------------------------------------------

def _table_exists(con, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _index_exists(con, index_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
        (str(index_name),),
    ).fetchone()
    return bool(row)


def _portfolio_backtest_schema_ready() -> bool:
    con = connect(readonly=True)
    try:
        for table_name in _PORTFOLIO_BACKTEST_SCHEMA_TABLES:
            if not _table_exists(con, table_name):
                return False
        for index_name in _PORTFOLIO_BACKTEST_SCHEMA_INDEXES:
            if not _index_exists(con, index_name):
                return False
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_BACKTEST_SCHEMA_READY_CLOSE_FAILED", e)
    return True


def _init_portfolio_backtest_schema(con) -> None:
    con.executescript(SCHEMA)


def init_portfolio_backtest_schema() -> None:
    if _portfolio_backtest_schema_ready():
        return
    run_write_txn(
        _init_portfolio_backtest_schema,
        table="portfolio_bt_runs",
        operation="init_portfolio_backtest_schema",
        direct=True,
    )


def _persist_backtest_results(
    con,
    *,
    now_ms: int,
    start_ms: int,
    curve: list[tuple[int, float, float, float, dict[str, Any]]],
    metrics: dict[str, Any],
) -> int:
    run_id = con.execute(
        """
        INSERT INTO portfolio_bt_runs(ts_ms, start_ts_ms, end_ts_ms, metrics_json)
        VALUES (?,?,?,?)
        """,
        (int(now_ms), int(start_ms), int(now_ms), "{}"),
    ).lastrowid
    run_id = _require_int(run_id, field="run_id")

    for ts_ms, step_ret, equity, drawdown, detail in curve:
        con.execute(
            """
            INSERT OR REPLACE INTO portfolio_bt_points
            (run_id, ts_ms, ret, equity, drawdown, exec_cost, slippage, fees, detail_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(run_id),
                int(ts_ms),
                float(step_ret),
                float(equity),
                float(drawdown),
                float(detail.get("exec_cost") or 0.0),
                float(detail.get("slippage") or 0.0),
                float(detail.get("fees") or 0.0),
                json.dumps(detail),
            ),
        )

    con.execute(
        """
        UPDATE portfolio_bt_runs
        SET end_ts_ms=?, metrics_json=?
        WHERE id=?
        """,
        (int(now_ms), json.dumps(metrics), int(run_id)),
    )
    return int(run_id)

def _get_tse_state(con):
    try:
        row = con.execute(
            "SELECT ts_ms, state, fp_streak, slippage_z, latency_var_z FROM trade_suppression_state WHERE id=1"
        ).fetchone()
        if not row:
            return None
        return {
            "ts_ms": int(row[0]),
            "state": str(row[1]),
            "fp_streak": int(row[2]) if row[2] is not None else None,
            "slippage_z": float(row[3]) if row[3] is not None else None,
            "latency_var_z": float(row[4]) if row[4] is not None else None,
        }
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_GET_TSE_STATE_FAILED",
            e,
            warn_key="portfolio_backtest_get_tse_state_failed",
        )
        return None

# -------------            -- ------------------------------------------------------
# Label lookup
# -------------            -- ------------------------------------------------------

def _resolve_event_id(con, alert_ts, title):
    t = (title or "").strip()
    if not t:
        return None

    row = con.execute(
        "SELECT id FROM events WHERE title=? AND ts_ms<=? ORDER BY ts_ms DESC LIMIT 1",
        (t, int(alert_ts)),
    ).fetchone()
    if row:
        return int(row[0])

    row = con.execute(
        "SELECT id FROM events WHERE title=? ORDER BY ABS(ts_ms-?) ASC LIMIT 1",
        (t, int(alert_ts)),
    ).fetchone()
    return int(row[0]) if row else None

def _realized_label(con, event_id, symbol, horizon_s):
    r = con.execute(
        """
        SELECT impact_z
        FROM labels
        WHERE event_id=? AND symbol=? AND horizon_s=? AND impact_z IS NOT NULL
        """,
        (int(event_id), str(symbol), int(horizon_s)),
    ).fetchone()
    if not r:
        return None
    try:
        v = float(r[0])
        return v if math.isfinite(v) else None
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_BACKTEST_REALIZED_LABEL_PARSE_FAILED",
            e,
            warn_key=f"portfolio_backtest_realized_label_parse_failed:{event_id}:{symbol}:{horizon_s}",
            event_id=int(event_id),
            symbol=str(symbol),
            horizon_s=int(horizon_s),
            value=r[0],
        )
        return None

# -------------            -- ------------------------------------------------------
# Portfolio construction from alerts (baseline)
# -------------            -- ------------------------------------------------------

def _targets_from_recent_alerts(con, now_ms, lookback_s):
    cutoff = int(now_ms) - int(lookback_s) * 1000

    # detect optional columns
    try:
        cols = [r[1] for r in (con.execute("PRAGMA table_info(alerts)").fetchall() or [])]
    except Exception:
        cols = []
    has_severity = "severity" in cols
    has_title = "event_title" in cols
    has_event_id = "event_id" in cols

    if has_event_id and has_severity and has_title:
        rows = con.execute(
            """
            SELECT id, ts_ms, symbol, horizon_s, expected_z, confidence, severity, event_title, event_id
            FROM alerts
            WHERE ts_ms >= ? AND ts_ms <= ?
            ORDER BY ts_ms DESC
            """,
            (int(cutoff), int(now_ms)),
        ).fetchall()
    elif has_severity and has_title:
        rows = con.execute(
            """
            SELECT id, ts_ms, symbol, horizon_s, expected_z, confidence, severity, event_title
            FROM alerts
            WHERE ts_ms >= ? AND ts_ms <= ?
            ORDER BY ts_ms DESC
            """,
            (int(cutoff), int(now_ms)),
        ).fetchall()
    elif has_severity:
        rows = con.execute(
            """
            SELECT id, ts_ms, symbol, horizon_s, expected_z, confidence, severity
            FROM alerts
            WHERE ts_ms >= ? AND ts_ms <= ?
            ORDER BY ts_ms DESC
            """,
            (int(cutoff), int(now_ms)),
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT id, ts_ms, symbol, horizon_s, expected_z, confidence
            FROM alerts
            WHERE ts_ms >= ? AND ts_ms <= ?
            ORDER BY ts_ms DESC
            """,
            (int(cutoff), int(now_ms)),
        ).fetchall()

    best = {}
    for row in (rows or []):
        ts = row[1]
        sym = str(row[2] or "").strip()
        h = row[3]
        z = _safe_f(row[4], 0.0)
        conf = _safe_f(row[5], 0.0)

        if not sym:
            continue
        if conf < float(PORTFOLIO_MIN_CONF):
            continue
        if abs(z) < float(PORTFOLIO_MIN_ABS_Z):
            continue

        sev = ""
        title = ""
        event_id = None

        if has_severity:
            sev = str(row[6] or "")
        if has_title:
            title = str(row[7] or "")
        if has_event_id:
            event_id = row[8] if len(row) > 8 else None

        sev_u = str(sev or "").upper()
        score = abs(z) * conf
        if sev_u == "CRIT":
            score *= 1.15
        elif sev_u == "HIGH":
            score *= 1.08

        cur = best.get(sym)
        if cur is None or float(score) > float(cur["_score"]):
            if event_id is None and title:
                event_id = _resolve_event_id(con, int(ts), title)

            best[sym] = {
                "symbol": sym,
                "side": "LONG" if z > 0 else "SHORT",
                "weight": float(
                    min(
                        (float(score) / 3.0) * float(PORTFOLIO_GROSS_CAP),
                        float(PORTFOLIO_MAX_W_PER_SYMBOL),
                    )
                ),
                "event_id": (int(event_id) if event_id is not None else None),
                "horizon_s": int(h),
                "expected_z": float(z),
                "confidence": float(conf),
                "severity": str(sev or ""),
                "event_title": str(title or ""),
                # optional execution fields for later patches; default 0
                "exec_cost": 0.0,
                "slippage": 0.0,
                "fees": 0.0,
                "regime_model_version": str(regime_model_version()),
                "regime_vector": compute_regime_vector(symbol=sym, ts_ms=int(now_ms), con=con),
                "regime_compatibility": 1.0,
                "_score": float(score),

            }

    pit_symbols = filter_symbols_for_snapshot(
        con,
        symbols=list(best.keys()),
        ts_ms=int(now_ms),
        limit=int(PORTFOLIO_MAX_POSITIONS),
    )
    allowed_symbols = {str(sym).upper().strip() for sym in list(pit_symbols.get("symbols") or []) if str(sym).strip()}
    if allowed_symbols:
        filtered_positions = [dict(value) for sym, value in best.items() if str(sym).upper().strip() in allowed_symbols]
    else:
        filtered_positions = [dict(value) for value in best.values()]

    positions = filtered_positions[: int(PORTFOLIO_MAX_POSITIONS)]
    gross = sum(abs(float(p.get("weight", 0.0))) for p in positions)
    if gross > float(PORTFOLIO_GROSS_CAP) and gross > 1e-12:
        s = float(PORTFOLIO_GROSS_CAP) / float(gross)
        for p in positions:
            p["weight"] = float(p["weight"]) * float(s)
    return positions

def _positions_filter_conf(positions, conf_min):
    return [p for p in positions or [] if _safe_f(p.get("confidence"), 0.0) >= conf_min]

def _posmap_signed(positions):
    m = {}
    for p in positions or []:
        sym = p.get("symbol")
        w = _safe_f(p.get("weight"), 0.0)
        side = str(p.get("side") or "").upper()
        sw = w if side == "LONG" else -w
        if sym:
            m[sym] = sw
    return m

def _turnover_between(prev, cur):
    a = _posmap_signed(prev)
    b = _posmap_signed(cur)
    return 0.5 * sum(abs(b.get(s, 0.0) - a.get(s, 0.0)) for s in set(a) | set(b))

def _recompute_step_ret(positions):
    gross = sum(abs(_safe_f(p.get("weight"), 0.0)) for p in positions) or 1.0
    r = 0.0
    for p in positions:
        if p.get("realized_impact_z") is None:
            continue
        w = _safe_f(p.get("weight"), 0.0)
        side = str(p.get("side") or "").upper()
        signed = w if side == "LONG" else -w
        r += (signed / gross) * float(p["realized_impact_z"])
    return r

# -------------            -- ------------------------------------------------------
# Main backtest
# -------------            -- ------------------------------------------------------

def run_backtest():
    con = None
    metrics = {}
    run_id = None
    try:
        try:
            init_portfolio_db()
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_BACKTEST_INIT_PORTFOLIO_DB_FAILED", e)

        init_portfolio_backtest_schema()
        con = connect(readonly=True)

        start_capital = _safe_f(os.environ.get("BT_START_EQUITY", "1.0"), 1.0)
        if (not _is_finite(start_capital)) or float(start_capital) <= 0.0:
            start_capital = 1.0

        days = _safe_i(os.environ.get("BT_DAYS", "60"), 60)
        days = max(1, int(days))

        lookback_s = _safe_i(os.environ.get("BT_LOOKBACK_S", PORTFOLIO_LOOKBACK_S), int(PORTFOLIO_LOOKBACK_S))
        lookback_s = max(60, int(lookback_s))

        now_ms = _now_ms()
        start_ms = now_ms - int(days) * 86400 * 1000

        equity = float(start_capital)
        peak = float(start_capital)
        max_dd = 0.0
        curve = []  # (ts_ms, ret, equity, drawdown, detail_dict)
        live_positions = []

        # TSE / suppression accounting (for tail-risk validation)
        suppression_blocks = 0
        suppression_nonblocks = 0
        suppression_state_counts = {}

        alerts = con.execute(
            """
            SELECT ts_ms
            FROM (
                SELECT ts_ms
                FROM alerts
                WHERE ts_ms >= ? AND ts_ms <= ?
                ORDER BY ts_ms DESC
                LIMIT 5000
            )
            ORDER BY ts_ms ASC
            """,
            (int(start_ms), int(now_ms)),
        ).fetchall()

        if not alerts:
            alerts = con.execute(
                """
                SELECT ts_ms
                FROM (
                    SELECT ts_ms
                    FROM alerts
                    ORDER BY ts_ms DESC
                    LIMIT 500
                )
                ORDER BY ts_ms ASC
                """
            ).fetchall()

        for row in (alerts or []):
            if not row:
                continue
            ts_ms = int(row[0])
            target_positions = _targets_from_recent_alerts(con, ts_ms, lookback_s)

            # --- Route through EPE so TSE can HARD_BLOCK / SOFT_THROTTLE / SIZE_COMPRESSION in backtest
            intents = []
            for p in (target_positions or []):
                sym = str(p.get("symbol") or "").strip().upper()
                if not sym:
                    continue
                side = str(p.get("side") or "").upper().strip()
                w = float(_safe_f(p.get("weight", 0.0), 0.0))
                if w <= 0.0:
                    continue
                intents.append(
                    {
                        "symbol": sym,
                        "to_side": ("LONG" if side == "LONG" else "SHORT"),
                        "to_weight": float(w),
                        "signal_ts_ms": int(ts_ms),
                        # provide defaults so EPE TTL/half-life works even without alert_id linkage
                        "alpha_ttl_ms": int(os.environ.get("EPE_DEFAULT_TTL_MS", str(5 * 60 * 1000))),
                        "alpha_half_life_ms": int(os.environ.get("EPE_DEFAULT_HALF_LIFE_MS", str(90 * 1000))),
                        "source_alert_id": None,
                        "source_order_id": None,
                    }
                )

            shaped = apply_execution_policy(
                con=con,
                intents=intents,
                actor="backtest",
                mode="backtest",
                broker="sim",
                portfolio_orders_batch_id=None,
                default_signal_ts_ms=int(ts_ms),
            )

            # Read current TSE state snapshot (written by EPE)
            tse_state = _get_tse_state(con)
            tse_key = str((tse_state or {}).get("state") or "NONE")
            suppression_state_counts[tse_key] = int(suppression_state_counts.get(tse_key, 0)) + 1

            if intents and not shaped:
                # HARD_BLOCK (or all intents suppressed) -> treat as no trades for this step
                suppression_blocks += 1
                positions = []
            else:
                suppression_nonblocks += 1
                # Apply any size compression / throttle already embedded in shaped to_weight
                wmap = {}
                smap = {}
                for o in (shaped or []):
                    try:
                        s = str(o.get("symbol") or "").strip().upper()
                        if not s:
                            continue
                        wmap[s] = float(o.get("to_weight") or 0.0)
                        smap[s] = str(o.get("to_side") or "").upper().strip()
                    except Exception as e:
                        _warn_nonfatal(
                            "PORTFOLIO_BACKTEST_SHAPED_ORDER_PARSE_FAILED",
                            e,
                            warn_key=f"portfolio_backtest_shaped_order_parse_failed:{repr(o)[:96]}",
                            order_repr=repr(o),
                        )
                        continue

                new_positions = []
                for p in (target_positions or []):
                    sym = str(p.get("symbol") or "").strip().upper()
                    if not sym:
                        continue
                    if sym not in wmap:
                        continue
                    nw = float(wmap.get(sym) or 0.0)
                    if nw <= 0.0:
                        continue
                    p = dict(p)
                    p["weight"] = float(nw)
                    # keep side consistent
                    if smap.get(sym) in ("LONG", "SHORT"):
                        p["side"] = smap.get(sym)
                    new_positions.append(p)
                positions = new_positions

            transition_costs = _estimate_transition_trade_costs(
                con,
                live_positions,
                positions,
                float(equity),
                int(ts_ms),
            )

            gross = sum(abs(float(p.get("weight", 0.0))) for p in positions)
            gross = float(gross if gross > 1e-12 else 1.0)

            step_ret = 0.0
            for p in positions:
                eid = p.get("event_id")
                if eid is None:
                    continue
                impact = _realized_label(con, int(eid), p.get("symbol"), int(p.get("horizon_s", 0)))
                if impact is None:
                    continue
                w = float(_safe_f(p.get("weight"), 0.0))
                side = str(p.get("side") or "").upper()
                signed = w if side == "LONG" else -w
                step_ret += (signed / gross) * float(impact)

            exec_cost = float(transition_costs.get("exec_cost") or 0.0)
            slippage = float(transition_costs.get("slippage") or 0.0)
            fees = float(transition_costs.get("fees") or 0.0)

            equity *= (1.0 + float(step_ret))
            equity -= float(exec_cost)

            if equity > peak:
                peak = float(equity)
            drawdown = (float(equity) / float(peak) - 1.0) if peak > 1e-12 else 0.0
            if drawdown < max_dd:
                max_dd = float(drawdown)

            detail = {
                "positions": positions,
                "lookback_s": int(lookback_s),
                "gross_abs_weight": float(gross),
                "exec_cost": float(exec_cost),
                "slippage": float(slippage),
                "fees": float(fees),
                "trade_costs": list(transition_costs.get("trade_costs") or []),

                # TSE snapshot for this step (if present)
                "tse_state": tse_state,
            }

            curve.append((int(ts_ms), float(step_ret), float(equity), float(drawdown), detail))
            live_positions = [dict(p) for p in positions]

        equity_end = float(equity)
        start_cap = float(start_capital) if float(start_capital) > 1e-12 else 1.0
        total_return = (equity_end / start_cap - 1.0)

        risk = _risk_metrics_from_curve(curve, total_return=float(total_return), max_drawdown=float(max_dd))

        total_exec_cost = sum(float((detail or {}).get("exec_cost") or 0.0) for _, _, _, _, detail in curve)
        total_slippage = sum(float((detail or {}).get("slippage") or 0.0) for _, _, _, _, detail in curve)
        total_fees = sum(float((detail or {}).get("fees") or 0.0) for _, _, _, _, detail in curve)

        metrics = {
            "final_equity": float(equity_end),
            "max_drawdown": float(max_dd),
            "total_return": float(total_return),
            "steps": int(len(curve)),
            **risk,
            "total_exec_cost": float(total_exec_cost),
            "total_slippage": float(total_slippage),
            "total_fees": float(total_fees),

            # TSE / suppression validation metrics
            "suppression_blocks": int(suppression_blocks),
            "suppression_nonblocks": int(suppression_nonblocks),
            "suppression_state_counts": dict(suppression_state_counts),
        }

        if not curve:
            metrics["storage_status"] = "skipped_no_points"
            logging.info(
                "BACKTEST_COMPLETE run_id=%s final_equity=%.4f max_dd=%.4f",
                "none",
                float(metrics.get("final_equity", 0.0)),
                float(metrics.get("max_drawdown", 0.0)),
            )
            return {"ok": True, "run_id": None, "metrics": metrics, "status": "no_points"}

        run_id = int(
            run_write_txn(
                lambda write_con: _persist_backtest_results(
                    write_con,
                    now_ms=int(now_ms),
                    start_ms=int(start_ms),
                    curve=curve,
                    metrics=metrics,
                ),
                table="portfolio_bt_runs",
                operation="portfolio_backtest_store",
                context={"points": int(len(curve))},
            )
        )
        logging.info(
            "BACKTEST_COMPLETE run_id=%s final_equity=%.4f max_dd=%.4f",
            int(run_id),
            float(metrics.get("final_equity", 0.0)),
            float(metrics.get("max_drawdown", 0.0)),
        )
        return {"ok": True, "run_id": int(run_id), "metrics": metrics}

    finally:
        try:
            if con is not None:
                con.close()
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_BACKTEST_CLOSE_FAILED", e, run_id=(None if run_id is None else int(run_id)))

# -------------            -- ------------------------------------------------------
# CLI entry
# -------------            -- ------------------------------------------------------

def main():
    res = run_backtest()
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    main()
