"""
FILE: evaluate_strategies.py

Runs the built-in portfolio strategies through the shared backtest harness and
writes comparable strategy metrics. This is the source of truth for
auto-selection in `strategy_selector.py`.
"""

import os
import json
import time
import math
import logging
from typing import List, Dict, Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect, init_db

import engine.strategy.portfolio_backtest as portfolio_backtest


# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [evaluate_strategies] %(message)s",
)
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


# ----------------------------------------------------------------------
# Metrics helpers
# ----------------------------------------------------------------------

def _sharpe(returns: List[float], eps: float = 1e-9) -> float:
    if not returns:
        return 0.0
    mu = sum(returns) / len(returns)
    var = sum((r - mu) ** 2 for r in returns) / max(1, (len(returns) - 1))
    sd = math.sqrt(max(var, eps))
    return mu / sd if sd > 0 else 0.0


def _sortino(returns: List[float], eps: float = 1e-9) -> float:
    if not returns:
        return 0.0
    mu = sum(returns) / len(returns)
    neg = [r for r in returns if r < 0]
    if not neg:
        return mu / eps
    var = sum(r ** 2 for r in neg) / max(1, len(neg))
    sd = math.sqrt(max(var, eps))
    return mu / sd if sd > 0 else 0.0


def _max_drawdown(equity: List[float]) -> float:
    peak = None
    max_dd = 0.0
    for e in equity or []:
        try:
            x = float(e)
        except Exception as e:
            _warn_nonfatal(
                "evaluate_strategies_equity_parse_failed",
                e,
                once_key="equity_parse",
                value=repr(e)[:120],
            )
            continue
        if peak is None or x > peak:
            peak = x
        if peak is not None:
            max_dd = min(max_dd, (x - peak))
    return abs(float(max_dd))


# ----------------------------------------------------------------------
# Strategy runner
# ----------------------------------------------------------------------

def _run(strategy: str) -> Dict[str, Any]:
    os.environ["BT_STRATEGY"] = str(strategy)
    res = portfolio_backtest.run_backtest()

    print(f"\n=== STRATEGY {strategy} ===")
    print(json.dumps(res.get("metrics", {}), indent=2))

    return res


def _load_bt_points(con, run_id: int) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT ts_ms, ret, equity, drawdown, detail_json
        FROM portfolio_bt_points
        WHERE run_id=?
        ORDER BY ts_ms ASC
        """,
        (int(run_id),),
    ).fetchall() or []

    out: List[Dict[str, Any]] = []
    for ts_ms, ret, equity, drawdown, detail_json in rows:
        try:
            d = json.loads(detail_json) if detail_json else {}
        except Exception:
            d = {}
        out.append(
            {
                "ts_ms": int(ts_ms or 0),
                "ret": float(ret or 0.0),
                "equity": float(equity or 0.0),
                "drawdown": float(drawdown or 0.0),
                "detail": d if isinstance(d, dict) else {},
            }
        )
    return out


def _symbols_from_bt_points(points: List[Dict[str, Any]]) -> List[str]:
    syms = set()
    for p in points or []:
        d = p.get("detail") or {}
        if not isinstance(d, dict):
            continue
        pos = d.get("positions")
        if isinstance(pos, dict):
            for k in pos.keys():
                if k:
                    syms.add(str(k).strip().upper())
    return sorted(syms)


def _alpha_decay_penalty(con, symbols: List[str], start_ts_ms: int, end_ts_ms: int) -> Dict[str, Any]:
    """
    Decay-aware Sharpe adjustment using realized decay metrics captured in alpha_decay_metrics.
    Penalty = (1 - expired_rate). If table missing / no rows => penalty=1.0 (fail-open).
    """
    if not symbols:
        return {"ok": True, "penalty": 1.0, "n": 0, "expired_rate": None, "median_ttm_ms": None}

    try:
        con.execute("SELECT 1 FROM alpha_decay_metrics LIMIT 1").fetchone()
    except Exception as e:
        _warn_nonfatal(
            "evaluate_strategies_alpha_decay_table_probe_failed",
            e,
            once_key="alpha_decay_table_probe",
        )
        return {
            "ok": False,
            "penalty": 1.0,
            "n": 0,
            "expired_rate": None,
            "median_ttm_ms": None,
            "reason": "no_alpha_decay_metrics_table",
        }

    # chunk IN (...) to avoid sqlite param limits
    syms = [str(s).strip().upper() for s in symbols if s]
    syms = [s for s in syms if s]
    if not syms:
        return {"ok": True, "penalty": 1.0, "n": 0, "expired_rate": None, "median_ttm_ms": None}

    rows_all = []
    chunk = 250
    for i in range(0, len(syms), chunk):
        part = syms[i : i + chunk]
        q_marks = ",".join(["?"] * len(part))
        rows = con.execute(
            f"""
            SELECT metrics_json
            FROM alpha_decay_metrics
            WHERE symbol IN ({q_marks})
              AND ts_ms BETWEEN ? AND ?
            """,
            (*part, int(start_ts_ms), int(end_ts_ms)),
        ).fetchall() or []
        rows_all.extend(rows)

    expired = 0
    ttm = []
    n = 0

    for (mj,) in rows_all:
        if not mj:
            continue
        try:
            m = json.loads(mj)
        except Exception as e:
            _warn_nonfatal(
                "evaluate_strategies_meta_json_parse_failed",
                e,
                once_key="meta_json_parse",
                meta_json=str(mj)[:200],
            )
            continue
        if not isinstance(m, dict):
            continue

        n += 1

        try:
            if bool(m.get("expired")):
                expired += 1
        except Exception as e:
            _warn_nonfatal(
                "EVALUATE_STRATEGIES_EXPIRED_FLAG_PARSE_FAILED",
                e,
                once_key="alpha_decay_expired_flag_parse",
                metrics_json_preview=str(mj)[:200],
            )

        v = m.get("time_to_mfe_ms")
        if v is not None:
            try:
                v = int(v)
                if v > 0:
                    ttm.append(v)
            except Exception as e:
                _warn_nonfatal(
                    "EVALUATE_STRATEGIES_TIME_TO_MFE_PARSE_FAILED",
                    e,
                    once_key="alpha_decay_time_to_mfe_parse",
                    raw_value=v,
                    metrics_json_preview=str(mj)[:200],
                )

    if n <= 0:
        return {"ok": True, "penalty": 1.0, "n": 0, "expired_rate": None, "median_ttm_ms": None}

    expired_rate = float(expired) / float(n)

    median_ttm_ms = None
    if ttm:
        ttm_sorted = sorted(ttm)
        median_ttm_ms = int(ttm_sorted[len(ttm_sorted) // 2])

    penalty = max(0.0, min(1.0, 1.0 - float(expired_rate)))
    return {
        "ok": True,
        "penalty": float(penalty),
        "n": int(n),
        "expired_rate": float(expired_rate),
        "median_ttm_ms": median_ttm_ms,
    }


# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------

def main() -> int:
    init_db()

    con = connect()
    try:
        # Ensure schema
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_metrics (
              strategy TEXT NOT NULL,
              metrics_json TEXT NOT NULL,
              ts_ms INTEGER NOT NULL
            )
            """
        )
        try:
            con.execute(
                "CREATE INDEX IF NOT EXISTS idx_strategy_metrics_ts "
                "ON strategy_metrics(ts_ms)"
            )
        except Exception as e:
            _warn_nonfatal(
                "EVALUATE_STRATEGIES_INDEX_CREATE_FAILED",
                e,
                once_key="strategy_metrics_index_create",
            )
        con.commit()

        for name in ("baseline", "conservative"):
            res = _run(name)

            if not bool(res.get("ok")):
                _warn_nonfatal(
                    "EVALUATE_STRATEGIES_BACKTEST_FAILED",
                    RuntimeError(str(res.get("error") or "backtest_failed")),
                    once_key=f"backtest_failed:{name}",
                    strategy_name=str(name),
                    result=json.dumps(res, separators=(",", ":"))[:400],
                )
                continue

            run_id = int(res.get("run_id") or 0)
            if run_id <= 0:
                _warn_nonfatal(
                    "EVALUATE_STRATEGIES_RUN_ID_MISSING",
                    RuntimeError("missing_run_id"),
                    once_key=f"run_id_missing:{name}",
                    strategy_name=str(name),
                    result=json.dumps(res, separators=(",", ":"))[:400],
                )
                continue

            points = _load_bt_points(con, int(run_id))
            returns = [float(x.get("ret", 0.0)) for x in points]
            equity = [float(x.get("equity", 0.0)) for x in points]

            pnl = sum(returns)
            sharpe = _sharpe(returns)
            sortino = _sortino(returns)
            max_dd = _max_drawdown(equity)

            # decay-aware Sharpe: apply penalty based on alpha_decay_metrics expired rate
            start_ts_ms = int(points[0]["ts_ms"]) if points else 0
            end_ts_ms = int(points[-1]["ts_ms"]) if points else 0
            syms = _symbols_from_bt_points(points)
            decay = _alpha_decay_penalty(con, syms, int(start_ts_ms), int(end_ts_ms))
            decay_penalty = float(decay.get("penalty") or 1.0)
            sharpe_decay = float(sharpe) * float(decay_penalty)

            metrics = {
                "pnl": float(pnl),
                "sharpe": float(sharpe),
                "sharpe_decay": float(sharpe_decay),
                "decay_penalty": float(decay_penalty),
                "decay_rows": int(decay.get("n") or 0),
                "decay_expired_rate": decay.get("expired_rate"),
                "decay_median_time_to_mfe_ms": decay.get("median_ttm_ms"),
                "sortino": float(sortino),
                "max_drawdown": float(max_dd),
                "n_points": int(len(points)),
                "run_id": int(run_id),
                "symbols_traded": syms,
            }

            con.execute(
                """
                INSERT INTO strategy_metrics(strategy, metrics_json, ts_ms)
                VALUES (?, ?, ?)
                """,
                (
                    str(name),
                    json.dumps(metrics, separators=(",", ":"), sort_keys=True),
                    int(time.time() * 1000),
                ),
            )
            con.commit()

            logging.info(
                "STRATEGY %s pnl=%.4f sharpe=%.3f sharpe_decay=%.3f sortino=%.3f max_dd=%.4f decay_pen=%.3f",
                str(name),
                float(pnl),
                float(sharpe),
                float(sharpe_decay),
                float(sortino),
                float(max_dd),
                float(decay_penalty),
            )

        print("\nDONE: strategy_metrics updated (see runtime storage strategy_metrics table)")
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
