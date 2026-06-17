"""
FILE: drawdown_state.py

Read helpers for portfolio drawdown state. This module computes current
drawdown from persisted equity history and exposes a simple short-horizon
velocity proxy for higher-level risk controls.
"""

import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from typing import Any

from engine.audit.chain import append_chain_row
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import _table_exists, connect, init_db

LOG = get_logger("strategy.drawdown_state")
_WARNED_NONFATAL_KEYS: set[str] = set()
DRAWDOWN_MIN_HISTORY_POINTS = int(os.environ.get("DRAWDOWN_MIN_HISTORY_POINTS", "5"))
DRAWDOWN_BOOTSTRAP_MAX_AGE_S = int(os.environ.get("DRAWDOWN_BOOTSTRAP_MAX_AGE_S", "86400"))


@dataclass(frozen=True)
class DrawdownDiagnostic:
    ok: bool
    drawdown: float | None
    reason_code: str
    source: str
    history_points: int
    min_history_points: int
    latest_equity: float | None = None
    peak_equity: float | None = None
    latest_ts_ms: int | None = None
    invalid_points: int = 0
    bootstrap_id: int | None = None
    bootstrap_actor: str | None = None
    bootstrap_reason: str | None = None
    bootstrap_expires_ts_ms: int | None = None
    table_present: bool | None = None
    error_type: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class DrawdownStateUnavailable(RuntimeError):
    """Raised when drawdown cannot be trusted for live risk gating."""

    def __init__(self, diagnostic: DrawdownDiagnostic):
        self.diagnostic = diagnostic
        super().__init__(str(diagnostic.reason_code))


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_drawdown_state_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.drawdown_state",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _ensure_bootstrap_schema(con) -> None:
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS drawdown_bootstrap_baseline (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts_ms INTEGER NOT NULL,
          baseline_equity REAL NOT NULL,
          actor TEXT NOT NULL,
          reason TEXT NOT NULL,
          source TEXT NOT NULL,
          expires_ts_ms INTEGER,
          detail_json TEXT NOT NULL,
          prev_hash BLOB,
          row_hash BLOB NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_drawdown_bootstrap_baseline_ts
          ON drawdown_bootstrap_baseline(ts_ms);
        """
    )


def record_drawdown_bootstrap_baseline(
    *,
    baseline_equity: float,
    actor: str,
    reason: str,
    source: str = "operator",
    ttl_s: int | None = None,
    detail: dict[str, Any] | None = None,
    con=None,
) -> dict[str, Any]:
    """Record an explicit audited baseline for a new live account."""

    actor_s = str(actor or "").strip()
    reason_s = str(reason or "").strip()
    if not actor_s:
        raise ValueError("drawdown_bootstrap_actor_required")
    if not reason_s:
        raise ValueError("drawdown_bootstrap_reason_required")
    equity_f = float(baseline_equity)
    if equity_f <= 0.0:
        raise ValueError("drawdown_bootstrap_baseline_equity_positive_required")

    now_ms = _now_ms()
    ttl_i = DRAWDOWN_BOOTSTRAP_MAX_AGE_S if ttl_s is None else int(ttl_s)
    expires_ts_ms = int(now_ms + max(0, ttl_i) * 1000) if ttl_i > 0 else None
    payload = {
        "ts_ms": int(now_ms),
        "baseline_equity": float(equity_f),
        "actor": actor_s,
        "reason": reason_s,
        "source": str(source or "operator").strip() or "operator",
        "expires_ts_ms": expires_ts_ms,
        "detail_json": json.dumps(dict(detail or {}), separators=(",", ":"), sort_keys=True),
    }

    owns = False
    if con is None:
        init_db()
        con = connect()
        owns = True
    caller_txn = bool(getattr(con, "in_transaction", False))
    try:
        _ensure_bootstrap_schema(con)
        result = append_chain_row("drawdown_bootstrap_baseline", payload, con)
        if owns or not caller_txn:
            con.commit()
        out = dict(payload)
        out["id"] = result.row_id
        out["row_hash"] = result.row_hash_hex
        out["prev_hash"] = result.prev_hash_hex
        return out
    finally:
        if owns:
            con.close()


def _active_bootstrap_baseline(con, *, now_ms: int) -> dict[str, Any] | None:
    try:
        if not _table_exists(con, "drawdown_bootstrap_baseline"):
            return None
        row = con.execute(
            """
            SELECT id, ts_ms, baseline_equity, actor, reason, source, expires_ts_ms, detail_json
            FROM drawdown_bootstrap_baseline
            WHERE expires_ts_ms IS NULL OR expires_ts_ms >= ?
            ORDER BY ts_ms DESC, id DESC
            LIMIT 1
            """,
            (int(now_ms),),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "DRAWDOWN_STATE_BOOTSTRAP_BASELINE_READ_FAILED",
            e,
            once_key="bootstrap_baseline_read",
        )
        return None

    if not row:
        return None
    try:
        return {
            "id": int(row[0] or 0),
            "ts_ms": int(row[1] or 0),
            "baseline_equity": float(row[2] or 0.0),
            "actor": str(row[3] or ""),
            "reason": str(row[4] or ""),
            "source": str(row[5] or ""),
            "expires_ts_ms": (int(row[6]) if row[6] is not None else None),
            "detail": json.loads(row[7] or "{}") if row[7] else {},
        }
    except Exception as e:
        _warn_nonfatal(
            "DRAWDOWN_STATE_BOOTSTRAP_BASELINE_PARSE_FAILED",
            e,
            once_key="bootstrap_baseline_parse",
        )
        return None


def _bootstrap_diagnostic(
    con,
    *,
    now_ms: int,
    reason_code: str,
    history_points: int,
    min_history_points: int,
    table_present: bool | None,
) -> DrawdownDiagnostic | None:
    baseline = _active_bootstrap_baseline(con, now_ms=int(now_ms))
    if not baseline:
        return None
    equity = float(baseline.get("baseline_equity") or 0.0)
    if equity <= 0.0:
        return None
    return DrawdownDiagnostic(
        ok=True,
        drawdown=0.0,
        reason_code="DRAWDOWN_BOOTSTRAP_BASELINE",
        source="drawdown_bootstrap_baseline",
        history_points=int(history_points),
        min_history_points=int(min_history_points),
        latest_equity=float(equity),
        peak_equity=float(equity),
        latest_ts_ms=int(baseline.get("ts_ms") or now_ms),
        bootstrap_id=int(baseline.get("id") or 0),
        bootstrap_actor=str(baseline.get("actor") or ""),
        bootstrap_reason=str(baseline.get("reason") or ""),
        bootstrap_expires_ts_ms=baseline.get("expires_ts_ms"),
        table_present=table_present,
        error=reason_code,
    )


def evaluate_current_drawdown(
    con=None,
    *,
    min_history_points: int | None = None,
    allow_bootstrap: bool = True,
    now_ms: int | None = None,
) -> DrawdownDiagnostic:
    """
    Compute current drawdown and return structured trust diagnostics.

    Missing, sparse, invalid, or unreadable equity history is not converted to
    a safe-looking zero. Live trading gates must fail closed unless an active
    audited bootstrap baseline is present.
    """
    min_points = max(1, int(min_history_points or DRAWDOWN_MIN_HISTORY_POINTS))
    ts_now = int(now_ms or _now_ms())
    owns = False
    if con is None:
        init_db()
        con = connect()
        owns = True
    try:
        try:
            table_present = bool(_table_exists(con, "equity_history"))
        except Exception as err:
            _warn_nonfatal(
                "DRAWDOWN_STATE_EQUITY_HISTORY_TABLE_CHECK_FAILED",
                err,
                once_key="equity_history_table_check",
            )
            return DrawdownDiagnostic(
                ok=False,
                drawdown=None,
                reason_code="DRAWDOWN_EQUITY_HISTORY_READ_ERROR",
                source="equity_history",
                history_points=0,
                min_history_points=int(min_points),
                table_present=None,
                error_type=type(err).__name__,
                error=str(err),
            )

        if not table_present:
            if allow_bootstrap:
                boot = _bootstrap_diagnostic(
                    con,
                    now_ms=ts_now,
                    reason_code="DRAWDOWN_EQUITY_HISTORY_MISSING",
                    history_points=0,
                    min_history_points=min_points,
                    table_present=False,
                )
                if boot is not None:
                    return boot
            return DrawdownDiagnostic(
                ok=False,
                drawdown=None,
                reason_code="DRAWDOWN_EQUITY_HISTORY_MISSING",
                source="equity_history",
                history_points=0,
                min_history_points=int(min_points),
                table_present=False,
            )

        try:
            rows = con.execute(
                "SELECT ts_ms, equity FROM equity_history ORDER BY ts_ms ASC"
            ).fetchall()
        except Exception as err:
            _warn_nonfatal(
                "DRAWDOWN_STATE_EQUITY_HISTORY_READ_FAILED",
                err,
                once_key="equity_history_read",
            )
            return DrawdownDiagnostic(
                ok=False,
                drawdown=None,
                reason_code="DRAWDOWN_EQUITY_HISTORY_READ_ERROR",
                source="equity_history",
                history_points=0,
                min_history_points=int(min_points),
                table_present=True,
                error_type=type(err).__name__,
                error=str(err),
            )

        history_points = int(len(rows or []))
        if history_points < int(min_points):
            if allow_bootstrap:
                boot = _bootstrap_diagnostic(
                    con,
                    now_ms=ts_now,
                    reason_code="DRAWDOWN_EQUITY_HISTORY_INSUFFICIENT",
                    history_points=history_points,
                    min_history_points=min_points,
                    table_present=True,
                )
                if boot is not None:
                    return boot
            return DrawdownDiagnostic(
                ok=False,
                drawdown=None,
                reason_code="DRAWDOWN_EQUITY_HISTORY_INSUFFICIENT",
                source="equity_history",
                history_points=history_points,
                min_history_points=int(min_points),
                table_present=True,
            )

        peak = 0.0
        cur = 0.0
        latest_ts_ms = 0
        valid_points = 0
        invalid_points = 0
        for row in rows:
            try:
                ts_ms = int(row[0] or 0)
                eq = row[1]
                e = float(eq or 0.0)
            except Exception as err:
                invalid_points += 1
                _warn_nonfatal(
                    "DRAWDOWN_STATE_EQUITY_PARSE_FAILED",
                    err,
                    once_key="equity_parse",
                    value=repr(eq)[:120],
                )
                continue
            valid_points += 1
            if e > peak:
                peak = e
            cur = e
            latest_ts_ms = int(ts_ms)

        if valid_points < int(min_points):
            if allow_bootstrap:
                boot = _bootstrap_diagnostic(
                    con,
                    now_ms=ts_now,
                    reason_code="DRAWDOWN_EQUITY_HISTORY_INSUFFICIENT_VALID_POINTS",
                    history_points=valid_points,
                    min_history_points=min_points,
                    table_present=True,
                )
                if boot is not None:
                    return boot
            return DrawdownDiagnostic(
                ok=False,
                drawdown=None,
                reason_code="DRAWDOWN_EQUITY_HISTORY_INSUFFICIENT_VALID_POINTS",
                source="equity_history",
                history_points=int(valid_points),
                min_history_points=int(min_points),
                invalid_points=int(invalid_points),
                table_present=True,
            )

        if peak <= 0 or cur <= 0:
            return DrawdownDiagnostic(
                ok=False,
                drawdown=None,
                reason_code="DRAWDOWN_EQUITY_HISTORY_INVALID_EQUITY",
                source="equity_history",
                history_points=int(valid_points),
                min_history_points=int(min_points),
                latest_equity=float(cur),
                peak_equity=float(peak),
                latest_ts_ms=int(latest_ts_ms or 0),
                invalid_points=int(invalid_points),
                table_present=True,
            )

        dd = 1.0 - (cur / peak)
        if dd < 0.0:
            dd = 0.0
        if dd > 1.0:
            dd = 1.0
        return DrawdownDiagnostic(
            ok=True,
            drawdown=float(dd),
            reason_code="DRAWDOWN_OK",
            source="equity_history",
            history_points=int(valid_points),
            min_history_points=int(min_points),
            latest_equity=float(cur),
            peak_equity=float(peak),
            latest_ts_ms=int(latest_ts_ms or 0),
            invalid_points=int(invalid_points),
            table_present=True,
        )
    finally:
        if owns:
            con.close()


def get_current_drawdown(con=None) -> float:
    """
    Compute current drawdown from trusted equity history.

    Raises ``DrawdownStateUnavailable`` when history is missing, sparse,
    invalid, or unreadable and no active audited bootstrap baseline exists.
    """
    diagnostic = evaluate_current_drawdown(con)
    if not diagnostic.ok:
        raise DrawdownStateUnavailable(diagnostic)
    return float(diagnostic.drawdown or 0.0)

def get_drawdown_velocity(con):
    try:
        rows = con.execute(
            """
            SELECT drawdown
            FROM equity_snapshots
            ORDER BY ts_ms DESC
            LIMIT 5
            """
        ).fetchall()
        if not rows or len(rows) < 2:
            return 0.0
        latest = float(rows[0][0] or 0.0)
        prev = float(rows[1][0] or 0.0)
        return abs(latest - prev)
    except Exception as e:
        _warn_nonfatal(
            "DRAWDOWN_STATE_GET_DRAWDOWN_VELOCITY_FAILED",
            e,
            once_key="drawdown_velocity",
        )
        return 0.0
