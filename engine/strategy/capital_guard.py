"""
FILE: capital_guard.py

Owns account-level trading stops and capital-preservation mode. This is where
drawdown, market stress, and execution degradation can suspend or compress
trading without changing the underlying signal models.
"""

import json
import logging
import os
import time
from typing import Dict, Any

from engine.runtime.failure_diagnostics import log_failure
from engine.strategy.drawdown_state import evaluate_current_drawdown
from engine.runtime.risk_state import get_state, set_state
from engine.runtime.storage import _table_exists, connect

LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


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
        component="engine.strategy.capital_guard",
        extra=extra or None,
        include_health=False,
        persist=False,
    )

# thresholds (hard stop)
MAX_DRAWDOWN = float(os.environ.get("CAPITAL_STOP_DRAWDOWN", "0.25"))  # 25%
COOLDOWN_DAYS = int(os.environ.get("CAPITAL_COOLDOWN_DAYS", "5"))

# -----------------------------
# Capital Preservation Mode (CPM)
# -----------------------------
CAPITAL_PRESERVE_DD_VELOCITY = float(os.environ.get("CAPITAL_PRESERVE_DD_VELOCITY", "0.02"))
CAPITAL_PRESERVE_STRESS_SCORE = float(os.environ.get("CAPITAL_PRESERVE_STRESS_SCORE", "0.75"))

# execution degradation triggers (best-effort from execution_analytics)
CAPITAL_PRESERVE_EXEC_COST_BPS = float(os.environ.get("CAPITAL_PRESERVE_EXEC_COST_BPS", "18.0"))
CAPITAL_PRESERVE_EXEC_LAT_MS = float(os.environ.get("CAPITAL_PRESERVE_EXEC_LAT_MS", "1200"))
CAPITAL_PRESERVE_EXEC_LOOKBACK_H = float(os.environ.get("CAPITAL_PRESERVE_EXEC_LOOKBACK_H", "24"))

# exit hysteresis / flip-flop control
CAPITAL_PRESERVE_MIN_DURATION_S = int(os.environ.get("CAPITAL_PRESERVE_MIN_DURATION_S", "1800"))
CAPITAL_PRESERVE_EXIT_STREAK = int(os.environ.get("CAPITAL_PRESERVE_EXIT_STREAK", "3"))

# regime-transition trigger
CAPITAL_PRESERVE_REGIME_TRANSITION = os.environ.get("CAPITAL_PRESERVE_REGIME_TRANSITION", "1") == "1"
CAPITAL_PRESERVE_REGIME_SYMBOL = str(os.environ.get("CAPITAL_PRESERVE_REGIME_SYMBOL", "SPY") or "SPY").strip() or "SPY"
CAPITAL_PRESERVE_REGIME_MIN_CONF = float(os.environ.get("CAPITAL_PRESERVE_REGIME_MIN_CONF", "0.55"))


def _live_mode_requested(con=None) -> bool | None:
    for name in ("EXECUTION_MODE", "ENGINE_MODE", "OPERATOR_MODE", "MODE"):
        if str(os.environ.get(name, "") or "").strip().lower() == "live":
            return True
    if con is None:
        return False
    try:
        if not _table_exists(con, "execution_mode"):
            return False
        row = con.execute("SELECT mode FROM execution_mode WHERE id=1").fetchone()
        return bool(row and str(row[0] or "").strip().lower() == "live")
    except Exception as e:
        _warn_nonfatal("CAPITAL_GUARD_LIVE_MODE_CHECK_FAILED", e, once_key="live_mode_check")
        return None


def _drawdown_payload(diagnostic) -> Dict[str, Any]:
    try:
        if hasattr(diagnostic, "to_dict"):
            return dict(diagnostic.to_dict())
        return dict(diagnostic or {})
    except Exception as e:
        _warn_nonfatal("CAPITAL_GUARD_DRAWDOWN_DIAGNOSTIC_PAYLOAD_FAILED", e, once_key="drawdown_diag_payload")
        return {"ok": False, "reason_code": "DRAWDOWN_DIAGNOSTIC_PAYLOAD_FAILED"}


def _persist_drawdown_diagnostic(diagnostic) -> None:
    payload = _drawdown_payload(diagnostic)
    try:
        set_state("capital_drawdown_diagnostic_json", json.dumps(payload, separators=(",", ":"), sort_keys=True))
        set_state("capital_drawdown_status", str(payload.get("reason_code") or "UNKNOWN"))
    except Exception as e:
        _warn_nonfatal(
            "CAPITAL_GUARD_DRAWDOWN_DIAGNOSTIC_STATE_SET_FAILED",
            e,
            once_key="drawdown_diagnostic_state_set",
        )


def _stop_for_untrusted_drawdown(diagnostic) -> None:
    payload = _drawdown_payload(diagnostic)
    reason_code = str(payload.get("reason_code") or "DRAWDOWN_STATE_UNAVAILABLE")
    _persist_drawdown_diagnostic(payload)
    try:
        set_state("trading_state", "stopped")
        set_state("stop_reason", f"drawdown_state_unavailable:{reason_code}")
        set_state("stop_ts_ms", str(int(time.time() * 1000)))
    except Exception as e:
        _warn_nonfatal(
            "CAPITAL_GUARD_DRAWDOWN_UNAVAILABLE_STOP_STATE_SET_FAILED",
            e,
            once_key="drawdown_unavailable_stop_state_set",
            reason_code=reason_code,
        )


def trading_allowed(con=None) -> bool:
    """Return whether account-level trading should remain enabled.

    Parameters
    ----------
    con : storage connection, optional
        Existing database connection forwarded to drawdown calculations.

    Returns
    -------
    bool
        ``False`` when trading has already been stopped or when current
        drawdown is greater than or equal to ``MAX_DRAWDOWN``. ``MAX_DRAWDOWN``
        is expressed as a fraction of equity, not a percentage integer.

    Notes
    -----
    The hard stop is sticky. Once triggered, separate cooldown logic must
    restore ``trading_state`` before execution can resume.

    Side Effects
    ------------
    On a fresh breach, writes ``trading_state``, ``stop_reason``, and
    ``stop_ts_ms`` into runtime risk state.
    """
    # Hard stop is sticky until separate cooldown logic releases it.
    state = get_state("trading_state", "enabled")
    if state != "enabled":
        return False

    diagnostic = evaluate_current_drawdown(con)
    _persist_drawdown_diagnostic(diagnostic)
    if not diagnostic.ok:
        reason_code = str(getattr(diagnostic, "reason_code", "") or "")
        if reason_code.endswith("_READ_ERROR"):
            _stop_for_untrusted_drawdown(diagnostic)
            return False
        live_mode = _live_mode_requested(con)
        if live_mode is not False:
            _stop_for_untrusted_drawdown(diagnostic)
            return False
        return True

    dd = float(diagnostic.drawdown or 0.0)
    if dd >= MAX_DRAWDOWN:
        set_state("trading_state", "stopped")
        set_state("stop_reason", f"drawdown={dd:.2%}")
        set_state("stop_ts_ms", str(int(time.time() * 1000)))
        return False

    return True


def maybe_release_cooldown(con=None):
    """
    Re-enable trading after cooldown days AND drawdown improved.
    """
    state = get_state("trading_state", "enabled")
    if state != "stopped":
        return

    ts = int(get_state("stop_ts_ms", "0") or "0")
    if ts <= 0:
        return

    days = (time.time() * 1000 - ts) / (86400 * 1000)
    if days < COOLDOWN_DAYS:
        return

    diagnostic = evaluate_current_drawdown(con)
    _persist_drawdown_diagnostic(diagnostic)
    if not diagnostic.ok:
        return

    dd = float(diagnostic.drawdown or 0.0)
    if dd < MAX_DRAWDOWN * 0.75:
        set_state("trading_state", "enabled")
        set_state("stop_reason", "")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _drawdown_velocity(con=None) -> float:
    """
    Velocity proxy: Δdrawdown since last call.
    Persist last snapshot in risk_state for stability across processes.

    NOTE: this preserves the existing raw-delta behavior rather than turning it
    into a true time-normalized velocity metric.
    """
    try:
        prev = float(get_state("capital_prev_drawdown", "0") or "0")
    except Exception:
        prev = 0.0
    try:
        diagnostic = evaluate_current_drawdown(con)
        _persist_drawdown_diagnostic(diagnostic)
        if not diagnostic.ok:
            return float(CAPITAL_PRESERVE_DD_VELOCITY)
        cur = float(diagnostic.drawdown or 0.0)
    except Exception:
        cur = float(CAPITAL_PRESERVE_DD_VELOCITY)
    try:
        set_state("capital_prev_drawdown", str(cur))
    except Exception as e:
        _warn_nonfatal("CAPITAL_GUARD_SET_PREV_DRAWDOWN_FAILED", e, once_key="set_prev_drawdown")
    return float(cur - prev)


def _stress_snapshot(con=None) -> Dict[str, Any]:
    try:
        from engine.strategy.market_stress import get_market_stress_snapshot
    except Exception as e:
        _warn_nonfatal("CAPITAL_GUARD_MARKET_STRESS_IMPORT_FAILED", e, once_key="market_stress_import")
        return {"stress_score": 0.0}

    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        return get_market_stress_snapshot(con=con, ts_ms=_now_ms()) or {"stress_score": 0.0}
    except Exception as e:
        _warn_nonfatal("CAPITAL_GUARD_MARKET_STRESS_SNAPSHOT_FAILED", e, once_key="market_stress_snapshot")
        return {"stress_score": 0.0}
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("CAPITAL_GUARD_STRESS_SNAPSHOT_CLOSE_FAILED", e, once_key="stress_snapshot_close")


def _exec_degradation_snapshot(con=None) -> Dict[str, Any]:
    """
    Best-effort query from execution_analytics (if present).
    Returns:
      ok, avg_total_cost_bps, avg_slippage_bps, avg_latency_ms, n

    Preserves existing behavior/shape exactly, but adds safety:
      - fail-soft if table/columns do not exist
    """
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        since = _now_ms() - int(float(CAPITAL_PRESERVE_EXEC_LOOKBACK_H) * 3600.0 * 1000.0)

        # Guard: execution_analytics might not exist yet
        try:
            chk = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='execution_analytics'"
            ).fetchone()
            if not chk:
                return {
                    "ok": False,
                    "n": 0,
                    "avg_total_cost_bps": 0.0,
                    "avg_slippage_bps": 0.0,
                    "avg_latency_ms": 0.0,
                }
        except Exception as e:
            # if master check fails, continue best-effort
            _warn_nonfatal("CAPITAL_GUARD_EXECUTION_SNAPSHOT_MASTER_CHECK_FAILED", e, once_key="execution_snapshot_master_check")

        try:
            row = con.execute(
                """
                SELECT
                  COUNT(*) AS n,
                  AVG(total_cost_bps) AS avg_cost,
                  AVG(slippage_bps) AS avg_slip,
                  AVG(age_ms) AS avg_age
                FROM execution_analytics
                WHERE ts_ms >= ?
                """,
                (int(since),),
            ).fetchone()
        except Exception:
            row = None

        if not row:
            return {
                "ok": False,
                "n": 0,
                "avg_total_cost_bps": 0.0,
                "avg_slippage_bps": 0.0,
                "avg_latency_ms": 0.0,
            }

        n, avg_cost, avg_slip, avg_age = row
        return {
            "ok": True,
            "n": int(n or 0),
            "avg_total_cost_bps": float(avg_cost or 0.0),
            "avg_slippage_bps": float(avg_slip or 0.0),
            "avg_latency_ms": float(avg_age or 0.0),
        }
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("CAPITAL_GUARD_EXECUTION_SNAPSHOT_CLOSE_FAILED", e, once_key="execution_snapshot_close")


def _ensure_cpm_tables(con) -> None:
    try:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS capital_preservation_audit (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              mode TEXT NOT NULL,
              changed INTEGER NOT NULL DEFAULT 0,
              reason TEXT,
              detail_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_cpm_audit_ts
              ON capital_preservation_audit(ts_ms);
            """
        )
    except Exception as e:
        _warn_nonfatal("CAPITAL_GUARD_ENSURE_CPM_TABLES_FAILED", e, once_key="ensure_cpm_tables")


def _regime_transition_snapshot(con=None) -> Dict[str, Any]:
    owns = False
    if con is None:
        con = connect()
        owns = True
    try:
        try:
            row = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='social_regimes'"
            ).fetchone()
            if not row:
                return {"ok": False, "transition": False}
        except Exception as e:
            _warn_nonfatal("CAPITAL_GUARD_SOCIAL_REGIMES_TABLE_CHECK_FAILED", e, once_key="social_regimes_table_check")
            return {"ok": False, "transition": False}

        rows = con.execute(
            """
            SELECT regime, regime_conf, bucket_ts_ms
            FROM social_regimes
            WHERE symbol=?
            ORDER BY bucket_ts_ms DESC
            LIMIT 2
            """,
            (str(CAPITAL_PRESERVE_REGIME_SYMBOL),),
        ).fetchall()

        if not rows or len(rows) < 2:
            return {"ok": False, "transition": False}

        cur_regime, cur_conf, cur_ts_ms = rows[0]
        prev_regime, prev_conf, prev_ts_ms = rows[1]

        cur_regime = str(cur_regime or "").strip()
        prev_regime = str(prev_regime or "").strip()

        try:
            cur_conf = float(cur_conf or 0.0)
        except Exception:
            cur_conf = 0.0

        transition = (
            bool(CAPITAL_PRESERVE_REGIME_TRANSITION)
            and bool(cur_regime)
            and bool(prev_regime)
            and cur_regime != prev_regime
            and cur_conf >= float(CAPITAL_PRESERVE_REGIME_MIN_CONF)
        )

        return {
            "ok": True,
            "transition": bool(transition),
            "symbol": str(CAPITAL_PRESERVE_REGIME_SYMBOL),
            "from_regime": prev_regime,
            "to_regime": cur_regime,
            "regime_conf": float(cur_conf),
            "cur_ts_ms": int(cur_ts_ms or 0),
            "prev_ts_ms": int(prev_ts_ms or 0),
        }
    except Exception as e:
        _warn_nonfatal("CAPITAL_GUARD_REGIME_TRANSITION_SNAPSHOT_FAILED", e, once_key="regime_transition_snapshot")
        return {"ok": False, "transition": False}
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("CAPITAL_GUARD_REGIME_TRANSITION_CLOSE_FAILED", e, once_key="regime_transition_close")


def _persist_cpm_snapshot(con, snapshot: Dict[str, Any]) -> None:
    try:
        set_state(
            "capital_mode_snapshot_json",
            json.dumps(snapshot or {}, separators=(",", ":"), sort_keys=True),
        )
    except Exception as e:
        _warn_nonfatal("CAPITAL_GUARD_STATE_SNAPSHOT_PERSIST_FAILED", e, once_key="state_snapshot_persist")

    try:
        con.execute(
            """
            INSERT INTO capital_preservation_audit(
              ts_ms, mode, changed, reason, detail_json
            )
            VALUES (?,?,?,?,?)
            """,
            (
                int(snapshot.get("ts_ms") or _now_ms()),
                str(snapshot.get("capital_mode") or "normal"),
                int(1 if snapshot.get("mode_changed") else 0),
                str(snapshot.get("reason") or ""),
                json.dumps(snapshot or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    except Exception as e:
        _warn_nonfatal("CAPITAL_GUARD_AUDIT_SNAPSHOT_PERSIST_FAILED", e, once_key="audit_snapshot_persist")


def update_capital_preservation_mode(con=None) -> Dict[str, Any]:
    """
    Entry/Exit rules for Capital Preservation Mode (CPM).

    Triggers:
      - drawdown velocity spikes
      - execution quality degradation (execution_analytics)
      - market stress elevation
      - regime transitions (social_regimes anchor)

    Effects are applied in:
      - engine/strategy/portfolio.py
      - engine/strategy/position_sizing.py
      - engine/execution/execution_policy_engine.py
    """
    owns = False
    if con is None:
        con = connect()
        owns = True

    try:
        _ensure_cpm_tables(con)

        now = _now_ms()

        mode = str(get_state("capital_mode", "normal") or "normal")
        ts0 = int(get_state("capital_mode_ts_ms", "0") or "0")
        reason0 = str(get_state("capital_mode_reason", "") or "")

        dd_vel = _drawdown_velocity(con)
        st = _stress_snapshot(con)
        try:
            stress_score = float(st.get("stress_score", 0.0) or 0.0)
        except Exception:
            stress_score = 0.0

        ex = _exec_degradation_snapshot(con)
        exec_bad = False
        if ex.get("ok") and int(ex.get("n") or 0) > 0:
            try:
                if float(ex.get("avg_total_cost_bps") or 0.0) >= float(CAPITAL_PRESERVE_EXEC_COST_BPS):
                    exec_bad = True
            except Exception as e:
                _warn_nonfatal("CAPITAL_GUARD_EXEC_COST_THRESHOLD_CHECK_FAILED", e, once_key="exec_cost_threshold_check")
            try:
                if float(ex.get("avg_latency_ms") or 0.0) >= float(CAPITAL_PRESERVE_EXEC_LAT_MS):
                    exec_bad = True
            except Exception as e:
                _warn_nonfatal("CAPITAL_GUARD_EXEC_LATENCY_THRESHOLD_CHECK_FAILED", e, once_key="exec_latency_threshold_check")

        regime = _regime_transition_snapshot(con)
        regime_bad = bool(regime.get("transition"))

        entry = (
            float(dd_vel) >= float(CAPITAL_PRESERVE_DD_VELOCITY)
            or float(stress_score) >= float(CAPITAL_PRESERVE_STRESS_SCORE)
            or bool(exec_bad)
            or bool(regime_bad)
        )

        # -----------------------------
        # ENTER
        # -----------------------------
        if entry and mode != "preserve":
            parts = [
                f"dd_vel={float(dd_vel):.4f}>=th={float(CAPITAL_PRESERVE_DD_VELOCITY):.4f}"
                if float(dd_vel) >= float(CAPITAL_PRESERVE_DD_VELOCITY)
                else f"dd_vel={float(dd_vel):.4f}",
                f"stress={float(stress_score):.3f}>=th={float(CAPITAL_PRESERVE_STRESS_SCORE):.3f}"
                if float(stress_score) >= float(CAPITAL_PRESERVE_STRESS_SCORE)
                else f"stress={float(stress_score):.3f}",
            ]
            if ex.get("ok"):
                parts.append(f"exec_cost_bps={float(ex.get('avg_total_cost_bps') or 0.0):.2f}")
                parts.append(f"exec_lat_ms={float(ex.get('avg_latency_ms') or 0.0):.0f}")
                parts.append(f"exec_slip_bps={float(ex.get('avg_slippage_bps') or 0.0):.2f}")
            if exec_bad:
                parts.append("exec_bad=1")
            if regime_bad:
                parts.append(
                    f"regime_transition={str(regime.get('from_regime') or '')}->{str(regime.get('to_regime') or '')}"
                )

            reason = "|".join(parts)

            set_state("capital_mode", "preserve")
            set_state("capital_mode_ts_ms", str(int(now)))
            set_state("capital_mode_reason", reason)
            set_state("capital_mode_exit_streak", "0")

            snap = {
                "ok": True,
                "ts_ms": int(now),
                "capital_mode": "preserve",
                "reason": reason,
                "mode_changed": True,
                "dd_vel": float(dd_vel),
                "stress_score": float(stress_score),
                "exec": ex,
                "regime": regime,
                "trigger_flags": {
                    "dd_velocity": bool(float(dd_vel) >= float(CAPITAL_PRESERVE_DD_VELOCITY)),
                    "stress": bool(float(stress_score) >= float(CAPITAL_PRESERVE_STRESS_SCORE)),
                    "execution": bool(exec_bad),
                    "regime_transition": bool(regime_bad),
                },
            }
            _persist_cpm_snapshot(con, snap)
            return snap

        # -----------------------------
        # EXIT (hysteresis + streak)
        # -----------------------------
        if mode == "preserve":
            if ts0 > 0 and (now - ts0) < int(CAPITAL_PRESERVE_MIN_DURATION_S) * 1000:
                snap = {
                    "ok": True,
                    "ts_ms": int(now),
                    "capital_mode": "preserve",
                    "reason": reason0,
                    "mode_changed": False,
                    "min_duration_hold": 1,
                    "dd_vel": float(dd_vel),
                    "stress_score": float(stress_score),
                    "exec": ex,
                    "regime": regime,
                }
                _persist_cpm_snapshot(con, snap)
                return snap

            good = (
                float(dd_vel) < float(CAPITAL_PRESERVE_DD_VELOCITY) * 0.50
                and float(stress_score) < float(CAPITAL_PRESERVE_STRESS_SCORE) * 0.85
                and (not bool(exec_bad))
                and (not bool(regime_bad))
            )

            streak = int(get_state("capital_mode_exit_streak", "0") or "0")
            if good:
                streak += 1
            else:
                streak = 0
            set_state("capital_mode_exit_streak", str(int(streak)))

            if streak >= int(CAPITAL_PRESERVE_EXIT_STREAK):
                set_state("capital_mode", "normal")
                set_state("capital_mode_reason", "")
                set_state("capital_mode_ts_ms", str(int(now)))
                set_state("capital_mode_exit_streak", "0")

                snap = {
                    "ok": True,
                    "ts_ms": int(now),
                    "capital_mode": "normal",
                    "reason": "",
                    "mode_changed": True,
                    "exited": 1,
                    "dd_vel": float(dd_vel),
                    "stress_score": float(stress_score),
                    "exec": ex,
                    "regime": regime,
                }
                _persist_cpm_snapshot(con, snap)
                return snap

            snap = {
                "ok": True,
                "ts_ms": int(now),
                "capital_mode": "preserve",
                "reason": reason0,
                "mode_changed": False,
                "exit_streak": int(streak),
                "dd_vel": float(dd_vel),
                "stress_score": float(stress_score),
                "exec": ex,
                "regime": regime,
            }
            _persist_cpm_snapshot(con, snap)
            return snap

        snap = {
            "ok": True,
            "ts_ms": int(now),
            "capital_mode": mode or "normal",
            "reason": reason0,
            "mode_changed": False,
            "dd_vel": float(dd_vel),
            "stress_score": float(stress_score),
            "exec": ex,
            "regime": regime,
        }
        _persist_cpm_snapshot(con, snap)
        return snap

    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("CAPITAL_GUARD_MAIN_CLOSE_FAILED", e, once_key="main_close")
