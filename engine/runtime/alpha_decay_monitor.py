"""
FILE: alpha_decay_monitor.py

Runtime subsystem module for `alpha_decay_monitor`.
"""

import json
import math
import os
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.allocator_status import _safe_float
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.lifecycle_state import DEGRADED, LIVE, get_state as get_lifecycle_state, set_state as set_lifecycle_state
from engine.runtime.logging import get_logger
from engine.runtime.risk_state import set_state as set_risk_state

ALPHA_DECAY_SHARPE_BAD = float(os.environ.get("ALPHA_DECAY_SHARPE_BAD", "0.15"))
ALPHA_DECAY_SHARPE_SEVERE = float(os.environ.get("ALPHA_DECAY_SHARPE_SEVERE", "-0.10"))
ALPHA_DECAY_HALF_LIFE_SHORT_BUCKETS = float(os.environ.get("ALPHA_DECAY_HALF_LIFE_SHORT_BUCKETS", "3.0"))
ALPHA_DECAY_HALF_LIFE_SEVERE_BUCKETS = float(os.environ.get("ALPHA_DECAY_HALF_LIFE_SEVERE_BUCKETS", "1.5"))
ALPHA_DECAY_BREAK_WARN = float(os.environ.get("ALPHA_DECAY_BREAK_WARN", "1.25"))
ALPHA_DECAY_BREAK_SEVERE = float(os.environ.get("ALPHA_DECAY_BREAK_SEVERE", "2.0"))
ALPHA_DECAY_THROTTLE_FLOOR = float(os.environ.get("ALPHA_DECAY_THROTTLE_FLOOR", "0.20"))
ALPHA_DECAY_THROTTLE_WARN = float(os.environ.get("ALPHA_DECAY_THROTTLE_WARN", "0.70"))
ALPHA_DECAY_MIN_OBS = int(os.environ.get("ALPHA_DECAY_MIN_OBS", "8"))
ALPHA_DECAY_LIFECYCLE_SEVERE_COUNT = int(os.environ.get("ALPHA_DECAY_LIFECYCLE_SEVERE_COUNT", "1"))
ALPHA_DECAY_STRATEGY_RETENTION_DAYS = int(os.environ.get("ALPHA_DECAY_STRATEGY_RETENTION_DAYS", "45"))
ALPHA_DECAY_RUNTIME_RETENTION_DAYS = int(os.environ.get("ALPHA_DECAY_RUNTIME_RETENTION_DAYS", "45"))
LOG = get_logger("engine.runtime.alpha_decay_monitor")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.runtime.alpha_decay_monitor",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

def _stddev(vals: List[float]) -> float:
    if not vals or len(vals) <= 1:
        return 0.0
    m = sum(vals) / float(len(vals))
    var = sum((x - m) ** 2 for x in vals) / float(len(vals) - 1)
    return math.sqrt(max(0.0, var))


def _lag1_autocorr(vals: List[float]) -> Optional[float]:
    if not vals or len(vals) < 3:
        return None
    m = sum(vals) / float(len(vals))
    x0 = [float(v) - float(m) for v in vals[:-1]]
    x1 = [float(v) - float(m) for v in vals[1:]]
    den0 = sum(v * v for v in x0)
    den1 = sum(v * v for v in x1)
    if den0 <= 1e-12 or den1 <= 1e-12:
        return None
    num = sum(a * b for a, b in zip(x0, x1))
    rho = float(num / math.sqrt(den0 * den1))
    return max(-0.999999, min(0.999999, rho))


def _half_life_buckets(vals: List[float]) -> Optional[float]:
    rho = _lag1_autocorr(vals)
    if rho is None:
        return None
    if rho <= 0.0:
        return 1.0
    if rho >= 0.999999:
        return None
    try:
        hl = -math.log(2.0) / math.log(rho)
        if not math.isfinite(hl) or hl <= 0.0:
            return None
        return float(hl)
    except Exception as e:
        _warn_nonfatal(
            "ALPHA_DECAY_HALF_LIFE_PARSE_FAILED",
            e,
            once_key="half_life_parse",
            value=repr(rho)[:120],
        )
        return None


def _structural_break_z(vals: List[float]) -> Tuple[float, Dict[str, Any]]:
    n = len(vals)
    if n < 6:
        return 0.0, {"n_pre": 0, "n_post": 0, "mean_pre": 0.0, "mean_post": 0.0}

    split = max(3, min(n - 3, n // 2))
    pre = [float(v) for v in vals[:split]]
    post = [float(v) for v in vals[split:]]
    mu_pre = sum(pre) / float(len(pre)) if pre else 0.0
    mu_post = sum(post) / float(len(post)) if post else 0.0
    sd_pre = _stddev(pre)
    sd_post = _stddev(post)
    pooled = math.sqrt(max(1e-12, ((sd_pre ** 2) + (sd_post ** 2)) / 2.0))
    denom = max(1e-9, pooled / math.sqrt(max(1.0, min(len(pre), len(post)))))
    z = (float(mu_post) - float(mu_pre)) / float(denom)
    return float(z), {
        "n_pre": int(len(pre)),
        "n_post": int(len(post)),
        "mean_pre": float(mu_pre),
        "mean_post": float(mu_post),
        "std_pre": float(sd_pre),
        "std_post": float(sd_post),
    }


def _severity_from_metrics(rolling_sharpe: float, half_life_buckets: Optional[float], break_z: float, n_obs: int) -> Tuple[str, float, List[str]]:
    reasons: List[str] = []
    score = 0.0

    # Decay classification stays inert until there is enough evidence; the
    # runtime should not throttle strategies on a handful of noisy buckets.
    if int(n_obs) < int(max(3, ALPHA_DECAY_MIN_OBS)):
        return "ok", 0.0, ["insufficient_observations"]

    if float(rolling_sharpe) <= float(ALPHA_DECAY_SHARPE_BAD):
        reasons.append("rolling_sharpe_weak")
        score += 0.35
    if float(rolling_sharpe) <= float(ALPHA_DECAY_SHARPE_SEVERE):
        reasons.append("rolling_sharpe_severe")
        score += 0.35

    if half_life_buckets is not None and float(half_life_buckets) <= float(ALPHA_DECAY_HALF_LIFE_SHORT_BUCKETS):
        reasons.append("half_life_short")
        score += 0.25
    if half_life_buckets is not None and float(half_life_buckets) <= float(ALPHA_DECAY_HALF_LIFE_SEVERE_BUCKETS):
        reasons.append("half_life_severe")
        score += 0.20

    neg_break = max(0.0, -float(break_z))
    if neg_break >= float(ALPHA_DECAY_BREAK_WARN):
        reasons.append("structural_break_warn")
        score += 0.25
    if neg_break >= float(ALPHA_DECAY_BREAK_SEVERE):
        reasons.append("structural_break_severe")
        score += 0.25

    score = max(0.0, min(1.0, float(score)))
    if score >= 0.75:
        return "severe", float(score), reasons
    if score >= 0.35:
        return "warn", float(score), reasons
    return "ok", float(score), reasons


def compute_alpha_decay_snapshot(
    *,
    strategy_name: str,
    bucket_returns: List[float],
    bucket_s: int,
    ts_ms: int,
) -> Dict[str, Any]:
    vals = [float(v) for v in (bucket_returns or [])]
    n_obs = int(len(vals))
    mu = sum(vals) / float(n_obs) if vals else 0.0
    sd = _stddev(vals)
    rolling_sharpe = float(mu / sd) if sd > 1e-12 else 0.0
    half_life = _half_life_buckets(vals)
    break_z, break_info = _structural_break_z(vals)
    severity, severity_score, reasons = _severity_from_metrics(
        rolling_sharpe=float(rolling_sharpe),
        half_life_buckets=half_life,
        break_z=float(break_z),
        n_obs=int(n_obs),
    )

    # The output throttle is advisory for downstream allocators and runtime
    # governance, not a direct order-placement switch.
    throttle = 1.0
    if severity == "warn":
        throttle = max(float(ALPHA_DECAY_THROTTLE_WARN), 1.0 - float(severity_score))
    elif severity == "severe":
        throttle = max(float(ALPHA_DECAY_THROTTLE_FLOOR), 1.0 - float(severity_score))

    return {
        "strategy_name": str(strategy_name),
        "ts_ms": int(ts_ms),
        "bucket_s": int(bucket_s),
        "n_obs": int(n_obs),
        "rolling_mean_bucket_pnl": float(mu),
        "rolling_std_bucket_pnl": float(sd),
        "rolling_sharpe": float(rolling_sharpe),
        "half_life_buckets": (float(half_life) if half_life is not None else None),
        "half_life_seconds": (float(half_life) * float(bucket_s) if half_life is not None else None),
        "structural_break_z": float(break_z),
        "structural_break": dict(break_info),
        "severity": str(severity),
        "severity_score": float(severity_score),
        "reasons": list(reasons),
        "throttle_mult": float(max(0.0, min(1.0, throttle))),
    }


def persist_alpha_decay_state(
    con,
    *,
    details: Dict[str, Dict[str, Any]],
    runtime_summary: Dict[str, Any],
    ts_ms: int,
    window_days: int = 0,
    bucket_s: int = 0,
) -> Dict[str, Any]:
    inserted_strategy_rows = 0
    inserted_runtime_rows = 0
    deleted_strategy_rows = 0
    deleted_runtime_rows = 0

    if con is None:
        return {
            "ok": False,
            "inserted_strategy_rows": 0,
            "inserted_runtime_rows": 0,
            "deleted_strategy_rows": 0,
            "deleted_runtime_rows": 0,
            "error": "missing_connection",
        }

    try:
        # Persist per-strategy snapshots first so runtime-wide summaries always
        # have drill-down rows backing them up.
        for strategy_name, rec in (details or {}).items():
            try:
                row = dict(rec or {})
                con.execute(
                    """
                    INSERT INTO alpha_decay_strategy_metrics(
                      strategy_name,
                      ts_ms,
                      window_days,
                      bucket_s,
                      rolling_sharpe,
                      half_life_buckets,
                      half_life_seconds,
                      structural_break_z,
                      severity,
                      severity_score,
                      throttle_mult,
                      n_obs,
                      detail_json
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(strategy_name, ts_ms, window_days) DO UPDATE SET
                      bucket_s=excluded.bucket_s,
                      rolling_sharpe=excluded.rolling_sharpe,
                      half_life_buckets=excluded.half_life_buckets,
                      half_life_seconds=excluded.half_life_seconds,
                      structural_break_z=excluded.structural_break_z,
                      severity=excluded.severity,
                      severity_score=excluded.severity_score,
                      throttle_mult=excluded.throttle_mult,
                      n_obs=excluded.n_obs,
                      detail_json=excluded.detail_json
                    """,
                    (
                        str(strategy_name),
                        int(ts_ms),
                        int(window_days),
                        int(bucket_s),
                        float(_safe_float(row.get("alpha_decay_rolling_sharpe"), 0.0)),
                        (
                            None
                            if row.get("alpha_decay_half_life_buckets") is None
                            else float(_safe_float(row.get("alpha_decay_half_life_buckets"), 0.0))
                        ),
                        (
                            None
                            if row.get("alpha_decay_half_life_seconds") is None
                            else float(_safe_float(row.get("alpha_decay_half_life_seconds"), 0.0))
                        ),
                        float(_safe_float(row.get("alpha_decay_structural_break_z"), 0.0)),
                        str(row.get("alpha_decay_severity") or "ok"),
                        float(_safe_float(row.get("alpha_decay_severity_score"), 0.0)),
                        float(max(0.0, min(1.0, _safe_float(row.get("alpha_decay_throttle_mult"), 1.0)))),
                        int(row.get("alpha_decay_n_obs") or 0),
                        json.dumps(row, separators=(",", ":"), sort_keys=True),
                    ),
                )
                inserted_strategy_rows += 1
            except Exception as e:
                _warn_nonfatal(
                    "ALPHA_DECAY_STRATEGY_HISTORY_ROW_FAILED",
                    e,
                    once_key=f"strategy_history_row:{row.get('strategy_name')}",
                    strategy_name=str(row.get("strategy_name") or ""),
                )
                continue
    except Exception as e:
        _warn_nonfatal(
            "ALPHA_DECAY_STRATEGY_HISTORY_WRITE_FAILED",
            e,
            once_key="strategy_history_write",
            ts_ms=int(ts_ms),
        )

    try:
        summary = dict(runtime_summary or {})
        con.execute(
            """
            INSERT INTO alpha_decay_runtime_history(
              ts_ms,
              status,
              min_throttle_mult,
              severe_count,
              warn_count,
              detail_json
            )
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(ts_ms) DO UPDATE SET
              status=excluded.status,
              min_throttle_mult=excluded.min_throttle_mult,
              severe_count=excluded.severe_count,
              warn_count=excluded.warn_count,
              detail_json=excluded.detail_json
            """,
            (
                int(ts_ms),
                str(summary.get("status") or "ok"),
                float(max(0.0, min(1.0, _safe_float(summary.get("min_throttle_mult"), 1.0)))),
                int(len(summary.get("severe_strategies") or [])),
                int(len(summary.get("warn_strategies") or [])),
                json.dumps(summary, separators=(",", ":"), sort_keys=True),
            ),
        )
        inserted_runtime_rows += 1
    except Exception as e:
        _warn_nonfatal(
            "ALPHA_DECAY_RUNTIME_HISTORY_WRITE_FAILED",
            e,
            once_key="runtime_history_write",
            ts_ms=int(ts_ms),
        )

    try:
        cutoff_ts_ms = int(ts_ms) - (int(max(1, ALPHA_DECAY_STRATEGY_RETENTION_DAYS)) * 86400 * 1000)
        cur = con.execute(
            """
            DELETE FROM alpha_decay_strategy_metrics
            WHERE ts_ms < ?
            """,
            (int(cutoff_ts_ms),),
        )
        deleted_strategy_rows = int(getattr(cur, "rowcount", 0) or 0)
    except Exception:
        deleted_strategy_rows = 0

    try:
        cutoff_ts_ms = int(ts_ms) - (int(max(1, ALPHA_DECAY_RUNTIME_RETENTION_DAYS)) * 86400 * 1000)
        cur = con.execute(
            """
            DELETE FROM alpha_decay_runtime_history
            WHERE ts_ms < ?
            """,
            (int(cutoff_ts_ms),),
        )
        deleted_runtime_rows = int(getattr(cur, "rowcount", 0) or 0)
    except Exception:
        deleted_runtime_rows = 0

    return {
        "ok": True,
        "inserted_strategy_rows": int(inserted_strategy_rows),
        "inserted_runtime_rows": int(inserted_runtime_rows),
        "deleted_strategy_rows": int(deleted_strategy_rows),
        "deleted_runtime_rows": int(deleted_runtime_rows),
    }


def apply_alpha_decay_runtime_state(details: Dict[str, Dict[str, Any]], ts_ms: int) -> Dict[str, Any]:
    severe = []
    warn = []
    ok = []
    min_mult = 1.0
    sharpe_vals: List[float] = []
    severity_vals: List[float] = []

    for name, rec in (details or {}).items():
        try:
            sev = str((rec or {}).get("alpha_decay_severity") or "ok").strip().lower()
            mult = float(_safe_float((rec or {}).get("alpha_decay_throttle_mult"), 1.0))
            min_mult = min(min_mult, mult)
            sharpe_vals.append(float(_safe_float((rec or {}).get("alpha_decay_rolling_sharpe"), 0.0)))
            severity_vals.append(float(_safe_float((rec or {}).get("alpha_decay_severity_score"), 0.0)))

            if sev == "severe":
                severe.append(str(name))
            elif sev == "warn":
                warn.append(str(name))
            else:
                ok.append(str(name))
        except Exception as e:
            _warn_nonfatal(
                "ALPHA_DECAY_SUMMARY_ROW_FAILED",
                e,
                once_key=f"summary_row:{name}",
                strategy_name=str(name),
            )
            continue

    strategy_count = int(len(severe) + len(warn) + len(ok))
    avg_sharpe = (sum(sharpe_vals) / float(len(sharpe_vals))) if sharpe_vals else 0.0
    avg_severity_score = (sum(severity_vals) / float(len(severity_vals))) if severity_vals else 0.0
    severe_share = (float(len(severe)) / float(strategy_count)) if strategy_count > 0 else 0.0
    warn_share = (float(len(warn)) / float(strategy_count)) if strategy_count > 0 else 0.0
    health_score = max(0.0, min(1.0, (1.0 - float(avg_severity_score)) * float(min_mult)))

    portfolio = {
        "strategy_count": int(strategy_count),
        "severe_count": int(len(severe)),
        "warn_count": int(len(warn)),
        "ok_count": int(len(ok)),
        "severe_share": float(severe_share),
        "warn_share": float(warn_share),
        "avg_rolling_sharpe": float(avg_sharpe),
        "avg_severity_score": float(avg_severity_score),
        "health_score": float(health_score),
    }

    summary = {
        "ts_ms": int(ts_ms),
        "severe_strategies": severe,
        "warn_strategies": warn,
        "ok_strategies": ok,
        "min_throttle_mult": float(min_mult),
        "status": ("severe" if severe else ("warn" if warn else "ok")),
        "portfolio": dict(portfolio),
    }

    try:
        set_risk_state("alpha_decay_summary", json.dumps(summary, separators=(",", ":"), sort_keys=True))
        set_risk_state("alpha_decay_status", str(summary.get("status") or "ok"))
        set_risk_state("alpha_decay_min_throttle_mult", str(_safe_float(summary.get("min_throttle_mult"), 1.0)))
        set_risk_state("alpha_decay_portfolio_health", json.dumps(portfolio, separators=(",", ":"), sort_keys=True))
        set_risk_state("alpha_decay_portfolio_status", str(summary.get("status") or "ok"))
    except Exception as e:
        _warn_nonfatal(
            "ALPHA_DECAY_RISK_STATE_WRITE_FAILED",
            e,
            once_key="risk_state_write",
            status=str(summary.get("status") or "ok"),
        )

    try:
        current = get_lifecycle_state() or {}
        cur_state = str(current.get("state") or "").strip().upper()
        cur_detail = str(current.get("detail") or "")
        if len(severe) >= int(max(1, ALPHA_DECAY_LIFECYCLE_SEVERE_COUNT)):
            if cur_state in ("LIVE", "DEGRADED"):
                set_lifecycle_state(DEGRADED, f"alpha_decay_monitor:{','.join(sorted(severe))}")
        elif cur_state == "DEGRADED" and cur_detail.startswith("alpha_decay_monitor:"):
            set_lifecycle_state(LIVE, "alpha_decay_monitor_recovered")
    except Exception as e:
        _warn_nonfatal(
            "ALPHA_DECAY_LIFECYCLE_UPDATE_FAILED",
            e,
            once_key="lifecycle_update",
            severe_count=int(len(severe)),
            warn_count=int(len(warn)),
        )

    return summary
