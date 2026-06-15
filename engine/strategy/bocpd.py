"""Bayesian online changepoint detection for slow-moving risk series.

The core implements Adams & MacKay style BOCPD with a constant hazard and a
Student-t posterior predictive from Normal-Inverse-Gamma updates.  The public
``bocpd_series`` helper recomputes a bounded posterior over a sequence and
returns compact summaries suitable for regime features and ensemble triggers.
"""

from __future__ import annotations

import json
import math
import os
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np


EPS = 1.0e-300
PRUNE_PROB = float(os.environ.get("BOCPD_PRUNE_PROB", "1e-6"))
EXPECTED_RUN = float(os.environ.get("BOCPD_EXPECTED_RUN", "60"))
DEFAULT_CP_WINDOW = int(os.environ.get("BOCPD_CP_WINDOW", "5"))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _logsumexp(values: Sequence[float]) -> float:
    vals = np.asarray(list(values or []), dtype=np.float64)
    vals = vals[np.isfinite(vals)]
    if vals.size == 0:
        return float("-inf")
    m = float(np.max(vals))
    if not math.isfinite(m):
        return m
    return float(m + math.log(float(np.sum(np.exp(vals - m)))))


def _student_t_logpdf(x: float, *, mu: float, kappa: float, alpha: float, beta: float) -> float:
    nu = max(2.0 * float(alpha), 1.0e-6)
    scale2 = float(beta) * (float(kappa) + 1.0) / max(float(alpha) * float(kappa), 1.0e-12)
    scale = math.sqrt(max(scale2, 1.0e-12))
    z = (float(x) - float(mu)) / float(scale)
    return (
        math.lgamma((nu + 1.0) / 2.0)
        - math.lgamma(nu / 2.0)
        - 0.5 * math.log(nu * math.pi)
        - math.log(scale)
        - ((nu + 1.0) / 2.0) * math.log1p((z * z) / nu)
    )


@dataclass(frozen=True)
class NIGState:
    run_length: int
    log_prob: float
    mu: float
    kappa: float
    alpha: float
    beta: float


def _posterior_update(state: NIGState, x: float) -> NIGState:
    kappa_n = float(state.kappa) + 1.0
    mu_n = ((float(state.kappa) * float(state.mu)) + float(x)) / kappa_n
    alpha_n = float(state.alpha) + 0.5
    beta_n = float(state.beta) + (
        (float(state.kappa) * (float(x) - float(state.mu)) ** 2) / (2.0 * kappa_n)
    )
    return NIGState(
        run_length=int(state.run_length) + 1,
        log_prob=float(state.log_prob),
        mu=float(mu_n),
        kappa=float(kappa_n),
        alpha=float(alpha_n),
        beta=float(max(beta_n, 1.0e-12)),
    )


def bocpd_series(
    values: Iterable[Any],
    *,
    expected_run: float | None = None,
    cp_window: int | None = None,
    prune_prob: float | None = None,
    prior_mu: float | None = None,
    prior_kappa: float = 1.0e-2,
    prior_alpha: float = 1.0,
    prior_beta: float | None = None,
) -> list[dict[str, Any]]:
    """Return one posterior summary per observation in ``values``."""

    xs = [_safe_float(v, math.nan) for v in list(values if values is not None else [])]
    xs = [float(v) for v in xs if math.isfinite(v)]
    if not xs:
        return []
    mu0 = float(np.nanmedian(xs[: max(1, min(20, len(xs)))])) if prior_mu is None else float(prior_mu)
    beta0 = float(np.nanvar(xs[: max(2, min(20, len(xs)))]) or 1.0) if prior_beta is None else float(prior_beta)
    beta0 = max(float(beta0), 1.0e-6)
    h = 1.0 / max(1.0, _safe_float(expected_run, EXPECTED_RUN))
    h = min(max(h, 1.0e-6), 0.999999)
    log_h = math.log(h)
    log_1mh = math.log1p(-h)
    window = max(1, _safe_int(cp_window, DEFAULT_CP_WINDOW))
    prune = max(0.0, _safe_float(prune_prob, PRUNE_PROB))
    states = [
        NIGState(
            run_length=0,
            log_prob=0.0,
            mu=float(mu0),
            kappa=float(max(prior_kappa, 1.0e-9)),
            alpha=float(max(prior_alpha, 1.0e-9)),
            beta=float(beta0),
        )
    ]
    out: list[dict[str, Any]] = []
    for idx, x in enumerate(xs):
        growth: list[NIGState] = []
        cp_terms: list[float] = []
        for state in states:
            pred = _student_t_logpdf(x, mu=state.mu, kappa=state.kappa, alpha=state.alpha, beta=state.beta)
            cp_terms.append(float(state.log_prob) + pred + log_h)
            updated = _posterior_update(
                NIGState(
                    run_length=state.run_length,
                    log_prob=float(state.log_prob) + pred + log_1mh,
                    mu=state.mu,
                    kappa=state.kappa,
                    alpha=state.alpha,
                    beta=state.beta,
                ),
                x,
            )
            growth.append(updated)
        cp_log_prob = _logsumexp(cp_terms)
        prior_state = NIGState(0, cp_log_prob, float(mu0), float(max(prior_kappa, 1.0e-9)), float(max(prior_alpha, 1.0e-9)), float(beta0))
        raw_states = [prior_state] + growth
        norm = _logsumexp([state.log_prob for state in raw_states])
        normalized = [
            NIGState(
                state.run_length,
                float(state.log_prob - norm),
                state.mu,
                state.kappa,
                state.alpha,
                state.beta,
            )
            for state in raw_states
        ]
        kept = [state for state in normalized if math.exp(float(state.log_prob)) >= prune]
        if not kept:
            kept = [max(normalized, key=lambda state: state.log_prob)]
        renorm = _logsumexp([state.log_prob for state in kept])
        states = [
            NIGState(state.run_length, float(state.log_prob - renorm), state.mu, state.kappa, state.alpha, state.beta)
            for state in kept
        ]
        probs = {int(state.run_length): float(math.exp(state.log_prob)) for state in states}
        cp_prob = float(sum(prob for run_length, prob in probs.items() if int(run_length) < int(window)))
        map_run = int(max(probs.items(), key=lambda item: item[1])[0]) if probs else 0
        expected_rl = float(sum(float(run_length) * float(prob) for run_length, prob in probs.items()))
        out.append(
            {
                "idx": int(idx),
                "value": float(x),
                "cp_prob_5d": float(min(1.0, max(0.0, cp_prob))),
                "map_run_length": int(map_run),
                "expected_run_length": float(expected_rl),
                "active_states": int(len(states)),
                "posterior": probs,
            }
        )
    return out


def run_length_z(map_run_length: int, *, expected_run: float | None = None) -> float:
    exp_run = max(1.0, _safe_float(expected_run, EXPECTED_RUN))
    return float((float(map_run_length) - exp_run) / math.sqrt(exp_run))


def latest_summary(
    values: Iterable[Any],
    *,
    series_key: str,
    ts_ms: int | None = None,
    expected_run: float | None = None,
    cp_window: int | None = None,
) -> dict[str, Any]:
    summaries = bocpd_series(values, expected_run=expected_run, cp_window=cp_window)
    if not summaries:
        return {
            "series_key": str(series_key),
            "ts_ms": int(ts_ms or time.time() * 1000),
            "cp_prob_5d": 0.0,
            "map_run_length": 0,
            "run_length_z": 0.0,
            "n_obs": 0,
            "posterior": {},
        }
    last = dict(summaries[-1])
    map_run = _safe_int(last.get("map_run_length"), 0)
    return {
        "series_key": str(series_key),
        "ts_ms": int(ts_ms or time.time() * 1000),
        "cp_prob_5d": float(last.get("cp_prob_5d") or 0.0),
        "map_run_length": int(map_run),
        "expected_run_length": float(last.get("expected_run_length") or 0.0),
        "run_length_z": float(run_length_z(map_run, expected_run=expected_run)),
        "active_states": int(last.get("active_states") or 0),
        "n_obs": int(len(summaries)),
        "posterior": dict(last.get("posterior") or {}),
    }


def feature_map_from_summary(summary: Mapping[str, Any] | None) -> dict[str, float]:
    payload = dict(summary or {})
    return {
        "bocpd_cp_prob_5d": float(max(0.0, min(1.0, _safe_float(payload.get("cp_prob_5d"), 0.0)))),
        "bocpd_run_length_z": float(_safe_float(payload.get("run_length_z"), 0.0)),
    }


def persist_summary(con, summary: Mapping[str, Any], *, series_type: str, symbol: str = "*") -> None:
    payload = dict(summary or {})
    con.execute(
        """
        INSERT INTO bocpd_regime_state(
          series_key, series_type, symbol, ts_ms, cp_prob_5d, map_run_length,
          expected_run_length, run_length_z, active_states, n_obs, posterior_json, created_ts_ms
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(series_key, ts_ms) DO UPDATE SET
          series_type=excluded.series_type,
          symbol=excluded.symbol,
          cp_prob_5d=excluded.cp_prob_5d,
          map_run_length=excluded.map_run_length,
          expected_run_length=excluded.expected_run_length,
          run_length_z=excluded.run_length_z,
          active_states=excluded.active_states,
          n_obs=excluded.n_obs,
          posterior_json=excluded.posterior_json,
          created_ts_ms=excluded.created_ts_ms
        """,
        (
            str(payload.get("series_key") or ""),
            str(series_type or ""),
            str(symbol or "*").upper().strip() or "*",
            int(payload.get("ts_ms") or time.time() * 1000),
            float(payload.get("cp_prob_5d") or 0.0),
            int(payload.get("map_run_length") or 0),
            float(payload.get("expected_run_length") or 0.0),
            float(payload.get("run_length_z") or 0.0),
            int(payload.get("active_states") or 0),
            int(payload.get("n_obs") or 0),
            json.dumps(dict(payload.get("posterior") or {}), separators=(",", ":"), sort_keys=True),
            int(time.time() * 1000),
        ),
    )


def ensemble_trigger_mode() -> str:
    raw = str(os.environ.get("BOCPD_ENSEMBLE_TRIGGER_MODE", "log_only") or "log_only").strip().lower()
    return raw if raw in {"off", "log_only", "adapt"} else "log_only"


def ensemble_trigger_threshold() -> float:
    return max(0.0, min(1.0, _safe_float(os.environ.get("BOCPD_ENSEMBLE_TRIGGER"), 0.5)))


def latest_ensemble_trigger(con, *, symbol: str = "*", horizon: int = 0) -> dict[str, Any]:
    mode = ensemble_trigger_mode()
    threshold = ensemble_trigger_threshold()
    if mode == "off":
        return {"enabled": False, "mode": mode, "triggered": False, "threshold": float(threshold)}
    try:
        summary = load_latest_summary(con, symbol=str(symbol or "*"), series_type="portfolio_correlation")
        if not summary:
            summary = load_latest_summary(con, symbol="*", series_type="portfolio_correlation")
    except Exception as exc:
        return {"enabled": True, "mode": mode, "triggered": False, "threshold": float(threshold), "reason": f"load_failed:{type(exc).__name__}"}
    cp_prob = float(max(0.0, min(1.0, _safe_float(summary.get("cp_prob_5d"), 0.0))))
    return {
        "enabled": True,
        "mode": mode,
        "triggered": bool(cp_prob >= threshold),
        "threshold": float(threshold),
        "cp_prob_5d": float(cp_prob),
        "series_key": str(summary.get("series_key") or ""),
        "ts_ms": int(summary.get("ts_ms") or 0),
        "symbol": str(symbol or "*").upper().strip() or "*",
        "horizon_s": int(horizon or 0),
    }


def effective_hedge_window(con, *, symbol: str, horizon: int, base_window: int) -> tuple[int, dict[str, Any]]:
    base = max(1, int(base_window or 1))
    trigger = latest_ensemble_trigger(con, symbol=str(symbol), horizon=int(horizon))
    effective = base
    if bool(trigger.get("triggered")):
        trigger["recommended_effective_window"] = int(max(1, math.ceil(base / 2.0)))
    if bool(trigger.get("triggered")) and str(trigger.get("mode") or "") == "adapt":
        effective = int(trigger.get("recommended_effective_window") or max(1, int(math.ceil(base / 2.0))))
    trigger["base_window"] = int(base)
    trigger["effective_window"] = int(effective)
    trigger["adapted"] = bool(bool(trigger.get("triggered")) and trigger.get("mode") == "adapt")
    return int(effective), trigger


def log_ensemble_trigger(con, trigger: Mapping[str, Any]) -> None:
    payload = dict(trigger or {})
    if not bool(payload.get("enabled")) or not bool(payload.get("triggered")):
        return
    con.execute(
        """
        INSERT INTO bocpd_ensemble_triggers(
          ts_ms, symbol, horizon_s, cp_prob_5d, threshold, mode, base_window,
          effective_window, series_key, meta_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(time.time() * 1000),
            str(payload.get("symbol") or "*").upper().strip() or "*",
            int(payload.get("horizon_s") or 0),
            float(payload.get("cp_prob_5d") or 0.0),
            float(payload.get("threshold") or 0.0),
            str(payload.get("mode") or "log_only"),
            int(payload.get("base_window") or 0),
            int(payload.get("effective_window") or 0),
            str(payload.get("series_key") or ""),
            json.dumps(dict(payload), separators=(",", ":"), sort_keys=True),
        ),
    )


def load_latest_summary(
    con,
    *,
    series_key: str | None = None,
    symbol: str = "*",
    series_type: str = "",
    as_of_ts_ms: int | None = None,
) -> dict[str, Any]:
    as_of = _safe_int(as_of_ts_ms, 0)
    if series_key:
        if as_of > 0:
            row = con.execute(
                """
                SELECT series_key, series_type, symbol, ts_ms, cp_prob_5d, map_run_length,
                       expected_run_length, run_length_z, active_states, n_obs, posterior_json
                FROM bocpd_regime_state
                WHERE series_key=?
                  AND ts_ms <= ?
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                (str(series_key), int(as_of)),
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT series_key, series_type, symbol, ts_ms, cp_prob_5d, map_run_length,
                       expected_run_length, run_length_z, active_states, n_obs, posterior_json
                FROM bocpd_regime_state
                WHERE series_key=?
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                (str(series_key),),
            ).fetchone()
    else:
        symbol_key = str(symbol or "*").upper().strip() or "*"
        type_key = str(series_type or "")
        if as_of > 0:
            row = con.execute(
                """
                SELECT series_key, series_type, symbol, ts_ms, cp_prob_5d, map_run_length,
                       expected_run_length, run_length_z, active_states, n_obs, posterior_json
                FROM bocpd_regime_state
                WHERE symbol IN (?, '*')
                  AND (?='' OR series_type=?)
                  AND ts_ms <= ?
                ORDER BY CASE WHEN symbol=? THEN 0 ELSE 1 END, ts_ms DESC
                LIMIT 1
                """,
                (symbol_key, type_key, type_key, int(as_of), symbol_key),
            ).fetchone()
        else:
            row = con.execute(
                """
                SELECT series_key, series_type, symbol, ts_ms, cp_prob_5d, map_run_length,
                       expected_run_length, run_length_z, active_states, n_obs, posterior_json
                FROM bocpd_regime_state
                WHERE symbol IN (?, '*')
                  AND (?='' OR series_type=?)
                ORDER BY CASE WHEN symbol=? THEN 0 ELSE 1 END, ts_ms DESC
                LIMIT 1
                """,
                (symbol_key, type_key, type_key, symbol_key),
            ).fetchone()
    if not row:
        return {}
    try:
        posterior = json.loads(row[10] or "{}")
    except Exception:
        posterior = {}
    return {
        "series_key": str(row[0] or ""),
        "series_type": str(row[1] or ""),
        "symbol": str(row[2] or ""),
        "ts_ms": int(row[3] or 0),
        "cp_prob_5d": float(row[4] or 0.0),
        "map_run_length": int(row[5] or 0),
        "expected_run_length": float(row[6] or 0.0),
        "run_length_z": float(row[7] or 0.0),
        "active_states": int(row[8] or 0),
        "n_obs": int(row[9] or 0),
        "posterior": posterior if isinstance(posterior, Mapping) else {},
    }


def evaluate_detection(
    values: Sequence[Any],
    *,
    breakpoints: Sequence[int],
    detection_threshold: float = 0.5,
    max_delay: int = 10,
) -> dict[str, Any]:
    summaries = bocpd_series(values)
    cps = [float(row.get("cp_prob_5d") or 0.0) for row in summaries]
    detected = []
    for bp in list(breakpoints or []):
        start = int(bp)
        stop = min(len(cps), start + int(max_delay) + 1)
        hit = next((idx for idx in range(start, stop) if cps[idx] >= float(detection_threshold)), None)
        detected.append({"breakpoint": int(bp), "detected_at": hit, "delay": (None if hit is None else int(hit - start))})
    break_mask = set()
    for bp in list(breakpoints or []):
        for idx in range(max(0, int(bp)), min(len(cps), int(bp) + int(max_delay) + 1)):
            break_mask.add(idx)
    false_positive = [idx for idx, prob in enumerate(cps) if prob >= float(detection_threshold) and idx not in break_mask]
    non_break_n = max(1, len(cps) - len(break_mask))
    return {
        "n": int(len(cps)),
        "detected": detected,
        "all_detected": all(item.get("detected_at") is not None for item in detected),
        "max_delay": max([item.get("delay") or 0 for item in detected], default=0),
        "false_positive_rate": float(len(false_positive) / non_break_n),
        "false_positive_count": int(len(false_positive)),
    }
