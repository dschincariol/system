"""Reactive limit-order-book simulation and shadow DeepLOB readiness helpers."""

from __future__ import annotations

import json
import math
import os
import time
from typing import Any, Dict, Iterable, List, Optional


_TRUTHY = {"1", "true", "yes", "on"}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return bool(default)
    return raw in _TRUTHY


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.environ.get(name, default)).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        out = float(str(os.environ.get(name, default)).strip())
        return out if math.isfinite(out) else float(default)
    except Exception:
        return float(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else float(default)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _clamp(value: float, lo: float, hi: float) -> float:
    return float(max(float(lo), min(float(hi), float(value))))


def _table_exists(con: Any, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name),),
        ).fetchone()
        if row:
            return True
    except Exception:
        # no-op-guard: allow SQLite catalog probe to fall through to Postgres catalog lookup.
        pass
    try:
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
    except Exception:
        return False


def _json_obj(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    try:
        parsed = json.loads(str(raw or "{}"))
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _latest_l2_row(
    con: Any,
    *,
    symbol: str,
    ts_ms: Optional[int] = None,
    max_age_ms: Optional[int] = None,
) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip()
    ref_ts = int(ts_ms or _now_ms())
    max_age = int(max_age_ms or _env_int("EXEC_LOB_MAX_L2_AGE_MS", 60_000))
    if not sym:
        return {"ok": False, "reason": "missing_symbol"}
    if not _table_exists(con, "market_microstructure_signals"):
        return {"ok": False, "reason": "missing_market_microstructure_signals"}
    try:
        row = con.execute(
            """
            SELECT
              ts_ms, provider, mid_px, bid_px, ask_px, bid_sz, ask_sz,
              spread_bps, spread_widening, order_book_imbalance,
              trade_aggressor_imbalance, composite_score, details_json
            FROM market_microstructure_signals
            WHERE symbol=?
              AND ts_ms <= ?
              AND ts_ms >= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (sym, int(ref_ts), int(ref_ts - max(1, max_age))),
        ).fetchone()
    except Exception as exc:
        return {"ok": False, "reason": f"l2_query_failed:{type(exc).__name__}"}
    if not row:
        return {"ok": False, "reason": "l2_snapshot_missing_or_stale"}

    details = _json_obj(row[12])
    bid_sz = _safe_float(row[5], _safe_float(details.get("bid_size") or details.get("bid_sz"), 0.0))
    ask_sz = _safe_float(row[6], _safe_float(details.get("ask_size") or details.get("ask_sz"), 0.0))
    out = {
        "ok": True,
        "symbol": sym,
        "ts_ms": int(row[0] or 0),
        "provider": str(row[1] or ""),
        "mid_px": _safe_float(row[2], 0.0),
        "bid_px": _safe_float(row[3], 0.0),
        "ask_px": _safe_float(row[4], 0.0),
        "bid_sz": float(max(0.0, bid_sz)),
        "ask_sz": float(max(0.0, ask_sz)),
        "spread_bps": float(max(0.0, _safe_float(row[7], 0.0))),
        "spread_widening": _safe_float(row[8], 0.0),
        "order_book_imbalance": _clamp(_safe_float(row[9], 0.0), -1.0, 1.0),
        "trade_aggressor_imbalance": _clamp(_safe_float(row[10], 0.0), -1.0, 1.0),
        "composite_score": _safe_float(row[11], 0.0),
        "details": details,
    }
    out["age_ms"] = int(max(0, ref_ts - int(out["ts_ms"] or 0)))
    return out


def _fetch_l2_rows(
    con: Any,
    *,
    symbol: Optional[str],
    ts_ms: Optional[int],
    lookback_ms: int,
    limit: int,
) -> List[tuple]:
    if not _table_exists(con, "market_microstructure_signals"):
        return []
    sym = str(symbol or "").upper().strip()
    ref_ts = int(ts_ms or _now_ms())
    try:
        if sym:
            return list(
                con.execute(
                    """
                    SELECT ts_ms, bid_px, ask_px, bid_sz, ask_sz, spread_bps,
                           order_book_imbalance, trade_aggressor_imbalance,
                           spread_widening, composite_score, details_json
                    FROM market_microstructure_signals
                    WHERE symbol=?
                      AND ts_ms <= ?
                      AND ts_ms >= ?
                    ORDER BY ts_ms DESC
                    LIMIT ?
                    """,
                    (sym, int(ref_ts), int(ref_ts - max(1, lookback_ms)), int(limit)),
                ).fetchall()
                or []
            )
        return list(
            con.execute(
                """
                SELECT ts_ms, bid_px, ask_px, bid_sz, ask_sz, spread_bps,
                       order_book_imbalance, trade_aggressor_imbalance,
                       spread_widening, composite_score, details_json
                FROM market_microstructure_signals
                WHERE ts_ms <= ?
                  AND ts_ms >= ?
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(ref_ts), int(ref_ts - max(1, lookback_ms)), int(limit)),
            ).fetchall()
            or []
        )
    except Exception:
        return []


def l2_data_quality_snapshot(
    con: Any,
    *,
    symbol: Optional[str] = None,
    ts_ms: Optional[int] = None,
    min_rows: Optional[int] = None,
    max_age_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Return L2/top-of-book quality needed by LOB simulation and shadow models."""

    required_rows = int(min_rows or _env_int("EXEC_LOB_MIN_L2_ROWS", 80))
    freshness_ms = int(max_age_ms or _env_int("EXEC_LOB_MAX_L2_AGE_MS", 60_000))
    rows = _fetch_l2_rows(
        con,
        symbol=symbol,
        ts_ms=ts_ms,
        lookback_ms=int(_env_int("EXEC_LOB_L2_LOOKBACK_MS", 10 * 60_000)),
        limit=max(required_rows * 4, _env_int("EXEC_LOB_FEATURE_WINDOW_N", 40), 120),
    )
    blockers: List[str] = []
    if not rows:
        blockers.append("l2_data_missing")
        return {
            "ok": False,
            "required_rows": int(required_rows),
            "sample_n": 0,
            "blockers": blockers,
            "reason": blockers[0],
        }

    latest_ts = max(_safe_int(row[0], 0) for row in rows)
    age_ms = int(max(0, int(ts_ms or _now_ms()) - latest_ts))
    if len(rows) < required_rows:
        blockers.append("l2_rows_insufficient")
    if age_ms > freshness_ms:
        blockers.append("l2_stale")

    depth_rows = 0
    depths: List[float] = []
    spreads: List[float] = []
    intervals: List[int] = []
    prior_ts: Optional[int] = None
    for row in sorted(rows, key=lambda r: _safe_int(r[0], 0), reverse=True):
        details = _json_obj(row[10] if len(row) > 10 else None)
        bid_sz = _safe_float(row[3], _safe_float(details.get("bid_size") or details.get("bid_sz"), 0.0))
        ask_sz = _safe_float(row[4], _safe_float(details.get("ask_size") or details.get("ask_sz"), 0.0))
        if bid_sz > 0.0 and ask_sz > 0.0:
            depth_rows += 1
            depths.append((float(bid_sz) + float(ask_sz)) / 2.0)
        spread = _safe_float(row[5], 0.0)
        if spread > 0.0:
            spreads.append(float(spread))
        cur_ts = _safe_int(row[0], 0)
        if prior_ts is not None and cur_ts > 0:
            intervals.append(abs(int(prior_ts) - int(cur_ts)))
        prior_ts = cur_ts

    if depth_rows < max(1, int(required_rows * 0.75)):
        blockers.append("l2_top_depth_missing")

    avg_depth = sum(depths) / float(len(depths)) if depths else 0.0
    avg_spread = sum(spreads) / float(len(spreads)) if spreads else 0.0
    median_interval_ms = sorted(intervals)[len(intervals) // 2] if intervals else 0
    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "required_rows": int(required_rows),
        "sample_n": int(len(rows)),
        "latest_ts_ms": int(latest_ts),
        "age_ms": int(age_ms),
        "max_age_ms": int(freshness_ms),
        "top_depth_rows": int(depth_rows),
        "avg_top_depth_qty": float(avg_depth),
        "avg_spread_bps": float(avg_spread),
        "median_interval_ms": int(median_interval_ms),
        "blockers": blockers,
        "reason": "ok" if not blockers else blockers[0],
    }


def latency_assumption_snapshot(con: Any | None = None, *, ts_ms: Optional[int] = None) -> Dict[str, Any]:
    assumed = _env_int("EXEC_LOB_ASSUMED_LATENCY_MS", _env_int("BROKER_LATENCY_MS", 120))
    max_assumed = _env_int("EXEC_LOB_MAX_ASSUMED_LATENCY_MS", 5_000)
    blockers: List[str] = []
    if assumed <= 0:
        blockers.append("latency_assumption_missing")
    if assumed > max_assumed:
        blockers.append("latency_assumption_too_high")

    observed_latest_ms: Optional[float] = None
    if con is not None and _table_exists(con, "price_provider_health"):
        try:
            ref_ts = int(ts_ms or _now_ms())
            row = con.execute(
                """
                SELECT latency_ms
                FROM price_provider_health
                WHERE ok=1
                  AND ts_ms <= ?
                  AND ts_ms >= ?
                  AND latency_ms IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                (ref_ts, ref_ts - _env_int("EXEC_LOB_PROVIDER_HEALTH_MAX_AGE_MS", 5 * 60_000)),
            ).fetchone()
            if row and row[0] is not None:
                observed_latest_ms = max(0.0, _safe_float(row[0], 0.0))
        except Exception:
            observed_latest_ms = None

    max_observed = _env_float("EXEC_LOB_MAX_OBSERVED_PROVIDER_LATENCY_MS", 2_500.0)
    if observed_latest_ms is not None and observed_latest_ms > max_observed:
        blockers.append("observed_provider_latency_too_high")

    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "assumed_latency_ms": int(assumed),
        "max_assumed_latency_ms": int(max_assumed),
        "observed_provider_latency_ms": observed_latest_ms,
        "blockers": blockers,
        "reason": "ok" if not blockers else blockers[0],
    }


def simulator_calibration_snapshot(
    con: Any,
    *,
    symbol: Optional[str] = None,
    ts_ms: Optional[int] = None,
    min_fills: Optional[int] = None,
) -> Dict[str, Any]:
    """Require recent simulator fills carrying LOB calibration evidence."""

    required_fills = int(min_fills or _env_int("EXEC_LOB_MIN_CALIBRATION_FILLS", 20))
    if required_fills <= 0:
        return {
            "ok": True,
            "required_fills": 0,
            "sample_n": 0,
            "blockers": [],
            "reason": "disabled_by_threshold",
        }
    if not _table_exists(con, "broker_fills"):
        return {
            "ok": False,
            "required_fills": int(required_fills),
            "sample_n": 0,
            "blockers": ["simulator_calibration_missing"],
            "reason": "simulator_calibration_missing",
        }

    sym = str(symbol or "").upper().strip()
    ref_ts = int(ts_ms or _now_ms())
    max_age = _env_int("EXEC_LOB_CALIBRATION_MAX_AGE_MS", 7 * 86400_000)
    try:
        if sym:
            rows = con.execute(
                """
                SELECT ts_ms, explain_json
                FROM broker_fills
                WHERE symbol=?
                  AND ts_ms <= ?
                  AND ts_ms >= ?
                  AND explain_json IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (sym, int(ref_ts), int(ref_ts - max_age), max(required_fills * 3, 50)),
            ).fetchall() or []
        else:
            rows = con.execute(
                """
                SELECT ts_ms, explain_json
                FROM broker_fills
                WHERE ts_ms <= ?
                  AND ts_ms >= ?
                  AND explain_json IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (int(ref_ts), int(ref_ts - max_age), max(required_fills * 3, 50)),
            ).fetchall() or []
    except Exception:
        rows = []

    applied = 0
    impact_values: List[float] = []
    adverse_values: List[float] = []
    latest = 0
    for row in rows:
        payload = _json_obj(row[1] if len(row) > 1 else None)
        lob = payload.get("lob_simulation")
        if not isinstance(lob, dict) or not bool(lob.get("applied")):
            continue
        applied += 1
        latest = max(latest, _safe_int(row[0], 0))
        impact_values.append(_safe_float(lob.get("market_impact_bps"), 0.0))
        adverse_values.append(_safe_float(lob.get("adverse_selection_bps"), 0.0))

    blockers: List[str] = []
    if applied <= 0:
        blockers.append("simulator_calibration_missing")
    elif applied < required_fills:
        blockers.append("simulator_calibration_insufficient")

    return {
        "ok": not blockers,
        "required_fills": int(required_fills),
        "sample_n": int(applied),
        "latest_ts_ms": int(latest),
        "avg_impact_bps": float(sum(impact_values) / float(len(impact_values))) if impact_values else 0.0,
        "avg_adverse_selection_bps": float(sum(adverse_values) / float(len(adverse_values))) if adverse_values else 0.0,
        "blockers": blockers,
        "reason": "ok" if not blockers else blockers[0],
    }


def build_reactive_lob_simulation(
    con: Any,
    *,
    symbol: str,
    side: str,
    qty: float,
    mid_px: float,
    order_type: str,
    aggressiveness: str,
    ts_ms: int,
    latency_ms: int,
    liquidity_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build deterministic LOB realism adjustments for one simulated child fill."""

    sym = str(symbol or "").upper().strip()
    side_u = str(side or "").upper().strip()
    qty_abs = abs(_safe_float(qty, 0.0))
    mid = _safe_float(mid_px, 0.0)
    if not sym or qty_abs <= 0.0 or mid <= 0.0 or side_u not in {"BUY", "SELL"}:
        return {"applied": False, "reason": "invalid_inputs", "blockers": ["invalid_inputs"]}

    l2 = _latest_l2_row(con, symbol=sym, ts_ms=int(ts_ms))
    if not bool(l2.get("ok")):
        return {
            "applied": False,
            "reason": str(l2.get("reason") or "l2_unavailable"),
            "blockers": [str(l2.get("reason") or "l2_unavailable")],
        }

    bid_sz = max(0.0, _safe_float(l2.get("bid_sz"), 0.0))
    ask_sz = max(0.0, _safe_float(l2.get("ask_sz"), 0.0))
    if bid_sz <= 0.0 or ask_sz <= 0.0:
        return {
            "applied": False,
            "reason": "l2_top_depth_missing",
            "blockers": ["l2_top_depth_missing"],
            "l2_snapshot": l2,
        }

    same_depth = bid_sz if side_u == "BUY" else ask_sz
    contra_depth = ask_sz if side_u == "BUY" else bid_sz
    order_type_u = str(order_type or "").upper().strip()
    aggr_u = str(aggressiveness or "").upper().strip()
    spread_crossed = order_type_u == "MARKET" or aggr_u == "MARKET"

    queue_factor = 0.45
    if aggr_u == "PASSIVE":
        queue_factor = 0.80
    elif aggr_u == "AGGRESSIVE":
        queue_factor = 0.20
    if spread_crossed:
        queue_factor = 0.0

    queue_ahead = max(0.0, float(same_depth) * float(queue_factor))
    queue_position_pct = _clamp(queue_ahead / max(1e-9, queue_ahead + qty_abs), 0.0, 1.0)

    latency_s = max(0.0, float(latency_ms) / 1000.0)
    latency_consumption = max(0.0, float(contra_depth) * min(1.0, latency_s) * 0.08)
    available_after_queue = max(0.0, float(same_depth) - float(queue_ahead) + float(latency_consumption))
    partial_fill_cap = 1.0 if spread_crossed else _clamp(available_after_queue / max(qty_abs, 1e-9), 0.05, 1.0)

    ob = _clamp(_safe_float(l2.get("order_book_imbalance"), 0.0), -1.0, 1.0)
    ta = _clamp(_safe_float(l2.get("trade_aggressor_imbalance"), 0.0), -1.0, 1.0)
    directional_pressure = 0.5 * float(ob) + 0.5 * float(ta)
    side_sign = 1.0 if side_u == "BUY" else -1.0
    adverse_score = _clamp(float(side_sign) * float(directional_pressure), -1.0, 1.0)

    spread_bps = _safe_float(
        l2.get("spread_bps"),
        _safe_float((liquidity_snapshot or {}).get("true_spread_bps"), 0.0),
    )
    spread_widening = max(0.0, _safe_float(l2.get("spread_widening"), 0.0))
    adverse_selection_bps = max(0.0, adverse_score) * max(0.5, spread_bps * 0.40)
    adverse_selection_bps += min(5.0, spread_widening * max(0.5, spread_bps * 0.10))

    top_depth_ref = max(1e-9, contra_depth if spread_crossed else same_depth)
    top_depth_participation = qty_abs / float(top_depth_ref)
    impact_mult = _env_float("EXEC_LOB_IMPACT_MULT", 1.0)
    market_impact_bps = min(
        _env_float("EXEC_LOB_MAX_MARKET_IMPACT_BPS", 35.0),
        max(0.0, top_depth_participation)
        * max(0.25, spread_bps * 0.55)
        * max(0.1, impact_mult),
    )

    sweep_levels = 1
    sweep_bps = 0.0
    if spread_crossed and contra_depth > 0.0 and qty_abs > contra_depth:
        sweep_levels = int(math.ceil(qty_abs / max(contra_depth, 1e-9)))
        sweep_bps = min(
            _env_float("EXEC_LOB_MAX_SWEEP_BPS", 20.0),
            float(max(0, sweep_levels - 1)) * max(0.25, spread_bps * 0.35),
        )

    fill_probability_mult = 1.0
    if not spread_crossed:
        fill_probability_mult = 1.0 - (0.65 * queue_position_pct)
        if adverse_score > 0.0:
            fill_probability_mult -= 0.20 * adverse_score
        elif adverse_score < 0.0:
            fill_probability_mult += 0.08 * abs(adverse_score)
        fill_probability_mult = _clamp(fill_probability_mult, 0.10, 1.0)

    return {
        "applied": True,
        "source": "market_microstructure_signals",
        "symbol": sym,
        "side": side_u,
        "order_type": order_type_u,
        "aggressiveness": aggr_u,
        "l2_ts_ms": int(l2.get("ts_ms") or 0),
        "l2_age_ms": int(l2.get("age_ms") or 0),
        "provider": str(l2.get("provider") or ""),
        "bid_sz": float(bid_sz),
        "ask_sz": float(ask_sz),
        "same_side_depth_qty": float(same_depth),
        "contra_side_depth_qty": float(contra_depth),
        "queue_ahead_qty": float(queue_ahead),
        "queue_position_pct": float(queue_position_pct),
        "partial_fill_cap": float(partial_fill_cap),
        "fill_probability_mult": float(fill_probability_mult),
        "spread_crossed": bool(spread_crossed),
        "sweep_levels": int(sweep_levels),
        "sweep_bps": float(sweep_bps),
        "directional_pressure": float(directional_pressure),
        "adverse_score": float(max(0.0, adverse_score)),
        "adverse_selection_bps": float(adverse_selection_bps),
        "top_depth_participation": float(top_depth_participation),
        "market_impact_bps": float(market_impact_bps),
        "spread_bps": float(spread_bps),
        "calibration": {
            "method": "top_of_book_depth_participation",
            "impact_mult": float(impact_mult),
            "max_market_impact_bps": float(_env_float("EXEC_LOB_MAX_MARKET_IMPACT_BPS", 35.0)),
        },
    }


def deeplob_shadow_enabled() -> bool:
    return _env_bool("EXEC_LOB_DEEPLOB_SHADOW_ENABLED", False) or _env_bool("DEEPLOB_SHADOW_ENABLED", False)


def lob_deeplob_readiness_snapshot(
    con: Any,
    *,
    symbol: Optional[str] = None,
    ts_ms: Optional[int] = None,
    require_enabled: bool = True,
) -> Dict[str, Any]:
    required = bool(deeplob_shadow_enabled()) if require_enabled else True
    if not required:
        return {
            "ok": True,
            "required": False,
            "enabled": False,
            "shadow_only": True,
            "reason": "disabled",
            "blockers": [],
        }

    l2 = l2_data_quality_snapshot(con, symbol=symbol, ts_ms=ts_ms)
    latency = latency_assumption_snapshot(con, ts_ms=ts_ms)
    calibration = simulator_calibration_snapshot(con, symbol=symbol, ts_ms=ts_ms)

    blockers: List[str] = []
    blockers.extend(str(item) for item in list(l2.get("blockers") or []))
    blockers.extend(str(item) for item in list(latency.get("blockers") or []))
    blockers.extend(str(item) for item in list(calibration.get("blockers") or []))
    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "required": True,
        "enabled": bool(deeplob_shadow_enabled()),
        "shadow_only": True,
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "symbol": str(symbol or "").upper().strip(),
        "l2_data": dict(l2),
        "latency": dict(latency),
        "simulator_calibration": dict(calibration),
    }


def _normalize_window_rows(rows: Iterable[tuple]) -> List[List[float]]:
    out: List[List[float]] = []
    for row in reversed(list(rows or [])):
        bid_px = _safe_float(row[1], 0.0)
        ask_px = _safe_float(row[2], 0.0)
        details = _json_obj(row[10] if len(row) > 10 else None)
        bid_sz = _safe_float(row[3], _safe_float(details.get("bid_size") or details.get("bid_sz"), 0.0))
        ask_sz = _safe_float(row[4], _safe_float(details.get("ask_size") or details.get("ask_sz"), 0.0))
        mid = (bid_px + ask_px) / 2.0 if bid_px > 0.0 and ask_px > 0.0 else max(bid_px, ask_px, 1.0)
        spread_bps = _safe_float(row[5], 0.0)
        ob = _clamp(_safe_float(row[6], 0.0), -1.0, 1.0)
        ta = _clamp(_safe_float(row[7], 0.0), -1.0, 1.0)
        spread_widening = _safe_float(row[8], 0.0)
        composite = _safe_float(row[9], 0.0)
        out.append(
            [
                float((bid_px / mid) - 1.0) if mid > 0.0 else 0.0,
                float((ask_px / mid) - 1.0) if mid > 0.0 else 0.0,
                math.log1p(max(0.0, bid_sz)),
                math.log1p(max(0.0, ask_sz)),
                float(spread_bps / 100.0),
                float(ob),
                float(ta),
                float(spread_widening),
                float(composite),
            ]
        )
    return out


def build_deeplob_feature_window(
    con: Any,
    *,
    symbol: str,
    ts_ms: Optional[int] = None,
    window_n: Optional[int] = None,
) -> Dict[str, Any]:
    sym = str(symbol or "").upper().strip()
    n = int(window_n or _env_int("EXEC_LOB_FEATURE_WINDOW_N", 40))
    rows = _fetch_l2_rows(
        con,
        symbol=sym,
        ts_ms=ts_ms,
        lookback_ms=_env_int("EXEC_LOB_L2_LOOKBACK_MS", 10 * 60_000),
        limit=max(1, n),
    )
    if len(rows) < n:
        return {
            "ok": False,
            "reason": "feature_window_insufficient",
            "sample_n": int(len(rows)),
            "required_n": int(n),
            "shadow_only": True,
        }
    features = _normalize_window_rows(rows[:n])
    return {
        "ok": True,
        "symbol": sym,
        "sample_n": int(len(features)),
        "required_n": int(n),
        "feature_names": [
            "bid_px_rel_mid",
            "ask_px_rel_mid",
            "log_bid_sz",
            "log_ask_sz",
            "spread_bps_scaled",
            "order_book_imbalance",
            "trade_aggressor_imbalance",
            "spread_widening",
            "composite_score",
        ],
        "features": features,
        "shadow_only": True,
    }


def shadow_deeplob_execution_signal(
    con: Any,
    *,
    symbol: str,
    side: str,
    ts_ms: Optional[int] = None,
    latency_ms: Optional[int] = None,
) -> Dict[str, Any]:
    """Return a shadow-only execution-timing/adverse-selection signal.

    The signal is intentionally not an asset-selection or portfolio-sizing
    output. Callers may log it for comparison, but readiness failures block the
    model path and return no trade directive.
    """

    readiness = lob_deeplob_readiness_snapshot(con, symbol=symbol, ts_ms=ts_ms)
    if not bool(readiness.get("ok")):
        return {
            "ok": False,
            "blocked": True,
            "shadow_only": True,
            "reason": str(readiness.get("reason") or "readiness_blocked"),
            "blockers": list(readiness.get("blockers") or []),
            "readiness": dict(readiness),
        }

    window = build_deeplob_feature_window(con, symbol=symbol, ts_ms=ts_ms)
    if not bool(window.get("ok")):
        return {
            "ok": False,
            "blocked": True,
            "shadow_only": True,
            "reason": str(window.get("reason") or "feature_window_blocked"),
            "blockers": [str(window.get("reason") or "feature_window_blocked")],
            "readiness": dict(readiness),
            "feature_window": dict(window),
        }

    features = list(window.get("features") or [])
    latest = features[-1] if features else [0.0] * 9
    ob = _safe_float(latest[5] if len(latest) > 5 else 0.0, 0.0)
    ta = _safe_float(latest[6] if len(latest) > 6 else 0.0, 0.0)
    spread_scaled = max(0.0, _safe_float(latest[4] if len(latest) > 4 else 0.0, 0.0))
    pressure = 0.5 * ob + 0.5 * ta
    side_sign = 1.0 if str(side or "").upper().strip() in {"BUY", "LONG"} else -1.0
    adverse_score = _clamp(side_sign * pressure + min(0.35, spread_scaled * 0.10), 0.0, 1.0)
    base_latency = int(latency_ms if latency_ms is not None else _env_int("EXEC_LOB_ASSUMED_LATENCY_MS", 120))
    timing_delay_ms = int(round(float(base_latency) * _clamp(adverse_score, 0.0, 1.0)))
    return {
        "ok": True,
        "blocked": False,
        "shadow_only": True,
        "model_family": "deeplob_style_shadow_v1",
        "signal_type": "execution_timing_adverse_selection",
        "symbol": str(symbol or "").upper().strip(),
        "side": str(side or "").upper().strip(),
        "adverse_selection_score": float(adverse_score),
        "timing_delay_ms": int(timing_delay_ms),
        "readiness": dict(readiness),
        "feature_window": {
            "sample_n": int(window.get("sample_n") or 0),
            "feature_names": list(window.get("feature_names") or []),
        },
        "constraints": {
            "portfolio_selection_allowed": False,
            "portfolio_sizing_allowed": False,
            "broker_routing_allowed": False,
            "execution_timing_only": True,
        },
    }


__all__ = [
    "build_deeplob_feature_window",
    "build_reactive_lob_simulation",
    "deeplob_shadow_enabled",
    "l2_data_quality_snapshot",
    "latency_assumption_snapshot",
    "lob_deeplob_readiness_snapshot",
    "shadow_deeplob_execution_signal",
    "simulator_calibration_snapshot",
]
