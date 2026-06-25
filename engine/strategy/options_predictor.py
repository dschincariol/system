"""Shadow-only options VRP forecast and contract selection.

This module is an evidence-gated research layer. It does not feed the equity
predictor, champion/challenger routing, portfolio construction, or any live
broker adapter. Runtime emission is default-off via ``USE_OPTIONS_PREDICTOR``
and additionally requires an OPT-04 ablation evidence record with an
``ENABLE_SUPPORTED`` verdict.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import math
import os
import time
from typing import Any, Mapping, Optional, Sequence

from engine.execution.options_readiness import force_options_shadow_intent
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.strategy.learning import confidence_from_n

try:
    from engine.data.options_instrument import parse_option_symbol  # type: ignore
except Exception:

    def parse_option_symbol(symbol: object):  # type: ignore
        return None


LOG = get_logger("strategy.options_predictor")
USE_OPTIONS_PREDICTOR = os.environ.get("USE_OPTIONS_PREDICTOR", "0") == "1"
ENABLE_SUPPORTED = "ENABLE_SUPPORTED"
_SNAPSHOT_STALE_MS = 15 * 60 * 1000
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="strategy_options_predictor_nonfatal",
        code=str(code),
        message=str(code),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.options_predictor",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _safe_pos(value: Any) -> Optional[float]:
    out = _safe_float(value, float("nan"))
    if not math.isfinite(out) or out <= 0.0:
        return None
    return float(out)


def _safe_str(value: Any) -> str:
    return str(value or "").strip()


def _json_dumps(payload: Mapping[str, Any]) -> str:
    return json.dumps(dict(payload or {}), separators=(",", ":"), sort_keys=True)


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value or ""))
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _table_exists(con, table_name: str) -> bool:
    name = str(table_name or "").strip()
    if not name.replace("_", "").isalnum():
        return False
    try:
        row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,)).fetchone()
        if row:
            return True
    except Exception:  # no-op-guard: allow - SQLite probe may fail before Postgres metadata fallback.
        pass
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name=?
            LIMIT 1
            """,
            (name,),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _load_surface_row(con, underlying: str, ts_ms: int) -> dict[str, Any]:
    try:
        row = con.execute(
            """
            SELECT ts_ms, atm_iv_near, atm_iv_next, skew_25d, term_structure_slope
            FROM options_surface
            WHERE underlying=?
              AND ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(underlying).upper().strip(), int(ts_ms)),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal("OPTIONS_PREDICTOR_SURFACE_LOAD_FAILED", e, once_key=f"surface:{underlying}")
        return {}
    if not row:
        return {}
    return {
        "ts_ms": int(row[0] or 0),
        "atm_iv_near": _safe_pos(row[1]),
        "atm_iv_next": _safe_pos(row[2]),
        "skew_25d": _safe_float(row[3], 0.0),
        "term_structure_slope": _safe_float(row[4], 0.0),
    }


def _history_series(con, underlying: str, column: str, limit: int) -> list[float]:
    try:
        from engine.data.options_features import _history_series as options_history_series

        values = options_history_series(con, str(underlying).upper().strip(), str(column), int(limit))
        return [float(v) for v in values or [] if _safe_pos(v) is not None]
    except Exception as e:
        _warn_nonfatal(
            "OPTIONS_PREDICTOR_IV_HISTORY_LOAD_FAILED",
            e,
            once_key=f"iv_history:{underlying}:{column}",
        )
        return []


def _load_price_history(con, underlying: str, ts_ms: int, limit: int) -> list[tuple[int, float]]:
    symbol = str(underlying or "").upper().strip()
    if not symbol:
        return []
    rows: list[tuple[int, float]] = []
    for table_name, expr in (("price_quotes", "last"), ("prices", "COALESCE(px, price)")):
        try:
            raw_rows = con.execute(
                f"""
                SELECT ts_ms, {expr}
                FROM {table_name}
                WHERE symbol=?
                  AND ts_ms <= ?
                  AND {expr} IS NOT NULL
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                (symbol, int(ts_ms), int(max(2, limit))),
            ).fetchall()
        except Exception:
            raw_rows = []
        for raw_ts, raw_px in raw_rows or []:
            px = _safe_pos(raw_px)
            if px is None:
                continue
            rows.append((int(raw_ts), float(px)))
        if rows:
            break
    latest_by_ts: dict[int, float] = {}
    for raw_ts, px in rows:
        latest_by_ts[int(raw_ts)] = float(px)
    return sorted(latest_by_ts.items(), key=lambda item: item[0])


def _realized_vol(prices: Sequence[tuple[int, float]]) -> tuple[Optional[float], int]:
    returns: list[float] = []
    prev_px: Optional[float] = None
    for _ts, px in prices or []:
        cur = _safe_pos(px)
        if cur is None:
            continue
        if prev_px is not None and prev_px > 0.0:
            ret = math.log(float(cur) / float(prev_px))
            if math.isfinite(ret):
                returns.append(float(ret))
        prev_px = float(cur)
    if len(returns) < 2:
        return None, int(len(returns))
    mean = sum(returns) / float(len(returns))
    var = sum((ret - mean) ** 2 for ret in returns) / float(max(1, len(returns) - 1))
    vol = math.sqrt(max(0.0, var)) * math.sqrt(252.0)
    return (float(vol) if math.isfinite(vol) and vol > 0.0 else None), int(len(returns))


def forecast_vrp(con, underlying: str, *, ts_ms: int) -> dict[str, Any] | None:
    """Forecast an implied-vs-realized-vol signal, returning ``None`` on gaps."""

    symbol = str(underlying or "").upper().strip()
    if not symbol:
        return None
    try:
        surface = _load_surface_row(con, symbol, int(ts_ms))
        atm_near = _safe_pos(surface.get("atm_iv_near"))
        atm_next = _safe_pos(surface.get("atm_iv_next"))
        if atm_near is None and atm_next is None:
            return None

        iv_history = _history_series(con, symbol, "atm_iv_near", 60)
        prices = _load_price_history(
            con,
            symbol,
            int(ts_ms),
            int(os.environ.get("OPTIONS_PRED_REALIZED_VOL_LOOKBACK", "45") or "45") + 1,
        )
        realized_vol, realized_n = _realized_vol(prices)
        if realized_vol is None:
            return None

        if atm_near is not None and atm_next is not None:
            iv_forecast = (0.70 * float(atm_near)) + (0.30 * float(atm_next))
        else:
            iv_forecast = float(atm_near if atm_near is not None else atm_next)
        if not math.isfinite(iv_forecast) or iv_forecast <= 0.0:
            return None

        vrp = float(iv_forecast) - float(realized_vol)
        scale = max(0.05, abs(float(realized_vol)) * 0.75)
        signal = math.tanh(float(vrp) / float(scale))
        confidence_n = min(int(realized_n), int(len(iv_history) or realized_n))
        return {
            "underlying": symbol,
            "ts_ms": int(ts_ms),
            "surface_ts_ms": int(surface.get("ts_ms") or 0),
            "vrp_signal": float(max(-1.0, min(1.0, signal))),
            "iv_forecast": float(iv_forecast),
            "realized_vol": float(realized_vol),
            "confidence": float(confidence_from_n(int(confidence_n))),
            "sample_count": int(realized_n),
            "iv_history_count": int(len(iv_history)),
            "hypothesis": "positive_signal_implied_vol_rich_vs_realized",
        }
    except Exception as e:
        _warn_nonfatal("OPTIONS_PREDICTOR_VRP_FORECAST_FAILED", e, once_key=f"forecast_vrp:{symbol}")
        return None


def _days_to_expiration(expiration: Any, ts_ms: int) -> Optional[float]:
    try:
        expiry_dt = datetime.strptime(str(expiration or "")[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return float((expiry_dt.timestamp() * 1000.0 - float(ts_ms)) / 86_400_000.0)
    except Exception:
        return None


def _normalize_contract_type(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"c", "call"}:
        return "call"
    if text in {"p", "put"}:
        return "put"
    return text


def _build_occ_symbol(underlying: str, expiration: str, contract_type: str, strike: float) -> str:
    root = str(underlying or "").upper().strip()
    expiry = datetime.strptime(str(expiration)[:10], "%Y-%m-%d")
    right = "C" if _normalize_contract_type(contract_type) == "call" else "P"
    strike_int = int(round(float(strike) * 1000.0))
    return f"{root}{expiry:%y%m%d}{right}{strike_int:08d}"


def _canonical_contract_symbol(row: Mapping[str, Any], underlying: str) -> tuple[str, Any] | tuple[str, None]:
    for candidate in (row.get("contract_symbol"), row.get("contract"), row.get("contract_key")):
        text = str(candidate or "").upper().strip().replace(" ", "")
        if not text:
            continue
        parsed = parse_option_symbol(text)
        if parsed is not None:
            return str(getattr(parsed, "occ_symbol", text)), parsed
    try:
        built = _build_occ_symbol(
            str(underlying),
            str(row.get("expiration") or ""),
            str(row.get("contract_type") or ""),
            float(row.get("strike") or 0.0),
        )
    except Exception:
        return "", None
    parsed = parse_option_symbol(built)
    if parsed is None:
        return "", None
    return str(getattr(parsed, "occ_symbol", built)), parsed


def _load_chain_rows(con, underlying: str, ts_ms: int) -> list[dict[str, Any]]:
    symbol = str(underlying or "").upper().strip()
    if not symbol:
        return []
    try:
        row = con.execute(
            """
            SELECT MAX(ts_ms)
            FROM options_chain_v2
            WHERE underlying=?
              AND ts_ms <= ?
            """,
            (symbol, int(ts_ms)),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal("OPTIONS_PREDICTOR_CHAIN_MAX_TS_FAILED", e, once_key=f"chain_max:{symbol}")
        return []
    snapshot_ts = int(row[0]) if row and row[0] is not None else None
    if snapshot_ts is None:
        return []
    try:
        rows = con.execute(
            """
            SELECT ts_ms, contract, expiration, contract_type, strike, iv, open_interest, volume, delta, gamma
            FROM options_chain_v2
            WHERE underlying=?
              AND ts_ms >= ?
              AND ts_ms <= ?
            ORDER BY contract ASC, ts_ms DESC
            """,
            (symbol, int(snapshot_ts) - _SNAPSHOT_STALE_MS, int(snapshot_ts)),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal("OPTIONS_PREDICTOR_CHAIN_ROWS_FAILED", e, once_key=f"chain_rows:{symbol}")
        return []

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_ts, contract, expiration, contract_type, strike, iv, open_interest, volume, delta, gamma in rows or []:
        base = {
            "ts_ms": int(raw_ts),
            "contract": str(contract or "").upper().strip(),
            "expiration": _safe_str(expiration),
            "contract_type": _normalize_contract_type(contract_type),
            "strike": _safe_float(strike, 0.0),
            "iv": _safe_pos(iv),
            "open_interest": _safe_pos(open_interest),
            "volume": _safe_pos(volume),
            "delta": (_safe_float(delta, float("nan")) if delta is not None else float("nan")),
            "gamma": (_safe_float(gamma, float("nan")) if gamma is not None else float("nan")),
        }
        contract_symbol, parsed = _canonical_contract_symbol(base, symbol)
        if not contract_symbol or contract_symbol in seen or parsed is None:
            continue
        dte = _days_to_expiration(base["expiration"], int(ts_ms))
        if dte is None:
            continue
        base["contract_symbol"] = contract_symbol
        base["dte"] = float(dte)
        seen.add(contract_symbol)
        deduped.append(base)
    return deduped


def _closest_by_abs_delta(rows: Sequence[Mapping[str, Any]], want_type: str, target_abs_delta: float) -> Optional[dict[str, Any]]:
    best: Optional[dict[str, Any]] = None
    best_dist: Optional[float] = None
    for row in rows or []:
        if str(row.get("contract_type") or "") != str(want_type):
            continue
        delta = _safe_float(row.get("delta"), float("nan"))
        if not math.isfinite(delta):
            continue
        dist = abs(abs(float(delta)) - float(target_abs_delta))
        if best is None or best_dist is None or dist < best_dist:
            best = dict(row)
            best_dist = float(dist)
    return best


def _hedge_leg(rows: Sequence[Mapping[str, Any]], short_leg: Mapping[str, Any], *, direction: str) -> Optional[dict[str, Any]]:
    ctype = str(short_leg.get("contract_type") or "")
    expiry = str(short_leg.get("expiration") or "")
    short_strike = float(short_leg.get("strike") or 0.0)
    candidates: list[Mapping[str, Any]] = [
        row
        for row in rows or []
        if str(row.get("contract_type") or "") == ctype and str(row.get("expiration") or "") == expiry
    ]
    if direction == "above":
        candidates = [row for row in candidates if float(row.get("strike") or 0.0) > short_strike]
        candidates.sort(key=lambda row: float(row.get("strike") or 0.0))
    else:
        candidates = [row for row in candidates if float(row.get("strike") or 0.0) < short_strike]
        candidates.sort(key=lambda row: float(row.get("strike") or 0.0), reverse=True)
    return dict(candidates[0]) if candidates else None


def _leg(row: Mapping[str, Any], side: str) -> dict[str, Any]:
    return {
        "contract_symbol": str(row.get("contract_symbol") or ""),
        "side": str(side).upper(),
        "ratio": 1,
        "delta": float(_safe_float(row.get("delta"), 0.0)),
        "dte": float(_safe_float(row.get("dte"), 0.0)),
        "expiration": str(row.get("expiration") or ""),
        "contract_type": str(row.get("contract_type") or ""),
        "strike": float(_safe_float(row.get("strike"), 0.0)),
    }


def _directional_score(value: Any) -> float:
    if isinstance(value, Mapping):
        for key in ("directional_view", "direction", "score", "signal", "view"):
            if key in value:
                return _safe_float(value.get(key), 0.0)
        return 0.0
    return _safe_float(value, 0.0)


def select_option_structure(
    con,
    *,
    underlying: str,
    vrp_signal: float,
    directional_view: Any,
    ts_ms: int,
) -> dict[str, Any] | None:
    """Select a defined-risk shadow options structure from the latest chain."""

    symbol = str(underlying or "").upper().strip()
    if not symbol:
        return None
    rows = _load_chain_rows(con, symbol, int(ts_ms))
    if not rows:
        return None
    min_dte = _safe_float(os.environ.get("OPTIONS_MIN_DTE_DAYS", "7"), 7.0)
    max_dte = _safe_float(os.environ.get("OPTIONS_MAX_DTE_DAYS", "60"), 60.0)
    target_delta = abs(_safe_float(os.environ.get("OPTIONS_PRED_TARGET_DELTA", "0.30"), 0.30))
    target_delta = min(0.95, max(0.05, target_delta))
    eligible = [row for row in rows if float(min_dte) <= float(row.get("dte") or 0.0) <= float(max_dte)]
    if not eligible:
        return None

    direction = _directional_score(directional_view)
    if float(vrp_signal) >= 0.0:
        if direction >= 0.0:
            short_leg = _closest_by_abs_delta(eligible, "put", target_delta)
            long_leg = _hedge_leg(eligible, short_leg or {}, direction="below") if short_leg else None
            structure_type = "put_credit_vertical"
        else:
            short_leg = _closest_by_abs_delta(eligible, "call", target_delta)
            long_leg = _hedge_leg(eligible, short_leg or {}, direction="above") if short_leg else None
            structure_type = "call_credit_vertical"
        if not short_leg or not long_leg:
            return None
        legs = [_leg(short_leg, "SELL"), _leg(long_leg, "BUY")]
    else:
        if direction >= 0.0:
            long_leg = _closest_by_abs_delta(eligible, "call", target_delta)
            short_leg = _hedge_leg(eligible, long_leg or {}, direction="above") if long_leg else None
            structure_type = "call_debit_vertical"
        else:
            long_leg = _closest_by_abs_delta(eligible, "put", target_delta)
            short_leg = _hedge_leg(eligible, long_leg or {}, direction="below") if long_leg else None
            structure_type = "put_debit_vertical"
        if not long_leg or not short_leg:
            return None
        legs = [_leg(long_leg, "BUY"), _leg(short_leg, "SELL")]

    return {
        "underlying": symbol,
        "structure_type": structure_type,
        "target_abs_delta": float(target_delta),
        "vrp_signal": float(max(-1.0, min(1.0, _safe_float(vrp_signal, 0.0)))),
        "directional_view": float(direction),
        "min_dte": float(min_dte),
        "max_dte": float(max_dte),
        "legs": legs,
    }


def build_options_shadow_intent(
    *,
    underlying: str,
    forecast: Mapping[str, Any],
    structure: Mapping[str, Any],
    ts_ms: int,
) -> dict[str, Any] | None:
    legs = list((structure or {}).get("legs") or [])
    if not legs:
        return None
    first_leg = dict(legs[0] or {})
    contract = str(first_leg.get("contract_symbol") or "")
    parsed = parse_option_symbol(contract)
    if parsed is None:
        return None
    intent = {
        "source": "options_predictor",
        "symbol": str(underlying or getattr(parsed, "underlying", "")).upper().strip(),
        "underlying": str(underlying or getattr(parsed, "underlying", "")).upper().strip(),
        "instrument_type": "option",
        "option_symbol": contract,
        "option_contract": contract,
        "contract_type": "call" if str(getattr(parsed, "right", "")).upper() == "C" else "put",
        "expiration": getattr(parsed, "expiry").isoformat(),
        "strike": float(getattr(parsed, "strike")),
        "execution_target": "shadow",
        "ts_ms": int(ts_ms),
        "option_strategy": str((structure or {}).get("structure_type") or ""),
        "legs": legs,
        "forecast": dict(forecast or {}),
        "structure": dict(structure or {}),
        "decision": {
            "source": "options_predictor",
            "vrp_signal": float(_safe_float((forecast or {}).get("vrp_signal"), 0.0)),
        },
        "competition": {"allowed": False, "blocked": True, "reason": "options_predictor_shadow_only"},
    }
    return force_options_shadow_intent(intent, reason="options_predictor_shadow_only")


def _ensure_shadow_table(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS options_predictor_shadow (
          underlying TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          vrp_signal DOUBLE PRECISION,
          iv_forecast DOUBLE PRECISION,
          realized_vol DOUBLE PRECISION,
          confidence DOUBLE PRECISION,
          structure_json TEXT,
          evidence_gate_ok BOOLEAN,
          UNIQUE(underlying, ts_ms)
        )
        """
    )


def _persist_shadow_row(
    con,
    *,
    underlying: str,
    ts_ms: int,
    forecast: Mapping[str, Any],
    structure: Mapping[str, Any] | None,
    intent: Mapping[str, Any] | None,
    evidence_gate_ok: bool,
) -> None:
    _ensure_shadow_table(con)
    payload = {
        "forecast": dict(forecast or {}),
        "structure": dict(structure or {}),
        "intent": dict(intent or {}),
        "shadow_only": True,
    }
    con.execute(
        """
        INSERT INTO options_predictor_shadow(
          underlying, ts_ms, vrp_signal, iv_forecast, realized_vol, confidence,
          structure_json, evidence_gate_ok
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(underlying, ts_ms) DO UPDATE SET
          vrp_signal=excluded.vrp_signal,
          iv_forecast=excluded.iv_forecast,
          realized_vol=excluded.realized_vol,
          confidence=excluded.confidence,
          structure_json=excluded.structure_json,
          evidence_gate_ok=excluded.evidence_gate_ok
        """,
        (
            str(underlying).upper().strip(),
            int(ts_ms),
            float(_safe_float((forecast or {}).get("vrp_signal"), 0.0)),
            float(_safe_float((forecast or {}).get("iv_forecast"), 0.0)),
            float(_safe_float((forecast or {}).get("realized_vol"), 0.0)),
            float(_safe_float((forecast or {}).get("confidence"), 0.0)),
            _json_dumps(payload),
            1 if bool(evidence_gate_ok) else 0,
        ),
    )


def _runtime_meta_payload(con, key: str) -> dict[str, Any]:
    if not _table_exists(con, "runtime_meta"):
        return {}
    try:
        row = con.execute("SELECT value FROM runtime_meta WHERE key=? LIMIT 1", (str(key),)).fetchone()
    except Exception as e:
        _warn_nonfatal("OPTIONS_PREDICTOR_EVIDENCE_READ_FAILED", e, once_key=f"evidence:{key}", key=str(key))
        return {}
    if not row:
        return {}
    return _json_dict(row[0])


def _evidence_verdict(payload: Mapping[str, Any]) -> str:
    data = dict(payload or {})
    for key in ("verdict", "status_verdict"):
        if data.get(key):
            return str(data.get(key) or "").upper().strip()
    for key in ("enablement", "result", "evaluation", "report"):
        child = data.get(key)
        if isinstance(child, Mapping):
            verdict = _evidence_verdict(child)
            if verdict:
                return verdict
    return ""


def _options_feature_evidence_ok(con, underlying: str) -> bool:
    symbol = str(underlying or "").upper().strip()
    keys = (
        f"options_feature_ablation_report::{symbol}",
        f"options_feature_ablation_status::{symbol}",
        "options_feature_ablation_report",
        "options_feature_ablation_status",
    )
    for key in keys:
        payload = _runtime_meta_payload(con, key)
        if not payload:
            continue
        if _evidence_verdict(payload) == ENABLE_SUPPORTED:
            return True
    return False


def _default_underlyings(con, ts_ms: int) -> list[str]:
    try:
        rows = con.execute(
            """
            SELECT DISTINCT underlying
            FROM options_surface
            WHERE ts_ms <= ?
            ORDER BY underlying ASC
            """,
            (int(ts_ms),),
        ).fetchall()
    except Exception:
        return []
    out: list[str] = []
    for row in rows or []:
        text = str(row[0] or "").upper().strip()
        if text and text not in out:
            out.append(text)
    return out


def run_options_predictor(con, underlyings: Optional[Sequence[str]] = None) -> dict[str, int]:
    """Run the shadow predictor if both env and OPT-04 evidence gates pass."""

    if not USE_OPTIONS_PREDICTOR:
        return {"forecasts": 0, "intents": 0}
    ts_ms = int(time.time() * 1000)
    symbols = [str(sym or "").upper().strip() for sym in list(underlyings or []) if str(sym or "").strip()]
    if not symbols:
        symbols = _default_underlyings(con, ts_ms)
    forecasts = 0
    intents = 0
    for symbol in symbols:
        if not _options_feature_evidence_ok(con, symbol):
            continue
        forecast = forecast_vrp(con, symbol, ts_ms=ts_ms)
        if forecast is None:
            continue
        forecasts += 1
        structure = select_option_structure(
            con,
            underlying=symbol,
            vrp_signal=float(forecast.get("vrp_signal") or 0.0),
            directional_view=0.0,
            ts_ms=ts_ms,
        )
        intent = None
        if structure:
            intent = build_options_shadow_intent(underlying=symbol, forecast=forecast, structure=structure, ts_ms=ts_ms)
            if intent is not None:
                intents += 1
        _persist_shadow_row(
            con,
            underlying=symbol,
            ts_ms=int(ts_ms),
            forecast=forecast,
            structure=structure,
            intent=intent,
            evidence_gate_ok=True,
        )
    try:
        con.commit()
    except Exception:  # no-op-guard: allow - optional shadow evidence commit is best-effort.
        pass
    return {"forecasts": int(forecasts), "intents": int(intents)}


__all__ = [
    "USE_OPTIONS_PREDICTOR",
    "build_options_shadow_intent",
    "forecast_vrp",
    "run_options_predictor",
    "select_option_structure",
]
