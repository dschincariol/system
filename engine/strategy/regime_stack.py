"""
FILE: regime_stack.py

Human-readable purpose:
Builds the multi-layer regime vector used by the strategy stack. It combines
macro, asset-class, and microstructure signals into a weighted market regime
representation that downstream strategy components can consume consistently.

Hierarchical Regime Stack (3-layer weighted regime vector)

Layers (weights, not labels):
- Macro: risk_on/off, vol_expansion, credit_stress
- Asset-class: etf_like vs single_stock_like
- Microstructure: momentum_dominant, auction_heavy, news_shock

Also:
- regime_compatibility(profile, vector) -> [0,1]
- regime_model_version() string
"""

import math
import os
import json
import logging
import time
from typing import Any, Dict, List, Optional

import numpy as np

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect
from engine.runtime.factor_universe import _get_feature_asof as _get_factor_feature_asof
from engine.strategy.distribution_drift import get_latest_distribution_drift_snapshot


_REGIME_MODEL_VERSION = os.environ.get("REGIME_MODEL_VERSION", "regime_stack_v1")
_REGIME_VOL_LOOKBACK = int(os.environ.get("REGIME_VOL_LOOKBACK", "60"))
_REGIME_VOL_RECENT_N = int(os.environ.get("REGIME_VOL_RECENT_N", "12"))
_REGIME_LIQ_WINDOW_MS = int(os.environ.get("REGIME_LIQ_WINDOW_MS", str(6 * 60 * 60 * 1000)))
_REGIME_DD_WINDOW_MS = int(os.environ.get("REGIME_DD_WINDOW_MS", str(24 * 60 * 60 * 1000)))
LOG = get_logger("engine.strategy.regime_stack")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.regime_stack",
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def regime_model_version() -> str:
    return str(_REGIME_MODEL_VERSION)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _clamp01(x: float) -> float:
    try:
        x = float(x)
    except Exception as e:
        _warn_nonfatal(
            "regime_stack_clamp01_parse_failed",
            "REGIME_STACK_CLAMP01_PARSE_FAILED",
            e,
            warn_key=f"regime_stack_clamp01:{x}",
            raw_value=x,
        )
        return 0.0
    if x != x:
        return 0.0
    return float(max(0.0, min(1.0, x)))


def _sigmoid01(x: float, k: float = 1.0) -> float:
    try:
        x = float(x)
        k = float(k)
    except Exception as e:
        _warn_nonfatal(
            "regime_stack_sigmoid_input_parse_failed",
            "REGIME_STACK_SIGMOID_INPUT_PARSE_FAILED",
            e,
            warn_key=f"regime_stack_sigmoid_input:{x}:{k}",
            x=x,
            k=k,
        )
        return 0.5
    if x != x:
        return 0.5
    try:
        return float(1.0 / (1.0 + math.exp(-k * x)))
    except Exception as e:
        _warn_nonfatal(
            "regime_stack_sigmoid_compute_failed",
            "REGIME_STACK_SIGMOID_COMPUTE_FAILED",
            e,
            warn_key=f"regime_stack_sigmoid_compute:{x}:{k}",
            x=x,
            k=k,
        )
        return 0.5


def _z_to_weight(z: float, k: float = 1.0, center: float = 0.0) -> float:
    return _sigmoid01(float(z) - float(center), k=float(k))


def _safe_f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v:
            return float(d)
        return float(v)
    except Exception as e:
        _warn_nonfatal(
            "regime_stack_safe_float_failed",
            "REGIME_STACK_SAFE_FLOAT_FAILED",
            e,
            warn_key=f"regime_stack_safe_float:{x}",
            raw_value=x,
        )
        return float(d)


def _std(vals: List[float]) -> float:
    if not vals or len(vals) < 2:
        return 0.0
    m = sum(vals) / float(len(vals))
    var = sum((float(x) - m) ** 2 for x in vals) / float(max(1, len(vals) - 1))
    return float(math.sqrt(max(0.0, var)))


def _read_price_series(con, symbol: str, ts_ms: int, limit: int) -> List[float]:
    try:
        # Reads are as-of the requested timestamp so backtests and live snapshots
        # use the same access semantics.
        rows = con.execute(
            """
            SELECT COALESCE(px, price) AS px
            FROM prices
            WHERE symbol=? AND ts_ms <= ? AND COALESCE(px, price) IS NOT NULL
            ORDER BY ts_ms DESC
            LIMIT ?
            """,
            (str(symbol), int(ts_ms), int(max(5, limit))),
        ).fetchall()
    except Exception:
        rows = []

    vals = [float(r[0]) for r in (rows or []) if r and r[0] is not None]
    vals.reverse()
    return vals


def _pct_returns(vals: List[float]) -> List[float]:
    out: List[float] = []
    for i in range(1, len(vals)):
        a = _safe_f(vals[i - 1], 0.0)
        b = _safe_f(vals[i], 0.0)
        if a > 0.0 and b > 0.0:
            out.append(float((b / a) - 1.0))
    return out


def _liquidity_snapshot(con, ts_ms: int) -> Dict[str, Any]:
    since_ms = int(ts_ms) - int(_REGIME_LIQ_WINDOW_MS)
    try:
        rows = con.execute(
            """
            SELECT liquidity, COUNT(*)
            FROM execution_fills
            WHERE fill_ts_ms BETWEEN ? AND ?
            GROUP BY liquidity
            """,
            (int(since_ms), int(ts_ms)),
        ).fetchall()
    except Exception:
        rows = []

    total = 0.0
    passive = 0.0
    aggressive = 0.0

    for r in rows or []:
        liq = str((r or [None])[0] or "").upper().strip()
        n = _safe_f((r or [None, 0])[1], 0.0)
        total += float(n)
        if liq in {"PASSIVE", "MAKER", "ADD", "ADDED", "POST_ONLY"}:
            passive += float(n)
        elif liq in {"AGGRESSIVE", "TAKER", "REMOVE", "REMOVED", "MARKET"}:
            aggressive += float(n)

    passive_ratio = float(passive / total) if total > 0.0 else 0.5
    aggressive_ratio = float(aggressive / total) if total > 0.0 else 0.5

    thin = _clamp01((0.55 - passive_ratio) / 0.55) if total > 0.0 else 0.5
    ample = _clamp01((passive_ratio - 0.45) / 0.55) if total > 0.0 else 0.0

    if total < 5.0:
        label = "UNKNOWN"
    elif thin >= 0.60:
        label = "THIN"
    elif ample >= 0.55:
        label = "AMPLE"
    else:
        label = "NORMAL"

    return {
        "fill_count": float(total),
        "passive_ratio": float(passive_ratio),
        "aggressive_ratio": float(aggressive_ratio),
        "thin_score": float(thin),
        "ample_score": float(ample),
        "label": str(label),
    }


def _drawdown_shift_snapshot(con, ts_ms: int) -> Dict[str, Any]:
    recent_since = int(ts_ms) - int(_REGIME_DD_WINDOW_MS)
    prior_since = int(recent_since) - int(_REGIME_DD_WINDOW_MS)

    try:
        recent = con.execute(
            """
            SELECT AVG(ABS(COALESCE(drawdown_contrib, 0.0))), COUNT(*)
            FROM execution_capital_efficiency
            WHERE ts_ms BETWEEN ? AND ?
            """,
            (int(recent_since), int(ts_ms)),
        ).fetchone()
    except Exception:
        recent = None

    try:
        prior = con.execute(
            """
            SELECT AVG(ABS(COALESCE(drawdown_contrib, 0.0))), COUNT(*)
            FROM execution_capital_efficiency
            WHERE ts_ms BETWEEN ? AND ?
            """,
            (int(prior_since), int(recent_since - 1)),
        ).fetchone()
    except Exception:
        prior = None

    recent_avg = _safe_f((recent or [0.0])[0], 0.0)
    recent_n = _safe_f((recent or [None, 0])[1], 0.0)
    prior_avg = _safe_f((prior or [0.0])[0], 0.0)
    prior_n = _safe_f((prior or [None, 0])[1], 0.0)

    ratio = float(recent_avg / max(1e-9, prior_avg if prior_avg > 0.0 else 1e-9)) if recent_avg > 0.0 else 0.0
    shift = _clamp01((ratio - 1.0) / 1.5) if prior_avg > 0.0 else _clamp01(recent_avg)

    if recent_n < 5.0:
        label = "UNKNOWN"
    elif shift >= 0.60:
        label = "SHIFT"
    else:
        label = "STABLE"

    return {
        "recent_avg": float(recent_avg),
        "recent_n": float(recent_n),
        "prior_avg": float(prior_avg),
        "prior_n": float(prior_n),
        "ratio": float(ratio),
        "shift_score": float(shift),
        "label": str(label),
    }


def _is_etf_like(sym: str) -> bool:
    s = str(sym or "").upper().strip()
    if not s:
        return False

    base = {
        "SPY",
        "QQQ",
        "IWM",
        "DIA",
        "VTI",
        "VOO",
        "IVV",
        "HYG",
        "LQD",
        "TLT",
        "IEF",
        "SHY",
        "GLD",
        "SLV",
        "USO",
        "UNG",
        "XLF",
        "XLK",
        "XLE",
        "XLV",
        "XLY",
        "XLP",
        "XLI",
        "XLB",
        "XLU",
        "XLC",
        "VIX",
    }

    raw = os.environ.get("REGIME_ETF_SYMBOLS", "").strip()
    if raw:
        try:
            extra = json.loads(raw)
            if isinstance(extra, list):
                for it in extra:
                    t = str(it or "").upper().strip()
                    if t:
                        base.add(t)
        except Exception as exc:
            _warn_nonfatal(
                "regime_stack_etf_symbols_parse_failed",
                "REGIME_STACK_ETF_SYMBOLS_PARSE_FAILED",
                exc,
                warn_key="regime_stack_etf_symbols_parse_failed",
            )

    return s in base


def compute_regime_vector(
    *,
    symbol: Optional[str] = None,
    ts_ms: Optional[int] = None,
    con=None,
    include_hmm: bool = True,
) -> Dict[str, Any]:

    sym = str(symbol or "").upper().strip()
    t = int(ts_ms or 0) or int(time.time() * 1000)

    close_con = False
    if con is None:
        con = connect()
        close_con = True

    try:

        # ------------------------------
        # Existing macro factors
        # ------------------------------

        try:
            vix_z = _safe_f(_get_factor_feature_asof(con, "vol.vix_z", int(t)), 0.0)
        except Exception:
            vix_z = 0.0

        try:
            rv20_z = _safe_f(_get_factor_feature_asof(con, "vol.rv20_z", int(t)), 0.0)
        except Exception:
            rv20_z = 0.0

        try:
            credit_z = _safe_f(_get_factor_feature_asof(con, "credit.hyg_lqd_spread_z", int(t)), 0.0)
        except Exception:
            credit_z = 0.0


        risk_off = _clamp01(_z_to_weight(vix_z, k=0.9))
        vol_expansion = _clamp01(_z_to_weight(rv20_z, k=0.9))
        credit_stress = _clamp01(_z_to_weight(credit_z, k=0.9))


        # ------------------------------
        # VOLATILITY CLUSTER DETECTION
        # ------------------------------

        recent_vol = 0.0
        prior_vol = 0.0
        vol_ratio = 0.0

        try:
            rows = con.execute(
                """
                SELECT px
                FROM prices
                WHERE symbol=?
                ORDER BY ts_ms DESC
                LIMIT 80
                """,
                (sym or "SPY",),
            ).fetchall()

            px = [float(r[0]) for r in rows if r and r[0] is not None]
            px.reverse()

            rets = []
            for i in range(1, len(px)):
                if px[i - 1] > 0:
                    rets.append((px[i] / px[i - 1]) - 1.0)

            recent = rets[-12:]
            prior = rets[:-12]

            if recent:
                recent_vol = float(np.std(recent))
            if prior:
                prior_vol = float(np.std(prior))

            if prior_vol > 0:
                vol_ratio = recent_vol / prior_vol

        except Exception as exc:
            _warn_nonfatal(
                "regime_stack_vol_cluster_failed",
                "REGIME_STACK_VOL_CLUSTER_FAILED",
                exc,
                warn_key="regime_stack_vol_cluster_failed",
                symbol=str(sym or "SPY"),
            )

        vol_cluster = _clamp01((vol_ratio - 1.0) / 1.5)


        # ------------------------------
        # LIQUIDITY REGIME
        # ------------------------------

        thin_liquidity = 0.0

        try:
            rows = con.execute(
                """
                SELECT liquidity, COUNT(*)
                FROM execution_fills
                WHERE fill_ts_ms > ?
                GROUP BY liquidity
                """,
                (t - 3600000 * 6,),
            ).fetchall()

            passive = 0
            total = 0

            for r in rows:

                typ = str(r[0]).upper()
                n = int(r[1])

                total += n

                if typ in ("PASSIVE", "MAKER", "POST_ONLY"):
                    passive += n

            if total > 0:

                passive_ratio = passive / total

                thin_liquidity = _clamp01((0.5 - passive_ratio) / 0.5)

        except Exception as exc:
            _warn_nonfatal(
                "regime_stack_liquidity_regime_failed",
                "REGIME_STACK_LIQUIDITY_REGIME_FAILED",
                exc,
                warn_key="regime_stack_liquidity_regime_failed",
                symbol=str(sym or "SPY"),
            )


        # ------------------------------
        # DRAWDOWN REGIME SHIFT
        # ------------------------------

        dd_shift = 0.0

        try:
            rows = con.execute(
                """
                SELECT ABS(drawdown_contrib)
                FROM execution_capital_efficiency
                ORDER BY ts_ms DESC
                LIMIT 200
                """
            ).fetchall()

            vals = [float(r[0]) for r in rows if r and r[0] is not None]

            if vals:

                recent = vals[:40]
                prior = vals[40:120]

                r = np.mean(recent) if recent else 0
                p = np.mean(prior) if prior else 0

                if p > 0:
                    dd_shift = _clamp01(float((r / p - 1.0) / 2.0))

        except Exception as exc:
            _warn_nonfatal(
                "regime_stack_drawdown_regime_failed",
                "REGIME_STACK_DRAWDOWN_REGIME_FAILED",
                exc,
                warn_key="regime_stack_drawdown_regime_failed",
                symbol=str(sym or "SPY"),
            )


        macro = {

            "risk_off": risk_off,
            "risk_on": 1.0 - risk_off,
            "vol_expansion": vol_expansion,
            "credit_stress": credit_stress,
            "drawdown_shift": dd_shift,

        }


        micro = {

            "vol_clustered": vol_cluster,
            "liquidity_thin": thin_liquidity

        }


        # ------------------------------
        # CONFIDENCE
        # ------------------------------

        macro_conf = _clamp01((abs(vix_z) + abs(rv20_z) + abs(credit_z)) / 6.0)

        micro_conf = _clamp01(
            vol_cluster * 0.5 +
            thin_liquidity * 0.5
        )

        overall_conf = _clamp01((macro_conf + micro_conf) / 2.0)


        result = {

            "ts_ms": int(t),

            "macro": macro,

            "micro": micro,

            "regimes": {

                "volatility": "CLUSTERED" if vol_cluster > 0.6 else "NORMAL",
                "liquidity": "THIN" if thin_liquidity > 0.6 else "NORMAL",
                "drawdown": "SHIFT" if dd_shift > 0.6 else "STABLE"

            },

            "confidence": {

                "overall": overall_conf,
                "macro": macro_conf,
                "micro": micro_conf

            },

        }

        if include_hmm:
            try:
                from engine.strategy.hmm_regime import resolve_hmm_regime_snapshot

                hmm_signal = resolve_hmm_regime_snapshot(
                    symbol=str(sym or "SPY"),
                    ts_ms=int(t),
                    con=con,
                    regime_vector=dict(result),
                )
                if isinstance(hmm_signal, dict):
                    result["hmm"] = dict(hmm_signal)
            except Exception as exc:
                _warn_nonfatal(
                    "regime_stack_hmm_signal_failed",
                    "REGIME_STACK_HMM_SIGNAL_FAILED",
                    exc,
                    warn_key=f"regime_stack_hmm_signal_failed:{sym or 'SPY'}",
                    symbol=str(sym or "SPY"),
                    ts_ms=int(t),
                )

        return result

    finally:

        if close_con:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "regime_stack_compute_close_failed",
                    "REGIME_STACK_COMPUTE_CLOSE_FAILED",
                    exc,
                    warn_key="regime_stack_compute_close_failed",
                )
    sym = str(symbol or "").upper().strip()
    t = int(ts_ms or 0) or _now_ms()

    close_con = False
    if con is None:
        con = connect()
        close_con = True

    try:
        # ----------------------------
        # MACRO layer
        # ----------------------------
        try:
            vix_z = _safe_f(_get_factor_feature_asof(con, "vol.vix_z", int(t)), 0.0)
        except Exception:
            vix_z = 0.0

        try:
            rv20_z = _safe_f(_get_factor_feature_asof(con, "vol.rv20_z", int(t)), 0.0)
        except Exception:
            rv20_z = 0.0

        try:
            credit_z = _safe_f(_get_factor_feature_asof(con, "credit.hyg_lqd_spread_z", int(t)), 0.0)
        except Exception:
            credit_z = 0.0

        dd_shift = _drawdown_shift_snapshot(con, int(t))
        drawdown_shift = _clamp01(dd_shift.get("shift_score", 0.0))

        risk_off = _clamp01(_z_to_weight(vix_z, k=0.85, center=0.0))
        vol_expansion = _clamp01(_z_to_weight(rv20_z, k=0.85, center=0.0))
        credit_stress = _clamp01(_z_to_weight(credit_z, k=0.85, center=0.0))

        macro = {
            "risk_off": float(risk_off),
            "risk_on": float(_clamp01(1.0 - risk_off)),
            "vol_expansion": float(vol_expansion),
            "credit_stress": float(credit_stress),
            "drawdown_shift": float(drawdown_shift),
        }

        # ----------------------------
        # ASSET layer
        # ----------------------------
        etf_like = 1.0 if _is_etf_like(sym) else 0.0
        asset = {
            "etf_like": float(etf_like),
            "single_stock_like": float(_clamp01(1.0 - etf_like)),
        }

        # ----------------------------
        # MICRO layer
        # ----------------------------
        try:
            from engine.strategy.tech_indicators import compute_tech_features
        except Exception:
            compute_tech_features = None

        kama_z = 0.0
        kama_slope = 0.0
        if compute_tech_features and sym:
            try:
                tf = compute_tech_features(sym, int(t)) or {}
                kama_z = _safe_f(tf.get("price_kama_z", 0.0), 0.0)
                kama_slope = _safe_f(tf.get("kama_slope", 0.0), 0.0)
            except Exception:
                kama_z = 0.0
                kama_slope = 0.0

        try:
            flow_z = _safe_f(_get_factor_feature_asof(con, "flows.spy_agg_ratio_z", int(t)), 0.0)
        except Exception:
            flow_z = 0.0

        try:
            opt_skew_z = _safe_f(_get_factor_feature_asof(con, "options.surface_skew_z", int(t)), 0.0)
        except Exception:
            opt_skew_z = 0.0

        try:
            term_slope_z = _safe_f(_get_factor_feature_asof(con, "options.term_structure_slope_z", int(t)), 0.0)
        except Exception:
            term_slope_z = 0.0

        try:
            vol_of_vol_z = _safe_f(_get_factor_feature_asof(con, "options.vol_of_vol_z", int(t)), 0.0)
        except Exception:
            vol_of_vol_z = 0.0

        mania = 0.0
        fear = 0.0
        churn = 0.0
        try:
            from engine.strategy.social_regime import get_social_regime_vector

            sv = get_social_regime_vector(symbol=sym, ts_ms=int(t)) or {}
            mania = _safe_f(sv.get("mania_score", 0.0), 0.0)
            fear = _safe_f(sv.get("fear_score", 0.0), 0.0)
            churn = _safe_f(sv.get("churn_score", 0.0), 0.0)
        except Exception:
            mania = 0.0
            fear = 0.0
            churn = 0.0

        px_symbol = sym or "SPY"
        px_vals = _read_price_series(con, px_symbol, int(t), max(20, _REGIME_VOL_LOOKBACK))
        if len(px_vals) < max(8, _REGIME_VOL_RECENT_N + 2) and px_symbol != "SPY":
            px_symbol = "SPY"
            px_vals = _read_price_series(con, px_symbol, int(t), max(20, _REGIME_VOL_LOOKBACK))

        rets = _pct_returns(px_vals)
        recent_n = int(max(4, _REGIME_VOL_RECENT_N))
        recent_rets = rets[-recent_n:]
        prior_rets = rets[:-recent_n] if len(rets) > recent_n else []

        recent_vol = _std(recent_rets)
        prior_vol = _std(prior_rets)
        vol_ratio = float(recent_vol / max(1e-9, prior_vol if prior_vol > 0.0 else 1e-9)) if recent_vol > 0.0 else 0.0
        vol_clustered = _clamp01((vol_ratio - 1.0) / 1.5) if prior_vol > 0.0 else _clamp01(recent_vol * 50.0)

        liq = _liquidity_snapshot(con, int(t))
        liquidity_thin = _clamp01(liq.get("thin_score", 0.0))

        momentum_dom = _clamp01(_z_to_weight(abs(kama_z) + abs(kama_slope) * 8.0, k=0.85, center=0.5))
        auction_heavy = _clamp01(_z_to_weight(abs(flow_z), k=0.85, center=0.75))
        news_shock = _clamp01(
            0.5 * _z_to_weight(mania, k=1.2, center=0.5) + 0.5 * _z_to_weight(fear, k=1.2, center=0.5)
        )
        options_surface_stress = _clamp01(
            (
                abs(opt_skew_z) * 0.40
                + abs(term_slope_z) * 0.30
                + abs(vol_of_vol_z) * 0.30
            ) / 3.0
        )

        micro = {
            "momentum_dominant": float(momentum_dom),
            "auction_heavy": float(auction_heavy),
            "news_shock": float(news_shock),
            "social_churn": float(_clamp01(churn)),
            "options_skew_stress": float(_clamp01(_z_to_weight(abs(opt_skew_z), k=0.85, center=0.75))),
            "term_structure_stress": float(_clamp01(_z_to_weight(abs(term_slope_z), k=0.85, center=0.75))),
            "vol_of_vol_stress": float(_clamp01(_z_to_weight(abs(vol_of_vol_z), k=0.85, center=0.75))),
            "options_surface_stress": float(options_surface_stress),
        }

        drift_snapshot = {}
        try:
            drift_snapshot = get_latest_distribution_drift_snapshot(symbol=sym, con=con) or {}
        except Exception:
            drift_snapshot = {}

        fshift = drift_snapshot.get("feature_shift") if isinstance(drift_snapshot, dict) else {}
        rshift = drift_snapshot.get("residual_shift") if isinstance(drift_snapshot, dict) else {}

        feature_shift_score = _clamp01(_safe_f((fshift or {}).get("max_drift_score", 0.0), 0.0))
        residual_shift_score = _clamp01(_safe_f((rshift or {}).get("drift_score", 0.0), 0.0))
        distribution_stable = _clamp01(
            _safe_f((drift_snapshot or {}).get("stable_score", 1.0), 1.0)
        )
        distribution_state = str((drift_snapshot or {}).get("state", "NORMAL") or "NORMAL").upper()

        drift = {
            "feature_shift": float(feature_shift_score),
            "residual_shift": float(residual_shift_score),
            "distribution_stable": float(distribution_stable),
            "state": str(distribution_state),
        }

        macro_conf = _clamp01((abs(vix_z) + abs(rv20_z) + abs(credit_z) + drawdown_shift * 2.0) / 8.0)
        asset_conf = 1.0 if sym else 0.60
        micro_conf = _clamp01(
            (
                abs(kama_z)
                + abs(kama_slope) * 8.0
                + abs(flow_z)
                + mania
                + fear
                + vol_clustered * 2.0
                + liquidity_thin * 2.0
            ) / 10.0
        )

        price_cov = _clamp01(len(rets) / float(max(8, _REGIME_VOL_LOOKBACK - 1)))
        liq_cov = _clamp01(liq.get("fill_count", 0.0) / 20.0)
        dd_cov = _clamp01(dd_shift.get("recent_n", 0.0) / 20.0)
        base_conf = _clamp01((macro_conf + asset_conf + micro_conf) / 3.0) * _clamp01(
            (price_cov + liq_cov + dd_cov) / 3.0 + 0.25
        )

        drift_penalty = 1.0
        if distribution_state == "CRITICAL":
            drift_penalty = max(0.20, 0.25 + 0.75 * float(distribution_stable))
        elif distribution_state == "WARN":
            drift_penalty = max(0.40, 0.50 + 0.50 * float(distribution_stable))
        elif distribution_state == "STALE":
            drift_penalty = 0.60
        else:
            drift_penalty = max(0.75, 0.85 + 0.15 * float(distribution_stable))

        overall_conf = _clamp01(base_conf * drift_penalty)

        vol_cluster_label = "UNKNOWN"
        if len(rets) >= max(8, recent_n):
            if vol_clustered >= 0.65:
                vol_cluster_label = "CLUSTERED"
            elif vol_clustered <= 0.30:
                vol_cluster_label = "CALM"
            else:
                vol_cluster_label = "NORMAL"

        return {
            "ts_ms": int(t),
            "version": regime_model_version(),
            "macro": macro,
            "asset": asset,
            "micro": micro,
            "drift": drift,
            "regimes": {
                "volatility": str(vol_cluster_label),
                "liquidity": str(liq.get("label", "UNKNOWN")),
                "drawdown": str(dd_shift.get("label", "UNKNOWN")),
                "distribution": str(distribution_state),
            },
            "confidence": {
                "overall": float(overall_conf),
                "base": float(base_conf),
                "drift_penalty": float(drift_penalty),
                "macro": float(macro_conf),
                "asset": float(asset_conf),
                "micro": float(micro_conf),
                "price_coverage": float(price_cov),
                "liquidity_coverage": float(liq_cov),
                "drawdown_coverage": float(dd_cov),
            },
            "diagnostics": {
                "price_symbol": str(px_symbol),
                "recent_vol": float(recent_vol),
                "prior_vol": float(prior_vol),
                "vol_ratio": float(vol_ratio),
                "liquidity_fill_count": float(liq.get("fill_count", 0.0)),
                "liquidity_passive_ratio": float(liq.get("passive_ratio", 0.5)),
                "drawdown_recent_avg": float(dd_shift.get("recent_avg", 0.0)),
                "drawdown_prior_avg": float(dd_shift.get("prior_avg", 0.0)),
                "drawdown_ratio": float(dd_shift.get("ratio", 0.0)),
                "distribution_state": str(distribution_state),
                "distribution_stable": float(distribution_stable),
                "feature_shift_score": float(feature_shift_score),
                "residual_shift_score": float(residual_shift_score),
            },
        }

    finally:
        if close_con:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal(
                    "regime_stack_vector_close_failed",
                    "REGIME_STACK_VECTOR_CLOSE_FAILED",
                    exc,
                    warn_key="regime_stack_vector_close_failed",
                )


def _flatten_regime_vector(v: Dict[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    if not isinstance(v, dict):
        return out
    for layer in ("macro", "asset", "micro", "drift"):
        lv = v.get(layer)
        if isinstance(lv, dict):
            for k, val in lv.items():
                out[f"{layer}.{k}"] = float(_safe_f(val, 0.0))
    return out


def _flatten_regime_profile(p: Dict[str, Any]) -> Dict[str, float]:
    if not isinstance(p, dict):
        return {}
    if any(k in p for k in ("macro", "asset", "micro")):
        return _flatten_regime_vector(p)
    out: Dict[str, float] = {}
    for k, val in p.items():
        try:
            out[str(k)] = float(_safe_f(val, 0.0))
        except Exception as e:
            _warn_nonfatal(
                "regime_stack_flatten_profile_value_failed",
                "REGIME_STACK_FLATTEN_PROFILE_VALUE_FAILED",
                e,
                warn_key=f"regime_stack_flatten_profile:{k}",
                key=str(k),
                raw_value=val,
            )
            continue
    return out


def regime_compatibility(profile: Dict[str, Any], vector: Dict[str, Any]) -> float:
    pv = _flatten_regime_profile(profile or {})
    vv = _flatten_regime_vector(vector or {})

    if not pv or not vv:
        return 1.0

    dot = 0.0
    p2 = 0.0
    v2 = 0.0

    for k, pval in pv.items():
        if k not in vv:
            continue
        vval = float(vv.get(k, 0.0))
        pval = float(pval)
        if pval < 0.0:
            pval = 0.0
        if vval < 0.0:
            vval = 0.0
        dot += pval * vval
        p2 += pval * pval
        v2 += vval * vval

    if p2 <= 1e-12 or v2 <= 1e-12:
        return 1.0

    c = float(dot / (math.sqrt(p2) * math.sqrt(v2)))
    if c != c:
        return 1.0
    return float(_clamp01(c))
