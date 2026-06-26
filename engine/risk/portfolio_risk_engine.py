"""Portfolio-level exposure, volatility, and drawdown risk controls.

The engine applies additive portfolio constraints to desired allocations,
persists risk snapshots for operator visibility, and raises fail-closed block
signals when projected exposure breaches configured limits.
"""

# engine/risk/portfolio_risk_engine.py
"""
Institutional Portfolio Risk Engine (additive, non-breaking).

Implements:
- Portfolio exposure accounting: gross/net, per-symbol, per-asset-class
- Rolling vol + correlation/cov proxy from prices (engine.strategy.risk)
- Correlated exposure clusters (graph components) capped by budget
- Vol-adjusted per-symbol sizing caps
- Asset-class risk budgets
- Portfolio-level gross/net caps
- Portfolio vol targeting (scale all weights) + hard-block threshold
- Max drawdown throttle + hard-block (engine.strategy.drawdown_state)

Integration:
- Called from engine.strategy.portfolio BEFORE portfolio_risk_gate
- Writes:
    risk_state:
      portfolio_risk_block (0/1)
      portfolio_risk_info  (json)
  and snapshots to portfolio_risk_snapshots (created in storage.init_db)
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.data.asset_map import asset_class_for_symbol
from engine.data.quiver_gov import sector_for_symbol
from engine.risk.futures_margin import (
    cap_contracts_by_margin,
    contract_notional,
    currency_conversion_rate,
    weight_to_contracts,
)
from engine.risk.covariance import correlation_matrix_dict, estimate_covariance
from engine.risk.equity_leverage_caps import equity_leverage_mode, max_equity_leverage
from engine.risk.notional_backstop import BACKSTOP_ENABLED
from engine.strategy.drawdown_state import evaluate_current_drawdown
from engine.strategy.risk import realized_vol_from_prices, corr_from_prices
from engine.strategy.har_rv import resolve_vol_forecast
from engine.strategy.equity_sizing import clamp_equity_gross_to_leverage, equity_deployable_base
from engine.strategy.fx_sizing import _fx_instrument, clamp_fx_weight_to_leverage, fx_weight_to_notional
from engine.strategy.crypto_sizing import (
    _crypto_instrument,
    attach_crypto_sizing_context,
    clamp_crypto_weight_to_leverage,
    crypto_weight_to_notional,
    normalize_crypto_symbol,
)
from engine.runtime.live_execution_control import live_execution_disabled
from engine.runtime.risk_state import set_state, get_state_row
from engine.runtime.event_log import record_risk_block
from engine.runtime.storage import _table_exists

LOG = logging.getLogger("engine.risk.portfolio_risk_engine")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_positive_float(name: str, default: str = "0.0") -> float:
    try:
        value = float(os.environ.get(name, default) or default)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_POSITIVE_FLOAT_ENV_PARSE_FAILED",
            e,
            once_key=f"env_positive_float:{name}",
            env_name=str(name),
        )
        return 0.0
    if value != value or value <= 0.0:
        return 0.0
    return float(value)


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.risk.portfolio_risk_engine",
        extra=extra or {},
        include_health=False,
        persist=False,
    )


# -----------------------------
# Enable / controls (env)
# -----------------------------
USE = os.environ.get("PORTFOLIO_USE_RISK_ENGINE", "1") == "1"

# Universe bound for cov/corr computations (top by abs weight)
MAX_SYMBOLS = int(os.environ.get("PORTFOLIO_RISK_MAX_SYMBOLS", "18"))

# Portfolio caps
MAX_GROSS = float(os.environ.get("PORTFOLIO_RISK_MAX_GROSS", os.environ.get("PORTFOLIO_GROSS_CAP", "1.00")))
MAX_NET = float(os.environ.get("PORTFOLIO_RISK_MAX_NET", "0.60"))

# Drawdown throttle/hard block
DD_THROTTLE_START = float(os.environ.get("PORTFOLIO_RISK_DD_THROTTLE_START", "0.06"))
DD_THROTTLE_MIN_SCALE = float(os.environ.get("PORTFOLIO_RISK_DD_THROTTLE_MIN_SCALE", "0.35"))
DD_HARD_BLOCK = float(os.environ.get("PORTFOLIO_RISK_DD_HARD_BLOCK", "0.15"))

# Portfolio vol proxy + targeting
VOL_LOOKBACK = int(os.environ.get("PORTFOLIO_RISK_VOL_LOOKBACK", os.environ.get("PORTFOLIO_VOL_LOOKBACK", "240")))
VOL_TARGET = float(os.environ.get("PORTFOLIO_RISK_VOL_TARGET", os.environ.get("PORTFOLIO_TARGET_VOL", "0.020")))
VOL_FORECAST_SOURCE = str(os.environ.get("VOL_FORECAST_SOURCE", "trailing") or "trailing").strip().lower()
PORTFOLIO_VOL_HARD_BLOCK = float(os.environ.get("PORTFOLIO_RISK_VOL_HARD_BLOCK", "0.0"))  # 0.0 = intentional soft-only default; vol targeting still scales; set >0 to make portfolio vol a hard stop
PORTFOLIO_VOL_FLOOR = float(os.environ.get("PORTFOLIO_RISK_VOL_FLOOR", "0.005"))
PORTFOLIO_VOL_CEIL = float(os.environ.get("PORTFOLIO_RISK_VOL_CEIL", "0.080"))
USE_GEX_VOL_MODIFIER = os.environ.get("PORTFOLIO_RISK_USE_GEX_VOL_MODIFIER", os.environ.get("USE_OPTIONS_FEATURES", "0")) == "1"
PORTFOLIO_RISK_USE_MONTE_CARLO = _env_bool("PORTFOLIO_RISK_USE_MONTE_CARLO", True)
PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE = _env_bool("PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE", True)
PORTFOLIO_RISK_MC_MAX_AGE_S = int(os.environ.get("PORTFOLIO_RISK_MC_MAX_AGE_S", "900"))
PORTFOLIO_RISK_MC_VAR_95_BLOCK = float(os.environ.get("PORTFOLIO_RISK_MC_VAR_95_BLOCK", "0.0"))
PORTFOLIO_RISK_MC_VAR_99_BLOCK = float(os.environ.get("PORTFOLIO_RISK_MC_VAR_99_BLOCK", "0.0"))
PORTFOLIO_RISK_MC_CVAR_95_BLOCK = float(os.environ.get("PORTFOLIO_RISK_MC_CVAR_95_BLOCK", "0.0"))
PORTFOLIO_RISK_MC_CVAR_99_BLOCK = float(os.environ.get("PORTFOLIO_RISK_MC_CVAR_99_BLOCK", "0.0"))
PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK = float(os.environ.get("PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK", "0.0"))
PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK = float(os.environ.get("PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK", "0.0"))

# Per-symbol vol caps (vol-adjusted sizing caps)
USE_VOL_CAPS = os.environ.get("PORTFOLIO_RISK_USE_VOL_CAPS", "1") == "1"
SYMBOL_CAP_MAX_W = float(os.environ.get("PORTFOLIO_RISK_SYMBOL_CAP_MAX_W", "0.35"))
SYMBOL_CAP_MIN_MULT = float(os.environ.get("PORTFOLIO_RISK_SYMBOL_CAP_MIN_MULT", "0.20"))
MAX_SYMBOL_GROSS = float(os.environ.get("PORTFOLIO_RISK_MAX_SYMBOL_GROSS", os.environ.get("PORTFOLIO_RISK_SYMBOL_CAP_MAX_W", "0.35")))

# Correlated exposure cluster caps (graph components)
USE_CORR_CLUSTERS = os.environ.get("PORTFOLIO_RISK_USE_CORR_CLUSTERS", "1") == "1"
CORR_LOOKBACK = int(os.environ.get("PORTFOLIO_RISK_CORR_LOOKBACK", "240"))
CLUSTER_CORR_TH = float(os.environ.get("PORTFOLIO_RISK_CLUSTER_CORR_TH", "0.85"))
CLUSTER_MAX_GROSS = float(os.environ.get("PORTFOLIO_RISK_CLUSTER_MAX_GROSS", "0.45"))
CLUSTER_MAX_COMPONENTS = int(os.environ.get("PORTFOLIO_RISK_CLUSTER_MAX_COMPONENTS", "12"))
USE_FX_CURRENCY_CLUSTERS = os.environ.get("PORTFOLIO_RISK_FX_CURRENCY_CLUSTERS", "1") == "1"
USE_FX_LEVERAGE_CAPS = os.environ.get("PORTFOLIO_RISK_USE_FX_LEVERAGE_CAPS", "1") == "1"
USE_EQUITY_LEVERAGE_CAPS = os.environ.get("PORTFOLIO_RISK_USE_EQUITY_LEVERAGE_CAPS", "1") == "1"
USE_CRYPTO_LEVERAGE_CAPS = os.environ.get("PORTFOLIO_RISK_USE_CRYPTO_LEVERAGE_CAPS", "1") == "1"
USE_FUTURES_MARGIN_CAPS = os.environ.get("PORTFOLIO_RISK_USE_FUTURES_MARGIN_CAPS", "1") == "1"
USE_OPTIONS_GREEK_LIMITS = os.environ.get("PORTFOLIO_RISK_USE_OPTIONS_GREEK_LIMITS", "1") == "1"
OPTIONS_MAX_POSITION_CONTRACTS = _env_positive_float("OPTIONS_MAX_POSITION_CONTRACTS")
OPTIONS_MARGIN_IMPACT_MAX_FRACTION = _env_positive_float("OPTIONS_MARGIN_IMPACT_MAX_FRACTION")
OPTIONS_MAX_PORTFOLIO_DELTA_ABS = _env_positive_float("OPTIONS_MAX_PORTFOLIO_DELTA_ABS")
OPTIONS_MAX_PORTFOLIO_GAMMA_ABS = _env_positive_float("OPTIONS_MAX_PORTFOLIO_GAMMA_ABS")
OPTIONS_MAX_PORTFOLIO_VEGA_ABS = _env_positive_float("OPTIONS_MAX_PORTFOLIO_VEGA_ABS")

# Asset-class budgets
USE_ASSET_CLASS_BUDGETS = os.environ.get("PORTFOLIO_RISK_USE_ASSET_CLASS_BUDGETS", "1") == "1"
PORTFOLIO_RISK_BIND_EQUITY_BUDGET = _env_bool("PORTFOLIO_RISK_BIND_EQUITY_BUDGET", True)
_EQUITY_ASSET_CLASS_BUDGET = 0.80 if PORTFOLIO_RISK_BIND_EQUITY_BUDGET else 1.00
_ASSET_CLASS_BUDGETS_JSON = os.environ.get("PORTFOLIO_RISK_ASSET_CLASS_BUDGETS_JSON", "").strip()
USE_SECTOR_BUDGETS = os.environ.get("PORTFOLIO_RISK_USE_SECTOR_BUDGETS", "1") == "1"
SECTOR_MAX_GROSS = float(os.environ.get("PORTFOLIO_RISK_SECTOR_MAX_GROSS", "0.30"))
_SECTOR_BUDGETS_JSON = os.environ.get("PORTFOLIO_RISK_SECTOR_BUDGETS_JSON", "").strip()

# Strategy-level budgets
USE_STRATEGY_BUDGETS = os.environ.get("PORTFOLIO_RISK_USE_STRATEGY_BUDGETS", "1") == "1"
STRATEGY_MAX_GROSS = float(os.environ.get("PORTFOLIO_RISK_MAX_STRATEGY_GROSS", "0.60"))
STRATEGY_MAX_NET = float(os.environ.get("PORTFOLIO_RISK_MAX_STRATEGY_NET", "0.40"))
USE_ALPHA_DECAY_THROTTLE = os.environ.get("PORTFOLIO_RISK_USE_ALPHA_DECAY_THROTTLE", "1") == "1"
ALPHA_DECAY_THROTTLE_FRESH_S = int(os.environ.get("PORTFOLIO_RISK_ALPHA_DECAY_FRESH_S", "21600"))

_DEFAULT_ASSET_CLASS_BUDGETS = {
    "EQUITY": _EQUITY_ASSET_CLASS_BUDGET,
    "CRYPTO": 0.35,
    "COMMODITY": 0.50,
    # Conservative default enforced by the existing asset-class budget path.
    "OPTION": 0.20,
    "FX": 0.50,
    "FUTURES": 0.40,
    "RATES": 0.60,
    "UNKNOWN": 0.40,
}

ASSET_CLASS_BUDGETS: Dict[str, float] = dict(_DEFAULT_ASSET_CLASS_BUDGETS)
if _ASSET_CLASS_BUDGETS_JSON:
    try:
        d = json.loads(_ASSET_CLASS_BUDGETS_JSON)
        if isinstance(d, dict):
            for k, v in d.items():
                ASSET_CLASS_BUDGETS[str(k).upper()] = float(v)
    except Exception:
        ASSET_CLASS_BUDGETS = dict(_DEFAULT_ASSET_CLASS_BUDGETS)

SECTOR_BUDGETS: Dict[str, float] = {}
if _SECTOR_BUDGETS_JSON:
    try:
        d = json.loads(_SECTOR_BUDGETS_JSON)
        if isinstance(d, dict):
            for k, v in d.items():
                key = str(k or "").upper().strip()
                if key:
                    SECTOR_BUDGETS[key] = float(v)
    except Exception:
        SECTOR_BUDGETS = {}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v:
            return float(default)
        return float(v)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_SAFE_FLOAT_FAILED",
            e,
            once_key="safe_float_failed",
            value_type=type(x).__name__,
        )
        fallback = float(default)
        return fallback


def _optional_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        if v != v:
            return None
        return float(v)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_OPTIONAL_FLOAT_FAILED",
            e,
            once_key=f"optional_float:{type(x).__name__}:{str(x)[:64]}",
            value_type=type(x).__name__,
        )
        return None


def _asset_class_for(con, symbol: str) -> str:
    try:
        futures = _futures_instrument(con, symbol)
        if isinstance(futures, dict):
            asset_class = str(futures.get("asset_class") or "").upper().strip()
            if asset_class:
                return asset_class
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_FUTURES_ASSET_CLASS_LOOKUP_FAILED",
            e,
            once_key=f"futures_asset_class:{symbol}",
            symbol=str(symbol),
        )
    try:
        instrument = _fx_instrument(con, symbol)
        if isinstance(instrument, dict):
            asset_class = str(instrument.get("asset_class") or "").upper().strip()
            if asset_class:
                return asset_class
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_FX_ASSET_CLASS_LOOKUP_FAILED",
            e,
            once_key=f"fx_asset_class:{symbol}",
            symbol=str(symbol),
        )
    try:
        crypto = _crypto_instrument(con, symbol)
        if isinstance(crypto, dict):
            asset_class = str(crypto.get("asset_class") or "").upper().strip()
            if asset_class:
                return asset_class
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_CRYPTO_ASSET_CLASS_LOOKUP_FAILED",
            e,
            once_key=f"crypto_asset_class:{symbol}",
            symbol=str(symbol),
        )
    try:
        fallback_asset_class = str(asset_class_for_symbol(str(symbol)) or "UNKNOWN").upper()
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ASSET_CLASS_FALLBACK_FAILED",
            e,
            once_key=f"asset_class_fallback:{symbol}",
            symbol=str(symbol),
        )
        fallback_asset_class = "UNKNOWN"
    return fallback_asset_class


def _sector_for(con, symbol: str) -> str:
    if con is None or not str(symbol or "").strip():
        return ""
    try:
        return str(sector_for_symbol(con, str(symbol)) or "").upper().strip()
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_SECTOR_LOOKUP_FAILED",
            e,
            once_key=f"sector_lookup:{symbol}",
            symbol=str(symbol),
        )
        return ""


def _futures_instrument(con, symbol: str) -> Optional[Dict[str, Any]]:
    try:
        from engine.data.universe import get_instrument_metadata

        if con is not None:
            raw = get_instrument_metadata(con, str(symbol))
            if isinstance(raw, dict) and str(raw.get("asset_class") or "").upper().strip() == "FUTURES":
                return dict(raw)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_FUTURES_METADATA_LOOKUP_FAILED",
            e,
            once_key=f"futures_metadata:{symbol}",
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
            "PORTFOLIO_RISK_FUTURES_PARSE_FAILED",
            e,
            once_key=f"futures_parse:{symbol}",
            symbol=str(symbol),
        )
        return None


def _futures_multiplier_factor(con, symbol: str) -> float:
    meta = _futures_instrument(con, str(symbol))
    if not isinstance(meta, dict):
        return 1.0
    multiplier = _safe_float(meta.get("multiplier", meta.get("fut_multiplier")), 1.0)
    if multiplier <= 0.0:
        return 1.0
    return float(multiplier)


def _is_live_risk_runtime() -> bool:
    engine_mode = str(os.environ.get("ENGINE_MODE", "") or "").strip().lower()
    execution_mode = str(os.environ.get("EXECUTION_MODE", "") or "").strip().lower()
    env = str(os.environ.get("ENV", "") or "").strip().lower()
    prod_lock = _env_bool("PROD_LOCK", env == "prod")
    return bool(engine_mode == "live" or execution_mode == "live" or (env == "prod" and prod_lock))


def _monte_carlo_required_in_current_runtime() -> bool:
    return bool(PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE and _is_live_risk_runtime())


def _risk_overlay_enabled(name: str) -> bool:
    overlay = str(name or "").strip()
    if overlay == "drawdown_throttle":
        return bool(float(DD_THROTTLE_START) > 0.0 and float(DD_THROTTLE_MIN_SCALE) < 1.0)
    if overlay == "alpha_decay_throttle":
        return bool(USE_ALPHA_DECAY_THROTTLE)
    if overlay == "symbol_vol_caps":
        return bool(USE_VOL_CAPS)
    if overlay == "corr_cluster_caps":
        return bool(USE_CORR_CLUSTERS)
    if overlay == "portfolio_vol_target":
        return bool(float(VOL_TARGET) > 0.0 or float(PORTFOLIO_VOL_HARD_BLOCK) > 0.0)
    return False


def _required_overlay_failures_block_current_runtime() -> bool:
    return bool(_is_live_risk_runtime())


def _record_overlay_failure(
    info: Dict[str, Any],
    overlay: str,
    error: Exception,
    *,
    now_ms: int,
) -> Optional[Dict[str, Any]]:
    name = str(overlay or "").strip()
    enabled = _risk_overlay_enabled(name)
    required = _required_overlay_failures_block_current_runtime()
    failure = {
        "name": name,
        "enabled": bool(enabled),
        "required": bool(required),
        "ts_ms": int(now_ms),
        "error_type": type(error).__name__,
        "error": str(error),
    }
    info.setdefault("overlay_failures", []).append(dict(failure))
    if not (enabled and required):
        return None

    required_failures = info.setdefault("required_overlay_failures", [])
    required_failures.append(dict(failure))
    info["overlay_failed"] = name
    info["overlay_failure_required"] = True
    return {
        "type": "required_overlay_failed",
        "overlay": name,
        "failures": list(required_failures),
    }


def _monte_carlo_failure_summary(
    reason_code: str,
    *,
    now_ms: int,
    ts_ms: int = 0,
    status: str = "",
    ready: bool = False,
    stale: bool = False,
    error_type: str | None = None,
    error: str | None = None,
) -> Dict[str, Any]:
    required = _monte_carlo_required_in_current_runtime()
    age_ms = int(max(0, int(now_ms) - int(ts_ms or 0))) if ts_ms else 0
    reason: Dict[str, Any] = {"type": str(reason_code)}
    if error_type:
        reason["error_type"] = str(error_type)
    if error:
        reason["error"] = str(error)

    out: Dict[str, Any] = {
        "enabled": bool(PORTFOLIO_RISK_USE_MONTE_CARLO),
        "required": bool(required),
        "ready": bool(ready),
        "status": str(status or reason_code),
        "ts_ms": int(ts_ms or 0),
        "age_s": float(age_ms) / 1000.0,
        "blocked": bool(required),
        "reasons": [reason],
    }
    if stale:
        out["stale"] = True
    return out


def _load_monte_carlo_risk_summary(now_ms: int) -> Dict[str, Any]:
    if not PORTFOLIO_RISK_USE_MONTE_CARLO:
        if _monte_carlo_required_in_current_runtime():
            return _monte_carlo_failure_summary(
                "monte_carlo_risk_disabled_while_required",
                now_ms=int(now_ms),
                status="disabled",
            )
        return {
            "enabled": False,
            "required": False,
            "ready": False,
            "status": "disabled",
            "ts_ms": 0,
            "age_s": 0.0,
            "blocked": False,
            "reasons": [{"type": "monte_carlo_risk_disabled"}],
        }

    try:
        raw, ts_ms = get_state_row("monte_carlo_risk_info", "")
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_MONTE_CARLO_STATE_READ_FAILED",
            e,
            once_key="monte_carlo_state_read",
        )
        return _monte_carlo_failure_summary(
            "monte_carlo_risk_state_read_error",
            now_ms=int(now_ms),
            error_type=type(e).__name__,
            error=str(e),
        )

    if not raw:
        return _monte_carlo_failure_summary(
            "monte_carlo_risk_state_missing",
            now_ms=int(now_ms),
            ts_ms=int(ts_ms or 0),
            status="missing",
        )

    try:
        info_raw = json.loads(raw or "{}")
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_MONTE_CARLO_JSON_FAILED",
            e,
            once_key="monte_carlo_json_parse",
        )
        return _monte_carlo_failure_summary(
            "monte_carlo_risk_state_parse_error",
            now_ms=int(now_ms),
            ts_ms=int(ts_ms or 0),
            status="parse_error",
            error_type=type(e).__name__,
            error=str(e),
        )

    if not isinstance(info_raw, dict):
        return _monte_carlo_failure_summary(
            "monte_carlo_risk_state_invalid",
            now_ms=int(now_ms),
            ts_ms=int(ts_ms or 0),
            status="invalid",
            error_type=type(info_raw).__name__,
        )

    info: Dict[str, Any] = dict(info_raw)
    age_ms = int(max(0, int(now_ms) - int(ts_ms or 0)))
    drawdown_percentiles_raw = info.get("drawdown_percentiles")
    drawdown_percentiles: Dict[str, Any] = dict(drawdown_percentiles_raw) if isinstance(drawdown_percentiles_raw, dict) else {}
    out: Dict[str, Any] = {
        "enabled": bool(info.get("enabled", True)),
        "required": bool(_monte_carlo_required_in_current_runtime()),
        "ready": bool(info.get("ready", False)),
        "status": str(info.get("status") or ""),
        "ts_ms": int(ts_ms or 0),
        "age_s": float(age_ms) / 1000.0,
        "var_95": float(_safe_float(info.get("var_95"), 0.0)),
        "var_99": float(_safe_float(info.get("var_99"), 0.0)),
        "cvar_95": float(_safe_float(info.get("cvar_95"), 0.0)),
        "cvar_99": float(_safe_float(info.get("cvar_99"), 0.0)),
        "worst_simulated_drawdown": float(_safe_float(info.get("worst_simulated_drawdown"), 0.0)),
        "drawdown_p95": float(_safe_float(drawdown_percentiles.get("p95"), 0.0)),
        "drawdown_p99": float(_safe_float(drawdown_percentiles.get("p99"), 0.0)),
    }

    status = str(out.get("status") or "").strip().lower()
    if not bool(out["enabled"]):
        out["blocked"] = bool(out["required"])
        out["reasons"] = [{"type": "monte_carlo_risk_disabled_while_required", "status": str(out.get("status") or "")}]
        return out

    if not bool(out["ready"]) or status in {"disabled", "error", "failed", "unavailable"}:
        reason_code = "monte_carlo_risk_simulation_error" if status == "error" else "monte_carlo_risk_not_ready"
        out["blocked"] = bool(out["required"])
        out["reasons"] = [{"type": reason_code, "status": str(out.get("status") or ""), "ready": bool(out["ready"])}]
        if info.get("error"):
            out["reasons"][0]["error"] = str(info.get("error"))
        return out

    # Monte Carlo results are advisory only while fresh. Stale simulations are
    # surfaced as stale rather than silently trusted for hard blocking decisions.
    if int(PORTFOLIO_RISK_MC_MAX_AGE_S) > 0 and float(out["age_s"]) > float(PORTFOLIO_RISK_MC_MAX_AGE_S):
        out["stale"] = True
        out["blocked"] = bool(out["required"])
        out["reasons"] = [
            {
                "type": "monte_carlo_risk_state_stale",
                "age_s": float(out["age_s"]),
                "max_age_s": int(PORTFOLIO_RISK_MC_MAX_AGE_S),
            }
        ]
        return out

    var_95_loss = max(0.0, -float(out["var_95"]))
    var_99_loss = max(0.0, -float(out["var_99"]))
    cvar_95_loss = max(0.0, -float(out["cvar_95"]))
    cvar_99_loss = max(0.0, -float(out["cvar_99"]))
    dd_p95 = max(0.0, float(out["drawdown_p95"]))
    dd_worst = max(0.0, float(out["worst_simulated_drawdown"]))

    blocked = False
    reasons: List[Dict[str, Any]] = []

    if float(PORTFOLIO_RISK_MC_VAR_95_BLOCK) > 0.0 and var_95_loss >= float(PORTFOLIO_RISK_MC_VAR_95_BLOCK):
        blocked = True
        reasons.append({"type": "monte_carlo_var_95_block", "value": float(var_95_loss), "threshold": float(PORTFOLIO_RISK_MC_VAR_95_BLOCK)})

    if float(PORTFOLIO_RISK_MC_VAR_99_BLOCK) > 0.0 and var_99_loss >= float(PORTFOLIO_RISK_MC_VAR_99_BLOCK):
        blocked = True
        reasons.append({"type": "monte_carlo_var_99_block", "value": float(var_99_loss), "threshold": float(PORTFOLIO_RISK_MC_VAR_99_BLOCK)})

    if float(PORTFOLIO_RISK_MC_CVAR_95_BLOCK) > 0.0 and cvar_95_loss >= float(PORTFOLIO_RISK_MC_CVAR_95_BLOCK):
        blocked = True
        reasons.append({"type": "monte_carlo_cvar_95_block", "value": float(cvar_95_loss), "threshold": float(PORTFOLIO_RISK_MC_CVAR_95_BLOCK)})

    if float(PORTFOLIO_RISK_MC_CVAR_99_BLOCK) > 0.0 and cvar_99_loss >= float(PORTFOLIO_RISK_MC_CVAR_99_BLOCK):
        blocked = True
        reasons.append({"type": "monte_carlo_cvar_99_block", "value": float(cvar_99_loss), "threshold": float(PORTFOLIO_RISK_MC_CVAR_99_BLOCK)})

    if float(PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK) > 0.0 and dd_p95 >= float(PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK):
        blocked = True
        reasons.append({"type": "monte_carlo_drawdown_p95_block", "value": float(dd_p95), "threshold": float(PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK)})

    if float(PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK) > 0.0 and dd_worst >= float(PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK):
        blocked = True
        reasons.append({"type": "monte_carlo_worst_drawdown_block", "value": float(dd_worst), "threshold": float(PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK)})

    out["blocked"] = bool(blocked)
    if reasons:
        out["reasons"] = reasons
    return out

def _load_latest_alpha_decay_strategy_metrics(con, now_ms: int) -> Dict[str, Dict[str, Any]]:
    if not USE_ALPHA_DECAY_THROTTLE:
        return {}
    if not _table_exists(con, "strategy_metrics"):
        return {}

    cutoff_ts_ms = int(now_ms) - (int(max(0, ALPHA_DECAY_THROTTLE_FRESH_S)) * 1000)

    try:
        rows = con.execute(
            """
            SELECT m.strategy_name, m.ts_ms, m.metrics_json
            FROM strategy_metrics m
            JOIN (
              SELECT strategy_name, MAX(ts_ms) AS ts_ms
              FROM strategy_metrics
              WHERE window_days=0
              GROUP BY strategy_name
            ) t
            ON t.strategy_name=m.strategy_name AND t.ts_ms=m.ts_ms
            WHERE m.window_days=0 AND m.ts_ms>=?
            """,
            (int(cutoff_ts_ms),),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_ALPHA_DECAY_METRICS_QUERY_FAILED",
            e,
            once_key="alpha_decay_metrics_query",
        )
        alpha_metrics: Dict[str, Dict[str, Any]] = {}
        return alpha_metrics

    out: Dict[str, Dict[str, Any]] = {}
    for strategy_name, ts_ms, metrics_json in rows or []:
        try:
            name = str(strategy_name or "").strip()
            if not name:
                continue
            mj = json.loads(metrics_json or "{}")
            if not isinstance(mj, dict):
                mj = {}
            out[name] = {
                "ts_ms": int(ts_ms or 0),
                "alpha_decay_throttle_mult": float(max(0.0, min(1.0, _safe_float(mj.get("alpha_decay_throttle_mult"), 1.0)))),
                "alpha_decay_severity": str(mj.get("alpha_decay_severity") or "ok").strip().lower(),
                "alpha_decay_severity_score": float(_safe_float(mj.get("alpha_decay_severity_score"), 0.0)),
                "alpha_decay_reasons": list(mj.get("alpha_decay_reasons") or []),
            }
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_RISK_ENGINE_ALPHA_DECAY_ROW_FAILED",
                e,
                strategy_name=str(strategy_name or ""),
            )
            continue
    return out


def _apply_alpha_decay_throttle(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any], now_ms: int) -> Dict[str, Dict[str, Any]]:
    out = dict(desired or {})
    latest = _load_latest_alpha_decay_strategy_metrics(con, now_ms=int(now_ms))
    if not latest:
        return out

    # This stage rescales desired weights; it does not rewrite strategy intent or
    # selection. It is a portfolio-level safety overlay, not a signal generator.
    hit: Dict[str, Any] = {}

    for strategy_name, rec in latest.items():
        mult = float(max(0.0, min(1.0, _safe_float((rec or {}).get("alpha_decay_throttle_mult"), 1.0))))
        if mult >= 0.999999:
            continue

        touched = False
        for sym, row in (out or {}).items():
            if _strategy_bucket_for_row(row) != str(strategy_name):
                continue
            sw = _signed_weight(row)
            out[sym]["weight"] = float(sw) * float(mult)
            out[sym].setdefault("reason", {})
            if isinstance(out[sym]["reason"], dict):
                out[sym]["reason"]["alpha_decay_throttle"] = {
                    "strategy": str(strategy_name),
                    "scale": float(mult),
                    "severity": str((rec or {}).get("alpha_decay_severity") or "ok"),
                    "severity_score": float(_safe_float((rec or {}).get("alpha_decay_severity_score"), 0.0)),
                    "reasons": list((rec or {}).get("alpha_decay_reasons") or []),
                    "metrics_ts_ms": int((rec or {}).get("ts_ms") or 0),
                }
            touched = True

        if touched:
            hit[str(strategy_name)] = {
                "scale": float(mult),
                "severity": str((rec or {}).get("alpha_decay_severity") or "ok"),
                "severity_score": float(_safe_float((rec or {}).get("alpha_decay_severity_score"), 0.0)),
                "metrics_ts_ms": int((rec or {}).get("ts_ms") or 0),
                "reasons": list((rec or {}).get("alpha_decay_reasons") or []),
            }

    if hit:
        info["alpha_decay_throttle"] = {
            "fresh_s": int(ALPHA_DECAY_THROTTLE_FRESH_S),
            "strategies": hit,
        }

    return out


def _side_sign(side: Any) -> float:
    s = str(side or "FLAT").upper()
    if s == "LONG":
        return 1.0
    if s == "SHORT":
        return -1.0
    return 0.0


def _signed_weight(row: Optional[Dict[str, Any]]) -> float:
    if not row:
        return 0.0
    w = _safe_float((row or {}).get("weight", 0.0), 0.0)
    sgn = _side_sign((row or {}).get("side", "FLAT"))
    # tolerate both conventions:
    # - weight is magnitude with side providing sign
    # - weight is already signed
    if w < 0.0:
        return float(w)
    return float(abs(w)) * float(sgn)


def _abs_weight(row: Optional[Dict[str, Any]]) -> float:
    return abs(_signed_weight(row))


def _signed_exposure_weight(con, symbol: str, row: Optional[Dict[str, Any]]) -> float:
    return float(_signed_weight(row) * _futures_multiplier_factor(con, str(symbol)))


def _abs_exposure_weight(con, symbol: str, row: Optional[Dict[str, Any]]) -> float:
    return abs(_signed_exposure_weight(con, str(symbol), row))


def _gross(rows: Dict[str, Dict[str, Any]], con=None) -> float:
    return float(sum(_abs_exposure_weight(con, str(sym), row) for sym, row in (rows or {}).items()))


def _net(rows: Dict[str, Dict[str, Any]], con=None) -> float:
    return float(sum(_signed_exposure_weight(con, str(sym), row) for sym, row in (rows or {}).items()))


def _annotate(desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> None:
    for sym in list((desired or {}).keys()):
        try:
            desired[sym].setdefault("reason", {})
            if not isinstance(desired[sym]["reason"], dict):
                desired[sym]["reason"] = {"raw": desired[sym]["reason"]}
            desired[sym]["reason"]["portfolio_risk_engine"] = dict(info)
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_ANNOTATE_FAILED", e, once_key=f"annotate:{sym}", symbol=str(sym))


def _top_symbols_by_abs(desired: Dict[str, Dict[str, Any]], n: int) -> List[str]:
    items: List[Tuple[str, float]] = []
    for sym, row in (desired or {}).items():
        try:
            aw = _abs_weight(row)
            if aw > 0.0:
                items.append((str(sym), float(aw)))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_TOP_SYMBOLS_FAILED", e, once_key=f"top_symbols:{sym}", symbol=str(sym))
    items.sort(key=lambda t: t[1], reverse=True)
    if n > 0:
        items = items[: int(n)]
    return [s for s, _w in items]


def _maybe_parse_json(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        parsed = json.loads(str(raw))
        return dict(parsed) if isinstance(parsed, dict) else {}
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_SAFE_JSON_FAILED",
            e,
            once_key="safe_json_dict",
            raw_type=type(raw).__name__,
        )
        parsed_default: Dict[str, Any] = {}
        return parsed_default


def _strategy_bucket_for_row(row: Optional[Dict[str, Any]]) -> str:
    r = row or {}
    for key in ("strategy_id", "strategy_name", "strategy"):
        val = r.get(key)
        if val not in (None, "", "null"):
            return str(val)

    reason_obj = r.get("reason")
    if isinstance(reason_obj, dict):
        for key in ("strategy_id", "strategy_name", "strategy", "strategy_key"):
            val = reason_obj.get(key)
            if val not in (None, "", "null"):
                return str(val)

    src_rule_id = r.get("source_rule_id")
    if src_rule_id not in (None, "", "null"):
        return f"rule:{src_rule_id}"

    src_alert_id = r.get("source_alert_id")
    if src_alert_id not in (None, "", "null"):
        return f"alert:{src_alert_id}"

    meta = _maybe_parse_json(r.get("explain_json"))
    for key in ("strategy_id", "strategy_name", "strategy", "model_name", "model", "strategy_key"):
        val = meta.get(key)
        if val not in (None, "", "null"):
            return str(val)

    reason = meta.get("reason")
    if isinstance(reason, dict):
        for key in ("strategy_id", "strategy_name", "strategy", "model_name", "model", "strategy_key"):
            val = reason.get(key)
            if val not in (None, "", "null"):
                return str(val)

    for key in ("model_name", "model"):
        val = r.get(key)
        if val not in (None, "", "null"):
            return str(val)

    return "UNKNOWN"


def _model_bucket_for_row(row: Optional[Dict[str, Any]]) -> str:
    r = row or {}
    for key in ("model_id", "model_name", "model"):
        val = r.get(key)
        if val not in (None, "", "null"):
            return str(val)

    meta = _maybe_parse_json(r.get("explain_json"))
    for key in ("model_id", "model_name", "model"):
        val = meta.get(key)
        if val not in (None, "", "null"):
            return str(val)

    reason = meta.get("reason")
    if isinstance(reason, dict):
        competition = reason.get("competition")
        if isinstance(competition, dict):
            val = competition.get("model_name")
            if val not in (None, "", "null"):
                return str(val)

    return "UNKNOWN"


def _exposure_snapshot(rows: Dict[str, Dict[str, Any]], con=None) -> Dict[str, Any]:
    by_symbol: Dict[str, Dict[str, Any]] = {}
    by_asset_class: Dict[str, Dict[str, float]] = {}
    by_strategy: Dict[str, Dict[str, float]] = {}
    by_model: Dict[str, Dict[str, float]] = {}
    by_sector: Dict[str, Dict[str, float]] = {}

    long_gross = 0.0
    short_gross = 0.0

    for sym, row in (rows or {}).items():
        s = str(sym)
        raw_sw = _signed_weight(row)
        factor = _futures_multiplier_factor(con, s)
        sw = float(raw_sw) * float(factor)
        aw = abs(sw)
        if aw <= 0.0:
            continue

        side = "LONG" if sw > 0.0 else ("SHORT" if sw < 0.0 else "FLAT")
        by_symbol[s] = {
            "signed": float(sw),
            "gross": float(aw),
            "side": side,
        }
        if abs(float(factor) - 1.0) > 1e-12:
            by_symbol[s]["raw_signed_weight"] = float(raw_sw)
            by_symbol[s]["exposure_multiplier"] = float(factor)

        if sw > 0.0:
            long_gross += float(aw)
        elif sw < 0.0:
            short_gross += float(aw)

        asset_class = _asset_class_for(con, s)

        ac = by_asset_class.setdefault(asset_class, {"gross": 0.0, "net": 0.0})
        ac["gross"] = float(ac.get("gross", 0.0) + aw)
        ac["net"] = float(ac.get("net", 0.0) + sw)

        sector = _sector_for(con, s)
        if sector:
            sec = by_sector.setdefault(sector, {"gross": 0.0, "net": 0.0})
            sec["gross"] = float(sec.get("gross", 0.0) + aw)
            sec["net"] = float(sec.get("net", 0.0) + sw)

        strategy = _strategy_bucket_for_row(row)
        st = by_strategy.setdefault(strategy, {"gross": 0.0, "net": 0.0})
        st["gross"] = float(st.get("gross", 0.0) + aw)
        st["net"] = float(st.get("net", 0.0) + sw)

        model_bucket = _model_bucket_for_row(row)
        md = by_model.setdefault(model_bucket, {"gross": 0.0, "net": 0.0})
        md["gross"] = float(md.get("gross", 0.0) + aw)
        md["net"] = float(md.get("net", 0.0) + sw)

    return {
        "gross": float(_gross(rows or {}, con)),
        "net": float(_net(rows or {}, con)),
        "long_gross": float(long_gross),
        "short_gross": float(short_gross),
        "by_symbol": by_symbol,
        "by_asset_class": dict(sorted(by_asset_class.items(), key=lambda kv: kv[0])),
        "by_strategy": dict(sorted(by_strategy.items(), key=lambda kv: kv[0])),
        "by_model": dict(sorted(by_model.items(), key=lambda kv: kv[0])),
        "by_sector": dict(sorted(by_sector.items(), key=lambda kv: kv[0])),
    }


def _option_contract_meta(con, symbol: str) -> Optional[Dict[str, Any]]:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return None
    try:
        from engine.data.universe import get_instrument_metadata

        if con is not None:
            raw = get_instrument_metadata(con, sym)
            if isinstance(raw, dict) and str(raw.get("asset_class") or "").upper().strip() == "OPTION":
                return dict(raw)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_OPTION_METADATA_LOOKUP_FAILED",
            e,
            once_key=f"option_metadata:{sym}",
            symbol=str(sym),
        )
    try:
        from engine.data.options_instrument import parse_option_symbol

        parsed = parse_option_symbol(sym)
        if parsed is not None:
            to_dict = getattr(parsed, "to_dict", None)
            payload: Any = to_dict() if callable(to_dict) else None
            if isinstance(payload, dict):
                return {str(key): value for key, value in payload.items()}
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_OPTION_METADATA_PARSE_FAILED",
            e,
            once_key=f"option_metadata_parse:{sym}",
            symbol=str(sym),
        )
    return None


def _option_contract_key(con, symbol: str, row: Optional[Dict[str, Any]] = None) -> Optional[str]:
    payload = row or {}
    for key in ("option_contract", "contract", "occ_symbol", "local_symbol"):
        raw = payload.get(key)
        if raw not in (None, ""):
            text = str(raw).upper().strip()
            if text.startswith("O:"):
                text = text[2:]
            if text:
                return text
    meta = _option_contract_meta(con, symbol)
    if not meta:
        return None
    contract = str(meta.get("occ_symbol") or meta.get("symbol") or symbol).upper().strip()
    if contract.startswith("O:"):
        contract = contract[2:]
    return contract or None


def _option_contract_multiplier(con, symbol: str) -> Optional[float]:
    meta = _option_contract_meta(con, symbol)
    if not meta:
        return None
    multiplier = _optional_float(meta.get("multiplier") if isinstance(meta, dict) else None)
    if multiplier is None or multiplier <= 0.0:
        return None
    return float(multiplier)


def _option_greeks(con, symbol: str) -> Optional[Dict[str, Any]]:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return None
    try:
        if str(_asset_class_for(con, sym) or "").upper().strip() != "OPTION":
            return None
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_OPTION_ASSET_CLASS_CHECK_FAILED",
            e,
            once_key=f"option_asset_class:{sym}",
            symbol=str(sym),
        )
        return None

    contract = _option_contract_key(con, sym)
    multiplier = _option_contract_multiplier(con, sym)
    if not contract or multiplier is None:
        return None

    try:
        row = con.execute(
            """
            SELECT delta, gamma, theta, vega, ts_ms
            FROM options_chain_v2
            WHERE contract=?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (str(contract),),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_OPTION_GREEKS_LOOKUP_FAILED",
            e,
            once_key=f"option_greeks:{contract}",
            symbol=str(sym),
            contract=str(contract),
        )
        return None
    if not row:
        return None

    delta = _optional_float(row[0])
    gamma = _optional_float(row[1])
    theta = _optional_float(row[2])
    vega = _optional_float(row[3])
    if delta is None or gamma is None or theta is None or vega is None:
        return None
    return {
        "delta": float(delta),
        "gamma": float(gamma),
        "theta": float(theta),
        "vega": float(vega),
        "multiplier": float(multiplier),
        "contract": str(contract),
        "quote_ts_ms": float(_safe_float(row[4], 0.0)),
    }


def _nested_numeric_value(row: Optional[Dict[str, Any]], keys: Tuple[str, ...]) -> Optional[float]:
    containers: List[Any] = [row or {}]
    reason = (row or {}).get("reason") if isinstance(row, dict) else None
    if isinstance(reason, dict):
        containers.append(reason)
    for json_key in ("meta_json", "explain_json"):
        parsed = _maybe_parse_json((row or {}).get(json_key) if isinstance(row, dict) else None)
        if parsed:
            containers.append(parsed)
            nested_reason = parsed.get("reason")
            if isinstance(nested_reason, dict):
                containers.append(nested_reason)
    for container in containers:
        if not isinstance(container, dict):
            continue
        for key in keys:
            value = _optional_float(container.get(key))
            if value is not None:
                return float(value)
    return None


def _option_signed_contracts(row: Optional[Dict[str, Any]]) -> float:
    explicit = _nested_numeric_value(row, ("contracts", "qty", "quantity", "order_qty", "target_contracts"))
    if explicit is not None:
        if explicit < 0.0:
            return float(explicit)
        return float(abs(explicit)) * float(_side_sign((row or {}).get("side", "LONG")))
    return float(_signed_weight(row))


def _option_margin_impact_fraction(row: Optional[Dict[str, Any]]) -> float:
    value = _nested_numeric_value(row, ("margin_impact_fraction", "estimated_margin_fraction", "margin_fraction"))
    return max(0.0, float(value or 0.0))


def _options_greek_snapshot(con, rows: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate option greeks from chain rows.

    Each option row contributes ``signed_contracts * contract_multiplier *
    per-contract-greek``. ``margin_impact_fraction`` is summed from row metadata
    as an already-normalized fraction of equity; missing values add zero rather
    than fabricating a margin estimate.
    """

    by_symbol: Dict[str, Dict[str, Any]] = {}
    net_delta = 0.0
    net_gamma = 0.0
    net_theta = 0.0
    net_vega = 0.0
    gross_contracts = 0.0
    max_position_contracts = 0.0
    margin_impact_fraction = 0.0
    missing_greeks: List[str] = []

    equity_ref, equity_source = _equity_reference(con)

    for sym, row in (rows or {}).items():
        symbol = str(sym or "").upper().strip()
        if not symbol:
            continue
        try:
            if str(_asset_class_for(con, symbol) or "").upper().strip() != "OPTION":
                continue
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_RISK_OPTION_SNAPSHOT_ASSET_CLASS_FAILED",
                e,
                once_key=f"option_snapshot_asset_class:{symbol}",
                symbol=str(symbol),
            )
            continue

        signed_contracts = _option_signed_contracts(row)
        if abs(float(signed_contracts)) <= 1e-12:
            continue
        greeks = _option_greeks(con, symbol)
        if not greeks:
            missing_greeks.append(symbol)
            continue
        multiplier = float(greeks.get("multiplier") or 0.0)
        if multiplier <= 0.0:
            missing_greeks.append(symbol)
            continue

        delta_contribution = float(signed_contracts) * float(greeks["delta"]) * multiplier
        gamma_contribution = float(signed_contracts) * float(greeks["gamma"]) * multiplier
        theta_contribution = float(signed_contracts) * float(greeks["theta"]) * multiplier
        vega_contribution = float(signed_contracts) * float(greeks["vega"]) * multiplier
        margin_fraction = _option_margin_impact_fraction(row)

        net_delta += float(delta_contribution)
        net_gamma += float(gamma_contribution)
        net_theta += float(theta_contribution)
        net_vega += float(vega_contribution)
        gross_contracts += abs(float(signed_contracts))
        max_position_contracts = max(float(max_position_contracts), abs(float(signed_contracts)))
        margin_impact_fraction += float(margin_fraction)

        by_symbol[symbol] = {
            "contract": str(greeks.get("contract") or symbol),
            "signed_contracts": float(signed_contracts),
            "gross_contracts": float(abs(float(signed_contracts))),
            "contract_multiplier": float(multiplier),
            "delta": float(greeks["delta"]),
            "gamma": float(greeks["gamma"]),
            "theta": float(greeks["theta"]),
            "vega": float(greeks["vega"]),
            "net_delta": float(delta_contribution),
            "net_gamma": float(gamma_contribution),
            "net_theta": float(theta_contribution),
            "net_vega": float(vega_contribution),
            "margin_impact_fraction": float(margin_fraction),
            "quote_ts_ms": int(greeks.get("quote_ts_ms") or 0),
        }

    return {
        "net_delta": float(net_delta),
        "net_gamma": float(net_gamma),
        "net_theta": float(net_theta),
        "net_vega": float(net_vega),
        "gross_contracts": float(gross_contracts),
        "max_position_contracts": float(max_position_contracts),
        "margin_impact_fraction": float(margin_impact_fraction),
        "by_symbol": dict(sorted(by_symbol.items(), key=lambda kv: kv[0])),
        "missing_greeks": sorted(missing_greeks),
        "equity_ref": (float(equity_ref) if float(equity_ref) > 0.0 else None),
        "equity_ref_source": str(equity_source),
        "enabled": bool(USE_OPTIONS_GREEK_LIMITS),
    }


def _asset_class_lookup(con, rows: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for sym in sorted((rows or {}).keys()):
        out[str(sym)] = _asset_class_for(con, str(sym))
    return out


def _delta_snapshot(
    current_rows: Dict[str, Dict[str, Any]],
    target_rows: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    syms = sorted(set((current_rows or {}).keys()) | set((target_rows or {}).keys()))
    by_symbol: Dict[str, Dict[str, float]] = {}
    gross_add = 0.0
    gross_reduce = 0.0
    net_delta = 0.0

    for sym in syms:
        cur = _signed_weight((current_rows or {}).get(sym))
        tgt = _signed_weight((target_rows or {}).get(sym))
        delta = float(tgt - cur)
        if abs(delta) <= 1e-12:
            continue
        by_symbol[str(sym)] = {
            "current": float(cur),
            "target": float(tgt),
            "delta": float(delta),
        }
        net_delta += float(delta)
        if abs(tgt) > abs(cur):
            gross_add += float(abs(tgt) - abs(cur))
        elif abs(cur) > abs(tgt):
            gross_reduce += float(abs(cur) - abs(tgt))

    return {
        "gross_add": float(gross_add),
        "gross_reduce": float(gross_reduce),
        "net_delta": float(net_delta),
        "by_symbol": by_symbol,
    }


def _reconciliation_summary(pre_snapshot: Dict[str, Any], post_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    def _bucket_delta(pre_map: Dict[str, Any], post_map: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
        out: Dict[str, Dict[str, float]] = {}
        keys = sorted(set((pre_map or {}).keys()) | set((post_map or {}).keys()))
        for key in keys:
            pre_gross = _safe_float(((pre_map or {}).get(key) or {}).get("gross"), 0.0)
            pre_net = _safe_float(((pre_map or {}).get(key) or {}).get("net"), 0.0)
            post_gross = _safe_float(((post_map or {}).get(key) or {}).get("gross"), 0.0)
            post_net = _safe_float(((post_map or {}).get(key) or {}).get("net"), 0.0)
            if abs(pre_gross - post_gross) <= 1e-12 and abs(pre_net - post_net) <= 1e-12:
                continue
            out[str(key)] = {
                "gross_pre": float(pre_gross),
                "gross_post": float(post_gross),
                "gross_delta": float(post_gross - pre_gross),
                "net_pre": float(pre_net),
                "net_post": float(post_net),
                "net_delta": float(post_net - pre_net),
            }
        return out

    return {
        "gross_pre": float(_safe_float(pre_snapshot.get("gross"), 0.0)),
        "gross_post": float(_safe_float(post_snapshot.get("gross"), 0.0)),
        "gross_delta": float(_safe_float(post_snapshot.get("gross"), 0.0) - _safe_float(pre_snapshot.get("gross"), 0.0)),
        "net_pre": float(_safe_float(pre_snapshot.get("net"), 0.0)),
        "net_post": float(_safe_float(post_snapshot.get("net"), 0.0)),
        "net_delta": float(_safe_float(post_snapshot.get("net"), 0.0) - _safe_float(pre_snapshot.get("net"), 0.0)),
        "by_strategy": _bucket_delta(
            dict(pre_snapshot.get("by_strategy") or {}),
            dict(post_snapshot.get("by_strategy") or {}),
        ),
        "by_model": _bucket_delta(
            dict(pre_snapshot.get("by_model") or {}),
            dict(post_snapshot.get("by_model") or {}),
        ),
    }


def _last_price(con, symbol: str) -> Optional[float]:
    try:
        row = con.execute(
            "SELECT px FROM prices WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1",
            (str(symbol),),
        ).fetchone()
        if row and row[0] is not None:
            px = float(row[0])
            if px > 0.0:
                return float(px)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_LAST_PRICE_LOOKUP_FAILED", e, once_key=f"last_price_px:{symbol}", symbol=str(symbol), column="px")

    try:
        row = con.execute(
            "SELECT price FROM prices WHERE symbol=? ORDER BY ts_ms DESC LIMIT 1",
            (str(symbol),),
        ).fetchone()
        if row and row[0] is not None:
            px = float(row[0])
            if px > 0.0:
                return float(px)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_LAST_PRICE_LOOKUP_FAILED", e, once_key=f"last_price_price:{symbol}", symbol=str(symbol), column="price")

    return None


def _equity_reference(con) -> Tuple[float, str]:
    if con is None:
        return 0.0, "unknown"
    if _table_exists(con, "broker_account"):
        try:
            row = con.execute("SELECT equity FROM broker_account WHERE id=1").fetchone()
            if row and row[0] is not None:
                eq = float(row[0])
                if eq > 0.0:
                    return float(eq), "broker_account"
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_EQUITY_REFERENCE_LOOKUP_FAILED", e, once_key="equity_reference:broker_account_id", source="broker_account", query="id=1")

        try:
            row = con.execute("SELECT equity FROM broker_account ORDER BY updated_ts_ms DESC LIMIT 1").fetchone()
            if row and row[0] is not None:
                eq = float(row[0])
                if eq > 0.0:
                    return float(eq), "broker_account"
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_EQUITY_REFERENCE_LOOKUP_FAILED", e, once_key="equity_reference:broker_account_latest", source="broker_account", query="latest")

    if _table_exists(con, "equity_history"):
        try:
            row = con.execute("SELECT equity FROM equity_history ORDER BY ts_ms DESC LIMIT 1").fetchone()
            if row and row[0] is not None:
                eq = float(row[0])
                if eq > 0.0:
                    return float(eq), "equity_history"
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_EQUITY_REFERENCE_LOOKUP_FAILED", e, once_key="equity_reference:equity_history_latest", source="equity_history", query="latest")

    return 0.0, "unknown"


def _broker_account_columns(con) -> set[str]:
    if con is None or not _table_exists(con, "broker_account"):
        return set()
    try:
        return {str(row[1]) for row in con.execute("PRAGMA table_info(broker_account)").fetchall() or []}
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_BROKER_ACCOUNT_COLUMNS_LOOKUP_FAILED",
            e,
            once_key="broker_account_columns",
            source="broker_account",
        )
        return set()


def _buying_power_reference(con) -> Tuple[Optional[float], str]:
    """Return latest broker buying power when the current schema exposes it.

    The repository has two broker_account shapes: a broker-sim table with
    cash/equity but no buying_power, and runtime repair/first-run tables with
    equity/buying_power. Probe columns before every query so the guard fails
    closed instead of assuming either schema.
    """

    if con is None or not _table_exists(con, "broker_account"):
        return None, "unavailable"
    cols = _broker_account_columns(con)
    if "buying_power" not in cols:
        return None, "unavailable"

    queries: List[Tuple[str, str]] = []
    if "id" in cols:
        queries.append(("broker_account:id=1", "SELECT buying_power FROM broker_account WHERE id=1 LIMIT 1"))
    if "ts_ms" in cols:
        queries.append(("broker_account:ts_ms", "SELECT buying_power FROM broker_account ORDER BY ts_ms DESC LIMIT 1"))
    if "updated_ts_ms" in cols:
        queries.append(
            (
                "broker_account:updated_ts_ms",
                "SELECT buying_power FROM broker_account ORDER BY updated_ts_ms DESC LIMIT 1",
            )
        )
    queries.append(("broker_account:first", "SELECT buying_power FROM broker_account LIMIT 1"))

    seen: set[str] = set()
    for source, sql in queries:
        if sql in seen:
            continue
        seen.add(sql)
        try:
            row = con.execute(sql).fetchone()
            if row and row[0] is not None:
                bp = _optional_float(row[0])
                if bp is not None and bp >= 0.0:
                    return float(bp), str(source)
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_RISK_BUYING_POWER_REFERENCE_LOOKUP_FAILED",
                e,
                once_key=f"buying_power_reference:{source}",
                source="broker_account",
                query=str(source),
            )
    return None, "unavailable"


def _row_with_signed_weight(base_row: Optional[Dict[str, Any]], signed_weight: float, source: str) -> Dict[str, Any]:
    row = dict(base_row or {})
    s = float(signed_weight)
    row["weight"] = float(abs(s))
    row["side"] = ("LONG" if s > 0.0 else ("SHORT" if s < 0.0 else "FLAT"))
    row.setdefault("reason", {})
    if not isinstance(row.get("reason"), dict):
        row["reason"] = {"raw": row.get("reason")}
    row["reason"].setdefault("portfolio_risk_engine_source", str(source))
    return row


def _load_live_positions(con) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    info: Dict[str, Any] = {"source": "none", "equity_ref": None, "equity_ref_source": "unknown"}
    out: Dict[str, Dict[str, Any]] = {}

    eq_ref, eq_ref_source = _equity_reference(con)
    info["equity_ref"] = (float(eq_ref) if eq_ref > 0.0 else None)
    info["equity_ref_source"] = str(eq_ref_source)

    if _table_exists(con, "broker_positions"):
        try:
            rows = con.execute(
                """
                SELECT symbol, qty, avg_px, updated_ts_ms
                FROM broker_positions
                """
            ).fetchall()
        except Exception:
            rows = []

        notionals: Dict[str, float] = {}
        gross_notional = 0.0

        for symbol, qty, avg_px, _updated_ts_ms in rows or []:
            try:
                q = float(qty or 0.0)
            except Exception:
                q = 0.0
            if abs(q) <= 1e-12:
                continue

            px = _last_price(con, str(symbol))
            if px is None or px <= 0.0:
                try:
                    px = float(avg_px or 0.0)
                except Exception:
                    px = 0.0
            if px <= 0.0:
                continue

            meta = _futures_instrument(con, str(symbol))
            if isinstance(meta, dict):
                multiplier = _safe_float(meta.get("multiplier", meta.get("fut_multiplier")), 1.0)
                price_ccy = str(meta.get("price_ccy") or meta.get("fut_price_ccy") or "USD").upper().strip() or "USD"
                fx_rates = _futures_fx_rates_for(con, price_ccy, "USD")
                fx_rate = currency_conversion_rate(price_ccy, "USD", fx_rates)
                signed_notional = float(q) * float(px) * float(multiplier) * float(fx_rate)
            else:
                signed_notional = float(q) * float(px)
            notionals[str(symbol)] = float(signed_notional)
            gross_notional += abs(float(signed_notional))

        denom = float(eq_ref) if float(eq_ref) > 0.0 else float(gross_notional)
        if denom > 1e-12 and notionals:
            for sym, signed_notional in notionals.items():
                signed_weight = float(signed_notional) / float(denom)
                out[str(sym)] = {
                    "symbol": str(sym),
                    "weight": float(abs(signed_weight)),
                    "side": ("LONG" if signed_weight > 0.0 else ("SHORT" if signed_weight < 0.0 else "FLAT")),
                    "reason": {
                        "portfolio_risk_engine_live": {
                            "source": "broker_positions",
                            "equity_ref": float(denom),
                            "equity_ref_source": str(eq_ref_source),
                            "signed_notional": float(signed_notional),
                        }
                    },
                }

            info["source"] = "broker_positions"
            info["positions"] = int(len(out))
            info["gross_notional"] = float(gross_notional)
            return out, info

        if rows:
            info["source"] = "broker_positions_unpriced"
        else:
            info["source"] = "broker_positions_empty"

    if not _table_exists(con, "portfolio_state"):
        return out, info

    try:
        rows = con.execute(
            """
            SELECT
              symbol,
              SUM(CASE
                    WHEN UPPER(COALESCE(side, '')) IN ('LONG','BUY') THEN ABS(COALESCE(weight, 0.0))
                    WHEN UPPER(COALESCE(side, '')) IN ('SHORT','SELL') THEN -ABS(COALESCE(weight, 0.0))
                    ELSE COALESCE(weight, 0.0)
                  END) AS signed_weight,
              MAX(updated_ts_ms) AS updated_ts_ms,
              '{}' AS explain_json
            FROM portfolio_state
            GROUP BY symbol
            """
        ).fetchall()
    except Exception:
        rows = []

    if not rows:
        if info.get("source") == "none":
            info["source"] = "portfolio_state_empty"
        return out, info

    gross = 0.0
    state_rows: Dict[str, Dict[str, Any]] = {}

    for symbol, weight, _updated_ts_ms, explain_json in rows or []:
        try:
            sw = float(weight or 0.0)
        except Exception:
            sw = 0.0

        if abs(sw) <= 1e-12:
            continue

        reason = {}
        try:
            parsed = json.loads(explain_json or "{}")
            if isinstance(parsed, dict):
                reason = parsed
        except Exception:
            reason = {}

        state_rows[str(symbol)] = {
            "symbol": str(symbol),
            "weight": float(abs(sw)),
            "side": ("LONG" if sw > 0.0 else ("SHORT" if sw < 0.0 else "FLAT")),
            "reason": {
                "portfolio_risk_engine_live": {
                    "source": "portfolio_state",
                    "equity_ref": (float(eq_ref) if float(eq_ref) > 0.0 else None),
                    "equity_ref_source": str(eq_ref_source),
                },
                "portfolio_state_explain": reason,
            },
        }
        gross += abs(float(sw))

    if not state_rows:
        if info.get("source") == "none":
            info["source"] = "portfolio_state_empty"
        return out, info

    info["source"] = "portfolio_state"
    info["positions"] = int(len(state_rows))
    info["gross_notional"] = float(gross)
    return state_rows, info


def _project_live_plus_orders(
    live_rows: Dict[str, Dict[str, Any]],
    current_rows: Dict[str, Dict[str, Any]],
    target_rows: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    syms = sorted(set((live_rows or {}).keys()) | set((current_rows or {}).keys()) | set((target_rows or {}).keys()))
    out: Dict[str, Dict[str, Any]] = {}

    for sym in syms:
        live_sw = _signed_weight((live_rows or {}).get(sym))
        cur_sw = _signed_weight((current_rows or {}).get(sym))
        tgt_sw = _signed_weight((target_rows or {}).get(sym))
        projected_sw = float(live_sw + (tgt_sw - cur_sw))

        if abs(projected_sw) <= 1e-12:
            continue

        base_row = (target_rows or {}).get(sym) or (current_rows or {}).get(sym) or (live_rows or {}).get(sym) or {"symbol": str(sym)}
        out[str(sym)] = _row_with_signed_weight(base_row, projected_sw, "live_plus_orders")

    return out


def _projected_to_desired_targets(
    projected_rows: Dict[str, Dict[str, Any]],
    live_rows: Dict[str, Dict[str, Any]],
    current_rows: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    syms = sorted(set((projected_rows or {}).keys()) | set((live_rows or {}).keys()) | set((current_rows or {}).keys()))
    out: Dict[str, Dict[str, Any]] = {}

    for sym in syms:
        projected_sw = _signed_weight((projected_rows or {}).get(sym))
        live_sw = _signed_weight((live_rows or {}).get(sym))
        current_sw = _signed_weight((current_rows or {}).get(sym))
        desired_sw = float(current_sw + (projected_sw - live_sw))

        if abs(desired_sw) <= 1e-12:
            continue

        base_row = (projected_rows or {}).get(sym) or (current_rows or {}).get(sym) or {"symbol": str(sym)}
        out[str(sym)] = _row_with_signed_weight(base_row, desired_sw, "projected_to_desired")

    return out


def _post_constraint_checks(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    checks: Dict[str, Any] = {
        "gross_within_cap": (float(MAX_GROSS) <= 0.0) or (float(snapshot.get("gross", 0.0) or 0.0) <= float(MAX_GROSS) + 1e-9),
        "net_within_cap": (float(MAX_NET) <= 0.0) or (abs(float(snapshot.get("net", 0.0) or 0.0)) <= float(MAX_NET) + 1e-9),
    }

    by_symbol = dict(snapshot.get("by_symbol") or {})
    checks["symbol_within_cap"] = True
    if float(MAX_SYMBOL_GROSS) > 0.0:
        for sym, row in by_symbol.items():
            try:
                if float((row or {}).get("gross", 0.0) or 0.0) > float(MAX_SYMBOL_GROSS) + 1e-9:
                    checks["symbol_within_cap"] = False
                    checks.setdefault("symbol_violations", {})[str(sym)] = float((row or {}).get("gross", 0.0) or 0.0)
            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_RISK_ENGINE_SYMBOL_CAP_CHECK_FAILED",
                    e,
                    symbol=str(sym),
                )
                continue

    checks["asset_class_within_cap"] = True
    if USE_ASSET_CLASS_BUDGETS:
        for cls, row in dict(snapshot.get("by_asset_class") or {}).items():
            cap = float(ASSET_CLASS_BUDGETS.get(str(cls).upper(), ASSET_CLASS_BUDGETS.get("UNKNOWN", 0.40)))
            try:
                gross = float((row or {}).get("gross", 0.0) or 0.0)
            except Exception:
                gross = 0.0
            if cap > 0.0 and gross > cap + 1e-9:
                checks["asset_class_within_cap"] = False
                checks.setdefault("asset_class_violations", {})[str(cls)] = {"gross": float(gross), "cap": float(cap)}

    checks["sector_within_cap"] = True
    if USE_SECTOR_BUDGETS:
        for sector, row in dict(snapshot.get("by_sector") or {}).items():
            sector_key = str(sector or "").upper().strip()
            cap = float(SECTOR_BUDGETS.get(sector_key, SECTOR_MAX_GROSS))
            try:
                gross = float((row or {}).get("gross", 0.0) or 0.0)
            except Exception:
                gross = 0.0
            if cap > 0.0 and gross > cap + 1e-9:
                checks["sector_within_cap"] = False
                checks.setdefault("sector_violations", {})[str(sector)] = {
                    "gross": float(gross),
                    "cap": float(cap),
                }

    checks["strategy_within_cap"] = True
    if USE_STRATEGY_BUDGETS:
        for strategy, row in dict(snapshot.get("by_strategy") or {}).items():
            try:
                gross = float((row or {}).get("gross", 0.0) or 0.0)
                net = float((row or {}).get("net", 0.0) or 0.0)
            except Exception:
                gross = 0.0
                net = 0.0
            if (float(STRATEGY_MAX_GROSS) > 0.0 and gross > float(STRATEGY_MAX_GROSS) + 1e-9) or (float(STRATEGY_MAX_NET) > 0.0 and abs(net) > float(STRATEGY_MAX_NET) + 1e-9):
                checks["strategy_within_cap"] = False
                checks.setdefault("strategy_violations", {})[str(strategy)] = {
                    "gross": float(gross),
                    "net": float(net),
                    "max_gross": float(STRATEGY_MAX_GROSS),
                    "max_net": float(STRATEGY_MAX_NET),
                }

    checks["options_greeks_within_cap"] = True
    if USE_OPTIONS_GREEK_LIMITS:
        option_snapshot = dict(snapshot.get("options_greeks") or {})
        violations: Dict[str, Any] = {}

        def _check_abs(name: str, value_key: str, cap: float) -> None:
            if float(cap) <= 0.0:
                return
            value = float(option_snapshot.get(value_key, 0.0) or 0.0)
            if abs(value) > float(cap) + 1e-9:
                violations[name] = {"value": float(value), "abs": float(abs(value)), "cap": float(cap)}

        _check_abs("delta", "net_delta", float(OPTIONS_MAX_PORTFOLIO_DELTA_ABS))
        _check_abs("gamma", "net_gamma", float(OPTIONS_MAX_PORTFOLIO_GAMMA_ABS))
        _check_abs("vega", "net_vega", float(OPTIONS_MAX_PORTFOLIO_VEGA_ABS))

        if float(OPTIONS_MARGIN_IMPACT_MAX_FRACTION) > 0.0:
            margin_fraction = float(option_snapshot.get("margin_impact_fraction", 0.0) or 0.0)
            if margin_fraction > float(OPTIONS_MARGIN_IMPACT_MAX_FRACTION) + 1e-9:
                violations["margin_impact_fraction"] = {
                    "value": float(margin_fraction),
                    "cap": float(OPTIONS_MARGIN_IMPACT_MAX_FRACTION),
                }

        if float(OPTIONS_MAX_POSITION_CONTRACTS) > 0.0:
            for sym, row in dict(option_snapshot.get("by_symbol") or {}).items():
                contracts = abs(float((row or {}).get("signed_contracts", 0.0) or 0.0))
                if contracts > float(OPTIONS_MAX_POSITION_CONTRACTS) + 1e-9:
                    violations.setdefault("position_contracts", {})[str(sym)] = {
                        "contracts": float(contracts),
                        "cap": float(OPTIONS_MAX_POSITION_CONTRACTS),
                    }

        if violations:
            checks["options_greeks_within_cap"] = False
            checks["options_greek_violations"] = violations

    return checks


def _apply_options_delta_cap(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if (not USE_OPTIONS_GREEK_LIMITS) or float(OPTIONS_MAX_PORTFOLIO_DELTA_ABS) <= 0.0:
        return dict(desired or {})

    out = dict(desired or {})
    snapshot = _options_greek_snapshot(con, out)
    net_delta = float(snapshot.get("net_delta", 0.0) or 0.0)
    cap = float(OPTIONS_MAX_PORTFOLIO_DELTA_ABS)
    if abs(net_delta) <= cap + 1e-9:
        return out

    scale = float(cap / abs(net_delta)) if abs(net_delta) > 1e-12 else 0.0
    adjusted: Dict[str, Any] = {}
    option_symbols = set((snapshot.get("by_symbol") or {}).keys())
    for sym in sorted(option_symbols):
        if sym not in out:
            continue
        try:
            signed_contracts = _option_signed_contracts(out.get(sym))
            post_contracts = float(signed_contracts) * float(scale)
            out[sym] = _row_with_signed_weight(out.get(sym), float(post_contracts), "options_delta_cap")
            out[sym]["contracts"] = float(abs(post_contracts))
            for qty_key in ("qty", "quantity", "order_qty", "target_contracts"):
                if qty_key in out[sym]:
                    out[sym][qty_key] = float(abs(post_contracts))
            out[sym].setdefault("reason", {})
            if isinstance(out[sym]["reason"], dict):
                out[sym]["reason"]["options_delta_cap"] = {
                    "pre_net_delta": float(net_delta),
                    "cap": float(cap),
                    "scale": float(scale),
                    "pre_contracts": float(signed_contracts),
                    "post_contracts": float(post_contracts),
                }
            adjusted[str(sym)] = {
                "pre_contracts": float(signed_contracts),
                "post_contracts": float(post_contracts),
                "scale": float(scale),
            }
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_RISK_OPTIONS_DELTA_CAP_APPLY_FAILED",
                e,
                once_key=f"options_delta_cap:{sym}",
                symbol=str(sym),
            )

    if adjusted:
        info["options_delta_cap_scaled"] = True
        info["options_delta_cap_scale"] = float(scale)
        info["options_delta_cap_pre"] = dict(snapshot)
        info["options_delta_cap_adjustments"] = adjusted
    return out


def _apply_portfolio_caps(desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out = dict(desired or {})

    try:
        out = _apply_options_delta_cap(info.get("_risk_con"), out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_OPTIONS_DELTA_CAP_FAILED", e, once_key="options_delta_cap")

    g = _gross(out)
    n = _net(out)

    info["caps_pre_gross"] = float(g)
    info["caps_pre_net"] = float(n)
    info["cap_max_gross"] = float(MAX_GROSS)
    info["cap_max_net"] = float(MAX_NET)

    # Gross cap: scale all abs weights proportionally
    if float(MAX_GROSS) > 0.0 and g > float(MAX_GROSS) + 1e-12:
        scale = float(MAX_GROSS) / float(g) if g > 1e-12 else 0.0
        for sym in list(out.keys()):
            try:
                sw = _signed_weight(out[sym])
                sgn = 1.0 if sw >= 0.0 else -1.0
                out[sym]["weight"] = float(abs(sw) * scale) * float(sgn)
                out[sym].setdefault("reason", {})
                if isinstance(out[sym]["reason"], dict):
                    out[sym]["reason"]["portfolio_gross_cap"] = {"pre": float(g), "cap": float(MAX_GROSS), "scale": float(scale)}
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_GROSS_CAP_APPLY_FAILED", e, once_key=f"gross_cap:{sym}", symbol=str(sym))
        info["caps_gross_scaled"] = True
        info["caps_gross_scale"] = float(scale)

    # Net cap: if |net| exceeds, scale signed weights down around 0
    g2 = _gross(out)
    n2 = _net(out)

    if float(MAX_NET) > 0.0 and abs(n2) > float(MAX_NET) + 1e-12 and g2 > 1e-12:
        # scale signed weights toward 0 preserving signs
        scaleN = float(MAX_NET) / float(abs(n2)) if abs(n2) > 1e-12 else 0.0
        for sym in list(out.keys()):
            try:
                sw = _signed_weight(out[sym])
                out[sym]["weight"] = float(sw) * float(scaleN)
                out[sym].setdefault("reason", {})
                if isinstance(out[sym]["reason"], dict):
                    out[sym]["reason"]["portfolio_net_cap"] = {"pre": float(n2), "cap": float(MAX_NET), "scale": float(scaleN)}
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_NET_CAP_APPLY_FAILED", e, once_key=f"net_cap:{sym}", symbol=str(sym))
        info["caps_net_scaled"] = True
        info["caps_net_scale"] = float(scaleN)

    info["caps_post_gross"] = float(_gross(out))
    info["caps_post_net"] = float(_net(out))
    return out


def _apply_drawdown_throttle(desired: Dict[str, Dict[str, Any]], dd: float, info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out = dict(desired or {})

    if dd <= 0.0:
        return out

    info["dd"] = float(dd)
    info["dd_throttle_start"] = float(DD_THROTTLE_START)
    info["dd_throttle_min_scale"] = float(DD_THROTTLE_MIN_SCALE)
    info["dd_hard_block"] = float(DD_HARD_BLOCK)

    if float(DD_HARD_BLOCK) > 0.0 and dd >= float(DD_HARD_BLOCK):
        info["dd_hard_block_hit"] = True
        return out

    if float(DD_THROTTLE_START) > 0.0 and dd >= float(DD_THROTTLE_START):
        # linear ramp from 1.0 at start -> min_scale at hard_block (or at start+0.10 if hard_block disabled)
        end = float(DD_HARD_BLOCK) if float(DD_HARD_BLOCK) > float(DD_THROTTLE_START) else (float(DD_THROTTLE_START) + 0.10)
        t = (dd - float(DD_THROTTLE_START)) / max(1e-9, (end - float(DD_THROTTLE_START)))
        t = max(0.0, min(1.0, float(t)))
        scale = 1.0 - t * (1.0 - float(max(0.0, min(1.0, float(DD_THROTTLE_MIN_SCALE)))))
        g = _gross(out)
        if g > 1e-12:
            for sym in list(out.keys()):
                try:
                    sw = _signed_weight(out[sym])
                    sgn = 1.0 if sw >= 0.0 else -1.0
                    out[sym]["weight"] = float(abs(sw) * scale) * float(sgn)
                    out[sym].setdefault("reason", {})
                    if isinstance(out[sym]["reason"], dict):
                        out[sym]["reason"]["drawdown_throttle"] = {"dd": float(dd), "scale": float(scale)}
                except Exception as e:
                    _warn_nonfatal("PORTFOLIO_RISK_DRAWDOWN_THROTTLE_APPLY_FAILED", e, once_key=f"drawdown_throttle:{sym}", symbol=str(sym))
            info["dd_throttle_applied"] = True
            info["dd_throttle_scale"] = float(scale)

    return out


def _apply_asset_class_budgets(desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_ASSET_CLASS_BUDGETS:
        return dict(desired or {})

    out = dict(desired or {})
    lookup = dict(info.get("asset_class_by_symbol") or {}) if isinstance(info.get("asset_class_by_symbol"), dict) else {}
    exposure_con = info.get("_risk_con")
    by_cls: Dict[str, float] = {}
    for sym, row in (out or {}).items():
        cls = str(lookup.get(str(sym)) or _asset_class_for(exposure_con, str(sym)) or "UNKNOWN").upper()
        by_cls[cls] = float(by_cls.get(cls, 0.0) + _abs_exposure_weight(exposure_con, str(sym), row))

    info["asset_class_gross_pre"] = dict(sorted(by_cls.items(), key=lambda kv: kv[0]))

    hit: Dict[str, Any] = {}
    for cls, gross in list(by_cls.items()):
        cap = float(ASSET_CLASS_BUDGETS.get(str(cls).upper(), ASSET_CLASS_BUDGETS.get("UNKNOWN", 0.40)))
        if cap > 0.0 and float(gross) > float(cap) + 1e-12:
            scale = float(cap) / float(gross) if gross > 1e-12 else 0.0
            for sym in list(out.keys()):
                try:
                    cls2 = str(lookup.get(str(sym)) or _asset_class_for(exposure_con, str(sym)) or "UNKNOWN").upper()
                    if cls2 == str(cls).upper():
                        sw = _signed_weight(out[sym])
                        sgn = 1.0 if sw >= 0.0 else -1.0
                        out[sym]["weight"] = float(abs(sw) * scale) * float(sgn)
                        out[sym].setdefault("reason", {})
                        if isinstance(out[sym]["reason"], dict):
                            out[sym]["reason"]["asset_class_budget"] = {
                                "asset_class": str(cls).upper(),
                                "gross_pre": float(gross),
                                "cap": float(cap),
                                "scale": float(scale),
                            }
                except Exception as e:
                    _warn_nonfatal("PORTFOLIO_RISK_ASSET_CLASS_BUDGET_APPLY_FAILED", e, once_key=f"asset_budget:{cls}:{sym}", asset_class=str(cls), symbol=str(sym))
            hit[str(cls).upper()] = {"gross_pre": float(gross), "cap": float(cap), "scale": float(scale)}

    if hit:
        info["asset_class_budgets_hit"] = hit

    # post
    by_cls2: Dict[str, float] = {}
    for sym, row in (out or {}).items():
        cls = str(lookup.get(str(sym)) or _asset_class_for(exposure_con, str(sym)) or "UNKNOWN").upper()
        by_cls2[cls] = float(by_cls2.get(cls, 0.0) + _abs_exposure_weight(exposure_con, str(sym), row))
    info["asset_class_gross_post"] = dict(sorted(by_cls2.items(), key=lambda kv: kv[0]))

    return out


def _apply_sector_budgets(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_SECTOR_BUDGETS:
        return dict(desired or {})

    out = dict(desired or {})
    sector_by_symbol: Dict[str, str] = {}
    by_sector: Dict[str, float] = {}
    for sym, row in (out or {}).items():
        sector = _sector_for(con, str(sym))
        if not sector:
            continue
        sector_by_symbol[str(sym)] = str(sector)
        by_sector[sector] = float(by_sector.get(sector, 0.0) + _abs_exposure_weight(con, str(sym), row))

    if by_sector:
        info["sector_gross_pre"] = dict(sorted(by_sector.items(), key=lambda kv: kv[0]))
    else:
        info["sector_gross_pre"] = {}

    hit: Dict[str, Any] = {}
    for sector, gross in list(by_sector.items()):
        sector_key = str(sector).upper().strip()
        cap = float(SECTOR_BUDGETS.get(sector_key, SECTOR_MAX_GROSS))
        if cap > 0.0 and float(gross) > float(cap) + 1e-12:
            scale = float(cap) / float(gross) if gross > 1e-12 else 0.0
            for sym in list(out.keys()):
                if sector_by_symbol.get(str(sym)) != sector_key:
                    continue
                try:
                    sw = _signed_weight(out[sym])
                    sgn = 1.0 if sw >= 0.0 else -1.0
                    out[sym]["weight"] = float(abs(sw) * scale) * float(sgn)
                    out[sym].setdefault("reason", {})
                    if isinstance(out[sym]["reason"], dict):
                        out[sym]["reason"]["sector_budget"] = {
                            "sector": sector_key,
                            "gross_pre": float(gross),
                            "cap": float(cap),
                            "scale": float(scale),
                        }
                except Exception as e:
                    _warn_nonfatal(
                        "PORTFOLIO_RISK_SECTOR_BUDGET_APPLY_FAILED",
                        e,
                        once_key=f"sector_budget:{sector_key}:{sym}",
                        sector=str(sector_key),
                        symbol=str(sym),
                    )
            hit[sector_key] = {"gross_pre": float(gross), "cap": float(cap), "scale": float(scale)}

    if hit:
        info["sector_budgets_hit"] = hit

    by_sector_post: Dict[str, float] = {}
    for sym, row in (out or {}).items():
        sector = sector_by_symbol.get(str(sym))
        if not sector:
            continue
        by_sector_post[sector] = float(by_sector_post.get(sector, 0.0) + _abs_exposure_weight(con, str(sym), row))
    info["sector_gross_post"] = dict(sorted(by_sector_post.items(), key=lambda kv: kv[0]))

    return out


def _apply_fx_leverage_caps(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_FX_LEVERAGE_CAPS:
        return dict(desired or {})

    out = dict(desired or {})
    fx_rows = [
        str(sym)
        for sym, row in (out or {}).items()
        if _abs_weight(row) > 0.0
        and str((info.get("asset_class_by_symbol") or {}).get(str(sym)) or _asset_class_for(con, str(sym))).upper() == "FX"
    ]
    if not fx_rows:
        return out

    equity, equity_source = _equity_reference(con)
    info["fx_leverage_equity_ref"] = float(equity or 0.0)
    info["fx_leverage_equity_ref_source"] = str(equity_source or "unknown")
    adjustments: Dict[str, Any] = {}
    hard_blocks: List[Dict[str, Any]] = []

    if equity <= 0.0:
        for sym in fx_rows:
            hard_blocks.append({"symbol": str(sym), "reason": "fx_equity_reference_unavailable"})
        info["fx_leverage_hard_blocks"] = hard_blocks
        return out

    for sym in fx_rows:
        row = dict(out.get(sym) or {})
        signed_weight = _signed_weight(row)
        instrument = _fx_instrument(con, sym)
        if not instrument:
            clamped, clamp_reason = clamp_fx_weight_to_leverage(sym, signed_weight, equity, instrument)
            row.setdefault("reason", {})
            if not isinstance(row.get("reason"), dict):
                row["reason"] = {"raw": row.get("reason")}
            row["reason"]["fx_leverage_cap"] = dict(clamp_reason)
            out[sym] = row
            adjustments[sym] = dict(clamp_reason)
            continue

        rate = _last_price(con, sym)
        if rate is None or float(rate) <= 0.0:
            reason = {
                "symbol": str(sym),
                "reason": "fx_pair_rate_unavailable",
                "asset_class": "FX",
            }
            hard_blocks.append(reason)
            row.setdefault("reason", {})
            if not isinstance(row.get("reason"), dict):
                row["reason"] = {"raw": row.get("reason")}
            row["reason"]["fx_leverage_cap"] = dict(reason)
            out[sym] = row
            adjustments[sym] = dict(reason)
            continue

        clamped, clamp_reason = clamp_fx_weight_to_leverage(sym, signed_weight, equity, instrument)
        fx_meta = fx_weight_to_notional(sym, clamped, equity, instrument, pair_rate=float(rate))
        fx_meta["pre_weight"] = float(signed_weight)
        fx_meta["post_weight"] = float(clamped)

        if abs(float(clamped)) + 1e-9 < abs(float(signed_weight)):
            row = _row_with_signed_weight(row, float(clamped), "fx_leverage_cap")

        row.setdefault("reason", {})
        if not isinstance(row.get("reason"), dict):
            row["reason"] = {"raw": row.get("reason")}
        row["reason"]["fx_leverage_cap"] = dict(clamp_reason)
        row["fx"] = dict(fx_meta)
        out[sym] = row
        adjustments[sym] = {"clamp": dict(clamp_reason), "fx": dict(fx_meta)}

        cap = float(fx_meta.get("effective_leverage_cap") or 0.0)
        eff = abs(float(fx_meta.get("effective_leverage") or 0.0))
        if cap <= 0.0 or eff > cap + 1e-9:
            hard_blocks.append(
                {
                    "symbol": str(sym),
                    "reason": "fx_leverage_residual_breach",
                    "effective_leverage": float(eff),
                    "effective_leverage_cap": float(cap),
                }
            )

    if adjustments:
        info["fx_leverage_adjustments"] = adjustments
    if hard_blocks:
        info["fx_leverage_hard_blocks"] = hard_blocks
    return out


def _apply_equity_leverage_caps(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_EQUITY_LEVERAGE_CAPS:
        return dict(desired or {})

    out = dict(desired or {})
    lookup = dict(info.get("asset_class_by_symbol") or {}) if isinstance(info.get("asset_class_by_symbol"), dict) else {}
    equity_rows = [
        str(sym)
        for sym, row in (out or {}).items()
        if _abs_weight(row) > 0.0 and str(lookup.get(str(sym)) or _asset_class_for(con, str(sym))).upper() == "EQUITY"
    ]
    if not equity_rows:
        return out

    rows_subset = {sym: dict(out.get(sym) or {}) for sym in equity_rows}
    gross_pre = float(_gross(rows_subset, con))
    account_equity, account_equity_source = _equity_reference(con)
    buying_power, buying_power_source = _buying_power_reference(con)
    mode = equity_leverage_mode()
    max_leverage = max_equity_leverage(mode=mode)
    hard_blocks: List[Dict[str, Any]] = []

    if account_equity <= 0.0:
        for sym in equity_rows:
            hard_blocks.append({"symbol": str(sym), "reason": "equity_account_reference_unavailable"})
        info["equity_leverage_account_equity"] = float(account_equity or 0.0)
        info["equity_leverage_account_equity_source"] = str(account_equity_source or "unknown")
        info["equity_leverage_gross_pre"] = float(gross_pre)
        info["equity_leverage_mode"] = str(mode)
        info["equity_leverage_hard_blocks"] = hard_blocks
        return out

    if str(mode) == "reg_t" and buying_power is None:
        for sym in equity_rows:
            hard_blocks.append({"symbol": str(sym), "reason": "equity_buying_power_unavailable"})
        info["equity_leverage_account_equity"] = float(account_equity)
        info["equity_leverage_account_equity_source"] = str(account_equity_source or "unknown")
        info["equity_leverage_buying_power"] = None
        info["equity_leverage_buying_power_source"] = str(buying_power_source or "unavailable")
        info["equity_leverage_gross_pre"] = float(gross_pre)
        info["equity_leverage_mode"] = str(mode)
        info["equity_leverage_max_leverage"] = float(max_leverage)
        info["equity_leverage_hard_blocks"] = hard_blocks
        return out

    account = {"equity": float(account_equity)}
    if buying_power is not None:
        account["buying_power"] = float(buying_power)
    buying_power_base, base_reason = equity_deployable_base(
        account,
        account_equity=float(account_equity),
        mode=str(mode),
        max_leverage=float(max_leverage),
    )
    allowed_gross_weight = min(
        float(max_leverage),
        (float(buying_power_base) / float(account_equity) if float(account_equity) > 0.0 else 0.0),
    )
    allowed_gross_weight = max(0.0, float(allowed_gross_weight))

    clamped_rows, clamp_reason = clamp_equity_gross_to_leverage(
        rows_subset,
        account_equity=float(account_equity),
        allowed_gross_weight=float(allowed_gross_weight),
        mode=str(mode),
    )
    if not bool(clamp_reason.get("clamped")):
        return out

    gross_post = float(_gross(clamped_rows, con))
    adjustments: Dict[str, Any] = {}
    for sym in equity_rows:
        row = dict(out.get(sym) or {})
        pre_signed = _signed_weight(row)
        post_signed = _signed_weight(clamped_rows.get(sym))
        if abs(float(post_signed)) + 1e-12 < abs(float(pre_signed)) or (pre_signed == 0.0 and post_signed != 0.0):
            row = _row_with_signed_weight(row, float(post_signed), "equity_leverage_cap")
        row.setdefault("reason", {})
        if not isinstance(row.get("reason"), dict):
            row["reason"] = {"raw": row.get("reason")}
        row["reason"]["equity_leverage_cap"] = dict(clamp_reason)
        equity_meta = {
            "mode": str(mode),
            "account_equity": float(account_equity),
            "account_equity_source": str(account_equity_source or "unknown"),
            "buying_power": (float(buying_power) if buying_power is not None else None),
            "buying_power_source": str(buying_power_source or "unavailable"),
            "buying_power_base": float(buying_power_base),
            "allowed_gross_weight": float(allowed_gross_weight),
            "gross_pre": float(gross_pre),
            "gross_post": float(gross_post),
            "effective_leverage_pre": float(gross_pre),
            "effective_leverage_post": float(gross_post),
        }
        row["equity"] = dict(equity_meta)
        out[sym] = row
        adjustments[sym] = {
            "pre_weight": float(pre_signed),
            "post_weight": float(post_signed),
            "clamp": dict(clamp_reason),
            "equity": dict(equity_meta),
        }

    if gross_post > allowed_gross_weight + 1e-9:
        for sym in equity_rows:
            hard_blocks.append(
                {
                    "symbol": str(sym),
                    "reason": "equity_leverage_residual_breach",
                    "gross_post": float(gross_post),
                    "allowed_gross_weight": float(allowed_gross_weight),
                }
            )

    info["equity_leverage_account_equity"] = float(account_equity)
    info["equity_leverage_account_equity_source"] = str(account_equity_source or "unknown")
    info["equity_leverage_buying_power"] = (float(buying_power) if buying_power is not None else None)
    info["equity_leverage_buying_power_source"] = str(buying_power_source or "unavailable")
    info["equity_leverage_buying_power_base"] = float(buying_power_base)
    info["equity_leverage_base_reason"] = dict(base_reason)
    info["equity_leverage_gross_pre"] = float(gross_pre)
    info["equity_leverage_gross_post"] = float(gross_post)
    info["equity_leverage_allowed_gross_weight"] = float(allowed_gross_weight)
    info["equity_leverage_mode"] = str(mode)
    info["equity_leverage_max_leverage"] = float(max_leverage)
    info["equity_leverage_adjustments"] = adjustments
    if hard_blocks:
        info["equity_leverage_hard_blocks"] = hard_blocks
    return out


def _apply_crypto_leverage_caps(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_CRYPTO_LEVERAGE_CAPS:
        return dict(desired or {})

    out = dict(desired or {})
    lookup = dict(info.get("asset_class_by_symbol") or {}) if isinstance(info.get("asset_class_by_symbol"), dict) else {}
    crypto_rows = [
        str(sym)
        for sym, row in (out or {}).items()
        if _abs_weight(row) > 0.0
        and str(lookup.get(str(sym)) or _asset_class_for(con, str(sym))).upper() in {"CRYPTO", "CRYPTOCURRENCY"}
    ]
    if not crypto_rows:
        return out

    equity, equity_source = _equity_reference(con)
    info["crypto_leverage_equity_ref"] = float(equity or 0.0)
    info["crypto_leverage_equity_ref_source"] = str(equity_source or "unknown")
    adjustments: Dict[str, Any] = {}
    hard_blocks: List[Dict[str, Any]] = []

    if equity <= 0.0:
        for sym in crypto_rows:
            hard_blocks.append({"symbol": str(sym), "reason": "crypto_equity_reference_unavailable"})
        info["crypto_leverage_hard_blocks"] = hard_blocks
        return out

    for sym in crypto_rows:
        row = dict(out.get(sym) or {})
        signed_weight = _signed_weight(row)
        instrument = _crypto_instrument(con, sym) or {"asset_class": "CRYPTO", "symbol": normalize_crypto_symbol(sym)}

        try:
            resolved = _symbol_vol_input(con, str(sym), ts_ms=int(info.get("ts_ms") or 0))
            if resolved.get("vol") is None:
                root = normalize_crypto_symbol(sym)
                if root and root != str(sym).upper().strip():
                    resolved = _symbol_vol_input(con, str(root), ts_ms=int(info.get("ts_ms") or 0))
            if resolved.get("vol") is not None:
                instrument = dict(instrument)
                instrument["volatility"] = float(resolved.get("vol") or 0.0)
                instrument["volatility_source"] = str(resolved.get("source") or "")
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_RISK_CRYPTO_VOL_INPUT_FAILED",
                e,
                once_key=f"crypto_vol:{sym}",
                symbol=str(sym),
            )

        price = _last_price(con, sym)
        if price is None or float(price) <= 0.0:
            root = normalize_crypto_symbol(sym)
            if root and root != str(sym).upper().strip():
                price = _last_price(con, root)

        clamped, clamp_reason = clamp_crypto_weight_to_leverage(sym, signed_weight, equity, instrument)
        crypto_meta = crypto_weight_to_notional(sym, clamped, equity, instrument, price=(float(price) if price is not None else None))
        crypto_meta["pre_weight"] = float(signed_weight)
        crypto_meta["post_weight"] = float(clamped)
        if instrument.get("volatility_source"):
            crypto_meta["volatility_source"] = str(instrument.get("volatility_source") or "")

        if abs(float(clamped)) + 1e-9 < abs(float(signed_weight)):
            row = _row_with_signed_weight(row, float(clamped), "crypto_leverage_cap")

        row = attach_crypto_sizing_context(row, crypto_meta, clamp_reason)
        out[sym] = row
        adjustments[sym] = {"clamp": dict(clamp_reason), "crypto": dict(crypto_meta)}

        cap = float(crypto_meta.get("effective_leverage_cap") or 0.0)
        eff = abs(float(crypto_meta.get("effective_leverage") or 0.0))
        if cap <= 0.0 or eff > cap + 1e-9:
            hard_blocks.append(
                {
                    "symbol": str(sym),
                    "reason": "crypto_leverage_residual_breach",
                    "effective_leverage": float(eff),
                    "effective_leverage_cap": float(cap),
                }
            )

    if adjustments:
        info["crypto_leverage_adjustments"] = adjustments
    if hard_blocks:
        info["crypto_leverage_hard_blocks"] = hard_blocks
    return out


def _margin_override_for(symbol: str, meta: Dict[str, Any]) -> Optional[float]:
    raw = str(os.environ.get("FUTURES_MARGIN_REQUIREMENTS_JSON") or os.environ.get("FUTURES_BROKER_MARGIN_JSON") or "").strip()
    if not raw:
        return None
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return None
        keys = [
            str(symbol),
            str(symbol).upper(),
            str(meta.get("symbol") or ""),
            str(meta.get("root") or meta.get("fut_root") or ""),
        ]
        for key in keys:
            if key and key in data:
                value = _safe_float(data.get(key), 0.0)
                if value > 0.0:
                    return float(value)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_FUTURES_MARGIN_JSON_FAILED",
            e,
            once_key="futures_margin_json",
        )
    return None


def _futures_fx_rates_for(con, price_ccy: str, account_ccy: str = "USD") -> Dict[str, float]:
    price = str(price_ccy or account_ccy or "USD").upper().strip() or "USD"
    account = str(account_ccy or "USD").upper().strip() or "USD"
    if price == account:
        return {}
    rates: Dict[str, float] = {}
    direct = f"{price}{account}"
    inverse = f"{account}{price}"
    try:
        px = _last_price(con, direct)
        if px is not None and float(px) > 0.0:
            rates[direct] = float(px)
            return rates
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_FUTURES_DIRECT_FX_RATE_LOOKUP_FAILED",
            e,
            once_key=f"futures_direct_fx_rate:{direct}",
            pair=direct,
        )
    try:
        px = _last_price(con, inverse)
        if px is not None and float(px) > 0.0:
            rates[inverse] = float(px)
            return rates
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_FUTURES_INVERSE_FX_RATE_LOOKUP_FAILED",
            e,
            once_key=f"futures_inverse_fx_rate:{inverse}",
            pair=inverse,
        )
    return rates


def _apply_futures_margin_caps(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_FUTURES_MARGIN_CAPS:
        return dict(desired or {})

    out = dict(desired or {})
    futures_rows = [
        str(sym)
        for sym, row in (out or {}).items()
        if _abs_weight(row) > 0.0 and _futures_instrument(con, str(sym)) is not None
    ]
    if not futures_rows:
        return out

    equity, equity_source = _equity_reference(con)
    info["futures_margin_equity_ref"] = float(equity or 0.0)
    info["futures_margin_equity_ref_source"] = str(equity_source or "unknown")
    budget_weight = float(ASSET_CLASS_BUDGETS.get("FUTURES", ASSET_CLASS_BUDGETS.get("UNKNOWN", 0.40)))
    info["futures_margin_budget_weight"] = float(budget_weight)

    adjustments: Dict[str, Any] = {}
    hard_blocks: List[Dict[str, Any]] = []

    if equity <= 0.0:
        for sym in futures_rows:
            hard_blocks.append({"symbol": str(sym), "reason": "futures_equity_reference_unavailable"})
        info["futures_margin_hard_blocks"] = hard_blocks
        return out

    for sym in futures_rows:
        row = dict(out.get(sym) or {})
        signed_weight = _signed_weight(row)
        meta = _futures_instrument(con, sym) or {}
        multiplier = _safe_float(meta.get("multiplier", meta.get("fut_multiplier")), 0.0)
        reference_margin = _safe_float(meta.get("margin_ref", meta.get("fut_margin_ref")), 0.0)
        price_ccy = str(meta.get("price_ccy") or meta.get("fut_price_ccy") or "USD").upper().strip() or "USD"
        px = _last_price(con, sym)
        if px is None or float(px) <= 0.0:
            reason = {
                "symbol": str(sym),
                "reason": "futures_price_unavailable",
                "asset_class": "FUTURES",
            }
            hard_blocks.append(reason)
            row.setdefault("reason", {})
            if not isinstance(row.get("reason"), dict):
                row["reason"] = {"raw": row.get("reason")}
            row["reason"]["futures_margin_cap"] = dict(reason)
            out[sym] = row
            adjustments[sym] = dict(reason)
            continue

        fx_rates = _futures_fx_rates_for(con, price_ccy, "USD")
        fx_rate = currency_conversion_rate(price_ccy, "USD", fx_rates)
        account_price = float(px) * float(fx_rate)
        desired_contracts = weight_to_contracts(signed_weight, equity, multiplier, account_price)
        broker_margin = _margin_override_for(sym, meta)
        capped_contracts, margin_meta = cap_contracts_by_margin(
            desired_contracts,
            equity,
            budget_weight,
            reference_margin,
            broker_margin,
            price_ccy=price_ccy,
            account_ccy="USD",
            fx_rates=fx_rates,
        )
        post_notional = contract_notional(
            capped_contracts,
            float(px),
            multiplier,
            price_ccy=price_ccy,
            account_ccy="USD",
            fx_rates=fx_rates,
        )
        post_signed_weight = (post_notional / float(equity)) * (1.0 if signed_weight >= 0.0 else -1.0)
        if abs(float(post_signed_weight) - float(signed_weight)) > 1e-12:
            row = _row_with_signed_weight(row, float(post_signed_weight), "futures_margin_cap")

        futures_meta = {
            "asset_class": "FUTURES",
            "pre_weight": float(signed_weight),
            "post_weight": float(post_signed_weight),
            "price": float(px),
            "price_ccy": price_ccy,
            "fx_rate": float(fx_rate),
            "multiplier": float(multiplier),
            "desired_contracts": int(desired_contracts),
            "contracts": int(capped_contracts),
            "reference_margin": float(reference_margin),
            "regulatory_or_broker_margin": (float(broker_margin) if broker_margin is not None else None),
            "notional": float(post_notional),
            "margin": dict(margin_meta),
        }
        row.setdefault("reason", {})
        if not isinstance(row.get("reason"), dict):
            row["reason"] = {"raw": row.get("reason")}
        row["reason"]["futures_margin_cap"] = dict(futures_meta)
        row["futures"] = dict(futures_meta)
        out[sym] = row
        adjustments[sym] = dict(futures_meta)

    if adjustments:
        info["futures_margin_adjustments"] = adjustments
    if hard_blocks:
        info["futures_margin_hard_blocks"] = hard_blocks
    return out


def _symbol_vol_input(con, symbol: str, *, ts_ms: int) -> Dict[str, Any]:
    try:
        resolved = resolve_vol_forecast(
            con,
            str(symbol),
            ts_ms=int(ts_ms),
            source=str(VOL_FORECAST_SOURCE or "trailing"),
            trailing_lookback=int(VOL_LOOKBACK),
        )
        vol = resolved.get("vol")
        if vol is None:
            return {"vol": None, "source": "missing"}
        return {
            "vol": float(vol),
            "source": str(resolved.get("resolved_source") or resolved.get("source") or VOL_FORECAST_SOURCE),
            "forecast_ratio": resolved.get("forecast_ratio"),
            "forecast_ts_ms": resolved.get("ts_ms"),
            "fallback": bool(resolved.get("fallback", False)),
        }
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_VOL_FORECAST_RESOLVE_FAILED",
            e,
            once_key=f"vol_forecast:{symbol}",
            symbol=str(symbol),
            source=str(VOL_FORECAST_SOURCE),
        )
        try:
            v = realized_vol_from_prices(con, str(symbol), lookback=int(VOL_LOOKBACK))
        except Exception:
            v = None
        return {"vol": (None if v is None else float(v)), "source": "trailing_exception_fallback", "fallback": True}


def _apply_symbol_vol_caps(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_VOL_CAPS:
        return dict(desired or {})

    out = dict(desired or {})
    vol_map: Dict[str, float] = {}
    vol_meta: Dict[str, Dict[str, Any]] = {}

    for sym in list(out.keys()):
        try:
            resolved = _symbol_vol_input(con, str(sym), ts_ms=int(info.get("ts_ms") or 0))
            v = resolved.get("vol")
            if v is None:
                continue
            vv = float(v)
            vv = max(float(PORTFOLIO_VOL_FLOOR), min(float(PORTFOLIO_VOL_CEIL), vv))
            vol_map[str(sym)] = float(vv)
            vol_meta[str(sym)] = dict(resolved)
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_RISK_ENGINE_VOL_CAP_ROW_FAILED",
                e,
                symbol=str(sym),
            )
            continue

    info["symbol_vol_n"] = int(len(vol_map))
    info["symbol_max_gross"] = float(MAX_SYMBOL_GROSS)
    info["vol_forecast_source"] = str(VOL_FORECAST_SOURCE or "trailing")
    if vol_meta:
        info["symbol_vol_inputs"] = dict(vol_meta)

    hit: Dict[str, Any] = {}
    for sym, row in list(out.items()):
        sw = _signed_weight(row)
        aw = abs(sw)
        if aw <= 0.0:
            continue

        v = vol_map.get(str(sym))
        meta = vol_meta.get(str(sym), {})
        mult = 1.0
        cap_source = "fallback_max_symbol_gross"

        if v is None or v <= 1e-12:
            cap = float(MAX_SYMBOL_GROSS) if float(MAX_SYMBOL_GROSS) > 0.0 else float(SYMBOL_CAP_MAX_W)
        else:
            mult = float(VOL_TARGET) / float(v)
            mult = max(float(SYMBOL_CAP_MIN_MULT), min(1.0, float(mult)))
            cap = min(float(SYMBOL_CAP_MAX_W), float(SYMBOL_CAP_MAX_W) * float(mult))
            if float(MAX_SYMBOL_GROSS) > 0.0:
                cap = min(float(cap), float(MAX_SYMBOL_GROSS))
            cap_source = str(meta.get("source") or VOL_FORECAST_SOURCE or "realized_vol")

        if cap <= 0.0:
            continue
        if aw > cap + 1e-12:
            scale = float(cap) / float(aw) if aw > 1e-12 else 0.0
            sgn = 1.0 if sw >= 0.0 else -1.0
            out[sym]["weight"] = float(abs(sw) * scale) * float(sgn)
            out[sym].setdefault("reason", {})
            if isinstance(out[sym]["reason"], dict):
                out[sym]["reason"]["symbol_vol_cap"] = {
                    "vol": (float(v) if v is not None else None),
                    "target": float(VOL_TARGET),
                    "mult": float(mult),
                    "cap": float(cap),
                    "pre": float(aw),
                    "scale": float(scale),
                    "cap_source": str(cap_source),
                    "forecast_ratio": meta.get("forecast_ratio"),
                }
            hit[str(sym)] = {
                "vol": (float(v) if v is not None else None),
                "cap": float(cap),
                "pre": float(aw),
                "scale": float(scale),
                "mult": float(mult),
                "cap_source": str(cap_source),
                "forecast_ratio": meta.get("forecast_ratio"),
            }

    if hit:
        info["symbol_vol_caps_hit"] = hit

    return out


def _corr_graph_components(
    con,
    syms: List[str],
) -> Tuple[List[List[str]], Dict[str, Dict[str, float]], List[Dict[str, Any]], Dict[str, Any]]:
    if not syms or len(syms) < 2:
        return [], {}, [], {}

    # Build adjacency (undirected) for abs(corr) >= threshold
    adj: Dict[str, List[str]] = {s: [] for s in syms}
    matrix: Dict[str, Dict[str, float]] = {s: {} for s in syms}
    covariance_diagnostics: Dict[str, Any] = {}
    corr_by_symbol: Dict[str, Dict[str, float]] = {}
    try:
        covariance_estimate = estimate_covariance(con, syms, lookback=int(CORR_LOOKBACK))
        covariance_diagnostics = dict(covariance_estimate.diagnostics or {})
        corr_by_symbol = correlation_matrix_dict(covariance_estimate)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_CLUSTER_COVARIANCE_FAILED",
            e,
            once_key="cluster_covariance",
            symbols=list(syms),
        )
    for i in range(len(syms)):
        matrix[syms[i]][syms[i]] = 1.0
        for j in range(i + 1, len(syms)):
            a, b = syms[i], syms[j]
            try:
                c = corr_by_symbol.get(a, {}).get(b)
                if c is None:
                    c = corr_from_prices(con, a, b, lookback=int(CORR_LOOKBACK))
                if c is None:
                    continue
                cc = max(-1.0, min(1.0, _safe_float(c, 0.0)))
                matrix[a][b] = float(cc)
                matrix[b][a] = float(cc)
                if abs(float(cc)) >= float(CLUSTER_CORR_TH):
                    adj[a].append(b)
                    adj[b].append(a)
            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_RISK_ENGINE_CLUSTER_CORR_ROW_FAILED",
                    e,
                    left_symbol=str(a),
                    right_symbol=str(b),
                )
                continue

    fx_shared_edges: List[Dict[str, Any]] = []
    if USE_FX_CURRENCY_CLUSTERS:
        fx_ccys: Dict[str, set[str]] = {}
        for s in syms:
            try:
                inst = _fx_instrument(con, str(s))
                if not isinstance(inst, dict) or str(inst.get("asset_class") or "").upper() != "FX":
                    continue
                ccys = {
                    str(inst.get("base_ccy") or "").upper().strip(),
                    str(inst.get("quote_ccy") or "").upper().strip(),
                }
                ccys.discard("")
                if ccys:
                    fx_ccys[str(s)] = ccys
            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_RISK_FX_CLUSTER_INSTRUMENT_FAILED",
                    e,
                    once_key=f"fx_cluster_instrument:{s}",
                    symbol=str(s),
                )

        fx_symbols = sorted(fx_ccys.keys())
        for i in range(len(fx_symbols)):
            for j in range(i + 1, len(fx_symbols)):
                a, b = fx_symbols[i], fx_symbols[j]
                shared = sorted(fx_ccys.get(a, set()) & fx_ccys.get(b, set()))
                if not shared:
                    continue
                if b not in adj.get(a, []):
                    adj[a].append(b)
                if a not in adj.get(b, []):
                    adj[b].append(a)
                fx_shared_edges.append({"left": a, "right": b, "shared_currency": shared})

    # DFS components
    seen = set()
    comps: List[List[str]] = []
    for s in syms:
        if s in seen:
            continue
        stack = [s]
        comp = []
        seen.add(s)
        while stack:
            x = stack.pop()
            comp.append(x)
            for y in adj.get(x, []):
                if y not in seen:
                    seen.add(y)
                    stack.append(y)
        if len(comp) >= 2:
            comps.append(sorted(comp))

    comps.sort(key=lambda comp: (-len(comp), ",".join(comp)))
    return comps, matrix, fx_shared_edges, covariance_diagnostics


def _apply_corr_cluster_caps(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_CORR_CLUSTERS:
        return dict(desired or {})

    out = dict(desired or {})
    syms = _top_symbols_by_abs(out, int(MAX_SYMBOLS))
    comps, corr_matrix, fx_shared_edges, covariance_diagnostics = _corr_graph_components(con, syms)

    info["corr_matrix"] = corr_matrix
    info["corr_cluster_symbols"] = list(syms)
    info["corr_cluster_corr_th"] = float(CLUSTER_CORR_TH)
    if covariance_diagnostics:
        info["corr_covariance_diagnostics"] = dict(covariance_diagnostics)
    if fx_shared_edges:
        info["corr_cluster_fx_shared_currency_edges"] = list(fx_shared_edges)

    cluster_exposures = []
    for comp in comps:
        gross = 0.0
        net = 0.0
        for s in comp:
            gross += _abs_exposure_weight(con, str(s), out.get(s))
            net += _signed_exposure_weight(con, str(s), out.get(s))
        comp_set = set(comp)
        comp_fx_edges = [
            edge
            for edge in fx_shared_edges
            if str(edge.get("left")) in comp_set and str(edge.get("right")) in comp_set
        ]
        cluster_exposures.append(
            {
                "cluster": list(comp),
                "gross": float(gross),
                "net": float(net),
                "cap": float(CLUSTER_MAX_GROSS),
                "fx_shared_currency": comp_fx_edges,
            }
        )
    if cluster_exposures:
        info["corr_cluster_exposures"] = cluster_exposures

    # bound number of components (worst-case compute)
    if CLUSTER_MAX_COMPONENTS > 0 and len(comps) > int(CLUSTER_MAX_COMPONENTS):
        comps = comps[: int(CLUSTER_MAX_COMPONENTS)]

    hit = []
    for comp in comps:
        gross = 0.0
        net = 0.0
        for s in comp:
            gross += _abs_exposure_weight(con, str(s), out.get(s))
            net += _signed_exposure_weight(con, str(s), out.get(s))
        if gross <= float(CLUSTER_MAX_GROSS) + 1e-12:
            continue

        comp_set = set(comp)
        comp_fx_edges = [
            edge
            for edge in fx_shared_edges
            if str(edge.get("left")) in comp_set and str(edge.get("right")) in comp_set
        ]
        scale = float(CLUSTER_MAX_GROSS) / float(gross) if gross > 1e-12 else 0.0
        for s in comp:
            try:
                sw = _signed_weight(out.get(s))
                sgn = 1.0 if sw >= 0.0 else -1.0
                out[s]["weight"] = float(abs(sw) * scale) * float(sgn)
                out[s].setdefault("reason", {})
                if isinstance(out[s]["reason"], dict):
                    out[s]["reason"]["corr_cluster_cap"] = {
                        "cluster": list(comp),
                        "gross_pre": float(gross),
                        "net_pre": float(net),
                        "cap": float(CLUSTER_MAX_GROSS),
                        "scale": float(scale),
                        "corr_th": float(CLUSTER_CORR_TH),
                        "fx_shared_currency": comp_fx_edges,
                    }
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_CORR_CLUSTER_APPLY_FAILED", e, once_key=f"corr_cluster:{s}", symbol=str(s), cluster=list(comp))

        hit.append(
            {
                "cluster": list(comp),
                "gross_pre": float(gross),
                "net_pre": float(net),
                "cap": float(CLUSTER_MAX_GROSS),
                "scale": float(scale),
                "fx_shared_currency": comp_fx_edges,
            }
        )

    if hit:
        info["corr_cluster_caps_hit"] = hit

    return out


def _portfolio_vol_proxy(con, desired: Dict[str, Dict[str, Any]], info: Optional[Dict[str, Any]] = None) -> Optional[float]:
    syms = _top_symbols_by_abs(desired, int(MAX_SYMBOLS))
    if not syms:
        return None

    # signed exposure weights; futures are scaled by contract multiplier.
    active_syms: List[str] = []
    w: List[float] = []
    vols: List[float] = []
    vol_inputs: Dict[str, Dict[str, Any]] = {}
    for s in syms:
        row = (desired or {}).get(s) or {}
        sw = _signed_exposure_weight(con, str(s), row)
        aw = abs(sw)
        if aw <= 0.0:
            continue
        try:
            resolved = _symbol_vol_input(con, str(s), ts_ms=int((info or {}).get("ts_ms") or 0))
            v = resolved.get("vol")
            if v is None:
                return None
            vv = float(v)
            vv = max(float(PORTFOLIO_VOL_FLOOR), min(float(PORTFOLIO_VOL_CEIL), vv))
            vol_inputs[str(s)] = dict(resolved)
        except Exception as e:
            _warn_nonfatal(
                "PORTFOLIO_RISK_ENGINE_PORTFOLIO_VOL_SYMBOL_FAILED",
                e,
                symbol=str(s),
            )
            portfolio_vol = None
            return portfolio_vol
        active_syms.append(str(s))
        w.append(float(sw))
        vols.append(float(vv))

    if not w:
        return None
    if info is not None:
        info["portfolio_vol_inputs"] = dict(vol_inputs)
        info["vol_forecast_source"] = str(VOL_FORECAST_SOURCE or "trailing")

    # normalize by gross to avoid pathological scaling
    gross = sum(abs(x) for x in w)
    if gross <= 1e-12:
        return None
    wn = [float(x / gross) for x in w]

    covariance_diagnostics: Dict[str, Any] = {}
    corr_by_symbol: Dict[str, Dict[str, float]] = {}
    try:
        covariance_estimate = estimate_covariance(con, active_syms, lookback=int(CORR_LOOKBACK))
        covariance_diagnostics = dict(covariance_estimate.diagnostics or {})
        corr_by_symbol = correlation_matrix_dict(covariance_estimate)
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_ENGINE_PORTFOLIO_VOL_COVARIANCE_FAILED",
            e,
            once_key="portfolio_vol_covariance",
            symbols=list(active_syms),
        )
    if info is not None and covariance_diagnostics:
        info["portfolio_covariance_diagnostics"] = dict(covariance_diagnostics)

    # covariance proxy via canonical covariance correlations and forecast vols
    var = 0.0
    for i in range(len(wn)):
        var += float(wn[i] * wn[i]) * float(vols[i] * vols[i])

    # pairwise cov
    for i in range(len(wn)):
        for j in range(i + 1, len(wn)):
            try:
                c = corr_by_symbol.get(active_syms[i], {}).get(active_syms[j])
                if c is None:
                    c = corr_from_prices(con, active_syms[i], active_syms[j], lookback=int(CORR_LOOKBACK))
                if c is None:
                    continue
                cc = max(-1.0, min(1.0, _safe_float(c, 0.0)))
                cov = float(vols[i]) * float(vols[j]) * float(cc)
                var += 2.0 * float(wn[i]) * float(wn[j]) * float(cov)
            except Exception as e:
                _warn_nonfatal(
                    "PORTFOLIO_RISK_ENGINE_PORTFOLIO_VOL_PAIR_FAILED",
                    e,
                    left_symbol=str(active_syms[i]),
                    right_symbol=str(active_syms[j]),
                )
                continue

    if var < 0.0:
        var = 0.0
    return float(var ** 0.5)


def _latest_options_gex_for_symbol(con, symbol: str, ts_ms: int) -> Optional[Dict[str, float]]:
    try:
        row = con.execute(
            """
            SELECT snapshot_ts_ms, gex_norm, gex_norm_z, gex_sign
            FROM options_symbol_features
            WHERE symbol=?
              AND snapshot_ts_ms <= ?
            ORDER BY snapshot_ts_ms DESC, bucket_ts_ms DESC
            LIMIT 1
            """,
            (str(symbol), int(ts_ms)),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_GEX_LOOKUP_FAILED",
            e,
            once_key=f"gex_lookup:{symbol}",
            symbol=str(symbol),
            ts_ms=int(ts_ms),
        )
        return None
    if not row:
        return None
    return {
        "snapshot_ts_ms": float(_safe_float(row[0], 0.0)),
        "gex_norm": float(_safe_float(row[1], 0.0)),
        "gex_norm_z": float(max(-10.0, min(10.0, _safe_float(row[2], 0.0)))),
        "gex_sign": float(max(-1.0, min(1.0, _safe_float(row[3], 0.0)))),
    }


def _gex_vol_target_modifier(con, desired: Dict[str, Dict[str, Any]], ts_ms: int) -> Dict[str, Any]:
    """Return a volatility-regime modifier from GEX, never a direction signal."""

    if not USE_GEX_VOL_MODIFIER:
        return {"enabled": False, "modifier": 1.0}
    weighted_norm = 0.0
    weighted_z = 0.0
    total_weight = 0.0
    rows: Dict[str, Dict[str, float]] = {}
    for sym, row in (desired or {}).items():
        abs_w = abs(_signed_weight(row))
        if abs_w <= 0.0:
            continue
        gex = _latest_options_gex_for_symbol(con, str(sym), int(ts_ms))
        if not gex:
            continue
        weighted_norm += float(abs_w) * float(gex.get("gex_norm", 0.0))
        weighted_z += float(abs_w) * float(gex.get("gex_norm_z", 0.0))
        total_weight += float(abs_w)
        rows[str(sym)] = dict(gex)
    if total_weight <= 0.0:
        return {"enabled": True, "modifier": 1.0, "symbols": rows, "coverage": 0.0}
    avg_norm = float(weighted_norm / total_weight)
    avg_z = float(weighted_z / total_weight)
    negative_pressure = max(0.0, min(1.0, -avg_z / 2.0))
    positive_damping = max(0.0, min(1.0, avg_z / 2.0))
    modifier = float(max(0.75, min(1.10, 1.0 - 0.12 * negative_pressure + 0.04 * positive_damping)))
    return {
        "enabled": True,
        "modifier": float(modifier),
        "gex_norm": float(avg_norm),
        "gex_norm_z": float(avg_z),
        "coverage": float(total_weight / max(1e-9, _gross(desired or {}))),
        "symbols": rows,
        "usage": "volatility_regime_conditioning_not_direction",
    }


def _apply_portfolio_vol_target(con, desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out = dict(desired or {})
    pv = _portfolio_vol_proxy(con, out, info)
    if pv is None:
        return out

    info["portfolio_vol_proxy"] = float(pv)
    gex_modifier = _gex_vol_target_modifier(con, out, int(info.get("ts_ms") or 0))
    effective_target = float(VOL_TARGET) * float(gex_modifier.get("modifier", 1.0) or 1.0)
    info["portfolio_vol_target"] = float(VOL_TARGET)
    info["portfolio_vol_effective_target"] = float(effective_target)
    info["portfolio_gex_vol_modifier"] = dict(gex_modifier)
    info["portfolio_vol_hard_block"] = float(PORTFOLIO_VOL_HARD_BLOCK)

    # Hard block if configured
    if float(PORTFOLIO_VOL_HARD_BLOCK) > 0.0 and float(pv) >= float(PORTFOLIO_VOL_HARD_BLOCK):
        info["portfolio_vol_hard_block_hit"] = True
        return out

    # Scale entire portfolio to target (if pv > target)
    if float(effective_target) > 0.0 and float(pv) > float(effective_target) + 1e-12:
        scale = float(effective_target) / float(pv) if pv > 1e-12 else 0.0
        for sym in list(out.keys()):
            try:
                sw = _signed_weight(out[sym])
                out[sym]["weight"] = float(sw) * float(scale)
                out[sym].setdefault("reason", {})
                if isinstance(out[sym]["reason"], dict):
                    out[sym]["reason"]["portfolio_vol_target"] = {
                        "pre_vol": float(pv),
                        "target": float(effective_target),
                        "base_target": float(VOL_TARGET),
                        "scale": float(scale),
                        "vol_source": str(
                            ((info.get("portfolio_vol_inputs") or {}).get(str(sym)) or {}).get("source")
                            or VOL_FORECAST_SOURCE
                            or "trailing"
                        ),
                        "gex_modifier": float(gex_modifier.get("modifier", 1.0) or 1.0),
                    }
            except Exception as e:
                _warn_nonfatal("PORTFOLIO_RISK_VOL_TARGET_APPLY_FAILED", e, once_key=f"vol_target:{sym}", symbol=str(sym))
        info["portfolio_vol_scaled"] = True
        info["portfolio_vol_scale"] = float(scale)

    return out


def _persist_snapshot(con, now_ms: int, info: Dict[str, Any]) -> None:
    try:
        con.execute(
            """
            INSERT OR REPLACE INTO portfolio_risk_snapshots(
              ts_ms, gross, net, vol_proxy, drawdown, blocked, info_json
            )
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                int(now_ms),
                float(info.get("final_gross", 0.0) or 0.0),
                float(info.get("final_net", 0.0) or 0.0),
                _optional_float(info.get("portfolio_vol_proxy")),
                _optional_float(info.get("drawdown")),
                (1 if bool(info.get("blocked", False)) else 0),
                json.dumps(info or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_SNAPSHOT_PERSIST_FAILED", e, once_key="persist_snapshot", ts_ms=int(now_ms))

    if not bool(info.get("blocked", False)):
        return

    try:
        block_reason = dict(info.get("block_reason") or {})
        con.execute(
            """
            INSERT INTO risk_events(
              ts_ms, trigger_type, reason, equity, drawdown_pct, var_pct, concentration, positions, metadata_json
            )
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                int(now_ms),
                str(block_reason.get("type") or "portfolio_risk_block"),
                str(block_reason.get("type") or "portfolio_risk_block"),
                _optional_float((info.get("live_positions") or {}).get("equity_ref")),
                _optional_float(info.get("drawdown")),
                _optional_float(info.get("portfolio_vol_proxy")),
                _optional_float(info.get("final_gross")),
                int(len(dict((info.get("target_exposure_post") or {}).get("by_symbol") or {}))),
                json.dumps(info or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_EVENT_PERSIST_FAILED", e, once_key="persist_risk_event", ts_ms=int(now_ms))


def apply_portfolio_risk_engine(
    con,
    desired: Dict[str, Dict[str, Any]],
    state: Dict[str, Dict[str, Any]],
    now_ms: int,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Apply portfolio-level risk limits to desired allocations.

    Parameters
    ----------
    con : storage connection
        Open runtime database connection used for prices, live positions, and
        persistence of risk state.
    desired : dict
        Proposed target allocations before portfolio-risk adjustment. The values
        are strategy/position payloads consumed by the portfolio engine's
        exposure helpers.
    state : dict
        Current portfolio state in the same structural shape as ``desired``.
    now_ms : int
        Evaluation timestamp in epoch milliseconds.

    Returns
    -------
    tuple[dict, dict]
        Two-tuple ``(adjusted_desired, info)``. ``adjusted_desired`` is the
        post-risk target map, and ``info`` contains the detailed risk summary
        that is also persisted for dashboards and audit trails.

    Notes
    -----
    Drawdown thresholds are fractions of equity rather than percentage
    integers. Hard blocks can be triggered by drawdown, portfolio volatility,
    Monte Carlo summaries, or post-cap validation. When the engine is disabled,
    the input allocations are returned unchanged with ``{"enabled": False}``.

    Side Effects
    ------------
    Updates ``portfolio_risk_*`` keys in runtime risk state, records a risk
    block event, persists a snapshot row, and annotates returned allocations
    with portfolio-risk metadata.
    """
    if not USE:
        try:
            ts_ms = int(now_ms)
        except Exception:
            ts_ms = 0
        try:
            is_live = not live_execution_disabled()
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_LIVE_STATUS_READ_FAILED", e, once_key="disable_live_status")
            is_live = True
        blocked = bool(is_live and not BACKSTOP_ENABLED)
        status = "risk_engine_disabled_live" if blocked else "disabled"
        clear_info = {
            "enabled": False,
            "ts_ms": int(ts_ms),
            "blocked": bool(blocked),
            "status": status,
            "is_live": bool(is_live),
            "notional_backstop_enabled": bool(BACKSTOP_ENABLED),
        }
        try:
            set_state("portfolio_risk_block", "1" if blocked else "0")
            state_blob = json.dumps(clear_info, separators=(",", ":"), sort_keys=True)
            set_state("portfolio_risk_info", state_blob)
            set_state("portfolio_risk_summary", state_blob)
            set_state("portfolio_risk_status", status)
            set_state("portfolio_risk_ts_ms", str(int(ts_ms)))
        except Exception as e:
            _warn_nonfatal("PORTFOLIO_RISK_DISABLE_STATE_SET_FAILED", e, once_key="disable_state_set", ts_ms=int(ts_ms))
        return desired, dict(clear_info)

    info: Dict[str, Any] = {"enabled": True, "ts_ms": int(now_ms)}

    drawdown_diagnostic = evaluate_current_drawdown(con)
    info["drawdown_state"] = drawdown_diagnostic.to_dict()
    dd = float(drawdown_diagnostic.drawdown or 0.0) if drawdown_diagnostic.ok else 0.0
    info["drawdown"] = (float(dd) if drawdown_diagnostic.ok else None)

    state_snapshot = _exposure_snapshot(state or {}, con)
    info["state_exposure"] = state_snapshot

    live_rows, live_info = _load_live_positions(con)
    live_snapshot = _exposure_snapshot(live_rows or {}, con)
    info["live_positions"] = dict(live_info or {})
    info["live_exposure"] = live_snapshot
    info["current_exposure"] = live_snapshot if live_rows else state_snapshot
    info["cur_gross"] = float(info["current_exposure"].get("gross", 0.0) or 0.0)
    info["cur_net"] = float(info["current_exposure"].get("net", 0.0) or 0.0)

    raw_desired = dict(desired or {})
    info["desired_exposure_raw"] = _exposure_snapshot(raw_desired, con)
    info["state_to_desired_delta"] = _delta_snapshot(state or {}, raw_desired)

    out = _project_live_plus_orders(live_rows or {}, state or {}, raw_desired)
    info["asset_class_by_symbol"] = _asset_class_lookup(con, out)
    info["projected_live_plus_orders_pre"] = _exposure_snapshot(out, con)
    info["target_exposure_pre"] = dict(info["projected_live_plus_orders_pre"])
    info["target_delta_pre"] = _delta_snapshot(live_rows or {}, out)

    blocked = False
    block_reason: Optional[Dict[str, Any]] = None

    if not drawdown_diagnostic.ok:
        blocked = True
        block_reason = {
            "type": "drawdown_state_unavailable",
            "reason_code": str(drawdown_diagnostic.reason_code),
            "drawdown_state": drawdown_diagnostic.to_dict(),
        }

    if drawdown_diagnostic.ok and float(DD_HARD_BLOCK) > 0.0 and float(dd) >= float(DD_HARD_BLOCK):
        blocked = True
        block_reason = {"type": "drawdown_hard_block", "dd": float(dd), "threshold": float(DD_HARD_BLOCK)}

    try:
        out = _apply_drawdown_throttle(out, float(dd), info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_DRAWDOWN_THROTTLE_FAILED", e, once_key="apply_drawdown_throttle", ts_ms=int(now_ms))
        overlay_block_reason = _record_overlay_failure(info, "drawdown_throttle", e, now_ms=int(now_ms))
        if overlay_block_reason is not None:
            blocked = True
            if block_reason is None or str(block_reason.get("type") or "") == "required_overlay_failed":
                block_reason = dict(overlay_block_reason)

    try:
        info["_risk_con"] = con
        out = _apply_asset_class_budgets(out, info)
        info["asset_class_by_symbol"] = _asset_class_lookup(con, out)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_ASSET_CLASS_BUDGETS_FAILED", e, once_key="apply_asset_class_budgets", ts_ms=int(now_ms))
    finally:
        info.pop("_risk_con", None)

    try:
        out = _apply_futures_margin_caps(con, out, info)
        if bool(info.get("futures_margin_hard_blocks")):
            blocked = True
            block_reason = {
                "type": "futures_margin_hard_block",
                "legs": list(info.get("futures_margin_hard_blocks") or []),
            }
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_FUTURES_MARGIN_CAPS_FAILED", e, once_key="apply_futures_margin_caps", ts_ms=int(now_ms))

    try:
        out = _apply_fx_leverage_caps(con, out, info)
        if bool(info.get("fx_leverage_hard_blocks")):
            blocked = True
            block_reason = {
                "type": "fx_leverage_hard_block",
                "legs": list(info.get("fx_leverage_hard_blocks") or []),
            }
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_FX_LEVERAGE_CAPS_FAILED", e, once_key="apply_fx_leverage_caps", ts_ms=int(now_ms))

    try:
        out = _apply_equity_leverage_caps(con, out, info)
        if bool(info.get("equity_leverage_hard_blocks")):
            blocked = True
            block_reason = {
                "type": "equity_leverage_hard_block",
                "legs": list(info.get("equity_leverage_hard_blocks") or []),
            }
    except Exception as e:
        _warn_nonfatal(
            "PORTFOLIO_RISK_EQUITY_LEVERAGE_CAPS_FAILED",
            e,
            once_key="apply_equity_leverage_caps",
            ts_ms=int(now_ms),
        )

    try:
        out = _apply_crypto_leverage_caps(con, out, info)
        if bool(info.get("crypto_leverage_hard_blocks")):
            blocked = True
            block_reason = {
                "type": "crypto_leverage_hard_block",
                "legs": list(info.get("crypto_leverage_hard_blocks") or []),
            }
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_CRYPTO_LEVERAGE_CAPS_FAILED", e, once_key="apply_crypto_leverage_caps", ts_ms=int(now_ms))

    try:
        out = _apply_sector_budgets(con, out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_SECTOR_BUDGETS_FAILED", e, once_key="apply_sector_budgets", ts_ms=int(now_ms))

    try:
        info["_risk_con"] = con
        out = _apply_strategy_budgets(out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_STRATEGY_BUDGETS_FAILED", e, once_key="apply_strategy_budgets", ts_ms=int(now_ms))
    finally:
        info.pop("_risk_con", None)

    try:
        out = _apply_alpha_decay_throttle(con, out, info, int(now_ms))
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_ALPHA_DECAY_THROTTLE_FAILED", e, once_key="apply_alpha_decay_throttle", ts_ms=int(now_ms))
        overlay_block_reason = _record_overlay_failure(info, "alpha_decay_throttle", e, now_ms=int(now_ms))
        if overlay_block_reason is not None:
            blocked = True
            if block_reason is None or str(block_reason.get("type") or "") == "required_overlay_failed":
                block_reason = dict(overlay_block_reason)

    try:
        out = _apply_symbol_vol_caps(con, out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_SYMBOL_VOL_CAPS_FAILED", e, once_key="apply_symbol_vol_caps", ts_ms=int(now_ms))
        overlay_block_reason = _record_overlay_failure(info, "symbol_vol_caps", e, now_ms=int(now_ms))
        if overlay_block_reason is not None:
            blocked = True
            if block_reason is None or str(block_reason.get("type") or "") == "required_overlay_failed":
                block_reason = dict(overlay_block_reason)

    try:
        out = _apply_corr_cluster_caps(con, out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_CORR_CLUSTER_CAPS_FAILED", e, once_key="apply_corr_cluster_caps", ts_ms=int(now_ms))
        overlay_block_reason = _record_overlay_failure(info, "corr_cluster_caps", e, now_ms=int(now_ms))
        if overlay_block_reason is not None:
            blocked = True
            if block_reason is None or str(block_reason.get("type") or "") == "required_overlay_failed":
                block_reason = dict(overlay_block_reason)

    try:
        out = _apply_portfolio_vol_target(con, out, info)
        if bool(info.get("portfolio_vol_hard_block_hit", False)):
            blocked = True
            block_reason = {
                "type": "portfolio_vol_hard_block",
                "vol": float(info.get("portfolio_vol_proxy", 0.0) or 0.0),
                "threshold": float(PORTFOLIO_VOL_HARD_BLOCK),
            }
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_VOL_TARGET_FAILED", e, once_key="apply_portfolio_vol_target", ts_ms=int(now_ms))
        overlay_block_reason = _record_overlay_failure(info, "portfolio_vol_target", e, now_ms=int(now_ms))
        if overlay_block_reason is not None:
            blocked = True
            if block_reason is None or str(block_reason.get("type") or "") == "required_overlay_failed":
                block_reason = dict(overlay_block_reason)

    try:
        mc_info = _load_monte_carlo_risk_summary(int(now_ms))
        if mc_info:
            info["monte_carlo_risk"] = mc_info
            if bool(mc_info.get("blocked", False)):
                blocked = True
                block_reason = {
                    "type": "monte_carlo_risk_block",
                    "monte_carlo": dict(mc_info),
                }
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_MONTE_CARLO_SUMMARY_FAILED", e, once_key="monte_carlo_summary", ts_ms=int(now_ms))

    try:
        info["_risk_con"] = con
        out = _apply_portfolio_caps(out, info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_PORTFOLIO_CAPS_FAILED", e, once_key="apply_portfolio_caps", ts_ms=int(now_ms))
    finally:
        info.pop("_risk_con", None)

    info["asset_class_by_symbol"] = _asset_class_lookup(con, out)
    final_snapshot = _exposure_snapshot(out, con)
    options_greek_snapshot = _options_greek_snapshot(con, out)
    final_snapshot["options_greeks"] = dict(options_greek_snapshot)
    info["projected_live_plus_orders_post"] = final_snapshot
    info["target_exposure_post"] = final_snapshot
    info["options_greeks_post"] = dict(options_greek_snapshot)
    info["target_delta_post"] = _delta_snapshot(live_rows or {}, out)
    info["state_to_target_delta_post"] = _delta_snapshot(state or {}, _projected_to_desired_targets(out, live_rows or {}, state or {}))
    info["final_gross"] = float(final_snapshot.get("gross", 0.0) or 0.0)
    info["final_net"] = float(final_snapshot.get("net", 0.0) or 0.0)

    post_checks = _post_constraint_checks(final_snapshot)
    if info.get("required_overlay_failures"):
        post_checks["required_overlays_ok"] = False
        post_checks["required_overlay_failures"] = list(info.get("required_overlay_failures") or [])
    info["post_checks"] = post_checks

    if (not blocked) and (not all(bool(v) for k, v in post_checks.items() if not str(k).endswith("_violations"))):
        blocked = True
        if not bool(post_checks.get("required_overlays_ok", True)):
            block_reason = {
                "type": "required_overlay_failed",
                "failures": list(post_checks.get("required_overlay_failures") or []),
            }
        elif not bool(post_checks.get("options_greeks_within_cap", True)):
            block_reason = {
                "type": "options_greek_limit_breached",
                "options_greeks": dict(options_greek_snapshot),
                "options_greek_violations": dict(post_checks.get("options_greek_violations") or {}),
                "checks": dict(post_checks),
            }
        else:
            block_reason = {
                "type": "post_cap_validation_failed",
                "final_gross": float(info["final_gross"]),
                "final_net": float(info["final_net"]),
                "max_gross": float(MAX_GROSS),
                "max_net": float(MAX_NET),
                "checks": dict(post_checks),
            }

    out = _projected_to_desired_targets(out, live_rows or {}, state or {})
    info["asset_class_by_symbol"] = _asset_class_lookup(con, out)
    info["desired_exposure_post"] = _exposure_snapshot(out, con)
    info["allocation_reconciliation"] = _reconciliation_summary(
        dict(info.get("desired_exposure_raw") or {}),
        dict(info.get("desired_exposure_post") or {}),
    )

    info["blocked"] = bool(blocked)
    if block_reason is not None:
        info["block_reason"] = dict(block_reason)

    try:
        state_blob = json.dumps(info or {}, separators=(",", ":"), sort_keys=True)
        summary_blob = json.dumps(
            {
                "enabled": True,
                "ts_ms": int(now_ms),
                "blocked": bool(blocked),
                "block_reason": (dict(block_reason) if block_reason is not None else None),
                "drawdown": float(info.get("drawdown", 0.0) or 0.0),
                "cur_gross": float(info.get("cur_gross", 0.0) or 0.0),
                "cur_net": float(info.get("cur_net", 0.0) or 0.0),
                "final_gross": float(info.get("final_gross", 0.0) or 0.0),
                "final_net": float(info.get("final_net", 0.0) or 0.0),
                "portfolio_vol_proxy": info.get("portfolio_vol_proxy"),
            },
            separators=(",", ":"),
            sort_keys=True,
        )

        set_state("portfolio_risk_block", "1" if blocked else "0")
        set_state("portfolio_risk_info", state_blob)
        set_state("portfolio_risk_summary", summary_blob)
        set_state("portfolio_risk_status", ("blocked" if blocked else "clear"))
        set_state("portfolio_risk_ts_ms", str(int(now_ms)))
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_STATE_SET_FAILED", e, once_key="state_set", ts_ms=int(now_ms), blocked=bool(blocked))

    try:
        record_risk_block(
            name="portfolio_risk_engine",
            blocked=bool(blocked),
            info=info,
            ts_ms=int(now_ms),
            con=con,
        )
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_RECORD_BLOCK_FAILED", e, once_key="record_risk_block", ts_ms=int(now_ms), blocked=bool(blocked))

    try:
        _persist_snapshot(con, int(now_ms), info)
    except Exception as e:
        _warn_nonfatal("PORTFOLIO_RISK_PERSIST_SNAPSHOT_FAILED", e, once_key="persist_snapshot_call", ts_ms=int(now_ms))

    _annotate(out, info)
    return out, info

def _apply_strategy_budgets(desired: Dict[str, Dict[str, Any]], info: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    if not USE_STRATEGY_BUDGETS:
        return dict(desired or {})

    out = dict(desired or {})
    exposure_con = info.get("_risk_con")

    strat_gross: Dict[str, float] = {}
    strat_net: Dict[str, float] = {}

    for sym, row in (out or {}).items():
        sid = _strategy_bucket_for_row(row)
        sw = _signed_exposure_weight(exposure_con, str(sym), row)
        strat_gross[sid] = float(strat_gross.get(sid, 0.0) + abs(sw))
        strat_net[sid] = float(strat_net.get(sid, 0.0) + sw)

    info["strategy_gross_pre"] = dict(strat_gross)
    info["strategy_net_pre"] = dict(strat_net)

    hit: Dict[str, Any] = {}

    for sid in strat_gross.keys():
        g = float(strat_gross.get(sid, 0.0))
        n = float(strat_net.get(sid, 0.0))

        scale = 1.0

        if STRATEGY_MAX_GROSS > 0.0 and g > STRATEGY_MAX_GROSS:
            scale = min(scale, float(STRATEGY_MAX_GROSS) / float(g) if g > 1e-12 else 0.0)

        if STRATEGY_MAX_NET > 0.0 and abs(n) > STRATEGY_MAX_NET:
            scale = min(scale, float(STRATEGY_MAX_NET) / float(abs(n)) if abs(n) > 1e-12 else 0.0)

        if scale < 1.0:
            for sym, row in (out or {}).items():
                if _strategy_bucket_for_row(row) == sid:
                    sw = _signed_weight(row)
                    out[sym]["weight"] = float(sw) * float(scale)
                    out[sym].setdefault("reason", {})
                    if isinstance(out[sym]["reason"], dict):
                        out[sym]["reason"]["strategy_budget"] = {
                            "strategy": sid,
                            "gross_pre": g,
                            "net_pre": n,
                            "scale": float(scale),
                            "max_gross": float(STRATEGY_MAX_GROSS),
                            "max_net": float(STRATEGY_MAX_NET),
                        }

            hit[sid] = {
                "gross_pre": g,
                "net_pre": n,
                "scale": float(scale),
            }

    if hit:
        info["strategy_budgets_hit"] = hit

    strat_gross_post: Dict[str, float] = {}
    strat_net_post: Dict[str, float] = {}

    for sym, row in (out or {}).items():
        sid = _strategy_bucket_for_row(row)
        sw = _signed_exposure_weight(exposure_con, str(sym), row)
        strat_gross_post[sid] = float(strat_gross_post.get(sid, 0.0) + abs(sw))
        strat_net_post[sid] = float(strat_net_post.get(sid, 0.0) + sw)

    info["strategy_gross_post"] = strat_gross_post
    info["strategy_net_post"] = strat_net_post

    return out
