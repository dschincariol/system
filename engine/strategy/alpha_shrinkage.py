"""Empirical-Bayes shrinkage for per-symbol alpha estimates.

The portfolio layer consumes model alpha as expected return or expected z-score
views. This module applies partial pooling before allocation optimizers so
thin-history symbols move toward a cross-sectional or historical prior while
well-supported symbols keep most of their own estimate.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import math
import os
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple


_TRUTHY = {"1", "true", "yes", "on"}
_FALSEY = {"0", "false", "no", "off"}
_CONTEXT_FIELDS = ("sector", "liquidity_bucket", "volatility_regime", "model_family")


@dataclass(frozen=True)
class AlphaShrinkageConfig:
    enabled: bool
    prior_strength: float = 24.0
    missing_prior_strength: float = 48.0
    min_prior_observations: float = 3.0
    fallback_mean: float = 0.0
    max_abs_estimate: float = 10.0
    allow_upsizing: bool = False
    lookback_ms: int = 90 * 24 * 60 * 60 * 1000
    prior_levels: Tuple[str, ...] = (
        "sector",
        "liquidity_bucket",
        "volatility_regime",
        "model_family",
        "global",
    )


@dataclass(frozen=True)
class AlphaObservation:
    key: str
    symbol: str
    raw_estimate: float
    n_obs: float
    horizon_s: int = 0
    source: str = "expected_ret_net"
    sector: str = ""
    liquidity_bucket: str = ""
    volatility_regime: str = ""
    model_family: str = ""


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(str(name))
    if raw is None:
        return bool(default)
    value = str(raw).strip().lower()
    if value in _TRUTHY:
        return True
    if value in _FALSEY:
        return False
    return bool(default)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        value = float(raw)
    except Exception:
        return float(default)
    return float(value) if math.isfinite(value) else float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(str(name))
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _live_default_enabled() -> bool:
    mode = str(os.environ.get("ENGINE_MODE") or os.environ.get("APP_ENV") or "").strip().lower()
    execution_mode = str(os.environ.get("EXECUTION_MODE") or "").strip().lower()
    return mode in {"live", "prod", "production"} or execution_mode == "live"


def config_from_env() -> AlphaShrinkageConfig:
    levels_raw = str(os.environ.get("ALPHA_SHRINKAGE_PRIOR_LEVELS") or "").strip()
    if levels_raw:
        levels = tuple(
            part.strip().lower()
            for part in levels_raw.replace(";", ",").split(",")
            if part.strip()
        )
    else:
        levels = ("sector", "liquidity_bucket", "volatility_regime", "model_family", "global")
    valid_levels = tuple(
        level
        for level in levels
        if level in {"sector", "liquidity_bucket", "volatility_regime", "model_family", "global"}
    )
    if not valid_levels:
        valid_levels = ("sector", "liquidity_bucket", "volatility_regime", "model_family", "global")

    live_default = _live_default_enabled()
    enabled = _env_bool("ALPHA_SHRINKAGE_ENABLED", live_default)
    if live_default:
        enabled = True

    return AlphaShrinkageConfig(
        enabled=bool(enabled),
        prior_strength=max(0.0, _env_float("ALPHA_SHRINKAGE_PRIOR_STRENGTH", 24.0)),
        missing_prior_strength=max(
            0.0,
            _env_float("ALPHA_SHRINKAGE_MISSING_PRIOR_STRENGTH", 48.0),
        ),
        min_prior_observations=max(
            0.0,
            _env_float("ALPHA_SHRINKAGE_MIN_PRIOR_OBSERVATIONS", 3.0),
        ),
        fallback_mean=_env_float("ALPHA_SHRINKAGE_FALLBACK_MEAN", 0.0),
        max_abs_estimate=max(1e-12, _env_float("ALPHA_SHRINKAGE_MAX_ABS_ESTIMATE", 10.0)),
        allow_upsizing=_env_bool("ALPHA_SHRINKAGE_ALLOW_UPSIZING", False),
        lookback_ms=max(0, _env_int("ALPHA_SHRINKAGE_LOOKBACK_MS", 90 * 24 * 60 * 60 * 1000)),
        prior_levels=valid_levels,
    )


def _safe_json_obj(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    if isinstance(value, str) and not value.strip():
        return default
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return int(default)
    if isinstance(value, str) and not value.strip():
        return int(default)
    try:
        return int(value)
    except Exception:
        return int(default)


def _norm_bucket(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text or text in {"none", "null", "nan", "unknown"}:
        return ""
    return text.replace(" ", "_")


def _numeric_bucket(value: Any, *, low: float, high: float, labels: Tuple[str, str, str]) -> str:
    number = _safe_float(value)
    if number is None:
        return ""
    if number <= float(low):
        return labels[0]
    if number >= float(high):
        return labels[2]
    return labels[1]


def _context_value(*containers: Mapping[str, Any], names: Sequence[str]) -> Any:
    for container in containers:
        if not isinstance(container, Mapping):
            continue
        for name in names:
            if name in container and container.get(name) not in (None, ""):
                return container.get(name)
    return None


def _estimate_from_target(tgt: Mapping[str, Any], explain: Mapping[str, Any], reason: Mapping[str, Any]) -> Tuple[Optional[float], str]:
    tradability = explain.get("tradability") if isinstance(explain.get("tradability"), Mapping) else {}
    model_intent = explain.get("model_intent") if isinstance(explain.get("model_intent"), Mapping) else {}
    ensemble_output = explain.get("ensemble_output") if isinstance(explain.get("ensemble_output"), Mapping) else {}

    for source, value in (
        ("expected_ret_net", tgt.get("adjusted_expected_ret_net")),
        ("expected_ret_net", tradability.get("expected_ret_net") if isinstance(tradability, Mapping) else None),
        ("expected_ret_net", model_intent.get("expected_ret_net") if isinstance(model_intent, Mapping) else None),
        ("expected_ret_net", tgt.get("expected_ret_net")),
        ("expected_ret_net", reason.get("expected_ret_net")),
        ("expected_z", model_intent.get("expected_z") if isinstance(model_intent, Mapping) else None),
        ("expected_z", tgt.get("expected_z")),
        ("expected_z", reason.get("expected_z")),
        ("expected_z", ensemble_output.get("blended_prediction") if isinstance(ensemble_output, Mapping) else None),
    ):
        estimate = _safe_float(value)
        if estimate is not None:
            return float(estimate), source
    return None, ""


def _n_obs_from_target(tgt: Mapping[str, Any], explain: Mapping[str, Any], reason: Mapping[str, Any]) -> float:
    model_intent = explain.get("model_intent") if isinstance(explain.get("model_intent"), Mapping) else {}
    alpha_payload = reason.get("alpha_shrinkage") if isinstance(reason.get("alpha_shrinkage"), Mapping) else {}
    for container in (reason, tgt, model_intent, alpha_payload):
        if not isinstance(container, Mapping):
            continue
        for name in (
            "alpha_n_obs",
            "n_obs",
            "n_observations",
            "observations",
            "sample_size",
            "n_samples",
            "labels_n",
            "n",
        ):
            value = _safe_float(container.get(name))
            if value is not None:
                return float(max(0.0, value))
    return 0.0


def _model_family_from_target(tgt: Mapping[str, Any], explain: Mapping[str, Any], reason: Mapping[str, Any]) -> str:
    model_intent = explain.get("model_intent") if isinstance(explain.get("model_intent"), Mapping) else {}
    for value in (
        tgt.get("model_family"),
        explain.get("model_family"),
        explain.get("served_model_family"),
        explain.get("requested_model_family"),
        model_intent.get("model_family") if isinstance(model_intent, Mapping) else None,
        tgt.get("model_kind"),
        explain.get("model_kind"),
        reason.get("model_family"),
        reason.get("model_kind"),
        tgt.get("model_name"),
    ):
        bucket = _norm_bucket(value)
        if bucket:
            return bucket
    return ""


def observation_from_target(
    key: str,
    tgt: Mapping[str, Any],
    *,
    label_count: Optional[int] = None,
    config: Optional[AlphaShrinkageConfig] = None,
) -> Optional[AlphaObservation]:
    cfg = config or config_from_env()
    explain = _safe_json_obj(tgt.get("explain_json"))
    reason = tgt.get("reason") if isinstance(tgt.get("reason"), Mapping) else {}
    estimate, source = _estimate_from_target(tgt, explain, reason)
    if estimate is None:
        return None
    estimate = max(-float(cfg.max_abs_estimate), min(float(cfg.max_abs_estimate), float(estimate)))
    n_obs = _n_obs_from_target(tgt, explain, reason)
    if n_obs <= 0.0 and label_count is not None:
        n_obs = float(max(0, int(label_count)))

    regimes = tgt.get("regime_signals") if isinstance(tgt.get("regime_signals"), Mapping) else {}
    regime_vector = tgt.get("regime_vector") if isinstance(tgt.get("regime_vector"), Mapping) else {}
    vector_regimes = regime_vector.get("regimes") if isinstance(regime_vector.get("regimes"), Mapping) else {}

    sector = _norm_bucket(
        _context_value(
            reason,
            tgt,
            explain,
            names=("sector", "gics_sector", "industry_sector"),
        )
    )
    liquidity = _norm_bucket(
        _context_value(
            reason,
            tgt,
            explain,
            regimes,
            vector_regimes,
            names=("liquidity_bucket", "liquidity_regime"),
        )
    )
    if not liquidity:
        liquidity = _numeric_bucket(
            _context_value(reason, tgt, explain, names=("liquidity", "adv", "dollar_volume", "volume")),
            low=1_000_000.0,
            high=20_000_000.0,
            labels=("thin", "normal", "deep"),
        )
    volatility = _norm_bucket(
        _context_value(
            reason,
            tgt,
            explain,
            regimes,
            vector_regimes,
            names=("volatility_regime", "vol_regime"),
        )
    )
    if not volatility:
        volatility = _numeric_bucket(
            _context_value(reason, tgt, explain, names=("volatility", "vol", "realized_volatility", "rv_20")),
            low=0.15,
            high=0.35,
            labels=("low_vol", "mid_vol", "high_vol"),
        )

    symbol = str(tgt.get("symbol") or str(key).split(":")[-1]).upper().strip()
    return AlphaObservation(
        key=str(key),
        symbol=symbol,
        raw_estimate=float(estimate),
        n_obs=float(max(0.0, n_obs)),
        horizon_s=_safe_int(tgt.get("horizon_s"), 0),
        source=str(source),
        sector=sector,
        liquidity_bucket=liquidity,
        volatility_regime=volatility,
        model_family=_model_family_from_target(tgt, explain, reason),
    )


def _aggregate_observations(observations: Iterable[AlphaObservation]) -> Dict[Tuple[str, str], Dict[str, float]]:
    aggregates: Dict[Tuple[str, str], Dict[str, float]] = {}
    for obs in observations:
        weight = max(0.0, float(obs.n_obs))
        if weight <= 0.0:
            continue
        contexts = {
            "sector": obs.sector,
            "liquidity_bucket": obs.liquidity_bucket,
            "volatility_regime": obs.volatility_regime,
            "model_family": obs.model_family,
            "global": "global",
        }
        for level, raw_key in contexts.items():
            key = _norm_bucket(raw_key)
            if not key:
                continue
            item = aggregates.setdefault((level, key), {"sum": 0.0, "n": 0.0})
            item["sum"] += float(obs.raw_estimate) * float(weight)
            item["n"] += float(weight)
    return aggregates


def _leave_one_out_prior(
    obs: AlphaObservation,
    aggregates: Mapping[Tuple[str, str], Mapping[str, float]],
    *,
    level: str,
    config: AlphaShrinkageConfig,
) -> Optional[Dict[str, Any]]:
    raw_key = "global" if level == "global" else getattr(obs, level, "")
    key = _norm_bucket(raw_key)
    if not key:
        return None
    aggregate = aggregates.get((str(level), key))
    if not aggregate:
        return None
    total_n = max(0.0, float(aggregate.get("n") or 0.0))
    total_sum = float(aggregate.get("sum") or 0.0)
    own_n = max(0.0, float(obs.n_obs))
    prior_n = max(0.0, total_n - own_n)
    if prior_n < float(config.min_prior_observations):
        return None
    prior_sum = total_sum - (float(obs.raw_estimate) * own_n)
    return {
        "level": str(level),
        "key": str(key),
        "mean": float(prior_sum / max(prior_n, 1e-12)),
        "n": float(prior_n),
        "source": "cross_section_leave_one_out",
    }


def _best_prior_for(
    obs: AlphaObservation,
    aggregates: Mapping[Tuple[str, str], Mapping[str, float]],
    *,
    config: AlphaShrinkageConfig,
    fallback_prior: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    for level in config.prior_levels:
        prior = _leave_one_out_prior(obs, aggregates, level=str(level), config=config)
        if prior is not None:
            return prior

    if fallback_prior:
        prior_n = _safe_float(fallback_prior.get("n"), 0.0) or 0.0
        prior_mean = _safe_float(fallback_prior.get("mean"), None)
        if prior_mean is not None and prior_n >= float(config.min_prior_observations):
            return {
                "level": str(fallback_prior.get("level") or "global"),
                "key": str(fallback_prior.get("key") or "historical"),
                "mean": float(prior_mean),
                "n": float(prior_n),
                "source": str(fallback_prior.get("source") or "historical_labels"),
            }

    return {
        "level": "fallback",
        "key": "neutral",
        "mean": float(config.fallback_mean),
        "n": 0.0,
        "source": "conservative_missing_prior",
        "missing": True,
    }


def shrink_alpha_estimates(
    observations: Sequence[AlphaObservation],
    *,
    config: Optional[AlphaShrinkageConfig] = None,
    fallback_prior: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Dict[str, Any]]:
    cfg = config or config_from_env()
    aggregates = _aggregate_observations(observations)
    out: Dict[str, Dict[str, Any]] = {}
    for obs in observations:
        prior = _best_prior_for(obs, aggregates, config=cfg, fallback_prior=fallback_prior)
        prior_strength = (
            float(cfg.missing_prior_strength)
            if bool(prior.get("missing"))
            else float(cfg.prior_strength)
        )
        n_obs = max(0.0, float(obs.n_obs))
        own_weight = 1.0 if prior_strength <= 0.0 else n_obs / (n_obs + prior_strength)
        own_weight = max(0.0, min(1.0, float(own_weight)))
        raw = float(obs.raw_estimate)
        prior_mean = max(
            -float(cfg.max_abs_estimate),
            min(float(cfg.max_abs_estimate), float(prior.get("mean") or 0.0)),
        )
        adjusted = (own_weight * raw) + ((1.0 - own_weight) * prior_mean)

        effective = float(adjusted)
        clipped_reason = ""
        if not bool(cfg.allow_upsizing):
            if raw == 0.0:
                effective = 0.0
            elif (raw > 0.0 and effective < 0.0) or (raw < 0.0 and effective > 0.0):
                effective = 0.0
                clipped_reason = "sign_flip_blocked"
            elif abs(effective) > abs(raw):
                effective = raw
                clipped_reason = "upsizing_blocked"

        size_multiplier = 1.0
        if abs(raw) > 1e-12:
            size_multiplier = abs(float(effective)) / abs(float(raw))
        size_multiplier = max(0.0, min(1.0 if not bool(cfg.allow_upsizing) else 10.0, float(size_multiplier)))

        out[str(obs.key)] = {
            "enabled": bool(cfg.enabled),
            "applied": True,
            "symbol": str(obs.symbol),
            "source": str(obs.source),
            "raw_estimate": float(raw),
            "adjusted_estimate": float(adjusted),
            "effective_estimate": float(effective),
            "n_obs": float(n_obs),
            "prior_mean": float(prior_mean),
            "prior_level": str(prior.get("level") or ""),
            "prior_key": str(prior.get("key") or ""),
            "prior_n": float(prior.get("n") or 0.0),
            "prior_source": str(prior.get("source") or ""),
            "prior_strength": float(prior_strength),
            "own_weight": float(own_weight),
            "shrinkage_fraction": float(1.0 - own_weight),
            "shrinkage_abs": float(abs(raw - effective)),
            "size_multiplier": float(size_multiplier),
            "conservative_fallback": bool(prior.get("missing")),
            "clipped_reason": str(clipped_reason),
            "contexts": {
                "sector": str(obs.sector),
                "liquidity_bucket": str(obs.liquidity_bucket),
                "volatility_regime": str(obs.volatility_regime),
                "model_family": str(obs.model_family),
            },
        }
    return out


def _table_exists(con: Any, table_name: str) -> bool:
    if con is None:
        return False
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name),),
        ).fetchone()
        if row:
            return True
    except Exception:
        # no-op-guard: allow - non-SQLite adapters fall through to information_schema.
        pass
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_name=?
            LIMIT 1
            """,
            (str(table_name),),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _table_columns(con: Any, table_name: str) -> set[str]:
    if con is None:
        return set()
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
        return {str(row[1]).strip().lower() for row in rows or []}
    except Exception:
        return set()


def _label_value_expr(columns: set[str]) -> str:
    if "realized_ret" in columns and "impact_z" in columns:
        return "COALESCE(realized_ret, impact_z)"
    if "realized_ret" in columns:
        return "realized_ret"
    if "impact_z" in columns:
        return "impact_z"
    return "NULL"


def _fetch_label_stats_for_symbol(
    con: Any,
    symbol: str,
    horizon_s: int,
    *,
    now_ms: int,
    config: AlphaShrinkageConfig,
) -> Dict[str, Any]:
    if con is None or not _table_exists(con, "labels"):
        return {"n": 0, "mean": None}
    columns = _table_columns(con, "labels")
    if "symbol" not in columns:
        return {"n": 0, "mean": None}
    value_expr = _label_value_expr(columns)
    if value_expr == "NULL":
        return {"n": 0, "mean": None}
    clauses = ["symbol=?"]
    params: list[Any] = [str(symbol).upper().strip()]
    if "horizon_s" in columns and int(horizon_s) > 0:
        clauses.append("horizon_s=?")
        params.append(int(horizon_s))
    time_col = "created_at_ms" if "created_at_ms" in columns else "ts_ms" if "ts_ms" in columns else ""
    if time_col and int(config.lookback_ms) > 0 and int(now_ms) > 0:
        clauses.append(f"{time_col}>=?")
        params.append(int(now_ms) - int(config.lookback_ms))
    where = " AND ".join(clauses)
    try:
        row = con.execute(
            f"""
            SELECT COUNT(1), AVG({value_expr})
            FROM labels
            WHERE {where}
              AND {value_expr} IS NOT NULL
            """,
            tuple(params),
        ).fetchone()
        return {
            "n": int(row[0] or 0) if row else 0,
            "mean": (_safe_float(row[1]) if row and row[1] is not None else None),
        }
    except Exception:
        return {"n": 0, "mean": None}


def _fetch_global_label_prior(
    con: Any,
    *,
    now_ms: int,
    config: AlphaShrinkageConfig,
) -> Optional[Dict[str, Any]]:
    if con is None or not _table_exists(con, "labels"):
        return None
    columns = _table_columns(con, "labels")
    value_expr = _label_value_expr(columns)
    if value_expr == "NULL":
        return None
    clauses = [f"{value_expr} IS NOT NULL"]
    params: list[Any] = []
    time_col = "created_at_ms" if "created_at_ms" in columns else "ts_ms" if "ts_ms" in columns else ""
    if time_col and int(config.lookback_ms) > 0 and int(now_ms) > 0:
        clauses.append(f"{time_col}>=?")
        params.append(int(now_ms) - int(config.lookback_ms))
    try:
        row = con.execute(
            f"SELECT COUNT(1), AVG({value_expr}) FROM labels WHERE {' AND '.join(clauses)}",
            tuple(params),
        ).fetchone()
        n = int(row[0] or 0) if row else 0
        mean = _safe_float(row[1]) if row and row[1] is not None else None
        if mean is None:
            return None
        return {
            "level": "global",
            "key": "historical_labels",
            "source": "historical_labels",
            "n": int(n),
            "mean": float(mean),
        }
    except Exception:
        return None


def _write_adjusted_estimate_to_target(tgt: Dict[str, Any], result: Mapping[str, Any]) -> None:
    source = str(result.get("source") or "")
    effective = float(result.get("effective_estimate") or 0.0)
    explain = _safe_json_obj(tgt.get("explain_json"))
    if source == "expected_ret_net":
        tgt["adjusted_expected_ret_net"] = float(effective)
        tradability = explain.get("tradability")
        if not isinstance(tradability, dict):
            tradability = {}
        tradability.setdefault("raw_expected_ret_net", tradability.get("expected_ret_net"))
        tradability["expected_ret_net"] = float(effective)
        tradability["alpha_shrinkage_adjusted"] = True
        explain["tradability"] = tradability
        model_intent = explain.get("model_intent")
        if isinstance(model_intent, dict) and "expected_ret_net" in model_intent:
            model_intent.setdefault("raw_expected_ret_net", model_intent.get("expected_ret_net"))
            model_intent["expected_ret_net"] = float(effective)
            explain["model_intent"] = model_intent
    elif source == "expected_z":
        reason = tgt.get("reason") if isinstance(tgt.get("reason"), dict) else {}
        if isinstance(reason, dict):
            reason.setdefault("raw_expected_z", reason.get("expected_z"))
            reason["expected_z"] = float(effective)
            tgt["reason"] = reason
        model_intent = explain.get("model_intent")
        if isinstance(model_intent, dict) and "expected_z" in model_intent:
            model_intent.setdefault("raw_expected_z", model_intent.get("expected_z"))
            model_intent["expected_z"] = float(effective)
            explain["model_intent"] = model_intent
    explain["alpha_shrinkage"] = dict(result)
    tgt["explain_json"] = json.dumps(explain, separators=(",", ":"), sort_keys=True)


def apply_alpha_shrinkage_to_desired(
    con: Any,
    desired: Dict[str, Dict[str, Any]],
    *,
    now_ms: Optional[int] = None,
    config: Optional[AlphaShrinkageConfig] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    cfg = config or config_from_env()
    if not desired:
        return desired, {"enabled": bool(cfg.enabled), "applied": False, "reason": "empty_desired"}
    if not bool(cfg.enabled):
        return desired, {"enabled": False, "applied": False, "reason": "disabled"}

    ts_ms = int(now_ms or 0)
    label_stats: Dict[Tuple[str, int], Dict[str, Any]] = {}
    observations: list[AlphaObservation] = []
    for key, tgt in list(desired.items()):
        if not isinstance(tgt, dict):
            continue
        symbol = str(tgt.get("symbol") or str(key).split(":")[-1]).upper().strip()
        horizon_s = _safe_int(tgt.get("horizon_s"), 0)
        stat_key = (symbol, horizon_s)
        if stat_key not in label_stats:
            label_stats[stat_key] = _fetch_label_stats_for_symbol(
                con,
                symbol,
                horizon_s,
                now_ms=ts_ms,
                config=cfg,
            )
        obs = observation_from_target(
            key,
            tgt,
            label_count=int(label_stats.get(stat_key, {}).get("n") or 0),
            config=cfg,
        )
        if obs is not None:
            observations.append(obs)

    if not observations:
        return desired, {"enabled": True, "applied": False, "reason": "no_alpha_estimates"}

    fallback_prior = _fetch_global_label_prior(con, now_ms=ts_ms, config=cfg)
    results = shrink_alpha_estimates(observations, config=cfg, fallback_prior=fallback_prior)
    adjusted_count = 0
    missing_prior_count = 0
    multipliers: list[float] = []
    rows: list[Dict[str, Any]] = []

    for key, result in results.items():
        tgt = desired.get(key)
        if not isinstance(tgt, dict):
            continue
        multiplier = max(0.0, float(result.get("size_multiplier") or 0.0))
        weight = _safe_float(tgt.get("weight"), 0.0) or 0.0
        if multiplier < 1.0 or bool(result.get("conservative_fallback")):
            tgt["weight"] = float(weight) * float(multiplier)
            adjusted_count += 1
        reason = tgt.get("reason")
        if not isinstance(reason, dict):
            reason = {"raw": reason}
        reason["alpha_shrinkage"] = dict(result)
        if bool(result.get("conservative_fallback")):
            missing_prior_count += 1
        tgt["reason"] = reason
        _write_adjusted_estimate_to_target(tgt, result)
        multipliers.append(float(multiplier))
        rows.append(
            {
                "key": str(key),
                "symbol": str(result.get("symbol") or ""),
                "source": str(result.get("source") or ""),
                "raw_estimate": float(result.get("raw_estimate") or 0.0),
                "effective_estimate": float(result.get("effective_estimate") or 0.0),
                "n_obs": float(result.get("n_obs") or 0.0),
                "prior_level": str(result.get("prior_level") or ""),
                "prior_key": str(result.get("prior_key") or ""),
                "prior_n": float(result.get("prior_n") or 0.0),
                "size_multiplier": float(multiplier),
                "conservative_fallback": bool(result.get("conservative_fallback")),
            }
        )

    avg_multiplier = sum(multipliers) / max(1, len(multipliers))
    diagnostics = {
        "enabled": True,
        "applied": True,
        "method": "empirical_bayes_partial_pooling",
        "observations": int(len(observations)),
        "adjusted": int(adjusted_count),
        "missing_prior": int(missing_prior_count),
        "avg_size_multiplier": float(avg_multiplier),
        "min_size_multiplier": float(min(multipliers) if multipliers else 1.0),
        "prior_strength": float(cfg.prior_strength),
        "missing_prior_strength": float(cfg.missing_prior_strength),
        "min_prior_observations": float(cfg.min_prior_observations),
        "prior_levels": list(cfg.prior_levels),
        "fallback_prior": dict(fallback_prior or {}),
        "rows": rows,
    }
    return desired, diagnostics


__all__ = [
    "AlphaObservation",
    "AlphaShrinkageConfig",
    "apply_alpha_shrinkage_to_desired",
    "config_from_env",
    "observation_from_target",
    "shrink_alpha_estimates",
]
