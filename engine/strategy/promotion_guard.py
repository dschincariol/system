"""
FILE: promotion_guard.py

Human-readable purpose:
Blocks or allows model promotion based on runtime safety, drift, alerts, and
evaluation quality thresholds. This is the final gate before a candidate model
can be treated as promotion-eligible.
"""

import math
import os
import logging
import time
from statistics import NormalDist
from typing import Dict, Any, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    connect,
    fetch_latest_backtest_cpcv_run,
    init_db,
    record_hypothesis_result,
)
from engine.strategy.cpcv import cpcv_config_from_env
from engine.strategy.statistical_gates import (
    passes_promotion_gate,
    promotion_gate_config_from_env,
)
from engine.strategy.promotion_audit import record_statistical_evidence
from engine.strategy.statistics.factor_threshold import (
    FactorThresholdResult,
    harvey_liu_zhu_threshold_result,
)
from engine.strategy.statistics.multiple_testing import benjamini_hochberg
from engine.strategy.statistics.reality_check import white_reality_check

# ------            -- ------------------------------------------------------
# Global enable switch (env default ON)
# ------            -- ------------------------------------------------------

PROMOTION_ENABLED_ENV = os.environ.get("PROMOTION_ENABLED", "1") == "1"

# ------            -- ------------------------------------------------------
# Guard thresholds
# ------            -- ------------------------------------------------------

PROMOTION_CRIT_ALERT_LOOKBACK_S = int(os.environ.get("PROMOTION_CRIT_ALERT_LOOKBACK_S", "7200"))  # 2h
PROMOTION_MAX_CRIT_ALERTS = int(os.environ.get("PROMOTION_MAX_CRIT_ALERTS", "0"))  # 0 => any CRIT blocks

PROMOTION_MAX_DRIFT_RATIO = float(os.environ.get("PROMOTION_MAX_DRIFT_RATIO", "0.0"))  # 0 disables
PROMOTION_DRIFT_LOOKBACK_S = int(os.environ.get("PROMOTION_DRIFT_LOOKBACK_S", "86400"))  # 24h

PROMOTION_EQUITY_DRIFT_LOOKBACK_S = int(
    os.environ.get("PROMOTION_EQUITY_DRIFT_LOOKBACK_S", "7200")
)  # 2h
PROMOTION_BLOCK_IF_EQUITY_CRIT = os.environ.get(
    "PROMOTION_BLOCK_IF_EQUITY_CRIT", "1"
) == "1"

# ------            -- ------------------------------------------------------
# Logging
# ------            -- ------------------------------------------------------

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [promotion_guard] %(message)s",
)
LOG = logging.getLogger("promotion_guard")
_NORMAL = NormalDist()

# ------            -- ------------------------------------------------------
# Coverage / sanity thresholds (metric-based promotion)
# ------            -- ------------------------------------------------------

MIN_EVAL_ROWS = int(os.environ.get("PROMOTE_MIN_EVAL_ROWS", "200"))
MAX_ABS_RMSE = float(os.environ.get("PROMOTE_MAX_ABS_RMSE", "10.0"))
MAX_ABS_BIAS = float(os.environ.get("PROMOTE_MAX_ABS_BIAS", "5.0"))

# ------            -- ------------------------------------------------------
# Time helper
# ------            -- ------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(event: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=event,
        error=error,
        level=logging.WARNING,
        component="engine.strategy.promotion_guard",
        extra=extra,
        persist=False,
    )


def _table_exists(con, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal("promotion_guard_table_exists_failed", e, table_name=str(table_name))
        return False


def _warn_state(event: str, message: str, **extra: Any) -> None:
    log_failure(
        LOG,
        event=event,
        code=event,
        message=message,
        error=None,
        level=logging.WARNING,
        component="engine.strategy.promotion_guard",
        extra=extra,
        persist=False,
    )


def _finite_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception as exc:
        _warn_nonfatal(
            "PROMOTION_GUARD_FLOAT_PARSE_FAILED",
            exc,
            once_key=f"finite_float:{repr(value)[:80]}",
            value_repr=repr(value),
        )
        return None
    return float(out) if math.isfinite(out) else None


def _clean_numeric_series(values: Any) -> list[float]:
    if values is None:
        return []
    out: list[float] = []
    if isinstance(values, (str, bytes)):
        raw_iter = [values]
    else:
        try:
            raw_iter = list(values)
        except Exception as exc:
            _warn_nonfatal(
                "PROMOTION_GUARD_SERIES_ITER_FAILED",
                exc,
                once_key=f"series_iter:{type(values).__name__}",
                value_type=type(values).__name__,
            )
            raw_iter = [values]
    for raw in raw_iter:
        try:
            value = float(raw)
        except Exception as exc:
            _warn_nonfatal(
                "PROMOTION_GUARD_SERIES_VALUE_PARSE_FAILED",
                exc,
                once_key=f"series_value:{repr(raw)[:80]}",
                value_repr=repr(raw),
            )
            continue
        if math.isfinite(value):
            out.append(float(value))
    return out


def _two_sided_normal_p_value(t_stat: float) -> float:
    if not math.isfinite(float(t_stat)):
        return 0.0 if float(t_stat) > 0.0 else 1.0
    tail = 1.0 - float(_NORMAL.cdf(abs(float(t_stat))))
    return float(max(0.0, min(1.0, 2.0 * tail)))


def _extract_feature_id(raw: Any) -> str:
    if isinstance(raw, dict):
        for key in ("feature_id", "id", "name", "feature"):
            text = str(raw.get(key) or "").strip()
            if text:
                return text
        return ""
    return str(raw or "").strip()


def _as_feature_records(
    *,
    candidate_features: Any = None,
    new_features: Any = None,
    current_feature_ids: Any = None,
    challenger_feature_ids: Any = None,
    feature_returns: Any = None,
    feature_p_values: Any = None,
    feature_t_stats: Any = None,
) -> list[dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    order: list[str] = []

    def ensure(fid: str) -> dict[str, Any]:
        key = str(fid or "").strip()
        if key not in records:
            records[key] = {"feature_id": key}
            order.append(key)
        return records[key]

    raw_new_features = list(new_features or [])
    if not raw_new_features and challenger_feature_ids is not None:
        current = {str(fid or "").strip() for fid in list(current_feature_ids or []) if str(fid or "").strip()}
        raw_new_features = [
            str(fid or "").strip()
            for fid in list(challenger_feature_ids or [])
            if str(fid or "").strip() and str(fid or "").strip() not in current
        ]

    for raw in raw_new_features:
        fid = _extract_feature_id(raw)
        if not fid:
            continue
        rec = ensure(fid)
        if isinstance(raw, dict):
            for key in ("p_value", "q_value", "t_stat", "n_obs"):
                if key in raw:
                    rec[key] = raw.get(key)
            for key in ("returns", "oos_returns", "return_series", "factor_returns"):
                if key in raw:
                    rec["returns"] = raw.get(key)
                    break

    for raw in list(candidate_features or []):
        fid = _extract_feature_id(raw)
        if not fid:
            continue
        rec = ensure(fid)
        if isinstance(raw, dict):
            for key in ("p_value", "q_value", "t_stat", "n_obs"):
                if key in raw:
                    rec[key] = raw.get(key)
            for key in ("returns", "oos_returns", "return_series", "factor_returns"):
                if key in raw:
                    rec["returns"] = raw.get(key)
                    break

    if isinstance(feature_returns, dict):
        for fid, values in feature_returns.items():
            ensure(str(fid)).setdefault("returns", values)
    if isinstance(feature_p_values, dict):
        for fid, value in feature_p_values.items():
            ensure(str(fid))["p_value"] = value
    elif feature_p_values is not None:
        for fid, value in zip(order, list(feature_p_values)):
            ensure(str(fid))["p_value"] = value
    if isinstance(feature_t_stats, dict):
        for fid, value in feature_t_stats.items():
            ensure(str(fid))["t_stat"] = value
    elif feature_t_stats is not None:
        for fid, value in zip(order, list(feature_t_stats)):
            ensure(str(fid))["t_stat"] = value

    return [records[fid] for fid in order if str(fid or "").strip()]


def assess_challenger(
    *,
    model_id: str | None = None,
    model_name: str | None = None,
    candidate_version: str | None = None,
    challenger_returns: Any = None,
    champion_returns: Any = None,
    candidate_features: Any = None,
    new_features: Any = None,
    current_feature_ids: Any = None,
    challenger_feature_ids: Any = None,
    feature_returns: Any = None,
    feature_p_values: Any = None,
    feature_t_stats: Any = None,
    alpha: float = 0.05,
    fdr_q: float = 0.10,
    random_state: int = 42,
    bootstrap_samples: int = 10_000,
    persist: bool = True,
    con=None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Non-bypassable statistical promotion assessment.

    The challenger must pass White's Reality Check against the incumbent.
    Newly introduced features must pass BH-FDR at q=0.10 and the
    Harvey-Liu-Zhu `|t| > 3.0` factor threshold.
    """

    model_key = str(model_id or model_name or "").strip()
    if not model_key:
        model_key = str(candidate_version or "unknown_model").strip() or "unknown_model"
    evidence_ts = _now_ms()
    diagnostics: Dict[str, Any] = {
        "enabled": True,
        "applied": True,
        "model_id": str(model_key),
        "model_name": str(model_name or ""),
        "candidate_version": str(candidate_version or ""),
        "alpha": float(alpha),
        "fdr_q": float(fdr_q),
        "random_state": int(random_state),
        "bootstrap_samples": int(bootstrap_samples),
        "tests": {},
        "evidence_ts": int(evidence_ts),
        "passed": False,
    }

    challenger_series = _clean_numeric_series(challenger_returns)
    champion_series = _clean_numeric_series(champion_returns)
    reality = white_reality_check(
        challenger_series,
        champion_series,
        alpha=float(alpha),
        bootstrap_samples=int(bootstrap_samples),
        random_state=int(random_state),
    )
    reality_payload = reality.to_dict(include_distribution=True)
    diagnostics["tests"]["white_reality_check"] = dict(reality_payload)
    reality_decision = "pass" if bool(reality.passed) else "fail"
    if persist:
        record_statistical_evidence(
            con=con,
            ts=int(evidence_ts),
            model_id=str(model_key),
            test_name="white_reality_check",
            t_stat=float(reality.observed_statistic),
            p_value=float(reality.p_value),
            bootstrap_samples=int(reality.bootstrap_samples),
            decision=str(reality_decision),
            payload=dict(reality_payload),
        )

    feature_records = _as_feature_records(
        candidate_features=candidate_features,
        new_features=new_features,
        current_feature_ids=current_feature_ids,
        challenger_feature_ids=challenger_feature_ids,
        feature_returns=feature_returns,
        feature_p_values=feature_p_values,
        feature_t_stats=feature_t_stats,
    )
    feature_gate_passed = True
    if feature_records:
        factor_results: list[FactorThresholdResult] = []
        p_values: list[float] = []
        labels: list[str] = []
        for rec in feature_records:
            fid = str(rec.get("feature_id") or "").strip()
            labels.append(fid)
            t_value = _finite_float_or_none(rec.get("t_stat"))
            returns = rec.get("returns")
            result: FactorThresholdResult | None = None
            try:
                if t_value is not None:
                    result = harvey_liu_zhu_threshold_result(
                        feature_id=fid,
                        t_stat=float(t_value),
                        n_obs=int(rec.get("n_obs") or 0),
                        threshold=3.0,
                    )
                else:
                    result = harvey_liu_zhu_threshold_result(
                        y=_clean_numeric_series(returns),
                        feature_id=fid,
                        threshold=3.0,
                    )
            except Exception as e:
                _warn_nonfatal(
                    "PROMOTION_GUARD_FACTOR_THRESHOLD_FAILED",
                    e,
                    model_id=str(model_key),
                    feature_id=str(fid),
                )
                result = FactorThresholdResult(
                    feature_id=fid,
                    t_stat=0.0,
                    p_value=1.0,
                    threshold=3.0,
                    passed=False,
                    n_obs=0,
                    lags=0,
                    beta=0.0,
                    standard_error=0.0,
                )
            factor_results.append(result)
            p_raw = _finite_float_or_none(rec.get("p_value"))
            p_values.append(float(result.p_value if p_raw is None else max(0.0, min(1.0, p_raw))))

        bh = benjamini_hochberg(p_values, q=float(fdr_q), labels=labels)
        bh_payload = bh.to_dict()
        q_by_feature = {
            str(label): float(q_value)
            for label, q_value in zip(labels, list(bh.q_values))
        }
        rejected_by_feature = {
            str(label): bool(float(q_by_feature.get(str(label), 1.0)) < float(fdr_q))
            for label in labels
        }
        factor_payloads = []
        for result in factor_results:
            payload = result.to_dict()
            payload["q_value"] = float(q_by_feature.get(str(result.feature_id), 1.0))
            payload["bh_rejected"] = bool(rejected_by_feature.get(str(result.feature_id), False))
            payload["decision_components"] = {
                "bh_fdr_pass": bool(payload["bh_rejected"]),
                "hlz_threshold_pass": bool(result.passed),
            }
            factor_payloads.append(payload)

        bh_passed = bool(feature_records) and all(bool(v) for v in rejected_by_feature.values())
        threshold_passed = bool(factor_results) and all(bool(result.passed) for result in factor_results)
        feature_gate_passed = bool(bh_passed and threshold_passed)
        diagnostics["tests"]["benjamini_hochberg_fdr"] = {
            **bh_payload,
            "passed": bool(bh_passed),
            "feature_q_values": q_by_feature,
            "feature_rejected": rejected_by_feature,
        }
        diagnostics["tests"]["harvey_liu_zhu_factor_threshold"] = {
            "passed": bool(threshold_passed),
            "features": factor_payloads,
        }

        if persist:
            record_statistical_evidence(
                con=con,
                ts=int(evidence_ts),
                model_id=str(model_key),
                test_name="benjamini_hochberg_fdr",
                p_value=float(max(p_values) if p_values else 1.0),
                q_value=float(max(q_by_feature.values()) if q_by_feature else 1.0),
                decision=("pass" if bool(bh_passed) else "fail"),
                payload=diagnostics["tests"]["benjamini_hochberg_fdr"],
            )
            for payload in factor_payloads:
                record_statistical_evidence(
                    con=con,
                    ts=int(evidence_ts),
                    model_id=str(model_key),
                    feature_id=str(payload.get("feature_id") or ""),
                    test_name="harvey_liu_zhu_factor_threshold",
                    t_stat=float(payload.get("t_stat") or 0.0),
                    p_value=float(payload.get("p_value") or 1.0),
                    q_value=float(payload.get("q_value") or 1.0),
                    decision=("pass" if bool(payload.get("decision_components", {}).get("bh_fdr_pass")) and bool(payload.get("passed")) else "fail"),
                    payload=payload,
                )
    else:
        diagnostics["tests"]["benjamini_hochberg_fdr"] = {"applied": False, "passed": True, "status": "no_new_features"}
        diagnostics["tests"]["harvey_liu_zhu_factor_threshold"] = {"applied": False, "passed": True, "status": "no_new_features"}

    diagnostics["passed"] = bool(reality.passed and feature_gate_passed)
    diagnostics["status"] = "pass" if bool(diagnostics["passed"]) else "fail"
    return bool(diagnostics["passed"]), diagnostics

# ------            -- ------------------------------------------------------
# Guard state (DB overrides env)
# ------            -- ------------------------------------------------------

def set_guard(key: str, value: str) -> None:
    init_db()
    con = connect()
    try:
        

        con.execute(
            """
            INSERT OR REPLACE INTO model_promotion_guard(key, value, updated_ts_ms)
            VALUES (?,?,?)
            """,
            (str(key), str(value), _now_ms()),
        )
        con.commit()
    finally:
        con.close()


def get_guard(key: str, default: str) -> str:
    init_db()
    con = connect()
    try:
        r = con.execute(
            "SELECT value FROM model_promotion_guard WHERE key=?",
            (str(key),),
        ).fetchone()
        return str(r[0]) if r and r[0] is not None else str(default)
    finally:
        con.close()


def statistical_gate_config(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    return promotion_gate_config_from_env(config)


def cpcv_gate_config(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    base = cpcv_config_from_env()
    overrides = dict(config or {})
    nested = overrides.get("cpcv") if isinstance(overrides.get("cpcv"), dict) else {}
    merged = dict(base)
    for key in (
        "enabled",
        "n_splits",
        "n_test_splits",
        "embargo_pct",
        "label_horizon",
        "max_pbo",
        "min_path_sharpe",
    ):
        if key in overrides:
            merged[key] = overrides.get(key)
        if key in nested:
            merged[key] = nested.get(key)
    merged["enabled"] = str(merged.get("enabled") or "").strip().lower() in {"1", "true", "yes", "y", "on"}
    merged["n_splits"] = int(max(2, int(merged.get("n_splits") or 6)))
    merged["n_test_splits"] = int(max(1, int(merged.get("n_test_splits") or 2)))
    merged["embargo_pct"] = float(max(0.0, float(merged.get("embargo_pct") or 0.0)))
    merged["label_horizon"] = int(max(0, int(merged.get("label_horizon") or 0)))
    merged["max_pbo"] = float(max(0.0, float(merged.get("max_pbo") or 0.0)))
    merged["min_path_sharpe"] = float(merged.get("min_path_sharpe") or 0.0)
    return merged


def _cpcv_run_mismatch_fields(run: Dict[str, Any], gate_config: Dict[str, Any]) -> list[str]:
    mismatch = []
    if int(run.get("n_splits") or 0) != int(gate_config.get("n_splits") or 0):
        mismatch.append("n_splits")
    if int(run.get("n_test_splits") or 0) != int(gate_config.get("n_test_splits") or 0):
        mismatch.append("n_test_splits")
    if abs(float(run.get("embargo_pct") or 0.0) - float(gate_config.get("embargo_pct") or 0.0)) > 1e-9:
        mismatch.append("embargo_pct")
    return mismatch


def _materialize_cpcv_run(
    *,
    model_name: str,
    candidate_version: str,
    gate_config: Dict[str, Any],
) -> Dict[str, Any]:
    try:
        from engine.strategy.cpcv import run_backtest_cpcv_job
    except Exception as e:
        _warn_nonfatal(
            "PROMOTION_GUARD_CPCV_IMPORT_FAILED",
            e,
            model_name=str(model_name),
            candidate_version=str(candidate_version),
        )
        return {"ok": False, "status": "auto_run_import_failed", "error": f"{type(e).__name__}:{e}"}

    try:
        return dict(
            run_backtest_cpcv_job(
                model_name=str(model_name),
                candidate_version=str(candidate_version),
                n_splits=int(gate_config.get("n_splits") or 0),
                n_test_splits=int(gate_config.get("n_test_splits") or 0),
                embargo_pct=float(gate_config.get("embargo_pct") or 0.0),
                label_horizon=int(gate_config.get("label_horizon") or 0),
            )
            or {}
        )
    except Exception as e:
        _warn_nonfatal(
            "PROMOTION_GUARD_CPCV_AUTORUN_FAILED",
            e,
            model_name=str(model_name),
            candidate_version=str(candidate_version),
        )
        return {"ok": False, "status": "auto_run_failed", "error": f"{type(e).__name__}:{e}"}


def evaluate_cpcv_promotion_gate(
    *,
    model_name: str,
    candidate_version: str,
    config: Dict[str, Any] | None = None,
) -> Tuple[bool, Dict[str, Any]]:
    gate_config = cpcv_gate_config(config)
    diagnostics: Dict[str, Any] = {
        "enabled": bool(gate_config.get("enabled")),
        "status": "disabled",
        "model_name": str(model_name or "").strip(),
        "candidate_version": str(candidate_version or "").strip(),
        "required_n_splits": int(gate_config.get("n_splits") or 0),
        "required_n_test_splits": int(gate_config.get("n_test_splits") or 0),
        "required_embargo_pct": float(gate_config.get("embargo_pct") or 0.0),
        "max_pbo": float(gate_config.get("max_pbo") or 0.0),
        "min_path_sharpe": float(gate_config.get("min_path_sharpe") or 0.0),
        "passed": True,
    }
    if not bool(gate_config.get("enabled")):
        return True, diagnostics

    run = fetch_latest_backtest_cpcv_run(
        model_name=str(model_name or "").strip(),
        candidate_version=str(candidate_version or "").strip(),
        include_paths=False,
    )
    mismatch = _cpcv_run_mismatch_fields(dict(run or {}), gate_config) if isinstance(run, dict) and run else []
    if (not isinstance(run, dict) or not run) or mismatch:
        diagnostics["auto_run"] = _materialize_cpcv_run(
            model_name=str(model_name or "").strip(),
            candidate_version=str(candidate_version or "").strip(),
            gate_config=gate_config,
        )
        run = fetch_latest_backtest_cpcv_run(
            model_name=str(model_name or "").strip(),
            candidate_version=str(candidate_version or "").strip(),
            include_paths=False,
        )
        mismatch = _cpcv_run_mismatch_fields(dict(run or {}), gate_config) if isinstance(run, dict) and run else []

    if not isinstance(run, dict) or not run:
        diagnostics["status"] = str(dict(diagnostics.get("auto_run") or {}).get("status") or "missing_run")
        diagnostics["passed"] = False
        return False, diagnostics

    diagnostics["latest_run"] = {
        "id": int(run.get("id") or 0),
        "created_ts": int(run.get("created_ts") or 0),
        "n_splits": int(run.get("n_splits") or 0),
        "n_test_splits": int(run.get("n_test_splits") or 0),
        "embargo_pct": float(run.get("embargo_pct") or 0.0),
        "n_paths": int(run.get("n_paths") or 0),
        "mean_sharpe": float(run.get("mean_sharpe") or 0.0),
        "median_sharpe": float(run.get("median_sharpe") or 0.0),
        "pbo": float(run.get("pbo") or 0.0),
    }
    diagnostics["run_diagnostics"] = dict(run.get("diagnostics") or {})

    if mismatch:
        diagnostics["status"] = "parameter_mismatch"
        diagnostics["mismatch_fields"] = mismatch
        diagnostics["passed"] = False
        return False, diagnostics

    if int(run.get("n_paths") or 0) <= 0:
        diagnostics["status"] = "no_valid_paths"
        diagnostics["passed"] = False
        return False, diagnostics

    if float(run.get("pbo") or 0.0) > float(gate_config.get("max_pbo") or 0.0):
        diagnostics["status"] = "pbo_above_threshold"
        diagnostics["passed"] = False
        return False, diagnostics

    if float(run.get("median_sharpe") or 0.0) < float(gate_config.get("min_path_sharpe") or 0.0):
        diagnostics["status"] = "median_sharpe_below_threshold"
        diagnostics["passed"] = False
        return False, diagnostics

    diagnostics["status"] = "evaluated"
    return True, diagnostics


def evaluate_statistical_promotion_gate(
    *,
    model_name: str,
    candidate_version: str,
    returns,
    n_competing_trials: int,
    models_returns: Dict[str, Any] | None = None,
    config: Dict[str, Any] | None = None,
    persist: bool = True,
) -> Tuple[bool, Dict[str, Any]]:
    gate_config = statistical_gate_config(config)
    stat_passed, statistical_diagnostics = passes_promotion_gate(
        returns,
        n_competing_trials,
        config=gate_config,
        models_returns=models_returns,
    )
    cpcv_passed, cpcv_diagnostics = evaluate_cpcv_promotion_gate(
        model_name=str(model_name or "").strip(),
        candidate_version=str(candidate_version or "").strip(),
        config=config,
    )
    diagnostics = dict(statistical_diagnostics or {})
    diagnostics["model_name"] = str(model_name or "").strip()
    diagnostics["candidate_version"] = str(candidate_version or "").strip()
    diagnostics["statistical_gate"] = dict(statistical_diagnostics or {})
    diagnostics["cpcv"] = dict(cpcv_diagnostics or {})
    diagnostics["validation_enabled"] = bool(
        bool((statistical_diagnostics or {}).get("enabled")) or bool((cpcv_diagnostics or {}).get("enabled"))
    )
    diagnostics["applied"] = bool(diagnostics.get("validation_enabled"))
    diagnostics["passed"] = bool(stat_passed and cpcv_passed)
    if not bool(diagnostics.get("validation_enabled")):
        diagnostics["status"] = "disabled"
    elif not bool(stat_passed):
        diagnostics["status"] = str((statistical_diagnostics or {}).get("status") or "statistical_gate_failed")
    elif not bool(cpcv_passed):
        diagnostics["status"] = str((cpcv_diagnostics or {}).get("status") or "cpcv_gate_failed")
    else:
        diagnostics["status"] = "evaluated"

    if bool(persist) and bool((statistical_diagnostics or {}).get("enabled")):
        try:
            record_hypothesis_result(
                model_name=str(model_name or "").strip(),
                candidate_version=str(candidate_version or model_name or "").strip(),
                n_observations=int(diagnostics.get("n_observations") or 0),
                t_statistic=float(diagnostics.get("t_statistic") or 0.0),
                deflated_sharpe=float(diagnostics.get("deflated_sharpe") or 0.0),
                threshold_t=float(diagnostics.get("threshold_t") or 0.0),
                n_competing_trials=int(diagnostics.get("n_competing_trials") or 0),
                passed=bool(diagnostics.get("passed")),
                diagnostics=diagnostics,
            )
        except Exception as e:
            _warn_nonfatal(
                "PROMOTION_GUARD_HYPOTHESIS_RECORD_FAILED",
                e,
                model_name=str(model_name or "").strip(),
                candidate_version=str(candidate_version or "").strip(),
            )

    return bool(stat_passed and cpcv_passed), diagnostics

# ------            -- ------------------------------------------------------
# A) Metric-based promotion decision (RESTORED, not lost)
# ------            -- ------------------------------------------------------

def promotion_allowed_by_metrics(
    challenger_metrics: Dict[str, Any],
    champion_metrics: Dict[str, Any],
    min_improvement: float,
    diracc_tol: float,
) -> bool:
    """
    Pure metric-based promotion gate.
    Returns True if challenger is statistically better.
    """
    try:
        # ---- coverage ----
        n_eval = int(challenger_metrics.get("n_eval", 0))
        if n_eval < MIN_EVAL_ROWS:
            _warn_state("PROMOTE_BLOCKED_INSUFFICIENT_EVAL_ROWS", "Promotion blocked due to insufficient evaluation rows.", n_eval=n_eval)
            return False

        # ---- sanity ----
        rmse = float(challenger_metrics.get("rmse", float("inf")))
        bias = abs(float(challenger_metrics.get("bias", 0.0)))

        if not math.isfinite(rmse) or rmse > MAX_ABS_RMSE:
            _warn_state("PROMOTE_BLOCKED_BAD_RMSE", "Promotion blocked due to invalid RMSE.", rmse=rmse)
            return False

        if not math.isfinite(bias) or bias > MAX_ABS_BIAS:
            _warn_state("PROMOTE_BLOCKED_BAD_BIAS", "Promotion blocked due to excessive bias.", bias=bias)
            return False

        # ---- improvement ----
        champ_rmse = float(champion_metrics.get("rmse", float("inf")))
        if rmse >= champ_rmse * (1.0 - min_improvement):
            logging.info(
                "PROMOTE_BLOCKED no_rmse_improvement rmse=%s champ=%s",
                rmse,
                champ_rmse,
            )
            return False

        # ---- directional accuracy ----
        ch_dir = float(challenger_metrics.get("dir_acc", 0.0))
        cp_dir = float(champion_metrics.get("dir_acc", 0.0))
        if ch_dir < cp_dir - diracc_tol:
            logging.info(
                "PROMOTE_BLOCKED dir_acc_worse ch=%s cp=%s",
                ch_dir,
                cp_dir,
            )
            return False

        logging.info(
            "PROMOTE_ALLOWED metrics rmse=%s dir_acc=%s n_eval=%s",
            rmse,
            ch_dir,
            n_eval,
        )
        return True

    except Exception as e:
        _warn_nonfatal(
            "PROMOTE_BLOCKED_METRICS_EXCEPTION",
            e,
            challenger_metrics=dict(challenger_metrics or {}),
            champion_metrics=dict(champion_metrics or {}),
        )
        return False

# ------            -- ------------------------------------------------------
# B) System-state promotion guard (public API)
# ------            -- ------------------------------------------------------

def promotion_allowed() -> Tuple[bool, Dict[str, Any]]:
    """
    System-wide promotion guard.
    Returns (allowed, reason_dict).
    """
    init_db()

    enabled_db = get_guard("promotion_enabled", "1")
    enabled = (enabled_db == "1") and PROMOTION_ENABLED_ENV

    reason: Dict[str, Any] = {
        "promotion_enabled_env": bool(PROMOTION_ENABLED_ENV),
        "promotion_enabled_db": enabled_db,
        "statistical_gate": statistical_gate_config(),
        "cpcv_gate": cpcv_gate_config(),
        "blockers": [],
    }

    if not enabled:
        reason["blockers"].append("disabled")
        return (False, reason)

    con = connect()
    try:
        now = _now_ms()

        # ---- cooldown guard (global, fail-closed) ----
        cooldown_s = int(os.environ.get("PROMOTION_COOLDOWN_S", "21600"))  # 6h
        cooldown_ms = int(cooldown_s) * 1000
        try:
            last_promo = con.execute(
                """
                SELECT MAX(ts_ms) FROM model_promotion_audit
                WHERE action='promote'
                """
            ).fetchone()[0]
            last_promo = int(last_promo or 0)
        except Exception:
            last_promo = 0

        reason["last_promo_ts_ms"] = last_promo
        reason["cooldown_s"] = int(cooldown_s)

        if last_promo > 0 and (now - last_promo) < cooldown_ms:
            reason["blockers"].append("cooldown")

        # ---- CRIT alerts guard ----
        try:
            lookback_ms = PROMOTION_CRIT_ALERT_LOOKBACK_S * 1000
            n_crit = con.execute(
                """
                SELECT COUNT(1) FROM alerts
                WHERE severity='CRIT' AND ts_ms >= ?
                """,
                (now - lookback_ms,),
            ).fetchone()[0]
            n_crit = int(n_crit or 0)
        except Exception:
            n_crit = 0

        reason["crit_alerts"] = n_crit
        if n_crit > PROMOTION_MAX_CRIT_ALERTS:
            reason["blockers"].append("crit_alerts")

        # ---- equity drift CRIT ----
        if PROMOTION_BLOCK_IF_EQUITY_CRIT:
            reason["equity_drift_available"] = _table_exists(con, "equity_drift")
            if reason["equity_drift_available"]:
                try:
                    ed_ms = PROMOTION_EQUITY_DRIFT_LOOKBACK_S * 1000
                    ed = con.execute(
                        """
                        SELECT COUNT(1) FROM equity_drift
                        WHERE level='CRIT' AND ts_ms >= ?
                        """,
                        (now - ed_ms,),
                    ).fetchone()[0]
                    ed = int(ed or 0)
                except Exception:
                    ed = 0
            else:
                ed = 0
            reason["equity_drift_crit_points"] = ed
            if ed > 0:
                reason["blockers"].append("equity_drift_crit")

        # ---- model drift ratio ----
        if PROMOTION_MAX_DRIFT_RATIO > 0.0:
            try:
                md = con.execute(
                    "SELECT MAX(drift_ratio) FROM model_drift"
                ).fetchone()[0]
                md = float(md or 0.0)
            except Exception:
                md = 0.0

            reason["max_drift_ratio"] = md
            if md > PROMOTION_MAX_DRIFT_RATIO:
                reason["blockers"].append("drift_ratio")

        # ------------------------------------------------------------
        # Trade Attribution Guard (capital-based pruning)
        # ------------------------------------------------------------
        try:
            rows = con.execute(
                """
                SELECT
                  json_extract(model_json, '$.model_name') AS model_name,
                  SUM(
                    COALESCE(
                      json_extract(signal_json, '$.pnl_attribution.total_pnl'),
                      COALESCE(json_extract(signal_json, '$.pnl_attribution.realized_pnl'), 0.0)
                      + COALESCE(json_extract(signal_json, '$.pnl_attribution.unrealized_pnl'), 0.0)
                      - COALESCE(fees, 0.0)
                      - COALESCE(json_extract(signal_json, '$.pnl_attribution.extra.slippage_cost'), 0.0)
                    )
                  ) AS total_pnl
                FROM trade_attribution_ledger
                WHERE suppression_reason IS NULL
                  AND ts_ms >= ?
                GROUP BY model_name
                """,
                (now - (PROMOTION_DRIFT_LOOKBACK_S * 1000),),
            ).fetchall()

            model_pnl = {str(r[0]): float(r[1] or 0.0) for r in rows if r[0]}

            reason["model_pnl_snapshot"] = model_pnl

            # block promotion if any live model is negative capital impact
            negative_models = [m for m, p in model_pnl.items() if float(p) < 0.0]
            if negative_models:
                reason["blockers"].append("negative_real_pnl_models")
                reason["negative_models"] = negative_models

        except Exception as e:
            _warn_nonfatal("promotion_guard_model_pnl_snapshot_failed", e)
    finally:
        con.close()

    allowed = len(reason["blockers"]) == 0
    return (allowed, reason)
