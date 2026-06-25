"""Crypto profitability evidence report built on existing gate paths.

The report is diagnostic only. A passing row here is not a promotion: every
crypto challenger must still clear the normal champion/challenger path,
including ``engine.strategy.promotion_guard.assess_challenger``, before any
promotion.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Sequence

import numpy as np

from engine.execution.crypto_costs import normalize_crypto_symbol
from engine.strategy.cpcv import compute_pbo, cpcv_backtest
from engine.strategy.gated_backtest import run_gated_backtest
from engine.strategy.statistical_gates import passes_promotion_gate


def _as_float_array(values: Any) -> np.ndarray:
    try:
        arr = np.asarray(list(values), dtype=float).reshape(-1)
    except Exception:
        arr = np.asarray([], dtype=float)
    return arr[np.isfinite(arr)]


def _sharpe(values: Sequence[float] | np.ndarray) -> float:
    arr = np.asarray(values, dtype=float).reshape(-1)
    arr = arr[np.isfinite(arr)]
    if arr.size <= 1:
        return 0.0
    std = float(np.std(arr, ddof=1))
    if std <= 1e-12:
        mean = float(np.mean(arr))
        return 10.0 if mean > 0.0 else (-10.0 if mean < 0.0 else 0.0)
    return float(np.mean(arr) / std)


def _default_sample_times(n: int) -> list[int]:
    base = 1_700_000_000_000
    return [int(base + (idx * 60_000)) for idx in range(max(0, int(n)))]


class _SingleFactorLinearModel:
    def __init__(self) -> None:
        self._coef = 0.0

    def fit(self, features: Any, labels: Any) -> "_SingleFactorLinearModel":
        x = np.asarray(features, dtype=float).reshape(-1)
        y = np.asarray(labels, dtype=float).reshape(-1)
        denom = float(np.dot(x, x))
        self._coef = float(np.dot(x, y) / denom) if denom > 1e-12 else 0.0
        return self

    def predict(self, features: Any) -> np.ndarray:
        x = np.asarray(features, dtype=float)
        if x.ndim == 1:
            x = x.reshape(-1, 1)
        return np.asarray(x[:, 0] * float(self._coef), dtype=float)


def _cost_drag_bps(frictionless: np.ndarray, net: np.ndarray) -> float:
    n = min(int(frictionless.size), int(net.size))
    if n <= 0:
        return 0.0
    return float(np.mean(frictionless[:n] - net[:n]) * 10000.0)


def evaluate_crypto_challengers(
    challengers: Iterable[Mapping[str, Any]],
    *,
    n_competing_trials: int,
    cost_config_base: Mapping[str, Any] | None = None,
    gate_config: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Return per-symbol/per-factor pass/fail evidence net of crypto costs."""

    report: Dict[str, Any] = {"symbols": {}, "summary": {"n_pass": 0, "n_fail": 0}}
    base_cost = dict(cost_config_base or {})
    for row in list(challengers or []):
        symbol = normalize_crypto_symbol(str(row.get("symbol") or row.get("pair") or "BTC")) or "BTC"
        factor = str(row.get("factor") or row.get("factor_id") or "unknown_factor")
        predictions = _as_float_array(row.get("predictions", []))
        realized = _as_float_array(row.get("realized_returns", []))
        n = min(int(predictions.size), int(realized.size))
        predictions = predictions[:n]
        realized = realized[:n]
        raw_times = row.get("sample_times_ms")
        sample_times = list(raw_times if raw_times is not None else _default_sample_times(n))[:n]
        if n <= 0:
            entry = {
                "passed": False,
                "net_sharpe": 0.0,
                "deflated_sharpe": 0.0,
                "pbo": 1.0,
                "reality_check_passed": False,
                "frictionless_sharpe": 0.0,
                "cost_drag_bps": 0.0,
                "reason": "empty_inputs",
            }
            report["symbols"].setdefault(symbol, {})[factor] = entry
            report["summary"]["n_fail"] = int(report["summary"]["n_fail"]) + 1
            continue

        cost_config = {
            **base_cost,
            "enabled": bool(base_cost.get("enabled", True)),
            "asset_class": str(base_cost.get("asset_class") or "CRYPTO"),
            "symbol": symbol,
            "nights": int(row.get("nights", base_cost.get("nights", 1))),
            "side_sign": float(row.get("side_sign", base_cost.get("side_sign", 1.0))),
        }
        gated = run_gated_backtest(
            predictions,
            realized,
            sample_times_ms=sample_times,
            symbols=[symbol] * n,
            cost_config=cost_config,
            model_id=f"crypto_profitability_report_{symbol}_{factor}",
        )
        net_returns = _as_float_array(gated.get("returns") or [])
        frictionless = _as_float_array(gated.get("frictionless_returns") or (np.sign(predictions) * realized))

        cpcv_result: Dict[str, Any] = {"ok": False, "pbo": 1.0, "diagnostics": {}}
        pbo_result: Dict[str, Any] = {"ok": False, "pbo": 1.0, "status": "not_run"}
        if n >= 6:
            cpcv_result = cpcv_backtest(
                predictions.reshape(-1, 1),
                realized,
                model_factory=_SingleFactorLinearModel,
                n_splits=min(3, max(2, n)),
                n_test_splits=1,
                embargo_pct=0.0,
                label_horizon=1,
                sample_times_ms=sample_times,
                cost_config=cost_config,
                symbols=[symbol] * n,
            )
            diagnostics = dict(cpcv_result.get("diagnostics") or {})
            pbo_result = compute_pbo(
                diagnostics.get("in_sample_scores") or [],
                diagnostics.get("out_of_sample_scores") or [],
            )

        gate_passed, gate_diag = passes_promotion_gate(
            [float(value) for value in net_returns.tolist()],
            n_competing_trials=n_competing_trials,
            config=dict(gate_config or row.get("gate_config") or {}),
            models_returns=row.get("models_returns"),
        )
        pbo = float(pbo_result.get("pbo", cpcv_result.get("pbo", 1.0)) or 1.0)
        passed = bool(gate_passed and pbo <= float(row.get("max_pbo", 1.0)))
        entry = {
            "passed": bool(passed),
            "net_sharpe": float(_sharpe(net_returns)),
            "deflated_sharpe": float(gate_diag.get("deflated_sharpe") or 0.0),
            "pbo": float(pbo),
            "reality_check_passed": bool(gate_diag.get("spa_pass", True)),
            "frictionless_sharpe": float(_sharpe(frictionless)),
            "cost_drag_bps": float(_cost_drag_bps(frictionless, net_returns)),
            "reason": str(gate_diag.get("status") or cpcv_result.get("status") or "evaluated"),
            "gate_diagnostics": dict(gate_diag),
            "cpcv": {
                "ok": bool(cpcv_result.get("ok", False)),
                "mean_sharpe": float(cpcv_result.get("mean_sharpe") or 0.0),
                "median_sharpe": float(cpcv_result.get("median_sharpe") or 0.0),
                "pbo_result": dict(pbo_result),
            },
        }
        report["symbols"].setdefault(symbol, {})[factor] = entry
        key = "n_pass" if passed else "n_fail"
        report["summary"][key] = int(report["summary"][key]) + 1
    return report


__all__ = ["evaluate_crypto_challengers"]
