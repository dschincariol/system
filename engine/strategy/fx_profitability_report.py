"""FX profitability evidence report built on existing backtest and gate paths.

The report is diagnostic only. A passing row here is not a promotion: every FX
challenger must still clear the normal champion/challenger path, including
``engine.strategy.promotion_guard.assess_challenger``, before promotion.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Sequence

import numpy as np

from engine.execution.fx_costs import normalize_fx_symbol
from engine.strategy.cpcv import cpcv_backtest
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


def evaluate_fx_challengers(
    challengers: Iterable[Mapping[str, Any]],
    *,
    n_competing_trials: int,
    cost_config_base: Mapping[str, Any] | None = None,
    gate_config: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Return per-pair/per-factor pass/fail evidence net of FX costs.

    This function calls the shared ``run_gated_backtest``/``cpcv_backtest`` and
    ``passes_promotion_gate`` paths. It does not promote, persist, route, or call
    live broker modules.
    """

    report: Dict[str, Any] = {"pairs": {}, "summary": {"n_pass": 0, "n_fail": 0}}
    base_cost = dict(cost_config_base or {})
    for row in list(challengers or []):
        pair = normalize_fx_symbol(str(row.get("pair") or row.get("symbol") or "EUR_USD"))
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
            report["pairs"].setdefault(pair, {})[factor] = entry
            report["summary"]["n_fail"] = int(report["summary"]["n_fail"]) + 1
            continue

        cost_config = {
            **base_cost,
            "enabled": bool(base_cost.get("enabled", True)),
            "asset_class": str(base_cost.get("asset_class") or "FX"),
            "symbol": pair,
            "nights": int(row.get("nights", base_cost.get("nights", 1))),
            "crosses_weekend": bool(row.get("crosses_weekend", base_cost.get("crosses_weekend", False))),
        }
        gated = run_gated_backtest(
            predictions,
            realized,
            sample_times_ms=sample_times,
            symbols=[pair] * n,
            cost_config=cost_config,
            model_id=f"fx_profitability_report_{pair}_{factor}",
        )
        net_returns = _as_float_array(gated.get("returns") or [])
        frictionless = _as_float_array(gated.get("frictionless_returns") or (np.sign(predictions) * realized))
        if net_returns.size <= 0:
            net_returns = np.asarray([], dtype=float)

        cpcv_result: Dict[str, Any] = {"ok": False, "pbo": 1.0}
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
                symbols=[pair] * n,
            )

        gate_passed, gate_diag = passes_promotion_gate(
            [float(value) for value in net_returns.tolist()],
            n_competing_trials=n_competing_trials,
            config=dict(gate_config or row.get("gate_config") or {}),
            models_returns=row.get("models_returns"),
        )
        pbo = float(cpcv_result.get("pbo") or 1.0)
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
            },
        }
        report["pairs"].setdefault(pair, {})[factor] = entry
        key = "n_pass" if passed else "n_fail"
        report["summary"][key] = int(report["summary"][key]) + 1
    return report
