"""Chronological promotion backtest replay through shared runtime gates.

This module is deliberately incremental: fast CPCV remains available for broad
screening, while promotion evaluation can route model intents through the same
max-position and execution-policy code used by live trading before computing
cost-adjusted PnL.
"""

from __future__ import annotations

import importlib
import logging
import math
from typing import Any, Dict, Mapping, Sequence

import numpy as np

from engine.execution.broker_sim import simulate_weight_order_batch
from engine.execution.cost_models.almgren_chriss import AlmgrenChrissCost
from engine.execution.execution_policy_engine import apply_execution_policy
from engine.execution.trade_suppression_engine import evaluate_trade_suppression
from engine.runtime.failure_diagnostics import log_failure
from engine.strategy.portfolio import apply_max_position_constraint


LOG = logging.getLogger(__name__)
_SQLITE_MODULE = "sqlite" + "3"


def _sqlite_module():
    return importlib.import_module(_SQLITE_MODULE)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.gated_backtest",
        extra=extra or None,
        persist=False,
    )


def _normalize_symbols(symbols: Sequence[Any] | np.ndarray | None, n: int) -> list[str]:
    if symbols is None:
        return [f"ASSET_{idx:05d}" for idx in range(int(n))]
    values = [str(value or "").upper().strip() or f"ASSET_{idx:05d}" for idx, value in enumerate(list(symbols))]
    if len(values) != int(n):
        raise ValueError(f"gated_backtest_symbol_length_mismatch symbols={len(values)} observations={int(n)}")
    return values


def _normalize_times(sample_times_ms: Sequence[Any] | np.ndarray | None, n: int) -> np.ndarray:
    if sample_times_ms is None:
        return np.arange(int(n), dtype=np.int64)
    values = np.asarray(sample_times_ms, dtype=float).reshape(-1)
    if values.size != int(n):
        raise ValueError(f"gated_backtest_time_length_mismatch sample_times={int(values.size)} observations={int(n)}")
    if not np.all(np.isfinite(values)):
        raise ValueError("gated_backtest_non_finite_sample_time")
    return values.astype(np.int64, copy=False)


def _side_from_prediction(prediction: float) -> str:
    return "SELL" if float(prediction) < 0.0 else "BUY"


def _neutral_execution_allowed(**_kwargs: Any) -> tuple[bool, str | None, Dict[str, Any]]:
    return True, None, {"source": "gated_backtest"}


def _neutral_capital_preservation(**_kwargs: Any) -> Dict[str, Any]:
    return {"source": "gated_backtest", "mode": "normal"}


def _neutral_trade_suppression(reason: str) -> Dict[str, Any]:
    return {
        "ok": True,
        "state": "NONE",
        "action": "NONE",
        "size_mult": 1.0,
        "throttle_mult": 1.0,
        "hard_block": False,
        "reason": str(reason),
    }


def _safe_trade_suppression(**kwargs: Any) -> Dict[str, Any]:
    if isinstance(kwargs.get("con"), _sqlite_module().Connection):
        return _neutral_trade_suppression("no_live_trade_suppression_history")
    try:
        return evaluate_trade_suppression(
            con=kwargs.get("con"),
            actor=str(kwargs.get("actor") or "gated_backtest"),
            mode=str(kwargs.get("mode") or "backtest"),
            broker=str(kwargs.get("broker") or "sim"),
            initialize_storage=False,
            now_ms=kwargs.get("now_ms"),
            persist_runtime_state=False,
        )
    except Exception as exc:
        _warn_nonfatal(
            "GATED_BACKTEST_TRADE_SUPPRESSION_HISTORY_UNAVAILABLE",
            exc,
            degradation="neutral_trade_suppression",
        )
        return _neutral_trade_suppression("no_backtest_trade_suppression_history")


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return float(default)
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _config_enabled(cost_config: Mapping[str, Any] | None) -> bool:
    if not isinstance(cost_config, Mapping):
        return True
    raw = cost_config.get("enabled", True)
    if isinstance(raw, bool):
        return bool(raw)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _futures_backtest_equity(cost_config: Mapping[str, Any] | None) -> float:
    cfg = dict(cost_config or {})
    for key in ("equity", "account_equity", "portfolio_equity", "notional"):
        value = _safe_float(cfg.get(key), 0.0)
        if value > 0.0:
            return float(value)
    return 100_000.0


def _cost_config_asset_class(cost_config: Mapping[str, Any] | None) -> str:
    if not isinstance(cost_config, Mapping):
        return ""
    return str(cost_config.get("asset_class") or "").upper().strip()


def _is_futures_symbol(symbol: str) -> bool:
    try:
        from engine.data.futures_instrument import parse_futures_symbol

        return parse_futures_symbol(str(symbol or "")) is not None
    except Exception as exc:
        _warn_nonfatal(
            "GATED_BACKTEST_FUTURES_SYMBOL_PARSE_FAILED",
            exc,
            symbol=str(symbol),
        )
        return False


def _should_apply_futures_costs(
    cost_config: Mapping[str, Any] | None,
    previous_weights: Mapping[str, float],
    selected_weights: Mapping[str, float],
) -> bool:
    if not _config_enabled(cost_config):
        return False
    if _cost_config_asset_class(cost_config) == "FUTURES":
        return True
    return any(_is_futures_symbol(str(symbol)) for symbol in set(previous_weights.keys()) | set(selected_weights.keys()))


def _estimate_futures_transition_costs(
    *,
    con: Any,
    previous_weights: Mapping[str, float],
    selected_weights: Mapping[str, float],
    equity: float,
    ts_ms: int,
    force_metadata_lookup: bool = False,
) -> Dict[str, Any]:
    """Apply the existing FUT-08 point-value cost estimator to futures deltas only."""
    try:
        from engine.strategy import portfolio_backtest
    except Exception as exc:
        _warn_nonfatal("GATED_BACKTEST_FUTURES_COST_IMPORT_FAILED", exc)
        return {
            "enabled": False,
            "status": "import_failed",
            "cost_return": 0.0,
            "trade_costs": [],
        }

    total_cost = 0.0
    trade_costs: list[Dict[str, Any]] = []
    symbols = sorted(set(previous_weights.keys()) | set(selected_weights.keys()))
    for symbol in symbols:
        symbol_text = str(symbol or "").upper().strip()
        if not symbol_text:
            continue
        if not bool(force_metadata_lookup) and not _is_futures_symbol(symbol_text):
            continue
        try:
            futures_meta = portfolio_backtest._futures_metadata(con, symbol_text)
        except Exception as exc:
            _warn_nonfatal(
                "GATED_BACKTEST_FUTURES_METADATA_FAILED",
                exc,
                symbol=symbol_text,
            )
            futures_meta = None
        if not isinstance(futures_meta, dict):
            continue

        delta_weight = float(selected_weights.get(symbol_text, 0.0)) - float(previous_weights.get(symbol_text, 0.0))
        if abs(float(delta_weight)) <= 1e-9:
            continue
        try:
            trade_cost = dict(
                portfolio_backtest._estimate_weight_delta_trade_cost(
                    con,
                    symbol_text,
                    delta_weight=float(delta_weight),
                    equity=float(equity),
                    ts_ms=int(ts_ms),
                )
                or {}
            )
        except Exception as exc:
            _warn_nonfatal(
                "GATED_BACKTEST_FUTURES_COST_ESTIMATE_FAILED",
                exc,
                symbol=symbol_text,
                ts_ms=int(ts_ms),
            )
            continue
        if str(trade_cost.get("status") or "") != "estimated_futures":
            trade_cost.setdefault("asset_class", "FUTURES")
            trade_costs.append(trade_cost)
            continue
        total_cost += float(trade_cost.get("exec_cost") or 0.0)
        trade_costs.append(trade_cost)

    equity_base = max(1e-12, float(equity))
    return {
        "enabled": True,
        "status": "evaluated",
        "equity": float(equity_base),
        "exec_cost": float(total_cost),
        "cost_return": float(total_cost / equity_base),
        "trade_costs": trade_costs,
    }


def run_gated_backtest(
    predictions: Sequence[float] | np.ndarray,
    realized_returns: Sequence[float] | np.ndarray,
    *,
    sample_times_ms: Sequence[Any] | np.ndarray | None = None,
    symbols: Sequence[Any] | np.ndarray | None = None,
    cost_config: Mapping[str, Any] | None = None,
    max_positions: int | None = None,
    target_weight: float = 1.0,
    model_id: str = "gated_backtest_model",
    alpha_ttl_ms: int = 15 * 60 * 1000,
    alpha_half_life_ms: int = 5 * 60 * 1000,
    con: Any = None,
    trade_suppression_fn: Any = None,
    cost_model: AlmgrenChrissCost | None = None,
) -> Dict[str, Any]:
    """Replay prediction-derived intents through shared live gate code."""
    pred = np.asarray(predictions, dtype=float).reshape(-1)
    realized = np.asarray(realized_returns, dtype=float).reshape(-1)
    if pred.size != realized.size:
        raise ValueError(f"gated_backtest_length_mismatch predictions={int(pred.size)} realized={int(realized.size)}")

    finite_mask = np.isfinite(pred) & np.isfinite(realized)
    pred = pred[finite_mask]
    realized = realized[finite_mask]
    n = int(pred.size)
    times = _normalize_times(sample_times_ms, int(finite_mask.size))
    times = times[finite_mask]
    symbol_values = [value for idx, value in enumerate(_normalize_symbols(symbols, int(finite_mask.size))) if bool(finite_mask[idx])]

    owns_con = con is None
    if con is None:
        con = _sqlite_module().connect(":memory:")

    ordered_indices = sorted(range(n), key=lambda idx: (int(times[idx]), str(symbol_values[idx]), int(idx)))
    previous_weights: Dict[str, float] = {}
    returns: list[float] = []
    frictionless_returns: list[float] = []
    cost_returns: list[float] = []
    futures_cost_returns: list[float] = []
    turnover_rows: list[float] = []
    cost_rows: list[Dict[str, float]] = []
    futures_cost_rows: list[Dict[str, Any]] = []
    selected_symbols_by_ts: list[Dict[str, Any]] = []
    shaped_order_count = 0
    blocked_order_count = 0
    futures_cost_equity = _futures_backtest_equity(cost_config)

    try:
        cursor = 0
        while cursor < len(ordered_indices):
            ts_ms = int(times[ordered_indices[cursor]])
            group_indices: list[int] = []
            while cursor < len(ordered_indices) and int(times[ordered_indices[cursor]]) == ts_ms:
                group_indices.append(int(ordered_indices[cursor]))
                cursor += 1

            desired: Dict[str, Dict[str, Any]] = {}
            realized_by_symbol: Dict[str, list[float]] = {}
            frictionless_group_return = 0.0
            for idx in group_indices:
                symbol = str(symbol_values[idx]).upper().strip()
                prediction = float(pred[idx])
                side = _side_from_prediction(prediction)
                signed_weight = float(math.copysign(abs(prediction) * float(target_weight), 1.0 if side == "BUY" else -1.0))
                desired[symbol] = {
                    "symbol": symbol,
                    "side": side,
                    "weight": abs(float(signed_weight)),
                    "signed_weight": float(signed_weight),
                    "score": abs(prediction),
                    "prediction": prediction,
                    "signal_ts_ms": int(ts_ms),
                }
                realized_by_symbol.setdefault(symbol, []).append(float(realized[idx]))
                frictionless_group_return += float(np.sign(prediction) * float(realized[idx]))

            selected = apply_max_position_constraint(desired, max_positions=max_positions)
            orders: list[Dict[str, Any]] = []
            for symbol, row in selected.items():
                signed_weight = float(row.get("signed_weight") or 0.0)
                side = "BUY" if signed_weight >= 0.0 else "SELL"
                orders.append(
                    {
                        "symbol": str(symbol),
                        "side": side,
                        "qty": 0.0,
                        "to_weight": abs(float(signed_weight)),
                        "delta_weight": 0.0,
                        "confidence": min(1.0, max(0.01, abs(float(row.get("prediction") or 0.0)))),
                        "expected_z": float(row.get("prediction") or 0.0),
                        "zscore": float(row.get("prediction") or 0.0),
                        "volatility": 0.0,
                        "true_spread_bps": 0.0,
                        "spread_bps": 0.0,
                        "entry_spread_bps": 0.0,
                        "intraday_vol_bps": 0.0,
                        "vol_bps": 0.0,
                        "adv_participation": 0.0,
                        "live_participation_rate": 0.0,
                        "model_id": str(model_id or "gated_backtest_model"),
                        "signal_ts_ms": int(ts_ms),
                        "alpha_ttl_ms": int(alpha_ttl_ms),
                        "alpha_half_life_ms": int(alpha_half_life_ms),
                    }
                )

            shaped = apply_execution_policy(
                intents=orders,
                con=con,
                actor="gated_backtest",
                mode="backtest",
                broker="sim",
                default_signal_ts_ms=int(ts_ms),
                now_ms=int(ts_ms),
                initialize_storage=False,
                execution_allowed_fn=_neutral_execution_allowed,
                trade_suppression_fn=trade_suppression_fn or _safe_trade_suppression,
                capital_preservation_fn=_neutral_capital_preservation,
                execution_mode_fn=lambda: "backtest",
                risk_state_getter_fn=lambda _key, default=None: default,
                regime_compatibility_fn=lambda _con, _symbol, _signal_ts, _order: (1.0, None),
                execution_feedback_fn=lambda _con, **_kwargs: {
                    "sample_n": 0,
                    "avg_realized_slippage_bps": 0.0,
                    "avg_slippage_error_bps": 0.0,
                    "avg_latency_ms": 0.0,
                    "avg_fill_quality_score": 0.65,
                },
            )
            shaped_order_count += int(len(shaped))
            blocked_order_count += int(max(0, len(orders) - len(shaped)))

            broker_result = simulate_weight_order_batch(
                orders=list(shaped),
                realized_returns_by_symbol=realized_by_symbol,
                previous_weights=previous_weights,
                cost_config=dict(cost_config or {}),
                cost_model=cost_model,
            )
            selected_weights = {
                str(symbol).upper().strip(): float(weight)
                for symbol, weight in dict(broker_result.get("weights") or {}).items()
            }
            turnover = float(broker_result.get("turnover") or 0.0)
            components = dict(broker_result.get("costs") or {})
            cost_return = float(components.get("cost_return") or 0.0)
            futures_cost = {
                "enabled": False,
                "status": "disabled",
                "cost_return": 0.0,
                "trade_costs": [],
            }
            if _should_apply_futures_costs(cost_config, previous_weights, selected_weights):
                futures_cost = _estimate_futures_transition_costs(
                    con=con,
                    previous_weights=previous_weights,
                    selected_weights=selected_weights,
                    equity=float(futures_cost_equity),
                    ts_ms=int(ts_ms),
                    force_metadata_lookup=(_cost_config_asset_class(cost_config) == "FUTURES"),
                )
            futures_cost_return = float(futures_cost.get("cost_return") or 0.0)
            returns.append(float(broker_result.get("net_return") or 0.0) - float(futures_cost_return))
            frictionless_returns.append(float(frictionless_group_return))
            cost_returns.append(float(cost_return))
            futures_cost_returns.append(float(futures_cost_return))
            turnover_rows.append(float(turnover))
            cost_rows.append(dict(components))
            futures_cost_rows.append(dict(futures_cost))
            previous_weights = dict(selected_weights)
            selected_symbols_by_ts.append(
                {
                    "ts_ms": int(ts_ms),
                    "input_symbols": sorted(desired.keys()),
                    "selected_symbols": sorted(selected_weights.keys()),
                    "excluded_symbols": sorted(set(desired.keys()) - set(selected.keys())),
                    "blocked_symbols": sorted(set(selected.keys()) - set(selected_weights.keys())),
                }
            )
    finally:
        if owns_con:
            try:
                con.close()
            except Exception as exc:
                _warn_nonfatal("GATED_BACKTEST_CONNECTION_CLOSE_FAILED", exc)

    returns_arr = np.asarray(returns, dtype=float)
    frictionless_arr = np.asarray(frictionless_returns, dtype=float)
    total_return = float(np.sum(returns_arr)) if returns_arr.size else 0.0
    frictionless_total = float(np.sum(frictionless_arr)) if frictionless_arr.size else 0.0
    return {
        "ok": True,
        "status": "evaluated",
        "returns": [float(value) for value in returns_arr.tolist()],
        "cost_adjusted_returns": [float(value) for value in returns_arr.tolist()],
        "frictionless_returns": [float(value) for value in frictionless_arr.tolist()],
        "cost_returns": [float(value) for value in cost_returns],
        "futures_cost_returns": [float(value) for value in futures_cost_returns],
        "turnover": [float(value) for value in turnover_rows],
        "costs": {
            "components": cost_rows,
            "futures_components": futures_cost_rows,
            "total_turnover": float(sum(turnover_rows)),
            "total_broker_cost_return": float(sum(cost_returns)),
            "total_futures_cost_return": float(sum(futures_cost_returns)),
            "total_cost_return": float(sum(cost_returns) + sum(futures_cost_returns)),
        },
        "selected_symbols_by_ts": selected_symbols_by_ts,
        "diagnostics": {
            "metric_basis": "gated_cost_adjusted",
            "n_input_observations": int(predictions.__len__() if hasattr(predictions, "__len__") else n),
            "n_observations": int(n),
            "n_time_buckets": int(len(returns)),
            "max_positions": (None if max_positions is None else int(max_positions)),
            "shaped_order_count": int(shaped_order_count),
            "blocked_order_count": int(blocked_order_count),
            "total_return": float(total_return),
            "frictionless_total_return": float(frictionless_total),
            "total_return_gap": float(total_return - frictionless_total),
        },
    }


__all__ = ["run_gated_backtest"]
