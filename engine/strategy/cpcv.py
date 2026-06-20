"""Combinatorial purged cross-validation utilities and CPCV job helpers."""

from __future__ import annotations

import itertools
import logging
import math
import os
from typing import Any, Callable, Dict, Mapping, Sequence

import numpy as np

from engine.backtest.deflated_sharpe import deflated_sharpe_ratio
from engine.execution.cost_models.almgren_chriss import AlmgrenChrissCost
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect_ro, init_db, record_backtest_cpcv_run
from engine.strategy.gated_backtest import run_gated_backtest


LOG = logging.getLogger("engine.strategy.cpcv")
_EPS = 1e-12


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.cpcv",
        extra=extra or None,
        persist=False,
    )


def _safe_bool_env(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "y", "on"}


def _safe_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    if raw in (None, ""):
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _safe_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    if raw in (None, ""):
        return float(default)
    try:
        value = float(raw)
    except Exception:
        return float(default)
    return float(value) if math.isfinite(value) else float(default)


def _table_exists(con, table: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table),),
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _contiguous_segments(indices: Sequence[int] | np.ndarray) -> list[tuple[int, int]]:
    values = np.asarray(indices, dtype=int).reshape(-1)
    if values.size <= 0:
        return []
    values = np.unique(np.sort(values))
    starts = [int(values[0])]
    ends: list[int] = []
    for left, right in zip(values[:-1], values[1:]):
        if int(right) != int(left) + 1:
            ends.append(int(left))
            starts.append(int(right))
    ends.append(int(values[-1]))
    return list(zip(starts, ends))


def _compute_sharpe(values: Sequence[float] | np.ndarray) -> float:
    arr = np.asarray(values, dtype=float).reshape(-1)
    arr = arr[np.isfinite(arr)]
    if arr.size <= 1:
        return 0.0
    mean = float(arr.mean())
    std = float(arr.std(ddof=1))
    if std <= _EPS:
        if mean > 0.0:
            return 10.0
        if mean < 0.0:
            return -10.0
        return 0.0
    return float((mean / std) * math.sqrt(float(arr.size)))


def _compute_sortino(values: Sequence[float] | np.ndarray) -> float:
    arr = np.asarray(values, dtype=float).reshape(-1)
    arr = arr[np.isfinite(arr)]
    if arr.size <= 1:
        return 0.0
    downside = arr[arr < 0.0]
    if downside.size <= 1:
        return 10.0 if float(arr.mean()) > 0.0 else 0.0
    downside_std = float(downside.std(ddof=1))
    if downside_std <= _EPS:
        return 10.0 if float(arr.mean()) > 0.0 else 0.0
    return float((float(arr.mean()) / downside_std) * math.sqrt(float(arr.size)))


def _total_return(values: Sequence[float] | np.ndarray) -> float:
    arr = np.asarray(values, dtype=float).reshape(-1)
    arr = arr[np.isfinite(arr)]
    if arr.size <= 0:
        return 0.0
    return float(np.prod(1.0 + arr) - 1.0)


def _max_drawdown(values: Sequence[float] | np.ndarray) -> float:
    arr = np.asarray(values, dtype=float).reshape(-1)
    arr = arr[np.isfinite(arr)]
    if arr.size <= 0:
        return 0.0
    equity = np.cumprod(1.0 + arr)
    peak = np.maximum.accumulate(equity)
    drawdown = (equity / np.maximum(peak, _EPS)) - 1.0
    return float(abs(np.min(drawdown)))


def _metrics_from_returns(values: Sequence[float] | np.ndarray) -> Dict[str, float]:
    arr = np.asarray(values, dtype=float).reshape(-1)
    arr = arr[np.isfinite(arr)]
    total = _total_return(arr)
    drawdown = _max_drawdown(arr)
    return {
        "n": int(arr.size),
        "mean_return": float(arr.mean()) if arr.size else 0.0,
        "total_return": float(total),
        "sharpe": float(_compute_sharpe(arr)),
        "sortino": float(_compute_sortino(arr)),
        "max_drawdown": float(drawdown),
        "calmar": float(total / drawdown) if drawdown > _EPS else (10.0 if total > 0.0 else 0.0),
    }


def _returns_from_predictions(predictions: np.ndarray, realized: np.ndarray) -> np.ndarray:
    pred = np.asarray(predictions, dtype=float).reshape(-1)
    y = np.asarray(realized, dtype=float).reshape(-1)
    if pred.size != y.size:
        raise ValueError("prediction_and_realized_size_mismatch")
    return np.sign(pred) * y


def _default_commission_bps(asset_class: str) -> float:
    explicit = os.environ.get("CPCV_COMMISSION_BPS")
    if explicit not in (None, ""):
        return max(0.0, _safe_float_env("CPCV_COMMISSION_BPS", 0.0))
    asset = str(asset_class or "").upper().strip()
    if "CRYPTO" in asset:
        default = _safe_float_env(
            "CPCV_CRYPTO_COMMISSION_BPS",
            _safe_float_env("CPCV_CRYPTO_TAKER_BPS", _safe_float_env("CPCV_CRYPTO_MAKER_BPS", 0.0)),
        )
        return max(0.0, float(default))
    return max(0.0, _safe_float_env("CPCV_EQUITY_COMMISSION_BPS", 1.0))


def cpcv_cost_config_from_env(config: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    """Resolve transaction-cost defaults for CPCV promotion backtests."""
    cfg = dict(config or {})
    asset_class = str(cfg.get("asset_class") or os.environ.get("CPCV_ASSET_CLASS") or "US_EQUITY").strip().upper()
    if not asset_class:
        asset_class = "US_EQUITY"
    sigma_default = _safe_float_env("CPCV_SIGMA_DAILY_BPS", _safe_float_env("CPCV_SIGMA_DAILY", 200.0))
    out = {
        "enabled": bool(cfg.get("enabled", _safe_bool_env("CPCV_COSTS_ENABLED", True))),
        "asset_class": asset_class,
        "commission_bps": float(cfg.get("commission_bps", _default_commission_bps(asset_class))),
        "half_spread_bps": float(cfg.get("half_spread_bps", _safe_float_env("CPCV_HALF_SPREAD_BPS", 1.0))),
        "notional": float(cfg.get("notional", _safe_float_env("CPCV_TRADE_NOTIONAL", 100_000.0))),
        "adv": float(cfg.get("adv", _safe_float_env("CPCV_ADV", 10_000_000.0))),
        "sigma_daily": float(cfg.get("sigma_daily", sigma_default)),
        "participation": float(cfg.get("participation", _safe_float_env("CPCV_PARTICIPATION", 0.10))),
    }
    out["commission_bps"] = max(0.0, float(out["commission_bps"]))
    out["half_spread_bps"] = max(0.0, float(out["half_spread_bps"]))
    out["notional"] = max(0.0, float(out["notional"]))
    out["adv"] = max(_EPS, float(out["adv"]))
    out["sigma_daily"] = max(0.0, float(out["sigma_daily"]))
    out["participation"] = max(0.0, min(1.0, float(out["participation"])))
    return out


def _cost_components_for_turnover(
    turnover: float,
    *,
    cost_config: Mapping[str, Any],
    cost_model: AlmgrenChrissCost | None = None,
) -> Dict[str, float]:
    turnover_f = max(0.0, float(turnover or 0.0))
    if turnover_f <= _EPS or not bool(cost_config.get("enabled", True)):
        return {
            "turnover": float(turnover_f),
            "commission_bps": 0.0,
            "half_spread_bps": 0.0,
            "temporary_impact_bps": 0.0,
            "permanent_impact_bps_excluded": 0.0,
            "total_cost_bps": 0.0,
            "cost_return": 0.0,
        }
    model = cost_model or AlmgrenChrissCost()
    base_notional = max(0.0, float(cost_config.get("notional") or 0.0))
    trade_notional = float(base_notional) * float(turnover_f)
    components = model.components_bps(
        notional=float(trade_notional),
        adv=max(_EPS, float(cost_config.get("adv") or 0.0)),
        sigma_daily=max(0.0, float(cost_config.get("sigma_daily") or 0.0)),
        participation=max(0.0, min(1.0, float(cost_config.get("participation") or 0.0))),
        half_spread_bps=0.0,
        asset_class=str(cost_config.get("asset_class") or "US_EQUITY"),
    )
    commission_bps = max(0.0, float(cost_config.get("commission_bps") or 0.0))
    half_spread_bps = max(0.0, float(cost_config.get("half_spread_bps") or 0.0))
    temporary_bps = max(0.0, float(components.get("temporary_impact_bps") or 0.0))
    total_bps = float(commission_bps + half_spread_bps + temporary_bps)
    return {
        "turnover": float(turnover_f),
        "commission_bps": float(commission_bps),
        "half_spread_bps": float(half_spread_bps),
        "temporary_impact_bps": float(temporary_bps),
        "permanent_impact_bps_excluded": float(components.get("permanent_impact_bps") or 0.0),
        "total_cost_bps": float(total_bps),
        "cost_return": float(turnover_f * total_bps / 10000.0),
    }


def _apply_transaction_costs_to_returns(
    predictions: Sequence[float] | np.ndarray,
    realized: Sequence[float] | np.ndarray,
    *,
    cost_config: Mapping[str, Any] | None = None,
    initial_position: float = 0.0,
) -> tuple[np.ndarray, Dict[str, Any]]:
    pred = np.asarray(predictions, dtype=float).reshape(-1)
    y = np.asarray(realized, dtype=float).reshape(-1)
    if pred.size != y.size:
        raise ValueError("prediction_and_realized_size_mismatch")
    cfg = cpcv_cost_config_from_env(cost_config)
    positions = np.sign(pred)
    frictionless = positions * y
    adjusted = np.asarray(frictionless, dtype=float).copy()
    cost_returns: list[float] = []
    turnovers: list[float] = []
    component_rows: list[Dict[str, float]] = []
    previous = float(np.sign(initial_position))
    for idx, position in enumerate(positions):
        turnover = abs(float(position) - float(previous))
        components = _cost_components_for_turnover(turnover, cost_config=cfg)
        cost_return = float(components.get("cost_return") or 0.0)
        adjusted[idx] = float(adjusted[idx]) - float(cost_return)
        turnovers.append(float(turnover))
        cost_returns.append(float(cost_return))
        component_rows.append(components)
        previous = float(position)
    return adjusted, {
        "enabled": bool(cfg.get("enabled")),
        "config": dict(cfg),
        "positions": [float(value) for value in positions.tolist()],
        "turnover": [float(value) for value in turnovers],
        "cost_returns": [float(value) for value in cost_returns],
        "components": component_rows,
        "total_turnover": float(np.sum(np.asarray(turnovers, dtype=float))) if turnovers else 0.0,
        "total_cost_return": float(np.sum(np.asarray(cost_returns, dtype=float))) if cost_returns else 0.0,
        "frictionless_returns": [float(value) for value in frictionless.tolist()],
        "cost_adjusted_returns": [float(value) for value in adjusted.tolist()],
    }


def _retrain_cadence_ms_from_env() -> int:
    explicit = _safe_int_env("CPCV_RETRAIN_CADENCE_MS", 0)
    if explicit > 0:
        return int(explicit)
    return int(max(1, _safe_int_env("MODEL_LIFECYCLE_RETRAIN_INTERVAL_MS", 6 * 60 * 60 * 1000)))


def _predict_with_retrain_cadence(
    *,
    X: np.ndarray,
    y: np.ndarray,
    train_idx: np.ndarray,
    test_idx: np.ndarray,
    sample_times_ms: np.ndarray,
    model_factory: Callable[[], Any],
    retrain_cadence_ms: int,
    min_train_samples: int,
) -> tuple[np.ndarray, np.ndarray, Dict[str, Any]]:
    ordered_test = np.asarray(test_idx, dtype=int)
    if ordered_test.size <= 0:
        return ordered_test, np.asarray([], dtype=float), {"enabled": True, "fit_count": 0, "fit_events": []}
    order = np.argsort(sample_times_ms[ordered_test], kind="stable")
    ordered_test = ordered_test[order]

    train_all = np.asarray(train_idx, dtype=int)
    train_times = np.asarray(sample_times_ms[train_all], dtype=float)
    predictions: list[float] = []
    kept_indices: list[int] = []
    fit_events: list[Dict[str, Any]] = []
    skipped_no_history = 0
    model = None
    last_fit_ts: float | None = None
    cadence = int(retrain_cadence_ms or 0)

    for sample_idx in ordered_test:
        decision_ts = float(sample_times_ms[int(sample_idx)])
        needs_fit = model is None or cadence <= 0 or last_fit_ts is None or (decision_ts - float(last_fit_ts)) >= float(cadence)
        if needs_fit:
            eligible = train_all[train_times < decision_ts]
            eligible = np.sort(np.unique(eligible.astype(int, copy=False)))
            if eligible.size < int(min_train_samples):
                if model is None:
                    skipped_no_history += 1
                    continue
            else:
                model = model_factory()
                model.fit(X[eligible], y[eligible])
                last_fit_ts = float(decision_ts)
                if len(fit_events) < 100:
                    fit_events.append(
                        {
                            "decision_ts_ms": int(decision_ts),
                            "train_size": int(eligible.size),
                            "train_min_ts_ms": int(float(np.min(sample_times_ms[eligible]))),
                            "train_max_ts_ms": int(float(np.max(sample_times_ms[eligible]))),
                        }
                    )

        if model is None:
            skipped_no_history += 1
            continue
        pred = np.asarray(model.predict(X[[int(sample_idx)]]), dtype=float).reshape(-1)
        if pred.size <= 0 or not math.isfinite(float(pred[0])):
            continue
        kept_indices.append(int(sample_idx))
        predictions.append(float(pred[0]))

    return (
        np.asarray(kept_indices, dtype=int),
        np.asarray(predictions, dtype=float),
        {
            "enabled": True,
            "cadence_ms": int(cadence),
            "fit_count": int(len(fit_events)),
            "fit_events": fit_events,
            "skipped_no_history": int(skipped_no_history),
            "min_train_samples": int(min_train_samples),
        },
    )


def make_cpcv_splits(
    n_samples: int,
    n_splits: int,
    n_test_splits: int,
) -> list[tuple[np.ndarray, np.ndarray]]:
    """Build combinatorial purged cross-validation train and test splits."""
    total_samples = int(max(0, int(n_samples or 0)))
    split_count = int(max(0, int(n_splits or 0)))
    test_split_count = int(max(0, int(n_test_splits or 0)))
    if total_samples <= 0 or split_count < 2 or total_samples < split_count:
        return []
    if test_split_count <= 0 or test_split_count >= split_count:
        return []

    groups = [np.asarray(chunk, dtype=int) for chunk in np.array_split(np.arange(total_samples, dtype=int), split_count)]
    if any(group.size <= 0 for group in groups):
        return []

    splits: list[tuple[np.ndarray, np.ndarray]] = []
    for combo in itertools.combinations(range(split_count), test_split_count):
        test_idx = np.concatenate([groups[group_idx] for group_idx in combo]).astype(int, copy=False)
        train_idx = np.concatenate(
            [groups[group_idx] for group_idx in range(split_count) if group_idx not in combo]
        ).astype(int, copy=False)
        splits.append((np.sort(train_idx), np.sort(test_idx)))
    return splits


def purge_train_indices(
    train_idx: Sequence[int] | np.ndarray,
    test_idx: Sequence[int] | np.ndarray,
    label_horizon: int,
) -> np.ndarray:
    """Drop train rows whose label horizon would overlap the leading edge of a test block."""
    train = np.unique(np.asarray(train_idx, dtype=int).reshape(-1))
    test = np.unique(np.asarray(test_idx, dtype=int).reshape(-1))
    horizon = int(max(0, int(label_horizon or 0)))
    if train.size <= 0 or test.size <= 0 or horizon <= 0:
        return train

    keep = np.ones(train.shape[0], dtype=bool)
    for start, _end in _contiguous_segments(test):
        purge_start = int(start) - horizon
        purge_end = int(start) - 1
        if purge_end < purge_start:
            continue
        keep &= ~((train >= purge_start) & (train <= purge_end))
    return train[keep]


def embargo_train_indices(
    train_idx: Sequence[int] | np.ndarray,
    test_idx: Sequence[int] | np.ndarray,
    embargo_pct: float,
) -> np.ndarray:
    """Drop post-test train rows that fall inside the configured embargo window."""
    train = np.unique(np.asarray(train_idx, dtype=int).reshape(-1))
    test = np.unique(np.asarray(test_idx, dtype=int).reshape(-1))
    embargo_fraction = float(max(0.0, float(embargo_pct or 0.0)))
    if train.size <= 0 or test.size <= 0 or embargo_fraction <= 0.0:
        return train

    total_samples = int(max(int(train.max(initial=0)), int(test.max(initial=0))) + 1)
    embargo_len = int(math.ceil(float(total_samples) * embargo_fraction))
    if embargo_len <= 0:
        return train

    keep = np.ones(train.shape[0], dtype=bool)
    for _start, end in _contiguous_segments(test):
        embargo_start = int(end) + 1
        embargo_end = int(end) + embargo_len
        keep &= ~((train >= embargo_start) & (train <= embargo_end))
    return train[keep]


def compute_pbo(
    in_sample_scores: Sequence[float] | Sequence[Sequence[float]],
    out_of_sample_scores: Sequence[float] | Sequence[Sequence[float]],
) -> Dict[str, Any]:
    """Estimate the probability of backtest overfitting from CPCV score matrices."""
    ins = np.asarray(in_sample_scores, dtype=float)
    outs = np.asarray(out_of_sample_scores, dtype=float)
    if ins.size <= 0 or outs.size <= 0:
        return {
            "ok": False,
            "status": "insufficient_inputs",
            "pbo": 1.0,
            "n_observations": 0,
            "selected_count": 0,
            "oos_rank_percentiles": [],
            "logits": [],
        }
    if ins.shape != outs.shape:
        return {
            "ok": False,
            "status": "shape_mismatch",
            "pbo": 1.0,
            "n_observations": int(ins.size),
            "selected_count": 0,
            "oos_rank_percentiles": [],
            "logits": [],
        }

    if ins.ndim == 1 or (ins.ndim == 2 and ins.shape[1] <= 1):
        flat_in = ins.reshape(-1)
        flat_out = outs.reshape(-1)
        valid = np.isfinite(flat_in) & np.isfinite(flat_out)
        if not bool(np.any(valid)):
            return {
                "ok": False,
                "status": "insufficient_inputs",
                "pbo": 1.0,
                "n_observations": 0,
                "selected_count": 0,
                "oos_rank_percentiles": [],
                "logits": [],
            }
        selected = valid & (flat_in > 0.0)
        if not bool(np.any(selected)):
            selected = valid
        chosen_out = flat_out[selected]
        pbo = float(np.mean(chosen_out <= 0.0)) if chosen_out.size > 0 else 1.0
        return {
            "ok": True,
            "status": "single_series_proxy",
            "pbo": float(max(0.0, min(1.0, pbo))),
            "n_observations": int(valid.sum()),
            "selected_count": int(chosen_out.size),
            "oos_rank_percentiles": [],
            "logits": [],
        }

    if ins.ndim != 2:
        return {
            "ok": False,
            "status": "unsupported_rank",
            "pbo": 1.0,
            "n_observations": int(ins.size),
            "selected_count": 0,
            "oos_rank_percentiles": [],
            "logits": [],
        }

    eps = 1e-6
    percentiles: list[float] = []
    logits: list[float] = []
    selected_rows = 0

    for row_in, row_out in zip(ins, outs):
        valid = np.isfinite(row_in) & np.isfinite(row_out)
        if int(valid.sum()) < 2:
            continue
        row_in_valid = row_in[valid]
        row_out_valid = row_out[valid]
        best_idx = int(np.argmax(row_in_valid))
        chosen_out = float(row_out_valid[best_idx])
        better = int(np.sum(row_out_valid > chosen_out))
        tied = int(np.sum(np.isclose(row_out_valid, chosen_out)))
        percentile = 1.0 - ((float(better) + (0.5 * float(max(0, tied - 1)))) / float(row_out_valid.size))
        percentile = float(min(1.0 - eps, max(eps, percentile)))
        percentiles.append(percentile)
        logits.append(float(math.log(percentile / (1.0 - percentile))))
        selected_rows += 1

    if selected_rows <= 0:
        return {
            "ok": False,
            "status": "insufficient_rank_rows",
            "pbo": 1.0,
            "n_observations": int(ins.shape[0]),
            "selected_count": 0,
            "oos_rank_percentiles": [],
            "logits": [],
        }

    pbo = float(np.mean(np.asarray(logits, dtype=float) <= 0.0))
    return {
        "ok": True,
        "status": "evaluated",
        "pbo": float(max(0.0, min(1.0, pbo))),
        "n_observations": int(ins.shape[0]),
        "selected_count": int(selected_rows),
        "oos_rank_percentiles": [float(value) for value in percentiles],
        "logits": [float(value) for value in logits],
    }


def cpcv_backtest(
    features: Sequence[Sequence[float]] | np.ndarray,
    labels: Sequence[float] | np.ndarray,
    model_factory: Callable[[], Any],
    n_splits: int,
    n_test_splits: int,
    embargo_pct: float,
    label_horizon: int,
    *,
    sample_times_ms: Sequence[int | float] | np.ndarray | None = None,
    cost_config: Mapping[str, Any] | None = None,
    replay_retrain_cadence: bool = False,
    retrain_cadence_ms: int | None = None,
    min_train_samples: int = 2,
    gated_backtest: bool = False,
    symbols: Sequence[Any] | np.ndarray | None = None,
) -> Dict[str, Any]:
    """Run CPCV backtesting over feature and label arrays."""
    try:
        X = np.asarray(features, dtype=float)
    except Exception as e:
        return {
            "ok": False,
            "status": "invalid_features",
            "n_paths": 0,
            "mean_sharpe": 0.0,
            "median_sharpe": 0.0,
            "pbo": 1.0,
            "paths": [],
            "diagnostics": {"error": f"{type(e).__name__}:{e}"},
        }

    y = np.asarray(labels, dtype=float).reshape(-1)
    if X.ndim == 1:
        X = X.reshape(-1, 1)
    if X.ndim != 2:
        return {
            "ok": False,
            "status": "invalid_features_rank",
            "n_paths": 0,
            "mean_sharpe": 0.0,
            "median_sharpe": 0.0,
            "pbo": 1.0,
            "paths": [],
            "diagnostics": {},
        }
    if X.shape[0] != y.shape[0]:
        return {
            "ok": False,
            "status": "length_mismatch",
            "n_paths": 0,
            "mean_sharpe": 0.0,
            "median_sharpe": 0.0,
            "pbo": 1.0,
            "paths": [],
            "diagnostics": {"n_features": int(X.shape[0]), "n_labels": int(y.shape[0])},
        }

    if sample_times_ms is not None:
        sample_times = np.asarray(sample_times_ms, dtype=float).reshape(-1)
        if sample_times.size != y.shape[0]:
            return {
                "ok": False,
                "status": "sample_time_length_mismatch",
                "n_paths": 0,
                "mean_sharpe": 0.0,
                "median_sharpe": 0.0,
                "pbo": 1.0,
                "paths": [],
                "diagnostics": {"n_sample_times": int(sample_times.size), "n_labels": int(y.shape[0])},
            }
    else:
        sample_times = np.arange(int(y.shape[0]), dtype=float)

    symbol_values: np.ndarray | None = None
    if symbols is not None:
        symbol_values = np.asarray(symbols, dtype=object).reshape(-1)
        if symbol_values.size != y.shape[0]:
            return {
                "ok": False,
                "status": "symbol_length_mismatch",
                "n_paths": 0,
                "mean_sharpe": 0.0,
                "median_sharpe": 0.0,
                "pbo": 1.0,
                "paths": [],
                "diagnostics": {"n_symbols": int(symbol_values.size), "n_labels": int(y.shape[0])},
            }

    finite_mask = np.isfinite(y) & np.all(np.isfinite(X), axis=1) & np.isfinite(sample_times)
    X = X[finite_mask]
    y = y[finite_mask]
    sample_times = sample_times[finite_mask]
    if symbol_values is not None:
        symbol_values = symbol_values[finite_mask]
    n_samples = int(y.shape[0])
    splits = make_cpcv_splits(n_samples, n_splits, n_test_splits)
    if n_samples <= 0 or not splits:
        return {
            "ok": False,
            "status": "insufficient_samples",
            "n_paths": 0,
            "mean_sharpe": 0.0,
            "median_sharpe": 0.0,
            "pbo": 1.0,
            "paths": [],
            "diagnostics": {
                "n_samples": int(n_samples),
                "n_splits": int(n_splits),
                "n_test_splits": int(n_test_splits),
            },
        }

    paths: list[Dict[str, Any]] = []
    in_sample_scores: list[float] = []
    out_of_sample_scores: list[float] = []
    frictionless_out_scores: list[float] = []
    cost_cfg = cpcv_cost_config_from_env(cost_config)
    replay_enabled = bool(replay_retrain_cadence)
    gated_enabled = bool(gated_backtest)
    metric_basis = "gated_cost_adjusted" if gated_enabled else "cost_adjusted"
    resolved_retrain_cadence_ms = int(
        retrain_cadence_ms if retrain_cadence_ms is not None else _retrain_cadence_ms_from_env()
    )

    for path_idx, (train_idx_raw, test_idx) in enumerate(splits):
        train_idx_purged = purge_train_indices(train_idx_raw, test_idx, label_horizon)
        train_idx = embargo_train_indices(train_idx_purged, test_idx, embargo_pct)
        if train_idx.size < 2 or test_idx.size < 1:
            paths.append(
                {
                    "path_idx": int(path_idx),
                    "status": "skipped_insufficient_train",
                    "train_size_raw": int(train_idx_raw.size),
                    "train_size": int(train_idx.size),
                    "test_size": int(test_idx.size),
                    "purged_rows": int(max(0, train_idx_raw.size - train_idx_purged.size)),
                    "embargoed_rows": int(max(0, train_idx_purged.size - train_idx.size)),
                    "returns": [],
                    "sharpe": 0.0,
                    "train_sharpe": 0.0,
                }
            )
            continue

        try:
            model = model_factory()
            model.fit(X[train_idx], y[train_idx])
            train_predictions = np.asarray(model.predict(X[train_idx]), dtype=float).reshape(-1)
            train_frictionless = _returns_from_predictions(train_predictions, y[train_idx])
            train_returns, train_costs = _apply_transaction_costs_to_returns(
                train_predictions,
                y[train_idx],
                cost_config=cost_cfg,
            )
            train_sharpe = float(_compute_sharpe(train_returns))

            replay_meta: Dict[str, Any] = {"enabled": False}
            if replay_enabled:
                effective_test_idx, test_predictions, replay_meta = _predict_with_retrain_cadence(
                    X=X,
                    y=y,
                    train_idx=train_idx,
                    test_idx=test_idx,
                    sample_times_ms=sample_times,
                    model_factory=model_factory,
                    retrain_cadence_ms=int(resolved_retrain_cadence_ms),
                    min_train_samples=int(max(2, int(min_train_samples or 2))),
                )
            else:
                effective_test_idx = np.asarray(test_idx, dtype=int)
                test_predictions = np.asarray(model.predict(X[effective_test_idx]), dtype=float).reshape(-1)

            if effective_test_idx.size <= 0 or test_predictions.size <= 0:
                paths.append(
                    {
                        "path_idx": int(path_idx),
                        "status": "skipped_no_replay_predictions" if replay_enabled else "skipped_no_predictions",
                        "train_size_raw": int(train_idx_raw.size),
                        "train_size": int(train_idx.size),
                        "test_size": int(test_idx.size),
                        "purged_rows": int(max(0, train_idx_raw.size - train_idx_purged.size)),
                        "embargoed_rows": int(max(0, train_idx_purged.size - train_idx.size)),
                        "returns": [],
                        "frictionless_returns": [],
                        "cost_adjusted_returns": [],
                        "sharpe": 0.0,
                        "frictionless_sharpe": 0.0,
                        "train_sharpe": float(train_sharpe),
                        "retrain_replay": dict(replay_meta),
                    }
                )
                continue

            test_frictionless = _returns_from_predictions(test_predictions, y[effective_test_idx])
            gated_meta: Dict[str, Any] = {"enabled": False}
            if gated_enabled:
                gated_result = run_gated_backtest(
                    test_predictions,
                    y[effective_test_idx],
                    sample_times_ms=sample_times[effective_test_idx],
                    symbols=(symbol_values[effective_test_idx] if symbol_values is not None else None),
                    cost_config=cost_cfg,
                    model_id="cpcv_candidate",
                )
                test_returns = np.asarray(gated_result.get("returns") or [], dtype=float).reshape(-1)
                test_frictionless = np.asarray(gated_result.get("frictionless_returns") or [], dtype=float).reshape(-1)
                test_costs = {
                    "gated": dict(gated_result.get("costs") or {}),
                    "diagnostics": dict(gated_result.get("diagnostics") or {}),
                }
                gated_meta = dict(gated_result.get("diagnostics") or {})
                gated_meta["enabled"] = True
            else:
                test_returns, test_costs = _apply_transaction_costs_to_returns(
                    test_predictions,
                    y[effective_test_idx],
                    cost_config=cost_cfg,
                )
            test_sharpe = float(_compute_sharpe(test_returns))
            frictionless_test_sharpe = float(_compute_sharpe(test_frictionless))
        except Exception as e:
            _warn_nonfatal(
                "CPCV_BACKTEST_PATH_FAILED",
                e,
                path_idx=int(path_idx),
                train_size=int(train_idx.size),
                test_size=int(test_idx.size),
            )
            paths.append(
                {
                    "path_idx": int(path_idx),
                    "status": f"path_error:{type(e).__name__}",
                    "train_size_raw": int(train_idx_raw.size),
                    "train_size": int(train_idx.size),
                    "test_size": int(test_idx.size),
                    "purged_rows": int(max(0, train_idx_raw.size - train_idx_purged.size)),
                    "embargoed_rows": int(max(0, train_idx_purged.size - train_idx.size)),
                    "returns": [],
                    "frictionless_returns": [],
                    "cost_adjusted_returns": [],
                    "sharpe": 0.0,
                    "frictionless_sharpe": 0.0,
                    "train_sharpe": 0.0,
                    "error": f"{type(e).__name__}:{e}",
                }
            )
            continue

        in_sample_scores.append(float(train_sharpe))
        out_of_sample_scores.append(float(test_sharpe))
        frictionless_out_scores.append(float(frictionless_test_sharpe))
        cost_adjusted_metrics = _metrics_from_returns(test_returns)
        frictionless_metrics = _metrics_from_returns(test_frictionless)
        paths.append(
            {
                "path_idx": int(path_idx),
                "status": "ok",
                "train_size_raw": int(train_idx_raw.size),
                "train_size": int(train_idx.size),
                "test_size": int(effective_test_idx.size),
                "test_size_raw": int(test_idx.size),
                "purged_rows": int(max(0, train_idx_raw.size - train_idx_purged.size)),
                "embargoed_rows": int(max(0, train_idx_purged.size - train_idx.size)),
                "returns": [float(value) for value in np.asarray(test_returns, dtype=float).reshape(-1)],
                "cost_adjusted_returns": [float(value) for value in np.asarray(test_returns, dtype=float).reshape(-1)],
                "frictionless_returns": [float(value) for value in np.asarray(test_frictionless, dtype=float).reshape(-1)],
                "sharpe": float(test_sharpe),
                "cost_adjusted_sharpe": float(test_sharpe),
                "frictionless_sharpe": float(frictionless_test_sharpe),
                "sortino": float(cost_adjusted_metrics.get("sortino") or 0.0),
                "calmar": float(cost_adjusted_metrics.get("calmar") or 0.0),
                "total_return": float(cost_adjusted_metrics.get("total_return") or 0.0),
                "max_drawdown": float(cost_adjusted_metrics.get("max_drawdown") or 0.0),
                "frictionless_metrics": frictionless_metrics,
                "cost_adjusted_metrics": cost_adjusted_metrics,
                "costs": {
                    "train": dict(train_costs),
                    "test": dict(test_costs),
                },
                "retrain_replay": dict(replay_meta),
                "gated_backtest": dict(gated_meta),
                "train_sharpe": float(train_sharpe),
                "train_frictionless_sharpe": float(_compute_sharpe(train_frictionless)),
                "test_indices": [int(value) for value in np.asarray(effective_test_idx, dtype=int).reshape(-1).tolist()],
            }
        )

    valid_path_rows = [row for row in paths if str(row.get("status") or "") == "ok"]
    out_scores_arr = np.asarray(out_of_sample_scores, dtype=float)
    frictionless_scores_arr = np.asarray(frictionless_out_scores, dtype=float)
    mean_sharpe = float(np.mean(out_scores_arr)) if out_scores_arr.size > 0 else 0.0
    median_sharpe = float(np.median(out_scores_arr)) if out_scores_arr.size > 0 else 0.0
    frictionless_mean_sharpe = float(np.mean(frictionless_scores_arr)) if frictionless_scores_arr.size > 0 else 0.0
    frictionless_median_sharpe = float(np.median(frictionless_scores_arr)) if frictionless_scores_arr.size > 0 else 0.0
    pbo_result = compute_pbo(in_sample_scores, out_of_sample_scores)
    dsr_result = deflated_sharpe_ratio(
        out_of_sample_scores,
        realized_sharpe=median_sharpe,
        n_trials=max(1, len(out_of_sample_scores)),
    )
    ok = bool(valid_path_rows)

    return {
        "ok": bool(ok),
        "status": ("evaluated" if ok else "no_valid_paths"),
        "n_paths": int(len(valid_path_rows)),
        "mean_sharpe": float(mean_sharpe),
        "median_sharpe": float(median_sharpe),
        "frictionless_mean_sharpe": float(frictionless_mean_sharpe),
        "frictionless_median_sharpe": float(frictionless_median_sharpe),
        "deflated_sharpe": dsr_result.to_dict(),
        "pbo": float(pbo_result.get("pbo") or 1.0),
        "paths": valid_path_rows,
        "diagnostics": {
            "metric_basis": str(metric_basis),
            "n_samples": int(n_samples),
            "n_splits": int(n_splits),
            "n_test_splits": int(n_test_splits),
            "embargo_pct": float(embargo_pct),
            "label_horizon": int(max(0, int(label_horizon or 0))),
            "total_paths": int(len(paths)),
            "valid_paths": int(len(valid_path_rows)),
            "skipped_paths": int(max(0, len(paths) - len(valid_path_rows))),
            "in_sample_scores": [float(value) for value in in_sample_scores],
            "out_of_sample_scores": [float(value) for value in out_of_sample_scores],
            "frictionless_out_of_sample_scores": [float(value) for value in frictionless_out_scores],
            "pbo_result": dict(pbo_result),
            "deflated_sharpe": dsr_result.to_dict(),
            "metrics": {
                "cost_adjusted": {
                    "mean_sharpe": float(mean_sharpe),
                    "median_sharpe": float(median_sharpe),
                    "path_sharpes": [float(value) for value in out_of_sample_scores],
                },
                "frictionless": {
                    "mean_sharpe": float(frictionless_mean_sharpe),
                    "median_sharpe": float(frictionless_median_sharpe),
                    "path_sharpes": [float(value) for value in frictionless_out_scores],
                },
            },
            "cost_model": dict(cost_cfg),
            "gated_backtest": {
                "enabled": bool(gated_enabled),
                "metric_basis": str(metric_basis),
            },
            "retrain_cadence_replay": {
                "enabled": bool(replay_enabled),
                "cadence_ms": int(resolved_retrain_cadence_ms),
                "min_train_samples": int(max(2, int(min_train_samples or 2))),
            },
        },
    }


class _LinearPredictionCalibrator:
    def __init__(self, ridge: float = 1e-6):
        self._ridge = float(max(0.0, ridge))
        self._coef: np.ndarray | None = None

    def fit(self, features: np.ndarray, labels: np.ndarray) -> "_LinearPredictionCalibrator":
        X = np.asarray(features, dtype=float)
        y = np.asarray(labels, dtype=float).reshape(-1)
        design = np.concatenate([np.ones((X.shape[0], 1), dtype=float), X], axis=1)
        gram = design.T @ design
        if self._ridge > 0.0:
            ridge = np.eye(gram.shape[0], dtype=float) * self._ridge
            ridge[0, 0] = 0.0
            gram = gram + ridge
        target = design.T @ y
        try:
            coef = np.linalg.solve(gram, target)
        except np.linalg.LinAlgError:
            coef = np.linalg.lstsq(design, y, rcond=None)[0]
        self._coef = np.asarray(coef, dtype=float).reshape(-1)
        return self

    def predict(self, features: np.ndarray) -> np.ndarray:
        if self._coef is None:
            raise RuntimeError("linear_prediction_calibrator_not_fit")
        X = np.asarray(features, dtype=float)
        design = np.concatenate([np.ones((X.shape[0], 1), dtype=float), X], axis=1)
        return np.asarray(design @ self._coef, dtype=float)


def _build_feature_matrix(rows: Sequence[Dict[str, Any]]) -> tuple[np.ndarray, np.ndarray, Dict[str, Any]]:
    if not rows:
        return np.zeros((0, 1), dtype=float), np.zeros((0,), dtype=float), {"feature_columns": []}

    symbols = sorted({str(row.get("symbol") or "").upper().strip() for row in rows if str(row.get("symbol") or "").strip()})
    horizons = sorted({int(row.get("horizon_s") or 0) for row in rows if int(row.get("horizon_s") or 0) > 0})
    vol_states = sorted({str(row.get("volatility_regime") or "unknown").strip().lower() for row in rows})
    trend_states = sorted({str(row.get("trend_regime") or "unknown").strip().lower() for row in rows})
    liquidity_states = sorted({str(row.get("liquidity_regime") or "unknown").strip().lower() for row in rows})

    base_columns = [
        "predicted_z",
        "predicted_x_confidence",
        "confidence",
        "confidence_raw",
        "prediction_strength",
        "horizon_scaled",
    ]
    feature_columns = list(base_columns)

    symbol_to_offset = {value: idx for idx, value in enumerate(symbols[1:], start=len(feature_columns))}
    feature_columns.extend([f"symbol:{value}" for value in symbols[1:]])
    horizon_to_offset = {value: idx for idx, value in enumerate(horizons[1:], start=len(feature_columns))}
    feature_columns.extend([f"horizon:{value}" for value in horizons[1:]])
    vol_to_offset = {value: idx for idx, value in enumerate(vol_states[1:], start=len(feature_columns))}
    feature_columns.extend([f"vol:{value}" for value in vol_states[1:]])
    trend_to_offset = {value: idx for idx, value in enumerate(trend_states[1:], start=len(feature_columns))}
    feature_columns.extend([f"trend:{value}" for value in trend_states[1:]])
    liquidity_to_offset = {
        value: idx for idx, value in enumerate(liquidity_states[1:], start=len(feature_columns))
    }
    feature_columns.extend([f"liq:{value}" for value in liquidity_states[1:]])

    max_horizon = float(max(horizons) if horizons else 1.0)
    X = np.zeros((len(rows), len(feature_columns)), dtype=float)
    y = np.zeros((len(rows),), dtype=float)

    for row_idx, row in enumerate(rows):
        predicted_z = float(row.get("predicted_z") or 0.0)
        confidence = float(row.get("confidence") or 0.0)
        confidence_raw = float(row.get("confidence_raw") or confidence)
        strength = float(row.get("prediction_strength") or (predicted_z * confidence))
        horizon = int(row.get("horizon_s") or 0)
        X[row_idx, 0] = predicted_z
        X[row_idx, 1] = predicted_z * confidence
        X[row_idx, 2] = confidence
        X[row_idx, 3] = confidence_raw
        X[row_idx, 4] = strength
        X[row_idx, 5] = float(horizon) / float(max(max_horizon, 1.0))

        symbol_key = str(row.get("symbol") or "").upper().strip()
        horizon_key = int(horizon)
        vol_key = str(row.get("volatility_regime") or "unknown").strip().lower()
        trend_key = str(row.get("trend_regime") or "unknown").strip().lower()
        liquidity_key = str(row.get("liquidity_regime") or "unknown").strip().lower()

        if symbol_key in symbol_to_offset:
            X[row_idx, symbol_to_offset[symbol_key]] = 1.0
        if horizon_key in horizon_to_offset:
            X[row_idx, horizon_to_offset[horizon_key]] = 1.0
        if vol_key in vol_to_offset:
            X[row_idx, vol_to_offset[vol_key]] = 1.0
        if trend_key in trend_to_offset:
            X[row_idx, trend_to_offset[trend_key]] = 1.0
        if liquidity_key in liquidity_to_offset:
            X[row_idx, liquidity_to_offset[liquidity_key]] = 1.0

        y[row_idx] = float(row.get("realized_z") or 0.0)

    return X, y, {
        "feature_columns": feature_columns,
        "symbols": symbols,
        "horizons": horizons,
        "volatility_regimes": vol_states,
        "trend_regimes": trend_states,
        "liquidity_regimes": liquidity_states,
    }


def _match_prediction_history_rows(con, model_name: str, candidate_version: str) -> tuple[list[tuple[Any, ...]], str]:
    filters = [
        (
            "model_version",
            """
            WHERE COALESCE(NULLIF(TRIM(ph.model_name), ''), '') = ?
              AND COALESCE(NULLIF(TRIM(ph.model_version), ''), '') = ?
            """,
            (str(model_name), str(candidate_version)),
        ),
        (
            "model_id",
            """
            WHERE COALESCE(NULLIF(TRIM(ph.model_name), ''), '') = ?
              AND COALESCE(NULLIF(TRIM(ph.model_id), ''), '') = ?
            """,
            (str(model_name), str(candidate_version)),
        ),
        (
            "model_name",
            """
            WHERE COALESCE(NULLIF(TRIM(ph.model_name), ''), '') = ?
            """,
            (str(model_name),),
        ),
    ]

    select_sql = """
        SELECT
          ph.id,
          ph.ts_ms,
          COALESCE(e.ts_ms, ph.ts_ms) AS event_ts_ms,
          ph.event_id,
          ph.symbol,
          ph.horizon_s,
          ph.predicted_z,
          ph.confidence,
          ph.confidence_raw,
          ph.prediction_strength,
          ph.model_name,
          ph.model_id,
          ph.model_version,
          ph.volatility_regime,
          ph.trend_regime,
          ph.liquidity_regime,
          COALESCE(le.net_z, l.impact_z) AS realized_z
        FROM prediction_history ph
        JOIN labels l
          ON l.event_id = ph.event_id
         AND l.symbol = ph.symbol
         AND l.horizon_s = ph.horizon_s
        LEFT JOIN labels_exec le
          ON le.event_id = l.event_id
         AND le.symbol = l.symbol
         AND le.horizon_s = l.horizon_s
        LEFT JOIN events e
          ON e.id = ph.event_id
        {where_clause}
          AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
        ORDER BY event_ts_ms ASC, ph.ts_ms ASC, ph.id ASC
    """

    for match_mode, where_clause, params in filters:
        rows = con.execute(select_sql.format(where_clause=where_clause), tuple(params)).fetchall() or []
        if rows:
            return list(rows), str(match_mode)
    return [], "none"


def _match_shadow_prediction_rows(con, model_name: str, candidate_version: str) -> tuple[list[tuple[Any, ...]], str]:
    numeric_version: int | None
    try:
        numeric_version = int(candidate_version)
    except Exception:
        numeric_version = None

    clauses = []
    params: list[Any] = [str(model_name)]
    if numeric_version is not None:
        clauses.append("sp.model_ts_ms = ?")
        params.append(int(numeric_version))
    version_clause = (" AND (" + " OR ".join(clauses) + ")") if clauses else ""

    rows = con.execute(
        f"""
        SELECT
          sp.id,
          sp.ts_ms,
          sp.ts_ms AS event_ts_ms,
          sp.event_id,
          sp.symbol,
          sp.horizon_s,
          COALESCE(sp.net_pred_z, sp.predicted_z) AS predicted_z,
          COALESCE(sp.confidence, 0.5) AS confidence,
          COALESCE(sp.confidence, 0.5) AS confidence_raw,
          COALESCE(sp.net_pred_z, sp.predicted_z) * COALESCE(sp.confidence, 0.5) AS prediction_strength,
          sp.model_name,
          NULL AS model_id,
          CAST(sp.model_ts_ms AS TEXT) AS model_version,
          'unknown' AS volatility_regime,
          'unknown' AS trend_regime,
          COALESCE(sp.regime, 'unknown') AS liquidity_regime,
          COALESCE(le.net_z, l.impact_z) AS realized_z
        FROM shadow_predictions sp
        LEFT JOIN labels_exec le
          ON le.event_id = sp.event_id
         AND le.symbol = sp.symbol
         AND le.horizon_s = sp.horizon_s
        LEFT JOIN labels l
          ON l.event_id = sp.event_id
         AND l.symbol = sp.symbol
         AND l.horizon_s = sp.horizon_s
        WHERE sp.model_name = ?
          AND COALESCE(le.net_z, l.impact_z) IS NOT NULL
          {version_clause}
        ORDER BY sp.ts_ms ASC, sp.id ASC
        """,
        tuple(params),
    ).fetchall() or []
    return list(rows), ("shadow_predictions" if rows else "none")


def _load_candidate_prediction_dataset(model_name: str, candidate_version: str) -> Dict[str, Any]:
    init_db()
    con = connect_ro()
    try:
        if (not _table_exists(con, "labels")) or (
            (not _table_exists(con, "prediction_history")) and (not _table_exists(con, "shadow_predictions"))
        ):
            return {"ok": False, "status": "required_tables_missing", "rows": [], "match_mode": "none"}

        rows: list[tuple[Any, ...]] = []
        match_mode = "none"
        if _table_exists(con, "prediction_history"):
            rows, match_mode = _match_prediction_history_rows(con, model_name, candidate_version)
        if not rows and _table_exists(con, "shadow_predictions"):
            rows, match_mode = _match_shadow_prediction_rows(con, model_name, candidate_version)

        deduped: Dict[tuple[int, str, int], Dict[str, Any]] = {}
        for row in rows or []:
            try:
                dedupe_key = (int(row[3] or 0), str(row[4] or "").upper().strip(), int(row[5] or 0))
            except Exception:
                continue
            rec = {
                "prediction_id": int(row[0] or 0),
                "prediction_ts_ms": int(row[1] or 0),
                "event_ts_ms": int(row[2] or 0),
                "event_id": int(row[3] or 0),
                "symbol": str(row[4] or "").upper().strip(),
                "horizon_s": int(row[5] or 0),
                "predicted_z": float(row[6] or 0.0),
                "confidence": float(row[7] or 0.0),
                "confidence_raw": float(row[8] or row[7] or 0.0),
                "prediction_strength": float(row[9] or 0.0),
                "model_name": str(row[10] or ""),
                "model_id": str(row[11] or ""),
                "model_version": str(row[12] or ""),
                "volatility_regime": str(row[13] or "unknown"),
                "trend_regime": str(row[14] or "unknown"),
                "liquidity_regime": str(row[15] or "unknown"),
                "realized_z": float(row[16] or 0.0),
            }
            prev = deduped.get(dedupe_key)
            if prev is None or int(rec["prediction_ts_ms"]) >= int(prev.get("prediction_ts_ms") or 0):
                deduped[dedupe_key] = rec

        prepared_rows = sorted(
            deduped.values(),
            key=lambda item: (
                int(item.get("event_ts_ms") or 0),
                int(item.get("prediction_ts_ms") or 0),
                int(item.get("event_id") or 0),
                str(item.get("symbol") or ""),
                int(item.get("horizon_s") or 0),
            ),
        )
        return {
            "ok": bool(prepared_rows),
            "status": ("loaded" if prepared_rows else "no_candidate_rows"),
            "rows": prepared_rows,
            "match_mode": str(match_mode),
        }
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("CPCV_DATASET_CLOSE_FAILED", e, model_name=str(model_name))


def _resolve_label_horizon(rows: Sequence[Dict[str, Any]]) -> int:
    explicit = _safe_int_env("CPCV_LABEL_HORIZON", 0)
    if explicit > 0:
        return int(explicit)
    horizons = sorted({int(row.get("horizon_s") or 0) for row in rows if int(row.get("horizon_s") or 0) > 0})
    if len(horizons) >= 2 and int(horizons[0]) > 0:
        return int(max(1, math.ceil(float(horizons[-1]) / float(horizons[0]))))
    return 1


def _candidate_identity(model_name: str, candidate_version: str) -> Dict[str, Any]:
    try:
        from engine.strategy.model_lifecycle import get_model_version
    except Exception as e:
        _warn_nonfatal("CPCV_MODEL_VERSION_IMPORT_FAILED", e, model_name=str(model_name))
        return {}

    try:
        version = get_model_version(str(model_name), str(candidate_version))
        return dict(version or {})
    except Exception as e:
        _warn_nonfatal(
            "CPCV_MODEL_VERSION_LOOKUP_FAILED",
            e,
            model_name=str(model_name),
            candidate_version=str(candidate_version),
        )
        return {}


def run_backtest_cpcv_job(
    *,
    model_name: str,
    candidate_version: str,
    n_splits: int | None = None,
    n_test_splits: int | None = None,
    embargo_pct: float | None = None,
    label_horizon: int | None = None,
    replay_retrain_cadence: bool | None = None,
    retrain_cadence_ms: int | None = None,
    cost_config: Mapping[str, Any] | None = None,
    gated_backtest: bool | None = None,
) -> Dict[str, Any]:
    """Load a candidate dataset and run the repo's CPCV backtest workflow."""
    init_db()
    resolved_model_name = str(model_name or "").strip()
    resolved_candidate_version = str(candidate_version or "").strip()
    if not resolved_model_name:
        return {"ok": False, "error": "missing_model_name"}
    if not resolved_candidate_version:
        try:
            from engine.strategy.model_lifecycle import get_latest_version

            latest = get_latest_version(resolved_model_name) or {}
            resolved_candidate_version = str(latest.get("model_version") or "").strip()
        except Exception as e:
            _warn_nonfatal("CPCV_LATEST_VERSION_LOOKUP_FAILED", e, model_name=resolved_model_name)
    if not resolved_candidate_version:
        return {"ok": False, "error": "missing_candidate_version"}

    dataset = _load_candidate_prediction_dataset(resolved_model_name, resolved_candidate_version)
    rows = list(dataset.get("rows") or [])
    resolved_n_splits = int(n_splits if n_splits is not None else _safe_int_env("CPCV_N_SPLITS", 6))
    resolved_n_test_splits = int(
        n_test_splits if n_test_splits is not None else _safe_int_env("CPCV_N_TEST_SPLITS", 2)
    )
    resolved_embargo_pct = float(
        embargo_pct if embargo_pct is not None else _safe_float_env("CPCV_EMBARGO_PCT", 0.01)
    )
    resolved_label_horizon = int(label_horizon if label_horizon is not None else _resolve_label_horizon(rows))
    resolved_replay_retrain = bool(
        replay_retrain_cadence
        if replay_retrain_cadence is not None
        else _safe_bool_env("CPCV_RETRAIN_CADENCE_REPLAY", True)
    )
    resolved_retrain_cadence_ms = int(
        retrain_cadence_ms if retrain_cadence_ms is not None else _retrain_cadence_ms_from_env()
    )
    resolved_gated_backtest = bool(
        gated_backtest
        if gated_backtest is not None
        else _safe_bool_env("CPCV_GATED_BACKTEST", True)
    )

    features, labels, feature_meta = _build_feature_matrix(rows)
    sample_times = [int(row.get("event_ts_ms") or row.get("prediction_ts_ms") or idx) for idx, row in enumerate(rows)]
    symbols = [str(row.get("symbol") or f"ASSET_{idx:05d}") for idx, row in enumerate(rows)]
    cpcv_result = cpcv_backtest(
        features,
        labels,
        model_factory=lambda: _LinearPredictionCalibrator(),
        n_splits=resolved_n_splits,
        n_test_splits=resolved_n_test_splits,
        embargo_pct=resolved_embargo_pct,
        label_horizon=resolved_label_horizon,
        sample_times_ms=sample_times,
        cost_config=cost_config,
        replay_retrain_cadence=resolved_replay_retrain,
        retrain_cadence_ms=resolved_retrain_cadence_ms,
        gated_backtest=resolved_gated_backtest,
        symbols=symbols,
    )
    metric_basis = "gated_cost_adjusted" if resolved_gated_backtest else "cost_adjusted"

    diagnostics = {
        "dataset": {
            "status": str(dataset.get("status") or ""),
            "match_mode": str(dataset.get("match_mode") or ""),
            "row_count": int(len(rows)),
            "symbols": sorted({str(row.get("symbol") or "") for row in rows if str(row.get("symbol") or "")}),
            "horizons": sorted({int(row.get("horizon_s") or 0) for row in rows if int(row.get("horizon_s") or 0) > 0}),
        },
        "feature_meta": dict(feature_meta),
        "candidate": {
            "model_name": str(resolved_model_name),
            "candidate_version": str(resolved_candidate_version),
            "version_info": _candidate_identity(resolved_model_name, resolved_candidate_version),
        },
        "cpcv": dict(cpcv_result.get("diagnostics") or {}),
        "metric_basis": str(metric_basis),
        "frictionless": {
            "mean_sharpe": float(cpcv_result.get("frictionless_mean_sharpe") or 0.0),
            "median_sharpe": float(cpcv_result.get("frictionless_median_sharpe") or 0.0),
        },
        "cost_adjusted": {
            "mean_sharpe": float(cpcv_result.get("mean_sharpe") or 0.0),
            "median_sharpe": float(cpcv_result.get("median_sharpe") or 0.0),
            "deflated_sharpe": dict(cpcv_result.get("deflated_sharpe") or {}),
        },
        "retrain_cadence_replay": {
            "enabled": bool(resolved_replay_retrain),
            "cadence_ms": int(resolved_retrain_cadence_ms),
        },
        "gated_backtest": {
            "enabled": bool(resolved_gated_backtest),
            "metric_basis": str(metric_basis),
        },
    }

    run_id = record_backtest_cpcv_run(
        model_name=resolved_model_name,
        candidate_version=resolved_candidate_version,
        n_splits=int(resolved_n_splits),
        n_test_splits=int(resolved_n_test_splits),
        embargo_pct=float(resolved_embargo_pct),
        path_returns=[list(path.get("returns") or []) for path in list(cpcv_result.get("paths") or [])],
        path_sharpes=[float(path.get("sharpe") or 0.0) for path in list(cpcv_result.get("paths") or [])],
        mean_sharpe=float(cpcv_result.get("mean_sharpe") or 0.0),
        median_sharpe=float(cpcv_result.get("median_sharpe") or 0.0),
        pbo=float(cpcv_result.get("pbo") or 1.0),
        deflated_sharpe=float(dict(cpcv_result.get("deflated_sharpe") or {}).get("deflated_sharpe") or 0.0),
        diagnostics=diagnostics,
    )

    return {
        "ok": bool(cpcv_result.get("ok")),
        "status": str(cpcv_result.get("status") or ""),
        "run_id": int(run_id or 0),
        "model_name": str(resolved_model_name),
        "candidate_version": str(resolved_candidate_version),
        "n_paths": int(cpcv_result.get("n_paths") or 0),
        "mean_sharpe": float(cpcv_result.get("mean_sharpe") or 0.0),
        "median_sharpe": float(cpcv_result.get("median_sharpe") or 0.0),
        "frictionless_mean_sharpe": float(cpcv_result.get("frictionless_mean_sharpe") or 0.0),
        "frictionless_median_sharpe": float(cpcv_result.get("frictionless_median_sharpe") or 0.0),
        "deflated_sharpe": dict(cpcv_result.get("deflated_sharpe") or {}),
        "pbo": float(cpcv_result.get("pbo") or 1.0),
        "diagnostics": diagnostics,
    }


def cpcv_config_from_env() -> Dict[str, Any]:
    """Build CPCV defaults from the current environment."""
    return {
        "enabled": bool(_safe_bool_env("CPCV_ENABLED", False)),
        "n_splits": int(max(2, _safe_int_env("CPCV_N_SPLITS", 6))),
        "n_test_splits": int(max(1, _safe_int_env("CPCV_N_TEST_SPLITS", 2))),
        "embargo_pct": float(max(0.0, _safe_float_env("CPCV_EMBARGO_PCT", 0.01))),
        "label_horizon": int(max(0, _safe_int_env("CPCV_LABEL_HORIZON", 0))),
        "max_pbo": float(max(0.0, _safe_float_env("CPCV_MAX_PBO", 0.5))),
        "min_path_sharpe": float(_safe_float_env("CPCV_MIN_PATH_SHARPE", 0.5)),
        "costs_enabled": bool(_safe_bool_env("CPCV_COSTS_ENABLED", True)),
        "retrain_cadence_replay": bool(_safe_bool_env("CPCV_RETRAIN_CADENCE_REPLAY", True)),
        "retrain_cadence_ms": int(max(1, _retrain_cadence_ms_from_env())),
        "gated_backtest": bool(_safe_bool_env("CPCV_GATED_BACKTEST", True)),
    }


__all__ = [
    "compute_pbo",
    "cpcv_backtest",
    "cpcv_cost_config_from_env",
    "cpcv_config_from_env",
    "embargo_train_indices",
    "make_cpcv_splits",
    "purge_train_indices",
    "run_backtest_cpcv_job",
]
