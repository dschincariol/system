#!/usr/bin/env python3
"""Options GEX/flow feature ablation harness.

This tool is evidence plumbing only. It compares the registry-defined base
options feature set against the same set plus the registry-defined GEX/flow
features under CPCV. The default enablement thresholds are conservative,
operator-tunable guardrails rather than external market facts:

* a row floor prevents small samples from supporting enablement;
* a positive rank-IC delta requires out-of-sample improvement;
* a fold-stability floor rejects one-fold accidents.

Any missing runtime data or missing LightGBM dependency yields an explicit
``ABSTAIN_INSUFFICIENT_DATA`` report, never a pass.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import numpy as np

from engine.backtest.cpcv import CombinatorialPurgedKFold
from engine.strategy import feature_registry

VERDICT_ENABLE_SUPPORTED = "ENABLE_SUPPORTED"
VERDICT_ENABLE_NOT_SUPPORTED = "ENABLE_NOT_SUPPORTED"
VERDICT_ABSTAIN_INSUFFICIENT_DATA = "ABSTAIN_INSUFFICIENT_DATA"
VALID_VERDICTS = frozenset(
    {
        VERDICT_ENABLE_SUPPORTED,
        VERDICT_ENABLE_NOT_SUPPORTED,
        VERDICT_ABSTAIN_INSUFFICIENT_DATA,
    }
)


def __getattr__(name: str) -> Any:
    if name in {"_BASE_OPTIONS_FEATURE_IDS", "_OPTIONS_GEX_FLOW_FEATURE_IDS"}:
        return getattr(feature_registry, name)
    raise AttributeError(name)

DEFAULT_MIN_ROWS = int(os.environ.get("OPTIONS_ABLATION_MIN_ROWS", "500"))
DEFAULT_MIN_RANK_IC_DELTA = float(os.environ.get("OPTIONS_ABLATION_MIN_RANK_IC_DELTA", "0.01"))
DEFAULT_MIN_STABILITY_FRACTION = float(os.environ.get("OPTIONS_ABLATION_MIN_STABILITY_FRACTION", "0.60"))
DEFAULT_MIN_GEX_COVERAGE = float(os.environ.get("OPTIONS_ABLATION_MIN_GEX_COVERAGE", "0.50"))
DEFAULT_SEED = int(os.environ.get("OPTIONS_ABLATION_SEED", "1729"))


@dataclass(frozen=True)
class TrainingDataset:
    X_by_feature: Dict[str, np.ndarray]
    y: np.ndarray
    sample_times_ms: np.ndarray
    label_end_times_ms: np.ndarray
    symbols: List[str]
    source: str
    meta: Dict[str, Any]

    @property
    def rows(self) -> int:
        return int(self.y.shape[0])


def _dedupe(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for value in values or []:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        out.append(text)
        seen.add(text)
    return out


def resolve_feature_sets(core_features: Optional[Sequence[str]] = None) -> Dict[str, List[str]]:
    """Return drift-resistant WITH/WITHOUT lists from the production registry."""

    base = list(feature_registry._BASE_OPTIONS_FEATURE_IDS)
    gex_flow = list(feature_registry._OPTIONS_GEX_FLOW_FEATURE_IDS)
    without = _dedupe([*(core_features or []), *base])
    with_features = _dedupe([*without, *gex_flow])
    return {
        "core": _dedupe(core_features or []),
        "base_options": base,
        "gex_flow": gex_flow,
        "without": without,
        "with": with_features,
    }


def _env_thresholds() -> Dict[str, float]:
    return {
        "min_rows": float(DEFAULT_MIN_ROWS),
        "min_rank_ic_delta": float(DEFAULT_MIN_RANK_IC_DELTA),
        "min_stability_fraction": float(DEFAULT_MIN_STABILITY_FRACTION),
        "min_gex_coverage": float(DEFAULT_MIN_GEX_COVERAGE),
    }


def evaluate_enablement(report: Mapping[str, Any], thresholds: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    """Map ablation metrics to the machine-readable verdict.

    The thresholds are deliberately configurable defaults, not claims about a
    universal IC or Sharpe requirement. ABSTAIN dominates whenever the evidence
    set is too small, unavailable, or the model run did not complete.
    """

    cfg = dict(_env_thresholds())
    cfg.update({str(k): v for k, v in dict(thresholds or {}).items()})
    min_rows = int(float(cfg.get("min_rows", DEFAULT_MIN_ROWS)))
    min_delta = float(cfg.get("min_rank_ic_delta", DEFAULT_MIN_RANK_IC_DELTA))
    min_stability = float(cfg.get("min_stability_fraction", DEFAULT_MIN_STABILITY_FRACTION))
    min_coverage = float(cfg.get("min_gex_coverage", DEFAULT_MIN_GEX_COVERAGE))

    dataset = dict(report.get("dataset") or {})
    metrics = dict(report.get("metrics") or {})
    delta = dict(report.get("delta") or {})
    usable_rows = int(dataset.get("usable_rows") or report.get("usable_rows") or 0)
    gex_coverage = float(dataset.get("gex_flow_nonzero_coverage") or 0.0)
    status = str(report.get("status") or "ok")
    training_status = str(metrics.get("training_status") or report.get("training_status") or status)
    reasons: List[str] = []

    if status not in {"ok", "complete"} or training_status not in {"ok", "complete"}:
        reasons.append(f"run_status:{training_status}")
    if usable_rows < min_rows:
        reasons.append("rows_below_floor")
    if gex_coverage < min_coverage:
        reasons.append("gex_flow_coverage_below_floor")
    if reasons:
        return {
            "verdict": VERDICT_ABSTAIN_INSUFFICIENT_DATA,
            "reasons": reasons,
            "thresholds": {
                "min_rows": int(min_rows),
                "min_rank_ic_delta": float(min_delta),
                "min_stability_fraction": float(min_stability),
                "min_gex_coverage": float(min_coverage),
            },
        }

    rank_ic_delta = float(delta.get("rank_ic_mean") or 0.0)
    stability = float(delta.get("positive_rank_ic_delta_fraction") or 0.0)
    if rank_ic_delta >= min_delta and stability >= min_stability:
        return {
            "verdict": VERDICT_ENABLE_SUPPORTED,
            "reasons": ["rank_ic_delta_supported", "fold_stability_supported"],
            "thresholds": {
                "min_rows": int(min_rows),
                "min_rank_ic_delta": float(min_delta),
                "min_stability_fraction": float(min_stability),
                "min_gex_coverage": float(min_coverage),
            },
        }
    return {
        "verdict": VERDICT_ENABLE_NOT_SUPPORTED,
        "reasons": ["rank_ic_delta_or_stability_below_threshold"],
        "thresholds": {
            "min_rows": int(min_rows),
            "min_rank_ic_delta": float(min_delta),
            "min_stability_fraction": float(min_stability),
            "min_gex_coverage": float(min_coverage),
        },
    }


def _json_loads(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value or ""))
    except Exception:
        return default


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(out)


def _table_available(con, table: str) -> bool:
    table_name = str(table)
    if table_name not in {"labels", "model_feature_snapshots"}:
        return False
    try:
        con.execute(f"SELECT 1 FROM {table_name} LIMIT 1").fetchone()
        return True
    except Exception:
        return False


def _table_columns(con, table: str) -> set[str]:
    table_name = str(table)
    if table_name not in {"labels", "model_feature_snapshots"}:
        return set()
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
        cols = {str(_row_value(row, 1, "name") or "").strip() for row in rows}
        if cols:
            return {col for col in cols if col}
    except Exception:  # no-op-guard: allow - SQLite probe may fail before information_schema fallback.
        pass
    try:
        rows = con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = ?
            """,
            (table_name,),
        ).fetchall() or []
        return {str(_row_value(row, 0, "column_name") or "").strip() for row in rows if _row_value(row, 0, "column_name")}
    except Exception:
        return set()


def _row_value(row: Any, index: int, name: str) -> Any:
    try:
        return row[name]
    except Exception:  # no-op-guard: allow - row mapping probe falls back to positional access.
        pass
    try:
        return row[index]
    except Exception:
        return None


def load_training_matrix(
    con,
    *,
    feature_ids: Sequence[str],
    max_rows: int = 5000,
    lookback_days: Optional[int] = None,
    horizon_s: Optional[int] = None,
) -> TrainingDataset:
    """Load a deterministic label/snapshot matrix from runtime tables."""

    ids = _dedupe(feature_ids)
    if not _table_available(con, "labels") or not _table_available(con, "model_feature_snapshots"):
        return TrainingDataset(
            X_by_feature={fid: np.asarray([], dtype=np.float32) for fid in ids},
            y=np.asarray([], dtype=np.float32),
            sample_times_ms=np.asarray([], dtype=np.float64),
            label_end_times_ms=np.asarray([], dtype=np.float64),
            symbols=[],
            source="runtime_tables",
            meta={"status": "missing_tables"},
        )

    label_columns = _table_columns(con, "labels")
    if "created_at_ms" in label_columns and "ts_ms" in label_columns:
        label_ts_expr = "COALESCE(l.created_at_ms, l.ts_ms, 0)"
        max_ts_expr = "COALESCE(created_at_ms, ts_ms, 0)"
    elif "created_at_ms" in label_columns:
        label_ts_expr = "COALESCE(l.created_at_ms, 0)"
        max_ts_expr = "COALESCE(created_at_ms, 0)"
    elif "ts_ms" in label_columns:
        label_ts_expr = "COALESCE(l.ts_ms, 0)"
        max_ts_expr = "COALESCE(ts_ms, 0)"
    else:
        return TrainingDataset(
            X_by_feature={fid: np.asarray([], dtype=np.float32) for fid in ids},
            y=np.asarray([], dtype=np.float32),
            sample_times_ms=np.asarray([], dtype=np.float64),
            label_end_times_ms=np.asarray([], dtype=np.float64),
            symbols=[],
            source="runtime_tables",
            meta={"status": "missing_label_timestamp_column"},
        )

    try:
        row = con.execute(
            f"""
            SELECT MAX({max_ts_expr})
            FROM labels
            WHERE impact_z IS NOT NULL
            """
        ).fetchone()
        max_ts_ms = int(_row_value(row, 0, "max") or 0)
    except Exception:
        max_ts_ms = 0
    cutoff_ms = 0
    if lookback_days is not None and int(lookback_days) > 0 and max_ts_ms > 0:
        cutoff_ms = int(max_ts_ms - int(lookback_days) * 86_400_000)

    params: List[Any] = [int(cutoff_ms)]
    horizon_clause = ""
    if horizon_s is not None and int(horizon_s) > 0:
        horizon_clause = "AND COALESCE(l.horizon_s, 0) = ?"
        params.append(int(horizon_s))
    params.append(int(max_rows))
    try:
        rows = con.execute(
            f"""
            SELECT
              l.symbol,
              {label_ts_expr} AS label_ts_ms,
              COALESCE(l.horizon_s, 0) AS horizon_s,
              l.impact_z,
              m.ts_ms AS snapshot_ts_ms,
              m.features_json,
              m.feature_ids_json
            FROM labels l
            JOIN model_feature_snapshots m
              ON m.symbol = l.symbol
             AND m.ts_ms = (
                SELECT MAX(m2.ts_ms)
                FROM model_feature_snapshots m2
                WHERE m2.symbol = l.symbol
                  AND m2.ts_ms <= {label_ts_expr}
             )
            WHERE l.impact_z IS NOT NULL
              AND {label_ts_expr} > ?
              {horizon_clause}
            ORDER BY label_ts_ms ASC, l.symbol ASC, m.ts_ms ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall() or []
    except Exception as exc:
        return TrainingDataset(
            X_by_feature={fid: np.asarray([], dtype=np.float32) for fid in ids},
            y=np.asarray([], dtype=np.float32),
            sample_times_ms=np.asarray([], dtype=np.float64),
            label_end_times_ms=np.asarray([], dtype=np.float64),
            symbols=[],
            source="runtime_tables",
            meta={"status": "query_failed", "error": type(exc).__name__},
        )

    y: List[float] = []
    sample_times: List[float] = []
    label_end_times: List[float] = []
    symbols: List[str] = []
    columns: Dict[str, List[float]] = {fid: [] for fid in ids}
    rows_seen = 0
    rows_used = 0
    for row in rows:
        rows_seen += 1
        label_ts = int(_row_value(row, 1, "label_ts_ms") or 0)
        h_s = int(_row_value(row, 2, "horizon_s") or 0)
        target = _finite_float(_row_value(row, 3, "impact_z"), float("nan"))
        if label_ts <= 0 or not math.isfinite(target):
            continue
        features = _json_loads(_row_value(row, 5, "features_json"), {})
        if not isinstance(features, dict):
            continue
        for fid in ids:
            columns[fid].append(_finite_float(features.get(fid), 0.0))
        y.append(float(target))
        sample_times.append(float(label_ts))
        label_end_times.append(float(label_ts + max(0, h_s) * 1000))
        symbols.append(str(_row_value(row, 0, "symbol") or "").upper().strip())
        rows_used += 1

    return TrainingDataset(
        X_by_feature={fid: np.asarray(values, dtype=np.float32) for fid, values in columns.items()},
        y=np.asarray(y, dtype=np.float32),
        sample_times_ms=np.asarray(sample_times, dtype=np.float64),
        label_end_times_ms=np.asarray(label_end_times, dtype=np.float64),
        symbols=symbols,
        source="runtime_tables",
        meta={"status": "ok", "rows_seen": int(rows_seen), "rows_used": int(rows_used)},
    )


def _matrix(dataset: TrainingDataset, feature_ids: Sequence[str]) -> np.ndarray:
    cols = []
    for fid in feature_ids:
        arr = dataset.X_by_feature.get(str(fid))
        if arr is None:
            arr = np.zeros(dataset.rows, dtype=np.float32)
        cols.append(np.asarray(arr, dtype=np.float32).reshape(-1))
    if not cols:
        return np.zeros((dataset.rows, 0), dtype=np.float32)
    return np.column_stack(cols).astype(np.float32, copy=False)


def _rankdata(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=float).reshape(-1)
    order = np.argsort(arr, kind="mergesort")
    ranks = np.empty(arr.shape[0], dtype=float)
    sorted_vals = arr[order]
    start = 0
    while start < arr.shape[0]:
        end = start + 1
        while end < arr.shape[0] and sorted_vals[end] == sorted_vals[start]:
            end += 1
        rank = 0.5 * (start + end - 1) + 1.0
        ranks[order[start:end]] = rank
        start = end
    return ranks


def _corr(a: np.ndarray, b: np.ndarray) -> float:
    x = np.asarray(a, dtype=float).reshape(-1)
    y = np.asarray(b, dtype=float).reshape(-1)
    mask = np.isfinite(x) & np.isfinite(y)
    if int(mask.sum()) < 2:
        return 0.0
    x = x[mask]
    y = y[mask]
    x = x - float(x.mean())
    y = y - float(y.mean())
    denom = float(np.sqrt(np.sum(x * x) * np.sum(y * y)))
    if denom <= 0.0:
        return 0.0
    return float(np.sum(x * y) / denom)


def _score_predictions(y_true: np.ndarray, y_pred: np.ndarray) -> Dict[str, float]:
    y = np.asarray(y_true, dtype=float).reshape(-1)
    p = np.asarray(y_pred, dtype=float).reshape(-1)
    if y.size <= 0:
        return {"rank_ic": 0.0, "pearson": 0.0, "oos_r2": 0.0, "hit_rate": 0.0, "rmse": 0.0}
    residual = y - p
    sse = float(np.sum(residual * residual))
    centered = y - float(y.mean())
    sst = float(np.sum(centered * centered))
    hit = float(np.mean(np.sign(y) == np.sign(p))) if y.size else 0.0
    return {
        "rank_ic": _corr(_rankdata(y), _rankdata(p)),
        "pearson": _corr(y, p),
        "oos_r2": float(1.0 - sse / sst) if sst > 0.0 else 0.0,
        "hit_rate": hit,
        "rmse": float(math.sqrt(max(0.0, sse / max(1, int(y.size))))),
    }


def _aggregate_metric_rows(rows: Sequence[Mapping[str, Any]]) -> Dict[str, float]:
    if not rows:
        return {"folds": 0, "rank_ic_mean": 0.0, "oos_r2_mean": 0.0, "hit_rate_mean": 0.0, "rmse_mean": 0.0}
    out: Dict[str, float] = {"folds": float(len(rows))}
    for source_key, dest_key in (
        ("rank_ic", "rank_ic_mean"),
        ("pearson", "pearson_mean"),
        ("oos_r2", "oos_r2_mean"),
        ("hit_rate", "hit_rate_mean"),
        ("rmse", "rmse_mean"),
    ):
        vals = [float(row.get(source_key) or 0.0) for row in rows]
        out[dest_key] = float(np.mean(vals)) if vals else 0.0
    return out


def _predict_from_blob(blob: bytes, X: np.ndarray) -> np.ndarray:
    from engine.strategy.gbm_regressor import load_gbm_model

    model, _schema = load_gbm_model(blob)
    pred = model.predict(np.asarray(X, dtype=np.float32))
    return np.asarray(pred, dtype=np.float64).reshape(-1)


def _train_blob(
    X: np.ndarray,
    y: np.ndarray,
    feature_ids: Sequence[str],
    hyperparams: Mapping[str, Any],
) -> bytes:
    from engine.strategy.gbm_regressor import train_gbm_model

    return train_gbm_model(X, y, list(feature_ids), dict(hyperparams or {}))


def _model_eval_for_feature_set(
    *,
    dataset: TrainingDataset,
    feature_ids: Sequence[str],
    splitter: CombinatorialPurgedKFold,
    hyperparams: Mapping[str, Any],
    seed: int,
    train_fn: Callable[[np.ndarray, np.ndarray, Sequence[str], Mapping[str, Any]], bytes],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, float]]]:
    X = _matrix(dataset, feature_ids)
    y = np.asarray(dataset.y, dtype=np.float32)
    fold_rows: List[Dict[str, Any]] = []
    predictions: List[Dict[str, float]] = []
    for fold_idx, (train_idx, test_idx) in enumerate(
        splitter.split(
            X,
            y,
            groups={
                "label_start": dataset.sample_times_ms,
                "label_end": dataset.label_end_times_ms,
            },
        )
    ):
        if len(train_idx) <= 0 or len(test_idx) <= 0:
            continue
        params = dict(hyperparams or {})
        params.setdefault("random_state", int(seed) + int(fold_idx))
        blob = train_fn(X[train_idx], y[train_idx], list(feature_ids), params)
        pred = _predict_from_blob(blob, X[test_idx])
        scores = _score_predictions(y[test_idx], pred)
        row = {
            "fold": int(fold_idx),
            "train_rows": int(len(train_idx)),
            "test_rows": int(len(test_idx)),
            **scores,
        }
        fold_rows.append(row)
        for local_idx, pred_value in zip(test_idx, pred):
            predictions.append(
                {
                    "row": int(local_idx),
                    "fold": int(fold_idx),
                    "prediction": float(pred_value),
                    "target": float(y[int(local_idx)]),
                }
            )
    metrics = _aggregate_metric_rows(fold_rows)
    metrics["feature_count"] = float(len(feature_ids))
    return metrics, fold_rows, predictions


def _permutation_importance(
    *,
    dataset: TrainingDataset,
    feature_ids: Sequence[str],
    gex_features: Sequence[str],
    splitter: CombinatorialPurgedKFold,
    hyperparams: Mapping[str, Any],
    seed: int,
    train_fn: Callable[[np.ndarray, np.ndarray, Sequence[str], Mapping[str, Any]], bytes],
) -> Dict[str, Dict[str, float]]:
    X = _matrix(dataset, feature_ids)
    y = np.asarray(dataset.y, dtype=np.float32)
    feature_index = {fid: idx for idx, fid in enumerate(feature_ids)}
    out: Dict[str, List[float]] = {fid: [] for fid in gex_features}
    for fold_idx, (train_idx, test_idx) in enumerate(
        splitter.split(
            X,
            y,
            groups={
                "label_start": dataset.sample_times_ms,
                "label_end": dataset.label_end_times_ms,
            },
        )
    ):
        if len(train_idx) <= 0 or len(test_idx) <= 0:
            continue
        params = dict(hyperparams or {})
        params.setdefault("random_state", int(seed) + int(fold_idx))
        blob = train_fn(X[train_idx], y[train_idx], list(feature_ids), params)
        base_pred = _predict_from_blob(blob, X[test_idx])
        base_score = _score_predictions(y[test_idx], base_pred)["rank_ic"]
        for fid in gex_features:
            idx = feature_index.get(fid)
            if idx is None:
                continue
            rng = np.random.default_rng(int(seed) + 10_000 + int(fold_idx) * 101 + int(idx))
            X_perm = np.array(X[test_idx], copy=True)
            X_perm[:, idx] = rng.permutation(X_perm[:, idx])
            perm_pred = _predict_from_blob(blob, X_perm)
            perm_score = _score_predictions(y[test_idx], perm_pred)["rank_ic"]
            out[fid].append(float(base_score - perm_score))
    return {
        fid: {
            "rank_ic_drop_mean": float(np.mean(values)) if values else 0.0,
            "positive_fraction": float(np.mean(np.asarray(values) > 0.0)) if values else 0.0,
            "folds": float(len(values)),
        }
        for fid, values in out.items()
    }


def _gex_flow_nonzero_coverage(dataset: TrainingDataset, gex_features: Sequence[str]) -> float:
    if dataset.rows <= 0:
        return 0.0
    present = np.zeros(dataset.rows, dtype=bool)
    for fid in gex_features:
        values = np.asarray(dataset.X_by_feature.get(fid, np.zeros(dataset.rows)), dtype=float)
        present |= np.isfinite(values) & (np.abs(values) > 1e-12)
    return float(np.mean(present))


def run_ablation(
    dataset: TrainingDataset,
    *,
    core_features: Optional[Sequence[str]] = None,
    n_splits: int = 6,
    n_test_splits: int = 2,
    embargo: float = 0.01,
    seed: int = DEFAULT_SEED,
    hyperparams: Optional[Mapping[str, Any]] = None,
    thresholds: Optional[Mapping[str, Any]] = None,
    train_fn: Callable[[np.ndarray, np.ndarray, Sequence[str], Mapping[str, Any]], bytes] = _train_blob,
) -> Dict[str, Any]:
    feature_sets = resolve_feature_sets(core_features)
    usable_rows = int(dataset.rows)
    coverage = _gex_flow_nonzero_coverage(dataset, feature_sets["gex_flow"])
    base_report: Dict[str, Any] = {
        "schema_version": 1,
        "generated_at_ms": int(time.time() * 1000),
        "status": "ok",
        "dataset": {
            "source": str(dataset.source),
            "usable_rows": int(usable_rows),
            "symbols": int(len(set(dataset.symbols))),
            "gex_flow_nonzero_coverage": float(coverage),
            "meta": dict(dataset.meta or {}),
        },
        "feature_sets": feature_sets,
        "metrics": {},
        "delta": {},
        "permutation_importance": {},
        "folds": [],
        "seed": int(seed),
    }

    provisional = evaluate_enablement(
        {
            **base_report,
            "status": "ok",
            "metrics": {"training_status": "ok"},
            "delta": {"rank_ic_mean": 0.0, "positive_rank_ic_delta_fraction": 0.0},
        },
        thresholds=thresholds,
    )
    if provisional["verdict"] == VERDICT_ABSTAIN_INSUFFICIENT_DATA:
        base_report["status"] = "abstain"
        base_report["metrics"] = {"training_status": "abstain_pre_training"}
        base_report["verdict"] = provisional["verdict"]
        base_report["verdict_reasons"] = list(provisional.get("reasons") or [])
        base_report["thresholds"] = dict(provisional.get("thresholds") or {})
        return base_report

    try:
        splitter = CombinatorialPurgedKFold(
            n_splits=int(n_splits),
            n_test_splits=int(n_test_splits),
            embargo=float(embargo),
            label_start_times=dataset.sample_times_ms,
            label_end_times=dataset.label_end_times_ms,
        )
        params = {
            "n_estimators": 80,
            "learning_rate": 0.05,
            "num_leaves": 15,
            "min_child_samples": 5,
            "verbosity": -1,
            "deterministic": True,
        }
        params.update(dict(hyperparams or {}))
        without_metrics, without_folds, _without_preds = _model_eval_for_feature_set(
            dataset=dataset,
            feature_ids=feature_sets["without"],
            splitter=splitter,
            hyperparams=params,
            seed=int(seed),
            train_fn=train_fn,
        )
        with_metrics, with_folds, _with_preds = _model_eval_for_feature_set(
            dataset=dataset,
            feature_ids=feature_sets["with"],
            splitter=splitter,
            hyperparams=params,
            seed=int(seed),
            train_fn=train_fn,
        )
        fold_deltas = []
        for left, right in zip(without_folds, with_folds):
            fold_deltas.append(
                {
                    "fold": int(right.get("fold") or left.get("fold") or 0),
                    "rank_ic_delta": float(right.get("rank_ic") or 0.0) - float(left.get("rank_ic") or 0.0),
                    "oos_r2_delta": float(right.get("oos_r2") or 0.0) - float(left.get("oos_r2") or 0.0),
                    "hit_rate_delta": float(right.get("hit_rate") or 0.0) - float(left.get("hit_rate") or 0.0),
                }
            )
        rank_deltas = [float(row["rank_ic_delta"]) for row in fold_deltas]
        delta = {
            "rank_ic_mean": float(with_metrics.get("rank_ic_mean") or 0.0)
            - float(without_metrics.get("rank_ic_mean") or 0.0),
            "oos_r2_mean": float(with_metrics.get("oos_r2_mean") or 0.0)
            - float(without_metrics.get("oos_r2_mean") or 0.0),
            "hit_rate_mean": float(with_metrics.get("hit_rate_mean") or 0.0)
            - float(without_metrics.get("hit_rate_mean") or 0.0),
            "positive_rank_ic_delta_fraction": float(np.mean(np.asarray(rank_deltas) > 0.0)) if rank_deltas else 0.0,
        }
        importance = _permutation_importance(
            dataset=dataset,
            feature_ids=feature_sets["with"],
            gex_features=feature_sets["gex_flow"],
            splitter=splitter,
            hyperparams=params,
            seed=int(seed),
            train_fn=train_fn,
        )
        base_report["metrics"] = {
            "training_status": "ok",
            "without": without_metrics,
            "with": with_metrics,
        }
        base_report["delta"] = delta
        base_report["folds"] = fold_deltas
        base_report["permutation_importance"] = importance
        verdict = evaluate_enablement(base_report, thresholds=thresholds)
        base_report["verdict"] = verdict["verdict"]
        base_report["verdict_reasons"] = list(verdict.get("reasons") or [])
        base_report["thresholds"] = dict(verdict.get("thresholds") or {})
        return base_report
    except Exception as exc:
        base_report["status"] = "abstain"
        base_report["metrics"] = {"training_status": type(exc).__name__}
        base_report["error"] = str(exc)
        verdict = evaluate_enablement(base_report, thresholds=thresholds)
        base_report["verdict"] = verdict["verdict"]
        base_report["verdict_reasons"] = list(verdict.get("reasons") or [])
        base_report["thresholds"] = dict(verdict.get("thresholds") or {})
        return base_report


def synthetic_training_dataset(rows: int = 96, *, seed: int = DEFAULT_SEED) -> TrainingDataset:
    feature_sets = resolve_feature_sets(["core.synthetic_momentum"])
    rng = np.random.default_rng(int(seed))
    n = int(rows)
    X_by_feature: Dict[str, np.ndarray] = {}
    for fid in feature_sets["with"]:
        X_by_feature[fid] = rng.normal(0.0, 1.0, n).astype(np.float32)
    signal = X_by_feature["options_symbol.gex_norm_z"] * 0.45 + X_by_feature["core.synthetic_momentum"] * 0.15
    y = signal + rng.normal(0.0, 0.25, n).astype(np.float32)
    times = np.arange(n, dtype=np.float64) * 300_000.0 + 1_700_000_000_000.0
    return TrainingDataset(
        X_by_feature=X_by_feature,
        y=np.asarray(y, dtype=np.float32),
        sample_times_ms=times,
        label_end_times_ms=times + 900_000.0,
        symbols=["SPY" for _ in range(n)],
        source="synthetic",
        meta={"status": "ok", "rows_used": n},
    )


def _open_sqlite_readonly(path: Path):
    uri = f"file:{path.expanduser().resolve()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _load_dataset_from_args(args: argparse.Namespace, feature_ids: Sequence[str]) -> TrainingDataset:
    if bool(getattr(args, "synthetic", False)):
        return synthetic_training_dataset(rows=int(args.synthetic_rows), seed=int(args.seed))
    if args.sqlite_db:
        with _open_sqlite_readonly(Path(args.sqlite_db)) as con:
            return load_training_matrix(
                con,
                feature_ids=feature_ids,
                max_rows=int(args.max_rows),
                lookback_days=args.lookback_days,
                horizon_s=args.horizon_s,
            )
    from engine.runtime.storage import connect_ro_direct

    with connect_ro_direct() as con:
        return load_training_matrix(
            con,
            feature_ids=feature_ids,
            max_rows=int(args.max_rows),
            lookback_days=args.lookback_days,
            horizon_s=args.horizon_s,
        )


def render_text_report(report: Mapping[str, Any]) -> str:
    dataset = dict(report.get("dataset") or {})
    delta = dict(report.get("delta") or {})
    metrics = dict(report.get("metrics") or {})
    lines = [
        "Options Feature Ablation Report",
        f"verdict: {report.get('verdict')}",
        f"reasons: {', '.join(str(v) for v in report.get('verdict_reasons') or []) or 'none'}",
        f"usable_rows: {int(dataset.get('usable_rows') or 0)}",
        f"symbols: {int(dataset.get('symbols') or 0)}",
        f"gex_flow_nonzero_coverage: {float(dataset.get('gex_flow_nonzero_coverage') or 0.0):.4f}",
        f"rank_ic_delta_mean: {float(delta.get('rank_ic_mean') or 0.0):.6f}",
        f"positive_rank_ic_delta_fraction: {float(delta.get('positive_rank_ic_delta_fraction') or 0.0):.4f}",
    ]
    if isinstance(metrics.get("without"), dict):
        lines.append(f"without_rank_ic_mean: {float(metrics['without'].get('rank_ic_mean') or 0.0):.6f}")
    if isinstance(metrics.get("with"), dict):
        lines.append(f"with_rank_ic_mean: {float(metrics['with'].get('rank_ic_mean') or 0.0):.6f}")
    lines.append("")
    lines.append("GEX/flow features:")
    for fid in (report.get("feature_sets") or {}).get("gex_flow") or []:
        imp = ((report.get("permutation_importance") or {}).get(fid) or {})
        lines.append(f"- {fid}: rank_ic_drop_mean={float(imp.get('rank_ic_drop_mean') or 0.0):.6f}")
    return "\n".join(lines) + "\n"


def write_report(report: Mapping[str, Any], out: Path) -> Tuple[Path, Path]:
    path = Path(out)
    if path.suffix:
        json_path = path
        text_path = path.with_suffix(".txt")
    else:
        path.mkdir(parents=True, exist_ok=True)
        json_path = path / "options_feature_ablation_report.json"
        text_path = path / "options_feature_ablation_report.txt"
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    text_path.write_text(render_text_report(report), encoding="utf-8")
    return json_path, text_path


def _parse_args(argv: Optional[Sequence[str]]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="", help="Output file path or directory for JSON+text reports")
    parser.add_argument("--sqlite-db", default="", help="Optional sqlite database path opened read-only")
    parser.add_argument("--core-feature", action="append", default=[], help="Core covariate feature id to include")
    parser.add_argument("--max-rows", type=int, default=5000)
    parser.add_argument("--lookback-days", type=int, default=None)
    parser.add_argument("--horizon-s", type=int, default=None)
    parser.add_argument("--n-splits", type=int, default=6)
    parser.add_argument("--n-test-splits", type=int, default=2)
    parser.add_argument("--embargo", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--min-rows", type=int, default=DEFAULT_MIN_ROWS)
    parser.add_argument("--min-rank-ic-delta", type=float, default=DEFAULT_MIN_RANK_IC_DELTA)
    parser.add_argument("--min-stability-fraction", type=float, default=DEFAULT_MIN_STABILITY_FRACTION)
    parser.add_argument("--min-gex-coverage", type=float, default=DEFAULT_MIN_GEX_COVERAGE)
    parser.add_argument("--synthetic", action="store_true", help="Use deterministic synthetic data for smoke checks")
    parser.add_argument("--synthetic-rows", type=int, default=96)
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    feature_sets = resolve_feature_sets(args.core_feature)
    thresholds = {
        "min_rows": int(args.min_rows),
        "min_rank_ic_delta": float(args.min_rank_ic_delta),
        "min_stability_fraction": float(args.min_stability_fraction),
        "min_gex_coverage": float(args.min_gex_coverage),
    }
    dataset = _load_dataset_from_args(args, feature_sets["with"])
    report = run_ablation(
        dataset,
        core_features=args.core_feature,
        n_splits=int(args.n_splits),
        n_test_splits=int(args.n_test_splits),
        embargo=float(args.embargo),
        seed=int(args.seed),
        thresholds=thresholds,
    )
    if args.out:
        json_path, text_path = write_report(report, Path(args.out))
        print(f"wrote_json={json_path}")
        print(f"wrote_text={text_path}")
    else:
        print(render_text_report(report), end="")
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if str(report.get("verdict") or "") in VALID_VERDICTS else 2


if __name__ == "__main__":
    raise SystemExit(main())
