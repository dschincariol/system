"""Cross-sectional LightGBM learning-to-rank model family.

The ranker keeps the same persisted feature-schema contract as
``lgbm_regressor`` while changing only the target construction and estimator:
rows are grouped by decision timestamp across the equity sleeve and LightGBM
receives one ranking group per timestamp.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from engine.backtest.cpcv import CombinatorialPurgedKFold
from engine.data.asset_map import asset_class_for_symbol
from engine.model_registry import register_model_family
from engine.runtime.workload_profiles import assert_offline_work_allowed, model_family_n_jobs
from engine.runtime.storage import connect, init_db
from engine.strategy.model_lifecycle import (
    load_lifecycle_plan,
    record_version_performance,
    update_model_version_status,
    version_from_ts,
)
from engine.strategy.ensemble.oos_store import upsert_oos_predictions
from engine.strategy.models.lgbm_regressor import (
    DEFAULT_HORIZON_S as REGRESSOR_DEFAULT_HORIZON_S,
    DEFAULT_LOOKBACK_DAYS as REGRESSOR_DEFAULT_LOOKBACK_DAYS,
    LGBMRegressorModel,
    _artifact_payload_from_alias,
    _as_weight_array,
    _expected_columns,
    _matrix_from_features,
    _mse_loss,
    _resolve_training_config,
    _safe_float,
    _safe_int,
    _feature_schema,
    _load_training_rows,
    register_shadow_model,
)
from engine.strategy.era_boost import (
    era_boost_config_from_env,
    era_labels_for,
    era_score_table,
    score_std,
    validation_degraded,
    worst_half_eras,
)
from engine.strategy.ood import build_ood_profile, summarize_ood_profile

FAMILY = "lgbm_ranker"
DEFAULT_MODEL_NAME = FAMILY
DEFAULT_MODEL_KIND = "lightgbm_ranker"
DEFAULT_MIN_SAMPLES = int(os.environ.get("LGBM_RANKER_MIN_SAMPLES", "60"))
DEFAULT_MIN_GROUP_SIZE = int(os.environ.get("LGBM_RANKER_MIN_GROUP_SIZE", "3"))
DEFAULT_LABEL_BINS = int(os.environ.get("LGBM_RANKER_LABEL_BINS", "5"))
DEFAULT_LOOKBACK_DAYS = int(os.environ.get("LGBM_RANKER_LOOKBACK_DAYS", str(REGRESSOR_DEFAULT_LOOKBACK_DAYS)))
DEFAULT_HORIZON_S = int(os.environ.get("LGBM_RANKER_HORIZON_S", str(REGRESSOR_DEFAULT_HORIZON_S)))
LOG = logging.getLogger(__name__)


def _register_family() -> None:
    try:
        register_model_family(
            FAMILY,
            training_entrypoint="engine.strategy.jobs.train_lgbm_ranker_models",
            inference_entrypoint="engine.strategy.models.lgbm_ranker.LGBMRankerModel",
            default_stage="shadow",
            promotion_guard="engine.strategy.promotion_guard.assess_challenger",
            metadata={"learning_scope": "cross_sectional_equities", "objective": "lambdarank"},
        )
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)


_register_family()


@dataclass(frozen=True)
class RankDataset:
    X_rows: list[dict[str, float]]
    y_relevance: np.ndarray
    y_return: np.ndarray
    group_counts: list[int]
    group_ts_ms: list[int]
    meta_rows: list[dict[str, int | str]]


_EQUITY_ASSET_CLASSES = {"EQUITY", "EQUITIES", "US_EQUITY", "STOCK", "STOCKS"}
_CRYPTO_ASSET_CLASSES = {"CRYPTO", "CRYPTOCURRENCY", "DIGITAL_ASSET", "DIGITAL_ASSETS"}
_DEFAULT_RANKER_EXCLUDED_ASSET_CLASSES = {
    "CRYPTO",
    "CRYPTOCURRENCY",
    "DIGITAL_ASSET",
    "DIGITAL_ASSETS",
    "COMMODITY",
    "FX",
    "RATES",
    "OPTION",
    "OPTIONS",
    "FUTURES",
}
_CRYPTO_QUOTE_SUFFIXES = ("USDT", "USD", "USDC", "EUR", "GBP")


def _normalize_ranker_asset_scope(asset_scope: Any = None) -> str:
    raw = str(asset_scope or "EQUITY").upper().strip()
    if not raw:
        return "EQUITY"
    if "CRYPTO" in raw or "DIGITAL_ASSET" in raw:
        return "CRYPTO"
    if raw in {"EQUITY", "EQUITIES", "US_EQUITY", "STOCK", "STOCKS", "CROSS_SECTIONAL_EQUITIES"}:
        return "EQUITY"
    return raw


def _crypto_base_symbol(symbol: str) -> str:
    sym = str(symbol or "").upper().strip().replace("/", "").replace("-", "")
    if not sym:
        return ""
    for suffix in _CRYPTO_QUOTE_SUFFIXES:
        if sym.endswith(suffix) and len(sym) > len(suffix):
            return sym[: -len(suffix)]
    return sym


def _asset_class_for_ranker_symbol(symbol: str) -> str:
    sym = str(symbol or "").upper().strip()
    try:
        asset_class = str(asset_class_for_symbol(sym) or "UNKNOWN").upper().strip()
    except Exception:
        asset_class = "UNKNOWN"
    if asset_class == "UNKNOWN":
        base = _crypto_base_symbol(sym)
        if base and base != sym:
            try:
                base_asset_class = str(asset_class_for_symbol(base) or "UNKNOWN").upper().strip()
            except Exception:
                base_asset_class = "UNKNOWN"
            if base_asset_class in _CRYPTO_ASSET_CLASSES:
                asset_class = base_asset_class
    return asset_class


def _is_equity_symbol(symbol: str) -> bool:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return False
    asset_class = _asset_class_for_ranker_symbol(sym)
    if asset_class in _EQUITY_ASSET_CLASSES:
        return True
    if asset_class in _DEFAULT_RANKER_EXCLUDED_ASSET_CLASSES:
        return False
    return bool(re.fullmatch(r"[A-Z][A-Z0-9.]{0,9}", sym))


def _ranker_symbol_in_scope(symbol: str, *, asset_scope: Any = None) -> bool:
    sym = str(symbol or "").upper().strip()
    if not sym:
        return False
    scope = _normalize_ranker_asset_scope(asset_scope)
    asset_class = _asset_class_for_ranker_symbol(sym)
    if scope == "CRYPTO":
        return asset_class in _CRYPTO_ASSET_CLASSES
    if scope == "EQUITY":
        return _is_equity_symbol(sym)
    if scope in _EQUITY_ASSET_CLASSES:
        return _is_equity_symbol(sym)
    if scope in _CRYPTO_ASSET_CLASSES:
        return asset_class in _CRYPTO_ASSET_CLASSES
    return False


def _rank_relevance(values: Sequence[Any], *, bins: int = DEFAULT_LABEL_BINS) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float64).reshape(-1)
    n = int(arr.size)
    if n <= 0:
        return np.asarray([], dtype=np.int32)
    if n == 1:
        return np.asarray([0], dtype=np.int32)
    safe = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
    order = np.argsort(safe, kind="mergesort")
    ordinal = np.empty(n, dtype=np.float64)
    ordinal[order] = np.arange(n, dtype=np.float64)
    b = max(2, int(bins or DEFAULT_LABEL_BINS))
    labels = np.floor(ordinal * float(b) / float(n)).astype(np.int32)
    return np.clip(labels, 0, b - 1).astype(np.int32, copy=False)


def make_cross_sectional_rank_dataset(
    X_rows: Sequence[Mapping[str, Any]],
    y_returns: Sequence[Any],
    meta_rows: Sequence[Mapping[str, Any]],
    *,
    min_group_size: int = DEFAULT_MIN_GROUP_SIZE,
    label_bins: int = DEFAULT_LABEL_BINS,
    asset_scope: Any = None,
) -> RankDataset:
    """Build timestamp ranking groups from per-symbol forward-return rows."""

    scope = _normalize_ranker_asset_scope(asset_scope)
    grouped: dict[int, list[tuple[str, dict[str, float], float, dict[str, int | str]]]] = {}
    for X, y, meta in zip(list(X_rows or []), list(y_returns or []), list(meta_rows or [])):
        sym = str((meta or {}).get("symbol") or "").upper().strip()
        if not _ranker_symbol_in_scope(sym, asset_scope=scope):
            continue
        ts_ms = _safe_int((meta or {}).get("ts") or (meta or {}).get("ts_ms"), 0)
        if ts_ms <= 0:
            continue
        ret = _safe_float(y, float("nan"))
        if not math.isfinite(float(ret)):
            continue
        row = {str(k): _safe_float(v, 0.0) for k, v in dict(X or {}).items()}
        grouped.setdefault(int(ts_ms), []).append(
            (
                str(sym),
                row,
                float(ret),
                {
                    "symbol": str(sym),
                    "ts": int(ts_ms),
                    "horizon": _safe_int((meta or {}).get("horizon") or (meta or {}).get("horizon_s"), 0),
                },
            )
        )

    out_X: list[dict[str, float]] = []
    out_labels: list[int] = []
    out_returns: list[float] = []
    out_groups: list[int] = []
    out_ts: list[int] = []
    out_meta: list[dict[str, int | str]] = []
    min_size = max(2, int(min_group_size or DEFAULT_MIN_GROUP_SIZE))
    for ts_ms in sorted(grouped.keys()):
        rows = sorted(grouped[int(ts_ms)], key=lambda item: item[0])
        if len(rows) < min_size:
            continue
        labels = _rank_relevance([item[2] for item in rows], bins=int(label_bins or DEFAULT_LABEL_BINS))
        out_groups.append(int(len(rows)))
        out_ts.append(int(ts_ms))
        for idx, (_sym, X, ret, meta) in enumerate(rows):
            out_X.append(dict(X))
            out_labels.append(int(labels[idx]))
            out_returns.append(float(ret))
            out_meta.append(dict(meta))

    return RankDataset(
        X_rows=out_X,
        y_relevance=np.asarray(out_labels, dtype=np.int32),
        y_return=np.asarray(out_returns, dtype=np.float32),
        group_counts=list(out_groups),
        group_ts_ms=list(out_ts),
        meta_rows=list(out_meta),
    )


def _rankdata(values: Sequence[Any]) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float64).reshape(-1)
    n = int(arr.size)
    if n <= 0:
        return np.asarray([], dtype=np.float64)
    order = np.argsort(arr, kind="mergesort")
    ranks = np.empty(n, dtype=np.float64)
    ranks[order] = np.arange(1, n + 1, dtype=np.float64)
    sorted_vals = arr[order]
    start = 0
    while start < n:
        end = start + 1
        while end < n and sorted_vals[end] == sorted_vals[start]:
            end += 1
        if end - start > 1:
            ranks[order[start:end]] = float(np.mean(np.arange(start + 1, end + 1, dtype=np.float64)))
        start = end
    return ranks


def spearman_rank_ic(y_true: Sequence[Any], y_score: Sequence[Any]) -> float:
    true = _rankdata(y_true)
    score = _rankdata(y_score)
    if int(true.size) <= 1 or int(score.size) != int(true.size):
        return 0.0
    true = true - float(np.mean(true))
    score = score - float(np.mean(score))
    denom = float(np.sqrt(np.sum(true * true) * np.sum(score * score)))
    if denom <= 0.0 or not math.isfinite(denom):
        return 0.0
    return float(np.sum(true * score) / denom)


def _group_slices(group_counts: Sequence[int]) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    start = 0
    for count in list(group_counts or []):
        n = int(count or 0)
        if n <= 0:
            continue
        out.append((int(start), int(start + n)))
        start += n
    return out


def top_bottom_quintile_spread(
    y_returns: Sequence[Any],
    y_score: Sequence[Any],
    group_counts: Sequence[int],
) -> float:
    returns = np.asarray(y_returns, dtype=np.float64).reshape(-1)
    scores = np.asarray(y_score, dtype=np.float64).reshape(-1)
    spreads: list[float] = []
    for start, end in _group_slices(group_counts):
        if end > int(returns.size) or end > int(scores.size):
            continue
        n = int(end - start)
        if n < 2:
            continue
        order = np.argsort(scores[start:end], kind="mergesort")
        q = max(1, int(math.ceil(n / 5.0)))
        bottom = returns[start:end][order[:q]]
        top = returns[start:end][order[-q:]]
        spreads.append(float(np.mean(top) - np.mean(bottom)))
    return float(np.mean(spreads)) if spreads else 0.0


def ranker_metrics(
    y_returns: Sequence[Any],
    y_score: Sequence[Any],
    group_counts: Sequence[int],
    *,
    group_ts_ms: Sequence[int] | None = None,
) -> dict[str, Any]:
    group_ics: list[float] = []
    for start, end in _group_slices(group_counts):
        if end > start:
            group_ics.append(float(spearman_rank_ic(np.asarray(y_returns)[start:end], np.asarray(y_score)[start:end])))
    metrics = {
        "rank_ic": float(np.mean(group_ics)) if group_ics else 0.0,
        "rank_ic_pooled": float(spearman_rank_ic(y_returns, y_score)),
        "top_bottom_quintile_spread": float(top_bottom_quintile_spread(y_returns, y_score, group_counts)),
        "group_count": int(len(list(group_counts or []))),
    }
    eras: list[dict[str, Any]] = []
    ts_values = list(group_ts_ms or [])
    for idx, (start, end) in enumerate(_group_slices(group_counts)):
        if end <= start:
            continue
        eras.append(
            {
                "ts_ms": int(ts_values[idx]) if idx < len(ts_values) else int(idx),
                "rank_ic": float(spearman_rank_ic(np.asarray(y_returns)[start:end], np.asarray(y_score)[start:end])),
                "top_bottom_quintile_spread": float(
                    top_bottom_quintile_spread(np.asarray(y_returns)[start:end], np.asarray(y_score)[start:end], [end - start])
                ),
                "n": int(end - start),
            }
        )
    if eras:
        metrics["era_metrics"] = eras
    return metrics


def cpcv_group_splits(
    group_ts_ms: Sequence[int],
    *,
    horizon_s: int,
    n_splits: int = 4,
    n_test_splits: int = 1,
    embargo: float = 0.0,
) -> list[tuple[np.ndarray, np.ndarray]]:
    ts = np.asarray(list(group_ts_ms or []), dtype=np.float64).reshape(-1)
    n_groups = int(ts.size)
    if n_groups < 2:
        return []
    splits = max(2, min(int(n_splits or 4), n_groups))
    test_splits = max(1, min(int(n_test_splits or 1), splits - 1))
    splitter = CombinatorialPurgedKFold(
        n_splits=int(splits),
        n_test_splits=int(test_splits),
        embargo=float(max(0.0, embargo)),
        label_start_times=ts,
        label_end_times=ts + float(max(0, int(horizon_s or 0)) * 1000),
    )
    return [(np.asarray(tr, dtype=int), np.asarray(te, dtype=int)) for tr, te in splitter.split(np.arange(n_groups))]


def _row_indices_for_groups(group_counts: Sequence[int], group_indices: Sequence[int]) -> np.ndarray:
    slices = _group_slices(group_counts)
    idx: list[int] = []
    for group_idx in sorted({int(i) for i in list(group_indices or [])}):
        if group_idx < 0 or group_idx >= len(slices):
            continue
        start, end = slices[group_idx]
        idx.extend(range(start, end))
    return np.asarray(idx, dtype=int)


def _group_counts_for_indices(group_counts: Sequence[int], group_indices: Sequence[int]) -> list[int]:
    counts = list(group_counts or [])
    return [int(counts[int(i)]) for i in sorted({int(i) for i in list(group_indices or [])}) if 0 <= int(i) < len(counts)]


def _expand_group_values(values: Sequence[Any], group_counts: Sequence[int]) -> list[Any]:
    raw = [] if values is None else list(values)
    out: list[Any] = []
    for idx, count in enumerate(list(group_counts or [])):
        value = raw[idx] if idx < len(raw) else None
        out.extend([value] * max(0, int(count or 0)))
    return out


def _fit_ranker_continuation(
    *,
    model_factory: Any,
    X: np.ndarray,
    y: np.ndarray,
    group_arr: np.ndarray,
    columns: Sequence[str],
    sample_weight: np.ndarray,
    init_model: Any,
) -> Any:
    estimator = model_factory()
    estimator.fit(
        X,
        y,
        group=np.asarray(group_arr, dtype=np.int32).reshape(-1).tolist(),
        sample_weight=np.asarray(sample_weight, dtype=np.float32).reshape(-1),
        feature_name=list(columns),
        init_model=init_model,
    )
    return estimator


def _apply_ranker_era_boost(
    *,
    model_factory: Any,
    initial_model: Any,
    X_train: np.ndarray,
    y_train: np.ndarray,
    group_arr: np.ndarray,
    columns: Sequence[str],
    base_sample_weight: np.ndarray,
    train_timestamps: Any = None,
    train_era_labels: Any = None,
    validation_matrix: np.ndarray | None = None,
    validation_target: np.ndarray | None = None,
) -> tuple[Any, dict[str, Any]]:
    cfg = era_boost_config_from_env()
    labels, label_diag = era_labels_for(
        n_obs=int(y_train.shape[0]),
        timestamps=train_timestamps,
        era_labels=train_era_labels,
    )
    payload: dict[str, Any] = {
        "enabled": bool(cfg.get("enabled")),
        "applied": False,
        "config": dict(cfg),
        "label_diagnostics": dict(label_diag),
    }
    if not bool(cfg.get("enabled")):
        return initial_model, payload
    if not bool(label_diag.get("applied")) or len(labels) < int(y_train.shape[0]):
        payload["status"] = "missing_training_eras"
        return initial_model, payload

    score_kind = str(cfg.get("score_kind") or "neg_mse")
    initial_pred = np.asarray(initial_model.predict(X_train), dtype=np.float64).reshape(-1)
    before_table = era_score_table(y_train, initial_pred, labels, score_kind=score_kind)
    payload["before"] = {
        "era_scores": before_table,
        "era_score_std": float(score_std(before_table)),
    }
    if len(before_table) < 2:
        payload["status"] = "insufficient_eras"
        return initial_model, payload

    current = initial_model
    current_val_loss = _mse_loss(current, validation_matrix, validation_target)
    iterations: list[dict[str, Any]] = []
    base_weights = _as_weight_array(base_sample_weight, int(y_train.shape[0]))
    multiplier = float(cfg.get("weight_multiplier") or 2.0)

    for iteration in range(int(cfg.get("iters") or 1)):
        train_pred = np.asarray(current.predict(X_train), dtype=np.float64).reshape(-1)
        table = era_score_table(y_train, train_pred, labels, score_kind=score_kind)
        worst = set(worst_half_eras(table))
        if not worst:
            iterations.append({"iteration": int(iteration + 1), "status": "no_worst_eras"})
            break
        weights = base_weights.copy()
        for idx, label in enumerate(labels[: int(weights.shape[0])]):
            if str(label) in worst:
                weights[idx] = float(weights[idx]) * float(multiplier)
        candidate = _fit_ranker_continuation(
            model_factory=model_factory,
            X=X_train,
            y=y_train,
            group_arr=group_arr,
            columns=columns,
            sample_weight=weights,
            init_model=current.booster_,
        )
        candidate_val_loss = _mse_loss(candidate, validation_matrix, validation_target)
        degraded = (
            current_val_loss is not None
            and candidate_val_loss is not None
            and validation_degraded(
                prior_loss=float(current_val_loss),
                candidate_loss=float(candidate_val_loss),
                max_degrade=float(cfg.get("max_degrade") or 0.0),
            )
        )
        iteration_payload = {
            "iteration": int(iteration + 1),
            "worst_eras": sorted(worst),
            "weight_source_eras": sorted({str(label) for label in labels}),
            "weighted_rows": int(sum(1 for label in labels if str(label) in worst)),
            "validation_loss_before": (None if current_val_loss is None else float(current_val_loss)),
            "validation_loss_after": (None if candidate_val_loss is None else float(candidate_val_loss)),
            "accepted": bool(not degraded),
        }
        iterations.append(iteration_payload)
        if degraded:
            payload["status"] = "stopped_validation_degrade"
            break
        current = candidate
        current_val_loss = candidate_val_loss

    final_pred = np.asarray(current.predict(X_train), dtype=np.float64).reshape(-1)
    after_table = era_score_table(y_train, final_pred, labels, score_kind=score_kind)
    payload.update(
        {
            "applied": True,
            "status": str(payload.get("status") or "completed"),
            "iterations": iterations,
            "after": {
                "era_scores": after_table,
                "era_score_std": float(score_std(after_table)),
            },
            "validation_rows": int(0 if validation_target is None else np.asarray(validation_target).reshape(-1).shape[0]),
        }
    )
    return current, payload


class LGBMRankerModel(LGBMRegressorModel):
    """LightGBM LambdaRank model with schema-bound feature vectorization."""

    family = FAMILY
    model_kind = DEFAULT_MODEL_KIND

    @staticmethod
    def _default_hyperparams() -> dict[str, Any]:
        return {
            "objective": "lambdarank",
            "metric": "ndcg",
            "boosting_type": "gbdt",
            "num_leaves": 31,
            "learning_rate": 0.05,
            "n_estimators": 100,
            "min_child_samples": 2,
            "random_state": 42,
            "n_jobs": model_family_n_jobs(
                "LGBM_RANKER_N_JOBS",
                fallback_keys=("LGBM_N_JOBS", "MODEL_TRAIN_N_JOBS"),
            ),
            "verbosity": -1,
            "deterministic": True,
            "force_col_wise": True,
        }

    def _new_estimator(self) -> Any:
        try:
            import lightgbm as lgb
        except ImportError as exc:  # pragma: no cover - dependency is declared
            raise RuntimeError("lightgbm_not_installed") from exc
        return lgb.LGBMRanker(**dict(self.hyperparams))

    def fit(
        self,
        X: Any,
        y: Any,
        *,
        group: Sequence[int],
        sample_weight: Any = None,
        era_timestamps: Any = None,
        era_labels: Any = None,
        validation_data: tuple[Any, Any] | None = None,
        validation_group: Sequence[int] | None = None,
        validation_timestamps: Any = None,
        validation_era_labels: Any = None,
    ) -> "LGBMRankerModel":
        columns = _expected_columns(self.feature_ids, model_name=self.model_name, model_spec=self.feature_schema)
        X_arr, preprocessing, _accounting = _matrix_from_features(
            X,
            columns,
            phase="train",
            model_name=self.model_name,
            fit_preprocessing=True,
            return_metadata=True,
        )
        y_arr = np.asarray(y, dtype=np.int32).reshape(-1)
        group_arr = np.asarray(list(group) if group is not None else [], dtype=np.int32).reshape(-1)
        if int(X_arr.shape[0]) != int(y_arr.shape[0]):
            raise ValueError("lgbm_ranker_row_count_mismatch")
        if int(group_arr.size) <= 0 or int(np.sum(group_arr)) != int(y_arr.shape[0]):
            raise ValueError("lgbm_ranker_group_count_mismatch")
        if np.any(group_arr <= 0):
            raise ValueError("lgbm_ranker_invalid_group")
        base_sample_weight = _as_weight_array(sample_weight, int(y_arr.shape[0]))
        self.feature_ids = list(columns)
        self.feature_preprocessing = dict(preprocessing or {})
        model = self._new_estimator()
        fit_kwargs: dict[str, Any] = {"group": group_arr.tolist(), "feature_name": list(columns)}
        if sample_weight is not None:
            fit_kwargs["sample_weight"] = base_sample_weight
        model.fit(X_arr, y_arr, **fit_kwargs)

        validation_matrix = None
        validation_target = None
        if validation_data is not None:
            val_X, val_y = validation_data
            validation_matrix = _matrix_from_features(
                val_X,
                columns,
                feature_schema=_feature_schema(columns, preprocessing=dict(preprocessing or {})),
                phase="serve",
                model_name=self.model_name,
            )
            validation_target = np.asarray(val_y, dtype=np.int32).reshape(-1)
            if int(validation_matrix.shape[0]) != int(validation_target.shape[0]):
                raise ValueError("lgbm_ranker_validation_row_count_mismatch")
            if validation_group is not None:
                val_group_arr = np.asarray(list(validation_group), dtype=np.int32).reshape(-1)
                if int(np.sum(val_group_arr)) != int(validation_target.shape[0]) or np.any(val_group_arr <= 0):
                    raise ValueError("lgbm_ranker_validation_group_count_mismatch")

        era_cfg = era_boost_config_from_env()
        if bool(era_cfg.get("enabled")):
            boost_hyperparams = dict(self.hyperparams)
            boost_hyperparams["n_estimators"] = int(era_cfg.get("rounds") or 20)

            def _factory() -> Any:
                try:
                    import lightgbm as lgb
                except ImportError as exc:  # pragma: no cover - dependency is declared
                    raise RuntimeError("lightgbm_not_installed") from exc
                return lgb.LGBMRanker(**boost_hyperparams)

            model, era_payload = _apply_ranker_era_boost(
                model_factory=_factory,
                initial_model=model,
                X_train=X_arr,
                y_train=y_arr,
                group_arr=group_arr,
                columns=columns,
                base_sample_weight=base_sample_weight,
                train_timestamps=era_timestamps,
                train_era_labels=era_labels,
                validation_matrix=validation_matrix,
                validation_target=validation_target,
            )
        else:
            era_payload = {"enabled": False, "applied": False, "config": dict(era_cfg)}
        self.model = model
        self.ood_profile = build_ood_profile(X_arr, columns)
        train_scores = np.asarray(model.predict(X_arr), dtype=np.float32).reshape(-1)
        self.training_metrics = {
            "n_train": int(y_arr.shape[0]),
            "n_groups": int(group_arr.size),
            "model_family": str(self.family),
            "model_kind": str(self.model_kind),
            "feature_schema": self.feature_schema,
            "ood_profile_summary": summarize_ood_profile(self.ood_profile),
            "train_rank_ic": float(spearman_rank_ic(y_arr, train_scores)),
            "train_top_bottom_quintile_spread": float(top_bottom_quintile_spread(y_arr, train_scores, group_arr.tolist())),
        }
        if bool(era_payload.get("enabled")):
            era_payload["validation_label_diagnostics"] = era_labels_for(
                n_obs=(0 if validation_target is None else int(validation_target.shape[0])),
                timestamps=validation_timestamps,
                era_labels=validation_era_labels,
            )[1]
            self.training_metrics["era_boost"] = dict(era_payload)
        self.persisted_feature_schema = dict(self.feature_schema)
        return self


def train_lgbm_ranker(
    X: Any,
    y: Any,
    *,
    group: Sequence[int],
    feature_ids: Sequence[Any] | None = None,
    sample_weight: Any = None,
    hyperparams: Mapping[str, Any] | None = None,
    model_name: str = DEFAULT_MODEL_NAME,
    era_timestamps: Any = None,
    era_labels: Any = None,
    validation_data: tuple[Any, Any] | None = None,
    validation_group: Sequence[int] | None = None,
    validation_timestamps: Any = None,
    validation_era_labels: Any = None,
) -> LGBMRankerModel:
    return LGBMRankerModel(
        model_name=str(model_name or DEFAULT_MODEL_NAME),
        feature_ids=feature_ids,
        hyperparams=hyperparams,
    ).fit(
        X,
        y,
        group=(list(group) if group is not None else []),
        sample_weight=sample_weight,
        era_timestamps=era_timestamps,
        era_labels=era_labels,
        validation_data=validation_data,
        validation_group=validation_group,
        validation_timestamps=validation_timestamps,
        validation_era_labels=validation_era_labels,
    )


def load_model_from_artifact(alias: str = "", sha256: str = "", path: str | Path | None = None) -> LGBMRankerModel:
    if path is not None and str(path).strip():
        return LGBMRankerModel.load(Path(path))
    payload = _artifact_payload_from_alias(str(alias or ""), str(sha256 or ""))
    if not payload:
        raise FileNotFoundError("lgbm_ranker_artifact_not_found")
    return LGBMRankerModel.from_bytes(payload)


def ranker_scores_to_signals(
    symbols: Sequence[str],
    scores: Sequence[Any],
    *,
    top_k: int,
    bottom_k: int,
) -> dict[str, dict[str, Any]]:
    syms = [str(sym or "").upper().strip() for sym in list(symbols or [])]
    vals = np.asarray(scores, dtype=np.float64).reshape(-1)
    if len(syms) != int(vals.size):
        raise ValueError("ranker_symbol_score_count_mismatch")
    n = int(vals.size)
    if n <= 0:
        return {}
    order = np.argsort(vals, kind="mergesort")
    top_n = max(0, min(int(top_k or 0), n))
    bottom_n = max(0, min(int(bottom_k or 0), max(0, n - top_n)))
    top_idx = list(order[-top_n:][::-1]) if top_n > 0 else []
    bottom_idx = list(order[:bottom_n]) if bottom_n > 0 else []
    selected_top = {int(idx) for idx in top_idx}
    selected_bottom = {int(idx) for idx in bottom_idx}
    center = float(np.median(vals))
    centered = vals - center
    denom = float(np.max(np.abs(centered))) if n > 0 else 0.0
    if denom <= 1e-12 or not math.isfinite(denom):
        denom = float(np.max(vals) - np.min(vals))
    if denom <= 1e-12 or not math.isfinite(denom):
        denom = 1.0
    rank_desc = {int(idx): int(pos + 1) for pos, idx in enumerate(list(order[::-1]))}
    out: dict[str, dict[str, Any]] = {}
    for idx, sym in enumerate(syms):
        if not sym:
            continue
        side = "FLAT"
        signed = 0.0
        selected = False
        if idx in selected_top:
            side = "LONG"
            signed = abs(float(centered[idx]) / float(denom)) or 1.0
            selected = True
        elif idx in selected_bottom:
            side = "SHORT"
            signed = -(abs(float(centered[idx]) / float(denom)) or 1.0)
            selected = True
        conf = float(min(1.0, max(0.0, abs(float(centered[idx]) / float(denom)))))
        if selected and conf <= 0.0:
            conf = 1.0
        out[str(sym)] = {
            "expected_z": float(max(-1.0, min(1.0, signed))),
            "confidence": float(conf if selected else 0.0),
            "rank_score": float(vals[idx]),
            "rank": int(rank_desc.get(idx, n)),
            "side": str(side),
            "selected": bool(selected),
        }
    return out


def _fit_ranker_on_group_indices(
    dataset: RankDataset,
    group_indices: Sequence[int],
    *,
    model_name: str,
    feature_ids: Sequence[str],
    hyperparams: Mapping[str, Any],
) -> LGBMRankerModel:
    row_idx = _row_indices_for_groups(dataset.group_counts, group_indices)
    group = _group_counts_for_indices(dataset.group_counts, group_indices)
    X = [dataset.X_rows[int(i)] for i in row_idx]
    y = dataset.y_relevance[row_idx]
    model = LGBMRankerModel(model_name=str(model_name), feature_ids=list(feature_ids), hyperparams=dict(hyperparams or {}))
    return model.fit(X, y, group=group)


def _cpcv_oos_predictions(
    dataset: RankDataset,
    *,
    model_name: str,
    feature_ids: Sequence[str],
    hyperparams: Mapping[str, Any],
    horizon_s: int,
) -> tuple[dict[int, float], int]:
    n_groups = int(len(dataset.group_counts))
    if n_groups < 3:
        return {}, 0
    n_splits = int(os.environ.get("LGBM_RANKER_CPCV_N_SPLITS", os.environ.get("CPCV_N_SPLITS", "4")))
    n_test_splits = int(os.environ.get("LGBM_RANKER_CPCV_N_TEST_SPLITS", os.environ.get("CPCV_N_TEST_SPLITS", "1")))
    embargo = float(os.environ.get("LGBM_RANKER_CPCV_EMBARGO_PCT", os.environ.get("CPCV_EMBARGO_PCT", "0.0")))
    splits = cpcv_group_splits(
        dataset.group_ts_ms,
        horizon_s=int(horizon_s),
        n_splits=int(n_splits),
        n_test_splits=int(n_test_splits),
        embargo=float(embargo),
    )
    preds: dict[int, float] = {}
    used = 0
    for train_groups, test_groups in splits:
        if train_groups.size <= 0 or test_groups.size <= 0:
            continue
        train_rows = _row_indices_for_groups(dataset.group_counts, train_groups)
        test_rows = _row_indices_for_groups(dataset.group_counts, test_groups)
        if train_rows.size <= 0 or test_rows.size <= 0:
            continue
        try:
            fold_model = _fit_ranker_on_group_indices(
                dataset,
                train_groups,
                model_name=str(model_name),
                feature_ids=list(feature_ids),
                hyperparams=dict(hyperparams or {}),
            )
            fold_scores = fold_model.predict([dataset.X_rows[int(i)] for i in test_rows])
        except Exception:
            LOG.debug("Ignored recoverable exception.", exc_info=True)
            continue
        for idx, row_idx in enumerate(test_rows):
            preds.setdefault(int(row_idx), float(fold_scores[int(idx)]))
        used += 1
    return preds, int(used)


def run_ranker_training_job(
    *,
    family: str = FAMILY,
    model_cls: type[LGBMRankerModel] = LGBMRankerModel,
    model_kind: str = DEFAULT_MODEL_KIND,
    version_prefix: str = "lgbm_ranker",
) -> int:
    try:
        assert_offline_work_allowed(job_name=f"train_{family}_models")
    except RuntimeError as exc:
        print(f"[workload_profile] {exc}")
        return 3

    init_db()
    plan = load_lifecycle_plan(str(family))
    cfg = _resolve_training_config(str(family), plan)
    now_ms = int(time.time() * 1000)
    lookback_days = int(cfg.get("training_window_days") or DEFAULT_LOOKBACK_DAYS)
    cutoff_ms = now_ms - lookback_days * 86_400_000
    feature_ids = list(cfg.get("feature_ids") or [])
    asset_scope = _normalize_ranker_asset_scope(
        cfg.get("asset_scope")
        or cfg.get("ranker_asset_scope")
        or cfg.get("training_asset_scope")
        or cfg.get("learning_scope")
        or os.environ.get("LGBM_RANKER_ASSET_SCOPE")
    )
    try:
        from engine.data.universe_pit import resolve_training_window_universe

        con_universe = connect(readonly=True)
        try:
            pit_universe = resolve_training_window_universe(
                con_universe,
                configured_symbols=list(cfg.get("symbol_universe") or ["*"]),
                lookback_days=int(lookback_days),
                as_of_ts_ms=int(now_ms),
            )
        finally:
            con_universe.close()
        if list(pit_universe.get("symbols") or []):
            cfg["symbol_universe"] = list(pit_universe.get("symbols") or [])
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)

    X_rows, y_rows, meta_rows = _load_training_rows(
        cutoff_ms=int(cutoff_ms),
        horizon_s=int(cfg.get("horizon_s") or DEFAULT_HORIZON_S),
        symbols=list(cfg.get("symbol_universe") or ["*"]),
        feature_ids=list(feature_ids),
        include_metadata=True,
    )
    dataset = make_cross_sectional_rank_dataset(
        X_rows,
        y_rows,
        meta_rows,
        min_group_size=int(os.environ.get("LGBM_RANKER_MIN_GROUP_SIZE", str(DEFAULT_MIN_GROUP_SIZE))),
        label_bins=int(os.environ.get("LGBM_RANKER_LABEL_BINS", str(DEFAULT_LABEL_BINS))),
        asset_scope=str(asset_scope),
    )
    min_samples = int(os.environ.get(f"{str(family).upper()}_MIN_SAMPLES", str(DEFAULT_MIN_SAMPLES)))
    if len(dataset.y_relevance) < max(2, min_samples) or len(dataset.group_counts) < 2:
        print(
            f"{family}: insufficient_samples n={len(dataset.y_relevance)} "
            f"groups={len(dataset.group_counts)} min_required={max(2, min_samples)}"
        )
        return 0

    hyperparams = dict(cfg.get("hyperparams") or {})
    model = model_cls(
        model_name=str(cfg.get("model_name") or family),
        feature_ids=list(feature_ids),
        hyperparams=dict(hyperparams),
    )
    if bool(era_boost_config_from_env().get("enabled")) and len(dataset.group_counts) > 1:
        split_group = min(max(1, int(len(dataset.group_counts) * 0.8)), int(len(dataset.group_counts) - 1))
        train_group_indices = list(range(0, split_group))
        eval_group_indices = list(range(split_group, len(dataset.group_counts)))
        train_row_idx = _row_indices_for_groups(dataset.group_counts, train_group_indices)
        eval_row_idx = _row_indices_for_groups(dataset.group_counts, eval_group_indices)
        train_groups = _group_counts_for_indices(dataset.group_counts, train_group_indices)
        eval_groups = _group_counts_for_indices(dataset.group_counts, eval_group_indices)
        train_X = [dataset.X_rows[int(idx)] for idx in train_row_idx]
        eval_X = [dataset.X_rows[int(idx)] for idx in eval_row_idx]
        train_group_ts = [dataset.group_ts_ms[int(idx)] for idx in train_group_indices]
        eval_group_ts = [dataset.group_ts_ms[int(idx)] for idx in eval_group_indices]
        model.fit(
            train_X,
            dataset.y_relevance[train_row_idx],
            group=train_groups,
            era_timestamps=_expand_group_values(train_group_ts, train_groups),
            validation_data=(eval_X, dataset.y_relevance[eval_row_idx]),
            validation_group=eval_groups,
            validation_timestamps=_expand_group_values(eval_group_ts, eval_groups),
        )
    else:
        model.fit(dataset.X_rows, dataset.y_relevance, group=list(dataset.group_counts))

    performance_metrics: dict[str, Any] = {}
    try:
        oos_pred_by_idx, cpcv_folds = _cpcv_oos_predictions(
            dataset,
            model_name=str(cfg.get("model_name") or family),
            feature_ids=list(feature_ids),
            hyperparams=dict(hyperparams),
            horizon_s=int(cfg.get("horizon_s") or DEFAULT_HORIZON_S),
        )
        if oos_pred_by_idx:
            eval_indices = sorted(oos_pred_by_idx.keys())
            eval_scores = np.asarray([float(oos_pred_by_idx[int(i)]) for i in eval_indices], dtype=np.float32)
            eval_returns = dataset.y_return[np.asarray(eval_indices, dtype=int)]
            eval_meta = [dataset.meta_rows[int(i)] for i in eval_indices]
            group_counts: list[int] = []
            group_ts_ms: list[int] = []
            last_ts: int | None = None
            for meta in eval_meta:
                ts = int((meta or {}).get("ts") or 0)
                if last_ts != ts:
                    group_counts.append(0)
                    group_ts_ms.append(int(ts))
                    last_ts = int(ts)
                group_counts[-1] += 1
            performance_metrics = dict(
                ranker_metrics(eval_returns, eval_scores, group_counts, group_ts_ms=group_ts_ms)
            )
            performance_metrics["cpcv_folds"] = int(cpcv_folds)
            oos_run_id = str(uuid.uuid4())
            upsert_oos_predictions(
                [
                    {
                        "symbol": str(meta.get("symbol") or "*"),
                        "horizon": int(meta.get("horizon") or cfg.get("horizon_s") or DEFAULT_HORIZON_S),
                        "family": str(family),
                        "ts": int(meta.get("ts") or 0),
                        "run_id": str(oos_run_id),
                        "prediction": float(eval_scores[idx]),
                        "target": float(eval_returns[idx]),
                    }
                    for idx, meta in enumerate(eval_meta)
                ]
            )
    except Exception:
        LOG.debug("Ignored recoverable exception.", exc_info=True)

    performance_metrics["asset_scope"] = str(asset_scope)
    performance_metrics["ranker_asset_scope"] = str(asset_scope)
    performance_metrics["learning_scope"] = (
        "cross_sectional_crypto" if str(asset_scope) == "CRYPTO" else "cross_sectional_equities"
    )
    version = str(
        plan.get("model_version")
        or cfg.get("training_version_id")
        or version_from_ts(str(model.model_name), now_ms, prefix=str(version_prefix))
    )
    result = register_shadow_model(
        model,
        symbol="*",
        version=str(version),
        family=str(family),
        model_kind=str(model_kind),
        performance_metrics=dict(performance_metrics),
    )
    metrics = dict(result.get("metrics") or {})
    quality = float(max(0.0, min(1.0, (float(metrics.get("rank_ic") or 0.0) + 1.0) / 2.0)))
    record_version_performance(
        model_name=str(model.model_name),
        model_version=str(version),
        metric_scope="training",
        metrics={
            "rank_ic": float(metrics.get("rank_ic") or 0.0),
            "top_bottom_quintile_spread": float(metrics.get("top_bottom_quintile_spread") or 0.0),
            "quality_score": float(quality),
            "trained_models": 1,
        },
        sample_n=int(len(dataset.y_relevance)),
        meta={"job_name": f"train_{family}_models", "cpcv_folds": int(metrics.get("cpcv_folds") or 0)},
    )
    update_model_version_status(
        str(model.model_name),
        str(version),
        stage="shadow",
        status="trained",
        live_ready=False,
        meta_patch={"training_completed_ts_ms": int(time.time() * 1000)},
    )
    print(json.dumps({"ok": True, "family": str(family), "version": str(version), "stage": "shadow"}))
    return 0


def main() -> int:
    return run_ranker_training_job(
        family=FAMILY,
        model_cls=LGBMRankerModel,
        model_kind=DEFAULT_MODEL_KIND,
        version_prefix="lgbm_ranker",
    )


__all__ = [
    "FAMILY",
    "LGBMRankerModel",
    "RankDataset",
    "cpcv_group_splits",
    "load_model_from_artifact",
    "main",
    "make_cross_sectional_rank_dataset",
    "_ranker_symbol_in_scope",
    "ranker_metrics",
    "ranker_scores_to_signals",
    "run_ranker_training_job",
    "spearman_rank_ic",
    "top_bottom_quintile_spread",
    "train_lgbm_ranker",
]


if __name__ == "__main__":
    raise SystemExit(main())
