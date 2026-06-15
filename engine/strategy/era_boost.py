"""Shared era bucketing and era-boost helpers for model training.

Era labels are point-in-time labels derived from row timestamps (UTC calendar
months by default) or explicit caller-supplied labels.  The boosting helpers use
training rows only to choose worst eras; validation rows are accepted only for
the mean-performance safeguard.
"""

from __future__ import annotations

import datetime as _dt
import math
import os
from typing import Any, Mapping, Sequence

import numpy as np


def safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def month_label(ts_ms: Any) -> str:
    try:
        ts_i = int(float(ts_ms))
    except Exception:
        return ""
    if ts_i <= 0:
        return ""
    dt = _dt.datetime.fromtimestamp(float(ts_i) / 1000.0, tz=_dt.timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"


def coerce_optional_labels(values: Any) -> list[str]:
    if values is None or isinstance(values, (str, bytes)):
        return []
    try:
        raw = list(values)
    except Exception:
        return []
    return [str(value or "").strip() for value in raw]


def era_labels_for(
    *,
    n_obs: int,
    timestamps: Any = None,
    era_labels: Any = None,
    regime_labels: Any = None,
) -> tuple[list[str], dict[str, Any]]:
    n = int(max(0, n_obs))
    explicit_eras = coerce_optional_labels(era_labels)
    regimes = coerce_optional_labels(regime_labels)
    months: list[str] = []
    raw_timestamps = []
    if timestamps is not None and not isinstance(timestamps, (str, bytes)):
        try:
            raw_timestamps = list(timestamps)
        except Exception:
            raw_timestamps = []
    if raw_timestamps:
        months = [month_label(value) for value in raw_timestamps]

    if len(explicit_eras) >= n:
        base = [label if label else f"era_{idx:04d}" for idx, label in enumerate(explicit_eras[:n])]
        mode = "explicit_era"
    elif len(months) >= n and any(str(label or "").strip() for label in months[:n]):
        base = [label if label else "unknown_month" for label in months[:n]]
        mode = "calendar_month"
    else:
        return [], {
            "applied": False,
            "status": "missing_era_timestamps",
            "passed": True,
            "n_obs": int(n),
        }

    if len(regimes) >= n and any(str(label or "").strip() for label in regimes[:n]):
        labels = [
            f"{base_label}|regime:{(regimes[idx] if regimes[idx] else 'unknown')}"
            for idx, base_label in enumerate(base[:n])
        ]
        mode = f"{mode}+regime"
    else:
        labels = list(base[:n])
    return labels, {
        "applied": True,
        "status": "labels_resolved",
        "bucket_mode": str(mode),
        "n_obs": int(n),
    }


def series_ic(x: Sequence[Any], y: Sequence[Any]) -> float:
    a = np.asarray([] if x is None else x, dtype=np.float64).reshape(-1)
    b = np.asarray([] if y is None else y, dtype=np.float64).reshape(-1)
    n = min(int(a.size), int(b.size))
    if n <= 1:
        return 0.0
    a = a[:n]
    b = b[:n]
    mask = np.isfinite(a) & np.isfinite(b)
    if int(np.sum(mask)) <= 1:
        return 0.0
    a = a[mask] - float(np.mean(a[mask]))
    b = b[mask] - float(np.mean(b[mask]))
    denom = float(np.sqrt(np.sum(a * a) * np.sum(b * b)))
    if denom <= 0.0 or not math.isfinite(denom):
        return 0.0
    return float(np.sum(a * b) / denom)


def era_score_table(
    y_true: Sequence[Any],
    y_pred: Sequence[Any],
    labels: Sequence[str],
    *,
    score_kind: str = "neg_mse",
    min_obs: int = 1,
) -> list[dict[str, Any]]:
    truth = np.asarray([] if y_true is None else y_true, dtype=np.float64).reshape(-1)
    pred = np.asarray([] if y_pred is None else y_pred, dtype=np.float64).reshape(-1)
    lab = [str(label or "unknown") for label in ([] if labels is None else list(labels))]
    n = min(int(truth.size), int(pred.size), int(len(lab)))
    by_era: dict[str, dict[str, list[float]]] = {}
    for idx in range(n):
        if not math.isfinite(float(truth[idx])) or not math.isfinite(float(pred[idx])):
            continue
        bucket = by_era.setdefault(str(lab[idx]), {"y": [], "pred": []})
        bucket["y"].append(float(truth[idx]))
        bucket["pred"].append(float(pred[idx]))

    rows: list[dict[str, Any]] = []
    for era in sorted(by_era.keys()):
        y = np.asarray(by_era[era]["y"], dtype=np.float64)
        p = np.asarray(by_era[era]["pred"], dtype=np.float64)
        if int(y.size) < max(1, int(min_obs)):
            continue
        err = y - p
        mse = float(np.mean(err * err)) if int(err.size) else 0.0
        ic = float(series_ic(p, y))
        score = float(ic if str(score_kind).lower() == "ic" else -mse)
        rows.append(
            {
                "era": str(era),
                "n_obs": int(y.size),
                "score": float(score),
                "score_kind": str(score_kind),
                "mse": float(mse),
                "ic": float(ic),
            }
        )
    return rows


def score_std(rows: Sequence[Mapping[str, Any]]) -> float:
    vals = [safe_float((row or {}).get("score"), float("nan")) for row in list(rows or [])]
    arr = [float(value) for value in vals if math.isfinite(float(value))]
    if len(arr) <= 1:
        return 0.0
    return float(np.std(np.asarray(arr, dtype=np.float64), ddof=1))


def worst_half_eras(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    table = [dict(row or {}) for row in list(rows or []) if str((row or {}).get("era") or "").strip()]
    if not table:
        return []
    ordered = sorted(table, key=lambda row: (safe_float(row.get("score"), 0.0), str(row.get("era") or "")))
    count = max(1, int(math.ceil(float(len(ordered)) / 2.0)))
    return [str(row.get("era") or "") for row in ordered[:count]]


def era_boost_config_from_env() -> dict[str, Any]:
    return {
        "enabled": safe_bool(os.environ.get("LGBM_ERA_BOOST", "0"), False),
        "rounds": max(1, safe_int(os.environ.get("LGBM_ERA_BOOST_ROUNDS"), 20)),
        "iters": max(1, safe_int(os.environ.get("LGBM_ERA_BOOST_ITERS"), 4)),
        "max_degrade": max(0.0, safe_float(os.environ.get("ERA_BOOST_MAX_DEGRADE"), 0.02)),
        "score_kind": str(os.environ.get("LGBM_ERA_BOOST_SCORE", "neg_mse") or "neg_mse").strip().lower(),
        "weight_multiplier": max(1.0, safe_float(os.environ.get("LGBM_ERA_BOOST_WEIGHT_MULTIPLIER"), 2.0)),
    }


def validation_degraded(*, prior_loss: float, candidate_loss: float, max_degrade: float) -> bool:
    old = safe_float(prior_loss, float("inf"))
    new = safe_float(candidate_loss, float("inf"))
    if not math.isfinite(old) or old < 0.0:
        return False
    if not math.isfinite(new):
        return True
    return bool(float(new) > float(old) * (1.0 + max(0.0, float(max_degrade))))


__all__ = [
    "coerce_optional_labels",
    "era_boost_config_from_env",
    "era_labels_for",
    "era_score_table",
    "month_label",
    "score_std",
    "series_ic",
    "validation_degraded",
    "worst_half_eras",
]
